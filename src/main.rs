//! Shopping Sessions: native parallel pipeline (Rust).
//! Reads $DATA_DIR, writes $OUTPUT_DIR/{user_daily,campaign_daily}/*.parquet.
//! Every rule below follows rules.md; comments name the rule they implement.

mod scan;
use memchr::{memchr, memchr_iter};
use memmap2::Mmap;
use rayon::prelude::*;
use rustc_hash::{FxHashMap, FxHasher};
use std::fs::File;
use std::hash::{Hash, Hasher};
use std::path::{Path, PathBuf};
use std::sync::Arc;

type S = &'static [u8];

/// Big buffers ask the kernel for transparent huge pages: far fewer page faults and TLB misses.
struct HugeAlloc;
unsafe impl std::alloc::GlobalAlloc for HugeAlloc {
    #[inline]
    unsafe fn alloc(&self, l: std::alloc::Layout) -> *mut u8 {
        let p = std::alloc::System.alloc(l);
        if l.size() >= (4 << 20) && !p.is_null() {
            advise(p, l.size());
        }
        p
    }
    #[inline]
    unsafe fn dealloc(&self, p: *mut u8, l: std::alloc::Layout) {
        std::alloc::System.dealloc(p, l)
    }
    #[inline]
    unsafe fn realloc(&self, p: *mut u8, l: std::alloc::Layout, n: usize) -> *mut u8 {
        let q = std::alloc::System.realloc(p, l, n);
        if n >= (4 << 20) && !q.is_null() {
            advise(q, n);
        }
        q
    }
}
unsafe fn advise(p: *mut u8, size: usize) {
    let a = ((p as usize) + (2 << 20) - 1) & !((2 << 20) - 1);
    let end = ((p as usize) + size) & !((2 << 20) - 1);
    if end > a {
        libc::madvise(a as *mut libc::c_void, end - a, libc::MADV_HUGEPAGE);
    }
}
#[global_allocator]
static GLOBAL: HugeAlloc = HugeAlloc;

use std::sync::atomic::{AtomicU64, Ordering::Relaxed};
static T: [AtomicU64; 10] = [const { AtomicU64::new(0) }; 10];
const TN: [&str; 10] = ["ev-parse", "ck-parse", "dd-gather", "dd-table", "dd-route", "ss-gather", "ss-sort", "ss-walk", "", ""];
#[inline]
fn tick(i: usize, t: std::time::Instant) -> std::time::Instant {
    let n = std::time::Instant::now();
    T[i].fetch_add((n - t).as_nanos() as u64, Relaxed);
    n
}

const DAY: i64 = 86_400_000;
const GAP: i64 = 1_800_000;
const WEEK: i64 = 7 * DAY;
const NB_BITS: u32 = 9; // 512 buckets
const NB: usize = 1 << NB_BITS;
const CHUNK: usize = 4 << 20;

/// wyhash-style 64-bit hash (folded 128-bit multiplies). Strong enough that a person is
/// identified by the hash of their name (collision odds ~1e-7 for millions of names).
#[inline]
fn mum(a: u64, b: u64) -> u64 {
    let r = (a as u128) * (b as u128);
    (r as u64) ^ ((r >> 64) as u64)
}
#[inline]
fn h64(b: &[u8]) -> u64 {
    const P0: u64 = 0xa076_1d64_78bd_642f;
    const P1: u64 = 0xe703_7ed1_a0b4_28db;
    const P2: u64 = 0x8ebc_6af0_9c88_c6e3;
    let mut h = P0 ^ (b.len() as u64).wrapping_mul(P2);
    let mut c = b.chunks_exact(8);
    for w in &mut c {
        h = mum(h ^ u64::from_le_bytes(w.try_into().unwrap()), P1);
    }
    let r = c.remainder();
    let mut t = [0u8; 8];
    t[..r.len()].copy_from_slice(r);
    h = mum(h ^ u64::from_le_bytes(t) ^ P2, P1 ^ (r.len() as u64));
    mum(h, P2)
}
#[inline]
fn bucket(h: u64) -> usize {
    (h >> (64 - NB_BITS)) as usize
}

// ---------------------------------------------------------------- files
fn load(path: &Path) -> S {
    let f = File::open(path).unwrap_or_else(|e| panic!("open {path:?}: {e}"));
    let len = f.metadata().map(|m| m.len()).unwrap_or(0);
    if len == 0 {
        return &[];
    }
    let m = unsafe { Mmap::map(&f) }.expect("mmap");
    let m: &'static Mmap = Box::leak(Box::new(m));
    &m[..]
}

fn list_csv(dir: &Path) -> Vec<PathBuf> {
    let mut v: Vec<PathBuf> = match std::fs::read_dir(dir) {
        Ok(rd) => rd
            .filter_map(|e| e.ok())
            .map(|e| e.path())
            .filter(|p| p.extension().map(|x| x == "csv").unwrap_or(false) && p.is_file())
            .collect(),
        Err(_) => vec![],
    };
    v.sort();
    v
}

struct Chunk {
    data: S,
    skip_header: bool,
}

fn make_chunks(files: &[S]) -> Vec<Chunk> {
    let mut out = Vec::new();
    for &d in files {
        let mut pos = 0;
        while pos < d.len() {
            let mut end = (pos + CHUNK).min(d.len());
            if end < d.len() {
                end = match memchr(b'\n', &d[end..]) {
                    Some(i) => end + i + 1,
                    None => d.len(),
                };
            }
            out.push(Chunk { data: &d[pos..end], skip_header: pos == 0 });
            pos = end;
        }
    }
    // big chunks first for better load balance
    out.sort_by(|a, b| b.data.len().cmp(&a.data.len()));
    out
}

/// Calls f on every non-empty line of the chunk (only '\n' ends a line; empty lines are malformed).
#[inline]
fn for_lines(c: &Chunk, mut f: impl FnMut(S)) {
    let d = c.data;
    let mut start = 0;
    let mut first = c.skip_header;
    for i in memchr_iter(b'\n', d) {
        if first {
            first = false;
        } else if i > start {
            f(&d[start..i]);
        }
        start = i + 1;
    }
    if start < d.len() && !first {
        f(&d[start..]);
    }
}

/// CSV dialect: exactly N comma-separated fields; quotes only around a whole field.
#[inline]
fn split<const N: usize>(line: S, out: &mut [S; N]) -> bool {
    let mut n = 0;
    let mut start = 0;
    for i in memchr_iter(b',', line) {
        if n >= N - 1 {
            return false;
        }
        out[n] = &line[start..i];
        n += 1;
        start = i + 1;
    }
    if n != N - 1 {
        return false;
    }
    out[N - 1] = &line[start..];
    if memchr(b'"', line).is_some() {
        for f in out.iter_mut() {
            if memchr(b'"', f).is_some() {
                let l = f.len();
                if l >= 2 && f[0] == b'"' && f[l - 1] == b'"' && memchr(b'"', &f[1..l - 1]).is_none() {
                    *f = &f[1..l - 1];
                } else {
                    return false;
                }
            }
        }
    }
    true
}

/// Reads a small whole file (header skipped) into rows of N fields.
fn read_small<const N: usize>(path: &Path) -> Vec<[S; N]> {
    let d = load(path);
    let mut rows = Vec::new();
    scan::scan::<N>(d, true, |f| rows.push(*f));
    rows
}

// ---------------------------------------------------------------- timestamps
#[inline(always)]
fn dig(b: u8) -> Option<i64> {
    if b.is_ascii_digit() {
        Some((b - b'0') as i64)
    } else {
        None
    }
}
#[inline(always)]
fn d2(f: &[u8], i: usize) -> Option<i64> {
    Some(dig(f[i])? * 10 + dig(f[i + 1])?)
}
fn days_from_civil(y: i64, m: i64, d: i64) -> i64 {
    let y = if m <= 2 { y - 1 } else { y };
    let era = y.div_euclid(400);
    let yoe = y - era * 400;
    let mp = (m + 9) % 12;
    let doy = (153 * mp + 2) / 5 + d - 1;
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    era * 146_097 + doe - 719_468
}
#[inline]
fn leap(y: i64) -> bool {
    (y % 4 == 0 && y % 100 != 0) || y % 400 == 0
}

/// Days since epoch for every valid calendar date 1900-01-01..2199-12-31 (-1 = impossible date),
/// indexed by (year-1900)*372 + (month-1)*31 + (day-1).
const Y0: i64 = 1900;
const NY: i64 = 300;
static DAYTAB: std::sync::OnceLock<Vec<i32>> = std::sync::OnceLock::new();
fn daytab() -> &'static [i32] {
    DAYTAB.get_or_init(|| {
        let mut t = vec![-1i32; (NY * 372) as usize];
        for y in Y0..Y0 + NY {
            for m in 1..=12 {
                let dim = match m {
                    1 | 3 | 5 | 7 | 8 | 10 | 12 => 31,
                    4 | 6 | 9 | 11 => 30,
                    _ => {
                        if leap(y) {
                            29
                        } else {
                            28
                        }
                    }
                };
                for d in 1..=dim {
                    t[((y - Y0) * 372 + (m - 1) * 31 + (d - 1)) as usize] = days_from_civil(y, m, d) as i32;
                }
            }
        }
        t
    })
}

#[derive(Clone, Copy)]
pub struct TsCache {
    tab: &'static [i32],
}
impl TsCache {
    pub fn new() -> Self {
        TsCache { tab: daytab() }
    }
}

/// F1 / F2 / F3 -> epoch ms. None for every bad form.
fn parse_ts(f: &[u8]) -> Option<i64> {
    parse_ts_c(f, &mut TsCache::new())
}

#[inline]
fn parse_ts_c(f: &[u8], cache: &mut TsCache) -> Option<i64> {
    let off_ms = match f.len() {
        20 if f[19] == b'Z' => 0,
        25 if (f[19] == b'+' || f[19] == b'-') && f[22] == b':' => {
            let oh = d2(f, 20)?;
            let om = d2(f, 23)?;
            if oh > 23 || om > 59 {
                return None;
            }
            let o = (oh * 3600 + om * 60) * 1000;
            if f[19] == b'+' {
                o
            } else {
                -o
            }
        }
        0 => return None,
        _ => {
            // F3: digits only (leading zeros fine); too long for i64 is merely out of the window
            let mut v: i64 = 0;
            for &b in f {
                if !b.is_ascii_digit() {
                    return None;
                }
                v = v.checked_mul(10)?.checked_add((b - b'0') as i64)?;
            }
            return Some(v);
        }
    };
    if f[10] != b'T' || f[13] != b':' || f[16] != b':' {
        // could still be an all-digit F3 of length 20 or 25
        return parse_f3(f);
    }
    if f[4] != b'-' || f[7] != b'-' {
        return None;
    }
    let y = d2(f, 0)? * 100 + d2(f, 2)?;
    let mo = d2(f, 5)?;
    let d = d2(f, 8)?;
    if !(1..=12).contains(&mo) || !(1..=31).contains(&d) {
        return None;
    }
    let days = if (Y0..Y0 + NY).contains(&y) {
        let v = cache.tab[((y - Y0) * 372 + (mo - 1) * 31 + (d - 1)) as usize];
        if v < 0 {
            return None;
        }
        v as i64
    } else {
        let dim = match mo {
            1 | 3 | 5 | 7 | 8 | 10 | 12 => 31,
            4 | 6 | 9 | 11 => 30,
            _ => {
                if leap(y) {
                    29
                } else {
                    28
                }
            }
        };
        if d > dim {
            return None;
        }
        days_from_civil(y, mo, d)
    };
    let h = d2(f, 11)?;
    let mi = d2(f, 14)?;
    let s = d2(f, 17)?;
    if h > 23 || mi > 59 || s > 59 {
        return None;
    }
    Some((days * 86_400 + h * 3600 + mi * 60 + s) * 1000 - off_ms)
}

#[cold]
fn parse_f3(f: &[u8]) -> Option<i64> {
    let mut v: i64 = 0;
    for &b in f {
        if !b.is_ascii_digit() {
            return None;
        }
        v = v.checked_mul(10)?.checked_add((b - b'0') as i64)?;
    }
    Some(v)
}

/// Digits only, 1..=10,000,000.
#[inline]
fn parse_price(f: &[u8]) -> Option<u32> {
    if f.is_empty() {
        return None;
    }
    let mut v: u64 = 0;
    for &b in f {
        if !b.is_ascii_digit() {
            return None;
        }
        v = v * 10 + (b - b'0') as u64;
        if v > 10_000_000 {
            return None;
        }
    }
    if v == 0 {
        None
    } else {
        Some(v as u32)
    }
}

// ---------------------------------------------------------------- links
fn build_links(dir: &Path) -> StrMap<(S, u64)> {
    let files: Vec<S> = list_csv(dir).iter().map(|p| load(p)).collect();
    let chunks = make_chunks(&files);
    let pairs: Vec<Vec<(S, S)>> = chunks
        .par_iter()
        .map(|c| {
            let mut v = Vec::new();
            scan::scan::<2>(c.data, c.skip_header, |f| {
                if !f[0].is_empty() && !f[1].is_empty() && f[0] != f[1] {
                    v.push((f[0], f[1]));
                }
            });
            v
        })
        .collect();
    let n: usize = pairs.iter().map(|v| v.len()).sum();
    let mut idx: FxHashMap<S, u32> = FxHashMap::with_capacity_and_hasher(n * 2, Default::default());
    let mut names: Vec<S> = Vec::with_capacity(n * 2);
    let mut parent: Vec<u32> = Vec::with_capacity(n * 2);
    fn find(p: &mut [u32], mut x: u32) -> u32 {
        while p[x as usize] != x {
            let g = p[p[x as usize] as usize];
            p[x as usize] = g;
            x = g;
        }
        x
    }
    let mut id = |s: S, names: &mut Vec<S>, parent: &mut Vec<u32>| -> u32 {
        *idx.entry(s).or_insert_with(|| {
            let i = names.len() as u32;
            names.push(s);
            parent.push(i);
            i
        })
    };
    for v in &pairs {
        for &(a, b) in v {
            let ia = id(a, &mut names, &mut parent);
            let ib = id(b, &mut names, &mut parent);
            let ra = find(&mut parent, ia);
            let rb = find(&mut parent, ib);
            if ra != rb {
                // keep the root as the byte-wise smaller name: the root then *is* the name
                if names[ra as usize] < names[rb as usize] {
                    parent[rb as usize] = ra;
                } else {
                    parent[ra as usize] = rb;
                }
            }
        }
    }
    let mut pairs_out: Vec<(S, (S, u64))> = Vec::with_capacity(names.len());
    for i in 0..names.len() as u32 {
        let r = find(&mut parent, i);
        if r != i {
            pairs_out.push((names[i as usize], (names[r as usize], h64(names[r as usize]))));
        }
    }
    let n = pairs_out.len();
    StrMap::new(pairs_out, n)
}

// ---------------------------------------------------------------- records
#[derive(Clone, Copy)]
struct Ev {
    eh: u64,
    uh: u64, // hash of user_id, computed while the line is hot
    eidp: *const u8,
    userp: *const u8,
    ingest: i64,
    ts: i64,
    price: u32,
    cat: u16,
    elen: u16,
    ulen: u16,
    etype: u8, // 0 = invalid copy
}
unsafe impl Send for Ev {}
unsafe impl Sync for Ev {}
impl Ev {
    #[inline]
    fn eid(&self) -> S {
        unsafe { std::slice::from_raw_parts(self.eidp, self.elen as usize) }
    }
    #[inline]
    fn user(&self) -> S {
        unsafe { std::slice::from_raw_parts(self.userp, self.ulen as usize) }
    }
}

#[derive(Clone, Copy)]
struct PE {
    nh: u64,
    ts: i64,
    nptr: *const u8,
    price: u32, // >0 only for purchases
    cat: u16,
    nlen: u16,
}
unsafe impl Send for PE {}
unsafe impl Sync for PE {}
impl PE {
    #[inline]
    fn name(&self) -> S {
        unsafe { std::slice::from_raw_parts(self.nptr, self.nlen as usize) }
    }
}

#[derive(Clone, Copy)]
struct RawClick {
    user: S,
    cid: S,
    ts: i64,
    camp: u32,
}

#[derive(Clone, Copy)]
struct PC {
    nh: u64,
    name: S,
    ts: i64,
    cid: S,
    camp: u32,
}

struct UserRow {
    name: S,
    day: i32,
    sessions: u64,
    events: u64,
    revenue: u64,
    top: Option<u16>,
}

fn main() {
    let data = PathBuf::from(std::env::var("DATA_DIR").unwrap_or_else(|_| "/data".into()));
    let out = PathBuf::from(std::env::var("OUTPUT_DIR").unwrap_or_else(|_| "/output".into()));
    let ncpu: usize = std::env::var("NCPU").ok().and_then(|v| v.parse().ok()).unwrap_or(0);
    let mut pool = rayon::ThreadPoolBuilder::new();
    if ncpu > 0 {
        pool = pool.num_threads(ncpu);
    }
    pool.build_global().ok();
    let t0 = std::time::Instant::now();
    let timing = std::env::var("TIMING").is_ok();
    let lap = |what: &str| {
        if timing {
            eprintln!("{:>8.3}s {what}", t0.elapsed().as_secs_f64());
        }
    };

    // manifest
    let man = std::fs::read_to_string(data.join("manifest.json")).expect("manifest");
    let grab = |key: &str| -> i64 {
        let i = man.find(key).expect("key") + key.len();
        let rest = &man[i..];
        let q1 = rest.find('"').unwrap();
        let rest = &rest[q1 + 1..];
        let q2 = rest.find('"').unwrap();
        parse_ts(rest[..q2].as_bytes()).expect("window ts")
    };
    let ws = grab("\"window_start\"");
    let we = grab("\"window_end\"");
    let day0 = ws.div_euclid(DAY);
    let ndays = ((we - 1).div_euclid(DAY) - day0 + 1).max(1) as usize;

    // small tables and the big files (mmap) first; links are built while events are parsed
    let ((products, campaigns), (ev_files, ck_files)) = rayon::join(
        || (read_small::<2>(&data.join("products.csv")), read_small::<2>(&data.join("campaigns.csv"))),
        || {
            (
                list_csv(&data.join("events")).par_iter().map(|p| load(p)).collect::<Vec<S>>(),
                list_csv(&data.join("clicks")).par_iter().map(|p| load(p)).collect::<Vec<S>>(),
            )
        },
    );
    lap("small tables + mmap");

    // categories, ranked byte-wise so the smaller index wins ties
    let mut cats: Vec<S> = products.iter().map(|r| r[1]).collect();
    cats.push(b"unknown");
    cats.sort_unstable();
    cats.dedup();
    let cat_idx: FxHashMap<S, u16> = cats.iter().enumerate().map(|(i, &c)| (c, i as u16)).collect();
    let unknown = cat_idx[&b"unknown"[..]];
    let prod: StrMap<u16> = StrMap::new(products.iter().map(|r| (r[0], cat_idx[&r[1]])), products.len());
    let ncat = cats.len();
    let camp_idx: StrMap<u32> = StrMap::new(campaigns.iter().enumerate().map(|(i, r)| (r[0], i as u32)), campaigns.len());
    let ncamp = campaigns.len();
    let organic = ncamp as u32;


    // ------------------------------------------------ events: parse + validate each copy, bucket by event_id
    let ev_chunks = make_chunks(&ev_files);
    let ck_chunks = make_chunks(&ck_files);
    let nthreads = rayon::current_num_threads().max(1);
    // a few groups per thread: few accumulators, decent balance (chunks are sorted big-first)
    let groups = |n: usize| -> Vec<Vec<usize>> {
        let g = (nthreads * 3).min(n.max(1));
        let mut v = vec![Vec::new(); g];
        for i in 0..n {
            // snake order so every group gets a mix of big and small chunks
            let r = i / g;
            let k = if r % 2 == 0 { i % g } else { g - 1 - i % g };
            v[k].push(i);
        }
        v
    };
    let ev_groups = groups(ev_chunks.len());
    let ck_groups = groups(ck_chunks.len());
    let (links, (ev_parts, raw_cks)) = rayon::join(|| build_links(&data.join("links")), || rayon::join(
        || {
            ev_groups
                .par_iter()
                .map(|grp| {
                    let bytes: usize = grp.iter().map(|&i| ev_chunks[i].data.len()).sum();
                    let est = bytes / 72 / NB + 16;
                    let t0 = std::time::Instant::now();
                    let mut acc: Vec<Vec<Ev>> = (0..NB).map(|_| Vec::with_capacity(est)).collect();
                    let mut c1 = TsCache::new();
                    let mut c2 = TsCache::new();
                    for &ci in grp {
                        let c = &ev_chunks[ci];
                        scan::scan::<7>(c.data, c.skip_header, |f| {
                            if f[0].is_empty() {
                                return;
                            }
                            let ingest = parse_ts_c(f[3], &mut c2).unwrap_or(i64::MAX);
                            let eh = h64(f[0]);
                            assert!(f[0].len() <= 65535 && f[1].len() <= 65535, "field too long");
                            let mut e = Ev {
                                eh,
                                uh: 0,
                                eidp: f[0].as_ptr(),
                                userp: f[1].as_ptr(),
                                ingest,
                                ts: 0,
                                price: 0,
                                cat: 0,
                                elen: f[0].len() as u16,
                                ulen: f[1].len() as u16,
                                etype: 0,
                            };
                            'v: {
                                if f[1].is_empty() || f[5].is_empty() {
                                    break 'v;
                                }
                                let ts = match parse_ts_c(f[2], &mut c1) {
                                    Some(t) if t >= ws && t < we => t,
                                    _ => break 'v,
                                };
                                let et = match f[4] {
                                    b"view" => 1,
                                    b"cart" => 2,
                                    b"purchase" => 3,
                                    _ => break 'v,
                                };
                                if et == 3 {
                                    match parse_price(f[6]) {
                                        Some(p) => e.price = p,
                                        None => break 'v,
                                    }
                                }
                                e.ts = ts;
                                e.etype = et;
                                e.cat = prod.get(f[5]).unwrap_or(unknown);
                                e.uh = h64(f[1]);
                            }
                            acc[bucket(eh)].push(e);
                        });
                    }
                    tick(0, t0);
                    acc
                })
                .collect::<Vec<_>>()
        },
        || {
            // clicks: no dedup needed (copies are identical, so the credit and assist sets are the same)
            ck_groups
                .par_iter()
                .map(|grp| {
                    let t0 = std::time::Instant::now();
                    let bytes: usize = grp.iter().map(|&i| ck_chunks[i].data.len()).sum();
                    let mut acc: Vec<RawClick> = Vec::with_capacity(bytes / 45 + 16);
                    let mut c1 = TsCache::new();
                    for &ci in grp {
                        let c = &ck_chunks[ci];
                        scan::scan::<4>(c.data, c.skip_header, |f| {
                            if f[0].is_empty() || f[1].is_empty() {
                                return;
                            }
                            let ts = match parse_ts_c(f[3], &mut c1) {
                                Some(t) if t >= ws - WEEK && t < we => t,
                                _ => return,
                            };
                            let camp = match camp_idx.get(f[2]) {
                                Some(c) => c,
                                None => return,
                            };
                            acc.push(RawClick { user: f[1], cid: f[0], ts, camp });
                        });
                    }
                    tick(1, t0);
                    acc
                })
                .collect::<Vec<_>>()
        },
    ));
    lap("parse events + clicks (+ links)");
    // returns (name, hash of name)
    let name_of = |u: S| -> (S, u64) {
        let h = h64(u);
        match links.get_h(u, h) {
            Some(n) => n,
            None => (u, h),
        }
    };


    // ------------------------------------------------ dedup per event bucket -> person buckets
    let bgroups: Vec<Vec<usize>> = {
        let g = nthreads * 4;
        (0..g).map(|k| (k..NB).step_by(g).collect()).collect()
    };
    let (pe_parts, ck_parts): (Vec<Vec<Vec<PE>>>, Vec<Vec<Vec<PC>>>) = rayon::join(
        || {
            bgroups
        .par_iter()
        .map(|bs| {
            let mine: usize = bs.iter().map(|&b| ev_parts.iter().map(|p| p[b].len()).sum::<usize>()).sum();
            let mut acc: Vec<Vec<PE>> = (0..NB).map(|_| Vec::with_capacity(mine / NB + mine / NB / 4 + 16)).collect();
            let mut all: Vec<Ev> = Vec::new();
            let mut table: Vec<u32> = Vec::new();
            let mut win: Vec<bool> = Vec::new();
            for &b in bs {
                let t = std::time::Instant::now();
                all.clear();
                for p in &ev_parts {
                    all.extend_from_slice(&p[b]);
                }
                let t = tick(2, t);
                let cap = (all.len() * 2).next_power_of_two().max(16);
                table.clear();
                table.resize(cap, u32::MAX);
                let mask = cap - 1;
                for (i, e) in all.iter().enumerate() {
                    let mut pos = (e.eh as usize) & mask;
                    loop {
                        let t = table[pos];
                        if t == u32::MAX {
                            table[pos] = i as u32;
                            break;
                        }
                        let k = &all[t as usize];
                        if k.eh == e.eh && k.eid() == e.eid() {
                            if e.ingest < k.ingest {
                                table[pos] = i as u32;
                            }
                            break;
                        }
                        pos = (pos + 1) & mask;
                    }
                }
                let t = tick(3, t);
                // mark winners, then walk `all` in memory order (cache friendly)
                win.clear();
                win.resize(all.len(), false);
                for &t in &table {
                    if t != u32::MAX {
                        win[t as usize] = true;
                    }
                }
                // survivors in batches: hash + prefetch the link slots first, then resolve
                const BATCH: usize = 32;
                let mut batch: [(usize, u64); BATCH] = [(0, 0); BATCH];
                let mut nb = 0;
                let mut flush = |batch: &[(usize, u64)], acc: &mut Vec<Vec<PE>>| {
                    for &(i, h) in batch {
                        let e = &all[i];
                        let (name, nh) = match links.get_hash_only(h) {
                            Some(n) => n,
                            None => (e.user(), h),
                        };
                        assert!(name.len() <= u16::MAX as usize, "user id too long");
                        acc[bucket(nh)].push(PE {
                            nh,
                            nptr: name.as_ptr(),
                            nlen: name.len() as u16,
                            ts: e.ts,
                            price: if e.etype == 3 { e.price } else { 0 },
                            cat: e.cat,
                        });
                    }
                };
                for (i, (e, &w)) in all.iter().zip(win.iter()).enumerate() {
                    if !w || e.etype == 0 || e.ingest - e.ts > DAY {
                        continue;
                    }
                    let h = e.uh;
                    links.prefetch(h);
                    batch[nb] = (i, h);
                    nb += 1;
                    if nb == BATCH {
                        flush(&batch, &mut acc);
                        nb = 0;
                    }
                }
                flush(&batch[..nb], &mut acc);
                tick(4, t);
            }
            acc
        })
        .collect::<Vec<_>>()
        },
        || {
            raw_cks
                .par_iter()
                .map(|raw| {
                    let mut acc: Vec<Vec<PC>> = (0..NB).map(|_| Vec::with_capacity(raw.len() / NB + raw.len() / NB / 4 + 16)).collect();
                    const BATCH: usize = 32;
                    for part in raw.chunks(BATCH) {
                        let mut hs = [0u64; BATCH];
                        for (k, r) in part.iter().enumerate() {
                            hs[k] = h64(r.user);
                            links.prefetch(hs[k]);
                        }
                        for (k, r) in part.iter().enumerate() {
                            let (name, nh) = match links.get_h(r.user, hs[k]) {
                                Some(n) => n,
                                None => (r.user, hs[k]),
                            };
                            acc[bucket(nh)].push(PC { nh, name, ts: r.ts, cid: r.cid, camp: r.camp });
                        }
                    }
                    acc
                })
                .collect::<Vec<_>>()
        },
    );
    drop(raw_cks);
    drop(ev_parts);
    lap("dedup");

    // ------------------------------------------------ per person bucket: sessions, credit, assists
    let width = ncamp + 1;
    let results: Vec<(Vec<UserRow>, Vec<[u64; 4]>)> = bgroups
        .par_iter()
        .map(|bs| {
            let mut camp = vec![[0u64; 4]; width * ndays];
            let mut stamp = vec![0u32; width];
            let mut sess_no: u32 = 0;
            let mut rows: Vec<UserRow> = Vec::new();
            let mut catsum = vec![0u64; ncat];
            let mut touched: Vec<u16> = Vec::new();
            let mut evs: Vec<PE> = Vec::new();
            let mut tmp: Vec<PE> = Vec::new();
            let mut cks: Vec<PC> = Vec::new();
            for &b in bs {
            let t = std::time::Instant::now();
            evs.clear();
            for p in &pe_parts {
                evs.extend_from_slice(&p[b]);
            }
            cks.clear();
            for p in &ck_parts {
                cks.extend_from_slice(&p[b]);
            }
            let t = tick(5, t);
            evs.sort_unstable_by_key(|e| (e.nh, e.ts));
            cks.sort_unstable_by(|a, b| a.nh.cmp(&b.nh).then(a.ts.cmp(&b.ts)).then_with(|| a.cid.cmp(b.cid)));
            let t = tick(6, t);
            let mut i = 0;
            let mut ci = 0;
            while i < evs.len() {
                let (nh, name) = (evs[i].nh, evs[i].name());
                let mut j = i;
                while j < evs.len() && evs[j].nh == nh {
                    j += 1;
                }
                // this person's clicks
                while ci < cks.len() && cks[ci].nh < nh {
                    ci += 1;
                }
                let mut cj = ci;
                while cj < cks.len() && cks[cj].nh == nh {
                    cj += 1;
                }
                let pc = &cks[ci..cj];
                let (mut lo, mut hi) = (0usize, 0usize); // window [lo, hi) of clicks with s-WEEK <= ts <= s

                let mut cur: Option<UserRow> = None;
                let mut k = i;
                while k < j {
                    // one session
                    let start = evs[k].ts;
                    let day = start.div_euclid(DAY);
                    let di = (day - day0) as usize;
                    let d32 = day as i32;
                    // a new day: close the previous row before this session's purchases are summed
                    if cur.as_ref().map(|r| r.day != d32).unwrap_or(false) {
                        let mut r = cur.take().unwrap();
                        r.top = top_of(&mut catsum, &mut touched);
                        rows.push(r);
                    }
                    let mut last = start;
                    let mut n_ev = 0u64;
                    let mut rev = 0u64;
                    let mut m = k;
                    while m < j && evs[m].ts - last <= GAP {
                        let e = &evs[m];
                        last = e.ts;
                        n_ev += 1;
                        if e.price > 0 {
                            rev += e.price as u64;
                            if catsum[e.cat as usize] == 0 {
                                touched.push(e.cat);
                            }
                            catsum[e.cat as usize] += e.price as u64;
                        }
                        m += 1;
                    }
                    // attribution
                    while hi < pc.len() && pc[hi].ts <= start {
                        hi += 1;
                    }
                    while lo < hi && pc[lo].ts < start - WEEK {
                        lo += 1;
                    }
                    let credited = if lo < hi {
                        let t = pc[hi - 1].ts;
                        let mut q = hi - 1;
                        while q > lo && pc[q - 1].ts == t {
                            q -= 1;
                        }
                        sess_no += 1;
                        for c in &pc[lo..hi] {
                            if stamp[c.camp as usize] != sess_no {
                                stamp[c.camp as usize] = sess_no;
                                camp[c.camp as usize * ndays + di][3] += 1;
                            }
                        }
                        pc[q].camp
                    } else {
                        organic
                    };
                    let cell = &mut camp[credited as usize * ndays + di];
                    cell[0] += 1;
                    if rev > 0 {
                        cell[1] += 1;
                    }
                    cell[2] += rev;

                    // user_daily: sessions come in time order, so days are non-decreasing
                    match &mut cur {
                        Some(r) => {
                            r.sessions += 1;
                            r.events += n_ev;
                            r.revenue += rev;
                        }
                        None => {
                            cur = Some(UserRow { name, day: d32, sessions: 1, events: n_ev, revenue: rev, top: None });
                        }
                    }
                    k = m;
                }
                if let Some(mut r) = cur.take() {
                    r.top = top_of(&mut catsum, &mut touched);
                    rows.push(r);
                }
                i = j;
            }
            tick(7, t);
            }
            (rows, camp)
        })
        .collect();
    drop(pe_parts);
    lap("sessions");

    // ------------------------------------------------ write
    let mut camp_total = vec![[0u64; 4]; width * ndays];
    for (_, c) in &results {
        for (t, v) in camp_total.iter_mut().zip(c.iter()) {
            for x in 0..4 {
                t[x] += v[x];
            }
        }
    }
    let ud = out.join("user_daily");
    let cd = out.join("campaign_daily");
    std::fs::create_dir_all(&ud).unwrap();
    std::fs::create_dir_all(&cd).unwrap();

    let cats = Arc::new(cats);
    let per = results.len().div_ceil(8).max(1);
    rayon::join(
        || {
            results.par_chunks(per).enumerate().for_each(|(g, part)| {
                write_users(&ud.join(format!("part-{g:05}.parquet")), part, &cats);
            })
        },
        || {
            let mut cid: Vec<String> = Vec::new();
            let mut ch: Vec<String> = Vec::new();
            let mut day: Vec<i32> = Vec::new();
            let mut cols = [Vec::<i64>::new(), Vec::new(), Vec::new(), Vec::new()];
            for c in 0..width {
                for d in 0..ndays {
                    let v = camp_total[c * ndays + d];
                    if v[0] == 0 && v[3] == 0 {
                        continue;
                    }
                    if c == ncamp {
                        cid.push("organic".into());
                        ch.push("organic".into());
                    } else {
                        cid.push(String::from_utf8_lossy(campaigns[c][0]).into_owned());
                        ch.push(String::from_utf8_lossy(campaigns[c][1]).into_owned());
                    }
                    day.push((day0 + d as i64) as i32);
                    for x in 0..4 {
                        cols[x].push(v[x] as i64);
                    }
                }
            }
            write_campaigns(&cd.join("part-00000.parquet"), cid, ch, day, cols);
        },
    );
    lap("write");
    if timing {
        for i in 0..8 {
            eprintln!("  {:>10} {:.3}s", TN[i], T[i].load(Relaxed) as f64 / 1e9);
        }
    }
}

/// Sort by (nh, ts): LSD radix on 33 bits of nh below the bucket bits, then fix up each run.
fn radix_sort_pe(v: &mut Vec<PE>, tmp: &mut Vec<PE>) {
    let n = v.len();
    if n < 64 {
        v.sort_unstable_by_key(|e| (e.nh, e.ts));
        return;
    }
    if tmp.len() < n {
        tmp.resize(n, v[0]);
    }
    for pass in 0..3u32 {
        let shift = 10 + 11 * pass;
        let mut cnt = [0usize; 2048];
        for e in v.iter() {
            cnt[((e.nh >> shift) & 2047) as usize] += 1;
        }
        let mut sum = 0;
        for c in cnt.iter_mut() {
            let t = *c;
            *c = sum;
            sum += t;
        }
        for e in v.iter() {
            let d = ((e.nh >> shift) & 2047) as usize;
            tmp[cnt[d]] = *e;
            cnt[d] += 1;
        }
        std::mem::swap(v, tmp);
        v.truncate(n);
    }
    let key = |e: &PE| (e.nh >> 10) & ((1u64 << 33) - 1);
    let mut i = 0;
    while i < n {
        let k = key(&v[i]);
        let mut j = i + 1;
        while j < n && key(&v[j]) == k {
            j += 1;
        }
        if j - i > 1 {
            v[i..j].sort_unstable_by_key(|e| (e.nh, e.ts));
        }
        i = j;
    }
}

fn top_of(catsum: &mut [u64], touched: &mut Vec<u16>) -> Option<u16> {
    if touched.is_empty() {
        return None;
    }
    let mut best: Option<(u64, u16)> = None;
    for &c in touched.iter() {
        let v = catsum[c as usize];
        best = match best {
            None => Some((v, c)),
            Some((bv, bc)) if v > bv || (v == bv && c < bc) => Some((v, c)),
            x => x,
        };
        catsum[c as usize] = 0;
    }
    touched.clear();
    best.map(|b| b.1)
}

// ---------------------------------------------------------------- parquet
use arrow_array::{ArrayRef, Date32Array, Int64Array, RecordBatch, StringArray};
use arrow_schema::{DataType, Field, Schema};
use parquet::arrow::ArrowWriter;
use parquet::basic::Compression;
use parquet::file::properties::WriterProperties;

fn props() -> WriterProperties {
    WriterProperties::builder()
        .set_compression(Compression::UNCOMPRESSED)
        .set_dictionary_enabled(false)
        .set_statistics_enabled(parquet::file::properties::EnabledStatistics::None)
        .build()
}

fn write_batch(path: &Path, schema: Arc<Schema>, cols: Vec<ArrayRef>) {
    let batch = RecordBatch::try_new(schema.clone(), cols).expect("batch");
    let f = File::create(path).expect("create parquet");
    let mut w = ArrowWriter::try_new(std::io::BufWriter::with_capacity(1 << 20, f), schema, Some(props())).unwrap();
    w.write(&batch).unwrap();
    w.close().unwrap();
}

fn lossy(b: &[u8]) -> std::borrow::Cow<'_, str> {
    String::from_utf8_lossy(b)
}

fn write_users(path: &Path, parts: &[(Vec<UserRow>, Vec<[u64; 4]>)], cats: &[S]) {
    let n: usize = parts.iter().map(|p| p.0.len()).sum();
    let mut name: Vec<std::borrow::Cow<str>> = Vec::with_capacity(n);
    let mut day = Vec::with_capacity(n);
    let mut s = Vec::with_capacity(n);
    let mut e = Vec::with_capacity(n);
    let mut r = Vec::with_capacity(n);
    let mut t: Vec<Option<std::borrow::Cow<str>>> = Vec::with_capacity(n);
    for (rows, _) in parts {
        for row in rows {
            name.push(lossy(row.name));
            day.push(row.day);
            s.push(row.sessions as i64);
            e.push(row.events as i64);
            r.push(row.revenue as i64);
            t.push(row.top.map(|c| lossy(cats[c as usize])));
        }
    }
    let schema = Arc::new(Schema::new(vec![
        Field::new("user_id", DataType::Utf8, false),
        Field::new("day", DataType::Date32, false),
        Field::new("sessions", DataType::Int64, false),
        Field::new("events", DataType::Int64, false),
        Field::new("revenue_cents", DataType::Int64, false),
        Field::new("top_category", DataType::Utf8, true),
    ]));
    let cols: Vec<ArrayRef> = vec![
        Arc::new(StringArray::from_iter_values(name.iter())),
        Arc::new(Date32Array::from(day)),
        Arc::new(Int64Array::from(s)),
        Arc::new(Int64Array::from(e)),
        Arc::new(Int64Array::from(r)),
        Arc::new(StringArray::from_iter(t.iter().map(|x| x.as_deref()))),
    ];
    write_batch(path, schema, cols);
}

fn write_campaigns(path: &Path, cid: Vec<String>, ch: Vec<String>, day: Vec<i32>, cols: [Vec<i64>; 4]) {
    let schema = Arc::new(Schema::new(vec![
        Field::new("campaign_id", DataType::Utf8, false),
        Field::new("channel", DataType::Utf8, false),
        Field::new("day", DataType::Date32, false),
        Field::new("sessions", DataType::Int64, false),
        Field::new("purchasing_sessions", DataType::Int64, false),
        Field::new("revenue_cents", DataType::Int64, false),
        Field::new("assisted_sessions", DataType::Int64, false),
    ]));
    let [a, b, c, d] = cols;
    let arrs: Vec<ArrayRef> = vec![
        Arc::new(StringArray::from(cid)),
        Arc::new(StringArray::from(ch)),
        Arc::new(Date32Array::from(day)),
        Arc::new(Int64Array::from(a)),
        Arc::new(Int64Array::from(b)),
        Arc::new(Int64Array::from(c)),
        Arc::new(Int64Array::from(d)),
    ];
    write_batch(path, schema, arrs);
}

// ---------------------------------------------------------------- fast string map
/// Open-addressing map from byte strings to small values. Keys up to 16 bytes are stored inline,
/// so a lookup usually touches a single cache line. Exact: keys are always compared in full.
#[derive(Clone, Copy)]
#[repr(C, align(64))]
struct Slot<V: Copy> {
    h: u64,
    len: u32,
    used: bool,
    inl: [u8; 16],
    ptr: *const u8,
    val: V,
}
unsafe impl<V: Copy> Sync for Slot<V> {}
unsafe impl<V: Copy> Send for Slot<V> {}

impl<V: Copy> Slot<V> {
    #[inline]
    fn key(&self) -> &'static [u8] {
        unsafe { std::slice::from_raw_parts(self.ptr, self.len as usize) }
    }
}

pub struct StrMap<V: Copy> {
    mask: usize,
    slots: Vec<Slot<V>>,
    filter: Vec<u64>, // 1 bit per hash prefix: a clear bit means "not present"
    fbits: u32,
}

impl<V: Copy + Default> StrMap<V> {
    pub fn new(items: impl IntoIterator<Item = (S, V)>, n: usize) -> Self {
        let cap = (n * 2).next_power_of_two().max(16);
        let fbits = ((n * 16).next_power_of_two().max(1 << 12).trailing_zeros()).min(30);
        let mut m = StrMap {
            mask: cap - 1,
            slots: vec![Slot { h: 0, len: 0, used: false, inl: [0; 16], ptr: std::ptr::null(), val: V::default() }; cap],
            filter: vec![0u64; (1usize << fbits) / 64],
            fbits,
        };
        for (k, v) in items {
            m.insert(k, v);
        }
        m
    }
    fn insert(&mut self, key: S, val: V) {
        let h = h64(key);
        let mut pos = (h as usize) & self.mask;
        loop {
            let s = &mut self.slots[pos];
            if !s.used {
                let mut inl = [0u8; 16];
                if key.len() <= 16 {
                    inl[..key.len()].copy_from_slice(key);
                }
                *s = Slot { h, len: key.len() as u32, used: true, inl, ptr: key.as_ptr(), val };
                let f = (h >> (64 - self.fbits)) as usize;
                self.filter[f >> 6] |= 1 << (f & 63);
                return;
            }
            if s.h == h && s.key() == key {
                s.val = val;
                return;
            }
            pos = (pos + 1) & self.mask;
        }
    }
    #[inline]
    pub fn get_h(&self, key: &[u8], h: u64) -> Option<V> {
        let f = (h >> (64 - self.fbits)) as usize;
        if self.filter[f >> 6] & (1 << (f & 63)) == 0 {
            return None;
        }
        let mut pos = (h as usize) & self.mask;
        loop {
            let s = &self.slots[pos];
            if !s.used {
                return None;
            }
            if s.h == h && s.len as usize == key.len() {
                let eq = if key.len() <= 16 { &s.inl[..key.len()] == key } else { s.key() == key };
                if eq {
                    return Some(s.val);
                }
            }
            pos = (pos + 1) & self.mask;
        }
    }
    #[inline]
    pub fn prefetch(&self, h: u64) {
        #[cfg(target_arch = "x86_64")]
        unsafe {
            use std::arch::x86_64::{_mm_prefetch, _MM_HINT_T0};
            let f = (h >> (64 - self.fbits)) as usize;
            _mm_prefetch(self.filter.as_ptr().add(f >> 6) as *const i8, _MM_HINT_T0);
            _mm_prefetch(self.slots.as_ptr().add((h as usize) & self.mask) as *const i8, _MM_HINT_T0);
        }
    }
    /// Lookup trusting the 64-bit hash alone (same identity assumption as persons): no key bytes read.
    #[inline]
    pub fn get_hash_only(&self, h: u64) -> Option<V> {
        let f = (h >> (64 - self.fbits)) as usize;
        if self.filter[f >> 6] & (1 << (f & 63)) == 0 {
            return None;
        }
        let mut pos = (h as usize) & self.mask;
        loop {
            let s = &self.slots[pos];
            if !s.used {
                return None;
            }
            if s.h == h {
                return Some(s.val);
            }
            pos = (pos + 1) & self.mask;
        }
    }
    #[inline]
    pub fn get(&self, key: &[u8]) -> Option<V> {
        self.get_h(key, h64(key))
    }
}
