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

use std::sync::atomic::{AtomicU32, AtomicU64, Ordering::Relaxed};
static T: [AtomicU64; 10] = [const { AtomicU64::new(0) }; 10];
const TN: [&str; 10] = [
    "ev-parse",
    "ck-parse",
    "dd-gather",
    "dd-table",
    "dd-route",
    "ss-gather",
    "ss-sort",
    "ss-walk",
    "",
    "",
];
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

struct Config {
    group_multiplier: usize,
    populate_input: bool,
    prefault_write: bool,
}

fn config() -> &'static Config {
    static CONFIG: std::sync::OnceLock<Config> = std::sync::OnceLock::new();
    CONFIG.get_or_init(|| {
        let group_multiplier = std::env::var("GROUP_MULTIPLIER")
            .unwrap_or_else(|_| "2".into())
            .parse::<usize>()
            .ok()
            .filter(|n| matches!(n, 1 | 2 | 4))
            .expect("GROUP_MULTIPLIER must be 1, 2, or 4");
        let switch = |name, default| match std::env::var(name) {
            Ok(value) => match value.as_str() {
                "1" | "true" => true,
                "0" | "false" => false,
                _ => panic!("{name} must be 0, 1, false, or true"),
            },
            Err(std::env::VarError::NotPresent) => default,
            Err(error) => panic!("{name}: {error}"),
        };
        Config {
            group_multiplier,
            populate_input: switch("MMAP_POPULATE", true),
            prefault_write: switch("PREFAULT_WRITE", true),
        }
    })
}

fn bucket_groups(nthreads: usize, multiplier: usize) -> Vec<Vec<usize>> {
    let groups = nthreads.saturating_mul(multiplier).clamp(1, NB);
    (0..groups)
        .map(|k| (k..NB).step_by(groups).collect())
        .collect()
}

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
    #[inline(always)]
    fn r8(b: &[u8], i: usize) -> u64 {
        u64::from_le_bytes(b[i..i + 8].try_into().unwrap())
    }
    #[inline(always)]
    fn r4(b: &[u8], i: usize) -> u64 {
        u32::from_le_bytes(b[i..i + 4].try_into().unwrap()) as u64
    }
    let n = b.len();
    let mut h = P0 ^ (n as u64).wrapping_mul(P2);
    // (a, c) together cover every byte (overlapping reads, no copies)
    let (a, c) = if n <= 16 {
        if n >= 8 {
            (r8(b, 0), r8(b, n - 8))
        } else if n >= 4 {
            (r4(b, 0), r4(b, n - 4))
        } else if n > 0 {
            (
                ((b[0] as u64) << 16) | ((b[n >> 1] as u64) << 8) | b[n - 1] as u64,
                0,
            )
        } else {
            (0, 0)
        }
    } else {
        let mut i = 0;
        while i + 16 < n {
            h = mum(r8(b, i) ^ P1, r8(b, i + 8) ^ h);
            i += 16;
        }
        (r8(b, n - 16), r8(b, n - 8))
    };
    h = mum(a ^ P1, c ^ h);
    mum(h ^ P1, P2 ^ n as u64)
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
    let mut options = memmap2::MmapOptions::new();
    if config().populate_input {
        options.populate();
    }
    let m = unsafe { options.map(&f) }.expect("mmap");
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
            out.push(Chunk {
                data: &d[pos..end],
                skip_header: pos == 0,
            });
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
                if l >= 2
                    && f[0] == b'"'
                    && f[l - 1] == b'"'
                    && memchr(b'"', &f[1..l - 1]).is_none()
                {
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
                    t[((y - Y0) * 372 + (m - 1) * 31 + (d - 1)) as usize] =
                        days_from_civil(y, m, d) as i32;
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

#[inline(always)]
fn ld8(f: &[u8], i: usize) -> u64 {
    u64::from_le_bytes(f[i..i + 8].try_into().unwrap())
}
/// Every byte selected by `m` is an ASCII digit.
#[inline(always)]
fn digits_at(x: u64, m: u64) -> bool {
    let x = (x & m) | (0x3030_3030_3030_3030 & !m); // non-selected bytes become '0'
    ((x & 0xF0F0_F0F0_F0F0_F0F0)
        | (((x.wrapping_add(0x0606_0606_0606_0606)) & 0xF0F0_F0F0_F0F0_F0F0) >> 4))
        == 0x3333_3333_3333_3333
}
/// 8 ASCII digits (first digit in the lowest byte) -> value.
#[inline(always)]
fn swar8(x: u64) -> u64 {
    let mut v = x.wrapping_sub(0x3030_3030_3030_3030);
    v = (v.wrapping_mul(10).wrapping_add(v >> 8)) & 0x00FF_00FF_00FF_00FF;
    v = (v.wrapping_mul(100).wrapping_add(v >> 16)) & 0x0000_FFFF_0000_FFFF;
    (v.wrapping_mul(10000).wrapping_add(v >> 32)) & 0xFFFF_FFFF
}
#[inline(always)]
fn byte(x: u64, i: u32) -> i64 {
    ((x >> (8 * i)) & 0xff) as i64 - 48
}

/// F1 / F2 / F3 -> epoch ms. None for every bad form (see rules.md).
#[inline]
fn parse_ts_c(f: &[u8], cache: &mut TsCache) -> Option<i64> {
    let n = f.len();
    if n != 20 && n != 25 {
        return parse_f3_fast(f);
    }
    // n >= 20: unchecked 8-byte loads are in bounds
    let p = f.as_ptr();
    let (a, b, t) = unsafe {
        (
            (p as *const u64).read_unaligned(),         // "YYYY-MM-"
            (p.add(8) as *const u64).read_unaligned(),  // "DDTHH:MM"
            (p.add(12) as *const u64).read_unaligned(), // "HH:MM:SS" (bytes 12..20)
        )
    };
    // separators '-' at 4,7; 'T' at 10; ':' at 13,16
    if a & 0xFF00_00FF_0000_0000 != 0x2D00_002D_0000_0000
        || b & 0x0000_FF00_00FF_0000 != 0x0000_3A00_0054_0000
        || (t >> 32) & 0xFF != 0x3A
    {
        return parse_f3_fast(f); // e.g. an all-digit F3 of length 20 or 25
    }
    // digits: a bytes 0-3,5,6; b bytes 0,1,3,4,6,7; t bytes 5,6 (= f[17], f[18])
    if !digits_at(a, 0x00FF_FF00_FFFF_FFFF)
        || !digits_at(b, 0xFFFF_00FF_FF00_FFFF)
        || !digits_at(t, 0x00FF_FF00_0000_0000)
    {
        return None;
    }
    // non-digit bytes are replaced by '0' first so the subtraction never borrows across bytes
    let z = |x: u64, m: u64| {
        ((x & m) | (0x3030_3030_3030_3030 & !m)).wrapping_sub(0x3030_3030_3030_3030)
    };
    let da = z(a, 0x00FF_FF00_FFFF_FFFF);
    let db = z(b, 0xFFFF_00FF_FF00_FFFF);
    let dt = z(t, 0x00FF_FF00_0000_0000);
    let g = |x: u64, i: u32| ((x >> (8 * i)) & 0xff) as i64;
    let y = g(da, 0) * 1000 + g(da, 1) * 100 + g(da, 2) * 10 + g(da, 3);
    let mo = g(da, 5) * 10 + g(da, 6);
    let d = g(db, 0) * 10 + g(db, 1);
    let h = g(db, 3) * 10 + g(db, 4);
    let mi = g(db, 6) * 10 + g(db, 7);
    let sec = g(dt, 5) * 10 + g(dt, 6);
    let off_ms = if n == 20 {
        if f[19] != b'Z' {
            return None;
        }
        0
    } else {
        // f[19] sign, f[20..22] HH, f[22] ':', f[23..25] MM
        let o = unsafe { (p.add(17) as *const u64).read_unaligned() }; // "SS+HH:MM"
        if (o >> 40) & 0xFF != 0x3A || !digits_at(o, 0xFFFF_00FF_FF00_0000) {
            return None;
        }
        let sign = f[19];
        if sign != b'+' && sign != b'-' {
            return None;
        }
        let dz = z(o, 0xFFFF_00FF_FF00_0000);
        let oh = g(dz, 3) * 10 + g(dz, 4);
        let om = g(dz, 6) * 10 + g(dz, 7);
        if oh > 23 || om > 59 {
            return None;
        }
        let v = (oh * 3600 + om * 60) * 1000;
        if sign == b'+' {
            v
        } else {
            -v
        }
    };
    if h > 23
        || mi > 59
        || sec > 59
        || mo.wrapping_sub(1) as u64 > 11
        || d.wrapping_sub(1) as u64 > 30
    {
        return None;
    }
    let days = if (y - Y0) as u64 >= NY as u64 {
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
    } else {
        let v = unsafe {
            *cache
                .tab
                .get_unchecked(((y - Y0) * 372 + (mo - 1) * 31 + (d - 1)) as usize)
        };
        if v < 0 {
            return None;
        }
        v as i64
    };
    Some((days * 86_400 + h * 3600 + mi * 60 + sec) * 1000 - off_ms)
}

/// F3: ASCII digits only, leading zeros allowed; a value too big for i64 is just out of the window.
#[inline]
fn parse_f3_fast(f: &[u8]) -> Option<i64> {
    if f.is_empty() {
        return None;
    }
    let mut i = 0;
    while i < f.len() && f[i] == b'0' {
        i += 1;
    }
    let g = &f[i..];
    if g.len() > 19 {
        return if g.iter().all(|b| b.is_ascii_digit()) && f.iter().all(|b| b.is_ascii_digit()) {
            None
        } else {
            None
        };
    }
    let mut v: u64 = 0;
    let mut k = 0;
    while k + 8 <= g.len() {
        let x = ld8(g, k);
        if !digits_at(x, u64::MAX) {
            return None;
        }
        v = v * 100_000_000 + swar8(x);
        k += 8;
    }
    while k < g.len() {
        let b = g[k];
        if !b.is_ascii_digit() {
            return None;
        }
        v = v * 10 + (b - b'0') as u64;
        k += 1;
    }
    if v > i64::MAX as u64 {
        return None;
    }
    Some(v as i64)
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
/// A byte string packed into one u64: pointer (48 bits) | length (16 bits).
#[inline]
fn pack(s: S) -> u64 {
    assert!(s.len() < 65536, "field too long");
    ((s.as_ptr() as u64) << 16) | s.len() as u64
}
#[inline]
fn unpack(v: u64) -> S {
    unsafe { std::slice::from_raw_parts((v >> 16) as *const u8, (v & 0xffff) as usize) }
}

/// Concurrent hash table of every linked ID: id hash -> (packed person name, hash of that name).
/// Lookups trust the 64-bit hash (same identity assumption as persons).
pub struct LinkMap {
    mask: usize,
    slots: Vec<[AtomicU64; 4]>, // key (0 = empty), own packed id, person name, person hash
    filter: Vec<AtomicU64>,
    fbits: u32,
}
impl LinkMap {
    fn with_capacity(n: usize) -> Self {
        let cap = ((n * 5) / 4).next_power_of_two().max(16);
        let fbits = ((n * 8).next_power_of_two().max(1 << 12).trailing_zeros()).min(30);
        LinkMap {
            mask: cap - 1,
            slots: (0..cap)
                .into_par_iter()
                .map(|_| {
                    [
                        AtomicU64::new(0),
                        AtomicU64::new(0),
                        AtomicU64::new(0),
                        AtomicU64::new(0),
                    ]
                })
                .collect(),
            filter: (0..(1usize << fbits) / 64)
                .into_par_iter()
                .map(|_| AtomicU64::new(0))
                .collect(),
            fbits,
        }
    }
    #[inline]
    fn key(h: u64) -> u64 {
        h.max(1)
    }
    /// Inserts (if new) and returns the slot index.
    fn insert(&self, h: u64, own: u64) -> u32 {
        let k = Self::key(h);
        let mut pos = (k as usize) & self.mask;
        loop {
            let s = &self.slots[pos];
            match s[0].compare_exchange(0, k, Relaxed, Relaxed) {
                Ok(_) => {
                    s[1].store(own, Relaxed);
                    let f = (k >> (64 - self.fbits)) as usize;
                    self.filter[f >> 6].fetch_or(1 << (f & 63), Relaxed);
                    return pos as u32;
                }
                Err(x) if x == k => return pos as u32,
                Err(_) => pos = (pos + 1) & self.mask,
            }
        }
    }
    #[inline]
    pub fn prefetch(&self, h: u64) {
        #[cfg(target_arch = "x86_64")]
        unsafe {
            use std::arch::x86_64::{_mm_prefetch, _MM_HINT_T0};
            let k = Self::key(h);
            let f = (k >> (64 - self.fbits)) as usize;
            _mm_prefetch(self.filter.as_ptr().add(f >> 6) as *const i8, _MM_HINT_T0);
            _mm_prefetch(
                self.slots.as_ptr().add((k as usize) & self.mask) as *const i8,
                _MM_HINT_T0,
            );
        }
    }
    #[inline]
    pub fn get_hash_only(&self, h: u64) -> Option<(u64, u64)> {
        let k = Self::key(h);
        let f = (k >> (64 - self.fbits)) as usize;
        if self.filter[f >> 6].load(Relaxed) & (1 << (f & 63)) == 0 {
            return None;
        }
        let mut pos = (k as usize) & self.mask;
        loop {
            let s = &self.slots[pos];
            let x = s[0].load(Relaxed);
            if x == 0 {
                return None;
            }
            if x == k {
                return Some((s[2].load(Relaxed), s[3].load(Relaxed)));
            }
            pos = (pos + 1) & self.mask;
        }
    }
}

/// Concurrent union-find over all link lines, on the hash table's slot indices (CAS links the
/// larger root under the smaller). Every linked ID then gets its person: the byte-wise smallest
/// ID of its component, and that ID's hash.
fn build_links(dir: &Path) -> LinkMap {
    let files: Vec<S> = list_csv(dir).par_iter().map(|p| load(p)).collect();
    let chunks = make_chunks(&files);
    let pairs: Vec<(u64, u64, S, S)> = chunks
        .par_iter()
        .flat_map_iter(|c| {
            let mut v = Vec::new();
            scan::scan::<2>(c.data, c.skip_header, |f| {
                if !f[0].is_empty() && !f[1].is_empty() && f[0] != f[1] {
                    v.push((h64(f[0]), h64(f[1]), f[0], f[1]));
                }
            });
            v
        })
        .collect();
    let map = LinkMap::with_capacity(pairs.len() * 2);
    let ip: Vec<(u32, u32)> = pairs
        .par_iter()
        .map(|p| (map.insert(p.0, pack(p.2)), map.insert(p.1, pack(p.3))))
        .collect();
    let cap = map.mask + 1;
    let parent: Vec<AtomicU32> = (0..cap as u32)
        .into_par_iter()
        .map(AtomicU32::new)
        .collect();
    let find = |mut x: u32| -> u32 {
        loop {
            let p = parent[x as usize].load(Relaxed);
            if p == x {
                return x;
            }
            let g = parent[p as usize].load(Relaxed);
            if g != p {
                let _ = parent[x as usize].compare_exchange(p, g, Relaxed, Relaxed);
            }
            x = g;
        }
    };
    ip.par_iter().for_each(|&(a0, b0)| {
        let (mut a, mut b) = (a0, b0);
        loop {
            a = find(a);
            b = find(b);
            if a == b {
                return;
            }
            let (lo, hi) = if a < b { (a, b) } else { (b, a) };
            if parent[hi as usize]
                .compare_exchange(hi, lo, Relaxed, Relaxed)
                .is_ok()
            {
                return;
            }
        }
    });
    let used = |i: usize| map.slots[i][0].load(Relaxed) != 0;
    let own = |i: usize| unpack(map.slots[i][1].load(Relaxed));
    let roots: Vec<u32> = (0..cap as u32)
        .into_par_iter()
        .map(|i| if used(i as usize) { find(i) } else { i })
        .collect();
    let best: Vec<AtomicU32> = (0..cap as u32)
        .into_par_iter()
        .map(AtomicU32::new)
        .collect();
    (0..cap).into_par_iter().filter(|&i| used(i)).for_each(|i| {
        let r = roots[i] as usize;
        let mut cur = best[r].load(Relaxed);
        while own(i) < own(cur as usize) {
            match best[r].compare_exchange(cur, i as u32, Relaxed, Relaxed) {
                Ok(_) => break,
                Err(x) => cur = x,
            }
        }
    });
    (0..cap).into_par_iter().filter(|&i| used(i)).for_each(|i| {
        let b = best[roots[i] as usize].load(Relaxed) as usize;
        let s = &map.slots[i];
        s[2].store(map.slots[b][1].load(Relaxed), Relaxed);
        s[3].store(map.slots[b][0].load(Relaxed), Relaxed);
    });
    map
}

/// Product -> category rank. IDs of the canonical form `p<digits>` (no leading zero) are looked
/// up in a dense array (cache resident); anything else goes through the exact string map.
struct ProdIndex {
    dense: Vec<u8>, // 255 = not in products.csv
    other: StrMap<u16>,
    unknown: u16,
}
impl ProdIndex {
    fn new(products: &[[S; 2]], cat_idx: &FxHashMap<S, u16>, unknown: u16) -> Self {
        let mut dense: Vec<u8> = Vec::new();
        let mut rest: Vec<(S, u16)> = Vec::new();
        for r in products {
            let c = cat_idx[&r[1]];
            match Self::canon(r[0]) {
                Some(n) if n < 50_000_000 => {
                    if dense.len() <= n {
                        dense.resize(n + 1, 255);
                    }
                    dense[n] = c as u8;
                }
                _ => rest.push((r[0], c)),
            }
        }
        let m = rest.len();
        ProdIndex {
            dense,
            other: StrMap::new(rest, m),
            unknown,
        }
    }
    /// `p` followed by 1..=9 ASCII digits without a leading zero (or exactly "p0").
    #[inline(always)]
    fn canon(s: &[u8]) -> Option<usize> {
        let n = s.len();
        if n < 2 || n > 10 || s[0] != b'p' || (s[1] == b'0' && n > 2) {
            return None;
        }
        let mut v = 0usize;
        for &b in &s[1..] {
            if !b.is_ascii_digit() {
                return None;
            }
            v = v * 10 + (b - b'0') as usize;
        }
        Some(v)
    }
    #[inline(always)]
    fn get(&self, s: &[u8]) -> u16 {
        match Self::canon(s) {
            Some(n) => match self.dense.get(n) {
                Some(&c) if c != 255 => c as u16,
                _ => self.unknown,
            },
            None => self.other.get(s).unwrap_or(self.unknown),
        }
    }
}

/// Campaign -> index. Canonical `c_<digits>` IDs (no leading zero) use a dense array; anything
/// else goes through the exact string map.
struct CampIndex {
    dense: Vec<u32>, // u32::MAX = not a campaign
    other: StrMap<u32>,
}
impl CampIndex {
    fn new(campaigns: &[[S; 2]]) -> Self {
        let mut dense: Vec<u32> = Vec::new();
        let mut rest: Vec<(S, u32)> = Vec::new();
        for (i, r) in campaigns.iter().enumerate() {
            match Self::canon(r[0]) {
                Some(n) if n < 10_000_000 => {
                    if dense.len() <= n {
                        dense.resize(n + 1, u32::MAX);
                    }
                    dense[n] = i as u32;
                }
                _ => rest.push((r[0], i as u32)),
            }
        }
        let m = rest.len();
        CampIndex {
            dense,
            other: StrMap::new(rest, m),
        }
    }
    #[inline(always)]
    fn canon(s: &[u8]) -> Option<usize> {
        let n = s.len();
        if n < 3 || n > 11 || s[0] != b'c' || s[1] != b'_' || (s[2] == b'0' && n > 3) {
            return None;
        }
        let mut v = 0usize;
        for &b in &s[2..] {
            if !b.is_ascii_digit() {
                return None;
            }
            v = v * 10 + (b - b'0') as usize;
        }
        Some(v)
    }
    #[inline(always)]
    fn get(&self, s: &[u8]) -> Option<u32> {
        match Self::canon(s) {
            Some(n) => match self.dense.get(n) {
                Some(&c) if c != u32::MAX => Some(c),
                _ => None,
            },
            None => self.other.get(s),
        }
    }
}

// ---------------------------------------------------------------- records
/// One structurally valid event copy, bucketed by event_id hash for dedup.
#[derive(Clone, Copy)]
struct Ev {
    eh: u64,     // hash of event_id (identity for dedup)
    nh: u64,     // hash of the person's name (identity of the person)
    name: u64,   // packed person name
    ingest: i64, // ingest_ts in ms
    ts: u32,     // ms since window_start; u32::MAX = invalid or late copy
    pc: u32,     // price << 8 | category rank (price 0 unless a purchase)
}

/// n slots of plain data in fresh, pre-faulted memory (one mmap + populate instead of page
/// faults one by one). Never unmapped: the process exits right after writing its output.
fn populated<T: Copy + 'static>(n: usize) -> &'static mut [T] {
    let bytes = n
        .checked_mul(std::mem::size_of::<T>())
        .expect("arena size overflow")
        .max(4096);
    assert!(bytes <= isize::MAX as usize, "arena too large");
    unsafe {
        let p = libc::mmap(
            std::ptr::null_mut(),
            bytes,
            libc::PROT_READ | libc::PROT_WRITE,
            libc::MAP_PRIVATE | libc::MAP_ANONYMOUS,
            -1,
            0,
        );
        assert!(p != libc::MAP_FAILED, "mmap failed");
        libc::madvise(p, bytes, libc::MADV_HUGEPAGE);
        if config().prefault_write && libc::madvise(p, bytes, 23 /* MADV_POPULATE_WRITE */) != 0 {
            // older kernels: touch every page
            let b = p as *mut u8;
            let mut i = 0;
            while i < bytes {
                std::ptr::write_volatile(b.add(i), 0);
                i += 4096;
            }
        }
        std::slice::from_raw_parts_mut(p as *mut T, n)
    }
}

/// NB buckets carved out of one big allocation (huge-page friendly: few page faults), with a
/// per-bucket overflow Vec in case a bucket outgrows its share.
struct Buckets<T: Copy + 'static> {
    arena: &'static mut [T],
    cap: usize,
    len: Vec<u32>,
    over: Vec<Vec<T>>,
}
impl<T: Copy + 'static> Buckets<T> {
    fn new(total_est: usize) -> Self {
        let cap = total_est.div_ceil(NB) + 64;
        let arena = populated::<T>(cap * NB);
        Buckets {
            arena,
            cap,
            len: vec![0; NB],
            over: (0..NB).map(|_| Vec::new()).collect(),
        }
    }
    #[inline(always)]
    fn push(&mut self, b: usize, x: T) {
        let l = self.len[b] as usize;
        if l < self.cap {
            unsafe { *self.arena.get_unchecked_mut(b * self.cap + l) = x };
        } else {
            self.over[b].push(x);
        }
        self.len[b] += 1;
    }
    #[inline]
    fn count(&self, b: usize) -> usize {
        self.len[b] as usize
    }
    #[inline]
    fn parts(&self, b: usize) -> (&[T], &[T]) {
        let l = (self.len[b] as usize).min(self.cap);
        (&self.arena[b * self.cap..b * self.cap + l], &self.over[b])
    }
    #[inline]
    fn extend_into(&self, b: usize, v: &mut Vec<T>) {
        let (a, o) = self.parts(b);
        v.extend_from_slice(a);
        v.extend_from_slice(o);
    }
    /// Reserve once for the complete bucket, retaining the scratch allocation across buckets.
    fn gather(parts: &[Self], b: usize, v: &mut Vec<T>) {
        v.clear();
        let total: usize = parts.iter().map(|p| p.count(b)).sum();
        v.reserve(total);
        for part in parts {
            part.extend_into(b, v);
        }
    }
}

/// Per-bucket map: person hash -> dense index (open addressing, grows at load 1/2).
struct PersonTable {
    keys: Vec<u64>, // 0 = empty (hash 0 is remapped to 1)
    vals: Vec<u32>,
    mask: usize,
}
impl PersonTable {
    fn new() -> Self {
        PersonTable {
            keys: vec![0; 1024],
            vals: vec![0; 1024],
            mask: 1023,
        }
    }
    #[inline(always)]
    fn k(nh: u64) -> u64 {
        nh.max(1)
    }
    /// Assigns each event its person's index; returns the number of persons.
    fn build(&mut self, evs: &[PE], out: &mut Vec<u32>) -> usize {
        let want = (evs.len() / 8).max(512).next_power_of_two() * 2;
        if self.keys.len() < want {
            self.keys.resize(want, 0);
            self.vals.resize(want, 0);
            self.mask = want - 1;
        }
        self.keys.fill(0);
        out.clear();
        out.reserve(evs.len());
        let mut n = 0usize;
        for e in evs {
            if 2 * (n + 1) > self.keys.len() {
                self.grow();
            }
            let k = Self::k(e.nh);
            let mut p = (k as usize) & self.mask;
            loop {
                let x = self.keys[p];
                if x == k {
                    out.push(self.vals[p]);
                    break;
                }
                if x == 0 {
                    self.keys[p] = k;
                    self.vals[p] = n as u32;
                    out.push(n as u32);
                    n += 1;
                    break;
                }
                p = (p + 1) & self.mask;
            }
        }
        n
    }
    fn grow(&mut self) {
        let (ok, ov) = (
            std::mem::take(&mut self.keys),
            std::mem::take(&mut self.vals),
        );
        let cap = ok.len() * 2;
        self.keys = vec![0; cap];
        self.vals = vec![0; cap];
        self.mask = cap - 1;
        for (k, v) in ok.into_iter().zip(ov) {
            if k != 0 {
                let mut p = (k as usize) & self.mask;
                while self.keys[p] != 0 {
                    p = (p + 1) & self.mask;
                }
                self.keys[p] = k;
                self.vals[p] = v;
            }
        }
    }
    #[inline(always)]
    fn get(&self, nh: u64) -> u32 {
        let k = Self::k(nh);
        let mut p = (k as usize) & self.mask;
        loop {
            let x = self.keys[p];
            if x == k {
                return self.vals[p];
            }
            if x == 0 {
                return u32::MAX;
            }
            p = (p + 1) & self.mask;
        }
    }
}

/// A surviving event, bucketed by person.
#[derive(Clone, Copy)]
struct PE {
    nh: u64,
    name: u64,
    ts: u32,
    pc: u32,
}

/// A valid click, bucketed by person.
#[derive(Clone, Copy)]
struct PC {
    nh: u64,
    cid: u64, // packed click_id
    ts: u32,  // ms since window_start - 7 days
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
    let ncpu: usize = std::env::var("NCPU")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(0);
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
    let cws = ws - WEEK; // click window start
    assert!(we - cws < u32::MAX as i64, "window too long");

    // links, small tables and mmaps in parallel
    let (links, ((products, campaigns), (ev_files, ck_files))) = rayon::join(
        || build_links(&data.join("links")),
        || {
            rayon::join(
                || {
                    (
                        read_small::<2>(&data.join("products.csv")),
                        read_small::<2>(&data.join("campaigns.csv")),
                    )
                },
                || {
                    (
                        list_csv(&data.join("events"))
                            .par_iter()
                            .map(|p| load(p))
                            .collect::<Vec<S>>(),
                        list_csv(&data.join("clicks"))
                            .par_iter()
                            .map(|p| load(p))
                            .collect::<Vec<S>>(),
                    )
                },
            )
        },
    );
    lap("links + small tables + mmap");

    // categories, ranked byte-wise so the smaller index wins ties
    let mut cats: Vec<S> = products.iter().map(|r| r[1]).collect();
    cats.push(b"unknown");
    cats.sort_unstable();
    cats.dedup();
    assert!(cats.len() < 255, "too many categories");
    let cat_idx: FxHashMap<S, u16> = cats
        .iter()
        .enumerate()
        .map(|(i, &c)| (c, i as u16))
        .collect();
    let unknown = cat_idx[&b"unknown"[..]];
    let prod = ProdIndex::new(&products, &cat_idx, unknown);
    let ncat = cats.len();
    let camp_idx = CampIndex::new(&campaigns);
    let ncamp = campaigns.len();
    let organic = ncamp as u32;

    let nthreads = rayon::current_num_threads().max(1);
    let groups = |n: usize| -> Vec<Vec<usize>> {
        let g = (nthreads * 3).min(n.max(1));
        let mut v = vec![Vec::new(); g];
        for i in 0..n {
            let r = i / g;
            let k = if r % 2 == 0 { i % g } else { g - 1 - i % g };
            v[k].push(i);
        }
        v
    };
    let ev_chunks = make_chunks(&ev_files);
    let ck_chunks = make_chunks(&ck_files);
    let ev_groups = groups(ev_chunks.len());
    let ck_groups = groups(ck_chunks.len());
    const BATCH: usize = 32;

    // ------------------------------------------------ parse events (bucket by event_id) and clicks (bucket by person)
    let (ev_parts, ck_parts) = rayon::join(
        || {
            ev_groups
                .par_iter()
                .map(|grp| {
                    let t0 = std::time::Instant::now();
                    let bytes: usize = grp.iter().map(|&i| ev_chunks[i].data.len()).sum();
                    let mut acc: Buckets<Ev> = Buckets::new(bytes / 70);
                    let mut c1 = TsCache::new();
                    let mut pend: [(Ev, u64); BATCH] = [(
                        Ev {
                            eh: 0,
                            nh: 0,
                            name: 0,
                            ingest: 0,
                            ts: 0,
                            pc: 0,
                        },
                        0,
                    ); BATCH];
                    let mut np = 0usize;
                    let flush = |pend: &[(Ev, u64)], acc: &mut Buckets<Ev>| {
                        for &(mut e, uh) in pend {
                            match links.get_hash_only(uh) {
                                Some((name, nh)) => {
                                    e.name = name;
                                    e.nh = nh;
                                }
                                None => e.nh = uh,
                            }
                            acc.push(bucket(e.eh), e);
                        }
                    };
                    for &ci in grp {
                        let c = &ev_chunks[ci];
                        scan::scan::<7>(c.data, c.skip_header, |f| {
                            if f[0].is_empty() {
                                return;
                            }
                            let ingest = parse_ts_c(f[3], &mut c1).unwrap_or(i64::MAX);
                            let mut e = Ev {
                                eh: h64(f[0]),
                                nh: 0,
                                name: 0,
                                ingest,
                                ts: u32::MAX,
                                pc: 0,
                            };
                            'v: {
                                if f[1].is_empty() || f[5].is_empty() {
                                    break 'v;
                                }
                                let ts = match parse_ts_c(f[2], &mut c1) {
                                    Some(t) if t >= ws && t < we => t,
                                    _ => break 'v,
                                };
                                let t = f[4];
                                let price = if t.len() == 4 && {
                                    let w = u32::from_le_bytes([t[0], t[1], t[2], t[3]]);
                                    w == u32::from_le_bytes(*b"view")
                                        || w == u32::from_le_bytes(*b"cart")
                                } {
                                    0
                                } else if t.len() == 8
                                    && u64::from_le_bytes([
                                        t[0], t[1], t[2], t[3], t[4], t[5], t[6], t[7],
                                    ]) == u64::from_le_bytes(*b"purchase")
                                {
                                    match parse_price(f[6]) {
                                        Some(p) => p,
                                        None => break 'v,
                                    }
                                } else {
                                    break 'v;
                                };
                                if ingest - ts > DAY {
                                    break 'v; // late: dropped if this copy is the kept one
                                }
                                e.ts = (ts - ws) as u32;
                                if price > 0 {
                                    e.pc = (price << 8) | u32::from(prod.get(f[5]));
                                }
                                e.name = pack(f[1]);
                                let uh = h64(f[1]);
                                links.prefetch(uh);
                                pend[np] = (e, uh);
                                np += 1;
                                if np == BATCH {
                                    flush(&pend, &mut acc);
                                    np = 0;
                                }
                                return;
                            }
                            acc.push(bucket(e.eh), e);
                        });
                    }
                    flush(&pend[..np], &mut acc);
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
                    let mut acc: Buckets<PC> = Buckets::new(bytes / 43);
                    let mut c1 = TsCache::new();
                    let mut pend: [(PC, u64); BATCH] = [(
                        PC {
                            nh: 0,
                            cid: 0,
                            ts: 0,
                            camp: 0,
                        },
                        0,
                    ); BATCH];
                    let mut np = 0usize;
                    let flush = |pend: &[(PC, u64)], acc: &mut Buckets<PC>| {
                        for &(mut c, uh) in pend {
                            c.nh = match links.get_hash_only(uh) {
                                Some((_, nh)) => nh,
                                None => uh,
                            };
                            acc.push(bucket(c.nh), c);
                        }
                    };
                    for &ci in grp {
                        let c = &ck_chunks[ci];
                        scan::scan::<4>(c.data, c.skip_header, |f| {
                            if f[0].is_empty() || f[1].is_empty() {
                                return;
                            }
                            let ts = match parse_ts_c(f[3], &mut c1) {
                                Some(t) if t >= cws && t < we => t,
                                _ => return,
                            };
                            let camp = match camp_idx.get(f[2]) {
                                Some(c) => c,
                                None => return,
                            };
                            let uh = h64(f[1]);
                            links.prefetch(uh);
                            pend[np] = (
                                PC {
                                    nh: 0,
                                    cid: pack(f[0]),
                                    ts: (ts - cws) as u32,
                                    camp,
                                },
                                uh,
                            );
                            np += 1;
                            if np == BATCH {
                                flush(&pend, &mut acc);
                                np = 0;
                            }
                        });
                    }
                    flush(&pend[..np], &mut acc);
                    tick(1, t0);
                    acc
                })
                .collect::<Vec<_>>()
        },
    );
    lap("parse events + clicks");

    // ------------------------------------------------ dedup per event bucket -> person buckets
    let bgroups = bucket_groups(nthreads, config().group_multiplier);
    let pe_parts: Vec<Buckets<PE>> = bgroups
        .par_iter()
        .map(|bs| {
            let mine: usize = bs
                .iter()
                .map(|&b| ev_parts.iter().map(|p| p.count(b)).sum::<usize>())
                .sum();
            let mut acc: Buckets<PE> = Buckets::new(mine + mine / 8);
            let mut all: Vec<Ev> = Vec::new();
            let mut table: Vec<u32> = Vec::new();
            let mut win: Vec<bool> = Vec::new();
            for &b in bs {
                let t = std::time::Instant::now();
                Buckets::gather(&ev_parts, b, &mut all);
                let t = tick(2, t);
                let cap = (all.len() * 2).next_power_of_two().max(16);
                table.clear();
                table.resize(cap, u32::MAX);
                let mask = cap - 1;
                for (i, e) in all.iter().enumerate() {
                    let mut pos = (e.eh as usize) & mask;
                    loop {
                        let k = table[pos];
                        if k == u32::MAX {
                            table[pos] = i as u32;
                            break;
                        }
                        let o = &all[k as usize];
                        if o.eh == e.eh {
                            if e.ingest < o.ingest {
                                table[pos] = i as u32;
                            }
                            break;
                        }
                        pos = (pos + 1) & mask;
                    }
                }
                let t = tick(3, t);
                win.clear();
                win.resize(all.len(), false);
                for &k in &table {
                    if k != u32::MAX {
                        win[k as usize] = true;
                    }
                }
                for (e, &w) in all.iter().zip(win.iter()) {
                    if w && e.ts != u32::MAX {
                        acc.push(
                            bucket(e.nh),
                            PE {
                                nh: e.nh,
                                name: e.name,
                                ts: e.ts,
                                pc: e.pc,
                            },
                        );
                    }
                }
                tick(4, t);
            }
            acc
        })
        .collect();
    std::mem::forget(ev_parts);
    lap("dedup");

    // ------------------------------------------------ per person bucket: sessions, credit, assists
    let width = ncamp + 1;
    let ud = out.join("user_daily");
    let cd = out.join("campaign_daily");
    std::fs::create_dir_all(&ud).unwrap();
    std::fs::create_dir_all(&cd).unwrap();
    // each task writes its own user_daily part as soon as it is done (no barrier before writing)
    let results: Vec<(Vec<UserRow>, Vec<[u64; 4]>)> = bgroups
        .par_iter()
        .enumerate()
        .map(|(g, bs)| {
            let mut camp = vec![[0u64; 4]; width * ndays];
            let mut wcnt = vec![0u32; width];
            let mut wpos = vec![0u32; width];
            let mut active: Vec<u32> = Vec::new();
            let mut rows: Vec<UserRow> = Vec::new();
            let mut catsum = vec![0u64; ncat];
            let mut touched: Vec<u16> = Vec::new();
            let mut evs: Vec<PE> = Vec::new();
            let mut cks: Vec<PC> = Vec::new();
            let mut tmp: Vec<PE> = Vec::new();
            let mut ctmp: Vec<PC> = Vec::new();
            let mut ptab = PersonTable::new();
            let mut pidx: Vec<u32> = Vec::new();
            let mut cidx: Vec<u32> = Vec::new();
            let mut pcount: Vec<u32> = Vec::new();
            let mut ccount: Vec<u32> = Vec::new();
            let mut pos: Vec<u32> = Vec::new();
            for &b in bs {
                let t = std::time::Instant::now();
                Buckets::gather(&pe_parts, b, &mut evs);
                Buckets::gather(&ck_parts, b, &mut cks);
                let t = tick(5, t);
                // group by person with a counting sort on a dense per-bucket person index,
                // then order each person's events by ts and clicks by (ts, click_id)
                let np = ptab.build(&evs, &mut pidx);
                pcount.clear();
                pcount.resize(np + 1, 0);
                for &x in &pidx {
                    pcount[x as usize + 1] += 1;
                }
                for x in 0..np {
                    pcount[x + 1] += pcount[x];
                }
                tmp.clear();
                tmp.reserve(evs.len());
                pos.clear();
                pos.extend_from_slice(&pcount[..np]);
                for (e, &x) in evs.iter().zip(pidx.iter()) {
                    let k = &mut pos[x as usize];
                    tmp.spare_capacity_mut()[*k as usize].write(*e);
                    *k += 1;
                }
                // The prefix sums scatter every event into a unique initialized slot.
                unsafe { tmp.set_len(evs.len()) };
                std::mem::swap(&mut evs, &mut tmp);
                for x in 0..np {
                    let r = &mut evs[pcount[x] as usize..pcount[x + 1] as usize];
                    if r.len() > 1 {
                        r.sort_unstable_by_key(|e| e.ts);
                    }
                }
                // clicks of persons without surviving events can never credit anything: skip them
                cidx.clear();
                cidx.reserve(cks.len());
                ccount.clear();
                ccount.resize(np + 1, 0);
                for c in &cks {
                    let x = ptab.get(c.nh);
                    cidx.push(x);
                    if x != u32::MAX {
                        ccount[x as usize + 1] += 1;
                    }
                }
                for x in 0..np {
                    ccount[x + 1] += ccount[x];
                }
                ctmp.clear();
                ctmp.reserve(ccount[np] as usize);
                pos.clear();
                pos.extend_from_slice(&ccount[..np]);
                for (c, &x) in cks.iter().zip(cidx.iter()) {
                    if x != u32::MAX {
                        let k = &mut pos[x as usize];
                        ctmp.spare_capacity_mut()[*k as usize].write(*c);
                        *k += 1;
                    }
                }
                // Only clicks with a person index occupy this initialized prefix.
                unsafe { ctmp.set_len(ccount[np] as usize) };
                std::mem::swap(&mut cks, &mut ctmp);
                for x in 0..np {
                    let r = &mut cks[ccount[x] as usize..ccount[x + 1] as usize];
                    if r.len() > 1 {
                        r.sort_unstable_by(|a, b| {
                            a.ts.cmp(&b.ts)
                                .then_with(|| unpack(a.cid).cmp(unpack(b.cid)))
                        });
                    }
                }
                let t = tick(6, t);
                for x in 0..np {
                    let (i, j) = (pcount[x] as usize, pcount[x + 1] as usize);
                    let name = unpack(evs[i].name);
                    let pc = &cks[ccount[x] as usize..ccount[x + 1] as usize];
                    let (mut lo, mut hi) = (0usize, 0usize); // clicks with s-WEEK <= ts <= s
                    let mut cur: Option<UserRow> = None;
                    let mut k = i;
                    while k < j {
                        // one session; times relative to window_start
                        let start = evs[k].ts as i64;
                        let day = (ws + start).div_euclid(DAY);
                        let di = (day - day0) as usize;
                        let d32 = day as i32;
                        if cur.as_ref().map(|r| r.day != d32).unwrap_or(false) {
                            let mut r = cur.take().unwrap();
                            r.top = top_of(&mut catsum, &mut touched);
                            rows.push(r);
                        }
                        let mut last = start;
                        let mut n_ev = 0u64;
                        let mut rev = 0u64;
                        let mut m = k;
                        while m < j && evs[m].ts as i64 - last <= GAP {
                            let e = &evs[m];
                            last = e.ts as i64;
                            n_ev += 1;
                            let price = (e.pc >> 8) as u64;
                            if price > 0 {
                                let c = (e.pc & 0xff) as usize;
                                rev += price;
                                if catsum[c] == 0 {
                                    touched.push(c as u16);
                                }
                                catsum[c] += price;
                            }
                            m += 1;
                        }
                        // attribution: click ts relative to window_start is c.ts - WEEK.
                        // [lo, hi) = clicks with s-WEEK <= ts <= s; `active` = their distinct campaigns.
                        let s_c = start + WEEK; // session start in click time base
                        while hi < pc.len() && (pc[hi].ts as i64) <= s_c {
                            let c = pc[hi].camp as usize;
                            if wcnt[c] == 0 {
                                wpos[c] = active.len() as u32;
                                active.push(c as u32);
                            }
                            wcnt[c] += 1;
                            hi += 1;
                        }
                        while lo < hi && (pc[lo].ts as i64) < s_c - WEEK {
                            let c = pc[lo].camp as usize;
                            wcnt[c] -= 1;
                            if wcnt[c] == 0 {
                                let p = wpos[c] as usize;
                                let last = active.pop().unwrap();
                                if p < active.len() {
                                    active[p] = last;
                                    wpos[last as usize] = p as u32;
                                }
                            }
                            lo += 1;
                        }
                        let credited = if lo < hi {
                            let t = pc[hi - 1].ts;
                            let mut q = hi - 1;
                            while q > lo && pc[q - 1].ts == t {
                                q -= 1;
                            }
                            for &c in &active {
                                camp[c as usize * ndays + di][3] += 1;
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
                        match &mut cur {
                            Some(r) => {
                                r.sessions += 1;
                                r.events += n_ev;
                                r.revenue += rev;
                            }
                            None => {
                                cur = Some(UserRow {
                                    name,
                                    day: d32,
                                    sessions: 1,
                                    events: n_ev,
                                    revenue: rev,
                                    top: None,
                                });
                            }
                        }
                        k = m;
                    }
                    if let Some(mut r) = cur.take() {
                        r.top = top_of(&mut catsum, &mut touched);
                        rows.push(r);
                    }
                    for &c in &active {
                        wcnt[c as usize] = 0;
                    }
                    active.clear();
                }
                tick(7, t);
            }
            let done = (rows, camp);
            write_users(
                &ud.join(format!("part-{g:05}.parquet")),
                std::slice::from_ref(&done),
                &cats,
            );
            (Vec::new(), done.1)
        })
        .collect();
    std::mem::forget(pe_parts);
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
    let cats = Arc::new(cats);
    let per = results.len().div_ceil(8).max(1);
    rayon::join(
        || {
            let _ = per; // user_daily parts were written by the session tasks
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
    // all output files are closed and flushed; skip freeing gigabytes of memory one by one
    unsafe { libc::_exit(0) };
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
    let mut w = ArrowWriter::try_new(
        std::io::BufWriter::with_capacity(1 << 20, f),
        schema,
        Some(props()),
    )
    .unwrap();
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

fn write_campaigns(
    path: &Path,
    cid: Vec<String>,
    ch: Vec<String>,
    day: Vec<i32>,
    cols: [Vec<i64>; 4],
) {
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
            slots: vec![
                Slot {
                    h: 0,
                    len: 0,
                    used: false,
                    inl: [0; 16],
                    ptr: std::ptr::null(),
                    val: V::default()
                };
                cap
            ],
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
                *s = Slot {
                    h,
                    len: key.len() as u32,
                    used: true,
                    inl,
                    ptr: key.as_ptr(),
                    val,
                };
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
                let eq = if key.len() <= 16 {
                    &s.inl[..key.len()] == key
                } else {
                    s.key() == key
                };
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
            _mm_prefetch(
                self.slots.as_ptr().add((h as usize) & self.mask) as *const i8,
                _MM_HINT_T0,
            );
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn person_table_reuses_grown_storage_and_resets_indices() {
        let mut table = PersonTable::new();
        let mut indices = Vec::new();
        let events: Vec<_> = (2..12_002)
            .map(|nh| PE {
                nh,
                name: 0,
                ts: 0,
                pc: 0,
            })
            .collect();
        assert_eq!(table.build(&events, &mut indices), events.len());
        let capacity = table.keys.len();
        let keys = table.keys.as_ptr();
        let vals = table.vals.as_ptr();
        let small = [events[3], events[7], events[3]];
        assert_eq!(table.build(&small, &mut indices), 2);
        assert_eq!(indices, [0, 1, 0]);
        assert_eq!(table.get(events[3].nh), 0);
        assert_eq!(table.get(events[7].nh), 1);
        assert_eq!(table.get(events[100].nh), u32::MAX);
        assert_eq!(table.keys.len(), capacity);
        assert_eq!(table.keys.as_ptr(), keys);
        assert_eq!(table.vals.as_ptr(), vals);
        assert_eq!(table.build(&[], &mut indices), 0);
        assert!(indices.is_empty());
        assert_eq!(table.get(events[3].nh), u32::MAX);
    }

    #[test]
    fn groups_cover_every_bucket_once_and_remain_balanced() {
        for threads in [0, 1, 2, 8, 1024, usize::MAX] {
            for multiplier in [1, 2, 4] {
                let groups = bucket_groups(threads, multiplier);
                assert!(groups.iter().all(|group| !group.is_empty()));
                let smallest = groups.iter().map(Vec::len).min().unwrap();
                let largest = groups.iter().map(Vec::len).max().unwrap();
                assert!(largest - smallest <= 1);
                let mut buckets: Vec<_> = groups.into_iter().flatten().collect();
                buckets.sort_unstable();
                assert_eq!(buckets, (0..NB).collect::<Vec<_>>());
            }
        }
    }

    #[test]
    fn gather_preserves_part_and_overflow_order_and_reuses_capacity() {
        let mut first = Buckets::<u64>::new(0);
        let mut second = Buckets::<u64>::new(0);
        let n = first.cap + 3;
        for value in 0..n as u64 {
            first.push(3, value);
        }
        second.push(3, 1000);
        second.push(3, 1001);
        second.push(4, 2000);
        let parts = [first, second];
        let mut scratch = Vec::new();
        Buckets::gather(&parts, 3, &mut scratch);
        assert_eq!(
            scratch,
            (0..n as u64).chain([1000, 1001]).collect::<Vec<_>>()
        );
        let storage = scratch.as_ptr();
        Buckets::gather(&parts, 4, &mut scratch);
        assert_eq!(scratch, [2000]);
        assert_eq!(scratch.as_ptr(), storage);
        Buckets::gather(&parts, 5, &mut scratch);
        assert!(scratch.is_empty());
    }
}
