#!/usr/bin/env python3
"""Slow, literal Python reading of rules.md, used only to cross-check the Rust pipeline on extra seeds.
Usage: reference.py DATA_DIR OUT_DIR   (writes user_daily.csv and campaign_daily.csv)"""
import calendar, csv, datetime as dt, glob, json, os, re, sys
from collections import defaultdict

DATA, OUT = sys.argv[1], sys.argv[2]
DAY, GAP, WEEK = 86_400_000, 1_800_000, 7 * 86_400_000

def lines(path):
    with open(path, 'rb') as f:
        raw = f.read()
    parts = raw.split(b'\n')
    if raw.endswith(b'\n'):
        parts.pop()
    return parts[1:]  # header

def fields(line, n):
    if line == b'':
        return None
    fs = line.split(b',')
    if len(fs) != n:
        return None
    out = []
    for f in fs:
        if b'"' in f:
            if len(f) >= 2 and f[:1] == b'"' and f[-1:] == b'"' and b'"' not in f[1:-1]:
                f = f[1:-1]
            else:
                return None
        out.append(f)
    return out

def rows(pattern, n):
    for p in sorted(glob.glob(os.path.join(DATA, pattern))):
        for l in lines(p):
            r = fields(l, n)
            if r is not None:
                yield r

ASCII = set(b'0123456789')
F1 = re.compile(rb'^(\d{4})-(\d{2})-(\d{2})T(\d{2}):(\d{2}):(\d{2})(Z|[+-]\d{2}:\d{2})$')

def ts(b):
    if b and all(c in ASCII for c in b):
        v = int(b)
        return v if v < 2**63 else None
    m = F1.match(b)
    if not m or not all(c in ASCII for c in b if c not in b'-T:Z+'):
        return None
    y, mo, d, h, mi, s = (int(x) for x in m.groups()[:6])
    if not (1 <= mo <= 12) or h > 23 or mi > 59 or s > 59 or d < 1 or d > calendar.monthrange(y, mo)[1] if 1 <= mo <= 12 else True:
        return None
    z = m.group(7)
    off = 0
    if z != b'Z':
        oh, om = int(z[1:3]), int(z[4:6])
        if oh > 23 or om > 59:
            return None
        off = (oh * 3600 + om * 60) * (1 if z[:1] == b'+' else -1)
    epoch = calendar.timegm((y, mo, d, h, mi, s)) - off
    return epoch * 1000

man = json.load(open(os.path.join(DATA, 'manifest.json')))
WS, WE = ts(man['window_start'].encode()), ts(man['window_end'].encode())

# links
parent = {}
def find(x):
    while parent[x] != x:
        parent[x] = parent[parent[x]]
        x = parent[x]
    return x
for a, b in rows('links/*.csv', 2):
    if not a or not b:
        continue
    for x in (a, b):
        parent.setdefault(x, x)
    ra, rb = find(a), find(b)
    if ra != rb:
        parent[ra] = rb
comp = defaultdict(list)
for x in parent:
    comp[find(x)].append(x)
name = {}
for members in comp.values():
    n = min(members)
    for x in members:
        name[x] = n
person = lambda u: name.get(u, u)

products = {r[0]: r[1] for r in rows('products.csv', 2)}
campaigns = {r[0]: r[1] for r in rows('campaigns.csv', 2)}

# events
kept = {}
for r in rows('events/*.csv', 7):
    eid, user, t, ing, et, prod, price = r
    if not eid:
        continue
    ing = ts(ing)
    if eid not in kept or ing < kept[eid][0]:
        kept[eid] = (ing, r)
evs = defaultdict(list)
for eid, (ing, r) in kept.items():
    _, user, t, _, et, prod, price = r
    t = ts(t)
    if not user or t is None or not (WS <= t < WE) or et not in (b'view', b'cart', b'purchase') or not prod:
        continue
    p = 0
    if et == b'purchase':
        if not price or not all(c in ASCII for c in price) or not (1 <= int(price) <= 10_000_000):
            continue
        p = int(price)
    if ing - t > DAY:
        continue
    evs[person(user)].append((t, p, products.get(prod, b'unknown')))

clicks = defaultdict(list)
for cid, user, camp, t in rows('clicks/*.csv', 4):
    if not cid or not user:
        continue
    t = ts(t)
    if t is None or not (WS - WEEK <= t < WE) or camp not in campaigns:
        continue
    clicks[person(user)].append((t, cid, camp))

ud = defaultdict(lambda: [0, 0, 0, defaultdict(int)])
cd = defaultdict(lambda: [0, 0, 0, 0])
for who, es in evs.items():
    es.sort(key=lambda e: e[0])
    sessions, cur, last = [], None, None
    for e in es:
        if cur is None or e[0] - last > GAP:
            cur = []
            sessions.append(cur)
        cur.append(e)
        last = e[0]
    cs = clicks.get(who, [])
    for s in sessions:
        start = s[0][0]
        day = dt.date(1970, 1, 1) + dt.timedelta(days=start // DAY)
        rev = sum(e[1] for e in s)
        u = ud[(who, day)]
        u[0] += 1; u[1] += len(s); u[2] += rev
        for e in s:
            if e[1]:
                u[3][e[2]] += e[1]
        q = [c for c in cs if start - WEEK <= c[0] <= start]
        if q:
            best = sorted(q, key=lambda c: (-c[0], c[1]))[0]
            camp = best[2]
            for a in {c[2] for c in q}:
                cd[(a, day)][3] += 1
        else:
            camp = b'organic'
        c = cd[(camp, day)]
        c[0] += 1; c[1] += 1 if rev else 0; c[2] += rev

os.makedirs(OUT, exist_ok=True)
dec = lambda b: b.decode('utf-8', 'replace')
with open(os.path.join(OUT, 'user_daily.csv'), 'w', newline='') as f:
    w = csv.writer(f, lineterminator='\n')
    w.writerow(['user_id', 'day', 'sessions', 'events', 'revenue_cents', 'top_category'])
    for (who, day), (s, e, r, cats) in ud.items():
        top = min(cats.items(), key=lambda kv: (-kv[1], kv[0]))[0] if cats else None
        w.writerow([dec(who), day.isoformat(), s, e, r, dec(top) if top else ''])
with open(os.path.join(OUT, 'campaign_daily.csv'), 'w', newline='') as f:
    w = csv.writer(f, lineterminator='\n')
    w.writerow(['campaign_id', 'channel', 'day', 'sessions', 'purchasing_sessions', 'revenue_cents', 'assisted_sessions'])
    for (camp, day), v in cd.items():
        ch = b'organic' if camp == b'organic' else campaigns[camp]
        w.writerow([dec(camp), dec(ch), day.isoformat(), *v])
