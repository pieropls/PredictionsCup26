"""Second pass on the cached 2024 1-min Polymarket tapes: remove empty-book 0.50 artifacts, classify >=10c 1-min steps,
time-to-call metrics on the cleaned winner series, false-side checks on the cleaned loser series.

Usage: python3 -I pm24_analyze2.py <raw_dir> <out_dir>    (no network; reads files cached by pm24_fetch.py)
"""
import json, os, sys, datetime as dt
from collections import Counter, defaultdict

RAW, OUT = sys.argv[1], sys.argv[2]
os.makedirs(OUT, exist_ok=True)
HIST = os.path.join(RAW, "hist24")
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from pm_common import PC, DISTRICT_PC, ED24, close_ts, et, parse_closed, q  # noqa: E402

ART_LO, ART_HI = 0.475, 0.525      # an emptied book prints a midpoint of ~0.50


def load(tok):
    h = json.load(open(os.path.join(HIST, f"{tok[:20]}_f1.json"))).get("history", [])
    tail = []
    for k in range(3):
        p = os.path.join(HIST, f"{tok[:20]}_tail10_{k}.json")
        if os.path.exists(p):
            tail += json.load(open(p)).get("history", [])
    s = [(x["t"], x["p"]) for x in h]
    last = s[-1][0] if s else 0
    return s, [(x["t"], x["p"]) for x in tail if x["t"] > last]


def clean(s):
    """Drop runs of ~0.50 prints whose neighbours on both sides are outside [0.30, 0.70] (empty-book midpoints)."""
    n, bad, i = len(s), [False] * len(s), 0
    while i < n:
        if ART_LO <= s[i][1] <= ART_HI:
            j = i
            while j + 1 < n and ART_LO <= s[j + 1][1] <= ART_HI:
                j += 1
            prev = s[i - 1][1] if i > 0 else None
            nxt = s[j + 1][1] if j + 1 < n else None
            far = lambda v: v is None or v < 0.30 or v > 0.70
            if far(prev) and far(nxt) and not (prev is None and nxt is None):
                for k in range(i, j + 1):
                    bad[k] = True
            i = j + 1
        else:
            i += 1
    return [x for x, b in zip(s, bad) if not b], sum(bad)


def at(s, ts):
    best = None
    for t, p in s:
        if t <= ts:
            best = p
        else:
            break
    return best


def first(s, th, after, held=False):
    for i, (t, p) in enumerate(s):
        if t >= after and p >= th:
            if not held:
                return t
            if i + 1 < len(s) and s[i + 1][1] >= th and s[i + 1][0] - t <= 90:
                return t
    return None


races = json.load(open(os.path.join(RAW, "races24.json")))
NIGHT0, NIGHT1 = ED24 + 18 * 3600, ED24 + 36 * 3600
BASE0, BASE1 = ED24 + 7 * 3600, ED24 + 17 * 3600
rows, moves, art_total, art_night = [], [], 0, 0
steps = defaultdict(list)
for r in races:
    w_raw, w_tail = load(r["win"]["tok"])
    l_raw, _ = load(r["lose"]["tok"])
    w, nb = clean(w_raw)
    l, nbl = clean(l_raw)
    art_total += nb
    art_night += sum(1 for (t, p) in w_raw if NIGHT0 <= t < NIGHT1 and ART_LO <= p <= ART_HI) - sum(1 for (t, p) in w if NIGHT0 <= t < NIGHT1 and ART_LO <= p <= ART_HI)
    if r["lose"].get("complement"):
        l = [(t, round(1 - p, 4)) for t, p in w]          # binary market: the loser is the exact complement
    pre = sorted(p for t, p in w if ED24 + 12 * 3600 <= t < close_ts(ED24, r["state"], r["district"], r["kind"] == "Control")[0])
    p_pre = pre[len(pre) // 2] if pre else None
    lm = dict(l)
    wa = w + w_tail
    cf, cl = close_ts(ED24, r["state"], r["district"], r["kind"] == "Control")
    p_first, p_close = at(w, cf), at(w, cl)
    pre = sorted(p for t, p in w if ED24 + 12 * 3600 <= t < cf)       # robust pre-close level: median of 12:00 ET .. first close
    p_pre = pre[len(pre) // 2] if pre else p_close
    t95, t95h, t98, t99 = first(wa, 0.95, cf - 6 * 3600), first(wa, 0.95, cf - 6 * 3600, True), first(wa, 0.98, cf - 6 * 3600), first(wa, 0.99, cf - 6 * 3600)
    t80 = first(wa, 0.80, cl) if (p_close is not None and p_close < 0.80) else None
    # sustained: first >= 0.95 after which the cleaned winner never prints < 0.90 again
    t95s = None
    for i, (t, p) in enumerate(wa):
        if t >= cf - 6 * 3600 and p >= 0.95 and all(pp >= 0.90 for _, pp in wa[i:]):
            t95s = t
            break
    after_call_min = min([p for t, p in wa if t95h and t > t95h] or [None]) if t95h else None
    lpost = [(t, p) for t, p in l if t >= cf]
    lmax = max([p for _, p in lpost] or [0])
    l95h = first(lpost, 0.95, cf, True)
    l95 = first(lpost, 0.95, cf)
    l98 = first(lpost, 0.98, cf)
    l90_min = sum(1 for _, p in lpost if p >= 0.90)
    # 1-min steps on the cleaned winner series
    for (t0, p0), (t1, p1) in zip(w, w[1:]):
        if t1 - t0 > 90:
            continue
        d = p1 - p0
        if NIGHT0 <= t1 < NIGHT1:
            steps[r["kind"]].append(d)
            steps["night"].append(d)
            steps["night safe" if (p_pre is not None and p_pre >= 0.95) else "night contested"].append(d)
            if abs(d) >= 0.10 - 1e-9:
                lv10 = at(w, t1 + 600)
                persistent = lv10 is not None and (lv10 - p0) * (1 if d > 0 else -1) >= 0.5 * abs(d)
                lo0, lo1 = at(l, t0 - 60), at(l, t1 + 60)
                confirm = None
                if not r["lose"].get("complement") and lo0 is not None and lo1 is not None:
                    confirm = (lo1 - lo0) * (-1 if d > 0 else 1) >= 0.5 * abs(d)
                safe_exc = p_pre is not None and p_pre >= 0.95
                cat = "safe race (thin book)" if safe_exc else ("contested, persistent" if persistent else "contested, reverting")
                moves.append(dict(race=f"{r['kind']} {r['district'] or r['state']}", kind=r["kind"], t=et(t1), frm=p0, to=p1, d=round(d, 4),
                                  lvl10=lv10, persistent=persistent, confirm=confirm, cat=cat,
                                  min_after_close=round((t1 - cl) / 60)))
        elif BASE0 <= t1 < BASE1:
            steps["baseline"].append(d)
            steps["baseline safe" if (p_pre is not None and p_pre >= 0.95) else "baseline contested"].append(d)
    res = parse_closed(r["win"].get("closed"))
    jump1 = jump5 = jump30 = None
    if t95h:
        p_at = at(wa, t95h)
        b1, b5, b30 = at(wa, t95h - 60), at(wa, t95h - 300), at(wa, t95h - 1800)
        jump1 = None if b1 is None else round(p_at - b1, 3)
        jump5 = None if b5 is None else round(p_at - b5, 3)
        jump30 = None if b30 is None else round(p_at - b30, 3)
    rows.append(dict(kind=r["kind"], state=r["state"], district=r["district"], winner=r["win"]["label"], loser=r["lose"]["label"], vol=round(r["win"]["vol"]),
                     close_first=et(cf), close_last=et(cl), p_first=p_first, p_close=p_close, p_pre=p_pre, liquid=r["win"]["vol"] >= 250_000, artifacts_removed=nb,
                     m95=None if t95 is None else round((t95 - cl) / 60), m95h=None if t95h is None else round((t95h - cl) / 60),
                     m98=None if t98 is None else round((t98 - cl) / 60), m99=None if t99 is None else round((t99 - cl) / 60),
                     m95s=None if t95s is None else round((t95s - cl) / 60),
                     m80_95=None if (t80 is None or t95h is None or t95h < t80) else round((t95h - t80) / 60),
                     m95h_99=None if (t95h is None or t99 is None or t99 < t95h) else round((t99 - t95h) / 60),
                     after_call_min=after_call_min, jump1=jump1, jump5=jump5, jump30=jump30, loser_max=lmax, loser_95=et(l95) if l95 else None, loser_95h=et(l95h) if l95h else None,
                     loser_98=et(l98) if l98 else None, loser_ge90_minutes=l90_min,
                     resolved=et(res) if res else None, resolved_h=None if res is None else round((res - cl) / 3600, 1),
                     resolved_after_99_h=None if (res is None or t99 is None) else round((res - t99) / 3600, 1)))

json.dump(rows, open(os.path.join(OUT, "races24_metrics_v2.json"), "w"), indent=1)
json.dump(moves, open(os.path.join(OUT, "moves24_v2.json"), "w"), indent=1)

print("artifact prints removed (all races, whole window):", art_total)
safe = [r for r in rows if r["p_pre"] is not None and r["p_pre"] >= 0.95]
cont = [r for r in rows if r not in safe]
print("races", len(rows), dict(Counter(r["kind"] for r in rows)), "| >=0.95 at last close:", len(safe), dict(Counter(r["kind"] for r in safe)),
      "| below:", len(cont), dict(Counter(r["kind"] for r in cont)))
for name, sel in (("contested", cont), ("contested liquid (vol>=250k)", [r for r in cont if r["liquid"]]), ("safe", safe), ("all", rows)):
    print(f"\n== {name} (n={len(sel)})")
    for k in ("m95", "m95h", "m98", "m99", "m95s", "m80_95", "m95h_99", "resolved_h", "resolved_after_99_h"):
        print(f"  {k:20}", json.dumps(q([r[k] for r in sel if r[k] is not None])))
    print("  never >=0.95 in the tape:", [f"{r['kind']} {r['district'] or r['state']}" for r in sel if r["m95h"] is None])
for kind in ("Senate", "Governor", "House", "Control"):
    sel = [r for r in cont if r["kind"] == kind]
    print(f"\n-- contested {kind} n={len(sel)} m95h", json.dumps(q([r["m95h"] for r in sel if r["m95h"] is not None])))

print("\ncontested detail:")
for r in sorted(cont, key=lambda r: (r["m95h"] if r["m95h"] is not None else 1e9)):
    print(f"  {r['kind']:8} {(r['district'] or r['state'])[:13]:13} {r['winner'][:16]:16} close {r['close_last'][6:]} liq={int(r['liquid'])} p_pre={r['p_pre']} p_close={r['p_close']} m95={r['m95']} m95h={r['m95h']} "
          f"m98={r['m98']} m99={r['m99']} m95s={r['m95s']} ramp80_95={r['m80_95']} 95to99={r['m95h_99']} minAfter={r['after_call_min']} "
          f"loserMax={r['loser_max']} l95={r['loser_95']} l98={r['loser_98']} l90min={r['loser_ge90_minutes']} res_h={r['resolved_h']}")
print("\nsafe detail:", len(safe))
for r in safe:
    print(f"  {r['kind']:8} {(r['district'] or r['state'])[:13]:13} p_first={r['p_first']} p_close={r['p_close']} m98={r['m98']} m99={r['m99']} minAfter={r['after_call_min']} loserMax={r['loser_max']} res_h={r['resolved_h']} art={r['artifacts_removed']}")


def bucket(v):
    a = [abs(x) + 1e-9 for x in v]
    return dict(n=len(a), ge2c=sum(x >= 0.02 for x in a), ge5c=sum(x >= 0.05 for x in a), ge10c=sum(x >= 0.10 for x in a),
                ge20c=sum(x >= 0.20 for x in a), ge30c=sum(x >= 0.30 for x in a), max=round(max(a), 3) if a else None)


print("\n1-min steps (cleaned):")
for k in ("Senate", "Governor", "House", "Control", "night", "night safe", "night contested", "baseline", "baseline safe", "baseline contested"):
    print(f"  {k:9}", bucket(steps[k]))
print("\n>=10c moves by category:", dict(Counter(m["cat"] for m in moves)))
print("  by kind x cat:", dict(Counter((m["kind"], m["cat"]) for m in moves)))
print("  confirmed by the other party's market (party races only):", dict(Counter((m["cat"], m["confirm"]) for m in moves)))
print("  races with >=1 contested persistent >=10c move:", len({m["race"] for m in moves if m["cat"] == "contested, persistent"}),
      "| with any >=10c move:", len({m["race"] for m in moves}))
sizes = sorted(abs(m["d"]) for m in moves if m["cat"] == "contested, persistent")
for cat in ("contested, persistent", "contested, reverting", "safe race (thin book)"):
    v = [abs(m["d"]) for m in moves if m["cat"] == cat]
    print("  ", cat, "n", len(v), "10-20c", sum(0.10 <= x < 0.20 for x in v), "20-30c", sum(0.20 <= x < 0.30 for x in v), ">=30c", sum(x >= 0.30 for x in v), "races", len({m["race"] for m in moves if m["cat"] == cat}))
print("  persistent by kind:", dict(Counter(m["kind"] for m in moves if m["cat"] == "contested, persistent")), "races:", sorted({m["race"] for m in moves if m["cat"] == "contested, persistent"}))
cr = [r for r in cont if r["jump1"] is not None]
print("  crossing into >=0.95 (contested): 1-min jump", q([r["jump1"] for r in cr]), "\n   5-min", q([r["jump5"] for r in cr if r["jump5"] is not None]), "\n   30-min", q([r["jump30"] for r in cr if r["jump30"] is not None]))
print("   crossings with a >=10c 1-min jump:", sum(1 for r in cr if r["jump1"] >= 0.10), "of", len(cr), "| >=5c in 5 min:", sum(1 for r in cr if (r["jump5"] or 0) >= 0.05))
print("  contested persistent sizes:", sizes)
print("\ncontested moves:")
for m in sorted(moves, key=lambda m: m["t"]):
    if m["cat"] != "safe-race excursion":
        print("  ", m["t"], m["race"], m["frm"], "->", m["to"], "lvl10", m["lvl10"], m["cat"], "confirm", m["confirm"], "min_after_close", m["min_after_close"])
print("\nsafe excursions:")
for m in sorted(moves, key=lambda m: m["t"]):
    if m["cat"] == "safe-race excursion":
        print("  ", m["t"], m["race"], m["frm"], "->", m["to"], "lvl10", m["lvl10"], "confirm", m["confirm"])
