"""PM state of each 2024 race at the SIG-equivalent settlement time (12:00 ET the day after, = Nov 4 2026 09:00 PT).
Usage: python3 -I pm24_settle.py <raw_dir> <out_dir>"""
import json, os, sys
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from pm_common import ED24, ED22, parse_closed, et  # noqa: E402
from pm24_analyze2_lib import load_clean  # noqa: E402
RAW, OUT = sys.argv[1], sys.argv[2]
races = json.load(open(os.path.join(RAW, "races24.json")))
m = {(r["kind"], r["state"], r["district"]): r for r in json.load(open(os.path.join(OUT, "races24_metrics_v2.json")))}
T = ED24 + 36 * 3600           # Nov 6 2024 12:00 ET
out, buckets = [], {}
for r in races:
    w = load_clean(RAW, r["win"]["tok"])
    pt = None
    for t, p in w:
        if t <= T:
            pt = p
    win_lo = min([p for t, p in w if ED24 + 33 * 3600 <= t <= T] or [None]) if w else None   # min over 09:00-12:00 ET
    res = parse_closed(r["win"].get("closed"))
    mm = m[(r["kind"], r["state"], r["district"])]
    safe = mm["p_pre"] is not None and mm["p_pre"] >= 0.95
    b = "no tape" if pt is None else ">=0.99" if pt >= 0.99 else ">=0.95" if pt >= 0.95 else "0.80-0.95" if pt >= 0.80 else "0.50-0.80" if pt >= 0.5 else "<0.50 (loser favoured)"
    buckets.setdefault((b, "safe" if safe else "contested"), []).append(f"{r['kind']} {r['district'] or r['state']}")
    out.append(dict(race=f"{r['kind']} {r['district'] or r['state']}", p_at_settle=pt, low_9_12=win_lo, resolved_before=bool(res and res <= T), safe=safe))
for k in sorted(buckets):
    print(k, len(buckets[k]), buckets[k])
print("PM-resolved before 12:00 ET Nov 6 2024:", sum(o["resolved_before"] for o in out), "of", len(out), [o["race"] for o in out if o["resolved_before"]])
json.dump(out, open(os.path.join(OUT, "settle24.json"), "w"), indent=1)
r22 = json.load(open(os.path.join(OUT, "races22_resolution.json")))
T22 = ED22 + 36 * 3600
print("2022 PM-resolved before 12:00 ET Nov 9 2022:", sum(1 for x in r22 if x["resolved_h"] is not None and 0 <= x["resolved_h"] and x["resolved"] and x["resolved"] <= "11-09 12:00 ET"), "of", sum(1 for x in r22 if (x["resolved_h"] or -1) >= 0))
