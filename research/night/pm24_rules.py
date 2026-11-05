"""Replay candidate call rules on the cleaned 2024 1-min tapes (after each race's first poll close).
Usage: python3 -I pm24_rules.py <raw_dir> <out_dir>"""
import json, os, sys
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from pm_common import ED24, close_ts  # noqa: E402
from pm24_analyze2_lib import load_clean  # noqa: E402
RAW, OUT = sys.argv[1], sys.argv[2]
races = json.load(open(os.path.join(RAW, "races24.json")))
ms = {(r["kind"], r["state"], r["district"]): r for r in json.load(open(os.path.join(OUT, "races24_metrics_v2.json")))}
END = ED24 + 8 * 86400


def fire(s, th, hold, after):
    for i, (t, p) in enumerate(s):
        if t < after or p < th:
            continue
        if hold == 0:
            return t
        k, ok = i, True
        while k + 1 < len(s) and s[k + 1][0] - t <= hold * 60:
            if s[k + 1][1] < th:
                ok = False
                break
            k += 1
        if ok and k > i:
            return t
    return None


RULES = [("any print >= 0.95", 0.95, 0), ("0.95 held 2 samples (>=1 min)", 0.95, 1), ("0.97 held 2 samples", 0.97, 1),
         ("any print >= 0.98", 0.98, 0), ("0.99 held 10 min", 0.99, 10)]
res = {name: dict(win=0, lose=[], scare90=[], scare80=[], m=[], m_cont=[]) for name, _, _ in RULES}
for r in races:
    if r["district"] == "CO-08":
        continue  # market opened Nov 9
    w = load_clean(RAW, r["win"]["tok"])
    l = [(t, round(1 - p, 4)) for t, p in w] if r["lose"].get("complement") else load_clean(RAW, r["lose"]["tok"])
    cf, cl = close_ts(ED24, r["state"], r["district"], r["kind"] == "Control")
    mm = ms[(r["kind"], r["state"], r["district"])]
    safe = mm["p_pre"] is not None and mm["p_pre"] >= 0.95
    name_r = f"{r['kind']} {r['district'] or r['state']}"
    for name, th, hold in RULES:
        tw, tl = fire(w, th, hold, cf), fire(l, th, hold, cf)
        R = res[name]
        if tl is not None and (tw is None or tl < tw):
            R["lose"].append(name_r)
        if tw is not None:
            R["win"] += 1
            R["m"].append((tw - cl) / 60)
            if not safe:
                R["m_cont"].append((tw - cl) / 60)
            later = [p for t, p in w if t > tw]
            if later and min(later) < 0.90:
                R["scare90"].append(name_r)
            if later and min(later) < 0.80:
                R["scare80"].append(name_r)


def med(v):
    v = sorted(v)
    return None if not v else round(v[len(v) // 2])


out = {}
for name, _, _ in RULES:
    R = res[name]
    out[name] = dict(fires_on_winner=R["win"], first_fire_med_min_after_last_close=med(R["m"]), contested_med_min=med(R["m_cont"]),
                     fires_on_loser_first=R["lose"], winner_later_below_090=len(R["scare90"]), winner_later_below_080=len(R["scare80"]),
                     below080_races=R["scare80"])
    print(name, json.dumps(out[name]))
json.dump(out, open(os.path.join(OUT, "rules24.json"), "w"), indent=1)
