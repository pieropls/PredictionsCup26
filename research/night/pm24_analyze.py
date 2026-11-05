"""Analyse the cached 2024 1-min Polymarket tapes (and 2022 resolution times).

Usage: python3 -I pm24_analyze.py <raw_dir> <out_dir>
Reads only cached files written by pm24_fetch.py / the gamma pagination; no network.
"""
import json, glob, os, sys, statistics as stt, datetime as dt
from collections import Counter, defaultdict

RAW, OUT = sys.argv[1], sys.argv[2]
os.makedirs(OUT, exist_ok=True)
HIST = os.path.join(RAW, "hist24")

# Poll closing times, ET (EST on all three election days: DST had ended), (first, last) in hours after midnight of election day.
# Source: 270toWin poll-closing table (statutory hours, unchanged 2022-2026) cross-checked with data/election/poll_close.json (statutes).
PC = {"Alabama": (20, 20), "Alaska": (24, 25), "Arizona": (21, 21), "Arkansas": (20.5, 20.5), "California": (23, 23), "Colorado": (21, 21),
      "Connecticut": (20, 20), "Delaware": (20, 20), "Florida": (19, 20), "Georgia": (19, 19), "Hawaii": (24, 24), "Idaho": (22, 23),
      "Illinois": (20, 20), "Indiana": (18, 19), "Iowa": (21, 21), "Kansas": (20, 21), "Kentucky": (18, 19), "Louisiana": (21, 21),
      "Maine": (20, 20), "Maryland": (20, 20), "Massachusetts": (20, 20), "Michigan": (20, 21), "Minnesota": (21, 21), "Mississippi": (20, 20),
      "Missouri": (20, 20), "Montana": (22, 22), "Nebraska": (21, 21), "Nevada": (22, 22), "New Hampshire": (19, 20), "New Jersey": (20, 20),
      "New Mexico": (21, 21), "New York": (21, 21), "North Carolina": (19.5, 19.5), "North Dakota": (20, 21), "Ohio": (19.5, 19.5),
      "Oklahoma": (20, 20), "Oregon": (22, 23), "Pennsylvania": (20, 20), "Rhode Island": (20, 20), "South Carolina": (19, 19),
      "South Dakota": (20, 21), "Tennessee": (20, 20), "Texas": (20, 21), "Utah": (22, 22), "Vermont": (19, 19), "Virginia": (19, 19),
      "Washington": (23, 23), "West Virginia": (19.5, 19.5), "Wisconsin": (21, 21), "Wyoming": (21, 21)}
DISTRICT_PC = {"FL-13": (19, 19), "OR-05": (23, 23)}   # Pinellas is ET; OR-05 has no Mountain-time county
ED24 = dt.datetime(2024, 11, 5, tzinfo=dt.timezone.utc).timestamp() + 5 * 3600   # midnight ET (EST = UTC-5)
ED22 = dt.datetime(2022, 11, 8, tzinfo=dt.timezone.utc).timestamp() + 5 * 3600


def close_ts(ed, state, district=None, control=None):
    if control:
        return ed + 19 * 3600, ed + 25 * 3600   # first wave 19:00 ET .. last state (Alaska Aleutians) 01:00 ET
    f, l = DISTRICT_PC.get(district, PC[state])
    return ed + f * 3600, ed + l * 3600


def et(ts):
    return dt.datetime.fromtimestamp(ts, dt.timezone(dt.timedelta(hours=-5))).strftime("%m-%d %H:%M ET")


def parse_closed(s):
    if not s:
        return None
    s = s.replace("+00", "+00:00") if s.endswith("+00") else s.replace("Z", "+00:00")
    return dt.datetime.fromisoformat(s).timestamp()


def series(tok):
    h = json.load(open(os.path.join(HIST, f"{tok[:20]}_f1.json"))).get("history", [])
    tail = []
    for k in range(3):
        p = os.path.join(HIST, f"{tok[:20]}_tail10_{k}.json")
        if os.path.exists(p):
            tail += json.load(open(p)).get("history", [])
    return [(x["t"], x["p"]) for x in h], [(x["t"], x["p"]) for x in tail]


def q(vals, ps=(0.1, 0.25, 0.5, 0.75, 0.9)):
    v = sorted(vals)
    if not v:
        return {}
    out = {}
    for p in ps:
        i = min(len(v) - 1, max(0, int(round(p * (len(v) - 1)))))
        out[f"p{int(p*100)}"] = v[i]
    out["n"], out["min"], out["max"], out["mean"] = len(v), v[0], v[-1], sum(v) / len(v)
    return out


races = json.load(open(os.path.join(RAW, "races24.json")))
rows, steps_all, steps_by = [], [], defaultdict(list)
NIGHT0, NIGHT1 = ED24 + 18 * 3600, ED24 + 36 * 3600          # 18:00 ET Nov 5 .. 12:00 ET Nov 6
BASE0, BASE1 = ED24 + 7 * 3600, ED24 + 17 * 3600             # 07:00-17:00 ET Nov 5: same-day pre-close baseline
big_moves = []
for r in races:
    w, wtail = series(r["win"]["tok"])
    l, _ = series(r["lose"]["tok"])
    c_first, c_last = close_ts(ED24, r["state"], r["district"], r["kind"] == "Control")
    allw = w + [x for x in wtail if x[0] > (w[-1][0] if w else 0)]

    def first_at(th, after, ser=allw):
        for t, p in ser:
            if t >= after and p >= th:
                return t
        return None

    def price_at(ts, ser=w):
        best = None
        for t, p in ser:
            if t <= ts:
                best = p
            else:
                break
        return best

    p_close = price_at(c_last)
    p_first = price_at(c_first)
    p_noon = price_at(ED24 + 12 * 3600)
    t95 = first_at(0.95, c_first - 6 * 3600)
    t98 = first_at(0.98, c_first - 6 * 3600)
    t99 = first_at(0.99, c_first - 6 * 3600)
    # "sustained" 0.95: first time after which the winner never trades below 0.90 again (within the 1-min window)
    t95s = None
    for i, (t, p) in enumerate(allw):
        if t >= c_first - 6 * 3600 and p >= 0.95 and all(pp >= 0.90 for _, pp in allw[i:]):
            t95s = t
            break
    # reversals: winner >= 0.95 then < 0.90 later
    rev = False
    if t95:
        rev = any(p < 0.90 for t, p in allw if t > t95)
    # eventual loser's peak after the first close (fake calls against the winner)
    lose_ser = l if not r["lose"].get("complement") else l
    lmax = max([p for t, p in lose_ser if t >= c_first] or [0])
    l95 = first_at(0.95, c_first, lose_ser)
    # time spent going 0.80 -> 0.95 (steepness of the call) for races below 0.80 at close
    t80 = None
    if p_close is not None and p_close < 0.80:
        t80 = first_at(0.80, c_last)
    # 1-min steps on the winner series
    nb = 0
    for (t0, p0), (t1, p1) in zip(w, w[1:]):
        if t1 - t0 > 90:
            continue
        d = p1 - p0
        if NIGHT0 <= t1 < NIGHT1:
            steps_all.append(d)
            steps_by[r["kind"]].append(d)
            if abs(d) >= 0.10:
                nb += 1
                big_moves.append(dict(race=f"{r['kind']} {r['district'] or r['state']}", t=et(t1), frm=p0, to=p1, d=round(d, 3),
                                      min_after_close=round((t1 - c_last) / 60, 1)))
        elif BASE0 <= t1 < BASE1:
            steps_by["baseline"].append(d)
    res = parse_closed(r["win"].get("closed"))
    uma = parse_closed(r["win"].get("uma_end"))
    rows.append(dict(kind=r["kind"], state=r["state"], district=r["district"], winner=r["win"]["label"], vol=round(r["win"]["vol"]),
                     close_first=et(c_first), close_last=et(c_last), p_noon=p_noon, p_first=p_first, p_close=p_close,
                     t95=t95, t98=t98, t99=t99, t95s=t95s,
                     m95=None if t95 is None else round((t95 - c_last) / 60, 1),
                     m95_first=None if t95 is None else round((t95 - c_first) / 60, 1),
                     m98=None if t98 is None else round((t98 - c_last) / 60, 1),
                     m99=None if t99 is None else round((t99 - c_last) / 60, 1),
                     m95s=None if t95s is None else round((t95s - c_last) / 60, 1),
                     m80_95=None if (t80 is None or t95 is None or t95 < t80) else round((t95 - t80) / 60, 1),
                     reversal=rev, loser_max=lmax, loser95=bool(l95), big1m=nb,
                     resolved_h=None if res is None else round((res - c_last) / 3600, 1),
                     uma_h=None if uma is None else round((uma - c_last) / 3600, 1),
                     resolved=None if res is None else et(res), n1=len(w)))

json.dump(rows, open(os.path.join(OUT, "races24_metrics.json"), "w"), indent=1)
json.dump(big_moves, open(os.path.join(OUT, "big_moves24.json"), "w"), indent=1)

# ---------- summaries ----------
def show(title, d):
    print(f"\n## {title}")
    print(json.dumps(d, indent=None, default=str))

safe = [r for r in rows if r["p_close"] is not None and r["p_close"] >= 0.95]
cont = [r for r in rows if r not in safe]
print("races", len(rows), Counter(r["kind"] for r in rows), "safe at last close (>=0.95):", len(safe), "contested:", len(cont))
for grp, sel in [("all", rows), ("contested", cont)]:
    for k in ("m95", "m98", "m99", "m95s", "m80_95", "resolved_h"):
        show(f"{grp} {k} (minutes after last poll close; resolved_h in hours)", q([r[k] for r in sel if r[k] is not None]))
print("\ncontested races detail:")
for r in sorted(cont, key=lambda r: (r["m95"] if r["m95"] is not None else 1e9)):
    print(f"  {r['kind']:8} {(r['district'] or r['state'])[:14]:14} win={r['winner'][:18]:18} close={r['close_last']} p_close={r['p_close']} "
          f"m95={r['m95']} m98={r['m98']} m99={r['m99']} m95s={r['m95s']} m80->95={r['m80_95']} rev={r['reversal']} lmax={r['loser_max']} big1m={r['big1m']} res_h={r['resolved_h']}")
print("\nsafe races detail (p_close, m95 [neg=before close], m99):")
for r in safe:
    print(f"  {r['kind']:8} {(r['district'] or r['state'])[:14]:14} p_noon={r['p_noon']} p_close={r['p_close']} m95={r['m95']} m99={r['m99']} rev={r['reversal']} res_h={r['resolved_h']}")

def bucket(steps):
    a = [abs(x) for x in steps]
    n = len(a)
    return dict(n_steps=n, ge2c=sum(x >= 0.02 for x in a), ge5c=sum(x >= 0.05 for x in a), ge10c=sum(x >= 0.10 for x in a),
                ge20c=sum(x >= 0.20 for x in a), ge30c=sum(x >= 0.30 for x in a), max=max(a) if a else None)

for k in ("Senate", "Governor", "House", "Control", "baseline"):
    show(f"1-min steps {k}", bucket(steps_by[k]))
show("1-min steps ALL night", bucket(steps_all))
print("\nraces with >=1 1-min move >= 10c:", sum(r["big1m"] > 0 for r in rows), "of", len(rows))
print("big moves:")
for b in sorted(big_moves, key=lambda b: -abs(b["d"]))[:60]:
    print("  ", b)

# ---------- 2022: resolution times only ----------
import re
ms = []
for f in sorted(glob.glob(os.path.join(RAW, "m22", "m_*.json"))):
    ms += json.load(open(f))
ST2 = {"New Hampshire", "Vermont", "Arizona", "Georgia", "Iowa", "Pennsylvania", "Nevada", "Wisconsin", "Colorado", "Michigan", "Maine",
       "Minnesota", "New Mexico", "Ohio", "Kansas", "Texas", "North Carolina", "Florida", "Oregon", "Alaska", "Utah", "Oklahoma", "New York",
       "Washington", "California"}
r22 = []
for m in ms:
    qn = m["question"]
    kind = state = dist = None
    mm = re.match(r"^(Senate|Governor):.* in ([A-Z][A-Za-z ]+)\?$", qn.strip())
    if mm:
        kind, state = mm.group(1), mm.group(2).strip()
    mm2 = re.match(r"^([A-Z]{2})-(\d+) House", qn)
    if mm2:
        kind, dist = "House", f"{mm2.group(1)}-{mm2.group(2)}"
        state = {"OH": "Ohio", "TX": "Texas", "MI": "Michigan", "CA": "California", "PA": "Pennsylvania"}[mm2.group(1)]
    if "Peltola" in qn:
        kind, state, dist = "House", "Alaska", "AK-AL"
    if "Murkowski" in qn:
        kind, state = "Senate", "Alaska"
    if qn.startswith("Which party will win the U.S. House"):
        kind, state, dist = "Control", "US", "House"
    if qn.startswith("Which party will control the U.S. Senate"):
        kind, state, dist = "Control", "US", "Senate"
    if not kind:
        continue
    cf, cl = close_ts(ED22, state, dist, kind == "Control")
    res = parse_closed(m.get("closedTime"))
    r22.append(dict(kind=kind, state=state, district=dist, q=qn, resolved=et(res) if res else None,
                    resolved_h=None if res is None else round((res - cl) / 3600, 1)))
json.dump(r22, open(os.path.join(OUT, "races22_resolution.json"), "w"), indent=1)
print("\n2022 races:", len(r22), Counter(r["kind"] for r in r22))
show("2022 resolved_h (hours after last poll close)", q([r["resolved_h"] for r in r22 if r["resolved_h"] is not None]))
for r in sorted(r22, key=lambda r: r["resolved_h"] or 0):
    print("  ", r["kind"], r["district"] or r["state"], r["resolved"], r["resolved_h"])
show("2024 resolved_h by kind", {k: q([r["resolved_h"] for r in rows if r["kind"] == k and r["resolved_h"] is not None]) for k in ("Senate", "Governor", "House", "Control")})
show("2022 resolved_h by kind", {k: q([r["resolved_h"] for r in r22 if r["kind"] == k and r["resolved_h"] is not None]) for k in ("Senate", "Governor", "House", "Control")})
