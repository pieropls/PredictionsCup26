"""Build the 2024 race universe from cached gamma event lists and fetch 1-min CLOB prices-history.

Public, unauthenticated endpoints only (gamma-api.polymarket.com, clob.polymarket.com/prices-history).
Gentle: >= 0.8 s between requests, every response cached on disk, never re-fetched.
Usage: python3 -I pm24_fetch.py <raw_dir>
"""
import json, glob, os, re, sys, time, urllib.request, urllib.parse

RAW = sys.argv[1]
HIST = os.path.join(RAW, "hist24")
os.makedirs(HIST, exist_ok=True)
GAP = 0.8
_last = [0.0]


def get(url, path):
    if os.path.exists(path) and os.path.getsize(path) > 0:
        return json.load(open(path))
    wait = _last[0] + GAP - time.time()
    if wait > 0:
        time.sleep(wait)
    req = urllib.request.Request(url, headers={"User-Agent": "research/1.0 (public market data)"})
    for attempt in range(4):
        try:
            with urllib.request.urlopen(req, timeout=30) as r:
                body = r.read()
            break
        except Exception as e:  # noqa: BLE001
            print("  retry", attempt, e, file=sys.stderr)
            time.sleep(3 * (attempt + 1))
    else:
        raise RuntimeError(url)
    _last[0] = time.time()
    open(path, "wb").write(body)
    return json.loads(body)


ST = {"ak": "Alaska", "az": "Arizona", "ca": "California", "co": "Colorado", "fl": "Florida", "ia": "Iowa", "nh": "New Hampshire",
      "nj": "New Jersey", "ny": "New York", "or": "Oregon", "pa": "Pennsylvania", "wa": "Washington"}

events = {}
for f in glob.glob(os.path.join(RAW, "tag_*.json")) + glob.glob(os.path.join(RAW, "ev24", "ev_*.json")) + glob.glob(os.path.join(RAW, "s2_*.json")):
    d = json.load(open(f))
    for e in (d.get("events", []) if isinstance(d, dict) else d):
        events.setdefault(e["slug"], e)

races = []
for slug, e in sorted(events.items()):
    end = (e.get("endDate") or "")[:4]
    if end != "2024":
        continue
    kind = state = district = None
    m = re.match(r"^([a-z-]+)-us-senate-election-winner$", slug)
    if m:
        kind, state = "Senate", m.group(1).replace("-", " ").title()
    elif slug == "nebraska-senate-special-election":
        kind, state = "Senate", "Nebraska"
        district = "special"
    elif re.match(r"^([a-z-]+)-governor-election-winner$", slug):
        kind, state = "Governor", re.match(r"^([a-z-]+)-governor-election-winner$", slug).group(1).replace("-", " ").title()
    elif re.match(r"^([a-z]{2})-(\d+)-(election|house-race)-", slug) and "primary" not in slug and "special" not in slug:
        mm = re.match(r"^([a-z]{2})-(\d+)-", slug)
        kind, state = "House", ST[mm.group(1)]
        q = e["markets"][0]["question"]
        dm = re.match(r"^([A-Z]{2})-(\d+)", q)
        district = f"{dm.group(1)}-{int(dm.group(2)):02d}" if dm else f"{mm.group(1).upper()}-{int(mm.group(2)):02d}"
    elif slug in ("house-anon-player-0022-election", "which-party-will-control-the-us-senate-anon-player-0023-election"):
        kind, state = "Control", "US"
        district = "House" if "house" in slug else "Senate"
    if not kind:
        continue
    # winner / loser tokens
    mk = e["markets"]
    legs = []
    for x in mk:
        toks = json.loads(x["clobTokenIds"]) if x.get("clobTokenIds") else []
        outs = json.loads(x["outcomes"])
        prices = [float(p) for p in json.loads(x["outcomePrices"])]
        legs.append(dict(q=x["question"], toks=toks, outs=outs, prices=prices, closed=x.get("closedTime"), uma_end=x.get("umaEndDate"),
                         vol=float(x.get("volume") or 0), group=x.get("groupItemTitle"), cond=x.get("conditionId")))
    if len(legs) == 1:  # binary: outcomes are the two candidates / parties
        L = legs[0]
        wi = max(range(2), key=lambda i: L["prices"][i])
        win = dict(tok=L["toks"][wi], label=L["outs"][wi], closed=L["closed"], uma_end=L["uma_end"], vol=L["vol"])
        lose = dict(tok=L["toks"][1 - wi], label=L["outs"][1 - wi], complement=True)
    else:  # one Yes/No market per party; winner = the market whose Yes resolved 1
        wl = [L for L in legs if L["prices"][0] > 0.5]
        if len(wl) != 1:
            print("skip (no unique winner)", slug)
            continue
        W = wl[0]
        others = sorted([L for L in legs if L is not W], key=lambda L: -L["vol"])
        win = dict(tok=W["toks"][0], label=W["group"] or W["q"], closed=W["closed"], uma_end=W["uma_end"], vol=W["vol"])
        lose = dict(tok=others[0]["toks"][0], label=others[0]["group"] or others[0]["q"], complement=False)
    races.append(dict(slug=slug, kind=kind, state=state, district=district, title=e["title"], win=win, lose=lose,
                      n_markets=len(legs), event_closed=e.get("closedTime")))

print(len(races), "races:", {k: sum(r["kind"] == k for r in races) for k in ("Senate", "Governor", "House", "Control")})
json.dump(races, open(os.path.join(RAW, "races24.json"), "w"), indent=1)

S = 1730808000            # 2024-11-05 12:00 UTC (07:00 ET)
E = S + 8 * 86400         # 2024-11-13 12:00 UTC
for r in races:
    for leg in ("win", "lose"):
        tok = r[leg]["tok"]
        p = os.path.join(HIST, f"{tok[:20]}_f1.json")
        url = "https://clob.polymarket.com/prices-history?" + urllib.parse.urlencode(dict(market=tok, startTs=S, endTs=E, fidelity=1))
        h = get(url, p).get("history", [])
        r[leg]["n1"] = len(h)
        if leg == "win":
            reached = any(x["p"] >= 0.95 for x in h)
            if not reached:  # slow race: 10-min tail in 14-day chunks (the API refuses intervals over ~15-20 days) to Dec 25
                for k in range(3):
                    a = E + k * 14 * 86400
                    p2 = os.path.join(HIST, f"{tok[:20]}_tail10_{k}.json")
                    url2 = "https://clob.polymarket.com/prices-history?" + urllib.parse.urlencode(dict(market=tok, startTs=a, endTs=a + 14 * 86400, fidelity=10))
                    t = get(url2, p2).get("history", [])
                    if any(x["p"] >= 0.99 for x in t):
                        break
    print(r["kind"], r["state"], r["district"] or "", r["win"]["label"], r["win"]["n1"], r["lose"]["n1"])
json.dump(races, open(os.path.join(RAW, "races24.json"), "w"), indent=1)
