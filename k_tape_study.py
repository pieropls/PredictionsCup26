"""Tape study: where did the money on SIG come from since the cup opened (corrected method, day 2 §5 / morning_plan 4.1).

python3 -u k_tape_study.py                 collect (incremental, resumable) + analyse -> docs/TAPE_STUDY.md
python3 -u k_tape_study.py --analyse-only  analyse what's on disk
python3 k_tape_study.py --daily            docs/daily/<date>.md + docs/leaders/<date>.md + tier A refresh (local files only;
                                           the engine also runs this at 07:00 PT)
python3 k_tape_study.py --leaders          docs/leaders/<date>.md only

Corrected method: the tape's `side` is NOT the aggressor (a tick-rule check agrees 50.7%), so there is no maker/taker split.
The number reported is the value that changed hands at a wrong price: sum |price - Polymarket fair| x shares, by situation,
distance from fair, size, hour and market. Trade ids are saved; prints are ordered by (time, id) per market.
Collection is incremental: data/tape/state.json keeps the newest trade per exchange (the engine's live collector keeps
its own in data/intel/state.json), so a re-run only reads what's new. Reads: <= READS_PER_MIN (the engine shares the
account's 100/min). Leaderboard periods: 1d / 7d / 30d / all (from /api/v1/openapi.json; "day" is a 400)."""
import bisect
import collections
import datetime as dt
import json
import os
import sys
import time
import urllib.request

import requests

import a_config as C
import l_intel as li

READS_PER_MIN = 20                       # account budget is 100/min, shared with the live engine (~60-75/min)
OUT = os.path.join(C.DATA, "tape")
GAMMA, CLOB = "https://gamma-api.polymarket.com", "https://clob.polymarket.com"
S = requests.Session()
S.headers.update({"Authorization": f"Bearer {C.API_KEY}"})
_reads = collections.deque()


# ------------------------------------------------------------------ SIG reads (throttled, retried)
def sig(path, **params):
    for attempt in range(6):
        while len(_reads) >= READS_PER_MIN:
            if _reads[0] < time.time() - 60:
                _reads.popleft()
            else:
                time.sleep(0.5)
        _reads.append(time.time())
        try:
            r = S.get(f"{C.BASE_URL}{path}", params=params, timeout=60)
        except requests.RequestException as e:
            print("  retry", path, repr(e)[:80], flush=True)
            time.sleep(5 * (attempt + 1))
            continue
        if r.status_code == 429:
            time.sleep(float(r.headers.get("Retry-After", 60)))
            continue
        if r.status_code >= 500:
            time.sleep(5 * (attempt + 1))
            continue
        r.raise_for_status()
        return r.json()
    raise RuntimeError(f"SIG read failed: {path}")


def pm(url):
    req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0"})
    return json.load(urllib.request.urlopen(req, timeout=30))


def ts(s):
    return dt.datetime.fromisoformat(s.replace("Z", "+00:00")).timestamp()


# ------------------------------------------------------------------ 1. collect
def collect():
    os.makedirs(OUT, exist_ok=True)
    t = sig(f"/tournaments/{C.TOURNAMENT}")
    tid, start = t["id"], t.get("startDate") or "2026-10-01T16:00:00Z"
    markets, cursor = [], None
    while True:
        d = sig(f"/tournaments/{C.TOURNAMENT}/markets", limit=100, **({"cursor": cursor} if cursor else {}))
        markets += d["data"]
        if not d["pagination"]["hasMore"]:
            break
        cursor = d["pagination"]["nextCursor"]
    mk = {str(m["exchanges"][0]["id"]): dict(mid=str(m["id"]), title=m["title"], created=m.get("createdAt"))
          for m in markets if m.get("exchanges")}
    json.dump(dict(tid=tid, start=start, markets=mk), open(os.path.join(OUT, "meta.json"), "w"))
    print(f"{len(mk)} markets, tape since {start}", flush=True)

    raw, state_path = os.path.join(OUT, "trades.jsonl"), os.path.join(OUT, "state.json")
    boundary = collections.defaultdict(set)
    if not os.path.exists(state_path) and os.path.exists(raw):     # seed from the first (id-less) collection
        newest = {}
        for line in open(raw):
            r = json.loads(line)
            if r["t"] > newest.get(r["ex"], ""):
                newest[r["ex"]] = r["t"]
        json.dump({ex: dict(id="0", t=t) for ex, t in newest.items()}, open(state_path, "w"))
    for line in (open(raw) if os.path.exists(raw) else []):           # prints at a seeded boundary time: skip re-reads
        r = json.loads(line)
        boundary[r["ex"]].add((r["t"], r["p"], r["q"]))
    col = li.TapeCollector(lambda path, p: sig(path, **p), tid, state_path, first_window_s=time.time() - ts(start), max_pages=1000)
    for i, ex in enumerate(sorted(mk)):
        new = col.visit(ex)
        rows = [dict(id=r["id"], ex=ex, t=dt.datetime.fromtimestamp(r["ts"], dt.timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z",
                     p=r["p"], q=r["q"]) for r in new]
        rows = [r for r in rows if (r["t"], r["p"], r["q"]) not in boundary.get(ex, ())]
        with open(raw, "a") as fh:
            for r in rows:
                fh.write(json.dumps(r) + "\n")
        print(f"  [{i + 1}/{len(mk)}] ex {ex}: {len(rows)} new trades", flush=True)

    leaders = {}
    for per in ("1d", "all"):                                     # the enum is 1d/7d/30d/all ("day" is a 400)
        try:
            leaders[per] = sig(f"/tournaments/{C.TOURNAMENT}/leaderboard", period=per, limit=100, sort="pnl")
        except Exception as e:
            print("  leaderboard", per, "failed:", repr(e)[:100], flush=True)
    json.dump(leaders, open(os.path.join(OUT, "leaders.json"), "w"))

    # Polymarket YES history per mapped exchange (candidate sums added up), 5-minute points
    mp = json.load(open(C.PM_MAP_FILE))
    ids = sorted({str(i) for m in mp for i in (m.get("pm_ids") or [m["pm_id"]])})
    token = {}
    for k in range(0, len(ids), 100):
        for x in pm(f"{GAMMA}/markets?limit=100&" + "&".join(f"id={j}" for j in ids[k:k + 100])):
            try:
                outs, toks = json.loads(x["outcomes"]), json.loads(x["clobTokenIds"])
                token[str(x["id"])] = toks[outs.index("Yes")]
            except Exception:
                pass
    s0, hist = int(ts(start)), {}
    for i in ids:
        if i in token:
            try:
                h = pm(f"{CLOB}/prices-history?market={token[i]}&startTs={s0}&endTs={int(time.time())}&fidelity=5")
                hist[i] = [(int(p["t"]), float(p["p"])) for p in h.get("history", [])]
            except Exception as e:
                print("  pm history failed", i, repr(e)[:60], flush=True)
    by_ex = {}
    for m in mp:
        legs = [hist.get(str(i)) for i in (m.get("pm_ids") or [m["pm_id"]])]
        if legs and all(legs):
            base = legs[0]
            by_ex[m["ex"]] = [(t, sum(_at(l, t) for l in legs)) for t, _ in base]
    json.dump(by_ex, open(os.path.join(OUT, "pm_history.json"), "w"))
    print(f"Polymarket history for {len(by_ex)} markets", flush=True)


def _at(series, t):
    """Value of a [(t, p)] series at time t (last point at or before t; first point if t is earlier)."""
    i = bisect.bisect_right([s[0] for s in series], t) - 1
    return series[max(i, 0)][1]


# ------------------------------------------------------------------ 2. analyse
def analyse(trades, pmh, meta, leaders):
    """trades: [{ex, t, p, q[, id]}] YES-normalized; pmh: {ex: [(t, fair)]}. Value at a wrong price, no maker/taker."""
    mk = meta["markets"]
    start = ts(meta["start"])
    times = {ex: [x[0] for x in s] for ex, s in pmh.items()}
    for i, r in enumerate(trades):
        r["ts"] = ts(r["t"])
        r.setdefault("id", str(i))

    def fair(ex, t, lag=0):
        s = pmh.get(ex)
        if not s:
            return None
        i = bisect.bisect_right(times[ex], t - lag) - 1
        if i < 0 or t - lag - s[i][0] > 1800:
            return None
        return s[i][1]

    in_burst = {r["id"] for b in li.bursts(trades) if len(b) >= 3 or abs(b[-1]["p"] - b[0]["p"]) >= 0.05 for r in b}
    rows = []
    for r in li.order(trades):
        f = fair(r["ex"], r["ts"])
        if f is None:
            continue
        f_before = fair(r["ex"], r["ts"], lag=600)
        pm_move = None if f_before is None else f - f_before
        if r["id"] in in_burst:
            kind = "sweep / burst (prints < 5 s apart)"
        elif r["ts"] - start < 3 * 3600 and abs(r["p"] - f) >= 0.10:
            kind = "opening mispricing (first 3 h)"
        elif pm_move is not None and abs(pm_move) >= 0.02:
            kind = "within 10 min of a >= 2c Polymarket move"
        else:
            kind = "ordinary"
        rows.append(dict(ex=r["ex"], id=r["id"], t=r["t"], p=r["p"], q=r["q"], fair=round(f, 4), kind=kind,
                         value=round(abs(r["p"] - f) * r["q"], 2), wrong=li.wrong_side(r["p"], f),
                         title=mk.get(r["ex"], {}).get("title", r["ex"])))

    def tally(key):
        out = collections.defaultdict(lambda: dict(trades=0, shares=0, value=0.0))
        for x in rows:
            o = out[key(x)]
            o["trades"] += 1
            o["shares"] += x["q"]
            o["value"] += x["value"]
        return {k: {kk: round(vv, 1) if isinstance(vv, float) else vv for kk, vv in v.items()} for k, v in sorted(out.items())}

    def size(x):
        q = x["q"]
        return "<100" if q < 100 else "100-1k" if q < 1000 else "1k-5k" if q < 5000 else ">5k"

    def hour(x):
        return dt.datetime.fromisoformat(x["t"].replace("Z", "+00:00")).astimezone(dt.timezone(dt.timedelta(hours=-7))).strftime("%m-%d %H:00 PT")

    by_market = tally(lambda x: x["title"])
    by_market = dict(sorted(by_market.items(), key=lambda kv: -kv[1]["value"])[:30])
    lead = []
    for per, d in leaders.items():
        for e in d.get("leaderboard", [])[:25]:
            v, nt = e.get("volume") or 0, e.get("tradesCount") or 0
            lead.append(dict(period=per, rank=e["rank"], user=e.get("username"), pnl=round(e["pnl"]), trades=nt,
                             volume=round(v), roi=e.get("roi"), style=li.style(e["pnl"], nt, v),
                             pnl_per_volume=round(e["pnl"] / v, 4) if v else None, volume_per_trade=round(v / nt) if nt else None))
    return dict(trades_total=len(trades), trades_with_fair=len(rows), value_total=round(sum(x["value"] for x in rows)),
                by_kind=tally(lambda x: x["kind"]), by_dislocation=tally(lambda x: li.dist_bin(abs(x["p"] - x["fair"]))),
                by_size=tally(size), by_hour=tally(hour), by_market=by_market,
                top_trades=sorted(rows, key=lambda x: -x["value"])[:100], leaders=lead)


# ------------------------------------------------------------------ 3. report
def report(s):
    L = ["# Tape study: where the money came from (corrected method)", "",
         f"Generated {time.strftime('%Y-%m-%d %H:%M')} by `k_tape_study.py`. {s['trades_total']:,} trades on the tape, "
         f"{s['trades_with_fair']:,} with a Polymarket fair at that time: **{s['value_total']:,}** SIG changed hands at a wrong "
         f"price (Σ |price − fair| × shares). The tape's side is not the aggressor, so there is no maker/taker split.", ""]

    def table(title, d):
        L.extend([f"## {title}", "", "| bucket | trades | shares | value at a wrong price |", "|---|---:|---:|---:|"])
        for k, v in d.items():
            L.append(f"| {k} | {v['trades']:,} | {v['shares']:,.0f} | {v['value']:,.0f} |")
        L.append("")

    table("By situation", s["by_kind"])
    table("By distance from fair", {b: s["by_dislocation"][b] for b in li.BINS if b in s["by_dislocation"]})
    table("By trade size (shares)", s["by_size"])
    table("By hour (PT)", s["by_hour"])
    table("By market (top 30)", s["by_market"])
    L.extend(["## Leaderboard: how the leaders trade", "",
              "| period | # | user | style | P&L | trades | volume | P&L / volume | volume / trade | ROI |",
              "|---|---:|---|---|---:|---:|---:|---:|---:|---:|"])
    for e in s["leaders"]:
        L.append(f"| {e['period']} | {e['rank']} | {e['user']} | {e['style']} | {e['pnl']:+,} | {e['trades']:,} | {e['volume']:,} | "
                 f"{e['pnl_per_volume']} | {e['volume_per_trade']} | {e['roi']} |")
    L.extend(["", "## 100 biggest trades by value at a wrong price", "",
              "| time | market | price | PM fair | shares | value | wrong side | situation |", "|---|---|---:|---:|---:|---:|---|---|"])
    for x in s["top_trades"]:
        L.append(f"| {x['t'][5:16]} | {x['title'][:55]} | {x['p']:.3f} | {x['fair']:.3f} | {x['q']:,.0f} | {x['value']:,.0f} | "
                 f"{x['wrong']} | {x['kind']} |")
    path = os.path.join(os.path.dirname(C.DATA), "docs", "TAPE_STUDY.md")
    keep = ""
    try:                                                # keep the hand-written reading above the generated part
        old = open(path).read()
        cut = old.find("\n---\n\n# Tape study")
        keep = old[:cut] + "\n---\n\n" if cut > 0 else ""
    except OSError:
        pass
    open(path, "w").write(keep + "\n".join(L) + "\n")
    return path


def main():
    docs = os.path.join(os.path.dirname(C.DATA), "docs")
    if "--daily" in sys.argv or "--leaders" in sys.argv:
        if "--daily" in sys.argv:
            print("daily:", li.run_daily(C.DATA, docs, time.time()), flush=True)
        else:
            import csv
            rows = list(csv.DictReader(open(os.path.join(C.DATA, "leaders.csv"))))
            evs = li._jsonl(os.path.join(C.DATA, "intel", "events.jsonl"), time.time() - 86400)
            md, _ = li.leaders_study(rows, evs, {}, time.time())
            os.makedirs(os.path.join(docs, "leaders"), exist_ok=True)
            path = os.path.join(docs, "leaders", time.strftime("%Y-%m-%d") + ".md")
            open(path, "w").write(md)
            print("leaders:", path, flush=True)
        return
    if "--analyse-only" not in sys.argv:
        collect()
    meta = json.load(open(os.path.join(OUT, "meta.json")))
    trades = [json.loads(l) for l in open(os.path.join(OUT, "trades.jsonl"))]
    pmh = {ex: [tuple(p) for p in s] for ex, s in json.load(open(os.path.join(OUT, "pm_history.json"))).items()}
    leaders = json.load(open(os.path.join(OUT, "leaders.json")))
    s = analyse(trades, pmh, meta, leaders)
    json.dump(s, open(os.path.join(OUT, "summary.json"), "w"), indent=1)
    print("report:", report(s), flush=True)


if __name__ == "__main__":
    main()
