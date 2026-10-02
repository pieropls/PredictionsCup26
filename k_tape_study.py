"""Tape study: where did the money on SIG come from since the cup opened, and what of it can we copy?

python3 -u k_tape_study.py            (run from ~/Desktop/susquehanna; resumable; reads only, never trades)

Reads (throttled to READS_PER_MIN so the live engine keeps its budget):
  every market's trade tape since the tournament start, the leaderboard (day + all), Polymarket price history.
Writes: data/tape/trades.jsonl (raw), data/tape/pm_history.json, data/tape/leaders.json, data/tape/summary.json,
        docs/TAPE_STUDY.md (the report).
The tape has no usernames: this measures the market's money flows (who captured value: makers or takers, in which
situations), and the leaderboard's volume/ROI per account. It cannot see sunny-acorn's trades directly."""
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

READS_PER_MIN = 30                       # account budget is 100/min, shared with the live engine
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

    done_file, raw = os.path.join(OUT, "done.txt"), os.path.join(OUT, "trades.jsonl")
    done = set(open(done_file).read().split()) if os.path.exists(done_file) else set()
    for i, ex in enumerate(sorted(mk)):
        if ex in done:
            continue
        rows, cursor = [], None
        while True:
            p = dict(tournamentId=tid, limit=200, **{"from": start})
            if cursor:
                p["cursor"] = cursor
            d = sig(f"/exchanges/{ex}/trades", **p)
            rows += [dict(ex=ex, t=x["createdAt"], p=x["price"], q=x["size"], side=x["side"], vol=x.get("volume"))
                     for x in d["data"] if x.get("price") is not None]
            if not d["pagination"]["hasMore"]:
                break
            cursor = d["pagination"]["nextCursor"]
        with open(raw, "a") as fh:
            for r in rows:
                fh.write(json.dumps(r) + "\n")
        with open(done_file, "a") as fh:
            fh.write(ex + "\n")
        print(f"  [{i + 1}/{len(mk)}] ex {ex}: {len(rows)} trades", flush=True)

    leaders = {}
    for per in ("day", "all"):
        try:
            leaders[per] = sig("/leaderboards", tournamentSlug=C.TOURNAMENT, period=per, limit=100)
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
    """trades: [{ex, t, p, q, side}] YES-normalized; pmh: {ex: [(t, fair)]}. Returns the summary dict."""
    mk = meta["markets"]
    start = ts(meta["start"])
    times = {ex: [x[0] for x in s] for ex, s in pmh.items()}
    for r in trades:
        r["ts"] = ts(r["t"])
    trades.sort(key=lambda r: (r["ex"], r["ts"]))

    # Which tape side is the aggressor? In a burst (same market, < 3 s apart) a buyer of YES walks the price up.
    agree = n = 0
    for a, b in zip(trades, trades[1:]):
        if a["ex"] == b["ex"] and b["ts"] - a["ts"] < 3 and a["side"] == b["side"] and b["p"] != a["p"]:
            n += 1
            agree += (b["p"] > a["p"]) == (b["side"] == "YES")
    side_is_taker = n == 0 or agree / n >= 0.5

    def fair(ex, t, lag=0):
        s = pmh.get(ex)
        if not s:
            return None
        i = bisect.bisect_right(times[ex], t - lag) - 1
        if i < 0 or t - lag - s[i][0] > 1800:
            return None
        return s[i][1]

    # bursts = sweeps: same market, same aggressor side, trades < 5 s apart, >= 3 prints or >= 5c of price travel
    burst_id, bid = {}, 0
    i = 0
    while i < len(trades):
        j = i
        while (j + 1 < len(trades) and trades[j + 1]["ex"] == trades[i]["ex"] and trades[j + 1]["side"] == trades[i]["side"]
               and trades[j + 1]["ts"] - trades[j]["ts"] < 5):
            j += 1
        if j - i + 1 >= 3 or abs(trades[j]["p"] - trades[i]["p"]) >= 0.05:
            bid += 1
            for k in range(i, j + 1):
                burst_id[k] = bid
        i = j + 1

    rows = []
    for k, r in enumerate(trades):
        f = fair(r["ex"], r["ts"])
        if f is None:
            continue
        buy_yes = (r["side"] == "YES") == side_is_taker            # taker bought YES?
        taker_edge = (f - r["p"]) if buy_yes else (r["p"] - f)     # per share, vs Polymarket fair at that time
        f_before = fair(r["ex"], r["ts"], lag=600)
        pm_move = None if f_before is None else f - f_before        # Polymarket move over the previous 10 min
        if burst_id.get(k):
            kind = "sweep (taker walked the book)"
        elif r["ts"] - start < 3 * 3600 and abs(r["p"] - f) >= 0.10:
            kind = "opening mispricing (first 3 h)"
        elif pm_move is not None and abs(pm_move) >= 0.02 and taker_edge >= 0.02 and (pm_move > 0) == buy_yes:
            kind = "stale quote picked off after a PM move"
        else:
            kind = "ordinary"
        rows.append(dict(ex=r["ex"], t=r["t"], p=r["p"], q=r["q"], fair=round(f, 4), buy_yes=buy_yes,
                         taker=round(taker_edge * r["q"], 2), edge=round(taker_edge, 4), kind=kind,
                         title=mk.get(r["ex"], {}).get("title", r["ex"])))

    def tally(key):
        out = collections.defaultdict(lambda: dict(trades=0, shares=0, taker_pnl=0.0, maker_pnl=0.0))
        for x in rows:
            o = out[key(x)]
            o["trades"] += 1
            o["shares"] += x["q"]
            o["taker_pnl"] += x["taker"]
            o["maker_pnl"] -= x["taker"]
        return {k: {kk: round(vv, 1) if isinstance(vv, float) else vv for kk, vv in v.items()} for k, v in sorted(out.items())}

    def disloc(x):
        d = abs(x["p"] - x["fair"])
        return "<2c" if d < .02 else "2-5c" if d < .05 else "5-10c" if d < .10 else "10-25c" if d < .25 else ">25c"

    def size(x):
        q = x["q"]
        return "<100" if q < 100 else "100-1k" if q < 1000 else "1k-5k" if q < 5000 else ">5k"

    def hour(x):
        return dt.datetime.fromisoformat(x["t"].replace("Z", "+00:00")).astimezone(dt.timezone(dt.timedelta(hours=-7))).strftime("%m-%d %H:00 PT")

    top = sorted(rows, key=lambda x: -abs(x["taker"]))[:100]
    lead = []
    for per, d in leaders.items():
        for e in d.get("leaderboard", [])[:25]:
            v, nt = e.get("volume") or 0, e.get("tradesCount") or 0
            lead.append(dict(period=per, rank=e["rank"], user=e.get("username"), pnl=round(e["pnl"]), trades=nt,
                             volume=round(v), roi=e.get("roi"), win=e.get("winRate"),
                             pnl_per_volume=round(e["pnl"] / v, 4) if v else None, volume_per_trade=round(v / nt) if nt else None))
    return dict(side_is_taker=side_is_taker, side_check=f"{agree}/{n}", trades_total=len(trades), trades_with_fair=len(rows),
                by_kind=tally(lambda x: x["kind"]), by_dislocation=tally(disloc), by_size=tally(size), by_hour=tally(hour),
                top_trades=top, leaders=lead)


# ------------------------------------------------------------------ 3. report
def report(s):
    L = ["# Tape study: where the money came from", "",
         f"Generated {time.strftime('%Y-%m-%d %H:%M')} by `k_tape_study.py`. {s['trades_total']:,} trades on the tape, "
         f"{s['trades_with_fair']:,} with a Polymarket fair at that time. Edge = vs the Polymarket mid at the trade time. "
         f"Tape side read as the {'aggressor' if s['side_is_taker'] else 'resting order'} (burst check {s['side_check']}).",
         "", "Taker P&L > 0: the aggressor got the better of the trade. Maker P&L > 0: resting orders won.", ""]

    def table(title, d):
        L.extend([f"## {title}", "", "| bucket | trades | shares | taker P&L | maker P&L |", "|---|---:|---:|---:|---:|"])
        for k, v in d.items():
            L.append(f"| {k} | {v['trades']:,} | {v['shares']:,} | {v['taker_pnl']:+,.0f} | {v['maker_pnl']:+,.0f} |")
        L.append("")

    table("By situation", s["by_kind"])
    table("By distance from fair", s["by_dislocation"])
    table("By trade size (shares)", s["by_size"])
    table("By hour (PT)", s["by_hour"])
    L.extend(["## Leaderboard: how the leaders trade", "",
              "| period | # | user | P&L | trades | volume | P&L / volume | volume / trade | ROI | win rate |",
              "|---|---:|---|---:|---:|---:|---:|---:|---:|---:|"])
    for e in s["leaders"]:
        L.append(f"| {e['period']} | {e['rank']} | {e['user']} | {e['pnl']:+,} | {e['trades']:,} | {e['volume']:,} | "
                 f"{e['pnl_per_volume']} | {e['volume_per_trade']} | {e['roi']} | {e['win']} |")
    L.extend(["", "## 100 biggest trades by value transferred", "",
              "| time | market | taker bought | price | PM fair | shares | taker P&L | situation |", "|---|---|---|---:|---:|---:|---:|---|"])
    for x in s["top_trades"]:
        L.append(f"| {x['t'][5:16]} | {x['title'][:55]} | {'YES' if x['buy_yes'] else 'NO'} | {x['p']:.3f} | {x['fair']:.3f} | "
                 f"{x['q']:,} | {x['taker']:+,.0f} | {x['kind']} |")
    L.extend(["", "## What's copyable (read with the tables above)", "",
              "- **Maker P&L large in sweeps / >10c buckets** → resting deep ladders on every market (night_plan §5.3) capture it.",
              "- **Taker P&L large in 'stale quote picked off'** → speed: P2 realtime + takes on PM moves (A9).",
              "- **Opening mispricing large** → the new-market sniper (§5.5) matters; it only pays if SIG adds markets.",
              "- **Leaders' P&L / volume and volume / trade** show their edge per SIG traded and their size: compare with ours.", ""])
    path = os.path.join(os.path.dirname(C.DATA), "docs", "TAPE_STUDY.md")
    open(path, "w").write("\n".join(L))
    return path


def main():
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
