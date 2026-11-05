"""L. INTEL: where the money on SIG goes, what we caught, what the leaders do (day 2 §5; morning_plan Part 4).

Pure functions over plain data; the engine (live tape collector, leaders logger) and k_tape_study.py (on demand) feed them.

  Trades are YES-normalized prints {id, ex, ts, p, q}. The tape's `side` is NOT the aggressor (tick-rule check: 50.7%),
  so nothing here uses it. Value at a wrong price = |p - fair| x q: what changed hands away from Polymarket fair.
  Within a market, prints are ordered by (ts, id): never by time alone (same-second prints).

  bursts(trades)            same-market runs of prints < 5 s apart
  detect(trades, fair_at)   big events (§4.3): a print or burst worth >= 1,000 at a wrong price; any print >= 40c from
                            fair with >= 5,000 shares; a Max sweep: one burst costing >= 50,000 for the wrong side
  missed_value(ev, levels)  what resting buy levels on the side that gained would have caught: best price first, up to
                            the burst's shares
  style(...)                the leaders' style labels (day 2 §5.1, documented rules)
  leaders_study(...)        docs/leaders/<date>.md
  daily_report(...)         docs/daily/<date>.md
"""
import collections
import datetime as dt
import json
import os
import statistics

BIG_VALUE, FAR_PX, FAR_QTY, SWEEP_COST, BURST_GAP_S = 1_000.0, 0.40, 5_000, 50_000.0, 5.0
JUMP, JUMP_WINDOW_S, LINK_WINDOW_S = 10_000.0, 600, 600
NO_FAIR_WALK = 0.20               # without a fair, a Max sweep must have walked the price this far
THIN_TAPE = 20_000                 # prints: fewer in the window and tier A only grows
STYLES = [
    ("Opening harvester", "P&L/volume > 0.5 and < 500 trades"),
    ("High-volume market maker", "volume/trade > 5k and P&L/volume 2-10%"),
    ("High-frequency bot", "> 2,000 trades"),
    ("Directional", "everything else"),
]


def order(trades):
    """Per market, oldest first; same-second prints by trade id (ids increase on SIG)."""
    return sorted(trades, key=lambda r: (str(r["ex"]), r["ts"], int(r["id"]) if str(r.get("id", "")).isdigit() else 0))


def bursts(trades):
    """[[print, ...], ...]: consecutive prints of one market less than BURST_GAP_S apart."""
    out = []
    for r in order(trades):
        if out and out[-1][-1]["ex"] == r["ex"] and r["ts"] - out[-1][-1]["ts"] < BURST_GAP_S:
            out[-1].append(r)
        else:
            out.append([r])
    return out


def wrong_side(p, f):
    """Who traded at the wrong price: above fair the YES buyer overpaid, below it the YES seller (a NO buyer at 1-p)."""
    return "YES buyer" if p > f else "YES seller"


def detect(trades, fair_at, names=None):
    """Big events in `trades`. fair_at(ex, ts) -> YES fair or None. One event per burst that qualifies:
    {t, ex, market, n, shares, p_lo, p_hi, fair, value, cost, kinds, wrong, gainer_side, first_id, last_id}."""
    names = names or {}
    out = []
    for b in bursts(trades):
        f = fair_at(b[0]["ex"], b[0]["ts"])
        shares = sum(r["q"] for r in b)
        kinds = []
        if f is None:
            value = None
            cost = sum(r["q"] * max(r["p"], 1 - r["p"]) for r in b)
            wrong = None
        else:
            value = sum(abs(r["p"] - f) * r["q"] for r in b)
            cost = sum(r["q"] * (r["p"] if r["p"] > f else 1 - r["p"]) for r in b)
            # the wrong side of the burst: where most of the value went
            up = sum((r["p"] - f) * r["q"] for r in b if r["p"] > f)
            dn = sum((f - r["p"]) * r["q"] for r in b if r["p"] < f)
            wrong = "YES buyer" if up >= dn else "YES seller"
            if value >= BIG_VALUE:
                kinds.append("value >= 1k")
            if any(abs(r["p"] - f) >= FAR_PX and r["q"] >= FAR_QTY for r in b):
                kinds.append(">= 40c from fair, >= 5k shares")
        if cost >= SWEEP_COST and (value >= BIG_VALUE if f is not None else max(r["p"] for r in b) - min(r["p"] for r in b) >= NO_FAIR_WALK):
            kinds.append("Max sweep")                     # no fair: only a burst that walked the book counts (big != wrong)
        if not kinds:
            continue
        ex = b[0]["ex"]
        out.append(dict(t=b[0]["ts"], t_end=b[-1]["ts"], ex=ex, market=names.get(ex, ex), n=len(b), shares=round(shares),
                        p_lo=min(r["p"] for r in b), p_hi=max(r["p"] for r in b), fair=None if f is None else round(f, 4),
                        value=None if value is None else round(value, 2), cost=round(cost, 2), kinds=kinds, wrong=wrong,
                        # the side whose resting bids were hit at a good price: YES bids when YES sold cheap, else NO bids
                        gainer_side=None if wrong is None else ("yes" if wrong == "YES seller" else "no"),
                        walk=("down: a YES seller" if b[-1]["p"] < b[0]["p"] else "up: a YES buyer" if b[-1]["p"] > b[0]["p"] else "flat"),
                        first_id=b[0].get("id"), last_id=b[-1].get("id")))
    return out


def missed_value(ev, levels, side_fair=None):
    """What resting buys on the side that gained would have caught. levels: [(price in that side's terms, shares)].
    The burst traded `shares` from best price inward; a level fills when the burst reached its price (side price >=
    the worst print in that side's terms), best price first, until the burst's shares run out.
    -> (shares caught, value at fair)."""
    if ev.get("fair") is None or not ev.get("gainer_side"):
        return 0.0, 0.0
    side = ev["gainer_side"]
    f = side_fair if side_fair is not None else (ev["fair"] if side == "yes" else 1 - ev["fair"])
    worst = ev["p_lo"] if side == "yes" else 1 - ev["p_hi"]       # deepest print, in the gaining side's terms
    left, got, val = float(ev["shares"]), 0.0, 0.0
    for px, q in sorted(levels, key=lambda l: -l[0]):
        if left <= 0 or px < worst - 1e-9:
            continue
        take = min(q, left)
        left -= take
        got += take
        val += take * (f - px)
    return round(got, 2), round(val, 2)


# ------------------------------------------------------------------------------------------------ leaders
def style(pnl, trades, volume):
    """Day 2 §5.1 style label; the first rule that matches wins (table order)."""
    ppv = pnl / volume if volume else 0.0
    vpt = volume / trades if trades else 0.0
    if ppv > 0.5 and trades < 500:
        return STYLES[0][0]
    if vpt > 5_000 and 0.02 <= ppv <= 0.10:
        return STYLES[1][0]
    if trades > 2_000:
        return STYLES[2][0]
    return STYLES[3][0]


def _ts(s):
    return dt.datetime.strptime(s, "%Y-%m-%d %H:%M:%S").timestamp()


def leader_series(rows):
    """leaders.csv rows -> {profile: [(ts, rank, name, pnl, trades, volume, roi, ours)]} oldest first."""
    out = collections.defaultdict(list)
    for r in rows:
        try:
            out[r["profile"]].append((_ts(r["time"]), int(r["rank"]), r["name"], float(r["value"]), int(float(r["trades"] or 0)),
                                      float(r["volume"] or 0), float(r["roi"] or 0), r.get("ours") in ("1", "True", "true")))
        except (ValueError, KeyError, TypeError):
            continue
    for v in out.values():
        v.sort()
    return out


def jumps(series, min_jump=JUMP, window=JUMP_WINDOW_S):
    """[(t0, t1, delta)] where P&L moved >= min_jump between snapshots <= window (+ 2 min slack) apart."""
    out = []
    for a, b in zip(series, series[1:]):
        if b[0] - a[0] <= window + 120 and abs(b[3] - a[3]) >= min_jump:
            out.append((a[0], b[0], b[3] - a[3]))
    return out


def thresholds(rows):
    """{ts: {rank: pnl}} for ranks 10/25/50/100, plus our row as 'ours' -> [(ts, {10:.., 25:.., 50:.., 100:.., ours:..})]."""
    snaps = collections.defaultdict(dict)
    for r in rows:
        try:
            t, rank = r["time"], int(r["rank"])
        except (KeyError, ValueError):
            continue
        if r.get("ours") in ("1", "True", "true"):
            snaps[t]["ours"] = (rank, float(r["value"]))
        elif rank in (10, 25, 50, 100):
            snaps[t][rank] = float(r["value"])
    return sorted(snaps.items())


def fmt_pct(x):
    return "–" if x is None else f"{x:.0%}"


def fmt(x, sign=False):
    if x is None:
        return "–"
    return f"{x:+,.0f}" if sign else f"{x:,.0f}"


def leaders_study(rows, events, our_layers, now, day_s=86400, tape_from=None):
    """Markdown for docs/leaders/<date>.md. rows: leaders.csv dicts; events: big events (detect); our_layers:
    {layer: P&L today at SIG marks}; tape_from: when the live tape collector's coverage starts (None: unknown)."""
    ser = leader_series(rows)
    t1 = max((s[-1][0] for s in ser.values() if s), default=now)
    t0 = t1 - day_s
    latest = {p: s[-1] for p, s in ser.items() if s and s[-1][0] >= t1 - 1800}
    acct = []
    for p, last in latest.items():
        s = ser[p]
        first = next((x for x in s if x[0] >= t0), s[0])
        _, rank, name, pnl, tr, vol, roi, ours = last
        js = [j for j in jumps(s) if j[1] >= t0]
        days = max(1 / 24, (last[0] - first[0]) / 86400)
        cap = max(1.0, first[3] + 100_000)                  # capital at the window start: P&L + the 100k stake
        acct.append(dict(profile=p, name=name, rank=rank, pnl=pnl, trades=tr, volume=vol, roi=roi, ours=ours,
                         d_pnl=pnl - first[3], d_trades=tr - first[4], d_volume=vol - first[5], since=first[0],
                         roc_day=(pnl - first[3]) / cap / days, turnover_day=(vol - first[5]) / cap / days,
                         ppv=pnl / vol if vol else None, vpt=vol / tr if tr else None, style=style(pnl, tr, vol), jumps=js))
    acct.sort(key=lambda a: a["rank"])
    L = [f"# Leaders study, {dt.datetime.fromtimestamp(t1).strftime('%Y-%m-%d %H:%M')} PT",
         "",
         f"Source: the engine's leaders logger (`data/leaders.csv`: top 50, #100 and our row every 10 min), "
         f"{len(ser)} accounts, window {dt.datetime.fromtimestamp(t0).strftime('%m-%d %H:%M')} → "
         f"{dt.datetime.fromtimestamp(t1).strftime('%m-%d %H:%M')} PT (or since the account was first logged). Only `/api/v1`.",
         "", "## Style rules (first match wins)", "", "| Style | Rule |", "|---|---|"]
    L += [f"| {s} | {r} |" for s, r in STYLES]
    # what's working now
    by = collections.defaultdict(list)
    for a in acct:
        if not a["ours"]:
            by[a["style"]].append(a["d_pnl"])
    L += ["", "## What's still working (Δ P&L over the window, by style)", "",
          "| Style | Accounts | Σ Δ P&L | median Δ P&L | best |", "|---|---:|---:|---:|---|"]
    for s, _ in STYLES:
        xs = by.get(s, [])
        best = max((a for a in acct if a["style"] == s and not a["ours"]), key=lambda a: a["d_pnl"], default=None)
        L.append(f"| {s} | {len(xs)} | {fmt(sum(xs), True) if xs else '–'} | {fmt(statistics.median(xs), True) if xs else '–'} | "
                 f"{'' if not best else best['name'] + ' ' + fmt(best['d_pnl'], True)} |")
    look = {"mm": "High-volume market maker", "fatfinger": "Opening harvester", "sniper": "Opening harvester",
            "ladder": "Directional", "pairs": "High-frequency bot", "recycler": "—", "release": "—", "position": "—"}
    if our_layers:
        L += ["", "Our layers today so far (SIG marks, realized + unrealized change), next to the style each resembles:", "", "| Our layer | P&L today | Resembles |",
              "|---|---:|---|"]
        L += [f"| {k} | {fmt(v, True)} | {look.get(k, '—')} |" for k, v in sorted(our_layers.items(), key=lambda kv: -abs(kv[1] or 0))]
    # per account
    L += ["", "## Per account", "",
          "| # | Account | Style | P&L | Δ P&L | Δ trades | Δ volume | return on capital / day | turnover / day | P&L/volume | volume/trade | jumps ≥ 10k in 10 min |",
          "|---:|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---|"]
    for a in acct:
        js = "; ".join(f"{dt.datetime.fromtimestamp(j[1]).strftime('%H:%M')} {fmt(j[2], True)}" for j in a["jumps"][:4])
        ppv = "–" if a["ppv"] is None else f"{a['ppv']:.2f}"
        L.append(f"| {a['rank']} | {a['name']}{' (us)' if a['ours'] else ''} | {a['style']} | {fmt(a['pnl'])} | {fmt(a['d_pnl'], True)} | "
                 f"{fmt(a['d_trades'], True)} | {fmt(a['d_volume'], True)} | {a['roc_day']:+.1%} | {a['turnover_day']:.1f}x | {ppv} | "
                 f"{fmt(a['vpt'])} | {js} |")
    # attribution
    L += ["", "## Attribution (inferred, not proof)", "",
          "A P&L jump ≥ 10k within 10 min, next to the big events (§4.3) within ±10 min: **likely counterparty**. "
          "A jump with no event is trading we can't see (OTC-like flow, marks, or markets the tape collector hadn't read yet)."
          + (f" The live tape covers only since {dt.datetime.fromtimestamp(tape_from).strftime('%m-%d %H:%M')} PT: earlier jumps "
             "can't be attributed yet." if tape_from else " The live tape collector has no data yet."), ""]
    n_j = 0
    for a in acct:
        for t0_, t1_, d in a["jumps"]:
            evs = [e for e in events if t0_ - LINK_WINDOW_S <= e["t"] <= t1_ + LINK_WINDOW_S]
            evs.sort(key=lambda e: -(e.get("value") or 0))
            n_j += 1
            when = dt.datetime.fromtimestamp(t1_).strftime("%m-%d %H:%M")
            if not tape_from or t1_ < tape_from:
                L.append(f"- {when} **{a['name']}** {fmt(d, True)}: before the live tape, can't attribute")
            elif evs:
                e = evs[0]
                L.append(f"- {when} **{a['name']}** {fmt(d, True)}: likely counterparty of {e['market']} "
                         f"({e['shares']:,} at {e['p_lo']:.3f}–{e['p_hi']:.3f}, fair {e['fair']}, value {fmt(e.get('value'))})"
                         + (f" and {len(evs) - 1} more events" if len(evs) > 1 else ""))
            else:
                L.append(f"- {when} **{a['name']}** {fmt(d, True)}: no big event within ±10 min (trading we can't see)")
    if not n_j:
        L.append("- no jumps ≥ 10k within 10 min in the window")
    # counterparty watch (afternoon §4.2)
    cw = counterparty_watch(ser, events, tape_from)
    L += ["", "## Counterparty watch (dumps ±10 min; inferred)", "",
          f"Dump catchers: P&L jumps ≥ 5k within ±10 min of a dump; possible dumpers: P&L drops ≥ 5k or volume spikes ≥ half the "
          f"dump's cost at the same time. Running tally in `data/intel/counterparties.json`.", "",
          "| account | role | events | Δ P&L at those times | when (PT) |", "|---|---|---:|---:|---|"]
    for name, r in sorted(cw["accounts"].items(), key=lambda kv: -abs(kv[1]["value"])):
        L.append(f"| {name} | {r['role']} | {r['n']} | {fmt(r['value'], True)} | {', '.join(r['when'][:4])} |")
    if not cw["accounts"]:
        L.append("| – | no dump inside the live tape's coverage yet | | | |")
    if cw["hours"]:
        L += ["", "Dumps by hour of day (PT): " + " · ".join(f"{h}h {n}" for h, n in sorted(cw["hours"].items()))]
    # thresholds
    th = thresholds(rows)
    L += ["", "## Thresholds over time (P&L of #10 / #25 / #50 / #100, our distance)", "",
          "| time | #10 | #25 | #50 | #100 | us (rank, P&L) | to #100 | to #10 |", "|---|---:|---:|---:|---:|---|---:|---:|"]
    seen_h = set()
    for t, s in th:
        h = t[:13]
        if h in seen_h and t != th[-1][0]:
            continue
        seen_h.add(h)
        o = s.get("ours")
        L.append(f"| {t[5:16]} | {fmt(s.get(10))} | {fmt(s.get(25))} | {fmt(s.get(50))} | {fmt(s.get(100))} | "
                 f"{'–' if not o else f'#{o[0]} {o[1]:+,.0f}'} | {'–' if not o or s.get(100) is None else fmt(s[100] - o[1], True)} | "
                 f"{'–' if not o or s.get(10) is None else fmt(s[10] - o[1], True)} |")
    # copy list
    L += ["", "## Copy list (at most 5; each needs the operator's approval unless it is tier membership or a queue step-up)", ""]
    L += [f"{i}. {c}" for i, c in enumerate(copy_list(acct, events, our_layers, tape_from, t0), 1)] or ["- nothing with enough evidence today"]
    # public profiles
    L += ["", "## Public profiles (for Piero to check by hand)", "",
          "`/api/v1` has no endpoint that says whether a profile's trade history is public (the only per-user endpoint is "
          "under `/dmm`, which we never call), and we don't scrape the website. The top 50 by P&L, to look up by hand:", ""]
    L.append(", ".join(f"{a['name']}" for a in acct if not a["ours"] and a["rank"] <= 50))
    return "\n".join(L) + "\n", acct


def counterparty_watch(ser, events, tape_from=None, jump=5_000.0, window=LINK_WINDOW_S):
    """Accounts whose P&L jumped (catchers) or dropped / whose volume spiked (possible dumpers) around big events."""
    acc, hours = {}, collections.Counter()
    dumps = [e for e in events if e.get("gainer_side") and (e.get("value") or 0) >= BIG_VALUE]
    for e in dumps:
        hours[dt.datetime.fromtimestamp(e["t"]).strftime("%H")] += 1
        for p, s in ser.items():
            for a, b in zip(s, s[1:]):
                if not (a[0] - window <= e["t"] <= b[0] + window) or b[0] - a[0] > window + 600:
                    continue
                d_pnl, d_vol, name = b[3] - a[3], b[5] - a[5], b[2]
                role = "catcher" if d_pnl >= jump else "possible dumper" if d_pnl <= -jump or d_vol >= 0.5 * e.get("cost", 0) else None
                if role:
                    r = acc.setdefault(name, dict(role=role, n=0, value=0.0, when=[]))
                    r["n"] += 1
                    r["value"] += d_pnl
                    r["when"].append(dt.datetime.fromtimestamp(e["t"]).strftime("%m-%d %H:%M") + f" {e['market']}")
                    if role != r["role"]:
                        r["role"] = "both"
    return dict(accounts=acc, hours=dict(hours))


def dump_path_stats(rows):
    """Afternoon §4.1: from tracked dumps (data/intel/dumps.jsonl: pre, extreme and the SIG mid path for 2 h, in the gaining
    side's terms) -> n, P(revert >= 50% within 1 h), median minutes to a 50% revert, median revert after 2 h."""
    rev50, t50, r2 = 0, [], []
    for r in rows:
        span = (r["pre"] - r["extreme"]) or 1e-9
        hit = next((m for m, v in r["path"] if (v - r["extreme"]) / span >= 0.5), None)
        if hit is not None and hit <= 60:
            rev50 += 1
        if hit is not None:
            t50.append(hit)
        if r["path"]:
            r2.append((r["path"][-1][1] - r["extreme"]) / span)
    n = len(rows)
    return dict(n=n, p_revert50_1h=rev50 / n if n else None, median_min_to_revert50=statistics.median(t50) if t50 else None,
                median_revert_2h=statistics.median(r2) if r2 else None)


def dump_params(stats, base_offsets=(0.07, 0.12, 0.20)):
    """Daily dump-catcher settings from the tracked dumps (applied by the engine only with >= 5 dumps): the rebound
    target as a share of the way back to the pre-dump level (median 2 h revert, clipped 0.3-1.0); deeper levels when
    dumps rarely revert within the hour."""
    if not stats.get("n") or stats["n"] < 5:
        return None
    share = max(0.3, min(1.0, stats.get("median_revert_2h") or 1.0))
    deeper = 0.02 if (stats.get("p_revert50_1h") or 0) < 0.5 else 0.0
    return dict(n=stats["n"], rebound_share=round(share, 2), offsets=[round(o + deeper, 3) for o in base_offsets])


def copy_list(acct, events, our_layers, tape_from=None, t0=None):
    """<= 5 concrete recommendations, each with its evidence (rules, not judgement)."""
    out = []
    others = [a for a in acct if not a["ours"]]
    by = collections.defaultdict(list)
    for a in others:
        by[a["style"]].append(a)
    mm = by.get("High-volume market maker", [])
    if mm:
        d = sum(a["d_pnl"] for a in mm)
        vol = sum(a["d_volume"] for a in mm)
        if d > 0 and vol > 0:
            ours = our_layers.get("mm")
            out.append(f"**Market-maker size.** High-volume MMs made {fmt(d, True)} on {fmt(vol)} of new volume in the window "
                       f"({d / vol:.1%} of volume; {', '.join(a['name'] for a in mm[:3])}). Our MM layer: {fmt(ours, True)} today. "
                       f"If ours is positive, raise MM size on the markets with the most value at a wrong price (see the daily report).")
    harv = by.get("Opening harvester", [])
    ev_v = sum(e.get("value") or 0 for e in events)
    if events:
        top = sorted(events, key=lambda e: -(e.get("value") or 0))[:3]
        # how deep each sweep went, in the gaining side's terms: our fat-finger levels sit at <= 0.12
        deep = lambda e: e["p_lo"] if e.get("gainer_side") == "yes" else 1 - e["p_hi"]
        shallow = [e for e in events if e.get("gainer_side") and e.get("fair") is not None and deep(e) > 0.12]
        sv = sum(e.get("value") or 0 for e in shallow)
        out.append(f"**Fat-finger levels.** {len(events)} big events moved {fmt(ev_v)} at a wrong price in the window "
                   f"(largest: {'; '.join(e['market'] + ' ' + fmt(e.get('value')) for e in top)}). Tier membership follows them "
                   f"automatically.")
        if sv > 0.5 * ev_v:
            ex_ = max(shallow, key=lambda e: e.get("value") or 0)
            sf = ex_["fair"] if ex_["gainer_side"] == "yes" else 1 - ex_["fair"]
            out.append(f"**Shallow sweeps.** {fmt(sv)} of it ({sv / ev_v:.0%}) came from sweeps that stopped above 0.12, out of "
                       f"reach of the fat-finger levels: e.g. {ex_['market']} {ex_['gainer_side'].upper()} swept to {deep(ex_):.3f} "
                       f"with fair {sf:.3f}. Consider resting bids 7-20c under fair on favourites (fair >= 0.95), sized so a full "
                       f"fill fits the opportunity reserve; ladders can't rest there while the reserve takes all spare cash. "
                       f"New level definitions need approval.")
    elif harv:
        d = sum(a["d_pnl"] for a in harv)
        if tape_from and t0 and tape_from <= t0:
            out.append(f"**Opening harvesters** made {fmt(d, True)} in the window with no big event on the tape: their gains come "
                       f"from marks, not new fat-fingers. Keep the fat-finger levels for new markets and election night; don't add size.")
        else:
            out.append(f"**Opening harvesters** made {fmt(d, True)} in the window. The live tape doesn't cover the whole window yet, "
                       f"so we can't tell new fat-fingers from marks; keep the fat-finger levels as they are.")
    hf = by.get("High-frequency bot", [])
    if hf:
        d = sum(a["d_pnl"] for a in hf)
        tr = sum(a["d_trades"] for a in hf)
        if tr > 0:
            out.append(f"**High-frequency bots** made {fmt(d, True)} on {fmt(tr)} trades ({d / tr:+,.1f} per trade). "
                       + ("Per-trade edge is thin: no change to our quote refresh." if d / tr < 5 else
                          "Edge per trade is real: test a faster requote on tier-A markets (needs approval)."))
    dr = by.get("Directional", [])
    if dr:
        d = sorted(dr, key=lambda a: -a["d_pnl"])[:3]
        out.append(f"**Directional** leaders' best: {', '.join(a['name'] + ' ' + fmt(a['d_pnl'], True) for a in d)}. "
                   f"Their markets aren't visible to us; nothing to copy without the tape link (see attribution).")
    return out[:5]


# ------------------------------------------------------------------------------------------------ daily report
def dist_bin(d):
    return "< 2c" if d < 0.02 else "2-5c" if d < 0.05 else "5-10c" if d < 0.10 else "10-25c" if d < 0.25 else "25-40c" if d < 0.40 else ">= 40c"


BINS = ["< 2c", "2-5c", "5-10c", "10-25c", "25-40c", ">= 40c"]


def daily_report(date, trades, events, fills, scoreboard, coverage, new_markets, recs, leaders_link, names=None, tape_from=None):
    """Markdown for docs/daily/<date>.md (morning_plan §4.5, sections in its order).
    trades: [{ex, ts, p, q, fair}] of the last 24 h (fair may be None); events: detect() output with captured/missed
    filled in; fills: our ledger fills of the window; scoreboard: {account_fair, account_sig, rank, n, thresholds{}};
    coverage: {pct, uncovered_minutes, samples}; new_markets: [{market, seen, covered_s}]; recs: [str]."""
    names = names or {}
    L = [f"# Daily report {date}", "", "Previous 24 h. Value at a wrong price = Σ |price − Polymarket fair| × shares. "
         "The tape's side is not the aggressor, so no maker/taker split."
         + (f" **The live tape covers only since {dt.datetime.fromtimestamp(tape_from).strftime('%m-%d %H:%M')} PT**; a print gets "
            "a fair only once the engine has sampled that market (30 s after each start)." if tape_from else
            " **No live tape yet.**"), ""]
    s = scoreboard or {}
    th = s.get("thresholds") or {}
    L += ["## 1. Scoreboard", "",
          f"- Account at SIG marks **{fmt(s.get('account_sig'))}** (the leaderboard number); at Polymarket fair {fmt(s.get('account_fair'))}",
          f"- Rank **#{s.get('rank', '–')}** of {s.get('n', '–')}" + (f"; to #10: {fmt(th.get('10') - s['pnl'], True)}" if th.get("10") is not None and s.get("pnl") is not None else ""),
          "- Thresholds: " + " · ".join(f"#{k} {fmt(th.get(k))}" for k in ("10", "25", "50", "100")), ""]
    with_f = [t for t in trades if t.get("fair") is not None]
    tot = sum(abs(t["p"] - t["fair"]) * t["q"] for t in with_f)
    by_bin = collections.Counter()
    by_mkt = collections.Counter()
    by_hour = collections.Counter()
    for t in with_f:
        v = abs(t["p"] - t["fair"]) * t["q"]
        by_bin[dist_bin(abs(t["p"] - t["fair"]))] += v
        by_mkt[t["ex"]] += v
        by_hour[dt.datetime.fromtimestamp(t["ts"]).strftime("%H")] += v
    L += ["## 2. Where the money went", "",
          f"{len(trades):,} prints read, {len(with_f):,} with a Polymarket fair: **{fmt(tot)}** SIG changed hands at a wrong price.", "",
          "| distance from fair | value | share |", "|---|---:|---:|"]
    L += [f"| {b} | {fmt(by_bin[b])} | {by_bin[b] / tot:.0%} |" for b in BINS if tot]
    L += ["", "Top markets: " + "; ".join(f"{names.get(ex, ex)} {fmt(v)}" for ex, v in by_mkt.most_common(10)), "",
          "By hour (PT): " + " · ".join(f"{h}h {fmt(v)}" for h, v in sorted(by_hour.items())), "",
          "### Top 20 events", "",
          "| time | market | shares | prices | fair | value | kind | we caught | missed |", "|---|---|---:|---|---:|---:|---|---:|---:|"]
    for e in sorted(events, key=lambda e: -(e.get("value") or e.get("cost") or 0))[:20]:
        L.append(f"| {dt.datetime.fromtimestamp(e['t']).strftime('%m-%d %H:%M:%S')} | {e['market']} | {e['shares']:,} | "
                 f"{e['p_lo']:.3f}–{e['p_hi']:.3f} | {e['fair'] if e['fair'] is not None else '–'} | {fmt(e.get('value'))} | "
                 f"{', '.join(e['kinds'])} | {fmt(e.get('captured_value'))} | {fmt(e.get('missed_value'))} |")
    if not events:
        L.append("| – | no big events | | | | | | | |")
    # captured vs missed
    cap_l = collections.Counter()
    for f in fills:
        if f.get("fair") is not None and f.get("action") == "buy":
            cap_l[f.get("layer") or "other (before the engine / manual)"] += (f["fair"] - f["price"]) * f["qty"]
    by_kind = collections.defaultdict(lambda: [0.0, 0.0])
    for e in events:
        k = "Max sweep" if "Max sweep" in e["kinds"] else e["kinds"][0]
        by_kind[k][0] += e.get("captured_value") or 0
        by_kind[k][1] += e.get("missed_value") or 0
    L += ["", "## 3. What we captured vs missed", "", "Edge at fill (fair − price) × shares, our buys, by layer:", "",
          "| layer | edge captured |", "|---|---:|"]
    L += [f"| {k} | {fmt(v, True)} |" for k, v in cap_l.most_common()] or ["| – | – |"]
    L += ["", "Big events, by type:", "", "| type | caught | missed (levels we'd have had resting) |", "|---|---:|---:|"]
    L += [f"| {k} | {fmt(v[0])} | {fmt(v[1])} |" for k, v in by_kind.items()] or ["| – | – | – |"]
    miss = sorted((e for e in events if (e.get("missed_value") or 0) > 0), key=lambda e: -e["missed_value"])
    if miss:
        L += ["", f"Missed {fmt(sum(e['missed_value'] for e in miss))}: " + "; ".join(
            f"{e['market']} {fmt(e['missed_value'])} ({e.get('missed_why') or 'no level resting'})" for e in miss[:8])]
    L += ["", "## 4. Leaders", "", f"See [{leaders_link}]({leaders_link}): styles, what's working now, attribution, copy list.", ""]
    c = coverage or {}
    L += ["## 5. Coverage", "",
          f"- Coverage now **{c.get('pct', '–')}%** of eligible fat-finger sides; uncovered side-minutes in the window: "
          f"{fmt(c.get('uncovered_minutes'))} (samples: {c.get('samples', 0)})",
          f"- New markets: {len(new_markets)}" + ("" if not new_markets else ": " + "; ".join(
              f"{m['market']} covered in {m['covered_s']:.0f} s" if m.get("covered_s") is not None else f"{m['market']} not covered"
              for m in new_markets)), ""]
    L += ["## 6. Recommendations", "",
          "Only fat-finger tier membership and queue step-ups (within the caps) are applied automatically; everything "
          "else waits for Piero. Applied changes are logged in `data/tuning_log.json`.", ""]
    L += [f"{i}. {r}" for i, r in enumerate(recs, 1)] or ["- none today"]
    return "\n".join(L) + "\n"


def tiers_from(events, trades, headline, n_value=25, n_traded=15, prev=()):
    """Fat-finger tier A (morning_plan 1.4, recomputed daily): the n_value markets with the most value at a wrong price,
    the n_traded most traded (shares) in the window, and the headline races. -> sorted list of exchange ids."""
    val, traded = collections.Counter(), collections.Counter()
    for t in trades:
        traded[t["ex"]] += t["q"]
        if t.get("fair") is not None:
            val[t["ex"]] += abs(t["p"] - t["fair"]) * t["q"]
    for e in events:
        val[e["ex"]] += e.get("value") or 0
    out = [ex for ex, _ in val.most_common(n_value)]
    out += [ex for ex, _ in traded.most_common() if ex not in out][:n_traded]
    out += [ex for ex in headline if ex not in out]
    if len(trades) < THIN_TAPE:                         # a thin window (a partial day of tape) can't demote anyone
        out += [ex for ex in prev if ex not in out]
    return sorted(set(out))


# ------------------------------------------------------------------------------------------------ tape collector
class TapeCollector:
    """Incremental, resumable tape reader (morning_plan 4.1). Per exchange it keeps the newest trade id seen and its
    time in `state_path`. A visit reads /exchanges/{ex}/trades from that time (newest first, 200 a page) and pages back
    only until it reaches a known id: a quiet market costs one read. A market never seen starts `first_window_s` back.
    get(path, params) -> the JSON page (the caller throttles: the engine's spare-read governor or k_tape_study's)."""

    def __init__(self, get, tournament_id, state_path, first_window_s=3600, max_pages=10, clock=None):
        import time as _t
        self.get, self.tid, self.path, self.first, self.max_pages = get, tournament_id, state_path, first_window_s, max_pages
        self.clock = clock or _t.time
        self.reads = 0
        try:
            with open(state_path) as fh:
                self.state = json.load(fh)
        except (OSError, ValueError):
            self.state = {}

    def save(self):
        tmp = self.path + ".tmp"
        with open(tmp, "w") as fh:
            json.dump(self.state, fh)
        os.replace(tmp, self.path)

    def visit(self, ex):
        """New prints of `ex`, oldest first: [{id, ex, ts, p, q}]."""
        ex = str(ex)
        st = self.state.get(ex)
        last = int(st["id"]) if st else None
        since = st["t"] if st else dt.datetime.fromtimestamp(self.clock() - self.first, dt.timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.000Z")
        rows, cursor = [], None
        for _ in range(self.max_pages):
            p = {"tournamentId": self.tid, "limit": 200, "from": since}
            if cursor:
                p["cursor"] = cursor
            d = self.get(f"/exchanges/{ex}/trades", p)
            self.reads += 1
            page = d.get("data") or []
            rows += page
            known = last is not None and any(int(x["id"]) <= last for x in page if str(x.get("id", "")).isdigit())
            if known or not (d.get("pagination") or {}).get("hasMore"):
                break
            cursor = d["pagination"]["nextCursor"]
        new = [dict(id=str(x["id"]), ex=ex, ts=_iso(x["createdAt"]), p=float(x["price"]), q=float(x["size"]))
               for x in rows if x.get("price") is not None and str(x.get("id", "")).isdigit() and (last is None or int(x["id"]) > last)]
        new = order({r["id"]: r for r in new}.values())
        if new:
            top = max(new, key=lambda r: int(r["id"]))
            raw = next(x for x in rows if str(x["id"]) == top["id"])
            self.state[ex] = dict(id=top["id"], t=raw["createdAt"])
            self.save()
        elif not st:
            self.state[ex] = dict(id="0", t=since)
            self.save()
        return new


def _iso(s):
    return dt.datetime.fromisoformat(s.replace("Z", "+00:00")).timestamp()


# ------------------------------------------------------------------------------------------------ daily job
def layers_today(data):
    """{layer: P&L today so far (realized + unrealized change, SIG marks)} from data/intel/layers_today.json, which the
    engine refreshes every minute; {} when it is missing or from another day."""
    try:
        d = json.load(open(os.path.join(data, "intel", "layers_today.json")))
    except (OSError, ValueError):
        return {}
    return d.get("layers") or {} if d.get("date") == dt.datetime.now().strftime("%Y-%m-%d") else {}


def _tape_from(idir):
    """When the live tape collector's coverage starts: its earliest saved print (None without one)."""
    try:
        first = sorted(f for f in os.listdir(os.path.join(idir, "trades")) if f.endswith(".jsonl"))[0]
        with open(os.path.join(idir, "trades", first)) as fh:
            return min(json.loads(l)["ts"] for l in fh if l.strip())
    except (OSError, IndexError, ValueError):
        return None


def _jsonl(path, since=None, key="t"):
    out = []
    try:
        with open(path) as fh:
            for line in fh:
                try:
                    r = json.loads(line)
                except ValueError:
                    continue
                if since is None or (r.get(key) or 0) >= since:
                    out.append(r)
    except OSError:
        pass
    return out


def run_daily(data, docs, now, apply_tiers=True, day_s=86400):
    """Daily report + leaders study + tier A refresh from local files only (no SIG reads): the engine at 07:00 PT and
    `python3 k_tape_study.py --daily` on demand. -> (daily path, leaders path, tier changes)."""
    import csv
    since = now - day_s
    date = dt.datetime.fromtimestamp(now).strftime("%Y-%m-%d")
    idir = os.path.join(data, "intel")
    meta = {}
    try:
        meta = json.load(open(os.path.join(idir, "names.json")))
    except (OSError, ValueError):
        pass
    names, headline = meta.get("names", {}), meta.get("headline", [])
    trades = []
    for d in sorted({dt.datetime.fromtimestamp(x).strftime("%Y-%m-%d") for x in (since, now)}):
        trades += _jsonl(os.path.join(idir, "trades", f"{d}.jsonl"), since, key="ts")
    events = _jsonl(os.path.join(idir, "events.jsonl"), since)
    try:
        fills = [f for f in json.load(open(os.path.join(data, "ledger.json"))).get("fills", [])
                 if f.get("t") and _ts(f["t"]) >= since]
    except (OSError, ValueError):
        fills = []
    try:
        rows = list(csv.DictReader(open(os.path.join(data, "leaders.csv"))))
    except OSError:
        rows = []
    sb = {}
    try:
        last = list(csv.DictReader(open(os.path.join(data, "pnl.csv"))))[-1]
        sb.update(account_sig=float(last["account_value"]), account_fair=float(last["account_pm"] or 0) or None)
        sb["pnl"] = sb["account_sig"] - 100_000
    except (OSError, IndexError, ValueError, KeyError):
        pass
    try:
        lm = json.load(open(os.path.join(data, "leaders_meta.json")))
        sb.update(rank=lm.get("my_rank"), n=lm.get("total"))
    except (OSError, ValueError):
        pass
    th = thresholds(rows)
    if th:
        sb["thresholds"] = {str(k): v for k, v in th[-1][1].items() if k != "ours"}
    cov = _jsonl(os.path.join(idir, "coverage.jsonl"), since)
    coverage = dict(pct=cov[-1]["pct"] if cov else None, samples=len(cov),
                    uncovered_minutes=sum(c.get("uncovered", 0) * min(10.0, (b["t"] - c["t"]) / 60) for c, b in zip(cov, cov[1:])))
    new_m = _jsonl(os.path.join(idir, "new_markets.jsonl"), since)
    our_layers = layers_today(data)
    os.makedirs(os.path.join(docs, "leaders"), exist_ok=True)
    os.makedirs(os.path.join(docs, "daily"), exist_ok=True)
    tape_from = _tape_from(idir)
    lmd, acct = leaders_study(rows, events, our_layers, now, tape_from=tape_from)
    lpath = os.path.join(docs, "leaders", f"{date}.md")
    with open(lpath, "w") as fh:
        fh.write(lmd)
    # tier A: recomputed from the window, applied automatically (morning_plan 4.5), logged
    tpath = os.path.join(idir, "tiers.json")
    try:
        old = json.load(open(tpath))
    except (OSError, ValueError):
        old = {}
    prev = old.get("tier_a") or []
    new = tiers_from(events, trades, headline, prev=prev) if trades else prev
    add, drop = sorted(set(new) - set(prev)), sorted(set(prev) - set(new))
    recs = []
    if add or drop:
        recs.append(f"**Applied: fat-finger tier A** {len(prev)} → {len(new)} markets. Added: "
                    f"{', '.join(names.get(x, x) for x in add) or 'none'}. Dropped: {', '.join(names.get(x, x) for x in drop) or 'none'} "
                    f"(the 25 markets with the most value at a wrong price in the window, the 15 most traded, the headline races).")
        if apply_tiers:
            json.dump(dict(t=dt.datetime.fromtimestamp(now).isoformat(), tier_a=new, method="daily: l_intel.tiers_from over the "
                           "last 24 h (value at a wrong price, most traded, headline races)"), open(tpath, "w"), indent=1)
            log_path = os.path.join(data, "tuning_log.json")
            try:
                log = json.load(open(log_path))
            except (OSError, ValueError):
                log = []
            log.append(dict(t=dt.datetime.fromtimestamp(now).isoformat(), change="fatfinger tier A", added=add, dropped=drop, auto=True))
            json.dump(log, open(log_path, "w"), indent=1)
    miss = collections.Counter()
    for e in events:
        miss[e["ex"]] += e.get("missed_value") or 0
    m_top = [(ex, v) for ex, v in miss.most_common(5) if v > 0]
    if m_top:
        recs.append("**Missed value by market** (levels we'd have had resting): " + "; ".join(f"{names.get(ex, ex)} {fmt(v)}" for ex, v in m_top)
                    + ". These are in tier A from today; size changes need approval.")
    recs += [f"(needs approval) {c}" for c in copy_list(acct, events, our_layers, tape_from, since)[:3]]
    # afternoon §4.1: dump stats -> tomorrow's dump settings (applied with >= 5 tracked dumps); §4.2 running counterparty tally
    dstats = dump_path_stats(_jsonl(os.path.join(idir, "dumps.jsonl")))
    dp = dump_params(dstats)
    if dp and apply_tiers:
        json.dump(dict(dp, t=dt.datetime.fromtimestamp(now).isoformat(), stats=dstats), open(os.path.join(idir, "dump_params.json"), "w"), indent=1)
    recs.insert(0, f"**Dumps tracked: {dstats['n']}.** P(revert ≥ 50% within 1 h) {fmt_pct(dstats['p_revert50_1h'])}, median time to a 50% "
                   f"revert {fmt(dstats['median_min_to_revert50'])} min, median revert after 2 h {fmt_pct(dstats['median_revert_2h'])}. "
                   + (f"Applied to the dump catcher: offsets {dp['offsets']}, rebound at {dp['rebound_share']:.0%} of the way back." if dp
                      else "Fewer than 5 dumps: the dump catcher keeps the plan's settings (0.07/0.12/0.20, rebound to the pre-dump level)."))
    try:
        cw = counterparty_watch(leader_series(rows), events, tape_from)
        tally_p = os.path.join(idir, "counterparties.json")
        tally = json.load(open(tally_p)) if os.path.exists(tally_p) else {}
        tally[date] = cw
        json.dump(tally, open(tally_p, "w"), indent=1)
    except (OSError, ValueError):
        pass
    md = daily_report(date, trades, events, fills, sb, coverage, new_m, recs, f"../leaders/{date}.md", names, tape_from)
    dpath = os.path.join(docs, "daily", f"{date}.md")
    with open(dpath, "w") as fh:
        fh.write(md)
    return dpath, lpath, (add, drop)
