"""M. DASHBOARD v4 payload (docs/dashboard_v4.md; day 5 §4 attribution). Pure functions over the engine's state; the engine
calls v4_view(eng, now) once per loop and the page reads only this.

  One clock: "today" is SIG's day, from 17:00 PT (00:00 UTC). Today's P&L = account now - account at 17:00 PT (pnl.csv),
  = sum of strategy P&L (fills since 17:00, valued at current SIG marks) + market drift on holdings held at 17:00. The
  drift is measured independently (SIG's account-level unrealized change minus the unrealized part of the new fills), so
  the check line "adds up" is a real check; a gap over 25 is flagged on Admin.
"""
import collections
import csv
import json
import os
import time

SIG_DAY_HOUR = 17            # PT hour at which SIG's day starts (00:00 UTC)
PARTY_CAP = 40_000           # day 5 §1.5: held dump inventory at cost per party side


def sig_day_start(now=None):
    """Epoch of the most recent 17:00 PT (local time is PT on this machine)."""
    now = time.time() if now is None else now
    lt = time.localtime(now)
    start = time.mktime((lt.tm_year, lt.tm_mon, lt.tm_mday, SIG_DAY_HOUR, 0, 0, 0, 0, -1))
    return start if start <= now else start - 86400


def sig_day_label(start):
    """Day 6 §5.3: a SIG day is labelled by its UTC date: the day that starts Oct 4 17:00 PT (= Oct 5 00:00 UTC) is 'Oct 5'."""
    return time.strftime("%b %-d", time.gmtime(start + 60))


def pnl_at(pts, t):
    """The pnl.csv row at or just after t (the first sample of the SIG day), else the last one before it."""
    after = [p for p in pts if p["t"] >= t]
    if after:
        return after[0]
    before = [p for p in pts if p["t"] < t]
    return before[-1] if before else None


def fill_ts(f):
    return time.mktime(time.strptime(f["t"], "%Y-%m-%d %H:%M:%S"))


def strategy_pnl(eng, fills):
    """{strategy: (pnl at current SIG marks, realized, unrealized part, trades, volume)} for fills, like Engine.layer_pnl."""
    out = {}
    for f in fills:
        l = eng.strategy_of(f["bot"], f["layer"])
        if l == "old engine":
            continue
        r = out.setdefault(l, dict(pnl=0.0, realized=0.0, unreal=0.0, trades=0, volume=0.0))
        sgn = 1 if f["action"] == "buy" else -1 if f["action"] == "sell" else 0
        m = eng.sig_mark(f["ex"], f["side"])
        pnl = sgn * ((m if m is not None else f["price"]) - f["price"]) * f["qty"]
        r["pnl"] += pnl
        r["realized"] += f.get("realized") or 0
        r["unreal"] += pnl - (f.get("realized") or 0)
        r["trades"] += 1
        r["volume"] += f["qty"] * f["price"]
    return out


def snapshot_path(eng, start):
    return os.path.join(eng.DATA, "sigday", time.strftime("%Y-%m-%d", time.localtime(start)) + ".json")


def take_snapshot(eng, now):
    """The SIG-day start snapshot (first loop at or after 17:00 PT; `late` when the engine was down then): every position
    (net YES) with SIG's own mark, the account value and cash. Market drift = Σ q0 x (mark_now - mark_0) over it."""
    start = sig_day_start(now)
    path = snapshot_path(eng, start)
    if os.path.exists(path) or not eng.book or not eng.acct:
        return None
    os.makedirs(os.path.dirname(path), exist_ok=True)
    pos = {ex: [b.get("pos", 0), eng.sig_mark(ex, "yes")] for ex, b in eng.book.items() if abs(b.get("pos", 0)) >= 1}
    snap = dict(start=start, t=now, late=now - start > 300, account=eng.acct.get("totalAccountValue"),
                cash=eng.acct.get("totalAccountValue", 0) - (eng.acct.get("totalHoldingsValue") or 0), positions=pos)
    tmp = path + ".tmp"
    with open(tmp, "w") as fh:
        json.dump(snap, fh)
    os.replace(tmp, path)
    return snap


def drift_estimate(eng, snap):
    """Σ q0 x (SIG mark now - SIG mark at the snapshot) over the positions held then: the independent market drift."""
    if not snap:
        return None
    out = 0.0
    for ex, (q0, m0) in (snap.get("positions") or {}).items():
        m = eng.sig_mark(ex, "yes")
        if m is None or m0 is None:
            continue
        out += q0 * (m - m0)
    return out


def sigday_view(eng, now, pts):
    """Today in SIG-day terms: today = Δ account since 17:00 PT; Σ strategies = fills since then at current SIG marks;
    market drift = the rest (exact: Δ account = Σ fills at marks + Σ q0 (mark_now - mark_0)); the check on Admin compares
    that drift with the independent estimate from the 17:00 position snapshot (within 25), when the snapshot was on time."""
    start = sig_day_start(now)
    a = eng.acct or {}
    acct = a.get("totalAccountValue")
    p0 = pnl_at(pts, start)
    snap = eng.read_json(snapshot_path(eng, start), None)
    fills = [f for f in eng.ledger.fills if not f["before_engine"] and fill_ts(f) >= start]
    strat = strategy_pnl(eng, fills)
    s_sum = sum(v["pnl"] for v in strat.values())
    a0 = snap["account"] if snap and not snap.get("late") and snap.get("account") is not None else (None if p0 is None else p0["sig"])
    today = None if acct is None or a0 is None else acct - a0
    drift = None if today is None else today - s_sum
    est = drift_estimate(eng, snap) if snap and not snap.get("late") else None
    check = None if drift is None or est is None else drift - est
    note = ("" if check is not None else "no 17:00 PT snapshot yet: the check runs from the next SIG day" if not snap
            else f"snapshot taken late ({time.strftime('%H:%M', time.localtime(snap['t']))}): the check runs from the next SIG day")
    return dict(since=start, since_str=time.strftime("%Y-%m-%d %H:%M", time.localtime(start)), label=sig_day_label(start),
                account_start=None if a0 is None else round(a0, 2), today=None if today is None else round(today, 2),
                our_trades=round(s_sum, 2), drift=None if drift is None else round(drift, 2),
                drift_estimate=None if est is None else round(est, 2),
                check=None if check is None else round(check, 2), ok=check is None or abs(check) <= 25, note=note,
                strategies={k: dict(pnl=round(v["pnl"], 2), realized=round(v["realized"], 2), trades=v["trades"], volume=round(v["volume"], 2))
                            for k, v in strat.items()},
                fair_account=None if not eng.book else round(eng.account_fair()[0], 2))


def daily_view(eng, now, pts):
    """One row per SIG day since Oct 1: Δ account, strategy P&L (fills in the window at current marks) and drift = total − Σ."""
    rows = []
    first = pts[0]["t"] if pts else now
    start = sig_day_start(first)
    while start <= now:
        end = start + 86400
        p0, p1 = pnl_at(pts, start), None
        inwin = [p for p in pts if start <= p["t"] < end]
        if inwin:
            p1 = inwin[-1]
        snap = eng.read_json(snapshot_path(eng, start), None)
        a0 = snap["account"] if snap and not snap.get("late") and snap.get("account") is not None else (None if p0 is None else p0["sig"])
        if a0 is not None and p1 is not None:
            fills = [f for f in eng.ledger.fills if not f["before_engine"] and start <= fill_ts(f) < end]
            strat = strategy_pnl(eng, fills)
            tot = p1["sig"] - a0 if end <= now else ((eng.acct or {}).get("totalAccountValue") or p1["sig"]) - a0
            s_sum = sum(v["pnl"] for v in strat.values())
            rows.append(dict(start=start, label=sig_day_label(start), partial=end > now, total=round(tot, 2),
                             strategies={k: round(v["pnl"], 2) for k, v in strat.items()}, drift=round(tot - s_sum, 2)))
        start = end
    return rows[-14:]


def status_view(eng, now):
    c = eng.controls
    p = eng.pause_view()
    rel = os.path.basename(os.path.realpath(os.path.join(os.path.dirname(eng.DATA), "run"))) if hasattr(eng, "DATA") else ""
    h = eng.api.health(600)
    thr = h.get("throttled", 0)
    loop_age = now - (eng.loop_end_t or now)
    if p:
        kind = "PAUSED"
        text = f"{p['reason']} · since {p['since']} · " + ("resumes by itself when the condition clears" if p["auto"] and not p["stuck"]
                                                           else "needs Piero" if p["stuck"] else "manual: resume from Admin")
    elif thr >= 6 or (h.get("read") or {}).get("bad_rate", 0) > 0.1 or loop_age > 120:
        kind = "DEGRADED"
        text = f"SIG slow: {thr} × 429 in 10 min" if thr >= 6 else f"last loop {loop_age:.0f} s ago" if loop_age > 120 else "SIG reads failing"
    else:
        kind = "TRADING"
        text = f"last loop {loop_age:.0f} s ago · {len(eng.open)} orders resting · {rel}"
    return dict(kind=kind, text=text, release=rel, loop_age=round(loop_age, 1), resting=len(eng.open), throttled=thr)


def feed_view(eng, limit=60):
    """The activity feed: trades (fills as sentences), alerts, market events, newest first, each with a chip."""
    out = []
    n_f = len(eng.ledger.fills)
    for i, f in enumerate(eng.ledger.fills[-200:]):
        if f["before_engine"]:
            continue
        out.append(dict(id=f"f{n_f - 200 + i if n_f > 200 else i}", t=f["t"], kind="trade", layer=eng.strategy_of(f["bot"], f["layer"]),
                        text=eng.fill_sentence(f)))
    for e in eng.events[-300:]:
        k = e.get("kind")
        if k in ("EVENT",):
            out.append(dict(id=f"e{e.get('id', e['t'])}", t=e["t"], kind="market", layer="", text=e["text"]))
        elif k in ("PAUSE", "STOP", "CASHSHORT", "BLIND", "BREAKER", "NEWS", "RATCHET", "CONTROL", "WALL", "SLEEVE") or (k == "DUMP" and "BREAKER" in e["text"]):
            out.append(dict(id=f"e{e.get('id', e['t'])}", t=e["t"], kind="alert", layer="", text=e["text"]))
    out.sort(key=lambda r: str(r["t"]), reverse=True)
    return out[:limit]


def strategies_v4(eng, now, sigday, since_oct1_fills):
    c = eng.controls
    start = sigday["since"]
    all_strat = strategy_pnl(eng, since_oct1_fills)
    cap = {b["key"]: b["sig"] for b in (getattr(eng, "last_capital", None) or {}).get("buckets", [])}
    resting = collections.Counter()
    for o in eng.open:
        rec = eng.ledger.orders.get(o["id"])
        if rec:
            resting[eng.strategy_of(rec["bot"], rec["layer"])] += 1
    last = collections.defaultdict(list)
    for f in reversed(eng.ledger.fills[-800:]):
        l = eng.strategy_of(f["bot"], f["layer"])
        if len(last[l]) < 10:
            last[l].append(dict(t=f["t"], text=eng.fill_sentence(f)))
    hold = bool(c.d.get("hold_gates"))
    rows = []
    for key in ["dump", "sleeve", "wall", "touch", "scalp", "mm", "sniper", "fatfinger", "longshot", "pairs", "recycler", "release", "position"]:
        st = c.state(key)
        v = c.layer(key)
        status = "PAUSED" if c.paused and key not in ("recycler", "release", "position") else "HELD" if (key == "mm" and (v.get("caps") or {}).get("inventory") == 0) \
            else {"full": "LIVE", "pilot": "PILOT", "off": "OFF"}.get(st, st.upper())
        why = v.get("killed", {}).get("reason") if v.get("killed") else ("gate held (hold_gates)" if st == "pilot" and hold and key not in (c.d.get("gate_free") or [])
                                                                        else "inventory cap 0" if status == "HELD" else "")
        t_ = sigday["strategies"].get(key, {})
        rows.append(dict(key=key, label=eng.LABELS.get(key, (key,))[0], what=eng.LABELS.get(key, ("", ""))[1], status=status, why=why,
                         resting=resting.get(key, 0), cash_in_use=round(eng.scalp_cost() if key == "scalp" else eng.touch_cost() if key == "touch" else eng.wall_cost() if key == "wall" else eng.sleeve_cost() if key == "sleeve" else cap.get("manual" if key == "position" else key, 0.0), 2),
                         trades_today=t_.get("trades", 0), pnl_today=t_.get("pnl", 0.0),
                         pnl_since=round(all_strat.get(key, {}).get("pnl", 0.0), 2), last=last.get(key, [])))
    return rows


def party_exposure(eng):
    """Held dump tranches at cost by party side (D favourites / R favourites)."""
    out = {"D": 0.0, "R": 0.0, "other": 0.0}
    for ex, books in eng.ledger.subbooks.items():
        for sub, q in books.items():
            if not (sub.startswith("dump:") or sub.startswith("touch:") or sub.startswith("sleeve:")) or abs(q) < 1:
                continue                                    # day 6 §2: the touch book shares the 40k party cap; day 9: the sleeve too
            side = "yes" if q > 0 else "no"
            t = eng.ledger.tranches.get(sub) or {}
            avg = t.get("avg") or eng.sig_mark(ex, side) or 0.5
            out[eng.pol(ex, side) if eng.pol(ex, side) in out else "other"] += abs(q) * avg
    return {k: round(v, 2) for k, v in out.items()}


def capital_v4(eng, now, capital, holdings=()):
    """One set of numbers: 'holdings' here is the sum of the Holdings page rows (our SIG marks), not SIG's totalHoldingsValue."""
    cash = capital["cash"]
    holdings_sig = round(sum(h.get("value_sig") or 0 for h in holdings), 2)
    free = cash - eng.CASH_RESERVE
    tr = sum(abs(q) * ((eng.ledger.tranches.get(sub) or {}).get("avg") or eng.sig_mark(ex, "yes" if q > 0 else "no") or 0)
             for ex, books in eng.ledger.subbooks.items() for sub, q in books.items() if sub.startswith("dump:") and abs(q) >= 1)
    scalp = eng.scalp_cost()
    takes = sum(abs(q) * (eng.sig_mark(ex, "yes" if q > 0 else "no") or 0) for ex, books in eng.ledger.subbooks.items() for sub, q in books.items() if sub == "sniper" and abs(q) >= 1)
    in_use = tr + scalp + takes
    use = in_use / (in_use + max(0.0, free)) if in_use + max(0.0, free) > 0 else None   # share of deployable cash in use (0-100%)
    eng.in_use_hist.append((round(now), round(in_use, 2)))
    if use is not None:
        eng.use_peak = max(eng.use_peak, use)                # day 7 §1.2: the bounded capital use (the same number as the page)
    eng.cap_use_hist.append((round(now), None if use is None else round(use, 4)))
    hist = [(t, u) for t, u in eng.cap_use_hist if t >= now - 86400]
    party = party_exposure(eng)
    settle = max(0.0, holdings_sig - in_use)
    return dict(cash=cash, free_cash=round(free, 2), holdings=holdings_sig, in_use=round(in_use, 2), tranches=round(tr, 2), scalper=round(scalp, 2), takes=round(takes, 2),
                use=None if use is None else round(use, 4), use_hist=hist[-300:], party=party, party_cap=PARTY_CAP,
                bar=[dict(key="floor", label="Cash floor", sig=eng.CASH_RESERVE), dict(key="reserve", label="Dump reserve", sig=min(max(0.0, free), eng.OPPORTUNITY_RESERVE)),
                     dict(key="free", label="Free for scalper / takes", sig=max(0.0, free - eng.OPPORTUNITY_RESERVE)),
                     dict(key="tranches", label="In tranches", sig=round(in_use, 2)), dict(key="settle", label="Holdings to settlement", sig=round(settle, 2))],
                released=capital.get("released_today"), idle=capital.get("idle"), idle_rule=dict(eng.controls.d.get("idle") or {}), scalp_slice=eng.scalp_slice(),
                cash_target=float((eng.controls.d.get("cash_target") or {}).get("T") or 25_000), rule0_blocked_h=eng.rule0_blocked_value(now - 3600))


def holdings_v4(eng, rows):
    """Holdings rows + entry, exit order, age, expected at settlement."""
    exits = {}
    for o in eng.open:
        p = eng.book.get(o["ex"], {}).get("pos", 0)
        rec = eng.ledger.orders.get(o["id"])
        if p and rec and eng.jx.reduces(o["side"], o["action"], p):
            px = o["price"] if o["action"] == "sell" else round(1 - o["price"], 3)
            exits.setdefault(o["ex"], []).append((px, o["qty"]))
    first = {}
    net = collections.defaultdict(float)
    for f in eng.ledger.fills:
        q = f["qty"] * (1 if (f["side"] == "yes") == (f["action"] == "buy") else -1)
        was = net[f["ex"]]
        net[f["ex"]] = was + q
        if abs(was) < 1 <= abs(net[f["ex"]]) or was * net[f["ex"]] < 0:
            first[f["ex"]] = f["t"]
    out = []
    for h in rows:
        ex = h["ex"]
        xs = exits.get(ex, [])
        age_h = None
        if first.get(ex):
            age_h = round((time.time() - fill_ts(dict(t=first[ex]))) / 3600, 1)
        out.append(dict(h, exit=None if not xs else dict(n=len(xs), px=round(max(p for p, _ in xs), 3), qty=sum(q for _, q in xs)),
                        age_h=age_h, settle=h.get("value_fair")))
    return out


def leaders_v4(eng):
    """Overall top 25 + #50/#100 lines + our row (leaders.csv), the daily board (data/leaders_day.json), last-hour movers."""
    path = os.path.join(eng.DATA, "leaders.csv")
    overall, ours, lines, movers = [], None, {}, []
    if os.path.exists(path):
        with open(path, newline="") as fh:
            rows = list(csv.DictReader(fh))[-60 * 120:]
        if rows:
            last_t = rows[-1]["time"]
            snap = [r for r in rows if r["time"] == last_t]
            for r in snap:
                try:
                    rk = int(r["rank"])
                except ValueError:
                    continue
                row = dict(rank=rk, name=r["name"], pnl=float(r["value"]), trades=int(float(r["trades"] or 0)), ours=r["ours"] == "1")
                if row["ours"]:
                    ours = row
                if rk <= 25:
                    overall.append(row)
                if rk in (50, 100):
                    lines[str(rk)] = row["pnl"]
            # last hour movers: Δ P&L per account between the sample nearest to 60 min ago and now
            t_last = time.mktime(time.strptime(last_t, "%Y-%m-%d %H:%M:%S"))
            older = [r for r in rows if abs(time.mktime(time.strptime(r["time"], "%Y-%m-%d %H:%M:%S")) - (t_last - 3600)) <= 480]
            if older:
                t_old = older[-1]["time"]
                prev = {r["name"]: float(r["value"]) for r in rows if r["time"] == t_old}
                for r in snap:
                    if r["name"] in prev:
                        movers.append(dict(name=r["name"], d_pnl=round(float(r["value"]) - prev[r["name"]], 2), rank=int(r["rank"] or 0)))
                movers.sort(key=lambda m: -m["d_pnl"])
    day = eng.read_json(os.path.join(eng.DATA, "leaders_day.json"), {}) or {}
    jumps = eng.read_json(os.path.join(eng.DATA, "leaders_jumps.json"), []) or []
    return dict(overall=sorted(overall, key=lambda r: r["rank"]), ours=ours, lines=lines, daily=day, movers=movers[:5], jumps=jumps[-40:][::-1])


def admin_v4(eng, now, sigday):
    dup = collections.Counter()
    for o in eng.open:
        rec = eng.ledger.orders.get(o["id"])
        if rec and not str(rec.get("sub", "")).startswith("pairset:"):
            dup[(rec["bot"], rec["layer"], rec.get("sub"), o["ex"], o["side"], o["action"], o["price"])] += 1
    foreign = sum(1 for o in eng.open if o["id"] not in eng.ledger.orders)
    cov = {}
    for layer, fn in (("dump", eng.dump_eligible), ("longshot", eng.longshot_eligible)):
        elig = {(b.ex, s[0]) for b in eng.bots if b.kind == "market" for s in fn(b.ex)}
        have = {(o["ex"], o["side"]) for o in eng.open if (eng.ledger.orders.get(o["id"]) or {}).get("layer") == layer}
        cov[layer] = dict(eligible=len(elig), covered=len(elig & have))
    fc = eng.coverage
    if fc:
        cov["fatfinger"] = dict(eligible=fc["eligible"], covered=fc["covered"])
    ratchet = eng.controls.d.get("ratchet") or {}
    m = eng.money_view() or {}
    return dict(trims=dict(eng.ex.trims), foreign=foreign, duplicates=sum(1 for v in dup.values() if v > 1), unknown=eng.ex.stats().get("unknown"),
                recon=dict(today=sigday.get("today"), sum=sigday.get("our_trades"), drift=sigday.get("drift"), estimate=sigday.get("drift_estimate"),
                           check=sigday.get("check"), ok=sigday.get("ok"), note=sigday.get("note")),
                coverage=cov, decisions=list(eng.controls.d.get("decisions") or []), ratchet=ratchet,
                pt_day=m.get("today"), release=getattr(eng, "release_name", ""), hold_gates=bool(eng.controls.d.get("hold_gates")),
                gate_free=list(eng.controls.d.get("gate_free") or []), truth_mode=eng.controls.d.get("truth_mode", "shadow"))


def v4_view(eng, now, capital, holdings):
    pts = eng.pnl_rows()
    sigday = sigday_view(eng, now, pts)
    oct1 = [f for f in eng.ledger.fills if not f["before_engine"]]
    return dict(sigday=sigday, status=status_view(eng, now), daily=daily_view(eng, now, pts), feed=feed_view(eng),
                strategies=strategies_v4(eng, now, sigday, oct1), capital=capital_v4(eng, now, capital, holdings),
                holdings=holdings_v4(eng, holdings), leaders=leaders_v4(eng), admin=admin_v4(eng, now, sigday))
