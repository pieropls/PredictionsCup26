"""Night reserve sizing (Piero 10:55 Oct 23 item 3; docs/night_reserve.md): the election-night P&L of the night layers at usable reserves
of 10k / 20k / 30k / 45k / 60k against the at-fair cost of raising that cash by the convergence sales.

Night side: tests/sim_election's rehearsal (tests/night_world + night_rig + night_report, called, not changed): the 2026 races of
data/election/poll_close.json, the 2024 Polymarket paths, the real j_executor.Executor on the fake SIG, n_election's constants as live.
"Today's books": the World's 2026 levels are built from a copy of the server's data/state.json (the same fields
research/night/night_sim_build.py writes into tests/fixtures/night26_levels.json; the fixture itself is not touched: night_world's
load_2026 is wrapped here). usable U -> cash0 = U + 7,000 (5k floor + 2k manual reserve). The uncapped reference (usable 1M, same seed,
same world) gives what each wave would deploy with no cash limit (the 15k-shares-a-race / 20k-a-state caps and the book still hold).

Cost side: every lot in a copy of the server's data/ledger.json sub-books (net side only), less the manual book, pair-set / lock legs,
RI Senate R NO (1077), CO-03 D NO (894), Kansas Senate D NO (1045) and the night layers; sale price = the convergence ask
max(tick_up(lot avg + 0.5c), side best ask) (filled there),
or the side best bid floored at lot avg + 0.5c (the asks stepped down to the bid, never under rule 0); a lot whose floor is above both
its touch ask and its fair is stuck (no fill to count on: left out, reported); lowest EV/$ (side fair / sale price - 1) first;
cost = (side fair - sale price) x shares.

Usage (offline; reads only the copies given and data/election/poll_close.json):
    python3 research/night/reserve_sizing.py --state <state.json> --ledger <ledger.json> [--overrides <overrides.json>] [--seeds 8]
           [--seeds-check 5] [--jobs 6] [--json out.json] [--md docs/night_reserve.md]       (~70 s with 10 jobs: 142 nights)
    python3 research/night/reserve_sizing.py --from-json out.json --state ... --ledger ... --md docs/night_reserve.md   (re-report)
    python3 research/night/reserve_sizing.py --rivals --state ... --ledger ... [--json rivals.json] --md docs/night_reserve.md
           (Piero 14:00 Oct 23: rivals take the top ask level of each called side in 30% / 50% of races; ~3 min; inserts its section only)"""
import argparse
import json
import math
import os
import random
import sys
import time

ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.insert(0, ROOT)
sys.path.insert(0, os.path.join(ROOT, "tests"))

FLOOR = 7_000.0                                            # 5k floor + the 2k manual reserve (n_election.CASH_FLOOR)
SIZES = (10_000, 20_000, 30_000, 45_000, 60_000)
EXTRA = (70_000,)                                          # ~ the end of the sellable pool at today's asks
UNCAPPED = 1_000_000
HELD_EX = {"1077": "RI Senate R NO (Piero keeps)", "894": "CO-03 D NO (SELL_HOLD)", "1045": "Kansas Senate D NO (SELL_HOLD)"}
SKIP_SUB = ("manual", "pairset:", "fmmlock:", "callsnipe:", "drift:", "settle:")
SCEN = {"normal": dict(), "slow": dict(stretch=(2.0, 3.0)), "reversal": dict(reversals=4, strict_reversals=2)}
TICK = 0.005


def tick_up(p):
    return round(math.ceil(p / TICK - 1e-9) * TICK, 3)


# ------------------------------------------------------------------------------------------------------------- cost side
def load_lots(state_path, ledger_path):
    st, lg = json.load(open(state_path)), json.load(open(ledger_path))
    bots = {str(b["ex"]): b for b in st["bots"]}
    tr = lg.get("tranches") or {}
    lots, skipped = [], {}
    for ex, subs in (lg.get("subbooks") or {}).items():
        b = bots.get(str(ex))
        if not b or not b.get("pos"):
            continue
        q = float(b["pos"])
        side = "yes" if q > 0 else "no"
        fair = b.get("fair")
        bid, ask = b.get("bid"), b.get("ask")
        if fair is None or bid is None or ask is None:
            continue
        fs = fair if side == "yes" else 1 - fair
        s_bid, s_ask = (bid, ask) if side == "yes" else (round(1 - ask, 3), round(1 - bid, 3))
        left = abs(q)                                      # never more than the net position
        for sub, v in sorted(subs.items(), key=lambda kv: str(kv[0]).startswith(SKIP_SUB) or ex in HELD_EX, reverse=True):
            if v * q <= 0 or abs(v) < 1:
                continue
            sh = min(abs(v), left)
            left -= sh
            why = HELD_EX.get(ex) or next((p.rstrip(":") for p in SKIP_SUB if str(sub).startswith(p)), None)
            if why:
                skipped[why] = skipped.get(why, 0.0) + sh * s_bid
                continue
            if sh < 1:
                continue
            avg = (tr.get(sub) or {}).get("avg") or b.get("avg") or 0.0
            floor_ = tick_up(round(float(avg) + 0.005, 6))
            stuck = floor_ > max(s_ask, fs) + 1e-9         # rule 0 needs a price above both the touch and fair: no fill to count on
            lots.append(dict(ex=ex, name=b.get("name"), market=b.get("market"), side=side, sub=sub, kind=str(sub).split(":")[0],
                             sh=sh, avg=round(float(avg), 4), fair=round(fs, 4), s_bid=s_bid, s_ask=s_ask, floor=floor_, stuck=stuck,
                             p_ask=None if stuck else max(floor_, s_ask), p_bid=None if stuck else max(floor_, s_bid)))
    cash = st["account"]["totalAccountValue"] - st["account"]["totalHoldingsValue"]
    return dict(lots=lots, skipped=skipped, cash=cash, usable=cash - FLOOR, snapshot=st.get("time"))


def walk(lots, mode):
    """The lots in sale order for one fill assumption: [(EV/$, lot, price, cash, cost)], lowest EV/$ first."""
    out = []
    for L in lots:
        p = L["p_ask"] if mode == "ask" else L["p_bid"]
        if p is None or p <= 0:
            continue
        out.append((L["fair"] / p - 1, L, p, L["sh"] * p, L["sh"] * (L["fair"] - p)))
    out.sort(key=lambda x: (x[0], x[1]["ex"], x[1]["sub"]))
    return out


def cost_to_raise(w, need):
    """At-fair cost of raising `need` cash down the sorted list (the boundary lot pro rata). -> (cost, raised)"""
    cost, got = 0.0, 0.0
    for ev, L, p, cash, c in w:
        if got >= need - 1e-9:
            break
        f = min(1.0, (need - got) / cash)
        cost += c * f
        got += cash * f
    return cost, got


# ------------------------------------------------------------------------------------------------------------- night side
_LV = {}


def _patch_world(state_path):
    """Point night_world's 2026 levels at today's books (state_path) or back at the fixture (None). Pool workers are reused: restore."""
    import night_world as nw
    if "orig" not in _LV:
        _LV["orig"] = nw.load_2026
    if not state_path:
        nw.load_2026 = _LV["orig"]
        return nw
    if state_path in _LV:
        nw.load_2026 = _LV[state_path]
        return nw
    st = json.load(open(state_path))
    pc = json.load(open(os.path.join(ROOT, "data", "election", "poll_close.json")))      # read-only (the same file as the server's)
    books = {str(b["ex"]): dict(fair=b.get("fair"), sig_bid=b.get("bid"), sig_ask=b.get("ask"), title=b.get("market"),
                                manual=b.get("kind") == "position") for b in st.get("bots", [])}
    lv = dict(snapshot=st.get("time"), note="built in memory from a copy of the server's data/state.json", books=books, poll_close=pc)
    nw.load_2026 = _LV[state_path] = (lambda: (lv, pc))
    return nw


def competed(W, seed, p):
    """The races where rivals snipe too: the round(p x N) races with the lowest per-race draw, seeded by (seed, race) only, so they are the
    same races at every reserve size and on every scenario of a seed, and the 30% set sits inside the 50% one."""
    if p <= 0:
        return set()
    u = {r: random.Random(f"{seed}:rivals:{r}").random() for r in W.races}
    return set(sorted(W.races, key=lambda r: (u[r], r))[:int(round(p * len(W.races)))])


def rival_rig(rig_mod):
    import n_election as el_mod

    class RivalRig(rig_mod.Rig):
        """night_rig.Rig + rivals (Piero 14:00 Oct 23): at each CALL in a competed race (n_election's record: one book's called side), a
        rival takes that side's whole TOP ask level at the call instant, before our deferred snipe goes out (engine mode: the snipe waits
        for the loop's fast step). The sniper can still buy the next levels at <= min(0.97, PM bid - 3c). The taken level refills the way our
        own takes do (night_world: 10-min constant). Wraps E.on_pm only; the rig, the world and n_election are not changed."""

        def __init__(self, W, competed=(), take="top", **kw):
            super().__init__(W, **kw)
            self.competed, self.rivals, self.take = set(competed), [], take

        def build(self):
            super().build()
            E, W, orig = self.E, self.W, self.E.on_pm
            hold = self.hold = {}
            if self.take == "hold":                        # the second stress check: for 10 min after the call the best level is the rivals'
                sl0 = W.side_levels

                def side_levels(ex, side, t):
                    lv = sl0(ex, side, t)
                    h = hold.get((ex, side))
                    return lv[1:] if (h is not None and h <= t < h + 600 and lv) else lv
                W.side_levels = side_levels                 # instance attribute: W.take and the rig's el_levels see it too

            def on_pm(ex, *a, **kw):
                n0 = len(E.records)
                out = orig(ex, *a, **kw)
                for rec in E.records[n0:]:
                    if rec.get("kind") == "CALL" and rec.get("race") in self.competed:
                        t = self.clock.time()
                        lv = W.side_levels(rec["ex"], rec["side"], t)
                        if self.take == "hold":
                            hold[(rec["ex"], rec["side"])] = t
                            got = [lv[0]] if lv else []
                        elif self.take == "cap":               # the stress check: every level at or under our cap goes
                            lim = round(min(el_mod.PRICE_CAP, (rec.get("pm_bid") or 0.0) - el_mod.PRICE_UNDER_PM), 3)
                            got = W.take(rec["ex"], rec["side"], lim, 10 ** 9, t)
                        elif lv and lv[0][1] >= 1:
                            got = W.take(rec["ex"], rec["side"], lv[0][0], lv[0][1], t)
                        else:
                            got = []
                        if got:
                            self.rivals.append((t, rec["ex"], rec["side"], sum(q for _, q in got), sum(px * q for px, q in got)))
                return out
            E.on_pm = on_pm
    return RivalRig


def run_job(job):
    """One night: (scenario, seed, usable, state_path or None = the Oct 21 fixture) -> per-wave spend / P&L of the night layers
    (by the race's wave: its last poll close), the sniper's own spend apart from the settlement morning, usable at each wave start."""
    scen, seed, usable, state_path = job[:4]
    p = job[4] if len(job) > 4 else 0.0                    # the rivals' share of races (0: the plain rig)
    take = job[5] if len(job) > 5 else "top"               # what they take at the call: the top level, or every level <= our cap
    import tests  # noqa: F401  (the package guard: SIG_DATA is a temp dir, never data/)
    nw = _patch_world(state_path)
    import night_rig as rig
    import night_report as nr
    import n_election as el
    W = nw.World(seed=seed, **SCEN[scen])
    if p > 0:
        R = rival_rig(rig)(W, competed=competed(W, seed, p), take=take, name=scen, seed=seed, cash0=usable + FLOOR).run()
    else:
        R = rig.Rig(W, name=scen, seed=seed, cash0=usable + FLOOR).run()
    riv = getattr(R, "rivals", [])
    m = nr.metrics(R, dict(call=0, info=0, need={}))
    waves = sorted(set(W.last.values()))
    wave_of = {ex: W.last[W.meta[ex]["race"]] for ex in W.legs}
    spend, snipe, pnl = ({w: 0.0 for w in waves} for _ in range(3))
    for t, oid, ex, side, action, q, px, layer, how in R.fills:
        if layer not in el.SNIPE_LAYERS:
            continue
        pay = W.payout(ex) if side == "yes" else 1 - W.payout(ex)
        w = wave_of[ex]
        if action == "buy":
            spend[w] += q * px
            snipe[w] += q * px if layer == "callsnipe" else 0.0
            pnl[w] += q * (pay - px)
        else:
            pnl[w] += q * (px - pay)
    at = {}
    for w in waves:
        before = [c for t, c in R.cash_path if t <= w]
        at[w] = (before[-1] if before else R.cash0) - FLOOR
    pre_settle = [c for t, c in R.cash_path if t <= nw.SETTLE_START]
    return dict(scen=scen, seed=seed, usable=usable, src="today" if state_path else "fixture", night=m["night"], by_layer=m["by_layer"],
                false_pnl=m["false_pnl"], false=m["false"],
                waves=[dict(w=nw.hm(w)[:5], spend=round(spend[w]), snipe=round(snipe[w]), pnl=round(pnl[w]), usable_at=round(at[w]))
                       for w in waves],
                left_06=round((pre_settle[-1] if pre_settle else R.cash) - FLOOR), cash_end=round(R.cash - FLOOR), sn_cost=m["sn_cost"],
                sn_sh=m["sn_sh"], p=p, take=take if p > 0 else None, rival_n=len(riv), rival_sh=round(sum(x[3] for x in riv)), rival_cost=round(sum(x[4] for x in riv)))


def run_plan(plan, jobs):
    t0 = time.time()
    if jobs <= 1:
        return [run_job(j) for j in plan]
    import multiprocessing as mp
    res = []
    with mp.get_context("fork").Pool(jobs) as pool:
        for i, r in enumerate(pool.imap_unordered(run_job, plan)):
            res.append(r)
            if i % 20 == 0:
                print(f"{i + 1}/{len(plan)} {time.time() - t0:.0f} s", flush=True)
    return res


RIVAL_SIZES, RIVAL_P = (30_000, 45_000, 60_000), (0.0, 0.3, 0.5)


def run_rivals(state_path, seeds, seeds_check, jobs):
    plan = [(scen, s, u, state_path, p, take) for scen, n in (("normal", seeds), ("slow", seeds_check), ("reversal", seeds_check))
            for s in range(1, n + 1) for u in RIVAL_SIZES for p, take in [(x, "top") for x in RIVAL_P] + [(0.5, "cap"), (0.5, "hold")]]
    return run_plan(plan, jobs)


def run_all(state_path, seeds, seeds_check, jobs):
    plan = []
    for scen, n in (("normal", seeds), ("slow", seeds_check), ("reversal", seeds_check)):
        for s in range(1, n + 1):
            for u in SIZES + EXTRA + (UNCAPPED,):
                plan.append((scen, s, u, state_path))
    for s in range(1, seeds + 1):                              # the book snapshot's weight: the Oct 21 fixture at 30k / 60k
        for u in (30_000, 60_000):
            plan.append(("normal", s, u, None))
    t0 = time.time()
    if jobs > 1:
        import multiprocessing as mp
        with mp.get_context("fork").Pool(jobs) as pool:
            res = []
            for i, r in enumerate(pool.imap_unordered(run_job, plan)):
                res.append(r)
                if i % 10 == 0:
                    print(f"{i + 1}/{len(plan)} {time.time() - t0:.0f} s", flush=True)
    else:
        res = [run_job(j) for j in plan]
    return res


# ------------------------------------------------------------------------------------------------------------- report
def k(v, nd=1):
    return f"{v / 1000:+.{nd}f}k"


def mean(v):
    v = list(v)
    return sum(v) / len(v)


def table_by(res, scen, src="today"):
    by = {}
    for r in res:
        if r["scen"] == scen and r.get("src", "today") == src:
            by.setdefault(r["usable"], {})[r["seed"]] = r
    return by


def binds(by, u, seed, i, tol=0.9):
    """Wave i at usable u binds on this seed: the sniper's spend there < 90% of what the same night spends with unlimited cash."""
    need = by[UNCAPPED][seed]["waves"][i]["snipe"]
    if need < 300:
        return None                                        # nothing to buy in that wave on this seed
    return by[u][seed]["waves"][i]["snipe"] < tol * need


def night_rows(by, sizes):
    """[(u, [P&L per seed], [marginal per +10k per seed])], common seeds across sizes; the marginal from the size before (0 -> 0)."""
    seeds = sorted(by[sizes[0]])
    out, pu, pv = [], 0, [0.0] * len(seeds)
    for u in sizes:
        v = [by[u][s]["night"] for s in seeds]
        out.append((u, v, [(a - b) * 10_000 / (u - pu) for a, b in zip(v, pv)]))
        pu, pv = u, v
    return out


def cost_rows(w, usable0, sizes, inflow=0.0):
    out, pc, pu = [], 0.0, 0
    for u in sizes:
        need = max(0.0, u - usable0 - inflow)
        c, got = cost_to_raise(w, need)
        out.append(dict(u=u, need=need, cost=c, got=got, short=need - got, mc=(c - pc) * 10_000 / (u - pu)))
        pc, pu = c, u
    return out


def write_md(res, cost, path):
    sizes = SIZES + EXTRA
    norm, slow, rev = table_by(res, "normal"), table_by(res, "slow"), table_by(res, "reversal")
    fix = table_by(res, "normal", "fixture")
    seeds = sorted(norm[sizes[0]])
    seeds_c = sorted(slow[sizes[0]])
    wn = [w["w"] for w in norm[sizes[0]][seeds[0]]["waves"]]
    usable0, lots = cost["usable"], cost["lots"]
    wa, wb = walk(lots, "ask"), walk(lots, "bid")
    own = ("fmm", "rot")
    wo = [x for x in wa if x[1]["kind"] not in own]
    own_cash = sum(x[3] for x in wa if x[1]["kind"] in own)
    nr_, sr_, rr_ = night_rows(norm, sizes), night_rows(slow, sizes), night_rows(rev, sizes)
    ca, cb, co = cost_rows(wa, usable0, sizes), cost_rows(wb, usable0, sizes), cost_rows(wo, usable0, sizes, inflow=own_cash)
    pool_a = sum(x[3] for x in wa)
    unc_snipe = [mean([norm[UNCAPPED][s]["waves"][i]["snipe"] for s in seeds]) for i in range(len(wn))]
    unc_total = mean([sum(w["snipe"] for w in norm[UNCAPPED][s]["waves"]) for s in seeds])
    unc_all = mean([sum(w["spend"] for w in norm[UNCAPPED][s]["waves"]) for s in seeds])
    unc_night = mean([norm[UNCAPPED][s]["night"] for s in seeds])
    n5 = len(SIZES)

    def bind_list(u):
        bw = []
        for i in range(len(wn)):
            v = [b for b in (binds(norm, u, s, i) for s in seeds) if b is not None]
            if v and sum(v) * 2 > len(v):
                bw.append(wn[i])
        return bw

    def bind_txt(u):
        bw = bind_list(u)
        if len(bw) == len(wn):
            return f"all {len(wn)}"
        if len(bw) >= len(wn) - 3:
            return "all but " + ", ".join(w for w in wn if w not in bw)
        return ", ".join(bw) or "none"

    def crossing(nrows, crows):
        """(last size whose marginal P&L >= marginal cost, first size where it is below) walking up from 10k."""
        best = None
        for (u, v, mg), c in zip(nrows, crows):
            if mean(mg) < c["mc"]:
                return best, u
            best = u
        return best, None
    rec, cross = crossing(nr_[:n5], ca[:n5])
    _, cross_x = crossing(nr_, ca)
    rec_r, cross_r = crossing(rr_, ca)
    _, cross_s = crossing(sr_, ca)
    mg_n = mean([mean(mg) for u, v, mg in nr_[:n5]])
    mg_r = mean([mean(mg) for u, v, mg in rr_[:n5]])
    net70 = mean(nr_[-1][2]) - ca[-1]["mc"]
    L = ["# Night reserve sizing: the usable cash to hold on election night", ""]
    L += [f"Written {time.strftime('%a %b %d %H:%M')} PT by Claude Code for Piero (his 10:55 Oct 23 item 3). Offline: the sim ran on the Mac "
          f"against read-only copies of the server's `state.json` / `ledger.json` ({cost['snapshot']} PT). No orders, no SIG or web calls. "
          f"Script: `research/night/reserve_sizing.py`. Night P&L: the normal night, {len(seeds)} seeds. Cost: the convergence asks, filled at the ask.", ""]
    L += ["| usable reserve | night P&L, mean (range) | marginal P&L per +10k | cash binds in waves | cash to raise | at-fair cost | marginal cost per +10k |",
          "|---:|---|---:|---|---:|---:|---:|"]
    for (u, v, mg), c in zip(nr_, ca):
        lab = f"{u / 1000:.0f}k" + (" (pool end)" if u in EXTRA else "")
        L.append(f"| {lab} | {k(mean(v))} ({k(min(v))} to {k(max(v))}) | {k(mean(mg), 2)} | {bind_txt(u)} | {c['need'] / 1000:.1f}k | "
                 f"{c['cost'] / 1000:.2f}k | {c['mc'] / 1000:.2f}k |")
    L.append("")
    if cross is None:
        L.append(f"**Recommendation: {rec / 1000:.0f}k usable, the ramp's Nov 3 target as it stands.** The marginal night P&L (about "
                 f"{k(mg_n, 1)} per 10k at every size) stays above the marginal cost ({ca[0]['mc'] / 1000:.2f}k rising to {ca[n5 - 1]['mc'] / 1000:.2f}k "
                 f"per 10k) all the way to 60k, so the crossing is not reached there. The next step, 60k to 70k (the pool's end), still nets "
                 f"{k(net70, 2)} per 10k on a normal night. But it needs {ca[-1]['got'] / pool_a:.0%} of every sellable lot filled at its ask by Nov 3, and "
                 f"it loses on the reversal night ({k(mean(rr_[-1][2]), 2)} against {ca[-1]['mc'] / 1000:.2f}k). The reversal night crosses "
                 + (f"between {rec_r / 1000:.0f}k and {cross_r / 1000:.0f}k." if rec_r and cross_r else f"at {(cross_r or 0) / 1000:.0f}k."))
    else:
        L.append(f"**Recommendation: {(rec or sizes[0]) / 1000:.0f}k usable.** The marginal night P&L drops below the marginal cost at {cross / 1000:.0f}k.")
    L.append("")
    L.append(f"**How \"cash binds\" is measured:** the wave's sniper spend is under 90% of what the same seed spends with unlimited cash, on most of "
             f"the seeds where that wave has anything to buy (16:30 has buys on only 3 of the {len(seeds)}). Unlimited cash still keeps the 15k-shares-a-race "
             f"and 20k-a-state caps and the book. With unlimited cash the sniper deploys about {unc_total / 1000:,.0f}k across the night. Even 70k reaches only "
             f"{mean([sum(w['snipe'] for w in norm[70_000][s]['waves']) for s in seeds]) / unc_total:.0%} of that, so every wave binds at every size. "
             f"The limit is the wave share (usable / the races still uncalled), not cash running out. A 60k reserve still has "
             f"{mean([norm[60_000][s]['waves'][-1]['usable_at'] for s in seeds]) / 60_000:.0%} left when the 22:00 wave starts, and "
             f"{mean([norm[60_000][s]['left_06'] for s in seeds]) / 1000:.1f}k for the settlement morning.")
    L.append("")
    if path and os.path.exists(path):                     # the operator's decision lines written into the top block by hand
        top = open(path).read().split("\n## ")[0].split("\n")
        L += [ln for ln in top if ln.startswith("**Decision")] and [ln for ln in top if ln.startswith("**Decision")] + [""]
    for sec in kept_sections(path):
        L += [sec, ""]
    L += ["---", "", "## Notes", "", "### Method", ""]
    L += [f"- **Night:** `tests/sim_election`'s rehearsal, called through `night_world` / `night_rig` / `night_report.metrics` and not changed. "
          "That covers the 116 races of `data/election/poll_close.json`, the 2024 Polymarket paths, the real `j_executor.Executor` on the fake SIG, and "
          "n_election as live: price <= min(0.97, PM bid - 3c), wave share 1x, <= 15k shares a race, <= 20k a state.",
          "  - P&L is the night layers (callsnipe + settle), valued at settlement on each path's winner. A usable U starts the sim with cash U + 7,000.",
          f"  - Today's books: the SIG bid / ask and PM fair per book are built in memory from the {cost['snapshot']} `state.json`. I wrapped "
          "`night_world.load_2026` to do it; the Oct 21 fixture is untouched.",
          f"- **Seeds:** normal 1-{len(seeds)}. Slow count (contested races counted 2-3x slower) and reversal night (6 injected 2024-style wrong calls): "
          f"1-{len(seeds_c)}. Every size runs on the same worlds, so a marginal is the per-seed difference between two sizes. The uncapped "
          "reference (usable 1M) uses the same seeds.",
          f"- **Cost:** walks the {cost['snapshot']} `ledger.json` sub-books, net side only.",
          "  - Sale price: the convergence ask, max(lot avg + 0.5c, the side's best ask), assumed to fill there.",
          "  - Order: lowest EV/$ first (side fair / sale price - 1).",
          "  - Cost: the sum of (side fair - sale price) x shares, pro rata on the boundary lot.",
          f"  - Cash to raise: reserve - today's usable ({usable0:,.0f} = cash {cost['cash']:,.0f} - 7,000).",
          "- **Left out of the sales** (at the side bid): " + ", ".join(f"{k_} {v / 1000:.1f}k" for k_, v in sorted(cost["skipped"].items(), key=lambda x: -x[1]))
          + ". Also out: lots whose rule-0 floor (lot avg + 0.5c) is above both their best ask and their fair, so no fill can be counted on. "
            f"There are {sum(1 for x in lots if x['stuck'])} of them, worth {sum(x['sh'] * x['s_bid'] for x in lots if x['stuck']) / 1000:.1f}k at the bid. "
            "Most of that is the rotation's VA-05 R NO, FL-22 D NO and WI-01 D NO, the coin flips you hold.",
          f"- **Sellable pool:** {pool_a / 1000:.1f}k at the ask, which funds at most about {(pool_a + usable0) / 1000:.0f}k usable. More would have to come "
          f"from the kept lots (RI Senate R NO, {cost['skipped'].get(HELD_EX['1077'], 0) / 1000:.1f}k at the bid) or from rule-0 exceptions.", ""]
    L += ["### Assumptions", "",
          "- **Cash:** nothing else comes in or goes out before Nov 3. FMM and rotation lots join the same EV/$ walk as everything else; their own exits "
          "post at the same max(lot avg + 0.5c, ask) price. New trading before the wind-down is ignored, and so is any drift in fairs and asks.",
          "- **Not counted as cost:** what the reserved cash would have earned from Oct 28 (the ramp keeps it from FMM and carry), and the "
          "night's recycling of winners into 0.98 bids (C7), which the sim doesn't model and SIG has rarely shown.",
          "- **Not in the night model:** competing snipers on SIG. How far SIG lags PM is the sim's assumption (`night_world`).", ""]
    L += [f"### Night P&L by wave (normal night, mean of {len(seeds)} seeds; the wave is the race's last poll close PT)", "",
          "| wave | " + " | ".join(f"{u / 1000:.0f}k" for u in sizes) + " | 60k range | sniper spend, unlimited cash | 60k: usable at the wave start | 60k: share of the unlimited spend |",
          "|---|" + "---:|" * (len(sizes) + 4)]
    for i, w in enumerate(wn):
        p60 = [norm[60_000][s]["waves"][i]["pnl"] for s in seeds]
        L.append(f"| {w} | " + " | ".join(f"{mean([norm[u][s]['waves'][i]['pnl'] for s in seeds]):+,.0f}" for u in sizes)
                 + f" | {min(p60):+,} to {max(p60):+,} | {unc_snipe[i] / 1000:,.0f}k | {mean([norm[60_000][s]['waves'][i]['usable_at'] for s in seeds]) / 1000:.1f}k"
                 + f" | {mean([norm[60_000][s]['waves'][i]['snipe'] for s in seeds]) / max(unc_snipe[i], 1):.0%} |")
    v60 = nr_[n5 - 1][1]
    L.append("| **total** | " + " | ".join(f"**{mean(v):+,.0f}**" for u, v, mg in nr_) + f" | {min(v60):+,} to {max(v60):+,} | {unc_total / 1000:,.0f}k | | "
             f"{mean([sum(w['snipe'] for w in norm[60_000][s]['waves']) for s in seeds]) / unc_total:.0%} |")
    L += ["", f"- **Return:** about {mean([mean(v) / u for u, v, mg in nr_]):.1%} on the reserve at every size. The wave share scales every snipe "
          "with usable, and at these sizes the books are deep enough. "
          f"Unlimited cash deploys {unc_all / 1000:,.0f}k (sniper plus settlement morning) for {k(unc_night)}, {unc_night / unc_all:.1%} per $. Past 70k, each "
          f"10k of reserve adds {k((unc_night - mean(nr_[-1][1])) * 10_000 / (UNCAPPED - sizes[-1]), 2)}.",
          "- **The 16:30 wave** (2 races) has calls on only 3 seeds. On those it binds too.", ""]
    L += ["### Slow-count and reversal nights", "",
          "| usable | slow count: P&L mean (range) | marginal per +10k | reversal night: P&L mean (range) | marginal per +10k | reversal: wrong-call P&L |",
          "|---:|---|---:|---|---:|---:|"]
    for (u, v, mg), (_, v2, mg2) in zip(sr_, rr_):
        fp = mean([rev[u][s]["false_pnl"] for s in sorted(rev[u])])
        L.append(f"| {u / 1000:.0f}k | {k(mean(v))} ({k(min(v))} to {k(max(v))}) | {k(mean(mg), 2)} | {k(mean(v2))} ({k(min(v2))} to {k(max(v2))}) | "
                 f"{k(mean(mg2), 2)} | {fp:+,.0f} |")
    L += ["", "- **Slow count:** keeps the normal night's slope"
          + (f" and never drops below the ask-fill cost up to 70k." if cross_s is None else f"; it crosses the ask-fill cost at {cross_s / 1000:.0f}k."),
          f"- **Reversal night:** halves the slope to {k(mg_r, 2)} per 10k, because the wrong calls cost in proportion to the reserve. It drops below "
          "the ask-fill cost " + (f"at {cross_r / 1000:.0f}k (the last size that still pays is {rec_r / 1000:.0f}k)." if cross_r and rec_r else "nowhere up to 70k."),
          f"- **Mixed nights:** if 1 night in 4 were a reversal night, 45k to 60k would still earn {k(0.75 * mean(nr_[n5 - 1][2]) + 0.25 * mean(rr_[n5 - 1][2]), 2)} "
          f"per 10k, against {ca[n5 - 1]['mc'] / 1000:.2f}k of cost.", ""]
    L += ["### The lots sold first (sale order, 100 shares or more)", "",
          "| # | book | side | sub-book | shares | lot avg | side fair | sale price | EV/$ | cash |", "|---:|---|---|---|---:|---:|---:|---:|---:|---:|"]
    n, last_i = 0, 0
    for i, (ev, Lt, p, cash, c) in enumerate(wa):
        if Lt["sh"] < 100:
            continue
        n += 1
        last_i = i
        L.append(f"| {n} | {str(Lt['name']).replace('MKT ', '')} | {Lt['side'].upper()} | `{Lt['sub'][:34]}` | "
                 f"{Lt['sh']:,.0f} | {Lt['avg']:.3f} | {Lt['fair']:.3f} | {p:.3f} | {ev:.1%} | {cash:,.0f} |")
        if n >= 10:
            break
    dust = [x for x in wa[:last_i + 1] if x[1]["sh"] < 100]
    bands = []
    for lo, hi in ((0.0, 0.03), (0.03, 0.05), (0.05, 0.07), (0.07, 0.09), (0.09, 9)):
        cs = sum(x[3] for x in wa if lo <= x[0] < hi)
        bands.append(f"{lo:.0%}-{hi:.0%} {cs / 1000:.1f}k" if hi < 9 else f"{lo:.0%} and up {cs / 1000:.1f}k")
    kinds = sorted({x[1]["kind"] for x in wa}, key=lambda kd: -sum(x[3] for x in wa if x[1]["kind"] == kd))
    cum, n10 = 0.0, 0
    for x in wa:
        cum += x[3]
        n10 += 1
        if cum >= 10_000:
            break
    L += ["", f"- **In between:** {len(dust)} smaller lots ({sum(x[3] for x in dust):,.0f} of cash).",
          f"- **The first 10k of cash** takes {n10} lots, nearly all FMM, at EV/$ up to {wa[n10 - 1][0]:.1%}. The 60k boundary sits at "
          f"EV/$ {next(ev for ev, cum_ in _cum(wa) if cum_ >= ca[n5 - 1]['need']):.1%}.",
          "- **The pool by EV/$** at the ask: " + "; ".join(bands) + ".",
          "- **The pool by kind:** " + "; ".join(f"{kd} {sum(x[3] for x in wa if x[1]['kind'] == kd) / 1000:.1f}k" for kd in kinds) + ".", ""]
    L += ["### Sensitivity", "", "| usable | marginal P&L per +10k (normal) | cost: ask fill (the table) | cost: bid fill | cost: FMM + rotation exit on their own |",
          "|---:|---:|---|---|---|"]
    for (u, v, mg), a, b, o in zip(nr_, ca, cb, co):
        L.append(f"| {u / 1000:.0f}k | {k(mean(mg), 2)} | {a['cost'] / 1000:.2f}k, {a['mc'] / 1000:.2f}k per 10k | {b['cost'] / 1000:.2f}k, {b['mc'] / 1000:.2f}k per 10k"
                 + (f" ({b['short'] / 1000:.1f}k short)" if b['short'] > 1 else "") + f" | {o['cost'] / 1000:.2f}k, {o['mc'] / 1000:.2f}k per 10k"
                 + (f" ({o['short'] / 1000:.1f}k short)" if o['short'] > 1 else "") + " |")
    fx = {u: [fix[u][s]["night"] for s in sorted(fix[u])] for u in fix}
    core = cost.get("core")
    core_line = None
    if core:
        wc = sorted(wa, key=lambda x: ((x[1]["ex"], x[1]["sub"]) in core, x[0]))
        cc = cost_rows(wc, usable0, sizes)
        d = [b["cost"] - a["cost"] for a, b in zip(ca, cc)]
        core_line = (f"- **The engine's own order:** `conv_select` sells non-core lots before the {len(core)} core lots (overrides.json). That costs "
                     f"{min(d) / 1000:.2f}k to {max(d) / 1000:.2f}k more than the pure EV/$ order "
                     f"(60k: {cc[n5 - 1]['cost'] / 1000:.2f}k).")
    L += ["",
          "- **Bid fill:** the asks step down to the side's best bid, but never below lot avg + 0.5c. Most SIG spreads are 0.5c, so the cost barely moves.",
          f"- **FMM + rotation exit on their own:** their {own_cash / 1000:.1f}k (at the ask) comes back through their own exits whatever the "
          "reserve. That makes it free cash for this decision, and the reserve only has to sell the carry / dump / TM / inventory lots, which carry higher EV/$. "
          "The reserve costs nothing until that cash is used up; after that the marginal cost is the column above, still under the night's marginal P&L.",
          (f"- **Book snapshot:** on the Oct 21 fixture's books (`tests/fixtures/night26_levels.json`), same seeds: 30k "
           f"{k(mean(fx[30_000]))}, 60k {k(mean(fx[60_000]))}, against {k(mean(norm[30_000][s]['night'] for s in seeds))} / "
           f"{k(mean(norm[60_000][s]['night'] for s in seeds))} on today's.") if 30_000 in fx and 60_000 in fx else None,
          core_line,
          f"- **Seed noise:** on the normal night the marginal per 10k ranges from {k(min(min(mg) for u, v, mg in nr_), 2)} to "
          f"{k(max(max(mg) for u, v, mg in nr_), 2)} across seeds and sizes. Its lowest seeds fall under the 45k-60k cost, so 60k is a positive bet "
          "on average, not on every night.",
          ""]
    out = "\n".join(x for x in L if x is not None) + "\n"
    if path:
        with open(path, "w") as fh:
            fh.write(out)
    return out, rec


RIVAL_HEAD = "## With competition (Piero 14:00 Oct 23): rivals take the best stale offer first"


def rivals_md(res, cost, extra=()):
    """The competition section: each step's night P&L (mean over seeds, and the seeds below the step's raising cost) for p = 0 / 30% / 50%."""
    B = {(r["scen"], r["p"], r["take"], r["usable"], r["seed"]): r for r in list(res) + list(extra)}
    seeds = {sc: sorted({k_[4] for k_ in B if k_[0] == sc}) for sc in ("normal", "slow", "reversal")}
    w = walk(cost["lots"], "ask")
    c = {u: cost_to_raise(w, max(0.0, u - cost["usable"]))[0] for u in RIVAL_SIZES}
    steps = [(30_000, 45_000), (45_000, 60_000)]

    def step(sc, p, take, a, b):
        d = [B[(sc, p, take, b, s)]["night"] - B[(sc, p, take, a, s)]["night"] for s in seeds[sc] if (sc, p, take, b, s) in B]
        return (mean(d), sum(1 for x in d if x < c[b] - c[a]), len(d)) if d else None

    def cell(v, first=False):
        return "–" if v is None else f"{k(v[0], 2)} ({v[1]} of {v[2]}{' below cost' if first else ''})"

    L = [RIVAL_HEAD, "",
         "| step | rivals in | normal (8 seeds) | slow count (5) | reversal night (5) | raising cost of the step |",
         "|---|---:|---:|---:|---:|---:|"]
    for a, b in steps:
        for p in RIVAL_P:
            take = "top" if p > 0 else None
            L.append(f"| {a // 1000} → {b // 1000}k | {p:.0%} | " + " | ".join(cell(step(sc, p, take, a, b), sc == "normal" and p == 0 and a == 30_000)
                                                                          for sc in ("normal", "slow", "reversal")) + f" | {(c[b] - c[a]) / 1000:.2f}k |")

    def lvl(sc, p, take, u):
        v = [B[(sc, p, take, u, s)] for s in seeds[sc] if (sc, p, take, u, s) in B]
        return v
    n60 = {p: lvl("normal", p, "top" if p else None, 60_000) for p in RIVAL_P}
    s45 = {p: step("slow", p, "top" if p else None, 45_000, 60_000) for p in RIVAL_P}
    n45 = {p: step("normal", p, "top" if p else None, 45_000, 60_000) for p in RIVAL_P}
    L += ["",
          "- **The model:** in a random share p of the 116 races, a rival takes the whole top ask level of each called side at the moment our "
          "rule calls that book. That is before our snipe goes out (engine timing: the loop's fast step, about 8 s later). Our sniper can still buy "
          "the next levels at <= min(0.97, PM bid - 3c), and the level refills like our own takes (10-min constant).",
          "  - The races are drawn per seed, so they are the same at every reserve size, and the 30% set sits inside the 50% one.",
          f"  - It is a wrapper on the rig's `on_pm` in `research/night/reserve_sizing.py`; the sim, n_election and the tests are unchanged. "
          f"p = 0 reproduces the earlier runs exactly.",
          f"  - Raising costs are recomputed from the same 10:46 ledger: 30→45k {c[45_000] - c[30_000]:,.0f}, 45→60k {c[60_000] - c[45_000]:,.0f}. "
          "The 11:30 table's 0.86k is the same step, rounded from 1.70k - 0.84k.",
          f"- **Result:** no step moves by more than about 0.1k.",
          "  - The 45→60k step is the 14:00 decision's elastic 15k. At p = 0 / 30% / 50% it earns "
          + " / ".join(f"{k(n45[p][0], 2)}" for p in RIVAL_P) + " on the normal night and " + " / ".join(f"{k(s45[p][0], 2)}" for p in RIVAL_P)
          + f" on the slow count, against {(c[60_000] - c[45_000]) / 1000:.2f}k of raising cost at today's asks. So the 11:30 slow-night test still passes.",
          "  - The elastic rule's own price limit (≥ PM fair − 5c) caps its cost at 5c a share, about 5-6% per $ or at most ~0.8k for the 15k.",
          "  - The reversal night stays under cost on 4 of 5 seeds, as before.",
          "- **Why competition costs so little here:** the rivals do take a lot. At 60k they take about "
          + " / ".join(f"{mean([x['rival_sh'] for x in n60[p]]) / 1000:,.0f}k" for p in RIVAL_P[1:]) + " shares a night, from "
          + " / ".join(f"{mean([x['rival_n'] for x in n60[p]]):.0f}" for p in RIVAL_P[1:]) + " called books. But the night is cash-bound: about 473k of "
          "snipes would go out with unlimited cash, against at most 60k of reserve. A snipe a rival blocks leaves its cash in usable, and the wave share "
          f"moves it to the next calls. Our average snipe price at 60k is " + " / ".join(f"{mean([x['sn_cost'] / x['sn_sh'] for x in n60[p]]):.3f}" for p in RIVAL_P)
          + " at p = 0 / 30% / 50%.",
          "  - So in this sim the first dollars don't get better offers than the last ones. When the best offer goes to a rival, the dollar buys "
          "the next-best one, and at 60k there are still many of those.",
          "  - This answers the 11:30 note above: with competition modelled this way, the 45→60k step is not overstated."]
    stress = [(0.5, "cap", "Rivals take every level at or under our cap at the call, 50% of races"),
              (0.5, "hold", "Rivals hold the best level for 10 min after each call, 50% of races"),
              (1.0, "hold", "The same in every race")]
    rows = []
    for p, take, lab in stress:
        if not any(k_[1] == p and k_[2] == take for k_ in B):
            continue
        v = [step(sc, p, take, a, b) for a, b in steps for sc in ("normal", "slow", "reversal")]
        rows.append(f"  - {lab}: 30→45k normal / slow / reversal {k(v[0][0], 2)} / {k(v[1][0], 2)} / {k(v[2][0], 2)}; "
                    f"45→60k {k(v[3][0], 2)} / {k(v[4][0], 2)} / {k(v[5][0], 2)}.")
    if rows:
        px = {(p, t_): mean([B[("normal", p, t_, 60_000, s_)]["sn_cost"] / B[("normal", p, t_, 60_000, s_)]["sn_sh"] for s_ in seeds["normal"]
                             if ("normal", p, t_, 60_000, s_) in B]) for p, t_, _ in stress if any(k_[1] == p and k_[2] == t_ for k_ in B)}
        L += ["- **Harsher checks** (same seeds). None of them lowers a step:"] + rows + \
             [f"  - Our average snipe price at 60k falls rather than rises: {mean([x['sn_cost'] / x['sn_sh'] for x in n60[0.0]]):.3f} with no rivals, "
              + " / ".join(f"{v:.3f}" for v in px.values()) + " in these three. The blocked cash finds other offers at the same or better prices; "
              "the wrong calls cost a little more, not less."]
    L += [""]
    be = (c[60_000] - c[45_000]) / 15_000
    r60 = mean([x["night"] for x in n60[0.0]]) / 60_000
    L += [f"- **What would change it:** rivals faster than us on every stale offer all night long. That is a faster-repricing SIG, which compresses "
          f"the edge on every dollar at every size. The 45→60k step breaks even at {be:.1%} return per $ of reserve; the sim gives {r60:.1%}. So it "
          f"stays above cost unless competition takes more than about {1 - be / r60:.0%} off the night's return across all races. The reversal night "
          f"(about 6%) is already below that line.", ""]
    return "\n".join(L) + "\n"


def insert_section(path, head, text):
    """Put `text` (a '## ' section) into the doc in place of the section with this heading, or else just before the '---' rule that ends the
    top part (before '## Notes'); everything else in the file is kept as is."""
    lines = open(path).read().split("\n")
    if head in lines:
        i = lines.index(head)
        j = next((x for x in range(i + 1, len(lines)) if lines[x].startswith("## ") or lines[x] == "---"), len(lines))
        lines[i:j] = text.rstrip("\n").split("\n") + [""]
    else:
        n = lines.index("## Notes") if "## Notes" in lines else len(lines)
        j = n
        while j > 0 and lines[j - 1] in ("", "---"):
            j -= 1
        lines[j:j] = [""] + text.rstrip("\n").split("\n")
    with open(path, "w") as fh:
        fh.write("\n".join(lines))


def kept_sections(path):
    """The '## ' sections of an existing doc that write_md does not make itself (the operator's step table, the competition section): kept on a
    full re-run."""
    if not path or not os.path.exists(path):
        return []
    lines, out, cur = open(path).read().split("\n"), [], None
    for ln in lines:
        if ln.startswith("## ") or ln == "---":
            if cur:
                out.append("\n".join(cur).rstrip("\n"))
            cur = [ln] if ln.startswith("## ") and ln != "## Notes" else None
            if ln == "## Notes":
                break
        elif cur is not None:
            cur.append(ln)
    return out


def _cum(w):
    c = 0.0
    for ev, L, p, cash, cc in w:
        c += cash
        yield ev, c


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--state", required=True)
    ap.add_argument("--ledger", required=True)
    ap.add_argument("--seeds", type=int, default=8)
    ap.add_argument("--seeds-check", type=int, default=5)
    ap.add_argument("--jobs", type=int, default=6)
    ap.add_argument("--json")
    ap.add_argument("--from-json")
    ap.add_argument("--md")
    ap.add_argument("--cost-only", action="store_true")
    ap.add_argument("--rivals", action="store_true", help="the competition grid (30k / 45k / 60k x p 0 / 30% / 50% + the stress checks); "
                    "inserts its section into --md and keeps the rest of the doc")
    ap.add_argument("--rivals-json", help="re-report the competition section from saved runs (comma-separated JSON files)")
    ap.add_argument("--overrides", help="a copy of the server's data/overrides.json: the core lots, for the engine-order line")
    a = ap.parse_args()
    cost = load_lots(a.state, a.ledger)
    if a.overrides:
        d = json.load(open(a.overrides))
        cost["core"] = {(str(m.get("ex")), m.get("sub")) for m in ((d.get("core") or {}).get("lots") or {}).values()}
    if a.cost_only:
        print(f"cash {cost['cash']:,.0f} usable {cost['usable']:,.0f} lots {len(cost['lots'])} skipped {cost['skipped']}")
        for mode in ("ask", "bid"):
            w = walk(cost["lots"], mode)
            print(mode, "pool", round(sum(x[3] for x in w)), "lots", len(w))
            for R in SIZES:
                need = max(0.0, R - cost["usable"])
                c, got = cost_to_raise(w, need)
                print(f"  R {R:,} need {need:,.0f} raised {got:,.0f} cost {c:,.0f}")
            for ev, L, p, cash, c in w[:15]:
                print(f"   {ev:+.3f} {L['name']:28} {L['side']} {L['sub'][:28]:28} sh {L['sh']:7,.0f} avg {L['avg']:.3f} fair {L['fair']:.3f} "
                      f"bid {L['s_bid']:.3f} ask {L['s_ask']:.3f} px {p:.3f} cash {cash:7,.0f} cost {c:+6,.0f}")
        return
    if a.rivals or a.rivals_json:
        if a.rivals_json:
            res = [r for f in a.rivals_json.split(",") for r in json.load(open(f))]
        else:
            st = os.path.abspath(a.state)
            res = run_rivals(st, a.seeds, a.seeds_check, a.jobs)
            res += run_plan([(sc, s_, u, st, 1.0, "hold") for sc, n in (("normal", a.seeds), ("slow", a.seeds_check), ("reversal", a.seeds_check))
                             for s_ in range(1, n + 1) for u in RIVAL_SIZES], a.jobs)
            if a.json:
                json.dump(res, open(a.json, "w"))
        text = rivals_md(res, cost)
        print(text)
        if a.md:
            insert_section(a.md, RIVAL_HEAD, text)
        return
    if a.from_json:
        res = json.load(open(a.from_json))
    else:
        res = run_all(os.path.abspath(a.state), a.seeds, a.seeds_check, a.jobs)
        if a.json:
            json.dump(res, open(a.json, "w"))
    out, rec = write_md(res, cost, a.md)
    print(out)
    print("recommend", rec)


if __name__ == "__main__":
    main()
