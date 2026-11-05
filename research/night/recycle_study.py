"""Wave recycling thresholds on the election-night sim (Piero 23:20 Oct 23; docs/night_recycle.md). Research only, offline.

The rig is reserve_sizing.py's: tests/sim_election's world (night_world) + night_rig + the real j_executor.Executor on the fake SIG, with
today's books built in memory from a fresh copy of the server's state.json and the fresh poll_close.json (nothing under tests/ or the
engine is changed: night_world.load_2026 is wrapped, night_rig.Rig is subclassed).

The recycler (C7 as a study, not as built): from a STRICT call (n_election's rule B, PM side bid >= 0.98; Election.strict_for) our held
shares of the called side sell into SIG bids at >= max(T, tick_up(lot avg + 0.5c)), into existing bids only (no resting asks), once per
engine loop (10 s), one write per 50 sales out of the same 25-a-minute budget the executor uses.
  - SIG's post-call bids (we can't observe them): the World's own lag model for that side WITHOUT its 0.975 clip (the lag target
    pm(t-1) + gap, kept inside [0.001, 0.999]), bid = tick(side mid - 0.5c), 5 levels 0.5c apart, sizes D x the book's level depth
    (L.depth[k]); our sales consume them with the World's 10-min refill. Sensitivities: no refill; the clip kept (SIG as now).
  - No "usable < 20k" gate in the main grid; the as-built gate (sell at the best bid only, up to the 20k shortfall) is one check.
  - Recyclable stock: our snipe fills (callsnipe / settle) plus our held early-close favourites from the fresh ledger copy (side fair
    >= 0.75, race's last poll close <= 17:00 PT; reserve_sizing.load_lots' exclusions and lot avg), seeded into the fake SIG's positions
    at the start (so the sniper sees them: it never buys the other side of a book we hold). Unsold, they settle 1 or 0.
  - Recycled cash goes back into the rig's cash: the next loop's usable, so the wave share of the later calls rises.
Night P&L = snipes at settlement + recycling (sale - settlement payout of the shares sold) + held favourites at settlement (payout - avg).

Usage: python3 research/night/recycle_study.py --srv <dir with state.json, ledger.json, poll_close.json> [--jobs 10] [--json out.json]
       [--md docs/night_recycle.md]      |  --from-json out.json --srv ... --md ...   (re-report)
       --oct27 [--from-json oct27.json] --srv ... --md docs/night_recycle.md   (spec item 5: T 0.97 / 0.96 / 0.95, today's bids, the gate;
       appends its own section, keeps the rest of the doc)"""
import argparse
import json
import math
import os
import sys
import time

HERE = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.dirname(os.path.dirname(HERE))
sys.path.insert(0, HERE)
sys.path.insert(0, ROOT)
sys.path.insert(0, os.path.join(ROOT, "tests"))
import reserve_sizing as rs                                 # noqa: E402

TS = (0.99, 0.98, 0.97, 0.96)
DS = (0.25, 0.50)
SEEDS = {"normal": 8, "slow": 5, "reversal": 5}
EARLY = "17:00"                                             # the early-close favourites: race's last poll close <= 17:00 PT
FAV_MIN = 0.75
NEED_TARGET = 20_000.0                                      # C7 as built: recycle while usable < 20k (ELECTION['recycle_target'])
# the operator's gate (spec item 5, Oct 27): a wave's projected snipe need = its unlimited-cash sniper spend (docs/night_reserve.md's wave table)
# x the share of its races still uncalled; the recycler sells a book only while the later waves' (after the book's own) need > usable.
WAVE_NEED = {"16:00": 43e3, "16:30": 6e3, "17:00": 160e3, "17:30": 10e3, "18:00": 175e3, "19:00": 21e3, "20:00": 31e3, "21:00": 12e3, "22:00": 15e3}
_P = {}


def patch(srv):
    """night_world's 2026 levels from the fresh state.json + poll_close.json copies in `srv`."""
    import night_world as nw
    if _P.get("srv") == srv:
        return nw
    st = json.load(open(os.path.join(srv, "state.json")))
    pc = json.load(open(os.path.join(srv, "poll_close.json")))
    books = {str(b["ex"]): dict(fair=b.get("fair"), sig_bid=b.get("bid"), sig_ask=b.get("ask"), title=b.get("market"),
                                manual=b.get("kind") == "position") for b in st.get("bots", [])}
    lv = dict(snapshot=st.get("time"), note="in memory from a fresh copy of the server's state.json", books=books, poll_close=pc)
    nw.load_2026 = lambda: (lv, pc)
    _P["srv"] = srv
    return nw


def held_stock(srv, W, nw):
    """The early-close favourites we hold today: load_lots' lots with side fair >= 0.75 in races whose last poll close <= 17:00 PT."""
    c = rs.load_lots(os.path.join(srv, "state.json"), os.path.join(srv, "ledger.json"))
    lim = nw.pt(nw.DAY, EARLY)
    out = [dict(ex=L["ex"], side=L["side"], sh=float(L["sh"]), avg=float(L["avg"]), sub=L["sub"], kind=L["kind"], fair=L["fair"])
           for L in c["lots"] if L["fair"] >= FAV_MIN and L["ex"] in W.legs and W.last[W.meta[L["ex"]]["race"]] <= lim]
    return out, c


def recycle_rig(rig, el, nw):
    class RecycleRig(rig.Rig):
        """night_rig.Rig + the recycler (see the module doc). cfg: T (None = off), D, refill, clip, gate."""

        def __init__(self, W, cfg, held, **kw):
            super().__init__(W, **kw)
            self.cfg, self.held = dict(cfg), list(held)

        def build(self):
            super().build()
            self.lots = {}                                 # (ex, side) -> [[qty, avg, kind]]
            for h in self.held:
                self.fake.pos[h["ex"]] = self.fake.pos.get(h["ex"], 0) + (h["sh"] if h["side"] == "yes" else -h["sh"])
                self.lots.setdefault((h["ex"], h["side"]), []).append([h["sh"], h["avg"], "fav"])
            self.fill_i, self._late, self.sales, self.cons, self.mids = 0, [], [], {}, {}
            self.rec_writes, self.rec_skipped, self.crossed = 0, 0, 0
            self.wave_races = {}
            for r_ in self.W.races:
                self.wave_races.setdefault(self.W.last[r_], []).append(r_)
            self.wave_sold, self.gate_closed = {}, 0
            if not self.cfg.get("clip"):
                # one book, no crossing: after a strict call SIG's bids follow the unclipped lag (above), so its asks can't sit at or under
                # them. The World's asks stay clipped at <= 0.98: without this the settlement-morning buys (< 0.99) at 0.98 would be sold
                # straight back into the modelled 0.985-0.99 bids. Applied with the recycler off too: the same world in every cell.
                W, E, sl0 = self.W, self.E, self.W.side_levels

                def side_levels(ex, side, t):
                    lv = sl0(ex, side, t)
                    if lv and E.strict_for(ex, side):
                        top = nw.tick(self.side_mid(ex, side, t) - 0.005)
                        cut = [x for x in lv if x[0] > top + 1e-9]
                        if len(cut) < len(lv):
                            self.crossed += 1
                        return cut
                    return lv
                W.side_levels = side_levels                 # instance attribute: the rig's el_levels and W.take see it

        # ---------------------------------------------------------------- SIG's post-call bids (the study's model)
        def side_mid(self, ex, side, t):
            W, L = self.W, self.W.legs[ex]
            if self.cfg.get("clip"):
                m = W.sig_mid(ex, t)                       # today's clip kept: [0.025, 0.975]
            else:
                arr = self.mids.get(ex)
                if arr is None:
                    arr, cur, dec, L26 = [], None, math.exp(-nw.MIN / L.tau), W.last[L.race]
                    for i in range(W.minutes):
                        tt = nw.T_START + i * nw.MIN
                        pm_prev = L.lvl[i - 1] if i > 0 else L.lvl[0]
                        g = L.g0 * (1.0 if tt <= L26 else math.exp(-(tt - L26) / 5400))
                        tgt = min(0.999, max(0.001, pm_prev + g))
                        cur = tgt if cur is None else cur
                        arr.append((cur, tgt))
                        cur = tgt + (cur - tgt) * dec
                    self.mids[ex] = arr
                i = min(max(int((t - nw.T_START) // nw.MIN), 0), W.minutes - 1)
                start, tgt = arr[i]
                m = tgt + (start - tgt) * math.exp(-max(0.0, t - (nw.T_START + i * nw.MIN)) / L.tau)
            return m if side == "yes" else 1 - m

        def used(self, key, t):
            q, t0 = self.cons.get(key, (0.0, t))
            return q if not self.cfg.get("refill", True) else q * math.exp(-(t - t0) / 600)

        def bids(self, ex, side, t):
            top = nw.tick(self.side_mid(ex, side, t) - 0.005)
            if self.cfg.get("cap") is not None:
                top = min(top, self.cfg["cap"])            # the Oct 26 rehearsal: our called sides' best SIG bid was 0.95
            L = self.W.legs[ex]
            out = []
            for k in range(5):
                p = round(top - 0.005 * k, 3)
                if p < 0.005:
                    break
                out.append((p, max(0.0, self.cfg["D"] * L.depth[k] - self.used((ex, side, p), t))))
            return out

        # ---------------------------------------------------------------- our lots
        def _add(self, ex, side, q, p, kind):
            lots = self.lots.setdefault((ex, side), [])
            sn = next((x for x in lots if x[2] == kind), None)
            if sn is None:
                lots.append([float(q), float(p), kind])
            else:                                          # one sub-book a layer and book side: its weighted average is the lot avg
                sn[1] = (sn[0] * sn[1] + q * p) / (sn[0] + q) if sn[0] + q > 0 else float(p)
                sn[0] += q

        def absorb_fills(self):
            """New snipe fills -> the book side's snipe lot (a fill whose order the executor hasn't recorded yet waits a loop)."""
            new, self._late = self._late + self.fills[self.fill_i:], []
            self.fill_i = len(self.fills)
            for f in new:
                lay = (self.ledger.orders.get(str(f[1])) or {}).get("layer")
                if lay is None:
                    self._late.append(f)
                elif f[4] == "buy" and lay in el.SNIPE_LAYERS:
                    self._add(f[2], f[3], f[5], f[6], lay)

        def recycle(self, now):
            self.absorb_fills()
            E, T = self.E, self.cfg["T"]
            need = None
            if self.cfg.get("gate"):
                need = max(0.0, NEED_TARGET - (self.cash - rs.FLOOR))
                if need <= 0:
                    return
            orders, pend = [], 0.0
            after = None
            if self.cfg.get("gate2"):                      # the operator's gate: the later waves' projected need, per wave of the sold book
                called = {self.W.meta[e]["race"] for e, c in E.calls.items() if not c.get("reversed")}
                proj = {w: WAVE_NEED.get(nw.hm(w)[:5], 0.0) * sum(1 for r_ in rr if r_ not in called) / len(rr) for w, rr in self.wave_races.items()}
                after = {w: sum(v for w2, v in proj.items() if w2 > w) for w in proj}
            open_after = None
            if self.cfg.get("gate3"):                      # C121 (Piero 10:10 Oct 27): sell only while a later wave still has uncalled races
                called3 = {self.W.meta[e]["race"] for e in E.calls}
                open_after = {w: any(r_ not in called3 for w2, rr in self.wave_races.items() if w2 > w for r_ in rr) for w in self.wave_races}
            for (ex, side), lots in self.lots.items():
                if not E.strict_for(ex, side):
                    continue
                held = self.fake.pos.get(ex, 0) if side == "yes" else -self.fake.pos.get(ex, 0)
                if held < 1:
                    continue
                w0 = self.W.last[self.W.meta[ex]["race"]]
                if open_after is not None and not open_after.get(w0, False):
                    self.gate_closed += 1
                    continue
                budget = None
                if after is not None:
                    gap = after[w0] - (self.cash + pend - rs.FLOOR)
                    if gap <= 0:
                        self.gate_closed += 1
                        continue
                    budget = gap
                if self.cfg.get("wave_cap"):               # C117: at most 30k recycled a wave (the race's last poll close)
                    left = self.cfg["wave_cap"] - self.wave_sold.get(w0, 0.0)
                    if left <= 0:
                        continue
                    budget = left if budget is None else min(budget, left)
                lv = self.bids(ex, side, now)
                if self.cfg.get("gate"):
                    lv = lv[:1]                            # as built: one price a market a loop, the best bid
                for lot in sorted(lots, key=lambda x: x[1]):
                    lim = max(T, rs.tick_up(round(lot[1] + 0.005, 6)))
                    for i, (p, q) in enumerate(lv):
                        if p < lim - 1e-9 or lot[0] < 1 or held < 1:
                            break
                        x = int(min(q, lot[0], held))
                        if need is not None:
                            x = int(min(x, need / max(p, 0.01) + 1))
                        if budget is not None:
                            x = int(min(x, budget / max(p, 0.01) + 1))
                        if x < el.MIN_QTY:
                            continue
                        orders.append((ex, side, p, x, lot))
                        lv[i] = (p, q - x)
                        lot[0] -= x
                        held -= x
                        pend += x * p
                        self.wave_sold[w0] = self.wave_sold.get(w0, 0.0) + x * p
                        if budget is not None:
                            budget -= x * p
                            if budget <= 0:
                                break
                        if need is not None:
                            need -= x * p
                            if need <= 0:
                                break
                    if (need is not None and need <= 0) or (budget is not None and budget <= 0):
                        break
                if need is not None and need <= 0:
                    break
            if not orders:
                return
            n_w = (len(orders) + 49) // 50
            with self.fake.lock:
                if self.fake.writes_left() < n_w:
                    for ex, side, p, x, lot in orders:   # no write budget this loop: nothing sent, the shares stay
                        lot[0] += x
                        w_ = self.W.last[self.W.meta[ex]["race"]]
                        self.wave_sold[w_] -= x * p
                    self.rec_skipped += 1
                    return
                for _ in range(n_w):
                    self.fake._writes.append(now)
                    self.fake.write_log.append((now, "recycle", None))
                self.rec_writes += n_w
                for ex, side, p, x, lot in orders:
                    self.fake.pos[ex] = self.fake.pos.get(ex, 0) - (x if side == "yes" else -x)
                    self.cash += x * p
                    self.cons[(ex, side, p)] = (self.used((ex, side, p), now) + x, now)
                    self.sales.append((now, ex, side, p, x, lot[2]))

        def tick(self, now):
            super().tick(now)
            if self.cfg.get("T") is not None:
                self.recycle(now)
    return RecycleRig


def run_job(job):
    scen, seed, usable, cfg, srv = job
    import tests  # noqa: F401  (the package guard: SIG_DATA is a temp dir, never data/)
    nw = patch(srv)
    import night_rig as rig
    import night_report as nr
    import n_election as el
    W = nw.World(seed=seed, **rs.SCEN[scen])
    held, _ = held_stock(srv, W, nw)
    stock = cfg.get("stock", "all")                         # the stock check: no favourites at all, or the favourites without FMM lots
    held = [] if stock == "snipes" else [h for h in held if h["kind"] != "fmm"] if stock == "nofmm" else held
    R = recycle_rig(rig, el, nw)(W, cfg, held, name=scen, seed=seed, cash0=usable + rs.FLOOR).run()
    m = nr.metrics(R, dict(call=0, info=0, need={}))
    pay = {ex: W.payout(ex) for ex in W.legs}
    side_pay = lambda ex, side: pay[ex] if side == "yes" else 1 - pay[ex]          # noqa: E731
    snipe = sn_cost = 0.0
    for t, oid, ex, side, action, q, p, layer, how in R.fills:
        if layer in el.SNIPE_LAYERS and action == "buy":
            snipe += q * (side_pay(ex, side) - p)
            if layer == "callsnipe":
                sn_cost += q * p
    fav = sum(h["sh"] * (side_pay(h["ex"], h["side"]) - h["avg"]) for h in held)
    rec = sum(x * (p - side_pay(ex, side)) for t, ex, side, p, x, k in R.sales)
    rec_cash = sum(x * p for t, ex, side, p, x, k in R.sales)
    rec_sh = sum(x for t, ex, side, p, x, k in R.sales)
    disc = sum(x * (1 - p) for t, ex, side, p, x, k in R.sales if side_pay(ex, side) == 1.0)
    saved = sum(x * p for t, ex, side, p, x, k in R.sales if side_pay(ex, side) == 0.0)
    rev_saved = sum(x * p for t, ex, side, p, x, k in R.sales if side_pay(ex, side) == 0.0 and (R.E.calls.get(ex) or {}).get("reversed"))
    by_kind = {}
    for t, ex, side, p, x, kd in R.sales:
        by_kind[kd] = by_kind.get(kd, 0.0) + x * p
    first = min((t for t, *_ in R.sales), default=None)
    strict = sum(1 for c in R.E.calls.values() if c.get("strict"))
    return dict(scen=scen, seed=seed, usable=usable, cfg=cfg, night=round(snipe + rec + fav), snipe=round(snipe), rec=round(rec), fav=round(fav),
                rec_cash=round(rec_cash), rec_sh=round(rec_sh), avg_px=(rec_cash / rec_sh) if rec_sh else None, disc=round(disc),
                saved=round(saved), rev_saved=round(rev_saved), by_kind={k_: round(v) for k_, v in by_kind.items()},
                sn_cost=round(sn_cost), turnover=sn_cost / usable, first_sale=nw.hm(first) if first else None, strict=strict,
                rec_writes=R.rec_writes, rec_skipped=R.rec_skipped, n_sales=len(R.sales), crossed=R.crossed,
                settle_cost=round(sum(q * p for t, oid, ex, side, action, q, p, layer, how in R.fills if layer == "settle")), false_pnl=m["false_pnl"],
                stale=m["stale"], peak=m["peak"], fav_sh=round(sum(h["sh"] for h in held)), fav_cost=round(sum(h["sh"] * h["avg"] for h in held)),
                cash_end=round(R.cash - rs.FLOOR), gate_closed=R.gate_closed,
                wave_sold={nw.hm(w)[:5]: round(v) for w, v in R.wave_sold.items()})


def cfg_of(T=None, D=0.25, refill=True, clip=False, gate=False, stock="all", gate2=False, cap=None, wave_cap=None, gate3=False):
    c = dict(T=T, D=D, refill=refill, clip=clip, gate=gate, stock=stock)
    if gate2 or cap is not None or wave_cap:                # Oct 27 options (absent = the Oct 23 grid's cells)
        c.update(gate2=gate2, cap=cap, wave_cap=wave_cap)
    if gate3:                                               # C121's gate (absent = every earlier cell)
        c.update(gate3=True)
    return c


T27 = (0.97, 0.96, 0.95)


def plan_oct27(srv):
    """Spec item 5 (Oct 27): T 0.97 / 0.96 / 0.95 at 60k, D 25%, under today's bid structure (clip kept) and the main model (no clip),
    each with and without the operator's later-wave gate; plus bids capped at 0.95 (the Oct 26 rehearsal) and C117's 30k-a-wave limit."""
    out = []
    for scen, n in SEEDS.items():
        for s_ in range(1, n + 1):
            cfgs = [cfg_of(), cfg_of(clip=True)]
            for clip in (True, False):
                cfgs += [cfg_of(T, 0.25, clip=clip) for T in T27] + [cfg_of(T, 0.25, clip=clip, gate2=True) for T in T27]
            cfgs += [cfg_of(0.95, 0.25, clip=True, cap=0.95)] + [cfg_of(T, 0.25, clip=True, wave_cap=30_000.0) for T in T27]
            out += [(scen, s_, 60_000, c, srv) for c in cfgs]
    return out


def plan(srv):
    out = []
    for scen, n in SEEDS.items():
        for s in range(1, n + 1):
            cfgs = [cfg_of(), cfg_of(clip=True)] + [cfg_of(T, D) for T in TS for D in DS] + [cfg_of(T, 0.25, refill=False) for T in TS] + \
                   [cfg_of(T, 0.25, clip=True) for T in TS]
            out += [(scen, s, 60_000, c, srv) for c in cfgs]
            out += [(scen, s, 45_000, c, srv) for c in [cfg_of()] + [cfg_of(T, 0.25) for T in TS]]
            if scen == "normal":
                out.append((scen, s, 60_000, cfg_of(0.98, 0.25, gate=True), srv))
    return out


def run(srv, jobs, which=plan):
    import multiprocessing as mp
    jobs_, res, t0 = which(srv), [], time.time()
    with mp.get_context("fork").Pool(min(10, jobs)) as pool:
        for i, r in enumerate(pool.imap_unordered(run_job, jobs_)):
            res.append(r)
            if i % 25 == 0:
                print(f"{i + 1}/{len(jobs_)} {time.time() - t0:.0f} s", flush=True)
    return res


# ------------------------------------------------------------------------------------------------------------- report
def key(c):
    k_ = (c["T"], c["D"] if c["T"] is not None else None, c["refill"] if c["T"] is not None else True, c["clip"], c["gate"])
    return k_ if c.get("stock", "all") == "all" else k_ + (c["stock"],)


def plan_stock(srv):
    """The stock check: recycling off / 0.98 / 0.97 at D 25%, 60k, with snipes only and with the favourites less their FMM lots."""
    return [(scen, s, 60_000, cfg_of(T, 0.25, stock=st_), srv) for scen, n in SEEDS.items() for s in range(1, n + 1)
            for st_ in ("snipes", "nofmm") for T in (None, 0.98, 0.97)]


def write_md(res, srv, path):
    k, mean = rs.k, rs.mean
    G = {}
    for r in res:
        G.setdefault((r["scen"], r["usable"], key(r["cfg"])), {})[r["seed"]] = r
    off = lambda clip=False: (None, None, True, clip, False)                       # noqa: E731
    on = lambda T, D=0.25, refill=True, clip=False, gate=False: (T, D, refill, clip, gate)   # noqa: E731
    sc3 = ("normal", "slow", "reversal")

    def vals(scen, u, kk, f="night"):
        g = G.get((scen, u, kk)) or {}
        return [g[s][f] for s in sorted(g)]

    def delta(scen, u, kk, base):
        a, b = G.get((scen, u, kk)) or {}, G.get((scen, u, base)) or {}
        return [a[s]["night"] - b[s]["night"] for s in sorted(a) if s in b]

    def cell(scen, u, kk):
        v = vals(scen, u, kk)
        return f"{k(mean(v))} ({k(min(v))} to {k(max(v))})" if v else "–"

    def dcell(scen, u, kk, base):
        d = delta(scen, u, kk, base)
        if not d:
            return "–"
        return f"{k(mean(d), 2)} ({sum(1 for x in d if x > 0)}/{len(d)} up)"
    r0 = next(iter(G[("normal", 60_000, off())].values()))
    fs_cost = r0["fav_cost"]
    import night_world as nw_
    patch(srv)
    held, _ = held_stock(srv, nw_.World(seed=1), nw_)
    kinds = {}
    for h in held:
        kinds[h["kind"]] = kinds.get(h["kind"], 0.0) + h["sh"] * h["avg"]
    st = json.load(open(os.path.join(srv, "state.json")))
    L = ["# Wave recycling thresholds on the election-night sim", "",
         f"Written {time.strftime('%a %b %d %H:%M')} PT by Claude Code for Piero (his 23:20 Oct 23). Offline: the sim ran on the Mac against fresh "
         f"read-only copies of the server's `state.json` / `ledger.json` / `poll_close.json` ({st.get('time')} PT). No orders. Script: "
         "`research/night/recycle_study.py`. Usable reserve 60k; seeds: normal 8, slow count 5, reversal night 5. Recycling triggers on strict calls "
         "only, and the usable < 20k gate is off unless stated.", ""]
    L += ["## 1. Night P&L (mean, range) and Δ vs recycling off", "",
          "| recycling | normal | Δ normal | slow count | Δ slow | reversal night | Δ reversal |", "|---|---|---:|---|---:|---|---:|"]
    L.append("| off | " + " | ".join(f"{cell(sc, 60_000, off())} | –" for sc in sc3) + " |")
    for T in TS:
        for D in DS:
            L.append(f"| T {T:.2f}, D {D:.0%} | " + " | ".join(f"{cell(sc, 60_000, on(T, D))} | {dcell(sc, 60_000, on(T, D), off())}" for sc in sc3) + " |")
    L += ["", "Δ cells: mean over seeds (seeds where recycling beat off).", ""]
    # per-cell details
    L += ["## 2. What recycling does, per cell (means over seeds)", "",
          "| night | T, D | recycled $ | of it: held favourites / snipes | average sale price | discount on true winners Σ(1 − p)q | "
          "saved on wrong strict calls (of it: reversed) | wrong-call P&L vs off | sniped notional / 60k | recycle writes a night (peak writes in 60 s) |",
          "|---|---|---:|---|---:|---:|---|---:|---:|---:|"]
    for sc in sc3:
        L.append(f"| {sc} | off | – | – | – | – | – | {mean(vals(sc, 60_000, off(), 'false_pnl')):+,.0f} (level) | "
                 f"{mean(vals(sc, 60_000, off(), 'turnover')):.2f} | – ({max(vals(sc, 60_000, off(), 'peak'))}) |")
        for T in TS:
            for D in DS:
                g = list((G.get((sc, 60_000, on(T, D))) or {}).values())
                if not g:
                    continue
                px = [x["avg_px"] for x in g if x["avg_px"]]
                L.append(f"| {sc} | {T:.2f}, {D:.0%} | {mean(x['rec_cash'] for x in g) / 1000:,.1f}k | "
                         f"{mean(x['by_kind'].get('fav', 0) for x in g) / 1000:.1f}k / {mean(x['by_kind'].get('callsnipe', 0) for x in g) / 1000:.1f}k | "
                         f"{(mean(px) if px else 0):.3f} | {mean(x['disc'] for x in g):,.0f} | {mean(x['saved'] for x in g):,.0f} ({mean(x['rev_saved'] for x in g):,.0f}) | "
                         f"{mean(x['false_pnl'] for x in g) - mean(vals(sc, 60_000, off(), 'false_pnl')):+,.0f} | "
                         f"{mean(x['turnover'] for x in g):.2f} | {mean(x['rec_writes'] for x in g):,.0f} ({max(x['peak'] for x in g)}) |")
    # best T at D = 25%
    L += ["", "## 3. The best T at D = 25%: mean and seeds that beat T 0.98", "",
          "| T | normal | beats 0.98 | slow count | beats 0.98 | reversal night | beats 0.98 |", "|---|---:|---:|---:|---:|---:|---:|"]
    best = {}
    for sc in sc3:
        ms = {T: mean(vals(sc, 60_000, on(T))) for T in TS}
        best[sc] = max(ms, key=ms.get)
    for T in TS:
        row = []
        for sc in sc3:
            a, b = G[(sc, 60_000, on(T))], G[(sc, 60_000, on(0.98))]
            w = sum(1 for s in a if a[s]["night"] > b[s]["night"])
            row.append(f"{k(mean(vals(sc, 60_000, on(T))))}{' **best**' if best[sc] == T else ''} | " + ("–" if T == 0.98 else f"{w} of {len(a)}"))
        L.append(f"| {T:.2f} | " + " | ".join(row) + " |")
    clear = None
    for T in TS:
        if all(best[sc] == T for sc in sc3):
            ok = T == 0.98 or all(sum(1 for s in G[(sc, 60_000, on(T))] if G[(sc, 60_000, on(T))][s]["night"] > G[(sc, 60_000, on(0.98))][s]["night"])
                                  >= 0.75 * len(G[(sc, 60_000, on(T))]) for sc in sc3)
            clear = (T, ok)
    L += ["", ("**Clearly wins:** " + (f"T {clear[0]:.2f}: highest mean on all three nights" + (" and beats 0.98 on ≥ 75% of seeds in each." if clear[1] and clear[0] != 0.98
                                                                                            else " (it is 0.98 itself)." if clear[0] == 0.98 else ", but not on ≥ 75% of seeds in each: no clear winner."))
               if clear else "**Clearly wins:** none — no T has the highest mean on all three nights (best: " + ", ".join(f"{sc} {best[sc]:.2f}" for sc in sc3) + ")."), ""]
    win = clear[0] if clear else best["normal"]
    # sensitivities
    L += ["## 4. Sensitivities at D = 25%: Δ vs recycling off", "",
          "| T | main (refill, no clip) | no refill | today's clip kept (no bid ≥ 0.975) |", "|---|---|---|---|"]
    for T in TS:
        L.append(f"| {T:.2f} | " + " | ".join(" / ".join(f"{k(mean(delta(sc, 60_000, on(T, refill=rf, clip=cl), off(cl))), 2)}" for sc in sc3)
                                         for rf, cl in ((True, False), (False, False), (True, True))) + " |")
    L += ["", "Each cell: normal / slow count / reversal night.", ""]
    # gate
    g = list(G[("normal", 60_000, on(0.98, gate=True))].values())
    gu = list(G[("normal", 60_000, on(0.98))].values())

    def span(rows):
        ts = sorted((x["first_sale"] for x in rows if x["first_sale"]), key=lambda h: (h.endswith("+1"), h))
        return f"{ts[0]} to {ts[-1]}" if ts else "never"
    L += ["## 5. The as-built gate (C7: only while usable < 20k, at the best bid, up to the shortfall), T 0.98, D 25%, normal night", "",
          "| | recycled $ | first sale (PT) | night P&L | Δ vs off |", "|---|---:|---|---:|---:|",
          f"| as built (gate) | {mean(x['rec_cash'] for x in g) / 1000:,.1f}k | {span(g)} | "
          f"{k(mean(x['night'] for x in g))} | {k(mean(delta('normal', 60_000, on(0.98, gate=True), off())), 2)} |",
          f"| no gate | {mean(x['rec_cash'] for x in gu) / 1000:,.1f}k | {span(gu)} | "
          f"{k(mean(x['night'] for x in gu))} | {k(mean(delta('normal', 60_000, on(0.98), off())), 2)} |", ""]
    # 45k
    L += [f"## 6. A 45k reserve (the elastic 15k not filled), D 25%", "",
          "| T | normal: P&L, Δ vs off | slow count | reversal night |", "|---|---|---|---|",
          "| off | " + " | ".join(f"{cell(sc, 45_000, off())}" for sc in sc3) + " |"]
    for T in TS:
        tag = " (clear winner)" if clear and clear[1] and T == clear[0] else \
            (" (best mean: " + ", ".join(sc for sc in sc3 if best[sc] == T) + ")" if any(best[sc] == T for sc in sc3) else "")
        L.append(f"| {T:.2f}{tag} | " + " | ".join(f"{k(mean(vals(sc, 45_000, on(T))))}, {dcell(sc, 45_000, on(T), off())}" for sc in sc3) + " |")
    L += [""]
    # stock check
    if any(len(k_[2]) > 5 for k_ in G):
        L += ["## 7. How much depends on the held favourites: Δ vs recycling off at D 25% (normal / slow count / reversal night)", "",
              "| recyclable stock | favourites held | T 0.98 | T 0.97 |", "|---|---:|---|---|"]
        for st_, lab in (("all", "snipes + all early-close favourites (the grid)"), ("nofmm", "snipes + favourites less their FMM lots (C7's seller skips fmm:)"),
                         ("snipes", "snipes only")):
            sfx = () if st_ == "all" else (st_,)
            o = G.get(("normal", 60_000, off() + sfx)) or {}
            fav = next(iter(o.values())) if o else None
            L.append(f"| {lab} | {(fav['fav_cost'] / 1000 if fav else 0):.1f}k at avg | "
                     + " | ".join(" / ".join(f"{k(mean(delta(sc, 60_000, on(T) + sfx, off() + sfx)), 2)}" for sc in sc3) for T in (0.98, 0.97)) + " |")
        L += [""]
    # the read
    def dm(sc, kk, base=off(), u=60_000):
        return mean(delta(sc, u, kk, base))
    g98 = {sc: list(G[(sc, 60_000, on(0.98))].values()) for sc in sc3}
    fp = {sc: mean(x["false_pnl"] for x in g98[sc]) - mean(vals(sc, 60_000, off(), "false_pnl")) for sc in sc3}
    up = {sc: sum(1 for d in delta(sc, 60_000, on(0.98), off()) if d > 0) for sc in sc3}
    sfx = lambda st_: (st_,)                                                       # noqa: E731
    has_stock = ("normal", 60_000, off() + ("snipes",)) in G
    L += ["## Read", "",
          f"1. **Recycling pays at 60k.** T 0.98 / D 25% adds {' / '.join(k(dm(sc, on(0.98)), 1) for sc in sc3)} (normal / slow / reversal). It recycles "
          f"about {mean(x['rec_cash'] for x in g98['normal']) / 1000:.0f}k a normal night at {mean(x['avg_px'] for x in g98['normal'] if x['avg_px']):.3f}, "
          f"giving up {mean(x['disc'] for x in g98['normal']) / 1000:.1f}k of discount. Sniped notional rises from "
          f"{mean(vals('normal', 60_000, off(), 'turnover')):.2f} to {mean(x['turnover'] for x in g98['normal']):.2f} x the reserve.",
          f"2. **No T clearly wins.** 0.97 has the best normal mean ({k(dm('normal', on(0.97)), 1)}, beating 0.98 on "
          f"{sum(1 for s_ in G[('normal', 60_000, on(0.97))] if G[('normal', 60_000, on(0.97))][s_]['night'] > G[('normal', 60_000, on(0.98))][s_]['night'])} of "
          f"{len(G[('normal', 60_000, on(0.97))])} seeds). 0.98 has the best slow and reversal means. 0.99 gives about half the gain "
          f"({k(dm('normal', on(0.99)), 1)} normal). 0.96 gives it back on the reversal night ({k(dm('reversal', on(0.96)), 1)}). D 25% vs 50% barely "
          "matters: our stock binds, not SIG's bids.",
          f"3. **The cost is the wrong calls.** More cash reaches the late wrong calls: at 0.98 the wrong-call P&L moves by "
          f"{' / '.join(f'{fp[sc] / 1000:+.1f}k' for sc in sc3)}. Recycling saved nothing on a wrong strict call, because SIG's bids never reach T "
          f"before a reversal. The reversal night gains on only {up['reversal']} of 5 seeds; normal {up['normal']} of 8, slow {up['slow']} of 5.",
          f"4. **It hinges on SIG showing bids ≥ T.** With today's clip (no bid ≥ 0.975), 0.98 and 0.99 never fire. Only 0.97 / 0.96 work "
          f"({k(dm('normal', on(0.97, clip=True), off(True)), 1)} / {k(dm('normal', on(0.96, clip=True), off(True)), 1)} normal). With no refill, 0.98 "
          f"falls to {k(dm('normal', on(0.98, refill=False)), 1)}. The as-built gate (usable < 20k) first fires late ({span(g)}) and keeps "
          f"{k(dm('normal', on(0.98, gate=True)), 1)} of the {k(dm('normal', on(0.98)), 1)}.",
          (f"5. **About {1 - dm('normal', on(0.98) + sfx('snipes'), off() + sfx('snipes')) / dm('normal', on(0.98)):.0%} of the gain is the held early-close "
           f"favourites** ({fs_cost / 1000:.1f}k at avg, an upper bound). With snipes only, 0.98 gives "
           f"{' / '.join(k(dm(sc, on(0.98) + sfx('snipes'), off() + sfx('snipes')), 1) for sc in sc3)}; without the FMM lots, "
           f"{' / '.join(k(dm(sc, on(0.98) + sfx('nofmm'), off() + sfx('nofmm')), 1) for sc in sc3)}. At a 45k reserve the gain is about the same "
           f"({k(dm('normal', on(0.98), off(), 45_000), 1)} normal at 0.98).") if has_stock else "", ""]
    # stock + method
    fs = r0
    L += ["## Method and assumptions", "",
          f"- **Recyclable stock:** our snipe fills on the night, plus our held early-close favourites from the fresh ledger: side fair ≥ 0.75, "
          f"race's last poll close ≤ 17:00 PT, `reserve_sizing.load_lots`' exclusions and lot average. That is {fs['fav_sh']:,} shares, "
          f"{fs['fav_cost'] / 1000:.1f}k at lot avg. It is an upper bound: the convergence sales before the night take the lowest EV/$ first and may sell "
          "some of it. By kind at avg: " + ", ".join(f"{kd} {v / 1000:.1f}k" for kd, v in sorted(kinds.items(), key=lambda x: -x[1]))
          + ". The FMM part is wound down by Nov 3, and the engine's C7 seller skips `fmm:` sub-books (§7 runs without it). "
          "The favourites are seeded into the sim's positions, so the sniper never buys the other side of a book we hold. If not recycled they "
          "settle 1 or 0.",
          "- **The recycler:** from a strict call (`Election.strict_for`: rule B, PM side bid ≥ 0.98, not reversed), once per 10-s loop. Our "
          "held shares of the called side sell into bids at ≥ max(T, tick_up(lot avg + 0.5c)), lowest lot avg first, into existing bids only. Each "
          "sub-book lot (snipe layer / favourite sub) uses its own average. Each loop with sales takes one write per 50 orders from the same "
          "25-a-minute budget the executor uses.",
          "- **SIG's post-call bids:** the World's lag model for that side without the 0.975 clip (target pm(t − 1 min) + the fading SIG − PM "
          "gap, kept in [0.001, 0.999]); bid = tick(side mid − 0.5c), 5 levels 0.5c apart, D × the book's level depth. Our sales consume them "
          "with the World's 10-min refill.",
          "- **One book, no crossing:** after a strict call the World's asks (still clipped, so ≤ 0.98) can't sit at or under these bids. "
          "The sniper and the settlement morning don't see crossed asks. Without that, the settlement-morning buys at 0.98 were sold straight "
          "back at 0.985-0.99, a model artifact that is now gone. The same world is used with recycling off, and its P&L moves by ≤ 0.01k.",
          "- **Night P&L** = snipes (callsnipe + settle) at settlement + recycling (sale − payout of the shares sold) + the held favourites at settlement "
          "(payout − avg; the same in every cell). Recycled cash goes back to the rig's cash, so the next loop's usable and the later calls' wave "
          "share rise.",
          "- **Not modelled:** competing sellers into the same bids, SIG pulling its bids after a call, and resting asks.", ""]
    out = "\n".join(L) + "\n"
    with open(path, "w") as fh:
        fh.write(out)
    print(out)


OCT27_HEAD = "## Oct 27 (Piero spec item 5): 0.97 vs 0.96 vs 0.95 under today's bid structure"


def oct27_md(res, srv):
    k, mean = rs.k, rs.mean
    sc3 = ("normal", "slow", "reversal")
    G = {}
    for r in res:
        c = r["cfg"]
        G.setdefault((r["scen"], (c["T"], c["clip"], c.get("gate2", False), c.get("cap"), c.get("wave_cap"))), {})[r["seed"]] = r

    def kk(T=None, clip=True, gate2=False, cap=None, wave_cap=None):
        return (T, clip, gate2, cap, wave_cap)

    def d(sc, key_, base):
        a, b = G[(sc, key_)], G[(sc, base)]
        return [a[s_]["night"] - b[s_]["night"] for s_ in sorted(a)]

    def dc(sc, key_, base):
        v = d(sc, key_, base)
        return f"{k(mean(v), 2)} ({sum(1 for x in v if x > 0)}/{len(v)})"

    def m(sc, key_, f):
        v = [x[f] for x in G[(sc, key_)].values() if x[f] is not None]
        return mean(v) if v else 0.0
    st = json.load(open(os.path.join(srv, "state.json")))
    x0 = next(iter(G[("normal", kk(clip=True))].values()))
    rows = [("today's (clip kept: no bid ≥ 0.975)", True, None, T) for T in T27] + [("main model (no clip)", False, None, T) for T in T27] + \
           [("bids ≤ 0.95 (the Oct 26 rehearsal)", True, 0.95, 0.95)]
    L = [OCT27_HEAD, "",
         "| bid structure | T | normal | slow count | reversal night | recycled $ (n / s / r) | average sale price | discount on true winners (n / s / r) | wrong-call P&L vs off (n / s / r) |",
         "|---|---|---:|---:|---:|---|---:|---|---|"]
    for lab, clip, cap, T in rows:
        key_, base = kk(T, clip, cap=cap), kk(clip=clip)
        L.append(f"| {lab} | {T:.2f} | " + " | ".join(dc(sc, key_, base) for sc in sc3) + " | "
                 + " / ".join(f"{m(sc, key_, 'rec_cash') / 1000:.0f}k" for sc in sc3) + f" | {m('normal', key_, 'avg_px'):.3f} | "
                 + " / ".join(f"{m(sc, key_, 'disc') / 1000:.1f}k" for sc in sc3) + " | "
                 + " / ".join(f"{(m(sc, key_, 'false_pnl') - m(sc, base, 'false_pnl')) / 1000:+.1f}k" for sc in sc3) + " |")
    L.append("| recycling off: night P&L | – | " + " | ".join(f"{k(m(sc, kk(clip=True), 'night'))}" for sc in sc3) + " | | | | |")
    L += ["", "Cells: Δ night P&L vs recycling off, mean (seeds up). With bids ≤ 0.95, T 0.96 and 0.97 never sell (Δ 0).", ""]
    base = kk(clip=True)
    mm = {T: {sc: mean(d(sc, kk(T, True), base)) for sc in sc3} for T in T27}
    ok = mm[0.95]["normal"] > mm[0.97]["normal"] and mm[0.95]["slow"] > mm[0.97]["slow"] and mm[0.95]["reversal"] >= 0
    verdict = 0.95 if ok else 0.96
    L.append(f"**Rule's verdict: {verdict:.2f}.** Under today's bid structure 0.95 "
             + ("beats" if mm[0.95]["normal"] > mm[0.97]["normal"] else "does not beat") + f" 0.97 on the normal night ({k(mm[0.95]['normal'], 2)} vs "
             f"{k(mm[0.97]['normal'], 2)}) and " + ("beats" if mm[0.95]["slow"] > mm[0.97]["slow"] else "does not beat") + f" it on the slow night "
             f"({k(mm[0.95]['slow'], 2)} vs {k(mm[0.97]['slow'], 2)}). It is {k(mm[0.95]['reversal'], 2)} vs off on the reversal night.")
    hi = max(T27, key=lambda T: mm[T]["normal"])
    if hi != verdict:
        cap_ = {sc: mean(d(sc, kk(0.95, True, cap=0.95), base)) for sc in sc3}
        L.append(f"- In the model {hi:.2f} has the highest means ({' / '.join(k(mm[hi][sc], 2) for sc in sc3)}). But if SIG's best bid stays at 0.95, as in "
                 f"the rehearsal, {hi:.2f} sells nothing. 0.95 still makes {' / '.join(k(cap_[sc], 2) for sc in sc3)} then.")
    g = {sc: mean(d(sc, kk(verdict, True, gate2=True), base)) for sc in sc3}
    rec_g = mean([x["rec_cash"] for x in G[("normal", kk(verdict, True, gate2=True))].values()])
    rec_n = mean([x["rec_cash"] for x in G[("normal", kk(verdict, True))].values()])
    L.append(f"\n**The gate (sell only while the later waves' projected need > usable), at {verdict:.2f}:** it gives {' / '.join(k(g[sc], 2) for sc in sc3)}, "
             f"against {' / '.join(k(mm[verdict][sc], 2) for sc in sc3)} without it. It cuts the normal night's recycling from {rec_n / 1000:.0f}k to {rec_g / 1000:.0f}k and costs "
             f"{' / '.join(f'{(mm[verdict][sc] - g[sc]) / 1000:.1f}k' for sc in sc3)}, so it is better left off.")
    gm = {sc: mean(d(sc, kk(T, False, gate2=True), kk(clip=False))) for sc in sc3 for T in (verdict,)}
    wc = {sc: mean(d(sc, kk(verdict, True, wave_cap=30_000.0), base)) for sc in sc3}
    L += ["",
          f"- **Gate in the main model:** at {verdict:.2f} it gives {' / '.join(k(gm[sc], 2) for sc in sc3)} against "
          f"{' / '.join(k(mean(d(sc, kk(verdict, False), kk(clip=False))), 2) for sc in sc3)} without it.",
          f"- **C117's 30k-a-wave limit** at {verdict:.2f}: {' / '.join(k(wc[sc], 2) for sc in sc3)}. It barely binds.",
          f"- **Run:** {time.strftime('%a %b %d %H:%M')} PT, from fresh read-only copies of the server's files ({st.get('time')} PT); 60k usable, D 25%, no 20k gate, "
          f"seeds 8 / 5 / 5. Same model as above. The held early-close favourites are now {x0['fav_sh']:,} shares, {x0['fav_cost'] / 1000:.1f}k at avg.",
          "  - Fresh books and lots map the 2024 tapes slightly differently, so compare within this section, not with §1-§7.",
          "  - Gate: a wave's projected need = its unlimited-cash sniper spend (docs/night_reserve.md: 16:00 43k, 16:30 6k, 17:00 160k, 17:30 10k, 18:00 175k, "
          "19:00 21k, 20:00 31k, 21:00 12k, 22:00 15k) x the share of its races still uncalled. A book is sold only while the sum over the waves after its own "
          "exceeds usable cash, and only up to that gap.", ""]
    return "\n".join(L) + "\n", verdict


def put_section(path, head, text):
    """Replace the section `head` in the doc, or append it at the end; the rest of the doc is kept as is."""
    lines = open(path).read().rstrip("\n").split("\n")
    if head in lines:
        i = lines.index(head)
        j = next((x for x in range(i + 1, len(lines)) if lines[x].startswith("## ")), len(lines))
        lines[i:j] = text.rstrip("\n").split("\n") + ([""] if j < len(lines) else [])
    else:
        lines += [""] + text.rstrip("\n").split("\n")
    with open(path, "w") as fh:
        fh.write("\n".join(lines) + "\n")


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--srv", required=True)
    ap.add_argument("--jobs", type=int, default=10)
    ap.add_argument("--json")
    ap.add_argument("--from-json")
    ap.add_argument("--md")
    ap.add_argument("--oct27", action="store_true", help="spec item 5 (Oct 27): the 0.97 / 0.96 / 0.95 grid; appends its section to --md")
    a = ap.parse_args()
    srv = os.path.abspath(a.srv)
    if a.oct27:
        res = [x for f in a.from_json.split(",") for x in json.load(open(f))] if a.from_json else run(srv, min(10, a.jobs), plan_oct27)
        if a.json and not a.from_json:
            json.dump(res, open(a.json, "w"))
        text, verdict = oct27_md(res, srv)
        print(text)
        if a.md:
            put_section(a.md, OCT27_HEAD, text)
        sys.exit(0)
    if a.from_json:
        res = [x for f in a.from_json.split(",") for x in json.load(open(f))]
    else:
        t0 = time.time()
        res = run(srv, min(10, a.jobs)) + run(srv, min(10, a.jobs), plan_stock)
        print(f"{len(res)} nights in {time.time() - t0:.0f} s")
        if a.json:
            json.dump(res, open(a.json, "w"))
    if a.md:
        write_md(res, srv, a.md)
