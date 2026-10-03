"""E. BOTS: pure decision logic, no I/O. The engine (f_engine.py) feeds each bot a snapshot and places what it wants.

  E1  helpers          want / matches / reconcile / trim_sells / exposure
  E2  PositionBot      a manual position, worked out with resting exits (AK, RI)
  E3  MarketBot        one per market: exit inventory, take gaps, join the SIG touch, ladder for sweeps
  E4  PairBot          one per race: riskless sets across the party markets (D + R [+ I])

Prices: SIG books are YES-normalized; an order's price is in its own side (a NO buy at 0.30 == selling YES at 0.70).
"""
import math

from a_config import (TICK, MIN_QTY, QTY_TOL, MARKET_MAX_POS, MB_MIN_PX, MB_MAX_PX, ROC_MIN, ROC_REF, ROC_MIN_TIGHT, DUMP_TTL_S, TMM_BUY_EDGE, TMM_TTL_S, RECYCLE_BID_EDGE,
                      RECYCLE_REST_EDGE, RECYCLE_TAKE_TTL_S, RECYCLE_REST_TTL_S, MM_EDGE, MM_U_MULT, MM_NOTIONAL, MM_TTL_S,
                      LADDER_LEVELS, LADDER_TTL_S, TAKE_EDGE, TAKE_U_MULT, TAKE_NOTIONAL, TAKE_TTL_S, FAT_TTL_S)

EPS = 1e-9


# =================================================================================================== E1. helpers

def tick_down(p):
    return round(math.floor(p / TICK + EPS) * TICK, 3)


def want(side, action, px, qty, lo, hi, qtol, layer="", qmax=None, sub=None, ttl=None, roc=None, reduce=False):
    """A desired resting order. A resting order matches it when side/action (and layer, sub-book when both are known)
    are equal, lo <= price <= hi, |qty - want| <= qtol * want, and (qmax) it isn't larger than qmax."""
    return dict(side=side, action=action, px=round(px, 3), qty=int(qty), lo=lo, hi=hi, qtol=qtol, layer=layer, qmax=qmax,
                sub=sub, ttl=ttl, roc=roc, reduce=reduce)


def matches(o, w):
    if o.get("layer") and w.get("layer") and o["layer"] != w["layer"]:
        return False
    if o.get("sub") and w.get("sub") and o["sub"] != w["sub"]:
        return False
    return (o["side"] == w["side"] and o["action"] == w["action"] and w["lo"] - EPS <= o["price"] <= w["hi"] + EPS
            and abs(o["qty"] - w["qty"]) <= w["qtol"] * w["qty"] + EPS and (w.get("qmax") is None or o["qty"] <= w["qmax"] + EPS))


def reconcile(wants, resting):
    """-> (kept orders, orders to cancel, wants to place)."""
    used, kept, cancels = set(), [], []
    for o in resting:
        j = next((i for i, w in enumerate(wants) if i not in used and matches(o, w)), None)
        if j is None:
            cancels.append(o)
        else:
            used.add(j)
            kept.append(o)
    return kept, cancels, [w for i, w in enumerate(wants) if i not in used]


def trim_sells(held, kept, places):
    """Never sell more than held: surviving resting sells + new sells <= held (cheapest new sells trimmed first)."""
    room = held - sum(o["qty"] for o in kept if o["action"] == "sell")
    out = []
    for w in sorted(places, key=lambda w: -w["px"]):
        if w["action"] == "sell":
            q = int(max(0, min(w["qty"], room)))
            room -= q
            if q < MIN_QTY:
                continue
            w = dict(w, qty=q)
        out.append(w)
    return out


def exposure(fair, inv):
    """Signed SIG at fair: + long YES, - long NO."""
    return inv * fair if inv > 0 else inv * (1 - fair)


def side_book(bid, ask, side):
    """(best bid, best ask) of one side from the YES book."""
    if side == "yes":
        return bid, ask
    return (None if ask is None else round(1 - ask, 3)), (None if bid is None else round(1 - bid, 3))


# =================================================================================================== E2. PositionBot

class PositionBot:
    """Works out a manual position with resting limit exits, allocated top-down so a sweep fills the cheapest first.
    Exits below the Polymarket fair are held back; adds are far bids that add on spikes."""
    kind = "position"

    def __init__(self, name, ex, market, exits, adds, ref, side, max_add=1.5):
        self.name, self.ex, self.market = name, ex, market
        self.exits = sorted(exits, key=lambda e: -e[0])
        self.adds, self.ref, self.side, self.max_add = adds, ref, side, max_add
        self.status, self.ladder = "init", []

    def plan(self, s):
        pos = s["pos"]
        held = int(abs(pos))
        if held < MIN_QTY:
            self.status, self.ladder = "flat", [dict(px=px, yes_px=self._yes(px), qty=0, state="filled-out") for px, _ in self.exits]
            return []
        side = "yes" if pos > 0 else "no"
        if side != self.side:
            self.status, self.ladder = f"held side flipped to {side}: no orders", []
            return []
        fair = s["fair"] if side == "yes" or s["fair"] is None else 1 - s["fair"]
        skipped = {px for px, _ in self.exits if fair is not None and px < fair - EPS}
        left = held - sum(int(f * self.ref) for px, f in self.exits if px in skipped)
        alloc = {}
        for px, f in self.exits:
            if px not in skipped:
                alloc[px] = max(0, min(int(f * self.ref), left))
                left -= alloc[px]
        active = [px for px, q in alloc.items() if q > 0]
        if left > 0 and active:
            alloc[min(active)] += left
        out = [want(side, "sell", px, q, px, px, 0.05, "exit", sub="manual") for px, q in alloc.items() if q >= MIN_QTY]
        if held < self.max_add * self.ref:
            out += [want(side, "buy", px, n / px, px, px, QTY_TOL, "add", sub="manual") for px, n in self.adds if n / px >= MIN_QTY]
        if s.get("recycle") and s.get("fair") is not None:      # D1: the share released to the recycler
            out += recycle_wants(s["recycle"], s["fair"], s.get("bid"), s.get("ask"), s.get("kappa", 0.0), s.get("own", ()))
        self.ladder = [dict(px=px, yes_px=self._yes(px), qty=alloc.get(px, 0),
                            state="skipped" if px in skipped else "resting" if alloc.get(px, 0) >= MIN_QTY else "filled-out")
                       for px, _ in self.exits]
        self.status = f"holding {side.upper()} {held:,}: {len(active)} exits" + (f", {len(skipped)} below fair held" if skipped else "")
        return out

    def _yes(self, px):
        return round(px if self.side == "yes" else 1 - px, 3)

    def urgency(self, o):
        return 0.0


# =================================================================================================== E3. MarketBot

def s_of(p):
    """Price scale (plan_review A5): 1 near 0.5, 0.3 at the extremes."""
    p = min(max(p, 0.0), 1.0)
    return max(0.3, 2 * math.sqrt(p * (1 - p)))


def roc_of(fair_side, px):
    """Return on capital of buying a side at px when it is worth fair_side (A3)."""
    return (fair_side - px) / px if px > 0 else 0.0


def roc_size(notional, roc):
    """A3: size scales with ROC, clipped to 0.5x..2x of the base notional."""
    return notional * min(2.0, max(0.5, roc / ROC_REF))


def tick_up(p):
    return round(math.ceil(p / TICK - EPS) * TICK, 3)


def recycle_wants(subs, fair, bid, ask, kappa, own=(), take_only=False):
    """Recycler (night plan §5.1, A2) for holding sub-books {sub: signed YES shares}: sell into the SIG bid while it is
    >= fair_side + 0.5c (marketable), else rest an exit at max(fair_side + 1c, best ask - 1 tick); never below
    fair_side - kappa. Each sell is the complement BUY at 1 - price (positions net on SIG). I4: our own resting orders
    (own) are not the market: we never sell into our own bid, and never undercut our own ask (we keep its price)."""
    out, sf = [], {"yes": fair, "no": 1 - fair}
    for sub, h in sorted(subs.items()):
        if abs(h) < MIN_QTY:
            continue
        have = "yes" if h > 0 else "no"
        close = "no" if have == "yes" else "yes"
        bid_h, ask_h = side_book(bid, ask, have)
        own_ask = {round(1 - o["price"], 3) for o in own if o["action"] == "buy" and o["side"] == close} | \
                  {round(o["price"], 3) for o in own if o["action"] == "sell" and o["side"] == have}
        own_bid = {round(o["price"], 3) for o in own if o["action"] == "buy" and o["side"] == have}
        if bid_h is not None and round(bid_h, 3) in own_bid:
            bid_h = None
        keep = ask_h is not None and round(ask_h, 3) in own_ask
        if bid_h is not None and bid_h >= sf[have] + RECYCLE_BID_EDGE - EPS:
            sell, ttl, band = bid_h, RECYCLE_TAKE_TTL_S, 0.005
        elif take_only:                                     # other layers' holdings: only sell into a bid above fair
            continue
        else:
            best = ask_h if keep else (ask_h - TICK) if ask_h is not None else 0
            sell, ttl, band = max(sf[have] + RECYCLE_REST_EDGE, best), RECYCLE_REST_TTL_S, 0.01
        px = tick_down(1 - max(sell, sf[have] - kappa))
        if MB_MIN_PX <= px <= MB_MAX_PX:
            out.append(want(close, "buy", px, int(abs(h)), px - band, px, 2 * QTY_TOL, "recycle", qmax=int(abs(h)), sub=sub,
                            ttl=ttl, reduce=True))
    return out


class MarketBot:
    """One per market. Every order is a canonical BUY (a sell of the held side is the complement buy at 1 - price), so
    positions net cleanly on SIG (A1). Layers (each gated by its flag in data/overrides.json, via s["layers"]):
      recycle  (recycler) each holding sub-book ('inventory', 'recycle:*'): sell into the SIG bid while it is >= fair_side
               + 0.5c (marketable), else rest an exit at max(fair_side + 1c, best ask - 1 tick); never below fair_side - kappa
      mm       (MM v2, A4) two-sided at fair -/+ e(p), e = max(2c, 1.5u) * s(p), +/-1c inventory skew at MARKET_MAX_POS,
               ROC filter on entries; the side that reduces the 'mm' sub-book is always quoted (A2 floor)
      ladder   (A5) deep bids at min(ref_side - off_k, fair_side - 2c*s(p)), off_k = (10c, 20c) * s(p), anchored to SIG's own
               30-min median mid; never at or above the side's best ask; TTL 4 h; re-placed only when the target moves > off/2
      rebound  (A1/A5) each ladder tranche sub-book is closed by the complement buy at 1 - max(ref_side - 0.3d, fair_side -
               kappa, fill + 1c); the news guard (engine) hands a tranche to the recycler when fair falls > off/2
      take     (sniper / A9) buy a SIG ask that gapped >= max(4c*s(p), 1.5u) below fair (TTL 5 min)
    Entries need ROC >= ROC_MIN and room in the market cap; exits, rebounds and the reducing MM side are exempt."""
    kind = "market"

    def __init__(self, ex, market):
        self.ex, self.market = ex, market
        self.name = f"MKT {market['race']} {market['party']}"
        self.status, self.fair, self.orders = "init", None, []

    def plan(self, s):
        f, L, k = s["fair"], s.get("layers", {}), s.get("kappa", 0.0)
        self.fair = f
        fat = [want(x["side"], "buy", x["px"], x["qty"], x["px"] - TICK / 2, x["px"] + TICK / 2, x.get("qtol", QTY_TOL),
                    "fatfinger", sub=x["sub"], ttl=FAT_TTL_S) for x in (s.get("fatfinger") or [])]   # day 2 §4: own caps
        if f is None:
            self.status, self.orders = "no fair (Polymarket missing, wide or stale)" + (" · fat-finger" if fat else ""), fat
            return fat
        sf, u = {"yes": f, "no": 1 - f}, s.get("u") or 0.0
        subs = s.get("subs", {})
        entries_ok = not s.get("paused")
        blocked = s.get("blocked", set())
        room = dict(s.get("room", {"yes": MARKET_MAX_POS, "no": MARKET_MAX_POS}))
        out = []

        def add_entry(side, px, notional, layer, lo, hi, ttl, sub=None, qtol=2 * QTY_TOL):
            if not entries_ok or side in blocked or not MB_MIN_PX <= px <= MB_MAX_PX:
                return
            r = roc_of(sf[side], px)
            rmin = s.get("roc_min", ROC_MIN)
            held = s.get("held", sum(subs.values()))
            if layer == "mm" and sf[side] >= 0.95 and ((side == "yes" and held > 0) or (side == "no" and held < 0)):
                rmin = max(rmin, ROC_MIN_TIGHT)         # §3.4: no more cheap-return favourites we already hold
            if layer == "mm":
                rmin = max(rmin, s.get("roc_hold", 0.0))    # afternoon §2.3: a classic MM entry is held to settlement
            if r < rmin - EPS:
                return
            n = min(roc_size(notional, r), room[side])
            q = int(n / px)
            if q < MIN_QTY:
                return
            room[side] -= q * px
            out.append(want(side, "buy", px, q, lo, hi, qtol, layer, sub=sub, ttl=ttl, roc=round(r, 4)))

        def reduce_buy(have, sell_px, qty, layer, sub, ttl, band=0.01, a2=True):
            """Close `qty` of a holding on `have` by buying the complement at 1 - sell_px (never below fair - kappa, unless
            a2=False: the dump catcher's rebound, the documented A2 exception)."""
            if a2:
                sell_px = max(sell_px, sf[have] - k)
            px = tick_down(1 - sell_px)
            if qty >= MIN_QTY and MB_MIN_PX <= px <= MB_MAX_PX:
                close = "no" if have == "yes" else "yes"
                out.append(want(close, "buy", px, int(qty), px - band, px, 2 * QTY_TOL, layer, qmax=int(qty), sub=sub, ttl=ttl, reduce=True))

        # release (day 2 §3.3): sell the planned shares into the SIG bid; the recycler steps aside for those shares
        rel = s.get("release") or {}
        if rel:
            have, close = rel["side"], ("no" if rel["side"] == "yes" else "yes")
            px = round(1 - rel["px"], 3)
            for sub, q in rel["subs"].items():
                out.append(want(close, "buy", px, q, px - 0.005, px, 2 * QTY_TOL, "release", qmax=q, sub=sub, ttl=RECYCLE_TAKE_TTL_S,
                                reduce=True))
            subs = {k2: (v - (rel["subs"].get(k2, 0) if v > 0 else -rel["subs"].get(k2, 0))) for k2, v in subs.items()}
        # recycler: every holding sub-book released to it, plus a bid above fair for MM / sniper holdings; only sub-books
        # on the same side as the net position (an audit residue of the other sign is not shares we hold)
        if L.get("recycler"):
            net = sum(subs.values())
            out += recycle_wants({k2: v for k2, v in subs.items() if (k2 == "inventory" or k2.startswith("recycle:")) and v * net > 0},
                                 f, s["bid"], s["ask"], k, s.get("own", ()))
            out += recycle_wants({k2: v for k2, v in subs.items() if k2 in ("mm", "sniper") and v * net > 0},
                                 f, s["bid"], s["ask"], k, s.get("own", ()), take_only=True)

        # turnover MM (afternoon D-C): quote at SIG's own level (EWMA mid +- max(1 tick, half the spread), joining or
        # improving the touch), skewed against inventory; entries never above fair - 1c, sells never below fair - kappa_mm
        t = s.get("tmm")
        if L.get("mm") and t:
            mm = subs.get("mm", 0)
            bid, ask = s["bid"], s["ask"]
            if t.get("mid") is not None and bid is not None and ask is not None and ask > bid:
                h = max(TICK, 0.5 * (ask - bid))
                bid_y, ask_y = tick_down(t["mid"] - h), tick_up(t["mid"] + h)
                bid_y, ask_y = max(bid_y, bid), min(ask_y, ask)          # join or improve the best price
                sk = t.get("skew", 0.0) * TICK                          # long YES -> both quotes lower (sell more, buy less)
                bid_y, ask_y = tick_down(bid_y - sk), tick_up(ask_y - sk)
                if t.get("offer_mid"):                                   # inventory older than 2 h: offered at SIG mid,
                    mc = t.get("max_cost", 0.05)                         # only within the release cost cap (night §1.3)
                    if mm > 0 and t["mid"] >= f * (1 - mc) - EPS:
                        ask_y = min(ask_y, tick_up(t["mid"]))
                    elif mm < 0 and (1 - t["mid"]) >= (1 - f) * (1 - mc) - EPS:
                        bid_y = max(bid_y, tick_down(t["mid"]))
                bid_y, ask_y = min(bid_y, tick_down(ask - TICK)), max(ask_y, tick_up(bid + TICK))   # rest, never take
                km = t["kappa_mm"]
                # YES bid: closes a NO holding (sell NO >= fair_no - kappa_mm) or opens YES (<= fair - 1c)
                if mm <= -MIN_QTY:
                    if bid_y <= f + km + EPS:
                        reduce_buy("no", 1 - bid_y, abs(mm), "mm", "mm", TMM_TTL_S, band=TICK, a2=False)
                elif entries_ok and "yes" not in blocked and bid_y <= f - TMM_BUY_EDGE + EPS and MB_MIN_PX <= bid_y <= MB_MAX_PX:
                    q = int(min(t["size"], t["room"]["yes"]) / bid_y)
                    if q >= MIN_QTY:
                        out.append(want("yes", "buy", bid_y, q, bid_y - TICK, bid_y, 2 * QTY_TOL, "mm", sub="mm", ttl=TMM_TTL_S))
                # YES ask (a NO bid at 1 - ask): closes a YES holding (>= fair - kappa_mm) or opens NO (NO <= fair_no - 1c)
                no_px = round(1 - ask_y, 3)
                if mm >= MIN_QTY:
                    if ask_y >= f - km - EPS:
                        reduce_buy("yes", ask_y, mm, "mm", "mm", TMM_TTL_S, band=TICK, a2=False)
                elif entries_ok and "no" not in blocked and no_px <= (1 - f) - TMM_BUY_EDGE + EPS and MB_MIN_PX <= no_px <= MB_MAX_PX:
                    q = int(min(t["size"], t["room"]["no"]) / no_px)
                    if q >= MIN_QTY:
                        out.append(want("no", "buy", no_px, q, no_px - TICK, no_px, 2 * QTY_TOL, "mm", sub="mm", ttl=TMM_TTL_S))

        # market maker v2 (classic: held to settlement; the engine runs the turnover MM above instead)
        if L.get("mm") and not t:
            mm = subs.get("mm", 0)
            e = max(MM_EDGE, MM_U_MULT * u) * s_of(f)
            skew = 0.01 * max(-1.0, min(1.0, exposure(f, mm) / MARKET_MAX_POS))
            for side in ("yes", "no"):
                target = sf[side] - e - (skew if side == "yes" else -skew)
                bid_s, ask_s = side_book(s["bid"], s["ask"], side)
                if ask_s is not None:
                    target = min(target, ask_s - TICK)            # rest, never take
                px = tick_down(target)
                reducing = (side == "no" and mm >= MIN_QTY) or (side == "yes" and mm <= -MIN_QTY)
                if reducing:
                    have = "yes" if side == "no" else "no"
                    reduce_buy(have, 1 - px, abs(mm), "mm", "mm", MM_TTL_S)
                else:
                    add_entry(side, px, MM_NOTIONAL, "mm", px - 0.01, max(px, min(px + TICK, sf[side] - e / 2)), MM_TTL_S, sub="mm")

        # deep ladders and their rebounds
        ref = s.get("ref")
        if L.get("ladder") and ref is not None:
            for side in ("yes", "no"):
                ref_side = ref if side == "yes" else 1 - ref
                bid_s, ask_s = side_book(s["bid"], s["ask"], side)
                sp = s_of(sf[side])
                for i, (base, notional) in enumerate(LADDER_LEVELS):
                    off = max(0.015, base * sp)
                    px = tick_down(min(ref_side - off, sf[side] - 0.02 * sp))
                    if ask_s is not None and px >= ask_s - EPS:
                        continue                                    # at/above the ask it's a take, not a ladder
                    sub = (s.get("ladder_ids") or {}).get((side, i))
                    add_entry(side, px, notional, "ladder", px - off / 2, min(px + off / 2, sf[side] - 0.02 * sp),
                              LADDER_TTL_S, sub=sub, qtol=0.5)
        for t in s.get("dump_tranches", []):                        # afternoon D-A: sell the rebound (A2 exception)
            q = subs.get(t["sub"], 0)
            if abs(q) >= MIN_QTY:
                reduce_buy("yes" if q > 0 else "no", t["rebound"], abs(q), "dumpexit", t["sub"], DUMP_TTL_S, a2=False)
        for t in s.get("tranches", []):                             # rebounds run even when entries are paused
            q = subs.get(t["sub"], 0)
            if abs(q) < MIN_QTY or t.get("state") == "news":
                continue
            bought = "yes" if q > 0 else "no"
            d = t["ref_side"] - t["avg"]
            rebound = tick_up(max(t["ref_side"] - 0.3 * d, sf[bought] - k, t["avg"] + 0.01))
            reduce_buy(bought, rebound, abs(q), "rebound", t["sub"], LADDER_TTL_S)

        # takes: SIG asks that gapped far below fair (sniper layer; A9 attack)
        if L.get("sniper"):
            for side in ("yes", "no"):
                bid_s, ask_s = side_book(s["bid"], s["ask"], side)
                need = max(TAKE_EDGE * s_of(sf[side]), TAKE_U_MULT * u)
                px = tick_down(sf[side] - need)
                if ask_s is not None and ask_s <= px + EPS:
                    add_entry(side, px, s.get("take_notional", TAKE_NOTIONAL), "take", px - 0.01, px, TAKE_TTL_S, sub="sniper")

        out += fat
        out += [want(x["side"], "buy", x["px"], x["qty"], x["px"] - 0.015, x["px"], QTY_TOL, "dump", sub=x["sub"], ttl=DUMP_TTL_S)
                for x in (s.get("dump") or [])]                 # afternoon D-A: own caps and reserve fit (engine)
        self.orders = out
        layers = sorted({w["layer"] for w in out})
        net = sum(subs.values())
        self.status = ((f"holding {'YES' if net > 0 else 'NO'} {abs(net):,.0f}" if abs(net) >= MIN_QTY else "flat")
                       + (f" · {', '.join(layers)}" if layers else " · nothing to do") + ("" if entries_ok else " · new risk paused"))
        return out

    def urgency(self, o):
        """> 0 when a resting buy sits above fair (being picked off). No fair: pull first."""
        if self.fair is None:
            return 1.0
        return o["price"] - ((self.fair if o["side"] == "yes" else 1 - self.fair) - TICK / 2)


# =================================================================================================== E4. PairBot

class PairBot:
    """One per race. NO on every party market pays >= legs-1 whoever wins: buy when the YES bids sum above legs-1+edge.
    YES on every party pays 1 unless an outsider wins: buy when the YES asks sum below 1 - outsider - edge, where the
    outsider is priced at max(third_party, 1 - sum of Polymarket fairs). Legs go out atomically (multi-leg)."""
    kind = "pair"

    def __init__(self, race, legs):
        """legs: [(party, ex)]"""
        self.race, self.parties, self.legs = race, [p for p, _ in legs], [ex for _, ex in legs]
        self.name = f"PAIR {race}"
        self.last_shot, self.status, self.view = -1e9, "init", {}

    def outsider(self, fair, third):
        return max(third, 1 - sum(fair[ex] for ex in self.legs)) if all(ex in fair for ex in self.legs) else None

    def screen(self, px, fair, edge, third):
        """Top-of-book check -> [(edge, side)] for sets worth sizing; fills self.view for the dashboard."""
        q = [px.get(ex, (None, None, None)) for ex in self.legs]
        bids, asks, n = [x[0] for x in q], [x[1] for x in q], len(self.legs)
        out = self.outsider(fair, third)
        no_edge = sum(bids) - (n - 1) if None not in bids else None
        yes_edge = 1 - out - sum(asks) if None not in asks and out is not None else None
        self.view = dict(bids=bids, asks=asks, bid_sum=None if None in bids else round(sum(bids), 3),
                         ask_sum=None if None in asks else round(sum(asks), 3), outsider=None if out is None else round(out, 3),
                         no_edge=None if no_edge is None else round(no_edge, 4), yes_edge=None if yes_edge is None else round(yes_edge, 4))
        best = max(no_edge if no_edge is not None else -9, yes_edge if yes_edge is not None else -9)
        self.status = "ARB OPEN" if best >= edge - EPS else ("watching" if best > -9 else "no prices")
        return [(e, side) for side, e in (("no", no_edge), ("yes", yes_edge)) if e is not None and e >= edge - EPS]

    def size(self, side, books, fair, edge, third, max_qty):
        """books: {ex: [(yes_price, qty)]} best first (YES bids for a NO set, YES asks for a YES set), own orders removed.
        Sweeps levels while the set still locks >= edge -> ([(ex, side, 'buy', limit, qty)], qty, margin per set)."""
        n = len(self.legs)
        need = (n - 1 + edge) if side == "no" else (1 - self.outsider(fair, third) - edge)
        cums = {}
        for ex in self.legs:
            c, cums[ex] = 0, []
            for p, q in books[ex]:
                c += q
                cums[ex].append((c, p))
        worst = lambda ex, q: next((p for c, p in cums[ex] if c >= q), None)
        best_q, wpx = 0, None
        for q in sorted({c for ex in self.legs for c, _ in cums[ex]} | {max_qty}):
            q = min(q, max_qty)
            w = {ex: worst(ex, q) for ex in self.legs}
            if None in w.values():
                break
            tot = sum(w.values())
            if (side == "no" and tot < need - EPS) or (side == "yes" and tot > need + EPS):
                break
            best_q, wpx = q, w
            if q >= max_qty:
                break
        if best_q < MIN_QTY:
            return [], 0, 0.0
        orders = [(ex, side, "buy", round(1 - wpx[ex], 3) if side == "no" else wpx[ex], int(best_q)) for ex in self.legs]
        margin = (sum(wpx.values()) - (n - 1)) if side == "no" else (1 - self.outsider(fair, third) - sum(wpx.values()))
        return orders, int(best_q), margin
