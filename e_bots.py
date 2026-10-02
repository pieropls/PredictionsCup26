"""E. BOTS: pure decision logic, no I/O. The engine (f_engine.py) feeds each bot a snapshot and places what it wants.

  E1  helpers          want / matches / reconcile / trim_sells / exposure
  E2  PositionBot      a manual position, worked out with resting exits (AK, RI)
  E3  MarketBot        one per market: exit inventory, take gaps, join the SIG touch, ladder for sweeps
  E4  PairBot          one per race: riskless sets across the party markets (D + R [+ I])

Prices: SIG books are YES-normalized; an order's price is in its own side (a NO buy at 0.30 == selling YES at 0.70).
"""
import math

from a_config import (TICK, MIN_QTY, QTY_TOL, MARKET_MAX_POS, MB_EXIT_EDGE, MB_TOUCH_PM_EDGE, MB_TOUCH_NOTIONAL,
                      MB_TAKE_EDGE, MB_TAKE_NOTIONAL, MB_LADDER, MB_MIN_PX, MB_MAX_PX)

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
        out = [want(side, "sell", px, q, px, px, QTY_TOL, "exit") for px, q in alloc.items() if q >= MIN_QTY]
        if held < self.max_add * self.ref:
            out += [want(side, "buy", px, n / px, px, px, QTY_TOL, "add") for px, n in self.adds if n / px >= MIN_QTY]
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

class MarketBot:
    """One per market. Every price is checked against Polymarket (the probability) AND the SIG book (where we trade).
      exit    holding inventory -> offer it back at max(PM fair + EXIT_EDGE, SIG best ask of that side - 1 tick),
              never more shares than held, never below PM fair
      take    the SIG ask has gapped >= TAKE_EDGE through PM fair -> buy the gap with a marketable limit at fair - TAKE_EDGE
      touch   (focus markets) join/improve the SIG best bid, only if that is below the SIG mid AND below PM fair - TOUCH_EDGE
      ladder  (focus markets) resting bids at PM fair - each distance: big market orders that sweep the book fill them
    Adds only on the side we hold (or when flat), only when the account risk check allows, and not right after a fill."""
    kind = "market"

    def __init__(self, ex, market):
        self.ex, self.market = ex, market
        self.name = f"MKT {market['race']} {market['party']}"
        self.status, self.fair, self.orders = "init", None, []

    def plan(self, s):
        fair, inv = s["fair"], s["pos"]
        self.fair = fair
        if fair is None:
            self.status, self.orders = "no Polymarket fair (missing or wide)", []
            return []
        sf = {"yes": fair, "no": 1 - fair}
        expo = exposure(fair, inv)
        held = {"yes": max(expo, 0), "no": max(-expo, 0)}
        out = []
        # exit: buying the opposite side closes what we hold
        if abs(inv) >= MIN_QTY:
            have = "yes" if inv > 0 else "no"
            close = "no" if have == "yes" else "yes"
            ask_have = side_book(s["bid"], s["ask"], have)[1]
            sell_at = max(sf[have] + MB_EXIT_EDGE, (ask_have - TICK) if ask_have is not None else 0)
            px = tick_down(1 - sell_at)
            if MB_MIN_PX <= px <= MB_MAX_PX:
                out.append(want(close, "buy", px, abs(inv), px - 0.01, sf[close] - TICK / 2, 2 * QTY_TOL, "exit", qmax=abs(inv)))
        # adds
        if s["add_ok"]:
            for side in ("yes", "no"):
                if (inv > 0 and side == "no") or (inv < 0 and side == "yes") or s["cooldown"].get(side):
                    continue
                bid, ask = side_book(s["bid"], s["ask"], side)
                mid = None if bid is None or ask is None else (bid + ask) / 2
                levels = []
                p = tick_down(sf[side] - MB_TAKE_EDGE)
                if ask is not None and ask <= p + EPS:
                    levels.append(("take", p, MB_TAKE_NOTIONAL, p - 0.01, p))
                if s["focus"]:
                    if bid is not None and mid is not None:
                        p = tick_down(min(bid + TICK, mid - TICK, sf[side] - MB_TOUCH_PM_EDGE))
                        if p >= bid - EPS:
                            levels.append(("touch", p, MB_TOUCH_NOTIONAL, p - TICK, min(p + TICK, sf[side] - 0.01)))
                    for d, n in MB_LADDER:
                        p = tick_down(sf[side] - d)
                        levels.append(("ladder", p, n, p - 0.01, min(p + 0.01, sf[side] - d + 0.01)))
                room, seen = MARKET_MAX_POS - held[side], set()
                for layer, p, n, lo, hi in levels:
                    if p in seen or not MB_MIN_PX <= p <= MB_MAX_PX or room < MIN_QTY * p:
                        continue
                    seen.add(p)
                    q = int(min(n, room) / p)
                    if q >= MIN_QTY:
                        room -= q * p
                        out.append(want(side, "buy", p, q, lo, hi, 2 * QTY_TOL, layer))
        self.orders = out
        layers = sorted({w["layer"] for w in out})
        self.status = ((f"holding {'YES' if inv > 0 else 'NO'} {abs(inv):,.0f}" if abs(inv) >= MIN_QTY else "flat")
                       + (f" · {', '.join(layers)}" if layers else " · nothing to do")
                       + ("" if s["add_ok"] else " · no new risk (account limit)"))
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
