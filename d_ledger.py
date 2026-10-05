"""D. LEDGER: every order the engine placed and what became of it, every fill on the account, and every arb basket.
Saved to data/ledger.json so history and arb baskets survive restarts."""
import json
import math
import os
import threading
import time
import time
from datetime import datetime

from a_config import LEDGER_FILE


def stamp():
    return time.strftime("%Y-%m-%d %H:%M:%S")


def write_atomic(path, text):
    with open(path + ".tmp", "w") as f:
        f.write(text)
    os.replace(path + ".tmp", path)


class Ledger:
    def __init__(self, persist):
        d = {}
        if persist and os.path.exists(LEDGER_FILE):
            with open(LEDGER_FILE) as fh:
                d = json.load(fh)
        self.persist = persist
        self.orders = d.get("orders", {})       # order id -> {bot, layer, ex, side, action, price, qty, filled, cost, status, ...}
        self.fills = d.get("fills", [])         # oldest first
        self.baskets = d.get("baskets", [])     # pair-bot sets: {race, side, legs, orders[], margin, repaired}
        self.subbooks = d.get("subbooks", {})   # I3: ex -> {"manual" | "inventory" | "pairset:<id>" | "tranche:<id>": signed YES shares}
        self.tranches = d.get("tranches", {})   # A1/A5: tranche sub-book id -> {ex, side, level, ref_side, off, fair_side, t, state}
        self.moves = d.get("moves", [])         # day 6 §9 rule 0: sub-book moves (ex, src, dst, qty, t) so lots keep their entry across a move
        self.ladder_seq = d.get("ladder_seq", {})  # (ex|side|level) -> current tranche number
        for i, b in enumerate(self.baskets):    # sets recorded before ids existed
            b.setdefault("id", f"{b.get('race', 'set')}:{int(b.get('ts', 0)) or i}")
        self.lock = threading.RLock()           # the executor's threads record placements while the engine syncs fills
        for f in self.fills:                    # records written by older versions
            for k, v in dict(fair=None, edge=None, sig_mid=None, sig_edge=None, layer="", action=None, before_engine=True,
                             mo5=None, mo30=None, realized=0.0).items():
                f.setdefault(k, v)
        for o in self.orders.values():
            for k, v in dict(cost=0.0, layer="", filled=0.0, left=0, closed=None, note="", sub="").items():
                o.setdefault(k, v)
        self.seen, self.n, self.synced = {f["key"] for f in self.fills}, 0, bool(d)

    # ---- orders
    def placed(self, bot, o, ok, data, layer="", note="", sub="", ttl=None):
        """Ownership record: every order the engine places, by id -> bot, sub-book, layer (and its TTL: a vanished
        order younger than its TTL that we didn't cancel was filled, evening §1.4)."""
        ex, side, action, price, qty = o
        oid = data.get("orderId") if ok else None
        with self.lock:
            self.n += 1
            key = str(oid) if oid is not None else f"{'dry' if ok else 'rejected'}-{int(time.time())}-{self.n}"
            self.orders[key] = dict(id=key, bot=bot, layer=layer, sub=sub, ex=ex, side=side, action=action, price=price,
                                    qty=int(qty), filled=0.0, cost=0.0, left=int(qty), placed=stamp(), ts=time.time(), closed=None,
                                    note=note if ok else str(data)[:160], ttl=ttl,
                                    status="open" if oid is not None else ("dry run" if ok else "rejected"))
            return key

    def adopt(self, oid, bot, sub, layer, o):
        """An order the REST audit found for an UNKNOWN write: record it as ours."""
        with self.lock:
            if str(oid) not in self.orders:
                self.placed(bot, o, True, {"orderId": oid}, layer, note="adopted by audit", sub=sub)

    def cancelled(self, oid):
        with self.lock:
            o = self.orders.get(str(oid))
            if o and o["status"] == "open":
                o.update(status="cancelled", closed=stamp())

    # ---- I3 sub-books: net position per exchange = sum of its sub-books
    def sub(self, ex, name):
        with self.lock:
            return self.subbooks.get(ex, {}).get(name, 0)

    def init_subbooks(self, positions, manual_exs):
        """First run only: manual positions -> 'manual', complete arb sets -> 'pairset:<n>', the rest -> 'inventory'."""
        with self.lock:
            if self.subbooks:
                return False
            hold = {}
            for i, b in enumerate(self.baskets):
                m = self.matched(b)
                for _, ex, side, _, _ in self.basket_fills(b):
                    if m and ex:
                        hold.setdefault(ex, {})[f"pairset:{b['id']}"] = m if side == "yes" else -m
            for ex, q in positions.items():
                if ex in manual_exs:
                    self.subbooks[ex] = {"manual": q}
                else:
                    books = hold.get(ex, {})
                    self.subbooks[ex] = dict(books, inventory=q - sum(books.values()))
            return True

    def move_sub(self, ex, src, dst, log=print):
        """Hand a sub-book's shares to another (news guard: tranche -> inventory; D1: manual -> recycle:ak-d1)."""
        with self.lock:
            books = self.subbooks.setdefault(ex, {})
            q = books.pop(src, 0)
            if q:
                books[dst] = books.get(dst, 0) + q
                self.moves.append((ex, src, dst, q, time.time()))
            return q

    def split_sub(self, ex, src, dst, qty):
        """Move qty shares (same sign as src) from src to dst: D1 releases part of a manual position to the recycler."""
        with self.lock:
            books = self.subbooks.setdefault(ex, {})
            have = books.get(src, 0)
            q = math.copysign(min(abs(qty), abs(have)), have) if have else 0
            books[src] = have - q
            books[dst] = books.get(dst, 0) + q
            return q

    def ladder_id(self, ex, side, level):
        """The tranche sub-book a ladder level's orders feed: a new id once the current one has shares (a fill)."""
        with self.lock:
            key = f"{ex}|{side}|{level}"
            n = self.ladder_seq.get(key, 0)
            sub = f"tranche:{ex}{side[0]}{level}-{n}"
            if abs(self.subbooks.get(ex, {}).get(sub, 0)) >= 1 or sub in self.tranches:
                n += 1
                self.ladder_seq[key] = n
                sub = f"tranche:{ex}{side[0]}{level}-{n}"
            return sub

    def tranche_avg(self, sub, layer="ladder", ex=None, side=None, since=None):
        """Average fill price of the entry fills (ladder, or dump) that built a tranche (orders placed into that sub-book).
        Without those order records (pruned), the layer's buy fills on that market and side since the tranche started."""
        ids = {i for i, o in self.orders.items() if o.get("sub") == sub and o.get("layer") == layer}
        fs = [f for f in self.fills if f["order"] in ids]
        if not fs and ex is not None and since is not None:
            fs = [f for f in self.fills if f["ex"] == ex and f.get("layer") == layer and f["side"] == side and f.get("action") == "buy"
                  and f["t"] >= since]
        q = sum(f["qty"] for f in fs)
        return sum(f["qty"] * f["price"] for f in fs) / q if q else None

    def audit_subbooks(self, positions, log=print, skip=()):
        """I3: the REST position is the truth. Any difference from the sum of sub-books goes to 'inventory' (logged).
        skip: markets whose difference is carried a little longer (night §1.2: one of our orders just left the book and its
        late fill may still arrive)."""
        with self.lock:
            fixed = []
            for ex in set(positions) | set(self.subbooks):
                if ex in skip:
                    continue
                q = positions.get(ex, 0)
                books = self.subbooks.setdefault(ex, {})
                diff = q - sum(books.values())
                if abs(diff) > 1e-6:
                    books["inventory"] = books.get("inventory", 0) + diff
                    fixed.append((ex, diff))
                for k in [k for k, v in books.items() if abs(v) < 1e-6 and k != "inventory"]:
                    del books[k]
            for ex, diff in fixed:
                log(f"  AUDIT  ex {ex}: sub-books were off by {diff:+,.0f} shares -> inventory")
            return fixed

    # ---- fills
    def sync(self, *a, **k):
        with self.lock:
            return self._sync(*a, **k)

    @staticmethod
    def realized(held, side, action, price, qty):
        """Cash P&L a fill realizes against the holding it reduces: held = (net YES position, position row with lots)
        before the fill. Buying the complement at p closes at 1 - p; selling the held side closes at p."""
        if not held or not held[0]:
            return 0.0
        pos, row = held
        have = "yes" if pos > 0 else "no"
        lots = [l for l in (row or {}).get("lots", []) if l.get("side", "").lower() == have]
        n = sum(float(l["quantity"]) for l in lots)
        cost = (sum(float(l["quantity"]) * float(l.get("entryPrice") or 0) for l in lots) / n) if n else float((row or {}).get("avgCost") or 0)
        r = min(qty, abs(pos))
        if action == "buy" and side != have:
            return r * ((1 - price) - cost)
        if action == "sell" and side == have:
            return r * (price - cost)
        return 0.0

    def _sync(self, open_orders, fills, bot_of, side_fair, side_mid, as_of=None, held=None):
        """Close orders that left the book; add fills new since last loop (API gives newest first) with their edge vs
        the Polymarket fair and vs the SIG book mid of their side at the time the engine sees them.
        as_of: when the open-orders read started. The executor places orders while the engine reads, so an order acked
        after that may be missing from the snapshot: it is only closed once a snapshot taken well after it misses it."""
        live = {o["id"]: o for o in open_orders}
        for o in self.orders.values():
            if o["id"] in live:
                if o["status"] == "done":                           # an older snapshot missed it: it is resting
                    o.update(status="open", closed=None)
                if o["status"] == "open":
                    o["left"] = live[o["id"]]["qty"]
            elif o["status"] == "open" and (as_of is None or o.get("ts", 0) < as_of - 60):
                o.update(status="done", left=0, closed=stamp())
        history, new, self.synced = not self.synced, [], True
        for f in reversed(fills):
            key = f"{f['id']}:{f.get('orderId')}"
            if key in self.seen:
                continue
            self.seen.add(key)
            oid, ex, side = str(f.get("orderId")), str(f["exchangeId"]), f["side"].lower()
            order = self.orders.get(oid)
            action, price, qty = (order or {}).get("action"), float(f["price"] or 0), abs(float(f["quantity"]))
            if order and order.get("truth_q", 0) > 1e-9:          # evening §1.2: already booked from order truth
                take = min(qty, order["truth_q"])
                order["truth_q"] -= take
                qty -= take
                if qty < 1e-9:
                    continue
            if order and order.get("qty"):                         # Oct 5 03:20: an order never fills more than its quantity, whatever
                over = max(0.0, (order.get("filled") or 0) + qty - order["qty"])   # the page replays (synthetic booked, truth_q lost)
                if over > 1e-9:
                    qty -= over
                    if qty < 1e-9:
                        continue
            sgn = 1 if action == "buy" else -1 if action == "sell" else None
            fair, mid = side_fair(ex, side), side_mid(ex, side)
            rec = dict(key=key, order=oid, ex=ex, side=side, qty=qty, price=price, action=action,
                       realized=round(self.realized(held(ex) if held else None, side, action or "buy", price, qty), 2),
                       t=datetime.fromisoformat(f["filledAt"].replace("Z", "+00:00")).astimezone().strftime("%Y-%m-%d %H:%M:%S"),
                       bot=order["bot"] if order else bot_of(ex), layer=(order or {}).get("layer", ""), before_engine=history,
                       fair=None if fair is None else round(fair, 4), sig_mid=None if mid is None else round(mid, 4),
                       edge=None if fair is None or sgn is None else round(sgn * (fair - price), 4),
                       sig_edge=None if mid is None or sgn is None else round(sgn * (mid - price), 4))
            if order:
                order["filled"] += qty
                order["cost"] += qty * price
                if sgn is not None and order.get("sub"):          # I3: an owned fill moves its own sub-book
                    self.apply_sub(ex, order, sgn * (qty if side == "yes" else -qty))
            self.fills.append(rec)
            if not history:
                new.append(rec)
        self.fills = self.fills[-4000:]
        closed = sorted((o for o in self.orders.values() if o["status"] != "open"), key=lambda o: o["placed"])
        keep = {i for b in self.baskets[-500:] for i in b["orders"]}
        keep |= {i for i, o in self.orders.items() if o.get("sub") and abs(self.subbooks.get(o["ex"], {}).get(o["sub"], 0)) >= 1}
        # Oct 3: an order whose sub-book still holds shares is kept (a pruned dump buy made the RI Senate D tranche lose
        # its entry price and its rebound from 12:46)
        for o in closed[:-4000]:
            if o["id"] not in keep:
                del self.orders[o["id"]]
        return new

    EXIT_LAYERS = {"dumpexit", "longexit", "scalpexit", "touchexit", "rebound", "recycle", "release", "exit", "unwind", "repair"}

    def apply_sub(self, ex, order, delta):
        """Night §1.1 (I2/I3): an owned fill moves its own sub-book, but an exit never takes it past zero: a fill that would
        sell more than the sub-book holds (e.g. an exit still resting when its tranche completed) is clipped at zero and the
        excess comes out of the sub-books that did hold those shares (inventory, then mm). -> the clipped excess."""
        books = self.subbooks.setdefault(ex, {})
        sub, cur = order["sub"], books.get(order["sub"], 0)
        reducing = (cur > 1e-9 and delta < 0) or (cur < -1e-9 and delta > 0) or (abs(cur) <= 1e-9 and order.get("layer") in self.EXIT_LAYERS)
        if not reducing:
            books[sub] = cur + delta
            return 0.0
        applied = delta if abs(delta) <= abs(cur) else -cur
        books[sub] = cur + applied
        excess = delta - applied
        for other in ("inventory", "mm"):
            if abs(excess) < 1e-9 or other == sub:
                continue
            v = books.get(other, 0)
            if v * excess < 0:                                # holds the side the excess sold
                take = excess if abs(excess) <= abs(v) else -v
                books[other] = v + take
                excess -= take
        if abs(excess) > 1e-9:                                # nobody held them: the audit reconciles with the position
            books["inventory"] = books.get("inventory", 0) + excess
        return delta - applied

    def book_fill(self, oid, qty, price, source, side_fair, side_mid, held=None, t=None):
        """Evening §1.2-1.4: book a fill of one of our orders from order truth (source 'order': exact totals) or the
        order-shrink fallback (source 'synthetic': price = the order's limit) while SIG's fills projection is stalled. Same
        sub-book (I3), realized and edge rules as a SIG fill; when the projection's own fills for that order arrive later
        they are consumed (truth_q), not booked twice."""
        with self.lock:
            order = self.orders.get(str(oid))
            if not order or qty < 1e-9:
                return None
            ex, side, action = order["ex"], order["side"], order["action"]
            sgn = 1 if action == "buy" else -1
            fair, mid = side_fair(ex, side), side_mid(ex, side)
            self.n += 1
            rec = dict(key=f"{source}:{oid}:{self.n}", order=str(oid), ex=ex, side=side, qty=float(qty), price=float(price),
                       action=action, realized=round(self.realized(held, side, action, price, qty), 2),
                       t=t or stamp(), bot=order["bot"], layer=order.get("layer", ""), before_engine=False, source=source,
                       fair=None if fair is None else round(fair, 4), sig_mid=None if mid is None else round(mid, 4),
                       edge=None if fair is None else round(sgn * (fair - price), 4),
                       sig_edge=None if mid is None else round(sgn * (mid - price), 4))
            order["filled"] += qty
            order["cost"] += qty * price
            order["truth_q"] = order.get("truth_q", 0) + qty
            if order.get("sub"):
                self.apply_sub(ex, order, sgn * (qty if side == "yes" else -qty))
            self.seen.add(rec["key"])
            self.fills.append(rec)
            return rec

    PHANTOM_SUBS = ("dump:", "touch:", "scalp:", "sniper", "mm")

    def dedup_synthetic(self, dry_run=False, window_s=180):
        """Oct 5 02:30: a synthetic (order_shrink) fill that duplicates one of SIG's own fills for the same order (same
        quantity, within 3 min) is removed from the fill history (stats, lots and the order's filled count follow). The
        sub-books are repaired conservatively: a market cannot hold a long tranche and a short inventory at once (SIG
        nets positions), so an engine sub-book (dump / touch / scalp tranche, sniper, mm) with the sign opposite to the
        market's inventory is the audit's compensation for a double booking: the overlap is netted out of both (the sum
        of the sub-books, which the audit keeps equal to the position, does not change). Pair-set legs are left alone.
        -> dict(removed=[fill keys], netted=[(ex, sub, from, to)])."""
        with self.lock:
            proj = {}
            for f in self.fills:
                if f.get("source") != "order_shrink" and f.get("order"):
                    proj.setdefault(str(f["order"]), []).append(f)
            def ts(x):
                return time.mktime(time.strptime(x, "%Y-%m-%d %H:%M:%S"))
            drop, used = [], set()
            for f in self.fills:
                if f.get("source") != "order_shrink":
                    continue
                for p in proj.get(str(f.get("order")), []):
                    if id(p) in used or abs(p["qty"] - f["qty"]) >= 1 or abs(ts(p["t"]) - ts(f["t"])) > window_s:
                        continue
                    used.add(id(p))
                    drop.append(f)
                    break
            netted = []
            for ex, books in self.subbooks.items():
                inv = books.get("inventory", 0)
                if abs(inv) < 1:
                    continue
                for sub in sorted(books):
                    q = books.get(sub, 0)
                    if sub == "inventory" or not sub.startswith(self.PHANTOM_SUBS) or q * inv >= 0 or abs(q) < 1:
                        continue
                    take = min(abs(q), abs(inv))
                    netted.append((ex, sub, q, q - take * (1 if q > 0 else -1)))
                    if not dry_run:
                        books[sub] = q - take * (1 if q > 0 else -1)
                        inv = inv - take * (1 if inv > 0 else -1)
                        books["inventory"] = inv
                        if abs(books[sub]) < 1:
                            books.pop(sub, None)
                            t = self.tranches.get(sub)
                            if t and t.get("state") == "open":
                                t["state"] = "voided"      # a phantom tranche: no round trip, no sell-back
                    if abs(inv) < 1:
                        break
                if not dry_run and abs(books.get("inventory", 0)) < 1:
                    books.pop("inventory", None)
            keys = {f["key"] for f in drop}
            if not dry_run:
                for f in drop:
                    order = self.orders.get(str(f.get("order")))
                    if order:
                        order["filled"] = max(0.0, (order.get("filled") or 0) - f["qty"])
                        order["cost"] = max(0.0, (order.get("cost") or 0) - f["qty"] * f["price"])
                        order["truth_q"] = max(0.0, (order.get("truth_q") or 0) - f["qty"])
                self.fills = [f for f in self.fills if f.get("key") not in keys]
            # same-sign overstatement: a touch / scalp sub-book (both younger than every order record they rest on) can never
            # hold more than the net of its own fills plus recorded moves; the excess is a duplicate's leftover -> back to
            # inventory. Dump levels are left alone: their entry orders can predate the records we still hold.
            net = {}
            for f in (f for f in self.fills if f.get("key") not in keys):
                sub = (self.orders.get(str(f.get("order"))) or {}).get("sub")
                if sub and sub.startswith(("touch:", "scalp:")) and f.get("action") in ("buy", "sell"):   # dump levels: fills older than their order records
                    net[(f["ex"], sub)] = net.get((f["ex"], sub), 0.0) + (1 if f["action"] == "buy" else -1) * (f["qty"] if f["side"] == "yes" else -f["qty"])
            for ex, src, dst, q, _t in getattr(self, "moves", []):
                if (ex, src) in net:
                    net[(ex, src)] -= q
                if (ex, dst) in net:
                    net[(ex, dst)] += q
            trimmed = []
            for (ex, sub), q in net.items():
                books = self.subbooks.get(ex, {})
                cur = books.get(sub, 0)
                if abs(cur) > abs(q) + 1 and cur * q >= 0:
                    trimmed.append((ex, sub, cur, round(q, 2)))
                    if not dry_run:
                        books["inventory"] = books.get("inventory", 0) + (cur - q)
                        if abs(q) < 1:
                            books.pop(sub, None)
                            t = self.tranches.get(sub)
                            if t and t.get("state") == "open":
                                t["state"] = "voided"
                        else:
                            books[sub] = round(q, 2)
                        if abs(books.get("inventory", 0)) < 1e-9:
                            books.pop("inventory", None)
            return dict(removed=[f["key"] for f in drop], netted=netted + trimmed)

    def markouts(self, side_mid, horizons=((5, "mo5"), (30, "mo30"))):
        """Mark-out of each engine fill 5 and 30 minutes later: (SIG mid of its side then - fill price), + for buys that
        went our way. Negative across many fills = we're being picked off (adverse selection)."""
        now = time.time()
        for f in self.fills[-1500:]:
            if f["before_engine"] or f["action"] not in ("buy", "sell"):
                continue
            age = (now - time.mktime(time.strptime(f["t"], "%Y-%m-%d %H:%M:%S"))) / 60
            for mins, k in horizons:
                if f.get(k) is None and age >= mins:
                    m = side_mid(f["ex"], f["side"])
                    if m is not None and age < mins + 15:            # too late to measure this horizon fairly: leave it empty
                        f[k] = round((m - f["price"]) * (1 if f["action"] == "buy" else -1), 4)

    # ---- arb baskets
    def basket_fills(self, b):
        """[(order id, ex, side, held, avg price)] for each leg of a basket: the leg's fills plus its repair (completing)
        orders' fills, minus what unwinds sold back (complement buys) at the leg's average."""
        out = []
        for oid in b["orders"]:
            o = self.orders.get(oid, {})
            parts = [o] + [self.orders.get(r, {}) for r in b.get("repairs", {}).get(oid, [])]   # repair orders join their leg
            f, c = sum(p.get("filled", 0.0) for p in parts), sum(p.get("cost", 0.0) for p in parts)
            avg = c / f if f else None
            sold = sum(self.orders.get(r, {}).get("filled", 0.0) for r in b.get("unwinds", {}).get(oid, []))
            out.append((oid, o.get("ex"), o.get("side"), max(0.0, f - sold), avg))
        return out

    def matched(self, b):
        legs = self.basket_fills(b)
        return min(l[3] for l in legs) if legs and len(legs) == b["legs"] else 0.0

    def arb_hold(self):
        """{ex: signed YES shares held by complete arb sets}; market bots leave these alone."""
        out = {}
        for b in self.baskets:
            m = self.matched(b)
            for _, ex, side, _, _ in self.basket_fills(b):
                if m and ex:
                    out[ex] = out.get(ex, 0) + (m if side == "yes" else -m)
        return out

    def save(self):
        if self.persist:
            with self.lock:
                text = json.dumps(dict(orders=self.orders, fills=self.fills, baskets=self.baskets[-1000:], subbooks=self.subbooks,
                                       tranches=self.tranches, ladder_seq=self.ladder_seq, moves=self.moves[-2000:]))
            write_atomic(LEDGER_FILE, text)
