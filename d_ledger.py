"""D. LEDGER: every order the engine placed and what became of it, every fill on the account, and every arb basket.
Saved to data/ledger.json so history and arb baskets survive restarts."""
import json
import math
import os
import threading
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

    def tranche_avg(self, sub, layer="ladder"):
        """Average fill price of the entry fills (ladder, or dump) that built a tranche (orders placed into that sub-book)."""
        ids = {i for i, o in self.orders.items() if o.get("sub") == sub and o.get("layer") == layer}
        q = sum(f["qty"] for f in self.fills if f["order"] in ids)
        return sum(f["qty"] * f["price"] for f in self.fills if f["order"] in ids) / q if q else None

    def audit_subbooks(self, positions, log=print):
        """I3: the REST position is the truth. Any difference from the sum of sub-books goes to 'inventory' (logged)."""
        with self.lock:
            fixed = []
            for ex in set(positions) | set(self.subbooks):
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
                    books = self.subbooks.setdefault(ex, {})
                    books[order["sub"]] = books.get(order["sub"], 0) + sgn * (qty if side == "yes" else -qty)
            self.fills.append(rec)
            if not history:
                new.append(rec)
        self.fills = self.fills[-4000:]
        closed = sorted((o for o in self.orders.values() if o["status"] != "open"), key=lambda o: o["placed"])
        keep = {i for b in self.baskets[-500:] for i in b["orders"]}
        for o in closed[:-4000]:
            if o["id"] not in keep:
                del self.orders[o["id"]]
        return new

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
                books = self.subbooks.setdefault(ex, {})
                books[order["sub"]] = books.get(order["sub"], 0) + sgn * (qty if side == "yes" else -qty)
            self.seen.add(rec["key"])
            self.fills.append(rec)
            return rec

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
                                       tranches=self.tranches, ladder_seq=self.ladder_seq))
            write_atomic(LEDGER_FILE, text)
