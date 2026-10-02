"""J. EXECUTOR: every SIG write goes through here (plan_review A7/A8, invariants I2, I4, I6-I8).

  priority     0 cancels · 1 position exits & trims · 2 takes · 3 rebounds/recycler · 4 pair sets · 5 MM quotes · 6 ladders
  two lanes    a cancel lane (only cancels, so a cancel never waits behind a placement) and a placement lane
  budget       one token bucket (b_sig.writes_left); placements leave CANCEL_RESERVE writes for the cancel lane;
               a 429 pauses both lanes for Retry-After
  capacity     EWMA of orders SIG actually processes per minute; admit() caps new placements to ~1 minute of it, and
               a write carries ~36 s of it (small jobs of one priority/TTL are merged; SIG stops a batch at ~60 s)
  staleness    a queued placement older than STALE_S is dropped, not sent: the next plan re-creates it if still wanted
  journal      every write is journaled IN_FLIGHT before it's sent and closed with its outcome. After a restart nothing
               is ever re-sent: IN_FLIGHT entries become UNKNOWN, their markets freeze, and the REST audit resolves them;
               ACKED entries newer than the ledger's last save are re-adopted as ours (recover_acked)
  ownership    every order the engine places is recorded order id -> bot, sub-book, layer (in the ledger). Anything not
               in that record is foreign: never cancelled. cancel-all on an exchange only when every order on it is ours.
  I2           on each exchange, reducing orders (resting + in flight + new, from every strategy and from foreign orders)
               never exceed the position they reduce: excess is trimmed here, never sent
"""
import heapq
import itertools
import json
import os
import threading
import time
import uuid
from datetime import datetime, timedelta

PRIORITY = {"cancel": 0, "position": 1, "trim": 1, "take": 2, "rebound": 3, "recycle": 3, "repair": 3, "unwind": 3,
            "set": 4, "mm": 5, "exit": 3, "touch": 5, "ladder": 6, "add": 1}


def reduces(side, action, pos):
    """Does this order reduce the net YES position pos? (sells only when backed by the holding)"""
    if action == "sell":
        return (side == "yes" and pos > 0) or (side == "no" and pos < 0)
    return (side == "yes" and pos < 0) or (side == "no" and pos > 0)


# ======================================================================================================= journal

class Journal:
    """Append-only JSON lines, one file per day, 7 days kept. A key's last line is its status."""

    def __init__(self, folder, clock=time.time, keep_days=7):
        self.folder, self.clock, self.lock = folder, clock, threading.Lock()
        os.makedirs(folder, exist_ok=True)
        self.entries = {}
        cutoff = (datetime.fromtimestamp(clock()) - timedelta(days=keep_days)).strftime("%Y%m%d")
        for name in sorted(os.listdir(folder)):
            if not name.startswith("journal_"):
                continue
            if name[8:16] < cutoff:
                os.remove(os.path.join(folder, name))
                continue
            with open(os.path.join(folder, name)) as f:
                for line in f:
                    try:
                        d = json.loads(line)
                    except ValueError:
                        continue
                    self.entries.setdefault(d["key"], {}).update(d)
        for e in self.entries.values():                    # a previous process died mid-write: never re-send it
            if e.get("status") == "IN_FLIGHT":
                e["status"] = "UNKNOWN"
                self._append(dict(key=e["key"], status="UNKNOWN", note="process restarted while in flight"))

    def _append(self, d):
        path = os.path.join(self.folder, f"journal_{datetime.fromtimestamp(self.clock()).strftime('%Y%m%d')}.jsonl")
        with open(path, "a") as f:
            f.write(json.dumps(dict(d, at=self.clock())) + "\n")

    def open(self, key, kind, items, ttl_s=None):
        e = dict(key=key, kind=kind, ts=self.clock(), status="IN_FLIGHT", ttl_s=ttl_s,
                 items=[dict(bot=b, sub=s, layer=l, order=list(o)) for b, s, l, o in items])
        with self.lock:
            self.entries[key] = e
            self._append(e)

    def close(self, key, status, **info):
        with self.lock:
            self.entries.setdefault(key, {}).update(status=status, **info)
            self._append(dict(key=key, status=status, **info))

    def unknown(self):
        with self.lock:
            return [dict(e) for e in self.entries.values() if e.get("status") == "UNKNOWN"]


# ======================================================================================================= executor

class Executor:
    def __init__(self, api, ledger, journal, batch_size=20, stale_s=30, cancel_reserve=3, min_qty=10,
                 clock=time.time, sleep=time.sleep, log=print):
        self.api, self.ledger, self.journal = api, ledger, journal
        self.batch_size, self.stale_s, self.cancel_reserve, self.min_qty = batch_size, stale_s, cancel_reserve, min_qty
        self.clock, self.sleep, self.log = clock, sleep, log
        self.lock, self.wake = threading.RLock(), threading.Condition(threading.RLock())
        self.queue, self.cancels, self.seq = [], [], itertools.count()
        self.in_flight = {}                                 # key -> job being sent
        self.recent = {}                                    # order id -> (order dict, acked at): acked, not yet in a snapshot
        self.pos, self.book, self.book_t = {}, [], 0.0      # latest REST snapshot from the engine
        self.cancelling = set()                             # ids with a DELETE sent or queued
        self.gone = set()                                   # ids SIG confirmed cancelled since the last snapshot
        self.frozen = {ex: e["key"] for e in journal.unknown() for it in e.get("items", []) for ex in [it["order"][0]]}
        self.recovered = self.recover_acked()               # acked after the last ledger save (process was killed)
        self.capacity = 60.0                                # orders SIG processes per minute (EWMA)
        self.paused_until, self.dropped_stale, self.sent_orders, self.halted = 0.0, 0, 0, False
        self.trims = {}                                     # layer -> I2 trims (an order cut or dropped to fit the position)
        self.cancels_on_wire = 0                            # DELETEs sent, not answered yet: placements wait for them (I6)
        self.trim_log = []                                  # (time, layer, ex, wanted, sent), last 200
        self.threads, self.running = [], False

    # ---------------------------------------------------------------------------------------- state from the engine
    def update(self, positions, open_orders, as_of=None):
        """Latest REST snapshot (positions net YES per exchange, every open order). Called by the engine each cycle.
        as_of: when the read started. An order acked after that may be missing from it, so it stays in `recent` (counted
        as resting) until a snapshot that should show it doesn't."""
        with self.wake, self.lock:
            self.pos, self.book, self.book_t = dict(positions), list(open_orders), self.clock()
            as_of = self.book_t - 120 if as_of is None else as_of
            seen = {str(o["id"]) for o in open_orders}
            self.recent = {i: v for i, v in self.recent.items() if i not in seen and v[1] > as_of - 60}
            self.cancelling &= seen
            self.gone &= seen

    def owned(self, oid):
        return str(oid) in self.ledger.orders

    def is_frozen(self, ex):
        return ex in self.frozen

    # ---------------------------------------------------------------------------------------- submission
    def place(self, items, layer_prio=None, ttl_s=None, atomic=False, on_done=None):
        """items: [(bot, sub, layer, (ex, side, action, price, qty))]. Queued, sent by the placement lane.
        Returns the job key (it becomes the idempotency key of the write)."""
        items = [it for it in items if not self.is_frozen(it[3][0])]
        if not items or self.halted:
            return None
        prio = layer_prio if layer_prio is not None else min(PRIORITY.get(it[2], 5) for it in items)
        job = dict(key=str(uuid.uuid4()), prio=prio, items=items, ttl_s=ttl_s, atomic=atomic, created=self.clock(),
                   on_done=on_done)
        with self.wake:
            heapq.heappush(self.queue, (prio, next(self.seq), job))
            self.wake.notify_all()
        return job["key"]

    def cancel(self, order, reason="", front=False):
        """DELETE one order (front: ahead of every queued cancel). Refused (False) for foreign orders and for orders
        already being cancelled."""
        oid = str(order["id"])
        if not self.owned(oid):
            self.log(f"  REFUSED cancel {oid}: not placed by the engine (foreign orders are never cancelled)")
            return False
        with self.wake:
            if oid in self.cancelling:
                return False
            self.cancelling.add(oid)
            self.cancels.insert(0 if front else len(self.cancels), dict(oid=oid, ex=order.get("ex"), reason=reason,
                                                                        created=self.clock()))
            self.wake.notify_all()
        return True

    def can_cancel_exchange(self, ex):
        """cancel-all on one exchange is allowed only when every resting order on it is engine-owned."""
        with self.lock:
            on = [o for o in self.book if o["ex"] == ex]
            return bool(on) and all(self.owned(o["id"]) for o in on)

    def halt(self):
        """Stop / outage: drop every queued placement and send no more until resume(). Queued cancels are dropped too
        (the plan's re-quotes, which would spend the write budget before the stop's own at-risk cancels); the caller
        queues the cancels it wants. -> placements dropped"""
        with self.wake:
            self.halted = True
            for c in self.cancels:
                self.cancelling.discard(c["oid"])
            self.cancels = []
        return self.drop_queued(lambda *a: True)

    def resume(self):
        with self.wake:
            self.halted = False

    def drop_queued(self, pred):
        """Remove queued (not yet sent) placement items matching pred(bot, sub, layer, order). Returns how many."""
        n = 0
        with self.wake:
            keep = []
            for prio, s, job in self.queue:
                left = [it for it in job["items"] if not pred(*it)]
                n += len(job["items"]) - len(left)
                if left:
                    job["items"] = left
                    keep.append((prio, s, job))
            heapq.heapify(keep)
            self.queue = keep
        return n

    # ---------------------------------------------------------------------------------------- views
    def pending(self):
        """Placements queued, in flight, or acked but not yet in a snapshot: the engine treats them as resting."""
        out = []
        with self.wake:
            for _, _, job in self.queue:
                out += [dict(bot=b, sub=s, layer=l, ex=o[0], side=o[1], action=o[2], price=o[3], qty=o[4], state="queued")
                        for b, s, l, o in job["items"]]
            for job in self.in_flight.values():
                out += [dict(bot=b, sub=s, layer=l, ex=o[0], side=o[1], action=o[2], price=o[3], qty=o[4], state="in_flight")
                        for b, s, l, o in job["items"]]
            out += [dict(v[0], state="acked") for v in self.recent.values()]
        return out

    def admit(self):
        """How many new orders to accept now: about one minute of SIG's measured capacity, minus what's queued."""
        with self.wake:
            queued = sum(len(j["items"]) for _, _, j in self.queue)
        return max(0, int(max(self.batch_size, self.capacity)) - queued)

    def stats(self):
        with self.wake:
            q = {}
            for prio, _, job in self.queue:
                q[str(prio)] = q.get(str(prio), 0) + len(job["items"])
            q["0"] = q.get("0", 0) + len(self.cancels)
            return dict(queue=q, in_flight=sum(len(j["items"]) for j in self.in_flight.values()), unknown=len(self.frozen),
                        frozen=sorted(self.frozen), capacity_ewma=round(self.capacity, 1), batch_cap=self.batch_cap(),
                        dropped_stale=self.dropped_stale, recovered=self.recovered, halted=self.halted, trims=dict(self.trims),
                        sent_orders=self.sent_orders, paused_s=max(0, round(self.paused_until - self.clock())))

    # ---------------------------------------------------------------------------------------- I2
    def _reducing_capacity(self, ex, exclude_key=None):
        """|position| minus every reducing order already resting (ours or foreign), in flight or just acked."""
        pos = self.pos.get(ex, 0)
        used = sum(o["qty"] for o in self.book if o["ex"] == ex and str(o["id"]) not in self.gone      # freed only once
                   and reduces(o["side"], o["action"], pos))                                            # SIG confirmed
        used += sum(v[0]["qty"] for v in self.recent.values() if v[0]["ex"] == ex and reduces(v[0]["side"], v[0]["action"], pos))
        for k, job in self.in_flight.items():
            if k != exclude_key:
                used += sum(o[4] for _, _, _, o in job["items"] if o[0] == ex and reduces(o[1], o[2], pos))
        return abs(pos) - used

    def trim(self, items, exclude_key=None, cap=None):
        """Enforce I2 on items about to be sent. Positions net, so ANY order on the reducing side (a sell of the held side,
        or a buy of the other side, whatever the strategy) reduces first: each is cut to the remaining reducing capacity."""
        cap, out = ({} if cap is None else cap), []
        for b, s, l, o in items:
            ex, side, action, price, qty = o
            pos = self.pos.get(ex, 0)
            if ex not in cap:
                cap[ex] = self._reducing_capacity(ex, exclude_key)
            want = qty
            if reduces(side, action, pos):
                qty = int(max(0, min(qty, cap[ex])))
                cap[ex] -= qty
            elif action == "sell":
                qty = 0                                     # an unbacked sell would become an opposite buy: never send it
            if qty < want:
                self.trims[l] = self.trims.get(l, 0) + 1
                self.trim_log = (self.trim_log + [(self.clock(), l, ex, want, qty)])[-200:]
            if qty >= self.min_qty:
                out.append((b, s, l, (ex, side, action, price, int(qty))))
        return out

    # ---------------------------------------------------------------------------------------- lanes
    def _tokens(self):
        return self.api.writes_left()

    def step_cancels(self, max_n=None):
        """Send queued cancels (all the budget allows, or at most max_n). Returns how many were sent."""
        n = 0
        while max_n is None or n < max_n:
            with self.wake:
                if not self.cancels or self.clock() < self.paused_until or self._tokens() < 1:
                    return n
                c = self.cancels.pop(0)
                self.cancels_on_wire += 1
                self.cancel_lat = (getattr(self, "cancel_lat", []) + [round(self.clock() - c["created"], 3)])[-500:]
            key = str(uuid.uuid4())
            self.journal.open(key, "cancel", [("executor", "", "cancel", (c["ex"], "", "cancel", 0, 0))])
            try:
                self.api.cancel(c["oid"])
                self.ledger.cancelled(c["oid"])
                with self.wake:
                    self.gone.add(c["oid"])
                self.journal.close(key, "ACKED", ids=[c["oid"]])
            except self.api.RateLimited as e:
                self.paused_until = self.clock() + e.retry_after
                with self.wake:
                    self.cancels.insert(0, c)
                self.journal.close(key, "NOT_SENT", note="429")
                return n
            except Exception as e:                          # a timed-out DELETE may or may not have happened: the next
                self.journal.close(key, "UNKNOWN", note=str(e)[:160])    # snapshot tells; the plan asks again if needed
                with self.wake:
                    self.cancelling.discard(c["oid"])
            finally:
                with self.wake:
                    self.cancels_on_wire -= 1
            n += 1
        return n

    def batch_cap(self):
        """Orders per write: what SIG processes in ~36 s at its measured rate (it cuts a batch off at ~60 s)."""
        return max(1, min(self.batch_size, int(self.capacity * 0.6)))

    def _next_write(self):
        """Pop the next write (caller holds wake): the top job, merged with the following non-atomic jobs of the same TTL,
        in priority order, up to batch_cap(). Stale jobs are dropped (the plan re-creates them if still wanted); frozen
        markets are filtered out; an atomic set with a frozen leg is dropped whole. -> [(job, items)]"""
        parts, n, cap = [], 0, self.batch_cap()
        while self.queue and n < cap:
            prio, s, job = self.queue[0]
            if self.clock() - job["created"] > self.stale_s:          # planned too long ago: re-plan, don't send
                heapq.heappop(self.queue)
                self.dropped_stale += len(job["items"])
                continue
            items = [it for it in job["items"] if not self.is_frozen(it[3][0])]
            if not items or (job["atomic"] and len(items) != len(job["items"])):
                heapq.heappop(self.queue)
                continue
            if job["atomic"]:
                if not parts:
                    heapq.heappop(self.queue)
                    parts = [(job, items)]
                break
            if parts and job["ttl_s"] != parts[0][0]["ttl_s"]:
                break
            heapq.heappop(self.queue)
            head, rest = items[:cap - n], items[cap - n:]
            if rest:
                heapq.heappush(self.queue, (prio, s, dict(job, items=rest)))
            parts.append((job, head))
            n += len(head)
        return parts

    def step_placements(self):
        """Send the next write if the budget allows (cancels go first). Returns how many orders were sent."""
        with self.wake:
            if (not self.queue or self.halted or self.clock() < self.paused_until or self._tokens() <= self.cancel_reserve
                    or self.cancels or self.cancels_on_wire):     # I6: a placement never goes while a cancel is pending
                return 0
            parts = self._next_write()
            key = str(uuid.uuid4())                         # the write's idempotency key
            atomic, ttl_s, sent = bool(parts) and parts[0][0]["atomic"], parts[0][0]["ttl_s"] if parts else None, []
            with self.lock:
                cap = {}
                for job, items in parts:
                    kept = self.trim(items, cap=cap)
                    if atomic and len(kept) != len(items):
                        self.log(f"  SKIP   a {len(items)}-leg set: a leg no longer fits its position (I2); not sent")
                        kept = []
                    if kept:
                        sent.append((job, kept))
            if not sent:
                return 0
            items = [it for _, kept in sent for it in kept]
            self.in_flight[key] = dict(key=key, items=items)
        self.journal.open(key, "multileg" if atomic else "batch", items, ttl_s)
        orders, t0 = [o for _, _, _, o in items], self.clock()
        try:
            res = (self.api.place_multileg if atomic else self.api.place_batch)(orders, ttl_s, key=key)
        except self.api.RateLimited as e:
            self.paused_until = self.clock() + e.retry_after
            self.journal.close(key, "NOT_SENT", note="429")
            with self.wake:
                del self.in_flight[key]
                for job, kept in sent if not self.halted else []:   # back in the queue (stale by then -> re-planned)
                    heapq.heappush(self.queue, (job["prio"], next(self.seq), dict(job, items=kept)))
            return 0
        except self.api.Unknown as e:                       # may or may not have landed: freeze those markets (I8)
            self.journal.close(key, "UNKNOWN", note=str(e)[:160])
            with self.wake:
                for o in orders:
                    self.frozen[o[0]] = key
                del self.in_flight[key]
                self.capacity = max(1.0, self.capacity * 0.5)          # SIG is struggling: smaller writes
            self.log(f"  UNKNOWN {len(items)} orders may or may not be placed; {len({o[0] for o in orders})} markets frozen until the audit")
            return 0
        except Exception as e:
            self.journal.close(key, "REJECTED", note=str(e)[:160])
            with self.wake:
                del self.in_flight[key]
            self.log(f"  ERR placing {len(items)} orders: {str(e)[:160]}")
            return 0
        dt = max(self.clock() - t0, 1e-3)
        placed, results = [], []
        with self.wake:
            self.capacity = 0.7 * self.capacity + 0.3 * min(600.0, len(orders) / dt * 60)
            for (b, sub, layer, o), (_, ok, data) in zip(items, res):
                self.ledger.placed(b, o, ok, data, layer, sub=sub)
                oid = str(data["orderId"]) if ok and data.get("orderId") is not None else None
                placed.append(oid)
                if oid:
                    self.recent[oid] = (dict(id=oid, ex=o[0], side=o[1], action=o[2], price=o[3], qty=o[4], bot=b, sub=sub,
                                             layer=layer), self.clock())
                elif not ok:
                    self.log(f"  REJECT {b}: {o[2]} {o[1]} {o[4]:,} @ {o[3]:.3f} -> {str(data)[:140]}")
                results.append((b, o, ok, data))
            del self.in_flight[key]
        self.journal.close(key, "ACKED", ids=[i for i in placed if i], placed=placed)
        self.sent_orders += len(orders)
        i = 0
        for job, kept in sent:                              # each job hears about its own orders
            part, i = results[i:i + len(kept)], i + len(kept)
            if job.get("on_done"):
                try:
                    job["on_done"](part)
                except Exception as e:
                    self.log(f"  ERR on_done: {e!r}")
        return len(orders)

    def recover_acked(self, within_s=3600):
        """After a restart: orders SIG acked after the ledger's last save are in the journal (ACKED, with their ids) but
        not in the ledger. Record them as ours again: otherwise they look foreign, are never cancelled, and use up I2."""
        n, cutoff = 0, self.clock() - within_s
        for e in list(self.journal.entries.values()):
            if e.get("status") != "ACKED" or e.get("kind") not in ("batch", "multileg") or e.get("ts", 0) < cutoff:
                continue
            for it, oid in zip(e.get("items", []), e.get("placed") or []):
                if oid and str(oid) not in self.ledger.orders:
                    self.ledger.adopt(oid, it["bot"], it["sub"], it["layer"], tuple(it["order"]))
                    n += 1
        return n

    # ---------------------------------------------------------------------------------------- audit (I8)
    def resolve_unknown(self, open_orders, fills, on_adopt=None):
        """Resolve UNKNOWN writes from a REST snapshot: an open order or fill matching an UNKNOWN item, placed after the
        write and not already ours, is adopted (ACKED, ownership recorded); otherwise NOT_PLACED. Markets unfreeze."""
        unknown = self.journal.unknown()
        if not unknown:
            self.frozen = {}
            return 0
        fill_by_order = {}
        for f in fills:
            fill_by_order.setdefault(str(f.get("orderId")), []).append(f)
        taken, resolved = set(self.ledger.orders), 0
        for e in unknown:
            if e.get("kind") == "cancel":
                self.journal.close(e["key"], "RESOLVED", note="cancel: the snapshot shows what is left")
                resolved += 1
                continue
            ids = []
            for it in e.get("items", []):
                ex, side, action, price, qty = it["order"]
                match = next((o for o in open_orders if o["id"] not in taken and o["ex"] == ex and o["side"] == side
                              and o["action"] == action and abs(o["price"] - price) < 1e-9 and o["qty"] <= qty), None)
                if match is None:                           # it may have filled completely: look for its fills
                    match = next((dict(id=oid, qty=0) for oid, fs in fill_by_order.items() if oid not in taken
                                  and all(str(f["exchangeId"]) == ex and f["side"].lower() == side for f in fs)
                                  and any(abs(float(f["price"] or 0) - price) < 1e-9 for f in fs)), None)
                if match is not None:
                    taken.add(match["id"])
                    ids.append(match["id"])
                    self.ledger.adopt(match["id"], it["bot"], it["sub"], it["layer"], it["order"])
            self.journal.close(e["key"], "ACKED" if ids else "NOT_PLACED", ids=ids, note="resolved by REST audit")
            if ids and on_adopt:
                try:
                    on_adopt(e, ids)                        # e.g. a pair set: recorded as a set, with repair tracking
                except Exception as err:
                    self.log(f"  ERR on_adopt: {err!r}")
            resolved += 1
        self.frozen = {}
        return resolved

    # ---------------------------------------------------------------------------------------- threads (live)
    def start(self):
        self.running = True                                 # two cancel workers: a burst of DELETEs goes out in parallel
        for fn in (self.step_cancels, self.step_cancels, self.step_placements):   # (A7: cancel queued -> sent <= 1 s)
            t = threading.Thread(target=self._loop, args=(fn,), daemon=True, name=fn.__name__)
            t.start()
            self.threads.append(t)

    def _loop(self, fn):
        while self.running:
            try:
                did = fn()
            except Exception as e:
                self.log(f"  ERR executor {fn.__name__}: {e!r}")
                did = 0
            if not did:
                with self.wake:
                    self.wake.wait(timeout=0.5)

    def stop(self, timeout=5):
        self.running = False
        with self.wake:
            self.wake.notify_all()
        for t in self.threads:
            t.join(timeout)
