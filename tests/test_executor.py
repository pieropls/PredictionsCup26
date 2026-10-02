"""Executor (j_executor.py) against the fake exchange: invariants I2/I3, restarts, ownership, 429, staleness.
python3 -m unittest tests.test_executor"""
import copy
import os
import random
import sys
import tempfile
import unittest

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import a_config  # noqa: E402

a_config.LEDGER_FILE = os.path.join(tempfile.mkdtemp(), "ledger.json")
import d_ledger  # noqa: E402
import j_executor as jx  # noqa: E402
from fake_sig import FakeSIG, VClock  # noqa: E402

EXS = ["101", "102", "103", "104", "105"]
quiet = lambda *a: None


def rig(seed=1, positions=None, stale_s=30, journal_dir=None, ledger=None, fake=None, clock=None):
    clock = clock or VClock()
    fake = fake or FakeSIG(prices={ex: (0.40, 0.45, 0.42) for ex in EXS}, positions=positions or {}, seed=seed, clock=clock,
                           writes_per_min=10_000)
    ledger = ledger or d_ledger.Ledger(persist=False)
    journal = jx.Journal(journal_dir or tempfile.mkdtemp(), clock=clock.time)
    ex = jx.Executor(fake, ledger, journal, batch_size=5, stale_s=stale_s, clock=clock.time, sleep=clock.advance, log=quiet)
    return fake, ledger, journal, ex, clock


def drain(ex, n=50):
    for _ in range(n):
        if not (ex.step_cancels() + ex.step_placements()):
            break


def snapshot(fake, ledger, ex):
    opens = fake.open_orders()
    ex.update(fake.pos, opens)
    ledger.sync(opens, fake.fills(), lambda e: "manual", lambda *a: None, lambda *a: None)
    ledger.audit_subbooks(fake.positions(), log=quiet)
    return opens


def reducing_on_book(fake, ex_id):
    pos = fake.pos.get(ex_id, 0)
    return sum(o["qty"] for o in fake.open_orders() if o["ex"] == ex_id and jx.reduces(o["side"], o["action"], pos))


class InvariantTests(unittest.TestCase):
    def test_I2_I3_hold_under_random_orders_and_fills(self):
        for seed in range(int(os.environ.get("I2_SEEDS", 25))):        # I2_SEEDS=500 for a gate run
            rng = random.Random(seed)
            fake, ledger, journal, ex, clock = rig(seed, positions={e: rng.choice([-800, -300, 0, 300, 800]) for e in EXS},
                                                   stale_s=1e9)
            snapshot(fake, ledger, ex)
            ledger.init_subbooks(fake.positions(), manual_exs={EXS[0]})
            for step in range(60):
                r = rng.random()
                if r < 0.45:                                   # strategies submit a random mix of orders
                    items = []
                    for _ in range(rng.randint(1, 4)):
                        e = rng.choice(EXS)
                        side, action = rng.choice([("yes", "buy"), ("no", "buy"), ("yes", "sell"), ("no", "sell")])
                        layer = rng.choice(["exit", "mm", "take", "recycle", "touch"])
                        sub = rng.choice(["inventory", "manual", "pairset:x"])
                        items.append(("MKT t", sub, layer, (e, side, action, round(rng.choice([0.1, 0.3, 0.5, 0.7]), 3),
                                                            rng.choice([50, 200, 600, 1500]))))
                    ex.place(items)
                elif r < 0.55:                                 # someone places a manual (foreign) order
                    e = rng.choice(EXS)
                    fake.manual_order(e, rng.choice(["yes", "no"]), "buy", 0.05, rng.randint(10, 300))
                elif r < 0.8:                                  # fills on random resting orders
                    book = fake.open_orders()
                    if book:
                        o = rng.choice(book)
                        fake.fill(o["id"], rng.randint(1, int(o["qty"])))
                else:                                          # cancels of random owned orders
                    owned = [o for o in fake.open_orders() if o["id"] in ledger.orders]
                    if owned:
                        ex.cancel(rng.choice(owned))
                snapshot(fake, ledger, ex)
                for _ in range(3):
                    over = {e: max(0, reducing_on_book(fake, e) - abs(fake.pos.get(e, 0))) for e in EXS}
                    sent = ex.step_cancels() + ex.step_placements()
                    for e in EXS:                              # I2: a send never adds reduction beyond the position
                        after = max(0, reducing_on_book(fake, e) - abs(fake.pos.get(e, 0)))
                        self.assertLessEqual(after, over[e] + 1e-9, f"seed {seed} step {step}: a send over-reduced {e}")
                    snapshot(fake, ledger, ex)
                    if not sent:
                        break
                for e in set(EXS) | set(ledger.subbooks):     # I3: sub-books sum to the position
                    self.assertAlmostEqual(sum(ledger.subbooks.get(e, {}).values()), fake.pos.get(e, 0), places=6)
            # the engine's own sells never got converted into opposite buys (manual sells may, they're not ours)
            ours = {(o["ex"], o["side"], o["action"], o["price"], o["qty"]) for o in ledger.orders.values()}
            self.assertFalse([c for c in fake.canonicalized if tuple(c) in ours], f"seed {seed}: an engine sell flipped a position")

    def test_netting_ladder_fill_keeps_I2_I3(self):
        """plan_review A1: holding 1,000 NO, a YES fill of 200 nets to 800 NO; the rebound can only close what it bought."""
        fake, ledger, journal, ex, clock = rig(positions={"101": -1000})
        snapshot(fake, ledger, ex)
        ledger.init_subbooks(fake.positions(), manual_exs=set())
        ex.place([("MKT t", "tranche:1", "ladder", ("101", "yes", "buy", 0.40, 200))])
        drain(ex)
        oid = next(o["id"] for o in fake.open_orders())
        fake.fill(oid, 200)
        snapshot(fake, ledger, ex)
        self.assertEqual(fake.pos["101"], -800)
        self.assertEqual(ledger.subbooks["101"], {"inventory": -1000, "tranche:1": 200})
        ex.place([("MKT t", "tranche:1", "rebound", ("101", "no", "buy", 0.53, 200))])     # the rebound re-buys the NO
        ex.place([("MKT t", "inventory", "exit", ("101", "yes", "buy", 0.30, 5000))])      # an oversized exit gets trimmed
        drain(ex)
        snapshot(fake, ledger, ex)
        self.assertLessEqual(reducing_on_book(fake, "101"), 800)


class RestartTests(unittest.TestCase):
    def test_three_kill_restarts_mid_batch(self):
        """Kill the process while a batch is half-landed, three times: no duplicates, nothing priced before a restart."""
        jdir, clock = tempfile.mkdtemp(), VClock()
        fake = FakeSIG(prices={e: (0.40, 0.45, 0.42) for e in EXS}, clock=clock, seed=7, writes_per_min=10_000)
        ledger = d_ledger.Ledger(persist=False)
        version, saved = 0, {}

        class Killed(BaseException):
            pass

        def save():                                            # the engine saves its ledger once per plan cycle
            saved.update({k: copy.deepcopy(getattr(ledger, k)) for k in ("orders", "fills", "baskets", "subbooks")},
                         seen=set(ledger.seen), synced=ledger.synced)

        def reload():                                          # a new process: the ledger as of its last save
            new = d_ledger.Ledger(persist=False)
            for k, v in saved.items():
                setattr(new, k, copy.deepcopy(v))
            return new

        def plan(ex, version):
            """One plan cycle: one YES bid per market at 0.40 + version ticks; keep a resting bid within 1c."""
            opens = snapshot(fake, ledger, ex)
            if ex.frozen or ex.journal.unknown():
                ex.resolve_unknown(opens, fake.fills())
            want = round(0.40 + 0.005 * version, 3)
            have = {}
            for o in opens:
                if o["id"] in ledger.orders:
                    have.setdefault(o["ex"], []).append(o)
            for p in ex.pending():
                have.setdefault(p["ex"], []).append(p)
            items = []
            for e in EXS:
                if ex.is_frozen(e):
                    continue
                mine = have.get(e, [])
                keep = [o for o in mine if abs(o["price"] - want) <= 0.01 + 1e-9]
                for o in mine:
                    if o not in keep[:1] and o.get("id"):
                        ex.cancel(o)
                if not keep:
                    items.append(("MKT t", "inventory", "mm", (e, "yes", "buy", want, 100)))
            if items:
                ex.place(items)
            save()
            return want

        for restart in range(3):
            journal = jx.Journal(jdir, clock=clock.time)
            ex = jx.Executor(fake, ledger, journal, batch_size=2, clock=clock.time, sleep=clock.advance, log=quiet)
            want = plan(ex, version)
            drain(ex)
            want = plan(ex, version)                           # steady state: nothing new
            drain(ex)
            # move the price, then die halfway through the next batch
            version += 2
            for o in [o for o in fake.open_orders() if o["id"] in ledger.orders]:
                ex.cancel(o)
            drain(ex)
            real, writes = fake._place, []

            def dying(orders, ttl, key, atomic, k=restart + 1):
                writes.append(key)
                if len(writes) == 1:                           # acked after the last ledger save: only the journal knows
                    return real(orders, ttl, key, atomic)
                for o in orders[:k]:                           # the next write lands k orders, then the process dies
                    fake._land(o, ttl)
                raise Killed()
            fake._place = dying
            plan(ex, version)
            with self.assertRaises(Killed):
                drain(ex)
            fake._place = real
            ledger = reload()
            version += 3                                       # prices moved while we were down
            journal = jx.Journal(jdir, clock=clock.time)
            self.assertTrue(journal.unknown(), "the half-sent batch is UNKNOWN after the restart")
            ex = jx.Executor(fake, ledger, journal, batch_size=2, clock=clock.time, sleep=clock.advance, log=quiet)
            self.assertTrue(ex.frozen, "its markets are frozen until the audit")
            self.assertEqual(ex.recovered, 2, "the acked-but-unsaved write is ours again (from the journal)")
            before = set(journal.entries)
            want = plan(ex, version)                          # audit adopts what landed, then re-plans at today's price
            drain(ex)
            plan(ex, version)
            drain(ex)
            book = [o for o in fake.open_orders()]
            per_market = {}
            for o in book:
                per_market[o["ex"]] = per_market.get(o["ex"], 0) + 1
            self.assertTrue(all(n == 1 for n in per_market.values()), f"restart {restart}: duplicates {per_market}")
            self.assertEqual(set(per_market), set(EXS))
            for k, e in journal.entries.items():               # nothing sent after the restart carries an old price
                if k not in before and e.get("kind") in ("batch", "multileg"):
                    for it in e["items"]:
                        self.assertEqual(it["order"][3], want, f"restart {restart}: order priced before the restart")
            self.assertFalse(any(e.get("status") == "IN_FLIGHT" for e in journal.entries.values() if e["key"] not in before
                                 and e.get("status") not in ("ACKED", "NOT_PLACED", "RESOLVED", "UNKNOWN")))


class ExecutorRuleTests(unittest.TestCase):
    def test_foreign_orders_are_never_cancelled_and_cancel_all_needs_full_ownership(self):
        fake, ledger, journal, ex, clock = rig(positions={"101": 500})
        manual = fake.manual_order("101", "yes", "sell", 0.9, 100)
        snapshot(fake, ledger, ex)
        self.assertFalse(ex.cancel(dict(id=str(manual), ex="101")))
        self.assertFalse(ex.can_cancel_exchange("101"))
        ex.place([("MKT t", "inventory", "mm", ("102", "yes", "buy", 0.30, 100))])
        drain(ex)
        snapshot(fake, ledger, ex)
        self.assertTrue(ex.can_cancel_exchange("102"))
        self.assertIn(manual, fake.book)
        with self.assertRaises(ValueError):
            fake.cancel_all(None)

    def test_foreign_reducing_orders_use_up_capacity(self):
        """A manual sell of 300 resting on a 500-share position leaves 200 for the engine's exit."""
        fake, ledger, journal, ex, clock = rig(positions={"101": 500})
        fake.manual_order("101", "yes", "sell", 0.95, 300)
        snapshot(fake, ledger, ex)
        ex.place([("MKT t", "inventory", "exit", ("101", "no", "buy", 0.08, 500))])
        drain(ex)
        ours = [o for o in fake.open_orders() if o["id"] in ledger.orders]
        self.assertEqual([o["qty"] for o in ours], [200.0])

    def test_429_pauses_and_retries_without_duplicates(self):
        fake, ledger, journal, ex, clock = rig()
        snapshot(fake, ledger, ex)
        fake.faults = [("429", 10)]
        ex.place([("MKT t", "inventory", "mm", ("101", "yes", "buy", 0.30, 100))])
        ex.step_placements()
        self.assertEqual(len(fake.book), 0)
        self.assertEqual(ex.step_placements(), 0, "paused during Retry-After")
        clock.advance(11)
        self.assertEqual(ex.step_placements(), 1, "re-sent after the pause")
        self.assertEqual(len(fake.book), 1)
        fake.faults = [("429", 60)]                            # a long pause: the job goes stale and is re-planned, not sent
        ex.place([("MKT t", "inventory", "mm", ("102", "yes", "buy", 0.30, 100))])
        ex.step_placements()
        clock.advance(61)
        self.assertEqual(ex.step_placements(), 0)
        self.assertEqual((len(fake.book), ex.dropped_stale), (1, 1))

    def test_unknown_outcome_freezes_until_the_audit(self):
        fake, ledger, journal, ex, clock = rig()
        snapshot(fake, ledger, ex)
        fake.faults = ["timeout_ok"]
        ex.place([("MKT t", "inventory", "mm", ("101", "yes", "buy", 0.30, 100))])
        ex.step_placements()
        self.assertTrue(ex.is_frozen("101"))
        self.assertIsNone(ex.place([("MKT t", "inventory", "mm", ("101", "yes", "buy", 0.30, 100))]), "frozen: not accepted")
        ex.resolve_unknown(fake.open_orders(), fake.fills())
        self.assertFalse(ex.is_frozen("101"))
        landed = next(iter(fake.book))
        self.assertIn(str(landed), ledger.orders, "the order that landed was adopted as ours")

    def test_stale_queued_placements_are_dropped_not_sent(self):
        fake, ledger, journal, ex, clock = rig(stale_s=30)
        snapshot(fake, ledger, ex)
        ex.place([("MKT t", "inventory", "mm", ("101", "yes", "buy", 0.30, 100))])
        clock.advance(31)
        self.assertEqual(ex.step_placements(), 0)
        self.assertEqual(len(fake.book), 0)
        self.assertEqual(ex.dropped_stale, 1)

    def test_journal_never_replays_after_restart(self):
        jdir = tempfile.mkdtemp()
        fake, ledger, journal, ex, clock = rig(journal_dir=jdir)
        journal.open("k1", "batch", [("MKT t", "inventory", "mm", ("101", "yes", "buy", 0.30, 100))])
        j2 = jx.Journal(jdir, clock=clock.time)
        self.assertEqual([e["key"] for e in j2.unknown()], ["k1"])
        ex2 = jx.Executor(fake, ledger, j2, clock=clock.time, log=quiet)
        drain(ex2)
        self.assertEqual(fake.calls, [], "nothing re-sent after the restart")


if __name__ == "__main__":
    unittest.main()
