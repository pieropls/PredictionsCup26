"""n_election.Election unit rules (day 5 §5; nothing deployed): a call needs an unbroken run of Polymarket prints past
0.97 for >= 1 s; the sniper never pays above 0.95, keeps free cash >= floor + 5k counting its unfilled orders, and stops a
race after 3 failed writes; the reversal guard; poll-close pulls and the kill switch keep exits and foreign orders.
python3 -m unittest tests.test_election"""
import os
import sys
import unittest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import n_election as el  # noqa: E402

META = {"1": dict(race="Ohio Senate", party="R", title="R Ohio Senate"), "2": dict(race="Ohio Senate", party="D", title="D Ohio Senate"),
        "3": dict(race="Utah Governor", party="R", title="R Utah Governor")}
CLOSE = {"Ohio Senate": 10_000.0, "Utah Governor": 20_000.0}


class Rig:
    def __init__(self, book=(0.80, 0.82, 5_000), fail=0):
        self.placed, self.cancelled, self.logs, self.fail = [], [], [], fail
        self.t = [0.0]
        self.bk = book
        self.E = el.Election(META, CLOSE, lambda: self.t[0], self.place, self.cancel, lambda ex: self.bk, lambda k, x: self.logs.append((k, x)),
                             manual=())

    def place(self, items, layer, ttl):
        if self.fail > 0:
            self.fail -= 1
            return None
        self.placed.append((items, layer, ttl))
        return "k%d" % len(self.placed)

    def cancel(self, o, reason=""):
        self.cancelled.append((o["id"], reason))
        return True


class CallTests(unittest.TestCase):
    def test_a_single_print_is_not_a_call_but_a_1s_run_is(self):
        r = Rig()
        r.t[0] = 100.0
        self.assertIsNone(r.E.on_pm("1", 0.98, 100.0, 50_000, {}))
        self.assertEqual(r.placed, [], "one print on a thin book: no order")
        self.assertIsNone(r.E.on_pm("1", 0.985, 100.5, 50_000, {}), "0.5 s: not yet")
        r.t[0] = 101.2
        key = r.E.on_pm("1", 0.99, 101.2, 50_000, {})
        self.assertIsNotNone(key)
        items, layer, ttl = r.placed[0]
        self.assertEqual((layer, ttl), ("callsnipe", el.SNIPER_TTL_S))
        ex, side, action, price, qty = items[0]
        self.assertEqual((ex, side, action, price), ("1", "yes", "buy", 0.82), "buys the called side at SIG's ask")
        self.assertEqual(qty, int(min(5_000, 0.25 * 50_000 / 0.82)), "25% of free cash per race, <= the visible depth")
        self.assertAlmostEqual(r.E.latency()["p50"], 1.2, places=3, msg="from the first print of the run to the order")

    def test_a_dip_breaks_the_run(self):
        r = Rig()
        r.E.on_pm("1", 0.98, 100.0, 50_000, {})
        r.E.on_pm("1", 0.90, 100.6, 50_000, {})
        r.t[0] = 101.5
        self.assertIsNone(r.E.on_pm("1", 0.98, 101.5, 50_000, {}))
        self.assertEqual(r.placed, [])

    def test_called_against_buys_the_complement_and_never_above_095(self):
        r = Rig(book=(0.02, 0.03, 9_000))                   # YES 0.02/0.03 -> NO ask 0.98: too expensive
        r.E.on_pm("2", 0.01, 100.0, 50_000, {})
        r.t[0] = 101.5
        r.E.on_pm("2", 0.005, 101.5, 50_000, {})
        self.assertEqual(r.E.calls["2"]["side"], "no")
        self.assertEqual(r.placed, [], "NO ask 0.98 > 0.95: nothing bought")
        r.bk = (0.06, 0.08, 9_000)                          # NO ask 0.94
        r.E.on_pm("2", 0.005, 103.0, 50_000, {})
        self.assertEqual(r.placed[0][0][0][:4], ("2", "no", "buy", 0.94))

    def test_floor_plus_5k_counts_unfilled_orders_and_the_race_budget_is_shared(self):
        r = Rig(book=(0.50, 0.52, 100_000))
        r.E.on_pm("1", 0.98, 100.0, 12_000, {})
        r.t[0] = 101.5
        r.E.on_pm("1", 0.99, 101.5, 12_000, {})
        q1 = r.placed[0][0][0][4]
        self.assertLessEqual(q1 * 0.52, 12_000 - 5_000 - 5_000 + 1e-6, "free cash stays >= floor + 5k")
        r.E.on_pm("1", 0.99, 102.0, 12_000, {})           # the same free cash again (not yet filled): nothing more
        self.assertEqual(len(r.placed), 1)
        r.E.on_pm("2", 0.01, 100.0, 50_000, {})           # the other market of the race shares the 25%
        r.t[0] = 103.0
        r.E.on_pm("2", 0.005, 103.0, 50_000, {})
        spent = sum(c["cost"] for c in r.E.calls.values())
        self.assertLessEqual(spent, 0.25 * 50_000 + 1e-6)

    def test_three_failed_writes_stop_the_race(self):
        r = Rig(fail=3)
        r.E.on_pm("1", 0.98, 100.0, 50_000, {})
        for t in (101.5, 102.5, 103.5, 104.5):
            r.t[0] = t
            r.E.on_pm("1", 0.99, t, 50_000, {})
        self.assertTrue(r.E.calls["1"]["stopped"])
        self.assertEqual(r.placed, [], "stopped before the writes came back")

    def test_reversal_exits_at_market_within_3c_and_stops(self):
        r = Rig(book=(0.80, 0.82, 5_000))
        r.E.on_pm("1", 0.98, 100.0, 50_000, {})
        r.t[0] = 101.5
        r.E.on_pm("1", 0.99, 101.5, 50_000, {})
        q = r.placed[0][0][0][4]
        r.bk = (0.70, 0.72, 5_000)
        r.t[0] = 400.0
        self.assertEqual(r.E.on_pm("1", 0.85, 400.0, 50_000, {"1": q}), "reversed")
        items, layer, _ = r.placed[-1]
        self.assertEqual(layer, "callexit")
        ex, side, action, price, qty = items[0]
        self.assertEqual((side, action, qty), ("no", "buy", q))
        self.assertAlmostEqual(1 - price, max(0.70, 0.82 - 0.03), places=3, msg="at the bid, never worse than 3c under entry")
        self.assertIsNone(r.E.on_pm("1", 0.99, 500.0, 50_000, {"1": q}), "the race is stopped")
        self.assertIsNone(r.E.called("1"))


class TickTests(unittest.TestCase):
    def orders(self):
        return [dict(id="a", ex="1", side="yes", action="buy", price=0.70, qty=100),      # dump entry
                dict(id="b", ex="1", side="no", action="buy", price=0.30, qty=50),        # exit of a long YES
                dict(id="c", ex="3", side="yes", action="buy", price=0.40, qty=100),      # other state
                dict(id="d", ex="1", side="yes", action="buy", price=0.60, qty=100)]      # foreign (hand-placed)

    def layer_of(self, o):
        return {"a": "dump", "b": "dumpexit", "c": "fatfinger", "d": None}.get(o["id"])

    def test_poll_close_pulls_entries_10_min_before_and_keeps_exits_and_foreign(self):
        r = Rig()
        pos = {"1": 500}
        r.E.on_tick(CLOSE["Ohio Senate"] - 700, 50_000, pos, self.orders(), self.layer_of)
        self.assertEqual(r.cancelled, [], "11 min before: nothing yet")
        r.E.on_tick(CLOSE["Ohio Senate"] - 590, 50_000, pos, self.orders(), self.layer_of)
        self.assertEqual([c[0] for c in r.cancelled], ["a"], "the entry goes; the exit, the other state and the foreign order stay")
        self.assertFalse(r.E.entries_allowed("1", CLOSE["Ohio Senate"] + 3_600))
        self.assertTrue(r.E.entries_allowed("1", CLOSE["Ohio Senate"] + 3 * 3_600))
        self.assertTrue(r.E.entries_allowed("3", CLOSE["Ohio Senate"] + 3_600), "Utah closes later")

    def test_kill_switch_pulls_entries_and_snipes_keeps_exits_and_foreign(self):
        r = Rig()
        n = r.E.kill(5_000.0, self.orders(), {"1": 500}, self.layer_of)
        self.assertEqual(n, 2)
        self.assertEqual(sorted(c[0] for c in r.cancelled), ["a", "c"])
        self.assertTrue(r.E.stopped)
        self.assertIsNone(r.E.on_pm("3", 0.99, 5_001.0, 50_000, {}), "no new calls after the stop")
        self.assertFalse(r.E.entries_allowed("3", 5_002.0))

    def test_post_call_bids_rest_only_under_the_touch(self):
        r = Rig(book=(0.80, 0.82, 5_000))
        r.E.on_pm("1", 0.98, 100.0, 50_000, {})
        r.t[0] = 101.5
        r.E.on_pm("1", 0.99, 101.5, 50_000, {})
        r.placed.clear()
        r.E.on_tick(200.0, 50_000, {}, [], lambda o: None)
        self.assertEqual(r.placed, [], "SIG still at 0.80/0.82: a 0.96 bid would lift the asks")
        r.bk = (0.965, 0.98, 5_000)
        r.E.on_tick(300.0, 50_000, {}, [], lambda o: None)
        prices = sorted(p for items, layer, _ in r.placed for (_, _, _, p, _) in items if layer == "postcall")
        self.assertEqual(prices, [0.96, 0.97], "resting bids up to the touch + 1 tick only")


if __name__ == "__main__":
    unittest.main()
