"""Bot logic (e_bots.py), no I/O.  python3 -m unittest tests.test_bots"""
import os
import sys
import unittest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import e_bots as bots  # noqa: E402


def snap(pos=0, fair=0.5, bid=0.48, ask=0.52, add_ok=True, focus=True, cooldown=None):
    return dict(pos=pos, fair=fair, bid=bid, ask=ask, last=None, avg=None, upnl=None, add_ok=add_ok, focus=focus,
                cooldown=cooldown or {})


def layers(wants):
    return sorted((w["layer"], w["side"], w["px"]) for w in wants)


class PositionBotTests(unittest.TestCase):
    AK = [(0.30, .15), (0.40, .20), (0.50, .20), (0.60, .20), (0.775, .25)]

    def test_ladder_top_down_with_leftover_to_lowest_exit(self):
        b = bots.PositionBot("AK", "1057", {}, self.AK, [(0.10, 300)], 12971, "no")
        w = b.plan(snap(pos=-12971, fair=0.785))
        self.assertEqual(sorted((x["px"], x["qty"]) for x in w if x["action"] == "sell"),
                         [(0.30, 1947), (0.40, 2594), (0.50, 2594), (0.60, 2594), (0.775, 3242)])
        self.assertEqual([(x["px"], x["qty"]) for x in w if x["action"] == "buy"], [(0.10, 3000)])

    def test_sweep_fills_cheapest_tranche_and_it_disappears(self):
        b = bots.PositionBot("AK", "1057", {}, self.AK, [], 12971, "no")
        w = b.plan(snap(pos=-(12971 - 1947), fair=0.785))
        self.assertNotIn(0.30, [x["px"] for x in w])

    def test_exits_below_fair_are_held(self):
        b = bots.PositionBot("RI", "1073", {}, [(0.97, .5), (0.98, .5)], [], 6283, "yes")
        self.assertEqual([(x["px"], x["qty"]) for x in b.plan(snap(pos=6283, fair=0.975))], [(0.98, 3142)])

    def test_trim_never_sells_more_than_held(self):
        kept = [dict(side="no", action="sell", price=0.775, qty=3242)]
        places = [dict(side="no", action="sell", px=0.6, qty=2594), dict(side="no", action="sell", px=0.3, qty=1947)]
        out = bots.trim_sells(5000, kept, places)
        self.assertLessEqual(sum(x["qty"] for x in out) + 3242, 5000)


class MarketBotTests(unittest.TestCase):
    def bot(self):
        return bots.MarketBot("1", dict(race="X", party="D"))

    def test_exit_is_capped_in_shares_and_never_crosses_zero(self):
        # Codex 5.1 repro: fair 0.90, +100 YES must never become a 1,125-share NO buy
        w = self.bot().plan(snap(pos=100, fair=0.90, bid=0.88, ask=0.92, add_ok=False))
        self.assertEqual([(x["layer"], x["side"], x["qty"], x["qmax"]) for x in w], [("exit", "no", 100, 100)])

    def test_exit_price_takes_the_better_of_fair_and_sig_book(self):
        w = self.bot().plan(snap(pos=100, fair=0.50, bid=0.55, ask=0.60, add_ok=False))
        self.assertEqual(w[0]["px"], 0.405)            # sell YES at 0.595 (SIG ask - tick) rather than 0.51 (PM + 1c)

    def test_no_adds_on_the_closing_side(self):
        w = self.bot().plan(snap(pos=200, fair=0.5, bid=0.40, ask=0.60))
        self.assertFalse([x for x in w if x["side"] == "no" and x["layer"] != "exit"])

    def test_take_only_when_sig_ask_gapped_through_pm(self):
        self.assertIn(("take", "yes", 0.46), layers(self.bot().plan(snap(fair=0.50, bid=0.40, ask=0.45, focus=False))))
        self.assertNotIn("take", [x[0] for x in layers(self.bot().plan(snap(fair=0.50, bid=0.40, ask=0.47, focus=False)))])

    def test_touch_only_below_sig_mid_and_pm(self):
        w = layers(self.bot().plan(snap(fair=0.60, bid=0.40, ask=0.50)))
        self.assertIn(("touch", "yes", 0.405), w)       # improve 0.40 by a tick; below SIG mid 0.45 and PM 0.585
        w = layers(self.bot().plan(snap(fair=0.41, bid=0.40, ask=0.50)))
        self.assertNotIn("touch", [x[0] for x in w if x[1] == "yes"])   # PM - 1.5c = 0.395 is below the SIG bid: don't join

    def test_ladder_and_focus(self):
        self.assertIn(("ladder", "yes", 0.4), layers(self.bot().plan(snap(fair=0.50))))
        self.assertNotIn("ladder", [x[0] for x in layers(self.bot().plan(snap(fair=0.50, focus=False)))])

    def test_no_risk_no_adds_and_cooldown(self):
        self.assertEqual(self.bot().plan(snap(fair=0.5, add_ok=False)), [])
        self.assertFalse([x for x in self.bot().plan(snap(fair=0.5, cooldown={"yes": True})) if x["side"] == "yes"])

    def test_per_market_cap(self):
        w = self.bot().plan(snap(pos=6000, fair=0.5, bid=0.4, ask=0.6))
        self.assertFalse([x for x in w if x["side"] == "yes"])

    def test_oversized_resting_exit_is_cancelled(self):
        w = self.bot().plan(snap(pos=100, fair=0.90, bid=0.88, ask=0.92, add_ok=False))
        big = dict(id="1", ex="1", side="no", action="buy", price=w[0]["px"], qty=140.0)
        self.assertEqual([o["id"] for o in bots.reconcile(w, [big])[1]], ["1"])


class PairBotTests(unittest.TestCase):
    def test_no_set_sweeps_while_it_still_locks_the_edge(self):
        p = bots.PairBot("MI", [("D", "d"), ("R", "r")])
        self.assertEqual(p.screen({"d": (0.705, 0.71, None), "r": (0.305, 0.31, None)}, {}, 0.005, 0.01), [(0.010000000000000009, "no")])
        orders, q, m = p.size("no", {"d": [(0.705, 1099), (0.70, 1906)], "r": [(0.305, 4794)]}, {}, 0.005, 0.01, 3000)
        self.assertEqual((orders, q), ([("d", "no", "buy", 0.3, 3000), ("r", "no", "buy", 0.695, 3000)], 3000))
        self.assertAlmostEqual(m, 0.005)

    def test_yes_set_prices_the_outsider_from_polymarket(self):
        p = bots.PairBot("SD", [("D", "d"), ("R", "r")])
        book = {"d": (0.025, 0.03, None), "r": (0.92, 0.93, None)}
        self.assertEqual(p.screen(book, {"d": 0.0015, "r": 0.91}, 0.005, 0.01), [])     # independent ~9%: not an arb
        self.assertEqual(p.screen(book, {}, 0.005, 0.01), [])                            # no fairs: no YES set
        q = bots.PairBot("X", [("D", "d"), ("R", "r")]).screen({"d": (0.47, 0.48, None), "r": (0.49, 0.50, None)}, {"d": .5, "r": .5}, 0.005, 0.01)
        self.assertEqual([s for _, s in q], ["yes"])

    def test_three_way_no_set_needs_bids_above_two(self):
        p = bots.PairBot("MT", [("D", "d"), ("I", "i"), ("R", "r")])
        self.assertEqual(p.screen({"d": (0.10, 0.11, None), "i": (0.16, 0.17, None), "r": (0.75, 0.76, None)}, {}, 0.005, 0.01), [])


if __name__ == "__main__":
    unittest.main()
