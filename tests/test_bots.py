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


def mb_state(**kw):
    s = dict(fair=0.50, u=0.005, bid=0.48, ask=0.52, ref=None, kappa=0.0, subs={}, layers={}, paused=False, blocked=set(),
             room={"yes": 3000, "no": 3000}, tranches=[], ladder_ids={}, take_notional=150)
    s.update(kw)
    return s


def sell_px(w):
    """A reducing complement buy at p sells the held side at 1 - p."""
    return round(1 - w["px"], 3)


class MarketBotTests(unittest.TestCase):
    def setUp(self):
        self.b = bots.MarketBot("1060", dict(race="MA Gov", party="R", title="t"))

    def test_recycler_sells_into_a_bid_above_fair_else_rests_above_fair(self):
        # holding 1,000 NO (fair NO 0.50); NO bid = 1 - YES ask = 0.51 >= 0.505 -> sell into it: buy YES @ 0.49
        ws = self.b.plan(mb_state(subs={"inventory": -1000}, layers={"recycler": True}, bid=0.47, ask=0.49))
        self.assertEqual([(w["side"], w["px"], w["qty"], w["layer"], w["sub"], w["ttl"]) for w in ws],
                         [("yes", 0.49, 1000, "recycle", "inventory", 300)])
        # NO bid 0.48 < 0.505: rest at max(fair + 1c, NO ask - 1 tick) = max(0.51, 0.515) -> buy YES @ 0.485
        ws = self.b.plan(mb_state(subs={"inventory": -1000}, layers={"recycler": True}, bid=0.48, ask=0.52))
        self.assertEqual([(w["px"], w["ttl"]) for w in ws], [(0.485, 1800)])
        self.assertGreaterEqual(sell_px(ws[0]), 0.51 - 1e-9)

    def test_recycler_never_undercuts_or_hits_its_own_orders(self):
        # our resting exit (buy YES @ 0.485 = sell NO @ 0.515) IS the NO best ask (YES bid 0.485): keep it, don't step down
        s = mb_state(subs={"inventory": -1000}, layers={"recycler": True}, bid=0.485, ask=0.52,
                     own=[dict(side="yes", action="buy", price=0.485, layer="recycle")])
        self.assertEqual([w["px"] for w in self.b.plan(s)], [0.485])
        # the NO bid 0.51 (>= fair + 0.5c) is our own MM NO bid: not a buyer to sell into
        s = mb_state(subs={"inventory": -1000}, layers={"recycler": True}, bid=0.47, ask=0.49,
                     own=[dict(side="no", action="buy", price=0.51, layer="mm")])
        self.assertEqual([w["ttl"] for w in self.b.plan(s)], [1800], "rests instead")

    def test_recycler_sells_mm_holdings_into_a_bid_above_fair_and_skips_wrong_sign_subs(self):
        # holding 400 NO in the MM sub-book; NO bid 0.51 >= NO fair 0.50 + 0.5c: sell into it
        ws = self.b.plan(mb_state(subs={"mm": -400}, layers={"recycler": True}, bid=0.47, ask=0.49))
        self.assertEqual([(w["side"], w["px"], w["qty"], w["layer"], w["sub"]) for w in ws], [("yes", 0.49, 400, "recycle", "mm")])
        # ... but no resting recycler exit for MM holdings when the bid is below fair (the MM rests its own)
        self.assertEqual(self.b.plan(mb_state(subs={"mm": -400}, layers={"recycler": True}, bid=0.48, ask=0.52)), [])
        # an inventory sub-book of the opposite sign to the net position is an audit residue: not recycled
        ws = self.b.plan(mb_state(subs={"inventory": 380, "pairset:x": -1379}, layers={"recycler": True}, bid=0.47, ask=0.49))
        self.assertEqual(ws, [])

    def test_A2_no_reducing_order_below_fair_minus_kappa(self):
        for kappa in (0.0, 0.01, 0.02, 0.03):
            for fair in (0.05, 0.3, 0.5, 0.8, 0.97):
                for bid, ask in ((fair - 0.10, fair - 0.05), (fair - 0.02, fair + 0.02), (fair + 0.03, fair + 0.06)):
                    bid, ask = max(0.005, round(bid, 3)), min(0.995, round(ask, 3))
                    for subs in ({"inventory": -500}, {"inventory": 500}, {"mm": 400}, {"mm": -400}, {"tranche:x": 300}):
                        tr = [dict(sub="tranche:x", ref_side=min(0.99, fair + 0.05), avg=max(0.01, fair - 0.10), state="open")]
                        ws = self.b.plan(mb_state(fair=fair, bid=bid, ask=ask, kappa=kappa, subs=subs, tranches=tr,
                                                  layers={"recycler": True, "mm": True, "ladder": True}, ref=fair))
                        for w in ws:
                            if w.get("reduce"):
                                have = "no" if w["side"] == "yes" else "yes"
                                fs = fair if have == "yes" else 1 - fair
                                self.assertGreaterEqual(sell_px(w), fs - kappa - 1e-9, (kappa, fair, bid, ask, subs, w))

    def test_A3_roc_filter_rejects_expensive_side_accepts_cheap_side(self):
        # MM entries: a 2c edge on NO at 0.95 is 2.1% ROC (rejected); on YES at 0.10 it is 25% (accepted)
        ws = self.b.plan(mb_state(fair=0.97, u=0.0, bid=0.90, ask=0.995, layers={"mm": True}))
        self.assertFalse([w for w in ws if w["side"] == "yes"], "YES at ~0.96: ROC ~1%, rejected")
        ws = self.b.plan(mb_state(fair=0.12, u=0.0, bid=0.05, ask=0.20, layers={"mm": True}))
        yes = [w for w in ws if w["side"] == "yes"]
        self.assertTrue(yes and yes[0]["roc"] >= 0.04, ws)
        self.assertEqual(bots.roc_of(0.97, 0.95) < 0.04, True)
        self.assertEqual(bots.roc_of(0.12, 0.10) >= 0.04, True)

    def test_mm_quotes_both_sides_around_fair_and_never_crosses(self):
        ws = self.b.plan(mb_state(fair=0.50, u=0.005, bid=0.40, ask=0.60, layers={"mm": True}))
        q = {w["side"]: w["px"] for w in ws}
        self.assertEqual(q, {"yes": 0.48, "no": 0.48})              # e = max(2c, 0.75c) * s(0.5)=1 -> 2c each side
        ws = self.b.plan(mb_state(fair=0.50, u=0.005, bid=0.40, ask=0.47, layers={"mm": True}))
        self.assertLess({w["side"]: w["px"] for w in ws}["yes"], 0.47, "a bid never at/above the ask")

    def test_turnover_mm_quotes_at_sig_level_with_pm_guardrails(self):
        tmm = lambda **k: dict(dict(mid=0.50, kappa_mm=0.04, size=300, room={"yes": 5000, "no": 5000}, skew=0.0, offer_mid=False), **k)
        # SIG 0.48 / 0.52, mid 0.50: quotes join the touch (half spread 2c); PM fair 0.53 -> YES bid 0.48 <= 0.52 ok,
        # NO bid 0.48 (YES ask 0.52) needs NO <= fair_no - 1c = 0.46 -> no NO entry
        ws = self.b.plan(mb_state(fair=0.53, bid=0.48, ask=0.52, layers={"mm": True}, tmm=tmm()))
        self.assertEqual([(w["side"], w["px"]) for w in ws if w["layer"] == "mm"], [("yes", 0.48)])
        # fair 0.50: both sides 0.48 (each <= fair_side - 1c); a 1-tick spread -> quotes stay one tick off the touch's far side
        ws = self.b.plan(mb_state(fair=0.50, bid=0.48, ask=0.52, layers={"mm": True}, tmm=tmm()))
        self.assertEqual(sorted((w["side"], w["px"]) for w in ws), [("no", 0.48), ("yes", 0.48)])
        # never buy above fair - 1c: PM fair 0.485 -> no YES bid at 0.48
        ws = self.b.plan(mb_state(fair=0.485, bid=0.48, ask=0.52, layers={"mm": True}, tmm=tmm()))
        self.assertNotIn("yes", [w["side"] for w in ws])
        # long YES 2,000: sell at the SIG ask 0.52 even with PM fair 0.55 (>= fair - kappa_mm = 0.51), skewed lower
        ws = self.b.plan(mb_state(fair=0.55, bid=0.48, ask=0.52, subs={"mm": 2000}, layers={"mm": True}, tmm=tmm(skew=1.0)))
        ex_ = [w for w in ws if w["reduce"]]
        self.assertEqual([(w["side"], w["qty"]) for w in ex_], [("no", 2000)])
        self.assertAlmostEqual(1 - ex_[0]["px"], 0.515, places=3)            # ask 0.52 - 1 tick of skew
        # ... but never below fair - kappa_mm: PM fair 0.60, kappa 0.04 -> 0.56 > ask -> no sell this loop
        ws = self.b.plan(mb_state(fair=0.60, bid=0.48, ask=0.52, subs={"mm": 2000}, layers={"mm": True}, tmm=tmm()))
        self.assertFalse([w for w in ws if w["reduce"]])
        # inventory older than 2 h: offered at SIG mid
        ws = self.b.plan(mb_state(fair=0.53, bid=0.48, ask=0.54, subs={"mm": 2000}, layers={"mm": True}, tmm=tmm(mid=0.51, offer_mid=True)))
        self.assertAlmostEqual(1 - [w for w in ws if w["reduce"]][0]["px"], 0.51, places=3)

    def test_mm_reducing_side_always_quoted_even_when_paused_and_skewed(self):
        ws = self.b.plan(mb_state(fair=0.50, subs={"mm": 3000}, layers={"mm": True}, paused=True))
        self.assertEqual([(w["side"], w["qty"], w["reduce"]) for w in ws], [("no", 3000, True)], "only the reducing NO bid")
        ws = self.b.plan(mb_state(fair=0.50, subs={"mm": 3000}, layers={"mm": True}))
        q = {w["side"]: w["px"] for w in ws}
        self.assertEqual(q["no"], 0.485, "long YES: the NO bid is skewed up by ~1.5c x share of the cap")

    def test_ladder_anchors_to_sig_ref_gated_by_fair(self):
        # A5 AK replay: ref 0.745, PM fair 0.785, s(0.785) ~ 0.82: YES levels at min(0.745 - off, 0.785 - 1.6c)
        b = bots.MarketBot("1057", dict(race="AK Gov", party="D", title="t"))
        ws = b.plan(mb_state(fair=0.785, u=0.005, bid=0.74, ask=0.75, ref=0.745, layers={"ladder": True},
                             ladder_ids={("yes", 0): "tranche:a", ("yes", 1): "tranche:b", ("no", 0): "tranche:c", ("no", 1): "tranche:d"}))
        yes = sorted((w["px"] for w in ws if w["side"] == "yes"), reverse=True)
        self.assertEqual(yes, [0.66, 0.58])
        self.assertTrue(all(w["sub"].startswith("tranche:") and w["ttl"] == 4 * 3600 for w in ws))
        self.assertFalse(b.plan(mb_state(fair=0.785, ref=None, layers={"ladder": True})), "no ref yet: no ladder")

    def test_A5_rebound_replay_with_and_without_kappa(self):
        """AK 11:00 sweep: ref 0.745, fair 0.785; YES fills at 0.70/0.65/0.60 rebound at max(ref - 0.3d, fair - k, fill + 1c)."""
        b = bots.MarketBot("1057", dict(race="AK Gov", party="D", title="t"))
        out = {}
        for kappa in (0.0, 0.06):
            tr = [dict(sub=f"tranche:{p}", ref_side=0.745, avg=p, state="open") for p in (0.70, 0.65, 0.60)]
            ws = b.plan(mb_state(fair=0.785, bid=0.60, ask=0.62, kappa=kappa, subs={t["sub"]: 200 for t in tr}, tranches=tr))
            out[kappa] = sorted((round(sell_px(w), 3) for w in ws if w["layer"] == "rebound"), reverse=True)
        self.assertEqual(out[0.0], [0.785, 0.785, 0.785], "kappa 0: they rest at fair")
        self.assertEqual(out[0.06], [0.735, 0.725, 0.725], "kappa 6c: ref - 0.3d = 0.7315 (ticked up), then the A2 floor 0.725")

    def test_take_only_far_below_fair(self):
        ws = self.b.plan(mb_state(fair=0.50, u=0.005, bid=0.40, ask=0.47, layers={"sniper": True}))
        self.assertFalse(ws, "3c gap < 4c: no take")
        ws = self.b.plan(mb_state(fair=0.50, u=0.005, bid=0.40, ask=0.45, layers={"sniper": True}))
        self.assertEqual([(w["side"], w["px"], w["layer"]) for w in ws], [("yes", 0.46, "take")])

    def test_paused_or_blocked_side_gets_no_entries(self):
        ws = self.b.plan(mb_state(layers={"mm": True}, paused=True))
        self.assertEqual(ws, [])
        ws = self.b.plan(mb_state(layers={"mm": True}, blocked={"yes"}))
        self.assertEqual({w["side"] for w in ws}, {"no"})

    def test_day2_roc_minimums(self):
        # a YES MM bid with ~5% ROC: accepted at 4%, rejected at 6% (free cash below its target)
        s = dict(fair=0.42, u=0.0, bid=0.30, ask=0.60, layers={"mm": True})
        ws = self.b.plan(mb_state(**s))
        yes = [w for w in ws if w["side"] == "yes"]
        self.assertTrue(yes and 0.04 <= yes[0]["roc"] < 0.06, ws)
        self.assertFalse([w for w in self.b.plan(mb_state(roc_min=0.06, **s)) if w["side"] == "yes"])
        # a favourite we already hold (fair 0.97, holding YES): 6% even at the normal 4%
        s2 = dict(fair=0.97, u=0.08, bid=0.80, ask=0.995, layers={"mm": True})   # wide PM: e ~4.1c, ROC ~4.4%
        free = [w for w in self.b.plan(mb_state(held=0, **s2)) if w["side"] == "yes"]
        held = [w for w in self.b.plan(mb_state(held=500, subs={"inventory": 500}, **s2)) if w["side"] == "yes"]
        self.assertTrue(free and free[0]["roc"] < 0.06, free)
        self.assertEqual(held, [])

    def test_market_cap_room(self):
        ws = self.b.plan(mb_state(layers={"mm": True}, room={"yes": 0.0, "no": 3000}))
        self.assertEqual({w["side"] for w in ws}, {"no"})


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
