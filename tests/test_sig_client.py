"""b_sig write path with a mocked HTTP session: a write whose first attempt timed out is never reported as "not sent".
python3 -m unittest tests.test_sig_client"""
import importlib.util
import json
import os
import time
import types
import unittest

import requests

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


def load_client():
    spec = importlib.util.spec_from_file_location("b_sig_under_test", os.path.join(ROOT, "b_sig.py"))
    m = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(m)
    m.DRY_RUN, m.T = False, {"id": "t", "slug": "s"}
    m.time = types.SimpleNamespace(time=time.time, sleep=lambda s: None)    # this module's sleeps only
    return m


class Resp:
    def __init__(self, status, body=None, headers=None):
        self.status_code, self.headers = status, headers or {}
        self.text = json.dumps(body or {})
        self.content = self.text.encode()
        self.ok = status < 400

    def json(self):
        return json.loads(self.text)


ORDERS = [("101", "yes", "buy", 0.30, 100), ("102", "no", "buy", 0.40, 100)]
OK = Resp(200, {"results": [{"index": 0, "ok": True, "data": {"orderId": 1}}, {"index": 1, "ok": True, "data": {"orderId": 2}}]})


class WritePathTests(unittest.TestCase):
    def run_with(self, *answers):
        m, sent = load_client(), []

        def request(method, url, timeout=None, json=None, **kw):
            sent.append(json)
            a = answers[len(sent) - 1]
            if isinstance(a, Exception):
                raise a
            return a
        m.S.request = request
        return m, sent

    def test_timeout_then_success_reuses_the_idempotency_key(self):
        m, sent = self.run_with(requests.Timeout(), OK)
        out = m.place_batch(ORDERS, key="k1")
        self.assertEqual([d["orderId"] for _, _, d in out], [1, 2])
        self.assertEqual([b["idempotencyKey"] for b in sent], ["k1", "k1"])

    def test_timeout_then_429_is_unknown_not_rate_limited(self):
        m, _ = self.run_with(requests.Timeout(), Resp(429, {"error": "slow down"}, {"Retry-After": "30"}))
        with self.assertRaises(m.Unknown):
            m.place_batch(ORDERS, key="k1")

    def test_timeout_then_no_budget_is_unknown(self):
        m, _ = self.run_with(requests.Timeout())
        m.WRITES_PER_MIN = 1
        with self.assertRaises(m.Unknown):
            m.place_batch(ORDERS, key="k1")

    def test_429_on_the_first_attempt_is_rate_limited(self):
        m, _ = self.run_with(Resp(429, {}, {"Retry-After": "7"}))
        with self.assertRaises(m.RateLimited) as cm:
            m.place_batch(ORDERS, key="k1")
        self.assertEqual(cm.exception.retry_after, 7.0)

    def test_two_timeouts_or_5xx_are_unknown(self):
        for answers in [(requests.Timeout(), requests.Timeout()), (Resp(503), Resp(500)), (Resp(503), requests.ConnectionError())]:
            m, _ = self.run_with(*answers)
            with self.assertRaises(m.Unknown):
                m.place_batch(ORDERS, key="k1")

    def test_request_in_flight_is_unknown(self):
        m, _ = self.run_with(requests.Timeout(), Resp(409, {"error": {"code": "REQUEST_IN_FLIGHT"}}))
        with self.assertRaises(m.Unknown):
            m.place_multileg(ORDERS, key="k1")

    def test_a_4xx_on_the_first_attempt_is_a_plain_rejection(self):
        m, _ = self.run_with(Resp(400, {"error": "bad price"}))
        with self.assertRaises(RuntimeError) as cm:
            m.place_batch(ORDERS, key="k1")
        self.assertNotIsInstance(cm.exception, m.Unknown)

    def test_multileg_results_without_ok_are_accepted(self):
        m, _ = self.run_with(Resp(200, {"results": [{"index": 0, "data": {"orderId": 5}}, {"index": 1, "data": {"orderId": 6}}]}))
        self.assertEqual([ok for _, ok, _ in m.place_multileg(ORDERS, key="k1")], [True, True])

    def test_tournament_wide_cancel_all_raises(self):
        m, sent = self.run_with()
        with self.assertRaises(ValueError):
            m.cancel_all(None)
        self.assertEqual(sent, [])


if __name__ == "__main__":
    unittest.main()
