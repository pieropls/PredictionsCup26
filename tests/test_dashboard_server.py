"""Dashboard v2 (g_dashboard.py): token, localhost, SSE snapshot + deltas, controls (confirm, read-only), crash isolation.
python3 -m unittest tests.test_dashboard_server"""
import json
import os
import shutil
import socket
import subprocess
import sys
import tempfile
import time
import unittest
import urllib.request

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)
import g_dashboard as dashboard  # noqa: E402

try:
    from fastapi.testclient import TestClient
except ImportError:                                         # pragma: no cover
    TestClient = None
NEEDS = unittest.skipUnless(TestClient, "pip3 install --user fastapi uvicorn httpx")
TOKEN = "test-token"


def payload(n=3, loop=1, read_only=False):
    return dict(t="2026-10-02 01:00:00", loop=loop, live=True,
                overview=dict(account_fair=95000 + loop, free_cash=10000, paused=False, read_only=read_only, kappa=0.01, kappa_src="x",
                              reserve=5000, committed=0, realized_today=0, leaderboard=92000, loss_stop=dict(pnl=0, limit=2000, tripped=False)),
                health=dict(sig={}, executor={}, feeds={}), layers=[], positions=[], pairs={}, leaders={}, alerts=[],
                markets=[dict(ex=str(100 + i), title=f"m{i}", race=f"r{i}", party="D", fair=0.5, bid=0.49, ask=0.51) for i in range(n)],
                events=[dict(id=i, t="01:00:00", kind="X", text=f"e{i}") for i in range(loop, 0, -1)])


class FakeControls:
    def __init__(self):
        self.calls = []

    def apply(self, action, args):
        self.calls.append((action, args))
        if action == "boom":
            raise RuntimeError("boom")
        return f"{action}: ok"


def client(get_state, controls=None, host="127.0.0.1"):
    app = dashboard.create_app(get_state, controls or FakeControls(), market_detail=lambda ex: dict(orders=[], fills=[ex]), token=TOKEN)
    return TestClient(app, base_url=f"http://{host}:8765"), app


def sse(c, limit):
    out, ev = [], None
    with c.stream("GET", f"/events?t={TOKEN}&limit={limit}") as r:
        for line in r.iter_lines():
            if line.startswith("event: "):
                ev = line[7:]
            elif line.startswith("data: "):
                out.append((ev, json.loads(line[6:])))
    return out


@NEEDS
class ServerTests(unittest.TestCase):
    def test_every_route_needs_the_token(self):
        c, _ = client(lambda: payload())
        for path in ["/", "/events", "/api/market/100"]:
            self.assertEqual(c.get(path).status_code, 403, path)
            self.assertEqual(c.get(path + "?t=wrong").status_code, 403, path)
        self.assertEqual(c.post("/api/control", json=dict(action="pause")).status_code, 403)
        r = c.get(f"/?t={TOKEN}")
        self.assertEqual(r.status_code, 200)
        self.assertIn('"account_fair":95001', r.text)
        self.assertEqual(c.get(f"/api/market/101?t={TOKEN}").json(), dict(orders=[], fills=["101"]))

    def test_other_hosts_and_origins_are_refused(self):
        c, _ = client(lambda: payload(), host="evil.example")
        self.assertEqual(c.get(f"/?t={TOKEN}").status_code, 403)
        c, _ = client(lambda: payload())
        r = c.post(f"/api/control?t={TOKEN}", json=dict(action="pause"), headers={"Origin": "http://evil.example"})
        self.assertEqual(r.status_code, 403)

    def test_stream_sends_a_snapshot_then_only_what_changed(self):
        states = iter([payload(loop=1), payload(loop=1), payload(loop=2)] + [payload(loop=2)] * 50)
        c, _ = client(lambda: next(states))
        dashboard.SSE_PERIOD_S, old = 0.01, dashboard.SSE_PERIOD_S
        try:
            msgs = sse(c, 2)
        finally:
            dashboard.SSE_PERIOD_S = old
        self.assertEqual(msgs[0][0], "snapshot")
        self.assertEqual(msgs[1][0], "delta")
        d = msgs[1][1]
        self.assertEqual(set(d), {"loop", "overview", "events"}, "markets unchanged: not sent")
        self.assertEqual([e["id"] for e in d["events"]], [2], "only the new event")

    def test_diff_market_rows_and_removals(self):
        a, b = payload(n=3), payload(n=3)
        b["markets"][1] = dict(b["markets"][1], bid=0.40)
        b["markets"] = b["markets"][:2] + [dict(ex="999", title="new")]
        d = dashboard.diff(a, b)
        self.assertEqual([r["ex"] for r in d["markets"]["rows"]], ["101", "999"])
        self.assertEqual(d["markets"]["gone"], ["102"])

    def test_snapshot_is_small_for_a_full_payload(self):
        p = payload(n=240, loop=300)
        for m in p["markets"]:
            m.update(subs=dict(inventory=-1234, manual=0), resting=dict(mm=2, ladder=4), last_fill="01:02 buy YES 100 @ 0.400", u=0.005, mis=0.01)
        self.assertLess(len(dashboard.dumps(p)), 300_000)

    def test_controls_need_confirm_and_respect_read_only(self):
        fc = FakeControls()
        c, _ = client(lambda: payload(), fc)
        post = lambda body: c.post(f"/api/control?t={TOKEN}", json=body)
        self.assertEqual(post(dict(action="nope")).status_code, 400)
        self.assertEqual(post(dict(action="pull_quotes")).status_code, 400, "destructive: needs confirm")
        self.assertEqual(post(dict(action="pull_quotes", confirm="yes")).status_code, 400)
        self.assertEqual(fc.calls, [])
        r = post(dict(action="pull_quotes", confirm=True))
        self.assertEqual(r.json(), dict(ok=True, message="pull_quotes: ok"))
        self.assertEqual(post(dict(action="pause")).status_code, 200, "pause needs no confirm")
        self.assertEqual([a for a, _ in fc.calls], ["pull_quotes", "pause"])
        c2, _ = client(lambda: payload(read_only=True), fc)
        self.assertEqual(c2.post(f"/api/control?t={TOKEN}", json=dict(action="pause")).status_code, 400)
        self.assertEqual(c2.post(f"/api/control?t={TOKEN}", json=dict(action="read_only", args=dict(on=False))).status_code, 200)

    def test_failures_stay_inside_the_dashboard(self):
        def broken():
            raise RuntimeError("engine state exploded")
        c, _ = client(broken)
        r = c.get(f"/?t={TOKEN}")
        self.assertEqual(r.status_code, 200)
        self.assertIn("could not read the engine state", r.text)

        class Bad(FakeControls):
            def apply(self, action, args):
                raise RuntimeError("control exploded")
        c, _ = client(lambda: payload(), Bad())
        r = c.post(f"/api/control?t={TOKEN}", json=dict(action="pause"))
        self.assertEqual(r.status_code, 500)
        self.assertEqual(c.get(f"/?t={TOKEN}").status_code, 200, "still serving")

    def test_static_render_has_no_token_and_no_controls(self):
        html = dashboard.render(payload())
        self.assertIn("const LIVE = false", html)
        self.assertIn('TOKEN = ""', html)


def free_port():
    s = socket.socket()
    s.bind(("127.0.0.1", 0))
    p = s.getsockname()[1]
    s.close()
    return p


@NEEDS
class ThreadTests(unittest.TestCase):
    def test_binds_localhost_streams_and_stops(self):
        port = free_port()
        app = dashboard.create_app(lambda: payload(), FakeControls(), token=TOKEN)
        h = dashboard.serve_in_thread(app, port=port)
        try:
            self.assertTrue(h.started)
            t = time.time()
            body = urllib.request.urlopen(f"http://127.0.0.1:{port}/?t={TOKEN}", timeout=5).read()
            self.assertLess(time.time() - t, 2.0, "page loads in under 2 s")
            self.assertIn(b"SIG engine", body)
            self.assertEqual(h.url, f"http://127.0.0.1:{port}/?t={TOKEN}")
        finally:
            h.stop()
        self.assertFalse(h.thread.is_alive())

    def test_refuses_to_bind_beyond_localhost(self):
        with self.assertRaises(ValueError):
            dashboard.serve_in_thread(dashboard.create_app(lambda: {}, FakeControls()), host="0.0.0.0")


def render_views(test, html, extra=""):
    """Run the page's JavaScript in node with a minimal DOM and draw every view (plus filters and an opened market)."""
    if not shutil.which("node"):
        return
    import re
    js = re.findall(r"<script>(.*?)</script>", html, re.S)[0]
    data = re.findall(r'<script id="data" type="application/json">(.*?)</script>', html, re.S)[0]
    stub = """
const els = {}; const mk = id => els[id] || (els[id] = {id, textContent: id === "data" ? DATA : "", innerHTML: "", value: "", style: {},
  classList: {toggle(){}, add(){}, remove(){}, contains(){ return false; }}, focus(){}, querySelectorAll(){ return []; }, dataset: {}});
const document = {getElementById: mk, body: {classList: {toggle(){}}}, addEventListener(){}, activeElement: null, querySelector(){ return null; }, querySelectorAll(){ return []; }};
const EventSource = function(){ this.addEventListener = () => {}; }; const setInterval = () => 0; const fetch = async () => ({json: async () => ({})});
"""
    run = """
for (const t of Object.keys(TABS)) { ui.tab = t; draw(); const out = els.main.innerHTML; if (!out.length) throw new Error("empty " + t); }
ui.tab = "markets"; ui.q = "a"; ui.layer = "mm"; ui.haspos = true; ui.mis = "1"; draw(); ui.q = ""; ui.layer = ""; ui.haspos = false; ui.mis = "";
if ((S.markets || []).length) { ui.open = S.markets[0].ex; ui.detail = {ex: S.markets[0].ex, orders: [{placed: "x", bot: "b", price: 0.4, qty: 1}], fills: [{t: "x", price: 0.4, qty: 1}]}; draw(); }
apply({loop: 999, markets: {rows: [], gone: []}, events: [{id: 1e9, t: "x", kind: "K", text: "t"}]}, "delta"); ui.tab = "events"; draw();
if (!els.main.innerHTML.includes("K")) throw new Error("delta not applied");
""" + extra + "\nconsole.log('ok');"
    fd, p = tempfile.mkstemp(suffix=".js")
    with os.fdopen(fd, "w") as fh:
        fh.write(f"const DATA = {json.dumps(data)};\n" + stub + js + "\n" + run)
    r = subprocess.run(["node", p], capture_output=True, text=True)
    os.remove(p)
    test.assertEqual(r.stdout.strip(), "ok", r.stderr[-1500:])


class PageTests(unittest.TestCase):
    def test_every_view_renders(self):
        render_views(self, dashboard.render(payload(n=60, loop=5)))


if __name__ == "__main__":
    unittest.main()
