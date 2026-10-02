"""G. DASHBOARD v2: served by the engine process, http://127.0.0.1:8765/?t=<token> (localhost only).

  create_app(get_state, controls, market_detail=None, token=None) -> FastAPI app
      get_state()            the engine's latest dashboard payload (Engine.dash_state(): a copy, never mutated after)
      controls.apply(action, args) -> short message; actions: pause, resume, pull_quotes, layer_state, layer_caps,
                             market, kappa, read_only. Every control acts through the engine's executor and risk gate
      market_detail(ex)      orders and fills of one market, fetched when a row is opened
  serve_in_thread(app)       runs it in a daemon thread: a dashboard crash never touches trading
  render(payload)            the static fallback (data/dashboard.html, no controls): the engine writes it <= every 60 s

  token     a random token printed at startup and stored in data/dashboard_url.txt; every route needs it
  stream    /events: one snapshot, then deltas (changed sections, changed market rows, new events) at most 2 a second
  confirm   destructive controls need {"confirm": true} (the page asks twice); read-only mode refuses every control
            except turning read-only off
"""
import asyncio
import json
import math
import secrets
import threading
import time
from urllib.parse import urlsplit

LOCALHOST = {"127.0.0.1", "localhost", "::1"}
SSE_PERIOD_S = 0.5              # at most 2 updates a second
SSE_KEEPALIVE_S = 15
ACTIONS = {"pause", "resume", "pull_quotes", "layer_state", "layer_caps", "market", "kappa", "read_only"}
DESTRUCTIVE = {"pull_quotes", "layer_state", "layer_caps", "market", "kappa"}


def _finite(x):
    if isinstance(x, float) and not math.isfinite(x):
        return None
    if isinstance(x, dict):
        return {k: _finite(v) for k, v in x.items()}
    if isinstance(x, (list, tuple)):
        return [_finite(v) for v in x]
    return x


def dumps(obj):
    return json.dumps(_finite(obj), separators=(",", ":"), default=str)


def diff(old, new):
    """What changed from old to new: whole sections, market rows by ex (plus removed ones), new events (by id)."""
    d = {}
    for k, v in new.items():
        if k == "markets":
            om = {r["ex"]: r for r in (old.get("markets") or [])}
            rows = [r for r in v if om.get(r["ex"]) != r]
            gone = sorted(set(om) - {r["ex"] for r in v})
            if rows or gone:
                d["markets"] = dict(rows=rows, gone=gone)
        elif k == "events":
            last = max((e["id"] for e in old.get("events") or []), default=0)
            new_ev = [e for e in v if e["id"] > last]
            if new_ev:
                d["events"] = new_ev
        elif old.get(k) != v:
            d[k] = v
    return d


def render(payload):
    """Static fallback page: the payload embedded, no stream, no controls."""
    return PAGE.replace("__LIVE__", "false").replace("__TOKEN__", "").replace("__DATA__", dumps(payload).replace("</", "<\\/"))


def _same_origin(headers):
    """Only the page this server served on localhost: another web page in the browser (CSRF) or a DNS name rebound
    to 127.0.0.1 can't reach the controls."""
    host, origin = headers.get("host", ""), headers.get("origin")
    return urlsplit("//" + host).hostname in LOCALHOST and (origin is None or urlsplit(origin).netloc == host)


def create_app(get_state, controls, market_detail=None, token=None):
    from fastapi import FastAPI, Request
    from fastapi.responses import HTMLResponse, JSONResponse, StreamingResponse
    from starlette.concurrency import run_in_threadpool

    token = token or secrets.token_urlsafe(16)
    app = FastAPI(title="SIG engine", docs_url=None, redoc_url=None, openapi_url=None)
    app.state.token, app.state.closing = token, threading.Event()
    lock, cache = threading.Lock(), dict(t=0.0, payload=None, text=None)

    def snapshot():
        """(payload dict, its JSON) built at most once per SSE period however many pages are open. Never raises."""
        with lock:
            if cache["payload"] is None or time.monotonic() - cache["t"] >= SSE_PERIOD_S * 0.8:
                try:
                    p = get_state() or {}
                    if not isinstance(p, dict):
                        raise TypeError(f"state is {type(p).__name__}, not a dict")
                except Exception as e:
                    p = dict(error=f"could not read the engine state: {e!r}")
                cache.update(t=time.monotonic(), payload=p, text=dumps(p))
            return cache["payload"], cache["text"]

    def ok(request):
        return secrets.compare_digest(request.query_params.get("t", ""), token) and _same_origin(request.headers)

    def denied():
        return JSONResponse(dict(ok=False, message="forbidden: open the URL in data/dashboard_url.txt"), status_code=403)

    @app.get("/", response_class=HTMLResponse)
    def index(request: Request):
        if not ok(request):
            return HTMLResponse("forbidden: open the URL in data/dashboard_url.txt", status_code=403)
        _, text = snapshot()
        page = PAGE.replace("__LIVE__", "true").replace("__TOKEN__", token).replace("__DATA__", text.replace("</", "<\\/"))
        return HTMLResponse(page, headers={"Cache-Control": "no-store"})

    @app.get("/events")
    async def events(request: Request, limit: int = 0):
        """A snapshot, then deltas at most 2 a second (keep-alive comments when idle). limit=N ends after N messages."""
        if not ok(request):
            return denied()

        async def stream():
            last, beat, n = None, time.monotonic(), 0
            yield "retry: 2000\n\n"
            while not app.state.closing.is_set():
                payload, text = await run_in_threadpool(snapshot)
                msg = None
                if last is None:
                    msg = f"event: snapshot\ndata: {text}\n\n"
                elif payload is not last:
                    d = diff(last, payload)
                    if d:
                        msg = f"event: delta\ndata: {dumps(d)}\n\n"
                if msg is None and time.monotonic() - beat >= SSE_KEEPALIVE_S:
                    msg = ": keep-alive\n\n"
                last = payload
                if msg:
                    yield msg
                    beat, n = time.monotonic(), n + 1
                    if limit and n >= limit:
                        return
                await asyncio.sleep(SSE_PERIOD_S)
        return StreamingResponse(stream(), media_type="text/event-stream", headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"})

    @app.get("/api/market/{ex}")
    async def market(request: Request, ex: str):
        if not ok(request):
            return denied()
        if market_detail is None:
            return JSONResponse(dict(orders=[], fills=[]))
        try:
            return JSONResponse(_finite(await run_in_threadpool(market_detail, ex)))
        except Exception as e:
            return JSONResponse(dict(ok=False, message=f"market {ex}: {e!r}"), status_code=500)

    @app.post("/api/control")
    async def control(request: Request):
        """{action, args, confirm} -> {ok, message}. 403 from another origin or without the token; 400 for an unknown
        action, a destructive one without confirm, or anything but read_only while in read-only mode."""
        if not ok(request):
            return denied()
        try:
            body = await request.json()
        except Exception:
            body = None
        if not isinstance(body, dict) or body.get("action") not in ACTIONS:
            return JSONResponse(dict(ok=False, message=f"unknown action; one of {sorted(ACTIONS)}"), status_code=400)
        action, args = body["action"], body.get("args") or {}
        if action in DESTRUCTIVE and body.get("confirm") is not True:
            return JSONResponse(dict(ok=False, message=f"{action}: needs confirm (the page asks twice)"), status_code=400)
        payload, _ = snapshot()
        ro = controls.read_only() if hasattr(controls, "read_only") else (payload.get("overview") or {}).get("read_only")
        if ro and action != "read_only":
            return JSONResponse(dict(ok=False, message="read-only mode: controls are off"), status_code=400)
        try:
            message = await run_in_threadpool(controls.apply, action, args)
        except (ValueError, KeyError) as e:
            return JSONResponse(dict(ok=False, message=f"{action}: {e}"), status_code=400)
        except Exception as e:
            return JSONResponse(dict(ok=False, message=f"{action} failed: {e!r}"), status_code=500)
        with lock:
            cache["payload"] = None                         # the next snapshot shows the change
        return JSONResponse(dict(ok=True, message=str(message)))

    return app


class DashboardServer:
    """uvicorn in a daemon thread. stop() ends open streams, then the server."""

    def __init__(self, app, host, port):
        self.app, self.host, self.port = app, host, port
        self.server, self.thread, self.error = None, None, None

    @property
    def started(self):
        return bool(self.server and self.server.started)

    @property
    def url(self):
        return f"http://{self.host}:{self.port}/?t={self.app.state.token}"

    def stop(self, timeout=5.0):
        self.app.state.closing.set()
        if self.server:
            self.server.should_exit = True
        if self.thread:
            self.thread.join(timeout)

    def __repr__(self):
        return f"dashboard {self.url} " + ("running" if self.started else f"not running ({self.error!r})")


def serve_in_thread(app, host="127.0.0.1", port=8765, wait_s=5.0):
    """Start the dashboard in a daemon thread, bound to localhost only. Never raises into the engine."""
    if host not in LOCALHOST:
        raise ValueError(f"the dashboard binds to localhost only, not {host}")
    import uvicorn
    h = DashboardServer(app, host, port)

    def run():
        try:
            h.server = uvicorn.Server(uvicorn.Config(app, host=host, port=port, log_level="warning", access_log=False,
                                                     lifespan="off"))
            h.server.install_signal_handlers = lambda: None
            h.server.run()
        except BaseException as e:                          # noqa: BLE001 - a dashboard failure stays in its thread
            h.error = e
    h.thread = threading.Thread(target=run, daemon=True, name="dashboard")
    h.thread.start()
    end = time.time() + wait_s
    while time.time() < end and not h.started and h.thread.is_alive():
        time.sleep(0.05)
    return h


PAGE = r"""<!doctype html>
<html lang="en"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>SIG engine</title>
<style>
:root{--bg:#f6f7f9;--card:#fff;--ink:#16181d;--mute:#6b7280;--line:#e5e7eb;--acc:#2563eb;--good:#15803d;--bad:#b91c1c;--warn:#b45309;--chip:#eef2ff}
@media (prefers-color-scheme:dark){:root:not([data-theme=light]){--bg:#0f1115;--card:#171a21;--ink:#e6e8ee;--mute:#9aa3b2;--line:#2a2f3a;--acc:#60a5fa;--good:#4ade80;--bad:#f87171;--warn:#fbbf24;--chip:#1e2633}}
:root[data-theme=dark]{--bg:#0f1115;--card:#171a21;--ink:#e6e8ee;--mute:#9aa3b2;--line:#2a2f3a;--acc:#60a5fa;--good:#4ade80;--bad:#f87171;--warn:#fbbf24;--chip:#1e2633}
*{box-sizing:border-box}body{margin:0;background:var(--bg);color:var(--ink);font:14px/1.4 -apple-system,BlinkMacSystemFont,"Segoe UI",Roboto,sans-serif}
header{position:sticky;top:0;z-index:5;background:var(--card);border-bottom:1px solid var(--line);padding:8px 16px;display:flex;flex-wrap:wrap;gap:8px;align-items:center}
header h1{font-size:16px;margin:0 12px 0 0}nav{display:flex;flex-wrap:wrap;gap:4px}
nav button,.btn{border:1px solid var(--line);background:var(--card);color:var(--ink);padding:5px 10px;border-radius:6px;cursor:pointer;font:inherit}
nav button.on{background:var(--acc);color:#fff;border-color:var(--acc)}.btn.danger{border-color:var(--bad);color:var(--bad)}.btn.arm{background:var(--bad);color:#fff}
.spacer{flex:1}.dot{display:inline-block;width:9px;height:9px;border-radius:50%;background:var(--mute);margin-right:6px}.dot.ok{background:var(--good)}.dot.bad{background:var(--bad)}
main{padding:16px;max-width:1400px;margin:0 auto}.grid{display:grid;grid-template-columns:repeat(auto-fit,minmax(220px,1fr));gap:12px}
.card{background:var(--card);border:1px solid var(--line);border-radius:10px;padding:12px 14px}.card h3{margin:0 0 6px;font-size:12px;color:var(--mute);font-weight:600;text-transform:uppercase;letter-spacing:.04em}
.big{font-size:34px;font-weight:700}.mid{font-size:20px;font-weight:600}.mute{color:var(--mute)}.good{color:var(--good)}.bad{color:var(--bad)}.warn{color:var(--warn)}
table{width:100%;border-collapse:collapse;background:var(--card);border:1px solid var(--line);border-radius:10px;overflow:hidden}
th,td{padding:6px 8px;border-bottom:1px solid var(--line);text-align:left;white-space:nowrap}th{font-size:12px;color:var(--mute);font-weight:600}
td.n,th.n{text-align:right;font-variant-numeric:tabular-nums}tr.click{cursor:pointer}tr.click:hover{background:var(--chip)}
.chip{display:inline-block;padding:1px 7px;border-radius:10px;background:var(--chip);font-size:12px;margin-right:3px}
.chip.full{background:var(--good);color:#fff}.chip.pilot{background:var(--warn);color:#fff}.chip.off{background:var(--line)}.chip.killed{background:var(--bad);color:#fff}
.wrap{overflow-x:auto}.row{display:flex;flex-wrap:wrap;gap:8px;align-items:center;margin:10px 0}
input,select{background:var(--card);color:var(--ink);border:1px solid var(--line);border-radius:6px;padding:4px 6px;font:inherit}input[type=number]{width:80px}
.msg{position:fixed;right:16px;bottom:16px;background:var(--card);border:1px solid var(--line);border-radius:8px;padding:8px 12px;max-width:420px;display:none}
.alert{border-left:4px solid var(--bad);padding:6px 10px;margin:6px 0;background:var(--card)}
.drawer{background:var(--card);border:1px solid var(--line);border-radius:10px;padding:12px;margin:12px 0}
svg text{fill:var(--mute);font-size:11px}.ro .ctl{display:none!important}
@media (max-width:640px){main{padding:8px}.big{font-size:26px}header{padding:8px}}
</style></head><body>
<header><h1>SIG engine</h1><nav id="tabs"></nav><span class="spacer"></span>
<span id="live" class="mute"><span class="dot"></span>…</span>
<span class="ctl"><button class="btn" id="pauseBtn"></button></span>
<span class="ctl"><button class="btn danger" data-confirm="1" id="pullBtn">Pause + pull engine quotes</button></span>
<span class="ctl">κ <input type="number" id="kv" min="0" max="3" step="0.5" placeholder="¢"> for <input type="number" id="km" min="1" max="1440" value="60"> min <button class="btn" data-confirm="1" id="kBtn">Set κ</button></span>
<button class="btn" id="roBtn"></button>
</header>
<main id="main"></main><div class="msg" id="msg"></div>
<script id="data" type="application/json">__DATA__</script>
<script>
const LIVE = __LIVE__, TOKEN = "__TOKEN__";
let S = JSON.parse(document.getElementById("data").textContent || "{}");
const ui = {tab: "overview", page: 0, q: "", layer: "", haspos: false, mis: "", ev: "", evq: "", open: null, detail: null, updated: Date.now(), connected: false};
const TABS = {overview: "Overview", strategies: "Strategies", markets: "Markets", pairs: "Pairs", positions: "Positions", leaders: "Leaders", events: "Events"};
const LAYERS = ["recycler", "mm", "ladder", "pairs", "sniper", "position", "leaders"];
const esc = s => String(s == null ? "" : s).replace(/[&<>"]/g, c => ({"&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;"}[c]));
const num = (x, d = 0) => x == null || isNaN(x) ? "–" : Number(x).toLocaleString("en-US", {minimumFractionDigits: d, maximumFractionDigits: d});
const sgn = (x, d = 0) => x == null || isNaN(x) ? "–" : (x > 0 ? "+" : "") + num(x, d);
const cls = x => x == null ? "" : x > 0 ? "good" : x < 0 ? "bad" : "";
const c2 = x => x == null ? "–" : sgn(x * 100, 1) + "¢";
const px = x => x == null ? "–" : Number(x).toFixed(3);
const ago = t => t == null ? "–" : (s => s < 90 ? s + " s" : s < 5400 ? Math.round(s / 60) + " min" : Math.round(s / 3600) + " h")(Math.max(0, Math.round(Date.now() / 1000 - t)));
const chip = (st, killed) => killed ? `<span class="chip killed">killed</span>` : `<span class="chip ${esc(st)}">${esc(st)}</span>`;

function editing() {
  const a = document.activeElement;
  return a && a.closest && a.closest("#main") && /^(INPUT|SELECT)$/.test(a.tagName) && !["q", "mis", "evq"].includes(a.id);
}
document.addEventListener("focusout", () => setTimeout(() => { if (ui.dirty && !editing()) { ui.dirty = false; draw(); } }, 0));

function apply(msg, kind) {
  if (kind === "snapshot") { S = msg; }
  else {
    for (const [k, v] of Object.entries(msg)) {
      if (k === "markets") {
        const by = new Map((S.markets || []).map(r => [r.ex, r]));
        for (const r of v.rows) by.set(r.ex, r);
        for (const ex of v.gone) by.delete(ex);
        S.markets = [...by.values()];
      } else if (k === "events") { S.events = [...v, ...(S.events || [])].slice(0, 300); }
      else S[k] = v;
    }
  }
  ui.updated = Date.now();
  if (editing()) { ui.dirty = true; return; }          // never redraw under a caps/state input being edited
  draw();
}

function views() {
  const o = S.overview || {}, h = S.health || {};
  return {
  overview() {
    const sig = h.sig || {}, rd = sig.read || {}, wr = sig.write || {}, ex = h.executor || {};
    const feeds = Object.entries(h.feeds || {}).map(([k, t]) => `<div>${esc(k)}: <b>${ago(t)}</b></div>`).join("");
    const alerts = (S.alerts || []).map(a => `<div class="alert">${esc(a)}</div>`).join("");
    return `${S.error ? `<div class="alert">${esc(S.error)}</div>` : ""}${alerts}
    <div class="grid">
      <div class="card" style="grid-column:span 2"><h3>Account @ PM fair (the score basis)</h3><div class="big">${num(o.account_fair)}</div>
        <div>since 00:00 <b class="${cls(o.change_today)}">${sgn(o.change_today)}</b> · since night start <b class="${cls(o.change_start)}">${sgn(o.change_start)}</b> · since 100k start <b class="${cls(o.account_fair - 100000)}">${sgn(o.account_fair - 100000)}</b></div></div>
      <div class="card"><h3>Realized today</h3><div class="mid ${cls(o.realized_today)}">${sgn(o.realized_today)}</div></div>
      <div class="card"><h3>Free cash vs reserve</h3><div class="mid">${num(o.free_cash)}</div><div class="mute">reserve ${num(o.reserve)} · committed entries ${num(o.committed)}</div></div>
      <div class="card"><h3>κ (capital discount)</h3><div class="mid">${o.kappa == null ? "–" : (o.kappa * 100).toFixed(1) + "¢"}</div><div class="mute">${esc(o.kappa_src || "")}</div></div>
      <div class="card"><h3>Loss stop (bot P&L at fair, 24 h)</h3><div class="mid ${o.loss_stop && o.loss_stop.tripped ? "bad" : ""}">${o.loss_stop ? (o.loss_stop.tripped ? "TRIPPED" : "ok") : "–"}</div><div class="mute">${o.loss_stop ? sgn(o.loss_stop.pnl) + " of −" + num(o.loss_stop.limit) : ""}</div></div>
      <div class="card"><h3>Leaderboard value</h3><div class="mid">${num(o.leaderboard)}</div><div class="mute">SIG marks, not the score</div></div>
      <div class="card"><h3>New risk</h3><div class="mid ${o.paused ? "bad" : "good"}">${o.paused ? "PAUSED" : "on"}</div><div class="mute">exits, rebounds, unwinds, repairs always run</div></div>
    </div>
    <h3 style="margin:18px 0 8px">Health</h3>
    <div class="grid">
      <div class="card"><h3>SIG reads</h3><div>p50 <b>${num(rd.p50, 2)} s</b> · p90 <b>${num(rd.p90, 2)} s</b></div><div>5xx/timeouts <b class="${rd.bad_rate > .1 ? "bad" : ""}">${num((rd.bad_rate || 0) * 100, 1)}%</b></div><div>used <b>${num(sig.reads_last_min)}</b> / ${num(sig.read_limit)} per min</div></div>
      <div class="card"><h3>SIG writes</h3><div>p50 <b>${num(wr.p50, 2)} s</b> · p90 <b>${num(wr.p90, 2)} s</b></div><div>5xx/timeouts <b class="${wr.bad_rate > .1 ? "bad" : ""}">${num((wr.bad_rate || 0) * 100, 1)}%</b></div><div>used <b>${num(sig.writes_last_min)}</b> / ${num(sig.write_limit)} per min</div></div>
      <div class="card"><h3>Executor</h3><div>queue <b>${esc(JSON.stringify(ex.queue || {}))}</b></div><div>in flight <b>${num(ex.in_flight)}</b> · capacity <b>${num(ex.capacity_ewma, 1)}</b>/min · batch ${num(ex.batch_cap)}</div><div>journal UNKNOWN <b class="${ex.unknown ? "bad" : ""}">${num(ex.unknown)}</b> · stale dropped ${num(ex.dropped_stale)}</div></div>
      <div class="card"><h3>Feeds (age)</h3>${feeds}<div>loop <b>${num(h.loop_s, 1)} s</b> · errors last hour <b class="${h.errors ? "bad" : ""}">${num(h.errors)}</b></div></div>
    </div>`;
  },
  strategies() {
    const rows = (S.layers || []).map(l => {
      const caps = Object.entries(l.caps || {}).map(([k, c]) => `<div>${esc(k)}: <b>${num(c.used)}</b> / ${num(c.cap)}
        <span class="ctl"><input type="number" data-cap="${esc(l.name)}:${esc(k)}" min="${c.min}" max="${c.max}" value="${c.cap}"></span></div>`).join("") || "–";
      const kills = (l.kill || []).map(k => `<div class="${k.hit ? "bad" : "mute"}">${esc(k.rule)}: ${esc(k.value)}</div>`).join("") || "–";
      return `<tr><td><b>${esc(l.name)}</b><div class="mute">${esc(l.note || "")}</div></td><td>${chip(l.state, l.killed)}<div class="mute">${l.killed ? esc(l.killed.reason) : "since " + ago(l.since)}</div>
        <div class="ctl"><select data-state="${esc(l.name)}">${["off", "pilot", "full"].map(s => `<option ${s === l.state ? "selected" : ""}>${s}</option>`).join("")}</select>
        <button class="btn" data-confirm="1" data-act="state" data-layer="${esc(l.name)}">Apply</button></div></td>
        <td>${caps}${Object.keys(l.caps || {}).length ? `<div class="ctl"><button class="btn" data-confirm="1" data-act="caps" data-layer="${esc(l.name)}">Set caps</button></div>` : ""}</td>
        <td class="n">${num(l.resting)}</td><td class="n">${num(l.fills)}</td><td class="n">${c2(l.edge)}</td><td class="n">${c2(l.markout30)}</td>
        <td class="n ${cls(l.pnl_fair)}">${sgn(l.pnl_fair)}</td><td class="n ${cls(l.realized)}">${sgn(l.realized)}</td><td>${kills}</td></tr>`;
    }).join("");
    return `<div class="wrap"><table><tr><th>Layer</th><th>Flag</th><th>Caps (used / cap)</th><th class="n">Resting</th><th class="n">Fills</th><th class="n">Edge vs fair</th><th class="n">Mark-out 30m</th><th class="n">P&L @ fair</th><th class="n">Realized</th><th>Kill rules</th></tr>${rows}</table></div>`;
  },
  markets() {
    let rows = S.markets || [];
    const q = ui.q.toLowerCase(), mis = parseFloat(ui.mis);
    rows = rows.filter(r => (!q || (r.title + " " + r.race + " " + r.ex).toLowerCase().includes(q)) && (!ui.haspos || r.pos)
      && (!ui.layer || (r.resting || {})[ui.layer]) && (isNaN(mis) || (r.mis != null && Math.abs(r.mis) * 100 >= mis)));
    rows.sort((a, b) => Math.abs(b.mis || 0) - Math.abs(a.mis || 0));
    const per = 50, pages = Math.max(1, Math.ceil(rows.length / per)); ui.page = Math.min(ui.page, pages - 1);
    const body = rows.slice(ui.page * per, ui.page * per + per).map(r => `<tr class="click" data-ex="${esc(r.ex)}"><td>${esc(r.race)} <b>${esc(r.party)}</b>${r.paused ? ' <span class="chip killed">paused</span>' : ""}</td>
      <td class="n">${px(r.fair)}${r.u != null ? ` <span class="mute">±${(r.u * 100).toFixed(1)}</span>` : ""}</td><td class="n">${px(r.bid)} / ${px(r.ask)}</td><td class="n ${cls(r.mis && -r.mis)}">${c2(r.mis)}</td>
      <td class="n">${r.pos ? sgn(r.pos) : "–"}</td><td>${Object.entries(r.subs || {}).map(([k, v]) => `<span class="chip">${esc(k)} ${sgn(v)}</span>`).join("")}</td>
      <td>${Object.entries(r.resting || {}).map(([k, v]) => `<span class="chip">${esc(k)} ${v}</span>`).join("")}</td><td class="mute">${esc(r.last_fill || "")}</td></tr>
      ${ui.open === r.ex ? `<tr><td colspan="8">${detail(r)}</td></tr>` : ""}`).join("");
    return `<div class="row"><input id="q" placeholder="search market" value="${esc(ui.q)}"> <select id="lf"><option value="">any layer</option>${LAYERS.map(l => `<option ${l === ui.layer ? "selected" : ""}>${l}</option>`).join("")}</select>
      <label><input type="checkbox" id="hp" ${ui.haspos ? "checked" : ""}> has position</label> mispriced > <input type="number" id="mis" value="${esc(ui.mis)}" step="0.5">¢
      <span class="mute">${rows.length} markets</span><span class="spacer"></span><button class="btn" id="prev">‹</button> ${ui.page + 1}/${pages} <button class="btn" id="next">›</button></div>
      <div class="wrap"><table><tr><th>Market</th><th class="n">PM fair ±u¢</th><th class="n">SIG bid / ask</th><th class="n">SIG mid − fair</th><th class="n">Net YES</th><th>Sub-books</th><th>Our resting</th><th>Last fill</th></tr>${body}</table></div>`;
  },
  pairs() {
    const p = S.pairs || {}, rows = (p.sets || []).map(s => `<tr><td>${esc(s.race)}</td><td>${esc(s.side)}</td><td class="n">${num(s.qty)}</td><td class="n good">${sgn(s.locked, 2)}</td>
      <td class="n">${num(s.unwind_value, 2)}</td><td class="n">${num(s.threshold, 3)}</td><td class="n ${s.residue ? "warn" : ""}">${num(s.residue)}</td><td class="n">${num(s.capital)}</td><td>${esc(s.state || "")}</td></tr>`).join("");
    return `<div class="row">capital used <b>${num(p.used)}</b> / cap ${num(p.cap)} · locked total <b class="good">${sgn(p.locked, 2)}</b></div>
      <div class="wrap"><table><tr><th>Race</th><th>Set</th><th class="n">Sets</th><th class="n">Locked</th><th class="n">Unwind value</th><th class="n">Unwind at</th><th class="n">One-leg residue</th><th class="n">Capital</th><th>State</th></tr>${rows || '<tr><td colspan="9" class="mute">no open sets</td></tr>'}</table></div>`;
  },
  positions() {
    return (S.positions || []).map(p => `<div class="card" style="margin-bottom:12px"><h3>${esc(p.name)} · ${esc(p.market || "")}</h3>
      <div>holding <b>${esc((p.side || "").toUpperCase())} ${num(p.held)}</b> · ladder sub-book ${num(p.manual)} ${p.tranche ? `· D1 tranche <b>${num(p.tranche.qty)}</b> (${esc(p.tranche.state)})` : ""} · status ${esc(p.status || "")}</div>
      <div class="wrap"><table><tr><th class="n">Price</th><th class="n">YES-equivalent</th><th class="n">Qty</th><th>State</th></tr>${(p.ladder || []).map(l => `<tr><td class="n">${px(l.px)}</td><td class="n">${px(l.yes_px)}</td><td class="n">${num(l.qty)}</td><td>${esc(l.state)}</td></tr>`).join("")}</table></div></div>`).join("") || '<div class="mute">no position bots</div>';
  },
  leaders() {
    const L = S.leaders || {}, series = L.series || [];
    if (!series.length) return '<div class="mute">no leaderboard samples yet (logged every 10 min to data/leaders.csv)</div>';
    const W = 900, H = 320, xs = series.flatMap(s => s.points.map(p => p[0])), ys = series.flatMap(s => s.points.map(p => p[1]));
    const x0 = Math.min(...xs), x1 = Math.max(...xs, x0 + 1), y0 = Math.min(...ys), y1 = Math.max(...ys, y0 + 1);
    const X = x => 50 + (x - x0) / (x1 - x0) * (W - 60), Y = y => H - 20 - (y - y0) / (y1 - y0) * (H - 40);
    const lines = series.map(s => `<polyline fill="none" stroke="${s.ours ? "var(--bad)" : "var(--acc)"}" stroke-width="${s.ours ? 3 : 1}" opacity="${s.ours ? 1 : .45}" points="${s.points.map(p => X(p[0]) + "," + Y(p[1])).join(" ")}"><title>${esc(s.name)}</title></polyline>`).join("");
    const rows = (L.latest || []).map(r => `<tr${r.ours ? ' style="font-weight:700"' : ""}><td class="n">${num(r.rank)}</td><td>${esc(r.name)}</td><td class="n">${num(r.value)}</td><td class="n">${num(r.trades)}</td></tr>`).join("");
    return `<div class="card"><svg viewBox="0 0 ${W} ${H}" width="100%"><text x="4" y="14">${num(y1)}</text><text x="4" y="${H - 22}">${num(y0)}</text>${lines}</svg><div class="mute">top 20 (blue) and our account (red), SIG leaderboard values</div></div>
      <div class="wrap" style="margin-top:12px"><table><tr><th class="n">Rank</th><th>Account</th><th class="n">Value</th><th class="n">Trades</th></tr>${rows}</table></div>`;
  },
  events() {
    const kinds = [...new Set((S.events || []).map(e => e.kind))].sort();
    const q = ui.evq.toLowerCase(), rows = (S.events || []).filter(e => (!ui.ev || e.kind === ui.ev) && (!q || e.text.toLowerCase().includes(q)));
    return `<div class="row"><select id="evk"><option value="">all kinds</option>${kinds.map(k => `<option ${k === ui.ev ? "selected" : ""}>${esc(k)}</option>`).join("")}</select> <input id="evq" placeholder="filter" value="${esc(ui.evq)}"></div>
      <div class="wrap"><table><tr><th>Time</th><th>Kind</th><th>Event</th></tr>${rows.map(e => `<tr><td class="mute">${esc(e.t)}</td><td><span class="chip">${esc(e.kind)}</span></td><td style="white-space:normal">${esc(e.text)}</td></tr>`).join("")}</table></div>`;
  }};
}

function detail(r) {
  const d = ui.detail && ui.detail.ex === r.ex ? ui.detail : null;
  const ctl = `<div class="row ctl"><button class="btn" data-confirm="1" data-act="mpause" data-ex="${esc(r.ex)}">${r.paused ? "Resume market" : "Pause market"}</button>
    exclude from: ${["recycler", "mm", "ladder", "pairs", "sniper"].map(l => `<label><input type="checkbox" data-excl="${l}" ${(r.exclude || []).includes(l) ? "checked" : ""}> ${l}</label>`).join(" ")}
    <button class="btn" data-confirm="1" data-act="mexcl" data-ex="${esc(r.ex)}">Apply exclusions</button></div>`;
  if (!d) return `<div class="drawer">${ctl}<div class="mute">loading history…</div></div>`;
  const ord = (d.orders || []).map(o => `<tr><td class="mute">${esc(o.placed)}</td><td>${esc(o.bot)}</td><td>${esc(o.layer)}</td><td>${esc(o.action)} ${esc(o.side)}</td><td class="n">${px(o.price)}</td><td class="n">${num(o.qty)}</td><td class="n">${num(o.filled)}</td><td>${esc(o.status)}</td></tr>`).join("");
  const fl = (d.fills || []).map(f => `<tr><td class="mute">${esc(f.t)}</td><td>${esc(f.bot)}</td><td>${esc(f.layer)}</td><td>${esc(f.action)} ${esc(f.side)}</td><td class="n">${px(f.price)}</td><td class="n">${num(f.qty)}</td><td class="n">${c2(f.edge)}</td><td class="n">${c2(f.mo30)}</td></tr>`).join("");
  return `<div class="drawer">${ctl}<h3>Orders</h3><div class="wrap"><table><tr><th>Placed</th><th>Bot</th><th>Layer</th><th>Order</th><th class="n">Price</th><th class="n">Qty</th><th class="n">Filled</th><th>Status</th></tr>${ord || '<tr><td colspan="8" class="mute">none</td></tr>'}</table></div>
    <h3>Fills</h3><div class="wrap"><table><tr><th>Time</th><th>Bot</th><th>Layer</th><th>Trade</th><th class="n">Price</th><th class="n">Qty</th><th class="n">Edge vs fair</th><th class="n">Mark-out 30m</th></tr>${fl || '<tr><td colspan="8" class="mute">none</td></tr>'}</table></div></div>`;
}

function draw() {
  const o = S.overview || {};
  document.body.classList.toggle("ro", !LIVE || !!o.read_only);
  document.getElementById("tabs").innerHTML = Object.entries(TABS).map(([k, v]) => `<button data-tab="${k}" class="${k === ui.tab ? "on" : ""}">${v}</button>`).join("");
  document.getElementById("pauseBtn").textContent = o.paused ? "Resume new risk" : "Pause new risk";
  document.getElementById("roBtn").textContent = !LIVE ? "static snapshot" : o.read_only ? "Unlock controls" : "Read-only";
  const age = Math.round((Date.now() - ui.updated) / 1000);
  document.getElementById("live").innerHTML = `<span class="dot ${LIVE && ui.connected ? "ok" : LIVE ? "bad" : ""}"></span>${LIVE ? (ui.connected ? "live" : "reconnecting") : "snapshot " + esc(S.t || "")} · ${esc(S.t || "")} · loop ${num(S.loop)}`;
  const active = document.activeElement && document.activeElement.id;
  document.getElementById("main").innerHTML = views()[ui.tab]();
  if (active && document.getElementById(active)) { const el = document.getElementById(active); el.focus(); if (el.setSelectionRange && el.type !== "number") el.setSelectionRange(el.value.length, el.value.length); }
}

function say(t, bad) { const m = document.getElementById("msg"); m.textContent = t; m.style.display = "block"; m.className = "msg " + (bad ? "bad" : ""); clearTimeout(say.t); say.t = setTimeout(() => m.style.display = "none", 6000); }

async function send(action, args, confirm) {
  if (!LIVE) return;
  try {
    const r = await fetch("/api/control?t=" + encodeURIComponent(TOKEN), {method: "POST", headers: {"Content-Type": "application/json"}, body: JSON.stringify({action, args, confirm: !!confirm})});
    const j = await r.json(); say(j.message, !j.ok);
  } catch (e) { say("control failed: " + e, true); }
}

function armed(btn) {
  if (!btn.dataset.confirm) return true;
  if (btn.classList.contains("arm")) { btn.classList.remove("arm"); btn.textContent = btn.dataset.label; return true; }
  btn.dataset.label = btn.textContent; btn.classList.add("arm"); btn.textContent = "Click again to confirm";
  setTimeout(() => { if (btn.classList.contains("arm")) { btn.classList.remove("arm"); btn.textContent = btn.dataset.label; } }, 4000);
  return false;
}

async function openMarket(ex) {
  ui.open = ui.open === ex ? null : ex; ui.detail = null; draw();
  if (!ui.open || !LIVE) return;
  try { const r = await fetch(`/api/market/${encodeURIComponent(ex)}?t=${encodeURIComponent(TOKEN)}`); ui.detail = Object.assign(await r.json(), {ex}); draw(); } catch (e) { say("history: " + e, true); }
}

document.addEventListener("click", e => {
  const b = e.target.closest("button, tr.click"); if (!b) return;
  if (b.dataset.tab) { ui.tab = b.dataset.tab; ui.open = null; draw(); return; }
  if (b.matches("tr.click")) { if (!e.target.closest(".drawer")) openMarket(b.dataset.ex); return; }
  if (b.id === "prev") { ui.page = Math.max(0, ui.page - 1); draw(); return; }
  if (b.id === "next") { ui.page++; draw(); return; }
  if (!armed(b)) return;
  const o = S.overview || {};
  if (b.id === "pauseBtn") send(o.paused ? "resume" : "pause", {});
  else if (b.id === "pullBtn") send("pull_quotes", {}, true);
  else if (b.id === "kBtn") { const v = document.getElementById("kv").value; send("kappa", {value: v === "" ? null : parseFloat(v) / 100, minutes: parseFloat(document.getElementById("km").value || "60")}, true); }
  else if (b.id === "roBtn") send("read_only", {on: !o.read_only});
  else if (b.dataset.act === "state") send("layer_state", {layer: b.dataset.layer, state: document.querySelector(`select[data-state="${b.dataset.layer}"]`).value}, true);
  else if (b.dataset.act === "caps") { const caps = {}; document.querySelectorAll(`input[data-cap^="${b.dataset.layer}:"]`).forEach(i => caps[i.dataset.cap.split(":")[1]] = parseFloat(i.value)); send("layer_caps", {layer: b.dataset.layer, caps}, true); }
  else if (b.dataset.act === "mpause") { const r = (S.markets || []).find(m => m.ex === b.dataset.ex) || {}; send("market", {ex: b.dataset.ex, paused: !r.paused}, true); }
  else if (b.dataset.act === "mexcl") { const ex = []; b.closest(".drawer").querySelectorAll("input[data-excl]").forEach(i => { if (i.checked) ex.push(i.dataset.excl); }); send("market", {ex: b.dataset.ex, exclude: ex}, true); }
});
document.addEventListener("input", e => {
  const id = e.target.id;
  if (id === "q") { ui.q = e.target.value; ui.page = 0; draw(); }
  else if (id === "mis") { ui.mis = e.target.value; ui.page = 0; draw(); }
  else if (id === "evq") { ui.evq = e.target.value; draw(); }
});
document.addEventListener("change", e => {
  const id = e.target.id;
  if (id === "lf") { ui.layer = e.target.value; ui.page = 0; draw(); }
  else if (id === "hp") { ui.haspos = e.target.checked; ui.page = 0; draw(); }
  else if (id === "evk") { ui.ev = e.target.value; draw(); }
});

draw();
if (LIVE) {
  const es = new EventSource("/events?t=" + encodeURIComponent(TOKEN));
  es.addEventListener("snapshot", m => { ui.connected = true; apply(JSON.parse(m.data), "snapshot"); });
  es.addEventListener("delta", m => { ui.connected = true; apply(JSON.parse(m.data), "delta"); });
  es.onerror = () => { ui.connected = false; draw(); };
  setInterval(() => { if (Date.now() - ui.updated > 20000) { ui.connected = false; } }, 5000);
}
</script></body></html>
"""
