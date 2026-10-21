"""G. DASHBOARD v3: served by the engine process, http://127.0.0.1:8765/?t=<token> (localhost only).

  The page answers four questions, in order: how much do I have (account value at SIG marks, rank), how did I do today
  (P&L split realized / unrealized, daily bars), where is my money (capital buckets, holdings), what is the engine doing
  (strategies, fills as sentences, health). Payload: docs/dashboard_v3_contract.md (v2 keys + money, daily, series,
  capital, strategies, holdings, activity, leaders.thresholds); any v3 key may be missing or null.

  create_app(get_state, controls, market_detail=None, token=None) -> FastAPI app
      get_state()            the engine's latest dashboard payload (Engine.dash_state(): a copy, never mutated after)
      controls.apply(action, args) -> short message; actions: pause, resume, pull_quotes, layer_state, layer_caps,
                             market, kappa, read_only. Every control acts through the engine's executor and risk gate
      market_detail(ex)      orders and fills of one market, fetched when a row is opened
  serve_in_thread(app)       runs it in a daemon thread: a dashboard crash never touches trading
  render(payload)            the static fallback (data/dashboard.html, no controls): the engine writes it <= every 60 s

  token     a random token printed at startup and stored in data/dashboard_url.txt; every route needs it
  stream    /events: one snapshot, then deltas at most 2 a second: changed sections, changed market and holding rows by
            ex (plus removed ones), new events and activity by id
  confirm   destructive controls need {"confirm": true} (the page asks twice); read-only mode refuses every control
            except turning read-only off
"""
import asyncio
import json
import math
import os
import secrets
import threading
import time
from urllib.parse import urlsplit

LOCALHOST = {"127.0.0.1", "localhost", "::1"}
SSE_PERIOD_S = 0.5              # at most 2 updates a second
SSE_KEEPALIVE_S = 15
ACTIONS = {"pause", "resume", "pull_quotes", "layer_state", "layer_caps", "market", "kappa", "read_only", "hold_gates", "backfill", "truth_mode", "subbook_fix", "clear_decisions", "gate_free", "travel", "wall", "wall_cancel", "rollback_request", "fmm_restart", "election_mode", "election_stop", "election_rearm"}
DESTRUCTIVE = {"election_mode", "election_stop", "election_rearm", "rollback_request", "pull_quotes", "layer_state", "layer_caps", "market", "kappa", "hold_gates", "backfill", "truth_mode", "subbook_fix", "clear_decisions", "gate_free", "travel", "wall", "wall_cancel"}


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


ROWS = {"markets": "ex", "holdings": "ex", "holdings4": "ex", "v5mk_rows": "ex"}       # sections diffed row by row (changed rows + removed keys)
APPEND = {"events", "activity", "feed"}                   # newest-first lists: only entries with a new id are sent


def diff(old, new):
    """What changed from old to new: whole sections; market and holding rows by ex (plus removed ones); new events and
    activity entries (by id). A section that is not a list on either side is sent whole."""
    d = {}
    for k, v in new.items():
        ov = old.get(k)
        if k in ROWS and isinstance(v, list) and isinstance(ov, list):
            key = ROWS[k]
            om = {r.get(key): r for r in ov if isinstance(r, dict)}
            rows = [r for r in v if not isinstance(r, dict) or om.get(r.get(key)) != r]
            gone = sorted(set(om) - {r.get(key) for r in v if isinstance(r, dict)}, key=str)
            if rows or gone:
                d[k] = dict(rows=rows, gone=gone)
        elif k in APPEND and isinstance(v, list) and isinstance(ov, list):
            try:
                ids = [e.get("id") for e in ov if isinstance(e, dict) and e.get("id") is not None]
                last = max(ids) if ids else None
                new_ev = [e for e in v if isinstance(e, dict) and (last is None or (e.get("id") is not None and e["id"] > last))]
            except TypeError:                               # ids of mixed types: send what is not on the page yet
                seen = [e.get("id") for e in ov if isinstance(e, dict)]
                new_ev = [e for e in v if isinstance(e, dict) and e.get("id") not in seen]
            if new_ev:
                d[k] = new_ev
        elif ov != v:
            d[k] = v
    for k in old:
        if k not in new:
            d[k] = None                                     # a section the engine stopped sending: "not available"
    return d


def _asset(name):
    """assets/<name> (WebP despite the .png name) as a data URI, so file:// and the tunnel both show it (dashboard_v5 §2.1)."""
    import base64
    try:
        with open(os.path.join(os.path.dirname(os.path.abspath(__file__)), "assets", name), "rb") as fh:
            return "data:image/webp;base64," + base64.b64encode(fh.read()).decode()
    except OSError:
        return ""


PAGE5 = None


def page5():
    """The page with the v5 CSS / code and the two images in place (built once)."""
    global PAGE5
    if PAGE5 is None:
        import r_dash5_ui as ui5
        PAGE5 = (PAGE.replace("/*__CSS5__*/", ui5.CSS5).replace("//__JS5__", ui5.JS5)
                 .replace("__CUP__", _asset("cup.png")).replace("__SIGLOGO__", _asset("sig.png")))
    return PAGE5


def render(payload):
    """Static fallback page: the payload embedded, no stream, no controls; it reloads itself every 60 s (v5 §2.6)."""
    return (page5().replace("__REFRESH__", '<meta http-equiv="refresh" content="60">').replace("__LIVE__", "false").replace("__TOKEN__", "")
            .replace("__DATA__", dumps(payload).replace("</", "<\\/")))


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
        page = page5().replace("__REFRESH__", "").replace("__LIVE__", "true").replace("__TOKEN__", token).replace("__DATA__", text.replace("</", "<\\/"))
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
<html lang="en"><head><meta charset="utf-8"><link rel="icon" href="data:,"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>Piero's Intel</title>__REFRESH__<link rel="preconnect" href="https://fonts.googleapis.com"><link href="https://fonts.googleapis.com/css2?family=Archivo:wght@400;500;600;700&display=swap" rel="stylesheet">
<style>
:root{color-scheme:light;--bg:#f4f4f1;--card:#fff;--ink:#0b0b0b;--ink2:#52514e;--mute:#6b6a65;--line:#e4e3dc;--axis:#c3c2b7;--acc:#2a78d6;
--good:#006300;--bad:#b42323;--warn:#8a5300;--chip:#efeee9;--neutral:#aaa8a0;--warnbg:#fff5dd;--badbg:#fdeceb;--shadow:rgba(0,0,0,.14);
--s1:#2a78d6;--s2:#eb6834;--s3:#1baf7a;--s4:#eda100;--s5:#e87ba4;--s6:#008300;--s7:#4a3aa7;--s8:#e34948;--s9:#0e8fa3;
--st-good:#0ca30c;--st-warn:#fab219;--st-crit:#d03b3b}
@media (prefers-color-scheme:dark){:root:not([data-theme=light]){color-scheme:dark;--bg:#0d0d0d;--card:#1a1a19;--ink:#fff;--ink2:#c3c2b7;--mute:#9a988f;
--line:#2c2c2a;--axis:#4a4a45;--acc:#3987e5;--good:#3cc23c;--bad:#f07272;--warn:#f5b83d;--chip:#262624;--neutral:#62615b;--warnbg:#2a2210;--badbg:#2e1616;
--shadow:rgba(0,0,0,.5);--s1:#3987e5;--s2:#d95926;--s3:#199e70;--s4:#c98500;--s5:#d55181;--s6:#008300;--s7:#9085e9;--s8:#e66767;--s9:#2bb3c8}}
:root[data-theme=dark]{color-scheme:dark;--bg:#0d0d0d;--card:#1a1a19;--ink:#fff;--ink2:#c3c2b7;--mute:#9a988f;
--line:#2c2c2a;--axis:#4a4a45;--acc:#3987e5;--good:#3cc23c;--bad:#f07272;--warn:#f5b83d;--chip:#262624;--neutral:#62615b;--warnbg:#2a2210;--badbg:#2e1616;
--shadow:rgba(0,0,0,.5);--s1:#3987e5;--s2:#d95926;--s3:#199e70;--s4:#c98500;--s5:#d55181;--s6:#008300;--s7:#9085e9;--s8:#e66767;--s9:#2bb3c8}
*{box-sizing:border-box}html{-webkit-text-size-adjust:100%}
body{margin:0;background:var(--bg);color:var(--ink);font:14px/1.45 system-ui,-apple-system,BlinkMacSystemFont,"Segoe UI",Roboto,sans-serif}
header{position:sticky;top:0;z-index:5;background:var(--card);border-bottom:1px solid var(--line);padding:8px 16px;display:flex;flex-wrap:wrap;gap:8px;align-items:center}
header h1{font-size:16px;margin:0 10px 0 0}nav{display:flex;flex-wrap:wrap;gap:4px}
nav button,.btn{border:1px solid var(--line);background:var(--card);color:var(--ink);padding:5px 11px;border-radius:7px;cursor:pointer;font:inherit}
nav button.on{background:var(--acc);color:#fff;border-color:var(--acc)}.btn.danger{border-color:var(--bad);color:var(--bad)}.btn.arm{background:var(--bad);color:#fff}
.spacer{flex:1}.dot{display:inline-block;width:9px;height:9px;border-radius:50%;background:var(--mute);margin-right:6px}.dot.ok{background:var(--st-good)}.dot.bad{background:var(--st-crit)}
#live{font-size:12.5px;color:var(--ink2)}
main{padding:16px;max-width:1440px;margin:0 auto}
section{margin:6px 0 30px;scroll-margin-top:64px}
h2{font-size:19px;margin:0 0 10px;letter-spacing:-.01em}h2 .q{font-size:13px;color:var(--mute);font-weight:400;margin-left:8px}
h3{margin:0 0 6px;font-size:13px;color:var(--ink2);font-weight:600}
.jump{display:flex;flex-wrap:wrap;gap:6px;margin:0 0 16px}.jump a{color:var(--ink2);text-decoration:none;font-size:13px;padding:3px 10px;border:1px solid var(--line);border-radius:14px;background:var(--card)}
.jump a:hover{color:var(--ink);border-color:var(--axis)}
.grid,.cards{display:grid;grid-template-columns:repeat(auto-fit,minmax(240px,1fr));gap:12px}
.tiles{display:grid;grid-template-columns:repeat(auto-fit,minmax(165px,1fr));gap:10px}
.card{background:var(--card);border:1px solid var(--line);border-radius:12px;padding:14px 16px;min-width:0}
.card.soft{background:transparent}section>.card,section>details.card,.tiles+.card{margin-top:12px}
.tiles.t8,.tiles.t4{grid-template-columns:repeat(4,minmax(0,1fr))}.tile .sub{margin-top:6px}.tile .v2{margin-top:6px}
.lab{font-size:12.5px;color:var(--ink2);font-weight:600}.lab[title],.info{cursor:help}
.info{display:inline-block;width:15px;height:15px;line-height:14px;text-align:center;border-radius:50%;border:1px solid var(--axis);color:var(--mute);font-size:10px;font-weight:700;margin-left:4px;vertical-align:1px}
.hero .v{font-size:48px;font-weight:650;letter-spacing:-.02em;line-height:1.08;margin:4px 0 2px}
.v2{font-size:30px;font-weight:650;line-height:1.15;margin:4px 0 2px;letter-spacing:-.01em}.v3{font-size:20px;font-weight:600;margin:3px 0 1px}
.sub{color:var(--ink2);font-size:13px;margin-top:4px}.sub b:not(.good):not(.bad){color:var(--ink)}
.big{font-size:34px;font-weight:700}.mid{font-size:20px;font-weight:600}.mute{color:var(--mute)}.good{color:var(--good)}.bad{color:var(--bad)}.warn{color:var(--warn)}
.na{color:var(--mute);font-style:italic;padding:8px 0}
table{width:100%;border-collapse:collapse;background:var(--card)}
.wrap{overflow-x:auto;max-width:100%;border:1px solid var(--line);border-radius:10px;background:var(--card)}.wrap table{border:0}
th,td{padding:6px 9px;border-bottom:1px solid var(--line);text-align:left;white-space:nowrap}th{font-size:12px;color:var(--ink2);font-weight:600;vertical-align:bottom}
tr:last-child td{border-bottom:0}td.n,th.n{text-align:right;font-variant-numeric:tabular-nums}tr.click{cursor:pointer}tr.click:hover{background:var(--chip)}
td.txt{white-space:normal;min-width:260px}td.wide{white-space:normal;min-width:180px;max-width:340px}th.grp{text-align:center;border-bottom:1px solid var(--axis)}
th.sort{cursor:pointer;user-select:none}th.sort:hover{color:var(--ink)}th .ar{font-size:10px;margin-left:3px;color:var(--acc)}
tfoot td{font-weight:600;border-top:1px solid var(--axis)}
.chip{display:inline-block;padding:1px 8px;border-radius:10px;background:var(--chip);font-size:12px;margin-right:3px}
.chip.full{background:var(--st-good);color:#fff}.chip.pilot{background:var(--st-warn);color:#1d1400}.chip.off{background:var(--line);color:var(--ink2)}.chip.killed{background:var(--st-crit);color:#fff}
.chip.k-trade,.chip.st-live{background:var(--st-good);color:#fff}.chip.k-alert,.chip.st-paused{background:var(--st-crit);color:#fff}.chip.k-market,.chip.st-off{background:var(--line);color:var(--ink2)}
.chip.st-pilot{background:var(--st-warn);color:#1d1400}.chip.st-held{background:var(--chip);color:var(--ink2);border:1px solid var(--axis)}
#live b.good{color:var(--good)}#live b.bad{color:var(--bad)}#live b.warn{color:var(--st-warn)}.warn{color:var(--st-warn)}.dot.warn{background:var(--st-warn)}.grid2{display:grid;grid-template-columns:repeat(auto-fit,minmax(300px,1fr));gap:12px}
.ld{display:inline-block;width:9px;height:9px;border-radius:50%;margin-right:6px;flex:none}.tag{display:inline-flex;align-items:center;white-space:nowrap}
.badge{display:inline-block;padding:0 7px;border-radius:9px;font-size:11px;font-weight:600;border:1px solid var(--acc);color:var(--acc);margin-left:6px;vertical-align:1px}
.row{display:flex;flex-wrap:wrap;gap:8px;align-items:center;margin:10px 0}
input,select{background:var(--card);color:var(--ink);border:1px solid var(--line);border-radius:7px;padding:5px 7px;font:inherit;max-width:100%}input[type=number]{width:80px}
.msg{position:fixed;right:16px;bottom:16px;background:var(--card);border:1px solid var(--line);border-radius:8px;padding:8px 12px;max-width:420px;display:none;z-index:30}
.alert{border-left:4px solid var(--st-crit);padding:8px 12px;margin:0 0 10px;background:var(--badbg);border-radius:6px}.alert.w{border-left-color:var(--st-warn);background:var(--warnbg)}
.drawer{background:var(--card);border:1px solid var(--line);border-radius:10px;padding:12px;margin:8px 0;white-space:normal}
details>summary{cursor:pointer;color:var(--ink2);font-weight:600;padding:2px 0}details.more{margin-top:10px}details.more>summary{font-size:13px}
details.card>summary{font-size:14px}details[open]>summary{margin-bottom:10px}
.legend{display:flex;flex-wrap:wrap;gap:4px 16px;font-size:12.5px;color:var(--ink2);margin:0 0 8px}.legend span{display:inline-flex;align-items:center}
.sw{display:inline-block;width:10px;height:10px;border-radius:3px;margin-right:6px;flex:none}.tk{display:inline-block;width:14px;height:2px;background:var(--ink);margin-right:6px;border-radius:1px}
.lk{display:inline-block;width:16px;height:2px;margin-right:6px;border-radius:1px}
.chead{display:flex;flex-wrap:wrap;gap:8px;align-items:center;margin-bottom:6px}.chead h3{margin:0}
.seg{display:inline-flex;border:1px solid var(--line);border-radius:7px;overflow:hidden}.seg button{border:0;background:var(--card);color:var(--ink2);padding:4px 11px;font:inherit;font-size:12.5px;cursor:pointer}
.seg button.on{background:var(--acc);color:#fff}
.pbar{position:relative;height:14px;margin:12px 0 8px;background:var(--chip);border-radius:4px}.pbar i{position:absolute;top:0;bottom:0}
.pbar .z{position:absolute;top:-4px;bottom:-4px;width:1px;background:var(--axis)}.pbar .t{position:absolute;top:-5px;bottom:-5px;width:3px;margin-left:-1px;background:var(--ink);border-radius:2px}
.eq{font-size:13px;color:var(--ink2);line-height:1.6}.eq b:not(.good):not(.bad){color:var(--ink)}
.pscale{display:flex;justify-content:space-between;font-size:11px;color:var(--mute);margin:-4px 0 6px}
.more-btn{margin-top:8px}
.ok{color:var(--good);font-size:12.5px;margin-top:6px}.nok{color:var(--bad);font-size:12.5px;margin-top:6px;font-weight:600}
.sbar{display:flex;gap:2px;height:24px;border-radius:5px;overflow:hidden;margin:4px 0 10px;background:var(--card)}.sbar i{display:block;height:100%;min-width:3px}
.meter{height:6px;background:var(--chip);border-radius:3px;overflow:hidden;margin-top:7px}.meter i{display:block;height:100%;background:var(--acc)}
.chart svg{display:block;max-width:100%}
svg text{fill:var(--mute);font-size:11px;font-family:inherit}svg .lbl{fill:var(--ink2)}svg .vl{fill:var(--ink);font-weight:600;font-size:11.5px}
svg .gl{stroke:var(--line);stroke-width:1}svg .zl{stroke:var(--axis);stroke-width:1}svg .tot{stroke:var(--ink);stroke-width:2;stroke-linecap:round}
svg .ln{fill:none;stroke-width:2;stroke-linejoin:round;stroke-linecap:round}svg .ring{stroke:var(--card);stroke-width:2}svg .xh{stroke:var(--ink2);stroke-width:1}
#tip{position:fixed;z-index:20;pointer-events:none;background:var(--card);color:var(--ink);border:1px solid var(--line);border-radius:8px;padding:7px 10px;font-size:12.5px;
box-shadow:0 6px 18px var(--shadow);display:none;max-width:300px;line-height:1.5}#tip .sw{vertical-align:-1px}
.ro .ctl{display:none!important}
#overlay{position:fixed;inset:0;z-index:25;background:var(--bg);padding:12px 16px;overflow:auto}#overlay .ovl-head{display:flex;flex-wrap:wrap;gap:10px;align-items:center;margin-bottom:8px}#overlay h3{margin:0}
.zoom-btn{padding:1px 7px;font-size:13px;line-height:1.4}.chead .zoom-btn{margin-left:auto}
@media (max-width:760px){.tiles.t8,.tiles.t4{grid-template-columns:repeat(2,minmax(0,1fr))}}
@media (max-width:640px){main{padding:10px}.hero .v{font-size:38px}.v2{font-size:25px}header{padding:8px 10px}h2 .q{display:block;margin:2px 0 0}.card{padding:12px}}
/*__CSS5__*/
</style></head><body>
<header><span class="logo"><img alt="Susquehanna" src="__SIGLOGO__"></span><h1>Piero's Intel</h1><nav id="tabs"></nav><span class="spacer"></span><span id="live"><span class="dot"></span>…</span>
<button class="btn" id="themeBtn" title="Light / dark theme">Theme</button></header>
<main id="main"></main><div id="side5"></div><div id="modal5"></div><div id="overlay" style="display:none"></div><div class="msg" id="msg"></div><div id="tip"></div>
<script id="data" type="application/json">__DATA__</script>
<script>
const LIVE = __LIVE__, TOKEN = "__TOKEN__";
let S = {};
try { S = JSON.parse(document.getElementById("data").textContent || "{}") || {}; } catch (e) { S = {error: "could not read the page data: " + e}; }
const ui = {tab: "overview", shown: null, cache: {}, basis: "sig", hsort: {k: "value_sig", dir: -1}, hq: "", hl: "", hr: false, hall: false, al: "", aq: "", aall: false, feed: "", hf: "", sopen: null, zoomId: null, zoomWin: null, range: 0, dropped: 0,
  page: 0, q: "", layer: "", haspos: false, mis: "", ev: "", evq: "", open: null, detail: null, updated: Date.now(), connected: false, errors: [], theme: "auto"};
const TABS = {overview: "Overview", live: "Live", markets: "Markets", leaders: "Leaders", admin: "Admin"};   // dashboard_v5 (Capital / Holdings folded into Overview, Live, Markets)
const CUP_SRC = "__CUP__", SIG_SRC = "__SIGLOGO__";   // dashboard_v4.md
const LAYERS = ["recycler", "release", "mm", "scalp", "tm", "sm", "touch", "wall", "sleeve", "swing", "fm", "ladder", "pairs", "sniper", "fatfinger", "dump", "longshot", "position", "leaders"];   // controls
const ORDER = ["position", "mm", "scalp", "tm", "sm", "touch", "wall", "sleeve", "swing", "fm", "ladder", "pairs", "sniper", "fatfinger", "dump", "longshot", "recycler", "release"];  // colour slots, fixed per layer
const SLOT = {position: 1, manual: 1, mm: 2, scalp: 2, tm: 2, sm: 2, touch: 9, wall: 8, sleeve: 5, swing: 4, fm: 2, ladder: 3, pairs: 4, sniper: 5, fatfinger: 6, recycler: 7, inventory: 7, release: 8, dump: 9, longshot: 6};
const NAMES = {position: "Position exits (AK, RI)", manual: "AK / RI manual positions", scalp: "Scalper (v1, off)", tm: "Touch maker (scalper v2)", sm: "Spread maker (two-sided at the touch)", touch: "Touch (big sellers)", wall: "Wall (manual pilot)", sleeve: "Sleeve (settlement holds)", swing: "RI swing", fm: "Fair maker (competitive races)", recycler: "Recycler", inventory: "Older inventory (recycler)",
  release: "Capital release", mm: "Market maker", ladder: "Deep ladders", pairs: "Pair sets", sniper: "Sniper", fatfinger: "Fat-finger catcher", dump: "Dump catcher", longshot: "Longshot catcher",
  leaders: "Leaderboard logger", cash: "Cash", "old engine": "Old engine"};
const FILTERS = new Set(["hq", "b-hlopts", "hr", "b-alopts", "aq", "b-evkinds", "evq", "q", "lf", "hp", "mis"]);
const MON = ["Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"];
const CH = {};                                   // chart id -> geometry + series, for the hover crosshair
const HOLD_FIRST = 30, ACT_FIRST = 40;           // rows shown before "Show all"

// ------------------------------------------------------------------------------------------- formatting
const esc = s => String(s == null ? "" : s).replace(/[&<>"']/g, c => ({"&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#39;"}[c]));
const arr = x => Array.isArray(x) ? x : [];
const obj = x => x && typeof x === "object" && !Array.isArray(x) ? x : {};
const val = x => x == null || x === "" || typeof x === "boolean" || isNaN(x) ? null : Number(x);
const num = (x, d = 0) => (x = val(x)) == null ? "–" : (x < 0 && Math.abs(x) >= 0.5 * Math.pow(10, -d) ? "−" : "") + Math.abs(x).toLocaleString("en-US", {minimumFractionDigits: d, maximumFractionDigits: d});
const sgn = (x, d = 0) => (x = val(x)) == null ? "–" : (x > 0 && x >= 0.5 * Math.pow(10, -d) ? "+" : "") + num(x, d);
const cls = x => (x = val(x)) == null ? "" : x > 0 ? "good" : x < 0 ? "bad" : "";
const c2 = x => val(x) == null ? "–" : sgn(x * 100, 1) + "¢";
const px = x => val(x) == null ? "–" : Number(x).toFixed(3);
const pct = (x, d = 1) => val(x) == null ? "–" : num(x * 100, d) + "%";
const ago = t => val(t) == null ? "–" : (s => s < 90 ? s + " s" : s < 5400 ? Math.round(s / 60) + " min" : Math.round(s / 3600) + " h")(Math.max(0, Math.round(Date.now() / 1000 - t)));
const na = t => `<div class="na">${esc(t)}</div>`;
const info = t => `<span class="info" title="${esc(t)}">?</span>`;
const colorOf = k => SLOT[k] ? `var(--s${SLOT[k]})` : "var(--neutral)";
const nameOf = k => { const s = arr(S.strategies).find(r => r && r.key === k); return (s && s.label) || NAMES[k] || (k == null || k === "" ? "–" : String(k)); };
const tag = k => `<span class="tag"><i class="ld" style="background:${colorOf(k)}"></i>${esc(nameOf(k))}</span>`;
const dday = s => { const m = /^(\d{4})-(\d\d)-(\d\d)/.exec(String(s || "")); return m ? MON[+m[2] - 1] + " " + (+m[3]) : String(s || "–"); };
const STATE = {full: ["On", "Running at full size"], pilot: ["Pilot", "Running with small pilot caps"], off: ["Off", "Not trading"]};
const chip = (st, killed) => killed ? `<span class="chip killed" title="${esc(obj(killed).reason || "stopped by a kill rule")}">stopped</span>`
  : `<span class="chip ${esc(st)}" title="${esc((STATE[st] || [])[1] || "")}">${esc((STATE[st] || [st])[0])}</span>`;
let fmtT = null, fmtD = null;
try { fmtT = new Intl.DateTimeFormat("en-US", {timeZone: "America/Los_Angeles", hour: "2-digit", minute: "2-digit", hourCycle: "h23"});
      fmtD = new Intl.DateTimeFormat("en-US", {timeZone: "America/Los_Angeles", month: "short", day: "numeric"}); } catch (e) { }
const hm = t => fmtT ? fmtT.format(new Date(t * 1000)) : new Date(t * 1000).toISOString().slice(11, 16);
const md = t => fmtD ? fmtD.format(new Date(t * 1000)) : new Date(t * 1000).toISOString().slice(5, 10);
function when(t) {                               // "2026-10-02 13:02:11" -> "13:02:11" today, "Oct 1 13:02" another day
  if (typeof t === "number") return md(t) + " " + hm(t);
  const s = String(t || ""), today = String(S.t || "").slice(0, 10);
  return s.length < 19 ? s || "–" : s.slice(0, 10) === today ? s.slice(11, 19) : dday(s) + " " + s.slice(11, 16);
}
function tzOff(t) {                              // minutes PT is ahead of UTC at t (−420 in PDT)
  if (!fmtT) return 0;
  const [h, m] = fmtT.format(new Date(t * 1000)).split(":").map(Number), d = new Date(t * 1000);
  let o = (h * 60 + m) - (d.getUTCHours() * 60 + d.getUTCMinutes());
  if (o > 720) o -= 1440; if (o < -720) o += 1440; return o;
}

// ------------------------------------------------------------------------------------------- chart helpers
const f1 = v => Math.round(v * 10) / 10;
function ticks(lo, hi, n) {
  if (!(hi > lo)) { hi = lo + 1; }
  const raw = (hi - lo) / n, mag = Math.pow(10, Math.floor(Math.log10(raw))), f = raw / mag;
  const step = (f < 1.5 ? 1 : f < 3 ? 2 : f < 7 ? 5 : 10) * mag, a = Math.floor(lo / step + 1e-9) * step, b = Math.ceil(hi / step - 1e-9) * step, out = [];
  for (let v = a; v <= b + step / 2; v += step) out.push(Math.round(v / step) * step);
  return {lo: a, hi: b, step, ticks: out};
}
const kf = (v, step) => v === 0 ? "0" : Math.abs(step) >= 1000 ? num(v / 1000, step % 1000 ? 1 : 0) + "k" : num(v, step < 1 ? 2 : 0);
function pad(lo, hi, f) { const s = (hi - lo) || Math.abs(hi) || 1; return [lo < 0 ? lo - s * f : lo, hi > 0 ? hi + s * f : hi]; }
function barPath(x, w, y0, y1, round) {           // a bar from the baseline y0 to y1, 4px rounded at the far end only
  const h = Math.abs(y1 - y0); if (h < 0.4) return "";
  const r = round ? Math.min(4, h, w / 2) : 0, up = y1 < y0, s = up ? 1 : -1;
  x = f1(x); w = f1(w); y0 = f1(y0); y1 = f1(y1);
  return `M${x},${y0}V${f1(y1 + s * r)}Q${x},${y1} ${f1(x + r)},${y1}H${f1(x + w - r)}Q${f1(x + w)},${y1} ${f1(x + w)},${f1(y1 + s * r)}V${y0}Z`;
}
function width(id) { const el = document.getElementById("b-" + id), w = el && el.clientWidth; return Math.max(300, Math.min(1600, w || 900)); }
function linePath(pts, X, Y) {                  // pts [[t, v]], a gap (null) starts a new segment
  let d = "", pen = false;
  for (const [t, v] of pts) { if (v == null) { pen = false; continue; } d += (pen ? "L" : "M") + f1(X(t)) + "," + f1(Y(v)); pen = true; }
  return d;
}
function nearest(ts, t) { let lo = 0, hi = ts.length - 1; if (hi < 0) return -1; while (hi - lo > 1) { const m = (lo + hi) >> 1; if (ts[m] < t) lo = m; else hi = m; } return Math.abs(ts[lo] - t) <= Math.abs(ts[hi] - t) ? lo : hi; }
function timeTicks(t0, t1, w) {
  const span = Math.max(1, t1 - t0), steps = [1800, 3600, 7200, 10800, 21600, 43200, 86400, 172800, 604800];
  const step = steps.find(s => span / s <= Math.max(2, w / 85)) || 604800, off = tzOff(t0) * 60, out = [];
  for (let t = Math.ceil((t0 + off) / step) * step - off; t <= t1; t += step) out.push(t);
  return {step, ticks: out, label: t => { const day = ((t + off) % 86400 + 86400) % 86400 === 0; return step >= 86400 || day ? md(t) : hm(t); }};
}
const tipAttr = html => ` data-tip="${esc(html)}"`;
const legend = items => `<div class="legend">${items.map(([mark, text]) => `<span>${mark}${text}</span>`).join("")}</div>`;
const sw = c => `<i class="sw" style="background:${c}"></i>`;

// ------------------------------------------------------------------------------------------- Money
function money() {
  const m = obj(S.money), o = obj(S.overview), L = obj(S.leaders);
  const acct = val(m.account_sig) ?? val(o.leaderboard), start = val(m.start) ?? 100000;
  const vs = val(m.vs_start) ?? (acct == null ? null : acct - start);
  const ours = arr(L.latest).find(r => r && r.ours), rank = val(m.rank) ?? (ours ? val(ours.rank) : null), nt = val(m.n_traders);
  const to = obj(m.to_rank), dist = k => { const v = val(to[k]); return v == null ? "" : v > 0 ? `<b>${sgn(v)}</b> to reach #${k}` : `ahead of #${k} by <b>${num(-v)}</b>`; };
  const dists = ["100", "10"].map(dist).filter(Boolean).join(" · ");
  const hero = acct == null ? `<div class="card hero"><div class="lab">Account value (SIG)</div>${na("Not available yet.")}</div>` :
    `<div class="card hero"><div class="lab" title="totalAccountValue from SIG's /portfolio/pnl: the number the leaderboard ranks">Account value (SIG marks)${info("SIG's own valuation of everything we hold plus cash. This is the leaderboard number.")}</div>
     <div class="v">${num(acct)}</div>
     <div class="sub"><b class="${cls(vs)}">${sgn(vs)}</b> (${vs == null ? "–" : sgn(vs / start * 100, 1) + "%"}) vs the ${num(start)} start</div>
     <div class="sub">${rank == null ? "Rank not available yet" : `Rank <b>#${num(rank)}</b>${nt == null ? "" : ` of ${num(nt)}`}`}</div>
     ${dists ? `<div class="sub">${dists}</div>` : ""}</div>`;
  const td = m.today && typeof m.today === "object" ? m.today : null;
  const today = !td ? `<div class="card"><div class="lab">Today's P&L (SIG)</div>${na("Not available yet.")}</div>` :
    `<div class="card"><div class="lab" title="Change in account value (SIG marks) since ${esc(td.since || "00:00 PT")}">Today's P&L (SIG)</div>
     <div class="v2 ${cls(td.pnl)}">${sgn(td.pnl)}</div>
     <div class="sub">${sw("var(--s1)")}Realized today <b class="${cls(td.realized)}">${sgn(td.realized)}</b>${info("Closed trades: cash actually made or lost today.")}</div>
     <div class="sub">${sw("var(--s2)")}Unrealized change <b class="${cls(td.unrealized_change)}">${sgn(td.unrealized_change)}</b>${info("How much the open positions moved at SIG marks today.")}</div>
     <div class="sub mute">since ${esc(String(td.since || "").slice(11, 16) || "00:00")} PT</div></div>`;
  const tt = m.total && typeof m.total === "object" ? m.total : null;
  let total = `<div class="card"><div class="lab">Total P&L since start (SIG)</div>${na("Not available yet.")}</div>`;
  if (tt && (val(tt.realized) != null || val(tt.unrealized) != null)) {
    const r = val(tt.realized) || 0, u = val(tt.unrealized) || 0, sum = val(tt.sum) ?? r + u;
    const neg = Math.min(0, r) + Math.min(0, u), pos = Math.max(0, r) + Math.max(0, u), lo = Math.min(neg, sum, 0), hi = Math.max(pos, sum, 0), span = (hi - lo) || 1;
    const P = v => ((v - lo) / span * 100).toFixed(2) + "%";
    const seg = (a, b, c, t) => `<i style="left:${P(Math.min(a, b))};width:${(Math.abs(b - a) / span * 100).toFixed(2)}%;background:${c}" title="${esc(t)}"></i>`;
    const rEnd = r, uStart = (u < 0) === (r < 0) ? r : 0;
    const bad = tt.ok === false || Math.abs(val(tt.diff) || 0) > 50;
    total = `<div class="card"><div class="lab" title="Realized (closed trades, cash) + unrealized (open positions at SIG marks)">Total P&L since start (SIG)</div>
     <div class="v2 ${cls(sum)}">${sgn(sum)}</div>
     <div class="pbar" role="img" aria-label="realized ${sgn(r)} plus unrealized ${sgn(u)} equals ${sgn(sum)}">
       ${seg(0, rEnd, "var(--s1)", "Realized " + sgn(r))}${seg(uStart, uStart + u, "var(--s2)", "Unrealized " + sgn(u))}
       <span class="z" style="left:${P(0)}"></span><span class="t" style="left:${P(sum)}" title="Total ${sgn(sum)}"></span></div>
     <div class="pscale"><span>${lo === 0 ? "0" : sgn(lo)}</span><span>${hi === 0 ? "0" : sgn(hi)}</span></div>
     <div class="eq">${sw("var(--s1)")}Realized <b class="${cls(r)}">${sgn(r)}</b> <span class="mute">closed trades</span><br>
       ${sw("var(--s2)")}Unrealized <b class="${cls(u)}">${sgn(u)}</b> <span class="mute">open positions at SIG marks</span><br>
       <span class="tk"></span>Total <b class="${cls(sum)}">${sgn(sum)}</b></div>
     ${tt.ok == null && val(tt.diff) == null ? "" : bad ? `<div class="nok">⚠ Does not match account − ${num(start)} (${sgn(tt.account_minus_start)}): off by ${sgn(tt.diff)}</div>`
       : `<div class="ok" title="realized + unrealized = account − ${num(start)}">✓ Adds up: account − ${num(start)} = ${sgn(tt.account_minus_start ?? vs)}</div>`}</div>`;
  }
  const fr = obj(m.fair), fa = val(fr.account) ?? val(o.account_fair), fd = val(fr.vs_sig) ?? (fa != null && acct != null ? fa - acct : null);
  const fair = `<div class="card soft"><div class="lab" title="Value of everything at Polymarket prices: what we expect at settlement if Polymarket is right">Expected at settlement (Polymarket fair)</div>
     ${fa == null ? na("Not available yet.") : `<div class="v3">${num(fa)}</div><div class="sub">if Polymarket is right</div>
     <div class="sub"><b class="${cls(fd)}">${sgn(fd)}</b> vs SIG marks</div><div class="sub">${sgn(fa - start)} vs the ${num(start)} start</div>`}</div>`;
  const v = obj(m.velocity);
  const vel = val(v.turnover_today) == null ? "" : `<div class="card"><div class="lab" title="Return on capital today = P&L at SIG today / average account value today; turnover = volume traded today / the same capital">Capital velocity today${info("The leaders make 20-40% a day by turning their capital over 3-12 times a day.")}</div>
     <div class="v2 ${cls(v.roc_today)}">${val(v.roc_today) == null ? "–" : sgn(v.roc_today * 100, 2) + "%"}</div><div class="sub">return on capital today</div>
     <div class="sub"><b>${num(v.turnover_today, 2)}x</b> turnover today</div><div class="sub mute">${num(v.volume_today)} traded on ${num(v.avg_capital)} average capital</div></div>`;
  return `<div class="cards">${hero}${today}${total}${fair}${vel}</div>`;
}

function basis() {
  return `<div class="seg" role="group" aria-label="basis">${[["sig", "SIG marks"], ["fair", "Polymarket fair"]].map(([k, t]) =>
    `<button data-basis="${k}" class="${ui.basis === k ? "on" : ""}">${t}</button>`).join("")}</div>`;
}

function accountChart(wid) {
  const s = obj(S.series), t = arr(s.t), sg = arr(s.account_sig), fr = arr(s.account_fair), rt = arr(s.rank_t), rk = arr(s.rank);
  let A = t.map((x, i) => [val(x), val(sg[i]), val(fr[i])]).filter(p => p[0] != null && (p[1] != null || p[2] != null));
  ui.dropped = 0;                                         // day 6 §5.2: a sample that jumps > 5% against both neighbours is a glitch
  A = A.filter((p, i) => { if (i === 0 || i === A.length - 1) return true; const a = A[i - 1], b = A[i + 1];
    for (const j of [1, 2]) { if (p[j] == null || a[j] == null || b[j] == null) continue;
      if (Math.abs(p[j] - a[j]) > 0.05 * Math.abs(a[j]) && Math.abs(p[j] - b[j]) > 0.05 * Math.abs(b[j])) { ui.dropped++; return false; } }
    return true; });
  if (ui.zoomWin) A = A.filter(p => p[0] >= ui.zoomWin[0] && p[0] <= ui.zoomWin[1]);
  else if (ui.range) A = A.filter(p => p[0] >= Date.now() / 1000 - ui.range);
  let K = rt.map((x, i) => [val(x), val(rk[i])]).filter(p => p[0] != null && p[1] != null);
  if (ui.zoomWin) K = K.filter(p => p[0] >= ui.zoomWin[0] && p[0] <= ui.zoomWin[1]);
  else if (ui.range) K = K.filter(p => p[0] >= Date.now() / 1000 - ui.range);
  if (A.length < 2 && K.length < 2) return na("Account history is not available yet (sampled every 2 minutes).");
  const start = val(obj(S.money).start) ?? 100000, ts = [...A.map(p => p[0]), ...K.map(p => p[0])];
  const t0 = Math.min(...ts), t1 = Math.max(...ts, t0 + 60);
  const W = width(wid || "account"), L = 56, R = 104, T = 12, H1 = A.length >= 2 ? 210 : 0, G = K.length >= 2 ? (H1 ? 34 : 18) : 0, H2 = K.length >= 2 ? 84 : 0, B = 26, H = T + H1 + G + H2 + B;
  const X = v => L + (v - t0) / (t1 - t0) * (W - L - R);
  let g = "";
  const tt = timeTicks(t0, t1, W - L - R);
  const series = [];
  if (H1) {
    const vs = A.flatMap(p => [p[1], p[2]]).filter(v => v != null).concat([start]);
    let [lo, hi] = [Math.min(...vs), Math.max(...vs)]; const sp = (hi - lo) || 1000; lo -= sp * 0.06; hi += sp * 0.06;
    const tk = ticks(lo, hi, 4), Y = v => T + (tk.hi - v) / (tk.hi - tk.lo) * H1;
    g += tk.ticks.map(v => `<line x1="${L}" x2="${W - R}" y1="${f1(Y(v))}" y2="${f1(Y(v))}" class="gl"/><text x="${L - 7}" y="${f1(Y(v)) + 4}" text-anchor="end">${kf(v, tk.step)}</text>`).join("");
    g += `<line x1="${L}" x2="${W - R}" y1="${f1(Y(start))}" y2="${f1(Y(start))}" class="zl"/><text x="${L + 4}" y="${f1(Y(start)) - 4}">start ${num(start)}</text>`;
    const lines = [["SIG marks", "var(--s1)", 1], ["Polymarket fair", "var(--s2)", 2]];
    const ends = [];
    for (const [name, c, j] of lines) {
      const pts = A.map(p => [p[0], p[j]]);
      if (!pts.some(p => p[1] != null)) continue;
      g += `<path d="${linePath(pts, X, Y)}" class="ln" stroke="${c}"/>`;
      const last = pts.filter(p => p[1] != null).pop();
      ends.push([name, c, last]);
      series.push({name, color: c, t: pts.filter(p => p[1] != null).map(p => p[0]), v: pts.filter(p => p[1] != null).map(p => p[1]), Y, gap: 1200, fmt: v => num(v)});
    }
    const clash = ends.length === 2 && Math.abs(Y(ends[0][2][1]) - Y(ends[1][2][1])) < 15;
    for (const [name, c, p] of ends) {
      g += `<circle cx="${f1(X(p[0]))}" cy="${f1(Y(p[1]))}" r="4" fill="${c}" class="ring"/>`;
      if (!clash) g += `<text x="${f1(X(p[0]) + 9)}" y="${f1(Y(p[1]) + 4)}" class="vl">${num(p[1])}</text>`;
    }
  }
  if (H2) {
    const y0 = T + H1 + G, rs = K.map(p => p[1]);
    let lo = Math.min(...rs), hi = Math.max(...rs); if (hi - lo < 4) { lo -= 2; hi += 2; }
    const tk = ticks(lo, hi, 2), Y2 = v => y0 + (v - tk.lo) / (tk.hi - tk.lo) * H2;    // rank 1 is the top
    g += `<text x="${L}" y="${y0 - 9}" class="lbl">Our rank (lower is better)</text>`;
    g += tk.ticks.map(v => `<line x1="${L}" x2="${W - R}" y1="${f1(Y2(v))}" y2="${f1(Y2(v))}" class="gl"/><text x="${L - 7}" y="${f1(Y2(v)) + 4}" text-anchor="end">#${num(v)}</text>`).join("");
    g += `<path d="${linePath(K, X, Y2)}" class="ln" stroke="var(--ink2)"/>`;
    const last = K[K.length - 1];
    g += `<circle cx="${f1(X(last[0]))}" cy="${f1(Y2(last[1]))}" r="4" fill="var(--ink2)" class="ring"/><text x="${f1(X(last[0]) + 9)}" y="${f1(Y2(last[1]) + 4)}" class="vl">#${num(last[1])}</text>`;
    series.push({name: "Rank", color: "var(--ink2)", t: K.map(p => p[0]), v: rs, Y: Y2, gap: 1800, fmt: v => "#" + num(v)});
  }
  const yb = T + H1 + G + H2;
  g += `<line x1="${L}" x2="${W - R}" y1="${yb}" y2="${yb}" class="zl"/>` + tt.ticks.map(v => `<text x="${f1(X(v))}" y="${yb + 17}" text-anchor="middle">${esc(tt.label(v))}</text>`).join("");
  g += `<line id="xh-account" class="xh" x1="0" x2="0" y1="${T}" y2="${yb}" display="none"/>` + series.map((s, i) => `<circle id="xh-account-${i}" r="4" fill="${s.color}" class="ring" display="none"/>`).join("");
  g += `<rect data-chart="account" data-drag="1" x="${L}" y="${T}" width="${W - L - R}" height="${yb - T}" fill="transparent"/>`;
  CH.account = {id: "account", L, R, W, t0, t1, X, series};
  const leg = [];
  if (H1) leg.push([`<i class="lk" style="background:var(--s1)"></i>`, "SIG marks (leaderboard)"], [`<i class="lk" style="background:var(--s2)"></i>`, "Polymarket fair"]);
  if (H2) leg.push([`<i class="lk" style="background:var(--ink2)"></i>`, "Rank"]);
  return legend(leg) + `<svg width="${W}" height="${H}" viewBox="0 0 ${W} ${H}" role="img" aria-label="Account value over time">${g}</svg>`;
}

// ------------------------------------------------------------------------------------------- Capital
function capital() {
  const c = S.capital, o = obj(S.overview);
  if (!c || typeof c !== "object") {
    if (val(o.free_cash) == null) return na("The capital view is not available yet.");
    return na("The full capital view is not available yet; the engine's v2 figures:") + `<div class="tiles">
      <div class="card"><div class="lab">Free cash</div><div class="v3">${num(o.free_cash)}</div></div>
      <div class="card"><div class="lab">Cash floor</div><div class="v3">${num(o.reserve)}</div></div>
      <div class="card"><div class="lab">Committed to resting buys</div><div class="v3">${num(o.committed)}</div></div></div>`;
  }
  const ft = val(c.free_to_trade), tg = val(c.target), cash = val(c.cash), rel = obj(c.released_today);
  const tile = (lab, v, sub, tip) => `<div class="card"><div class="lab"${tip ? ` title="${esc(tip)}"` : ""}>${lab}${tip ? info(tip) : ""}</div><div class="v3">${v}</div>${sub ? `<div class="sub">${sub}</div>` : ""}</div>`;
  const fc = val(c.free_cash) ?? (cash != null && val(c.floor) != null ? cash - c.floor : cash);   // the target excludes the floor
  const prog = tg && fc != null ? Math.max(0, Math.min(1, fc / tg)) : null;
  const tiles = [
    tile("Cash", num(cash), "", "Cash in the SIG account right now."),
    tile("Holdings", num(c.holdings_sig), `${num(c.holdings_fair)} at Polymarket fair`, "Open positions valued at SIG marks (fair alongside)."),
    tile("Cash floor", num(c.floor), "never spent", "Cash the engine always keeps."),
    tile("Opportunity reserve", num(c.reserve), `used ${num(c.reserve_used)}`, "Ring-fenced for big opportunities: fat-finger fills, sniper takes on new markets, takes 8¢+ through fair."),
    tile("Committed", num(c.committed), "resting buy orders", "Entry buys resting on SIG, counted against cash."),
    tile("Free to trade", `<span class="${ft != null && ft < 0 ? "bad" : ""}">${num(ft == null ? null : Math.max(0, ft))}</span>`,
      ft != null && ft < 0 ? `<span class="bad">cash is ${num(-ft)} short of floor + reserve + committed</span>` : "for new entries", "Cash − floor − reserve − committed. What the market maker, ladders and pairs may still spend."),
    tile("Free-cash target", tg == null ? "–" : `${num(fc)} <span class="mute" style="font-size:14px">of ${num(tg)}</span>`,
      (c.target_date ? `by ${esc(dday(c.target_date))}` : "") + (prog == null ? "" : `<div class="meter"><i style="width:${(prog * 100).toFixed(1)}%"></i></div>`), "Free cash (cash above the 5k floor) the release layer is working towards."),
    ...(val(obj(obj(c.idle).no_exit).sig) == null ? [] : [(() => { const a = obj(c.idle.idle_3d), b = obj(c.idle.no_exit);
      return tile("Capital idle", num(a.sig), `${num(a.n)} holdings &gt; 3 days with no exit order · <b>${num(b.sig)}</b> in ${num(b.n)} holdings with no exit order at all`,
        "Holdings older than 3 days with no exit order resting (SIG marks). Also: every holding with no exit order, whatever its age."); })()]),
    tile("Released today", val(rel.sig) == null ? "–" : num(rel.sig), val(rel.sig) == null ? "not available yet" : `freed at ${pct(rel.avg_cost)} average cost`,
      "Cash the release layer freed today by selling where it is cheap, and what it gave up vs fair on average."),
  ].join("");
  const bk = arr(c.buckets).filter(b => b && typeof b === "object");
  let bar = na("Where the capital sits is not available yet.");
  if (bk.length) {
    const tot = bk.reduce((a, b) => a + Math.max(0, val(b.sig) || 0), 0) || 1, ftot = bk.reduce((a, b) => a + Math.max(0, val(b.fair) || 0), 0);
    const color = b => b.key === "cash" ? "var(--neutral)" : colorOf(b.key);
    bar = `<div class="sbar" role="img" aria-label="where the capital sits">${bk.filter(b => (val(b.sig) || 0) > 0).map(b =>
      `<i style="flex:${(val(b.sig) / tot * 100).toFixed(3)} 1 0;background:${color(b)}"${tipAttr(`${sw(color(b))}<b>${esc(b.label || b.key)}</b><div>${num(b.sig)} at SIG marks (${pct(val(b.sig) / tot)})</div><div>${num(b.fair)} at Polymarket fair</div>`)}></i>`).join("")}</div>
      <div class="wrap"><table><thead><tr><th>Where</th><th class="n">SIG marks</th><th class="n">Share</th><th class="n">Polymarket fair</th></tr></thead><tbody>${bk.map(b =>
      `<tr><td><span class="tag">${sw(color(b))}${esc(b.label || nameOf(b.key))}</span></td><td class="n">${num(b.sig)}</td><td class="n">${pct((val(b.sig) || 0) / tot)}</td><td class="n">${num(b.fair)}</td></tr>`).join("")}</tbody>
      <tfoot><tr><td>Total</td><td class="n">${num(tot)}</td><td class="n">100%</td><td class="n">${num(ftot)}</td></tr></tfoot></table></div>`;
  }
  return `<div class="tiles t8">${tiles}</div><div class="card"><h3>Where the capital sits</h3>${bar}</div>`;
}

// ------------------------------------------------------------------------------------------- Strategies
function strategies() {
  const rows = arr(S.strategies).filter(r => r && typeof r === "object");
  if (!rows.length) {
    const ls = arr(S.layers).filter(l => l && typeof l === "object");
    if (!ls.length) return na("Strategies are not available yet.");
    return na("Strategy P&L at SIG marks is not available yet; the engine's v2 figures (since the night start):") + `<div class="wrap"><table><thead><tr><th>Strategy</th><th>State</th>
      <th class="n">Fills</th><th class="n">Resting orders</th><th class="n">P&L at fair</th><th class="n">Realized</th></tr></thead><tbody>${ls.map(l => `<tr><td>${tag(l.name)}<div class="mute">${esc(l.note || "")}</div></td>
      <td>${chip(l.state, l.killed)}</td><td class="n">${num(l.fills)}</td><td class="n">${num(l.resting)}</td><td class="n ${cls(l.pnl_fair)}">${sgn(l.pnl_fair)}</td><td class="n ${cls(l.realized)}">${sgn(l.realized)}</td></tr>`).join("")}</tbody></table></div>`;
  }
  const three = p => ["realized", "unrealized_sig", "unrealized_fair"].map(k => { const v = obj(p)[k]; return `<td class="n ${cls(v)}">${sgn(v)}</td>`; }).join("");
  const tot = (g, k) => rows.reduce((a, r) => a + (val(obj(r[g])[k]) || 0), 0);
  const body = rows.map(r => `<tr><td class="wide">${tag(r.key)}<div class="mute" style="font-size:12px">${esc(r.what || "")}</div></td>
    <td>${chip(r.state, r.killed)}${r.killed ? `<div class="bad" style="font-size:12px;white-space:normal">${esc(obj(r.killed).reason || "")}</div>` : ""}</td>
    ${three(r.today)}${three(r.total)}<td class="n">${num(r.fills_today)}</td><td class="n">${num(r.capital_used)}</td>
    <td class="n" title="Volume today ${esc(num(r.volume_today))} SIG">${val(r.turnover_today) == null ? "–" : num(r.turnover_today, 1) + "x"}</td><td class="n">${num(r.resting)}</td>
    <td class="wide">${esc(r.last_action || "–")}</td></tr>`).join("");
  const foot = `<tr><td>All strategies</td><td></td>${["today", "total"].map(g => ["realized", "unrealized_sig", "unrealized_fair"].map(k => { const v = tot(g, k); return `<td class="n ${cls(v)}">${sgn(v)}</td>`; }).join("")).join("")}
    <td class="n">${num(rows.reduce((a, r) => a + (val(r.fills_today) || 0), 0))}</td><td class="n">${num(rows.reduce((a, r) => a + (val(r.capital_used) || 0), 0))}</td><td></td><td class="n">${num(rows.reduce((a, r) => a + (val(r.resting) || 0), 0))}</td><td></td></tr>`;
  const h3 = `<th class="n" title="Closed trades: cash made or lost">Realized</th><th class="n" title="Open positions at SIG marks">Unrealized (SIG)</th><th class="n" title="Open positions at Polymarket fair">Unrealized (fair)</th>`;
  return `<div class="wrap"><table><thead><tr><th rowspan="2">Strategy</th><th rowspan="2">State</th><th colspan="3" class="grp">Today</th><th colspan="3" class="grp">Since start</th>
    <th rowspan="2" class="n">Fills today</th><th rowspan="2" class="n" title="Capital in this strategy's positions, at SIG marks">Capital used</th><th rowspan="2" class="n" title="Volume traded today / capital used">Turnover today</th><th rowspan="2" class="n">Resting orders</th><th rowspan="2">Last action</th></tr>
    <tr>${h3}${h3}</tr></thead><tbody>${body}</tbody><tfoot>${foot}</tfoot></table></div>`;
}

function pairsView() {
  const p = S.pairs;
  if (!p || typeof p !== "object") return na("Pair sets are not available.");
  const rows = arr(p.sets).map(s => `<tr><td>${esc(s.race)}</td><td>${esc(s.side)}</td><td class="n">${num(s.qty)}</td><td class="n good">${sgn(s.locked, 2)}</td>
    <td class="n">${num(s.unwind_value, 3)}</td><td class="n">${num(s.threshold, 3)}</td><td class="n ${s.residue ? "warn" : ""}">${num(s.residue)}</td><td class="n">${num(s.capital)}</td><td>${esc(s.state || "")}</td></tr>`).join("");
  return `<div class="row">capital used <b>${num(p.used)}</b> of the ${num(p.cap)} cap · profit locked in <b class="good">${sgn(p.locked, 2)}</b></div>
    <div class="wrap"><table><thead><tr><th>Race</th><th>Set</th><th class="n">Sets</th><th class="n" title="Profit locked in if every leg settles">Locked</th><th class="n" title="What one set sells for now">Unwind value</th>
    <th class="n" title="Unwind early above this">Unwind at</th><th class="n" title="Shares left on one leg only">One-leg residue</th><th class="n">Capital</th><th>State</th></tr></thead>
    <tbody>${rows || '<tr><td colspan="9" class="mute">no open sets</td></tr>'}</tbody></table></div>`;
}

function laddersView() {
  const ps = arr(S.positions).filter(p => p && typeof p === "object");
  if (!ps.length) return na("No position exit ladders.");
  return ps.map(p => `<div style="margin-bottom:14px"><h3>${esc(p.name)} · ${esc(p.market || "")}</h3>
    <div class="sub">holding <b>${esc(String(p.side || "").toUpperCase())} ${num(p.held)}</b> · ladder sub-book ${num(p.manual)}${p.tranche ? ` · D1 tranche <b>${num(p.tranche.qty)}</b> (${esc(p.tranche.state)})` : ""} · ${esc(p.status || "")}</div>
    <div class="wrap" style="margin-top:6px"><table><thead><tr><th class="n">Sell at</th><th class="n">YES-equivalent</th><th class="n">Shares</th><th>State</th></tr></thead><tbody>${arr(p.ladder).map(l =>
    `<tr><td class="n">${px(l.px)}</td><td class="n">${px(l.yes_px)}</td><td class="n">${num(l.qty)}</td><td>${esc(l.state)}</td></tr>`).join("")}</tbody></table></div></div>`).join("");
}

// ------------------------------------------------------------------------------------------- Holdings
const HCOLS = [
  {k: "market", t: "Market"}, {k: "side", t: "Side"}, {k: "shares", t: "Shares", n: 1}, {k: "avg_cost", t: "Avg cost", n: 1, tip: "Average price paid per share (held side)"},
  {k: "sig_mark", t: "SIG mark", n: 1, tip: "SIG's valuation of one share: what the leaderboard uses"}, {k: "sig_bid", t: "SIG bid", n: 1, tip: "Best price we could sell at on SIG now"},
  {k: "fair", t: "PM fair", n: 1, tip: "Polymarket's price for the held side"}, {k: "value_sig", t: "Value (SIG)", n: 1, tip: "shares × SIG mark"},
  {k: "unreal_sig", t: "Unrealized (SIG)", n: 1, tip: "shares × (SIG mark − avg cost)"}, {k: "unreal_fair", t: "Unrealized (fair)", n: 1, tip: "shares × (PM fair − avg cost)"},
  {k: "cost_to_free", t: "Cost to free", n: 1, tip: "(fair − bid) / bid: what selling into the SIG bid now gives up vs Polymarket fair. Negative: the bid is above fair."},
  {k: "layer", t: "Strategy", tip: "The strategy that owns most of the position"}];
function holdRows() {
  const q = ui.hq.trim().toLowerCase(), {k, dir} = ui.hsort, numeric = (HCOLS.find(c => c.k === k) || {}).n;
  const rows = arr(S.holdings).filter(h => h && typeof h === "object" && (!ui.hl || h.layer === ui.hl) && (!ui.hr || h.release_candidate)
    && (!q || (String(h.market || "") + " " + h.ex).toLowerCase().includes(q)));
  const key = h => k === "layer" ? nameOf(h.layer) : numeric ? val(h[k]) : (h[k] == null ? null : String(h[k]));
  return rows.sort((a, b) => { const x = key(a), y = key(b); if (x == null || y == null) return x == null ? (y == null ? 0 : 1) : -1;
    return (numeric ? x - y : x.localeCompare(y)) * dir; });
}
function hlopts() {
  const ks = new Set(arr(S.holdings).map(h => h && h.layer).filter(Boolean)), keys = [...ORDER, "inventory", ...[...ks].filter(k => !ORDER.includes(k) && k !== "inventory").sort()].filter(k => ks.has(k));
  return `<option value="">All strategies</option>` + keys.map(k => `<option value="${esc(k)}" ${k === ui.hl ? "selected" : ""}>${esc(nameOf(k))}</option>`).join("");
}
function holdings() {
  if (!Array.isArray(S.holdings)) {
    const ms = arr(S.markets).filter(m => m && val(m.pos));
    if (!ms.length) return na("Holdings are not available yet.");
    return na("Holdings with cost and P&L are not available yet; net positions from the markets list:") + `<div class="wrap"><table><thead><tr><th>Market</th><th>Side</th>
      <th class="n">Shares</th><th class="n">PM fair (YES)</th><th class="n">SIG bid / ask (YES)</th></tr></thead><tbody>${ms.map(m => `<tr><td>${esc(m.race)} <b>${esc(m.party)}</b></td>
      <td>${m.pos > 0 ? "YES" : "NO"}</td><td class="n">${num(Math.abs(m.pos))}</td><td class="n">${px(m.fair)}</td><td class="n">${px(m.bid)} / ${px(m.ask)}</td></tr>`).join("")}</tbody></table></div>`;
  }
  const all = arr(S.holdings).filter(h => h && typeof h === "object"), rows = holdRows(), sum = k => rows.reduce((a, h) => a + (val(h[k]) || 0), 0);
  const shown = ui.hall ? rows : rows.slice(0, HOLD_FIRST);
  const nrel = all.filter(h => h.release_candidate).length;
  const head = HCOLS.map(c => `<th class="sort${c.n ? " n" : ""}" data-sort="${c.k}"${c.tip ? ` title="${esc(c.tip)}"` : ""} aria-sort="${ui.hsort.k === c.k ? (ui.hsort.dir > 0 ? "ascending" : "descending") : "none"}">${c.t}${ui.hsort.k === c.k ? `<span class="ar">${ui.hsort.dir > 0 ? "▲" : "▼"}</span>` : ""}</th>`).join("");
  const body = shown.map(h => {
    const ctf = val(h.cost_to_free);
    return `<tr class="click" data-ex="${esc(h.ex)}" data-layer="${esc(h.layer)}"><td>${esc(h.market || h.ex)}${h.release_candidate ? '<span class="badge" title="The release layer would sell this now: freeing the cash costs little">release candidate</span>' : ""}</td>
    <td>${esc(String(h.side || "").toUpperCase())}</td><td class="n">${num(h.shares)}</td><td class="n">${px(h.avg_cost)}</td><td class="n">${px(h.sig_mark)}</td><td class="n">${px(h.sig_bid)}</td>
    <td class="n">${px(h.fair)}</td><td class="n">${num(h.value_sig)}</td><td class="n ${cls(h.unreal_sig)}">${sgn(h.unreal_sig)}</td><td class="n ${cls(h.unreal_fair)}">${sgn(h.unreal_fair)}</td>
    <td class="n ${ctf != null && ctf < 0 ? "good" : ""}"${ctf != null && ctf < 0 ? ' title="The SIG bid is above fair: selling gains vs fair"' : ""}>${ctf == null ? "–" : num(ctf * 100, 1) + "%"}</td><td>${tag(h.layer)}</td></tr>
    ${ui.open === h.ex ? `<tr><td colspan="${HCOLS.length}">${detail(arr(S.markets).find(m => m && m.ex === h.ex) || {ex: h.ex})}</td></tr>` : ""}`;
  }).join("");
  return `<div class="sub" style="margin:0 0 8px">${rows.length === all.length ? `<b>${all.length}</b> positions` : `<b>${rows.length}</b> of ${all.length} positions match`} · value <b>${num(sum("value_sig"))}</b> at SIG marks, ${num(sum("value_fair"))} at fair ·
    unrealized <b class="${cls(sum("unreal_sig"))}">${sgn(sum("unreal_sig"))}</b> SIG, <span class="${cls(sum("unreal_fair"))}">${sgn(sum("unreal_fair"))}</span> fair · ${nrel} release candidate${nrel === 1 ? "" : "s"} · click a row for its orders and fills</div>
    <div class="wrap"><table><thead><tr>${head}</tr></thead><tbody>${body || `<tr><td colspan="${HCOLS.length}" class="mute">${all.length ? "No positions match the filter." : "No open positions."}</td></tr>`}</tbody>
    ${rows.length ? `<tfoot><tr><td colspan="7">Total of the ${rows.length} position${rows.length === 1 ? "" : "s"}${shown.length < rows.length ? ` (${shown.length} shown)` : ""}</td><td class="n">${num(sum("value_sig"))}</td><td class="n ${cls(sum("unreal_sig"))}">${sgn(sum("unreal_sig"))}</td><td class="n ${cls(sum("unreal_fair"))}">${sgn(sum("unreal_fair"))}</td><td colspan="2"></td></tr></tfoot>` : ""}</table></div>
    ${rows.length > HOLD_FIRST ? `<button class="btn more-btn" data-more="hall">${ui.hall ? `Show the first ${HOLD_FIRST}` : `Show all ${rows.length}`}</button>` : ""}`;
}

// ------------------------------------------------------------------------------------------- Activity
function alopts() {
  const ks = new Set(arr(S.activity).map(a => a && a.layer).filter(Boolean)), keys = [...ORDER.filter(k => ks.has(k)), ...[...ks].filter(k => !ORDER.includes(k)).sort()];
  return `<option value="">All strategies</option>` + keys.map(k => `<option value="${esc(k)}" ${k === ui.al ? "selected" : ""}>${esc(nameOf(k))}</option>`).join("");
}
function activity() {
  if (!Array.isArray(S.activity)) return na("Fills as sentences are not available yet.");
  const q = ui.aq.trim().toLowerCase();
  const rows = S.activity.filter(a => a && typeof a === "object" && (!ui.al || a.layer === ui.al) && (!q || String(a.text || "").toLowerCase().includes(q)))
    .sort((a, b) => (val(b.id) ?? -1) - (val(a.id) ?? -1)).slice(0, 300);
  if (!rows.length) return `<div class="na">${S.activity.length ? "No fills match the filter." : "No fills yet."}</div>`;
  const shown = ui.aall ? rows : rows.slice(0, ACT_FIRST);
  return `<div class="sub" style="margin:0 0 8px">${rows.length} fill${rows.length === 1 ? "" : "s"}, newest first${shown.length < rows.length ? ` (${shown.length} shown)` : ""}</div><div class="wrap"><table><tbody>${shown.map(a =>
    `<tr data-id="${esc(a.id)}" data-layer="${esc(a.layer)}"><td class="mute" style="width:1%">${esc(when(a.t))}</td><td style="width:1%">${tag(a.layer)}</td><td class="txt">${esc(a.text)}</td></tr>`).join("")}</tbody></table></div>
    ${rows.length > ACT_FIRST ? `<button class="btn more-btn" data-more="aall">${ui.aall ? `Show the newest ${ACT_FIRST}` : `Show all ${rows.length}`}</button>` : ""}`;
}

// ------------------------------------------------------------------------------------------- Health + engine log
function cov() {
  const un = arr(obj(S.coverage).uncovered);
  if (!un.length) return val(obj(S.coverage).pct) == null ? na("Coverage is not available yet.") : `<div class="mute">Every eligible side has its fat-finger bids resting.</div>`;
  return `<div class="wrap"><table><thead><tr><th>Market</th><th>Side</th><th class="n">Uncovered for</th><th>Why not covered</th></tr></thead><tbody>${un.map(u =>
    `<tr><td>${esc(u.market || u.ex)}</td><td>${esc(String(u.side || "").toUpperCase())}</td><td class="n${val(u.minutes) > 5 ? " warn" : ""}">${val(u.minutes) == null ? "–" : num(u.minutes, 0) + " min"}</td><td>${esc(u.reason || "")}</td></tr>`).join("")}</tbody></table></div>`;
}
function health() {
  const o = obj(S.overview), h = obj(S.health), sig = obj(h.sig), rd = obj(sig.read), wr = obj(sig.write), ex = obj(h.executor);
  if (!Object.keys(h).length && !Object.keys(o).length) return na("Engine health is not available yet.");
  const feeds = Object.entries(obj(h.feeds)).map(([k, t]) => `<div>${esc(k)}: <b>${ago(t)}</b></div>`).join("") || '<div class="mute">–</div>';
  const LAT = {fill_to_queued: "fill → exit queued", pm_to_defend: "Polymarket move → quotes defended", plan_cycle: "planning cycle", cancel_to_sent: "cancel → sent"};
  const lat = Object.entries(obj(h.latency)).map(([k, v]) => `<div>${esc(LAT[k] || k)}: p50 <b>${num(obj(v).p50, 2)} s</b> · p90 ${num(obj(v).p90, 2)} s <span class="mute">(${num(obj(v).n)})</span></div>`).join("") || '<div class="mute">–</div>';
  const rt = Object.entries(obj(h.realtime)).map(([k, v]) => `<div>${esc(k === "sig" ? "SIG" : k === "pm" ? "Polymarket" : k)}: ${esc(Object.entries(obj(v)).map(([a, b]) => `${a} ${b}`).join(" · ") || "–")}</div>`).join("") || '<div class="mute">–</div>';
  const ls = obj(o.loss_stop);
  return `<div class="grid">
    <div class="card"><h3>New risk</h3><div class="mid ${o.paused ? "bad" : "good"}">${o.paused ? "PAUSED" : "on"}</div><div class="mute">exits, rebounds, unwinds, repairs always run</div></div>
    <div class="card"><h3>κ (capital discount)</h3><div class="mid">${val(o.kappa) == null ? "–" : (o.kappa * 100).toFixed(1) + "¢"}</div><div class="mute">${esc(o.kappa_src || "")}</div></div>
    <div class="card"><h3>Loss stop (bot P&L at fair, 24 h)</h3><div class="mid ${ls.tripped ? "bad" : ""}">${o.loss_stop ? (ls.tripped ? "TRIPPED" : "ok") : "–"}</div><div class="mute">${o.loss_stop ? sgn(ls.pnl) + " of −" + num(ls.limit) : ""}</div></div>
    <div class="card"><h3>Engine's fair-value account</h3><div class="mid">${num(o.account_fair)}</div><div class="mute">since 00:00 ${sgn(o.change_today)} · since night start ${sgn(o.change_start)} · realized today ${sgn(o.realized_today)}</div></div>
    <div class="card"><h3>Free cash vs floor</h3><div class="mid">${num(o.free_cash)}</div><div class="mute">floor ${num(o.reserve)} · committed entries ${num(o.committed)}</div></div>
    <div class="card"><h3>SIG reads</h3><div>p50 <b>${num(rd.p50, 2)} s</b> · p90 <b>${num(rd.p90, 2)} s</b></div><div>5xx/timeouts <b class="${rd.bad_rate > .1 ? "bad" : ""}">${num((rd.bad_rate || 0) * 100, 1)}%</b></div><div>used <b>${num(sig.reads_last_min)}</b> / ${num(sig.read_limit)} per min</div></div>
    <div class="card"><h3>SIG writes</h3><div>p50 <b>${num(wr.p50, 2)} s</b> · p90 <b>${num(wr.p90, 2)} s</b></div><div>5xx/timeouts <b class="${wr.bad_rate > .1 ? "bad" : ""}">${num((wr.bad_rate || 0) * 100, 1)}%</b></div><div>used <b>${num(sig.writes_last_min)}</b> / ${num(sig.write_limit)} per min</div></div>
    <div class="card"><h3>Executor</h3><div>queue <b>${esc(JSON.stringify(ex.queue || {}))}</b></div><div>in flight <b>${num(ex.in_flight)}</b> · capacity <b>${num(ex.capacity_ewma, 1)}</b>/min · batch ${num(ex.batch_cap)}</div><div>journal UNKNOWN <b class="${ex.unknown ? "bad" : ""}">${num(ex.unknown)}</b> · stale dropped ${num(ex.dropped_stale)}</div></div>
    <div class="card"><h3>Feeds (age)</h3>${feeds}<div>loop <b>${num(h.loop_s, 1)} s</b> · errors last hour <b class="${h.errors ? "bad" : ""}">${num(h.errors)}</b></div></div>
    <div class="card"><h3>Latency</h3>${lat}</div><div class="card"><h3>Realtime feeds</h3>${rt}</div></div>`;
}
function evkinds() {
  const kinds = [...new Set(arr(S.events).map(e => e && e.kind).filter(Boolean))].sort();
  return `<option value="">all kinds</option>` + kinds.map(k => `<option ${k === ui.ev ? "selected" : ""}>${esc(k)}</option>`).join("");
}
function events() {
  if (!Array.isArray(S.events)) return na("The engine log is not available.");
  const q = ui.evq.toLowerCase(), rows = S.events.filter(e => e && (!ui.ev || e.kind === ui.ev) && (!q || String(e.text || "").toLowerCase().includes(q)));
  return `<div class="wrap"><table><thead><tr><th>Time</th><th>Kind</th><th>Event</th></tr></thead><tbody>${rows.map(e => `<tr><td class="mute">${esc(e.t)}</td><td><span class="chip">${esc(e.kind)}</span></td><td class="txt">${esc(e.text)}</td></tr>`).join("")
    || '<tr><td colspan="3" class="mute">nothing logged</td></tr>'}</tbody></table></div>`;
}

// ------------------------------------------------------------------------------------------- Leaders
function lead() {
  const Ld = obj(S.leaders), series = arr(Ld.series).filter(s => s && arr(s.points).length), th = obj(Ld.thresholds), m = obj(S.money);
  const ours = arr(Ld.latest).find(r => r && r.ours), mine = val(m.vs_start) ?? (ours ? val(ours.value) : null);
  const tiles = ["10", "25", "50", "100"].filter(k => val(th[k]) != null).map(k => { const gap = mine == null ? null : th[k] - mine;
    return `<div class="card"><div class="lab">#${k} has</div><div class="v3">${sgn(th[k])}</div><div class="sub">${gap == null ? "" : gap > 0 ? `we need <b>${sgn(gap)}</b>` : `we are ahead by <b>${num(-gap)}</b>`}</div></div>`; }).join("");
  let out = tiles ? `<div class="tiles">${tiles}</div>` : na("Rank thresholds (#10 / #25 / #50 / #100) are not available yet.");
  if (!series.length) return out + na("No leaderboard samples yet (logged every 10 min to data/leaders.csv).");
  const W = width("lead"), H = 330, L = 60, R = 16, T = 12, B = 26;
  const xs = series.flatMap(s => s.points.map(p => val(p[0]))).filter(v => v != null), ys = series.flatMap(s => s.points.map(p => val(p[1]))).filter(v => v != null);
  const x0 = Math.min(...xs), x1 = Math.max(...xs, x0 + 60), tk = ticks(Math.min(...ys, 0), Math.max(...ys), 5);
  const X = x => L + (x - x0) / (x1 - x0) * (W - L - R), Y = y => T + (tk.hi - y) / (tk.hi - tk.lo) * (H - T - B), tt = timeTicks(x0, x1, W - L - R);
  let g = tk.ticks.map(v => `<line x1="${L}" x2="${W - R}" y1="${f1(Y(v))}" y2="${f1(Y(v))}" class="${v === 0 ? "zl" : "gl"}"/><text x="${L - 7}" y="${f1(Y(v)) + 4}" text-anchor="end">${kf(v, tk.step)}</text>`).join("");
  g += tt.ticks.map(v => `<text x="${f1(X(v))}" y="${H - B + 17}" text-anchor="middle">${esc(tt.label(v))}</text>`).join("");
  for (const s of [...series.filter(s => !s.ours), ...series.filter(s => s.ours)])
    g += `<path d="${linePath(s.points.map(p => [val(p[0]), val(p[1])]), X, Y)}" class="ln" stroke="${s.ours ? "var(--s1)" : "var(--neutral)"}" stroke-width="${s.ours ? 2.5 : 1.2}" opacity="${s.ours ? 1 : .7}"><title>${esc(s.name)}</title></path>`;
  const me = series.find(s => s.ours), lp = me && me.points.filter(p => val(p[1]) != null).pop();
  if (lp) g += `<circle cx="${f1(X(lp[0]))}" cy="${f1(Y(lp[1]))}" r="4" fill="var(--s1)" class="ring"/><text x="${f1(X(lp[0]) - 8)}" y="${f1(Y(lp[1]) - 9)}" text-anchor="end" class="vl">us ${sgn(lp[1])}</text>`;
  const rows = arr(Ld.latest).map(r => `<tr${r.ours ? ' style="font-weight:700"' : ""}><td class="n">${num(r.rank)}</td><td>${esc(r.name)}${r.ours ? " (us)" : ""}</td><td class="n">${num(r.value)}</td><td class="n">${num(r.trades)}</td></tr>`).join("");
  return out + `<div class="card chart"><div class="chead"><h3>Leaderboard P&L over time</h3></div>${legend([['<i class="lk" style="background:var(--s1)"></i>', "Our account"], ['<i class="lk" style="background:var(--neutral)"></i>', "Top 20"]])}
    <svg width="${W}" height="${H}" viewBox="0 0 ${W} ${H}" role="img" aria-label="Leaderboard over time">${g}</svg></div>
    <div class="wrap" style="margin-top:12px"><table><thead><tr><th class="n">Rank</th><th>Account</th><th class="n">P&L</th><th class="n">Trades</th></tr></thead><tbody>${rows}</tbody></table></div>`;
}

// ------------------------------------------------------------------------------------------- Controls (v2, unchanged behaviour)
function ctlbar() {
  const o = obj(S.overview), k = val(o.kappa);
  return `<div class="card"><div class="row" style="margin-top:0"><b>New risk:</b> ${o.paused ? '<span class="chip killed">PAUSED</span>' : '<span class="chip full">on</span>'}
    <span class="mute">Exits, rebounds, unwinds and repairs always run.</span></div>
    <div class="row ctl"><button class="btn" id="pauseBtn">${o.paused ? "Resume new risk" : "Pause new risk"}</button>
      <button class="btn danger" data-confirm="1" id="pullBtn">Pause + pull engine quotes</button></div>
    <div class="row ctl">κ (capital discount) now <b>${k == null ? "–" : (k * 100).toFixed(1) + "¢"}</b> <span class="mute">${esc(o.kappa_src || "")}</span> · override
      <input type="number" id="kv" min="0" max="3" step="0.5" placeholder="¢">¢ for <input type="number" id="km" min="1" max="1440" value="60"> min <button class="btn" data-confirm="1" id="kBtn">Set κ</button></div>
    <div class="row" style="margin-bottom:0"><button class="btn" id="roBtn">${!LIVE ? "static snapshot" : o.read_only ? "Unlock controls" : "Read-only"}</button>
      <span class="mute">${!LIVE ? "This is a saved snapshot: controls are off." : o.read_only ? "Read-only mode: every control is refused until you unlock." : "Destructive actions ask twice."}</span></div></div>`;
}
function ctllayers() {
  const rows = arr(S.layers).filter(l => l && typeof l === "object").map(l => {
    const caps = Object.entries(obj(l.caps)).map(([k, c]) => `<div>${esc(k)}: <b>${num(c.used)}</b> / ${num(c.cap)}
      <span class="ctl"><input type="number" data-cap="${esc(l.name)}:${esc(k)}" min="${esc(c.min)}" max="${esc(c.max)}" value="${esc(c.cap)}"></span></div>`).join("") || "–";
    const kills = arr(l.kill).map(k => `<div class="${k.hit ? "bad" : "mute"}">${esc(k.rule)}: ${esc(k.value)}</div>`).join("") || "–";
    return `<tr><td><b>${esc(l.name)}</b><div class="mute">${esc(l.note || "")}</div></td><td>${chip(l.state, l.killed)}<div class="mute">${l.killed ? esc(l.killed.reason) : "since " + ago(l.since)}</div>
      <div class="ctl"><select data-state="${esc(l.name)}">${["off", "pilot", "full"].map(s => `<option ${s === l.state ? "selected" : ""}>${s}</option>`).join("")}</select>
      <button class="btn" data-confirm="1" data-act="state" data-layer="${esc(l.name)}">Apply</button></div></td>
      <td>${caps}${Object.keys(obj(l.caps)).length ? `<div class="ctl"><button class="btn" data-confirm="1" data-act="caps" data-layer="${esc(l.name)}">Set caps</button></div>` : ""}</td>
      <td class="n">${num(l.resting)}</td><td class="n">${num(l.fills)}</td><td class="n">${c2(l.edge)}</td><td class="n">${c2(l.markout30)}</td>
      <td class="n ${cls(l.pnl_fair)}">${sgn(l.pnl_fair)}</td><td class="n ${cls(l.realized)}">${sgn(l.realized)}</td><td>${kills}</td></tr>`;
  }).join("");
  if (!rows) return na("No strategy layers reported.");
  return `<div class="wrap"><table><thead><tr><th>Layer</th><th>Flag</th><th>Caps (used / cap)</th><th class="n">Resting</th><th class="n">Fills</th><th class="n">Edge vs fair</th><th class="n">Mark-out 30m</th>
    <th class="n">P&L @ fair</th><th class="n">Realized</th><th>Kill rules</th></tr></thead><tbody>${rows}</tbody></table></div>`;
}
function ctlmarkets() {
  let rows = arr(S.markets).filter(r => r && typeof r === "object");
  const q = ui.q.toLowerCase(), mis = parseFloat(ui.mis);
  rows = rows.filter(r => (!q || (r.title + " " + r.race + " " + r.ex).toLowerCase().includes(q)) && (!ui.haspos || r.pos)
    && (!ui.layer || obj(r.resting)[ui.layer]) && (isNaN(mis) || (r.mis != null && Math.abs(r.mis) * 100 >= mis)));
  rows.sort((a, b) => Math.abs(b.mis || 0) - Math.abs(a.mis || 0));
  const per = 50, pages = Math.max(1, Math.ceil(rows.length / per)); ui.page = Math.min(ui.page, pages - 1);
  const body = rows.slice(ui.page * per, ui.page * per + per).map(r => `<tr class="click" data-ex="${esc(r.ex)}"><td>${esc(r.race)} <b>${esc(r.party)}</b>${r.paused ? ' <span class="chip killed">paused</span>' : ""}</td>
    <td class="n">${px(r.fair)}${r.u != null ? ` <span class="mute">±${(r.u * 100).toFixed(1)}</span>` : ""}</td><td class="n">${px(r.bid)} / ${px(r.ask)}</td><td class="n ${cls(r.mis && -r.mis)}">${c2(r.mis)}</td>
    <td class="n">${r.pos ? sgn(r.pos) : "–"}</td><td>${Object.entries(obj(r.subs)).map(([k, v]) => `<span class="chip">${esc(k)} ${sgn(v)}</span>`).join("")}</td>
    <td>${Object.entries(obj(r.resting)).map(([k, v]) => `<span class="chip">${esc(k)} ${esc(v)}</span>`).join("")}</td><td class="mute">${esc(r.last_fill || "")}</td></tr>
    ${ui.open === r.ex ? `<tr><td colspan="8">${detail(r)}</td></tr>` : ""}`).join("");
  return `<div class="row"><span class="mute">${rows.length} markets · click a row to pause it, exclude strategies, or see its orders and fills</span><span class="spacer"></span>
    <button class="btn" id="prev">‹</button> ${ui.page + 1}/${pages} <button class="btn" id="next">›</button></div>
    <div class="wrap"><table><thead><tr><th>Market</th><th class="n">PM fair ±u¢</th><th class="n">SIG bid / ask</th><th class="n">SIG mid − fair</th><th class="n">Net YES</th><th>Sub-books</th><th>Our resting</th><th>Last fill</th></tr></thead>
    <tbody>${body || '<tr><td colspan="8" class="mute">no markets</td></tr>'}</tbody></table></div>`;
}

function detail(r) {
  const d = ui.detail && ui.detail.ex === r.ex ? ui.detail : null;
  const ctl = `<div class="row ctl"><button class="btn" data-confirm="1" data-act="mpause" data-ex="${esc(r.ex)}">${r.paused ? "Resume market" : "Pause market"}</button>
    exclude from: ${["recycler", "mm", "ladder", "pairs", "sniper"].map(l => `<label><input type="checkbox" data-excl="${l}" ${arr(r.exclude).includes(l) ? "checked" : ""}> ${l}</label>`).join(" ")}
    <button class="btn" data-confirm="1" data-act="mexcl" data-ex="${esc(r.ex)}">Apply exclusions</button></div>`;
  if (!d) return `<div class="drawer">${ctl}<div class="mute">${LIVE ? "loading history…" : "history is only available on the live page"}</div></div>`;
  const ord = arr(d.orders).map(o => `<tr><td class="mute">${esc(o.placed)}</td><td>${esc(o.bot)}</td><td>${esc(o.layer)}</td><td>${esc(o.action)} ${esc(o.side)}</td><td class="n">${px(o.price)}</td><td class="n">${num(o.qty)}</td><td class="n">${num(o.filled)}</td><td>${esc(o.status)}</td></tr>`).join("");
  const fl = arr(d.fills).map(f => `<tr><td class="mute">${esc(f.t)}</td><td>${esc(f.bot)}</td><td>${esc(f.layer)}</td><td>${esc(f.action)} ${esc(f.side)}</td><td class="n">${px(f.price)}</td><td class="n">${num(f.qty)}</td><td class="n">${c2(f.edge)}</td><td class="n">${c2(f.mo30)}</td></tr>`).join("");
  return `<div class="drawer">${ctl}<h3>Orders</h3><div class="wrap"><table><thead><tr><th>Placed</th><th>Bot</th><th>Layer</th><th>Order</th><th class="n">Price</th><th class="n">Qty</th><th class="n">Filled</th><th>Status</th></tr></thead><tbody>${ord || '<tr><td colspan="8" class="mute">none</td></tr>'}</tbody></table></div>
    <h3 style="margin-top:10px">Fills</h3><div class="wrap"><table><thead><tr><th>Time</th><th>Bot</th><th>Layer</th><th>Trade</th><th class="n">Price</th><th class="n">Qty</th><th class="n">Edge vs fair</th><th class="n">Mark-out 30m</th></tr></thead><tbody>${fl || '<tr><td colspan="8" class="mute">none</td></tr>'}</tbody></table></div></div>`;
}

// ------------------------------------------------------------------------------------------- v4: one clock, six tabs
const SD = () => obj(S.sigday), ST = () => obj(S.status);
const since17 = () => { const s = SD(); return s.since_str ? `since ${esc(String(s.since_str).slice(11, 16))} PT (${esc(s.label || "")})` : "since 17:00 PT"; };
const tile = (lab, big, line, cl) => `<div class="card tile"><div class="lab">${lab}</div><div class="v2 ${cl || ""}">${big}</div><div class="sub">${line}</div></div>`;
const chipK = (k, t) => `<span class="chip ${k === "trade" ? "k-trade" : k === "alert" ? "k-alert" : "k-market"}">${esc(t || k)}</span>`;
const statusChip = s => `<span class="chip st-${esc(String(s || "").toLowerCase())}">${esc(s || "–")}</span>`;

function statusbar() {
  const st = ST(), o = obj(S.overview);
  if (S.error) return `<span class="dot bad"></span><b>ERROR</b> · ${esc(S.error)}`;
  if (S.stopped) return `<span class="dot bad"></span><b>STOPPED</b> · the engine is stopped: nothing is trading`;
  if (!st.kind) return `<span class="dot ${LIVE && ui.connected ? "ok" : ""}"></span>${LIVE ? (ui.connected ? "live" : "reconnecting") : "snapshot"} · ${esc(S.t || "")} · loop ${num(S.loop)}`;
  const dot = st.kind === "TRADING" ? "ok" : st.kind === "PAUSED" ? "bad" : "warn";
  return `<span class="dot ${dot}"></span><b class="${dot === "ok" ? "good" : dot === "bad" ? "bad" : "warn"}">${esc(st.kind)}</b> · ${esc(st.text)}${LIVE && !ui.connected ? " · <span class='bad'>reconnecting</span>" : ""}${o.read_only ? ' · <span class="chip pilot">read-only</span>' : ""}`;
}

// ---- Live
function liveTiles() {
  const m = obj(S.money), s = SD(), L = obj(S.leaders4), day = obj(L.daily), to = obj(m.to_rank);
  const acct = val(m.account_sig) ?? val(obj(S.overview).leaderboard), start = val(m.start) ?? 100000;
  if (acct == null && s.today == null) return na("The engine's money view is not available yet.");
  const fa = val(obj(m.fair).account) ?? val(s.fair_account);
  const rank = val(m.rank) ?? val(obj(L.ours).rank), nt = val(m.n_traders), drank = val(day.my_rank);
  const need100 = val(to["100"]);
  return `<div class="tiles t4">
    ${tile("Leaderboard value (SIG prices)", num(acct), `<b class="${cls(acct - start)}">${sgn(acct - start)}</b> since start${rank == null ? "" : ` · rank <b>#${num(rank)}</b>${nt ? ` of ${num(nt)}` : ""}`}`)}
    ${tile("Expected at settlement (Polymarket fair)", fa == null ? "–" : num(fa), fa == null ? "not available yet" : `<b class="${cls(fa - acct)}">${sgn(fa - acct)}</b> ${fa >= acct ? "above" : "below"} the leaderboard value`)}
    ${tile(`Today <span class="mute" style="font-weight:400">(SIG day, ${since17()})</span>`, s.today == null ? "–" : sgn(s.today), s.today == null ? "no sample at 17:00 PT yet" : `our trades <b class="${cls(s.our_trades)}">${sgn(s.our_trades)}</b> · market drift <b class="${cls(s.drift)}">${sgn(s.drift)}</b>`, cls(s.today))}
    ${tile("Rank", rank == null ? "–" : `#${num(rank)}`, `${drank == null ? "today: no daily board yet" : `today <b>#${num(drank)}</b>`}${need100 == null ? "" : ` · #100 needs <b>${sgn(need100)}</b>`}`)}
  </div>`;
}

function daily4(id = "daily4") {
  const rows = arr(S.daily4).filter(r => r && typeof r === "object");
  if (!rows.length) return na("Daily P&L by SIG day is not available yet.");
  const keys = [...new Set(rows.flatMap(r => Object.keys(obj(r.strategies))))];
  const order = ["dump", "mm", "sniper", "fatfinger", "longshot", "pairs", "recycler", "release", "position"];
  keys.sort((a, b) => (order.indexOf(a) + 1 || 99) - (order.indexOf(b) + 1 || 99));
  let lo = 0, hi = 0;
  for (const r of rows) {
    let p = 0, n = 0;
    for (const v of [...keys.map(k => val(obj(r.strategies)[k])), val(r.drift)]) if (v != null) { if (v >= 0) p += v; else n += v; }
    lo = Math.min(lo, n, val(r.total) ?? 0); hi = Math.max(hi, p, val(r.total) ?? 0);
  }
  [lo, hi] = pad(lo, hi, 0.14);
  const W = width(id), H = 240, Lm = 50, R = 14, T = 14, B = 38, tk = ticks(lo, hi, 5);
  const Y = v => T + (tk.hi - v) / (tk.hi - tk.lo) * (H - T - B);
  const n = rows.length, band = (W - Lm - R) / Math.max(n, 6), off = Lm + (W - Lm - R - n * band) / 2, bw = Math.min(30, band * 0.55);
  let g = tk.ticks.map(v => `<line x1="${Lm}" x2="${W - R}" y1="${f1(Y(v))}" y2="${f1(Y(v))}" class="${v === 0 ? "zl" : "gl"}"/><text x="${Lm - 7}" y="${f1(Y(v)) + 4}" text-anchor="end">${kf(v, tk.step)}</text>`).join("");
  rows.forEach((r, i) => {
    const bx = off + i * band + (band - bw) / 2, cx = bx + bw / 2;
    const parts = [...keys.map(k => [val(obj(r.strategies)[k]), colorOf(k), nameOf(k)]), [val(r.drift), "var(--neutral)", "Market drift"]];
    let pb = 0, nb = 0;
    for (const [v, c] of parts) {
      if (v == null || v === 0) continue;
      const a = v > 0 ? pb : nb, b = a + v; if (v > 0) pb = b; else nb = b;
      let y0 = Y(a); const y1 = Y(b); if (a !== 0) y0 += v > 0 ? -1.5 : 1.5;
      if (Math.abs(y1 - y0) >= 0.5 && Math.sign(y0 - y1) === Math.sign(v)) g += `<path d="${barPath(bx, bw, y0, y1, false)}" fill="${c}"/>`;
    }
    const tot = val(r.total);
    if (tot != null) {
      const yt = Y(tot);
      g += `<line x1="${f1(bx - 5)}" x2="${f1(bx + bw + 5)}" y1="${f1(yt)}" y2="${f1(yt)}" class="tot"/>`;
      g += `<text x="${f1(cx)}" y="${f1(tot >= 0 ? Math.min(Y(pb), yt) - 7 : Math.max(Y(nb), yt) + 15)}" text-anchor="middle" class="vl">${sgn(tot)}</text>`;
    }
    g += `<text x="${f1(cx)}" y="${H - B + 17}" text-anchor="middle" class="lbl">${esc(r.label)}</text>`;
    if (r.partial) g += `<text x="${f1(cx)}" y="${H - B + 31}" text-anchor="middle">so far</text>`;
    const tip = `<b>${esc(r.label)} (SIG day${r.partial ? ", so far" : ""})</b>` + parts.filter(p => p[0] != null && p[0] !== 0).map(p => `<div>${sw(p[1])}${esc(p[2])} ${sgn(p[0])}</div>`).join("") + `<div><span class="tk"></span>Day total <b>${sgn(tot)}</b></div>`;
    g += `<rect x="${f1(off + i * band)}" y="${T}" width="${f1(band)}" height="${H - T - B}" fill="transparent"${tipAttr(tip)}/>`;
  });
  return legend([...keys.map(k => [sw(colorOf(k)), esc(nameOf(k))]), [sw("var(--neutral)"), "Market drift"], ['<span class="tk"></span>', "Day total"]])
    + `<svg width="${W}" height="${H}" viewBox="0 0 ${W} ${H}" role="img" aria-label="Daily P&L by SIG day">${g}</svg>`;
}

function feedopts() {
  return `<div class="seg" role="group" aria-label="feed filter">${[["", "all"], ["trade", "trades"], ["alert", "alerts"], ["market", "market"]].map(([k, t]) =>
    `<button data-feed="${k}" class="${ui.feed === k ? "on" : ""}">${t}</button>`).join("")}</div>`;
}
function feed() {
  if (!Array.isArray(S.feed)) return na("The activity feed is not available yet.");
  const rows = S.feed.filter(a => a && typeof a === "object" && (!ui.feed || a.kind === ui.feed)).slice(0, 30);
  if (!rows.length) return `<div class="na">${S.feed.length ? "Nothing of that kind yet." : "Nothing yet."}</div>`;
  return `<div class="wrap"><table><tbody>${rows.map(a => `<tr data-id="${esc(a.id)}"><td class="mute" style="width:1%">${esc(when(a.t))}</td><td style="width:1%">${chipK(a.kind)}</td>
    <td class="txt">${a.layer ? `<i class="ld" style="background:${colorOf(a.layer)}"></i>` : ""}${esc(a.text)}</td></tr>`).join("")}</tbody></table></div>`;
}

// ---- Strategies
function strat4() {
  const rows = arr(S.strategies4).filter(r => r && typeof r === "object"), s = SD();
  if (!rows.length) return na("Strategies are not available yet.");
  const tot = k => rows.reduce((a, r) => a + (val(r[k]) || 0), 0);
  const body = rows.map(r => `<tr class="click" data-skey="${esc(r.key)}"><td>${tag(r.key)}<div class="mute" style="font-size:12px;white-space:normal">${esc(r.what || "")}</div></td>
    <td><span title="${esc(r.why || "")}">${statusChip(r.status)}</span></td><td class="n">${num(r.resting)}</td><td class="n">${num(r.cash_in_use)}</td><td class="n">${num(r.trades_today)}</td>
    <td class="n ${cls(r.pnl_today)}">${sgn(r.pnl_today)}</td><td class="n ${cls(r.pnl_since)}">${sgn(r.pnl_since)}</td></tr>
    ${ui.sopen === r.key ? `<tr><td colspan="7"><div class="drawer"><b>Last 10 trades</b>${arr(r.last).length ? `<table><tbody>${arr(r.last).map(x => `<tr><td class="mute" style="width:1%">${esc(when(x.t))}</td><td class="txt">${esc(x.text)}</td></tr>`).join("")}</tbody></table>` : '<div class="mute">none yet</div>'}
      <div class="mute" style="margin-top:6px">Rule: ${esc(r.what || "")}</div></div></td></tr>` : ""}`).join("");
  const drift = val(s.drift), today = val(s.today);
  return `<div class="wrap"><table><thead><tr><th>Strategy</th><th>Status</th><th class="n">Orders resting</th><th class="n">Cash in use</th><th class="n">Trades today</th>
    <th class="n">P&L today</th><th class="n">P&L since Oct 1</th></tr></thead><tbody>${body}
    <tr><td><span class="tag"><i class="ld" style="background:var(--neutral)"></i>Market drift</span><div class="mute" style="font-size:12px">price change on holdings held at 17:00 PT</div></td><td></td><td></td><td></td><td></td><td class="n ${cls(drift)}">${sgn(drift)}</td><td></td></tr></tbody>
    <tfoot><tr><td>Total <span class="mute">(= today's P&L)</span></td><td></td><td class="n">${num(tot("resting"))}</td><td class="n">${num(tot("cash_in_use"))}</td><td class="n">${num(tot("trades_today"))}</td>
    <td class="n ${cls(today)}">${today == null ? sgn(tot("pnl_today") + (drift || 0)) : sgn(today)}</td><td class="n ${cls(tot("pnl_since"))}">${sgn(tot("pnl_since"))}</td></tr></tfoot></table></div>
    <div class="mute" style="font-size:12px;margin-top:6px">Today = SIG day, ${since17()}. Σ strategies ${sgn(s.our_trades)} + drift ${sgn(drift)} = ${sgn(today)}${s.ok === false ? ` <span class="bad">(off by ${sgn(s.check)}: see Admin)</span>` : ""}.</div>`;
}

// ---- Capital
function spark(hist, big) {
  let pts = arr(hist).map(p => [val(p[0]), val(p[1])]).filter(p => p[0] != null && p[1] != null);
  if (big && ui.zoomWin) pts = pts.filter(p => p[0] >= ui.zoomWin[0] && p[0] <= ui.zoomWin[1]);
  else if (big && ui.range) pts = pts.filter(p => p[0] >= Date.now() / 1000 - ui.range);
  if (pts.length < 2) return `<div class="mute" style="font-size:12px">24 h sparkline: not enough samples yet</div>`;
  const W = big ? width("ovl") : 260, H = big ? 320 : 40, t0 = pts[0][0], span = Math.max(1, pts[pts.length - 1][0] - t0), hi = Math.max(1, ...pts.map(p => p[1]));
  const X = t => (t - t0) / span * (W - 2) + 1, Y = v => H - 2 - v / hi * (H - 4);
  return `<svg width="${W}" height="${H}" viewBox="0 0 ${W} ${H}" role="img" aria-label="capital use, 24 h"><line x1="1" x2="${W - 1}" y1="${f1(Y(0.4))}" y2="${f1(Y(0.4))}" class="gl"/><path d="${linePath(pts, X, Y)}" class="ln" stroke="var(--s1)"/></svg><div class="mute" style="font-size:11px">24 h · the line is the 40% floor of day 5 §3</div>`;
}
function cap4() {
  const c = S.capital4, o = obj(S.overview);
  if (!c || typeof c !== "object") return na("The capital view is not available yet.");
  const p = obj(c.party), capP = val(c.party_cap) || 40000, use = val(c.use);
  const tiles = `<div class="tiles t4">
    ${tile("Free cash <span class='mute' style='font-weight:400'>(above the 5k floor)</span>", num(c.free_cash), `cash ${num(c.cash)} · reserve ${num(obj(S.capital).reserve)}`)}
    ${tile("In use", num(c.in_use), `dump tranches ${num(c.tranches)} · scalper ${num(c.scalper)}${val(c.scalp_slice) ? ` of ${num(c.scalp_slice)}` : ""} · takes ${num(c.takes)}${obj(c.idle_rule).scalp_bonus ? ` · idle rule +${num(obj(c.idle_rule).scalp_bonus)}` : ""}${val(obj(c.idle_rule).roc_until) > Date.now() / 1000 ? " · takes at ROC ≥ 8%" : ""}`)}
    ${tile("Capital use", use == null ? "–" : pct(use, 0), spark(c.use_hist) + `<button class="btn zoom-btn" data-zoom="spark" title="Expand" style="margin-top:4px">⤢</button>`, use != null && use < 0.4 ? "warn" : "")}
    ${tile("Party exposure", `<span style="font-size:22px">D ${num(p.D)} · R ${num(p.R)}</span>`, `held dump inventory at cost, caps ${num(capP)} each${(p.D >= capP || p.R >= capP) ? ' · <b class="bad">at cap</b>' : ""}`)}</div>`;
  const bk = arr(c.bar).filter(b => b && val(b.sig) > 0), tot = bk.reduce((a, b) => a + val(b.sig), 0) || 1;
  const col = {floor: "var(--neutral)", reserve: "var(--s4)", free: "var(--s3)", tranches: "var(--s9)", settle: "var(--s7)"};
  const bar = bk.length ? `<div class="sbar" role="img" aria-label="capital, to scale">${bk.map(b => `<i style="flex:${(val(b.sig) / tot * 100).toFixed(3)} 1 0;background:${col[b.key] || "var(--neutral)"}"${tipAttr(`<b>${esc(b.label)}</b><div>${num(b.sig)} (${pct(val(b.sig) / tot)})</div>`)}></i>`).join("")}</div>
    ${legend(bk.map(b => [sw(col[b.key] || "var(--neutral)"), `${esc(b.label)} ${num(b.sig)}`]))}` : na("Not available yet.");
  const r = obj(c.released);
  const br = obj(r.by_branch), brs = Object.keys(br).length ? " · " + Object.entries(br).map(([k, v]) => `${esc(k)} ${num(v)}`).join(", ") : "";
  const rel = val(r.sig) == null ? na("No release this SIG day.") : `<div class="sub">${num(r.fills)} fills · freed <b>${num(r.sig)}</b>${brs} · realized <b class="${cls(r.realized)}">${sgn(r.realized)}</b> vs entry · <b class="${cls(r.vs_fair)}">${sgn(r.vs_fair)}</b> vs fair (${pct(r.avg_cost)} average cost)
    ${obj(r.demand).constrained != null ? ` · dump catcher ${obj(r.demand).constrained ? `<b class="warn">cash-constrained</b> (needs ${num(obj(r.demand).need)}, funded ${num(obj(r.demand).funded)})` : "funded"}` : ""}</div>`;
  return `${tiles}<div class="card"><h3>Where the money is, to scale</h3>${bar}</div><div class="card"><h3>Release today <span class="mute" style="font-weight:400">(SIG day, since 17:00 PT; sold only at a profit, or to fund the dump catcher)</span></h3>${rel}</div>`;
}

// ---- Holdings
const H4 = [{k: "market", t: "Market & side"}, {k: "shares", t: "Shares", n: 1}, {k: "avg_cost", t: "Entry", n: 1}, {k: "sig_mark", t: "SIG price", n: 1}, {k: "fair", t: "Fair", n: 1},
  {k: "unreal_sig", t: "P&L at SIG", n: 1}, {k: "settle", t: "Expected at settlement", n: 1}, {k: "layer", t: "Owner"}, {k: "exit", t: "Exit order"}, {k: "age_h", t: "Age", n: 1}];
function hold4() {
  if (!Array.isArray(S.holdings4)) return Array.isArray(S.holdings) ? holdingsLegacy() : na("Holdings are not available yet.");
  const all = S.holdings4.filter(h => h && typeof h === "object");
  let rows = all.filter(h => ui.hf === "" || (ui.hf === "dump" && String(h.layer) === "dump") || (ui.hf === "settle" && h.layer !== "dump" && !h.exit) || (ui.hf === "noexit" && !h.exit));
  const {k, dir} = ui.hsort, numeric = (H4.find(c => c.k === k) || {}).n;
  const key = h => k === "exit" ? (h.exit ? 1 : 0) : k === "layer" ? nameOf(h.layer) : numeric ? val(h[k]) : (h[k] == null ? null : String(h[k]));
  rows.sort((a, b) => { const x = key(a), y = key(b); if (x == null || y == null) return x == null ? (y == null ? 0 : 1) : -1; return (typeof x === "number" ? x - y : String(x).localeCompare(String(y))) * dir; });
  const sum = kk => rows.reduce((a, h) => a + (val(h[kk]) || 0), 0);
  const head = H4.map(c => `<th class="sort${c.n ? " n" : ""}" data-sort="${c.k}">${c.t}${ui.hsort.k === c.k ? `<span class="ar">${ui.hsort.dir > 0 ? "▲" : "▼"}</span>` : ""}</th>`).join("");
  const shown = ui.hall ? rows : rows.slice(0, HOLD_FIRST);
  const body = shown.map(h => `<tr class="click" data-ex="${esc(h.ex)}"><td>${esc(h.market || h.ex)} <b>${esc(String(h.side || "").toUpperCase())}</b></td><td class="n">${num(h.shares)}</td><td class="n">${px(h.avg_cost)}</td><td class="n">${px(h.sig_mark)}</td>
    <td class="n">${px(h.fair)}</td><td class="n ${cls(h.unreal_sig)}">${sgn(h.unreal_sig)}</td><td class="n">${num(h.settle)}</td><td>${tag(h.layer)}</td>
    <td>${h.exit ? `<span class="chip k-trade">${num(obj(h.exit).qty)} @ ${px(obj(h.exit).px)}</span>` : '<span class="chip">none</span>'}</td><td class="n">${val(h.age_h) == null ? "–" : h.age_h < 48 ? num(h.age_h, 0) + " h" : num(h.age_h / 24, 0) + " d"}</td></tr>
    ${ui.open === h.ex ? `<tr><td colspan="${H4.length}">${detail(arr(S.markets).find(m => m && m.ex === h.ex) || {ex: h.ex})}</td></tr>` : ""}`).join("");
  return `<div class="row" style="margin-top:0"><div class="seg">${[["", "all"], ["dump", "dump tranches"], ["settle", "to settlement"], ["noexit", "no exit order"]].map(([kk, t]) => `<button data-hf="${kk}" class="${ui.hf === kk ? "on" : ""}">${t}</button>`).join("")}</div>
    <span class="mute">${rows.length} of ${all.length} · value <b>${num(sum("value_sig"))}</b> at SIG · P&L <b class="${cls(sum("unreal_sig"))}">${sgn(sum("unreal_sig"))}</b> · expected at settlement ${num(sum("settle"))}</span></div>
    <div class="wrap"><table><thead><tr>${head}</tr></thead><tbody>${body || `<tr><td colspan="${H4.length}" class="mute">none</td></tr>`}</tbody>
    <tfoot><tr><td>Total (${rows.length})</td><td class="n">${num(sum("shares"))}</td><td></td><td></td><td></td><td class="n ${cls(sum("unreal_sig"))}">${sgn(sum("unreal_sig"))}</td><td class="n">${num(sum("settle"))}</td><td colspan="3">value at SIG ${num(sum("value_sig"))}</td></tr></tfoot></table></div>
    ${rows.length > HOLD_FIRST ? `<button class="btn more-btn" data-more="hall">${ui.hall ? `Show the first ${HOLD_FIRST}` : `Show all ${rows.length}`}</button>` : ""}`;
}
function holdingsLegacy() { return holdings(); }

// ---- Leaders
function lead4() {
  const L = S.leaders4;
  if (!L || typeof L !== "object") return lead();
  const ov = arr(L.overall), me = obj(L.ours), ln = obj(L.lines), day = obj(L.daily), top = arr(day.top), mv = arr(L.movers);
  const gap = k => val(ln[k]) == null || val(me.pnl) == null ? "–" : sgn(ln[k] - me.pnl);
  const rowsO = ov.map(r => `<tr${r.ours ? ' style="font-weight:700"' : ""}><td class="n">${num(r.rank)}</td><td>${esc(r.name)}</td><td class="n ${cls(r.pnl)}">${sgn(r.pnl)}</td><td class="n">${num(r.trades)}</td></tr>`).join("");
  const lines = ["50", "100"].map(k => val(ln[k]) == null ? "" : `<tr class="mute"><td class="n">${k}</td><td>— line —</td><td class="n">${sgn(ln[k])}</td><td class="n">gap ${gap(k)}</td></tr>`).join("");
  const mine = me.rank == null ? "" : `<tr style="font-weight:700;background:var(--chip)"><td class="n">${num(me.rank)}</td><td>${esc(me.name)} (us)</td><td class="n ${cls(me.pnl)}">${sgn(me.pnl)}</td><td class="n">to #100 ${gap("100")} · to #50 ${gap("50")}</td></tr>`;
  const rowsD = top.map(r => `<tr><td class="n">${num(r.rank)}</td><td>${esc(r.name)}</td><td class="n ${cls(r.pnl)}">${sgn(r.pnl)}</td><td class="n">${num(r.trades)}</td></tr>`).join("");
  const mineD = day.my_rank == null ? "" : `<tr style="font-weight:700;background:var(--chip)"><td class="n">${num(day.my_rank)}</td><td>us</td><td class="n">${sgn(SD().today)}</td><td class="n">of ${num(day.total)}</td></tr>`;
  return `<div class="grid2"><div class="card"><h3>Overall</h3><div class="wrap"><table><thead><tr><th class="n">#</th><th>Account</th><th class="n">P&L</th><th class="n">Trades</th></tr></thead><tbody>${rowsO || '<tr><td colspan="4" class="mute">no sample yet</td></tr>'}${lines}${mine}</tbody></table></div></div>
    <div class="card"><h3>Today (SIG day, ${since17()})</h3><div class="wrap"><table><thead><tr><th class="n">#</th><th>Account</th><th class="n">P&L today</th><th class="n">Trades</th></tr></thead><tbody>${rowsD || '<tr><td colspan="4" class="mute">daily board not read yet</td></tr>'}${mineD}</tbody></table></div>${day.t ? `<div class="mute" style="font-size:12px">read ${esc(when(day.t))}</div>` : ""}</div></div>
    <div class="card" style="margin-top:12px"><h3>Who made money today, and on what <span class="mute" style="font-weight:400">(daily board every 5 min; a gain ≥ 1k between reads matched to the tape ±5 min)</span></h3>
      ${arr(L.jumps).length ? `<div class="wrap"><table><thead><tr><th>When</th><th>Account</th><th class="n">Daily #</th><th class="n">Gain</th><th class="n">Trades</th><th>Likely trade</th><th class="n">We caught</th><th>Why not</th></tr></thead><tbody>
      ${arr(L.jumps).map(j => (arr(j.likely).length ? arr(j.likely) : [null]).map(lk => `<tr><td class="mute">${esc(String(j.t || "").slice(11, 16))}</td><td>${esc(j.name)}</td><td class="n">${num(j.rank)}</td><td class="n good">${sgn(j.gain)}</td><td class="n">${num(j.trades)}</td>
        <td class="txt">${lk ? `${esc(lk.market)} · ${esc(lk.who || "")} · ${num(lk.shares)} sh at ${esc(lk.px)} · value ${num(lk.value)}` : '<span class="mute">no big tape event in the window (marks moving on a held book?)</span>'}</td>
        <td class="n">${lk ? num(lk.caught) : ""}</td><td class="txt">${lk ? esc(lk.why_not || "") : ""}</td></tr>`).join("")).join("")}</tbody></table></div>` : na("No jump ≥ 1k yet on today's board.")}</div>
    <div class="card" style="margin-top:12px"><h3>Last hour: who gained most</h3>${mv.length ? `<div class="wrap"><table><tbody>${mv.map(m => `<tr><td>#${num(m.rank)} ${esc(m.name)}</td><td class="n ${cls(m.d_pnl)}">${sgn(m.d_pnl)}</td><td class="mute">see the market events of the last hour in the Live feed</td></tr>`).join("")}</tbody></table></div>` : na("Not enough samples yet (needs two leaderboard reads an hour apart).")}</div>`;
}

// ---- Admin
function admin4() {
  const a = obj(S.admin4), h = obj(S.health), sig = obj(h.sig), ex = obj(h.executor), rc = obj(a.recon), cov = obj(a.coverage), rt = obj(a.ratchet), pt = obj(a.pt_day);
  const feeds = Object.entries(obj(h.feeds)).map(([k, t]) => `${esc(k)} ${ago(t)}`).join(" · ") || "–";
  const covs = Object.entries(cov).map(([k, v]) => `<div>${tag(k)} <b>${num(obj(v).covered)}</b> / ${num(obj(v).eligible)}</div>`).join("") || '<div class="mute">–</div>';
  const rat = Object.entries(rt).map(([k, v]) => `<div>${tag(k)} level <b>${num(obj(v).level)}</b> (x${(Math.pow(1.25, val(obj(v).level) || 0)).toFixed(2)})${obj(v).hold_until > Date.now() / 1000 ? ' · <span class="warn">holding</span>' : ""}</div>`).join("") || '<div class="mute">no ratchet yet</div>';
  return `<div class="grid">
    <div class="card"><h3>Health</h3><div>reads <b>${num(sig.reads_last_min)}</b>/${num(sig.read_limit)} · writes <b>${num(sig.writes_last_min)}</b>/${num(sig.write_limit)} per min</div>
      <div>429s (10 min) <b class="${val(sig.throttled) > 5 ? "bad" : ""}">${num(sig.throttled)}</b> · reads failing ${pct(obj(sig.read).bad_rate, 0)} · writes failing ${pct(obj(sig.write).bad_rate, 0)}</div>
      <div>loop <b>${num(h.loop_s, 1)} s</b> · errors last hour <b class="${h.errors ? "bad" : ""}">${num(h.errors)}</b> · chart samples dropped as glitches <b>${num(ui.dropped || 0)}</b></div><div class="mute" style="font-size:12px">feeds: ${feeds}</div></div>
    <div class="card"><h3>Audits</h3><div>I2 trims <b>${esc(JSON.stringify(a.trims || {}))}</b></div><div>foreign orders <b class="${a.foreign ? "bad" : ""}">${num(a.foreign)}</b> · duplicates <b class="${a.duplicates ? "bad" : ""}">${num(a.duplicates)}</b> · UNKNOWN writes <b class="${a.unknown ? "bad" : ""}">${num(a.unknown)}</b></div>
      <div class="${rc.ok === false ? "nok" : "ok"}">${rc.ok === false ? "⚠" : "✓"} today ${sgn(rc.today)} = Σ strategies ${sgn(rc.sum)} + drift ${sgn(rc.drift)}${rc.check == null ? "" : ` · drift from the 17:00 snapshot ${sgn(rc.estimate)} (off by ${sgn(rc.check)})`}</div>
      ${rc.note ? `<div class="mute" style="font-size:12px">${esc(rc.note)}</div>` : ""}</div>
    <div class="card"><h3>Coverage (sides with our bids / eligible)</h3>${covs}</div>
    <div class="card"><h3>Size ratchet (day 5 §1.7)</h3>${rat}<div class="mute" style="font-size:12px">gates held: ${a.hold_gates ? "yes" : "no"} · free to promote: ${esc(arr(a.gate_free).join(", ") || "none")} · fills from order records: ${esc(a.truth_mode || "")}</div></div>
    <div class="card"><h3>PT calendar day <span class="mute" style="font-weight:400">(labelled; everywhere else "today" is the SIG day)</span></h3><div>since 00:00 PT: <b class="${cls(pt.pnl)}">${sgn(pt.pnl)}</b> = realized ${sgn(pt.realized)} + unrealized ${sgn(pt.unrealized_change)}</div></div>
    <div class="card"><h3>Decisions (manual only)</h3>${arr(a.decisions).length ? arr(a.decisions).map(d => `<div>· ${esc(typeof d === "string" ? d : JSON.stringify(d))}</div>`).join("") : '<div class="mute">none</div>'}</div>
    <div class="card"><h3>Release and intel</h3><div>running <b>${esc(a.release || ST().release || "–")}</b></div><div class="mute" style="font-size:12px">deploy log: docs/RUN_LOG.md · intel: data/intel/report.md · impact: docs/impact_P1_P2.md</div></div></div>`;
}

//__JS5__
const zb = id => `<span class="spacer"></span><button class="btn zoom-btn" data-zoom="${id}" title="Expand">⤢</button>`;
const SKEL = {
  overview: () => `<section class="sec5"><div id="b-ovHero"></div><div id="b-ovBand"></div></section>
    <section class="sec5 g2"><div class="p5"><div class="h5"><h3>Daily P&L, SIG days</h3><div class="seg5" id="b-ovBasis"></div></div><div id="b-ovDays"></div></div>
      <div class="p5"><div class="h5"><h3>Account value, SIG and fair</h3></div><div id="b-ovAcct"></div></div></section>
    <section class="sec5 g2"><div class="p5"><div class="h5"><h3>Ranking</h3></div><div id="b-ovRank"></div></div><div class="p5"><div class="h5"><h3>Our rank over time</h3></div><div id="b-ovRankC"></div></div></section>
    <section class="sec5 p5"><div id="b-ovPort"></div></section>`,
  live: () => `<section class="sec5"><div id="b-lvStrip"></div></section><section class="sec5"><div id="b-lvPipe"></div></section>
    <section class="sec5 p5"><div class="h5"><h3>Live activity</h3></div><div id="b-lvAct"></div></section>`,
  markets: () => `<section class="sec5 p5"><div id="b-mkHead"></div><div id="b-mkTable"></div></section>`,
  leaders: () => `<section><div id="b-lead4"></div></section>`,
  admin: () => `<section class="sec5 p5"><div id="b-adCtl"></div></section><section class="sec5 p5"><div id="b-adHealth"></div></section>
    <section class="sec5 p5"><div id="b-adAlerts"></div></section><section class="sec5 p5"><div id="b-adAdv"></div></section>`,
};
const BLOCKS = {
  overview: {ovHero: ov_hero, ovBand: ov_band, ovBasis: () => `<button data-basis5="board" class="${ui.v5.basis === "board" ? "on" : ""}">board</button><button data-basis5="fair" class="${ui.v5.basis === "fair" ? "on" : ""}">at fair</button>`,
             ovDays: ov_days, ovAcct: ov_acct, ovRank: ov_rank, ovRankC: ov_rankchart, ovPort: ov_port},
  live: {lvStrip: lv_strip, lvPipe: lv_pipe, lvAct: lv_act},
  markets: {mkHead: mk_head, mkTable: mk_table},
  leaders: {lead4},
  admin: {adCtl: ad_controls, adHealth: ad_health, adAlerts: ad_alerts, adAdv: ad_adv},
};
const OPTIONAL = new Set([]);

function block(id, fn) {
  try { return fn(); }
  catch (e) { ui.errors.push(id + ": " + (e && e.stack || e)); ui.errors = ui.errors.slice(-20); return `<div class="na">This part could not be drawn (${esc(e && e.message)}).</div>`; }
}
function put(id, html) {
  if (ui.cache[id] === html) return;
  const el = document.getElementById("b-" + id); if (!el) return;
  ui.cache[id] = html;
  const w = el.querySelector && el.querySelector(".wrap"), sl = w ? w.scrollLeft : 0;
  const a = document.activeElement, aid = a && a.id && el.contains && el.contains(a) ? a.id : null;
  el.innerHTML = html;
  if (sl) { const w2 = el.querySelector(".wrap"); if (w2) w2.scrollLeft = sl; }
  if (aid) { const e2 = document.getElementById(aid); if (e2 && e2.focus) e2.focus(); }
}
function setHTML(id, html) { const el = document.getElementById(id); if (el && el._h !== html) { el._h = html; el.innerHTML = html; } }
function editing() {
  const a = document.activeElement;
  return a && a.closest && a.closest("#main") && /^(INPUT|SELECT)$/.test(a.tagName) && !FILTERS.has(a.id);
}
document.addEventListener("focusout", () => setTimeout(() => { if (ui.dirty && !editing()) { ui.dirty = false; draw(); } }, 0));

function draw() {
  const o = obj(S.overview);
  document.body.classList.toggle("ro", !LIVE || !!o.read_only);
  setHTML("tabs", Object.entries(TABS).map(([k, v]) => `<button data-tab="${k}" class="${k === ui.tab ? "on" : ""}">${v}</button>`).join(""));
  setHTML("live", S.v5hdr ? hdr5() : statusbar());
  setHTML("themeBtn", {auto: "Theme: auto", light: "Theme: light", dark: "Theme: dark"}[ui.theme] || "Theme");
  if (ui.shown !== ui.tab) { document.getElementById("main").innerHTML = SKEL[ui.tab](); ui.shown = ui.tab; ui.cache = {}; }
  for (const [id, fn] of Object.entries(BLOCKS[ui.tab])) put(id, block(id, fn));
  setHTML("side5", ui.tab === "live" ? block("side", lv_side) : ui.tab === "markets" ? block("mkp", mk_panel) : "");
  setHTML("modal5", confirm5());
  drawOverlay();
}
const ZOOM = {daily4: ["Daily P&L by SIG day", () => daily4("ovl"), false], daily4b: ["P&L by strategy per SIG day", () => daily4("ovl"), false],
              account: ["Account value over time", () => accountChart("ovl"), true], spark: ["Capital use, 24 h", () => spark(obj(S.capital4).use_hist, true), true]};
const RANGES = [["6h", 6 * 3600], ["24h", 86400], ["3d", 3 * 86400], ["all", 0]];
function drawOverlay() {
  const ov = document.getElementById("overlay"); if (!ov) return;
  if (!ui.zoomId || !ZOOM[ui.zoomId]) { ov.style.display = "none"; ov.innerHTML = ""; ui.cache.ovl = null; return; }
  const [title, fn, timed] = ZOOM[ui.zoomId];
  ov.style.display = "block";
  const head = `<div class="ovl-head"><h3>${esc(title)}</h3>${timed ? `<div class="seg">${RANGES.map(([k, s]) => `<button data-range="${s}" class="${(ui.range || 0) === s && !ui.zoomWin ? "on" : ""}">${k}</button>`).join("")}</div>` : ""}
    <span class="mute" style="font-size:12px">${timed ? "drag to zoom · double-click resets · " : ""}Esc closes</span><span class="spacer"></span><button class="btn" data-zoomclose="1">✕</button></div>`;
  const html = head + `<div id="b-ovl" class="chart"></div>`;
  if (ov._h !== html) { ov._h = html; ov.innerHTML = html; }
  put("ovl", block("ovl", fn));
}

function apply(msg, kind) {
  if (kind === "snapshot") S = obj(msg);
  else for (const [k, v] of Object.entries(obj(msg))) {
    if ((k === "markets" || k === "holdings" || k === "holdings4" || k === "v5mk_rows") && v && !Array.isArray(v) && Array.isArray(v.rows)) {
      const by = new Map(arr(S[k]).map(r => [r && r.ex, r]));
      for (const r of v.rows) by.set(r && r.ex, r);
      for (const ex of arr(v.gone)) by.delete(ex);
      S[k] = [...by.values()];
    } else if ((k === "events" || k === "activity" || k === "feed") && Array.isArray(v) && Array.isArray(S[k])) {
      const have = new Set(S[k].map(e => e && e.id));
      S[k] = [...v.filter(e => !have.has(e && e.id)), ...S[k]].slice(0, 300);
    } else S[k] = v;
  }
  ui.updated = Date.now();
  if (editing()) { ui.dirty = true; return; }
  draw();
}

// ------------------------------------------------------------------------------------------- tooltips + crosshair
function showTip(html, e) {
  const t = document.getElementById("tip"); if (!t) return;
  if (t._h !== html) { t._h = html; t.innerHTML = html; }
  t.style.display = "block";
  const w = t.offsetWidth || 200, h = t.offsetHeight || 60, vw = window.innerWidth, vh = window.innerHeight;
  t.style.left = Math.max(8, Math.min(e.clientX + 14, vw - w - 8)) + "px";
  t.style.top = (e.clientY + 16 + h > vh ? Math.max(8, e.clientY - h - 12) : e.clientY + 16) + "px";
}
function hideTip() {
  const t = document.getElementById("tip"); if (t && t.style.display !== "none") t.style.display = "none";
  for (const C of Object.values(CH)) { const x = document.getElementById("xh-" + C.id); if (x) x.setAttribute("display", "none"); C.series.forEach((s, i) => { const d = document.getElementById(`xh-${C.id}-${i}`); if (d) d.setAttribute("display", "none"); }); }
}
function lineHover(el, e) {
  const C = CH[el.getAttribute("data-chart")], svg = el.ownerSVGElement; if (!C || !svg) return hideTip();
  const r = svg.getBoundingClientRect(), t = C.t0 + (e.clientX - r.left - C.L) / (C.W - C.L - C.R) * (C.t1 - C.t0), lines = [];
  C.series.forEach((s, i) => {
    const j = nearest(s.t, t), d = document.getElementById(`xh-${C.id}-${i}`);
    if (j < 0 || Math.abs(s.t[j] - t) > s.gap || s.v[j] == null) { if (d) d.setAttribute("display", "none"); return; }
    if (d) { d.setAttribute("cx", f1(C.X(s.t[j]))); d.setAttribute("cy", f1(s.Y(s.v[j]))); d.removeAttribute("display"); }
    lines.push(`<div>${sw(s.color)}${esc(s.name)} <b>${s.fmt(s.v[j])}</b></div>`);
  });
  const x = document.getElementById("xh-" + C.id), cx = f1(C.X(Math.max(C.t0, Math.min(C.t1, t))));
  if (x) { x.setAttribute("x1", cx); x.setAttribute("x2", cx); x.removeAttribute("display"); }
  showTip(`<div class="mute">${esc(md(t) + " " + hm(t))} PT</div>` + (lines.join("") || '<div class="mute">no sample here</div>'), e);
}
document.addEventListener("mousemove", e => {
  const t = e.target; if (!t || !t.closest) return;
  const a = t.closest("[data-tip]"); if (a) { hideTip(); return showTip(a.getAttribute("data-tip"), e); }
  const c = t.closest("[data-chart]"); if (c) return lineHover(c, e);
  hideTip();
});

// ------------------------------------------------------------------------------------------- controls + interactions
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

function setTheme(t) {
  ui.theme = t; const r = document.documentElement;
  if (r && r.setAttribute) { if (t === "auto") r.removeAttribute("data-theme"); else r.setAttribute("data-theme", t); }
  try { localStorage.setItem("sig-dash-theme", t); } catch (e) { }
}
try { const t = localStorage.getItem("sig-dash-theme"); if (t === "light" || t === "dark") setTheme(t); } catch (e) { }
try { const h = new URLSearchParams(String(location.hash || "").replace(/^#/, "")); if (h.get("tab") && TABS[h.get("tab")]) ui.tab = h.get("tab"); if (h.get("theme")) setTheme(h.get("theme")); } catch (e) { }

document.addEventListener("click", e => {
  const t = e.target; if (!t || !t.closest) return;
  if (typeof v5click === "function" && v5click(e)) return;
  const th = t.closest("th[data-sort]");
  if (th) { const k = th.dataset.sort, c = (ui.tab === "holdings" ? H4 : HCOLS).find(c => c.k === k) || {}; ui.hsort = {k, dir: ui.hsort.k === k ? -ui.hsort.dir : c.n ? -1 : 1}; draw(); return; }
  const zm = t.closest("[data-zoom]"); if (zm) { ui.zoomId = zm.dataset.zoom; ui.zoomWin = null; draw(); return; }
  const zc = t.closest("[data-zoomclose]"); if (zc) { ui.zoomId = null; ui.zoomWin = null; draw(); return; }
  const rg = t.closest("[data-range]"); if (rg) { ui.range = +rg.dataset.range || 0; ui.zoomWin = null; draw(); return; }
  const fd = t.closest("[data-feed]"); if (fd) { ui.feed = fd.dataset.feed; draw(); return; }
  const hf = t.closest("[data-hf]"); if (hf) { ui.hf = hf.dataset.hf; ui.hall = false; draw(); return; }
  const sk = t.closest("tr.click[data-skey]"); if (sk) { if (!t.closest(".drawer")) { ui.sopen = ui.sopen === sk.dataset.skey ? null : sk.dataset.skey; draw(); } return; }
  const bs = t.closest("[data-basis]"); if (bs) { ui.basis = bs.dataset.basis; draw(); return; }
  const mo = t.closest("[data-more]"); if (mo) { ui[mo.dataset.more] = !ui[mo.dataset.more]; draw(); return; }
  const tp = t.closest("[data-tip]"); if (tp) showTip(tp.getAttribute("data-tip"), e);      // touch screens: tap for the tooltip
  const b = t.closest("button, tr.click"); if (!b) return;
  if (b.dataset.tab) { ui.tab = b.dataset.tab; ui.open = null; draw(); return; }
  if (b.id === "themeBtn") { setTheme({auto: "light", light: "dark", dark: "auto"}[ui.theme] || "auto"); draw(); return; }
  if (b.matches("tr.click")) { if (!t.closest(".drawer")) openMarket(b.dataset.ex); return; }
  if (b.id === "prev") { ui.page = Math.max(0, ui.page - 1); draw(); return; }
  if (b.id === "next") { ui.page++; draw(); return; }
  if (!armed(b)) return;
  const o = obj(S.overview);
  if (b.id === "pauseBtn") send(o.paused ? "resume" : "pause", {});
  else if (b.id === "pullBtn") send("pull_quotes", {}, true);
  else if (b.id === "kBtn") { const v = document.getElementById("kv").value; send("kappa", {value: v === "" ? null : parseFloat(v) / 100, minutes: parseFloat(document.getElementById("km").value || "60")}, true); }
  else if (b.id === "roBtn") send("read_only", {on: !o.read_only});
  else if (b.dataset.act === "state") send("layer_state", {layer: b.dataset.layer, state: document.querySelector(`select[data-state="${b.dataset.layer}"]`).value}, true);
  else if (b.dataset.act === "caps") { const caps = {}; document.querySelectorAll(`input[data-cap^="${b.dataset.layer}:"]`).forEach(i => caps[i.dataset.cap.split(":")[1]] = parseFloat(i.value)); send("layer_caps", {layer: b.dataset.layer, caps}, true); }
  else if (b.dataset.act === "mpause") { const r = arr(S.markets).find(m => m.ex === b.dataset.ex) || {}; send("market", {ex: b.dataset.ex, paused: !r.paused}, true); }
  else if (b.dataset.act === "mexcl") { const ex = []; b.closest(".drawer").querySelectorAll("input[data-excl]").forEach(i => { if (i.checked) ex.push(i.dataset.excl); }); send("market", {ex: b.dataset.ex, exclude: ex}, true); }
});
document.addEventListener("input", e => {
  const id = e.target.id;
  if (id === "q") { ui.q = e.target.value; ui.page = 0; draw(); }
  else if (id === "mis") { ui.mis = e.target.value; ui.page = 0; draw(); }
  else if (id === "evq") { ui.evq = e.target.value; draw(); }
  else if (id === "hq") { ui.hq = e.target.value; draw(); }
  else if (id === "aq") { ui.aq = e.target.value; draw(); }
});
document.addEventListener("change", e => {
  const id = e.target.id;
  if (id === "lf") { ui.layer = e.target.value; ui.page = 0; draw(); }
  else if (id === "hp") { ui.haspos = e.target.checked; ui.page = 0; draw(); }
  else if (id === "b-evkinds") { ui.ev = e.target.value; draw(); }
  else if (id === "b-hlopts") { ui.hl = e.target.value; draw(); }
  else if (id === "hr") { ui.hr = e.target.checked; draw(); }
  else if (id === "b-alopts") { ui.al = e.target.value; draw(); }
});
if (typeof window !== "undefined" && window.addEventListener) {
  let rz = 0; window.addEventListener("resize", () => { clearTimeout(rz); rz = setTimeout(draw, 150); });
  document.addEventListener("keydown", e => { if (e.key === "Escape" && ui.zoomId) { ui.zoomId = null; ui.zoomWin = null; draw(); } });
  let drag = null;
  const tAt = (el, x) => { const C = CH[el.getAttribute("data-chart")], r = el.ownerSVGElement.getBoundingClientRect(); return C ? C.t0 + (x - r.left - C.L) / (C.W - C.L - C.R) * (C.t1 - C.t0) : null; };
  document.addEventListener("mousedown", e => { const el = e.target.closest && e.target.closest("[data-drag]"); if (el && ui.zoomId) drag = {el, x0: e.clientX, t0: tAt(el, e.clientX)}; });
  document.addEventListener("mouseup", e => { if (!drag) return; const t1 = tAt(drag.el, e.clientX); if (t1 != null && Math.abs(e.clientX - drag.x0) > 8) { ui.zoomWin = [Math.min(drag.t0, t1), Math.max(drag.t0, t1)]; draw(); } drag = null; });
  document.addEventListener("dblclick", e => { if (e.target.closest && e.target.closest("[data-drag]")) { ui.zoomWin = null; ui.range = 0; draw(); } });
}

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
