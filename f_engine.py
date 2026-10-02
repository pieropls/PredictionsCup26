"""F. ENGINE: the loop. One process, one write budget, one bot per market and one per race.

    python3 -u f_engine.py              dry run: reads everything, sends nothing
    python3 -u f_engine.py --live       trade        (options: --loops N, --only position,market,pair)
    stop:  kill $(cat data/engine.pid)  pulls every market/pair order; position exits and manual orders stay

Every loop:
  F1  read      Polymarket fairs; SIG positions, open orders, best bid/ask, new fills, account
  F2  plan      every bot says what it wants resting (e_bots.py); account risk is reserved order by order
  F3  execute   cancel what nobody wants, place what's missing (position exits first, then by urgency and edge)
  F4  pairs     every PairBot screens its race; the best sets are sized from the books and sent atomically
  F5  record    data/ledger.json, data/state.json, data/dashboard.html, data/pnl.csv
"""
import csv
import fcntl
from concurrent.futures import ThreadPoolExecutor
import json
import math
import os
import signal
import subprocess
import sys
import threading
import time
import uuid
from datetime import datetime

import b_sig as api
import c_polymarket as pm
import e_bots as bots
import g_dashboard as dashboard
import i_realtime as rt
import j_executor as jx
from a_config import (ENGINE_HOST, LOOP_S, BATCH_SIZE, PM_MAX_AGE_S, MIN_QTY, TICK, INITIAL_BALANCE,
                      RISK_MAX_EXPOSURE, CASH_RESERVE, POSITION_BOTS, POSITION_MAX_ADD, MANUAL_MARKETS, MB_PM_MAX_SPREAD,
                      MB_FOCUS, MB_REFILL_S, MB_QUOTE_TTL_S, PAIR_ENABLED, PAIR_EDGE, PAIR_THIRD_PARTY, PAIR_MAX_QTY,
                      PAIR_MAX_SHOTS, PAIR_MIN_LOCK, PAIR_TTL_S, PAIR_COOLDOWN_S, DATA, POSITION_REF_FILE, PNL_FILE, PNL_EVERY_S,
                      STATE_FILE, DASHBOARD_FILE, PID_FILE, LOCK_FILE, OVERRIDES_FILE, NIGHT_LOG_FILE, HOST_ID_FILE,
                      DASHBOARD_URL_FILE, STATE_EVERY_S, LAYER_CAPS, KAPPA_FROM_OCT30, LOSS_STOP, MARKET_MAX_POS,
                      LADDER_LEVELS, LADDER_CAPACITY_SHARE, TAKE_NOTIONAL, REF_WINDOW_S, REF_MIN_SAMPLES, REF_MAX_SPREAD,
                      BREAKER_WINDOW_S, BREAKER_MARKETS, BREAKER_SHARE, BREAKER_PAUSE_S, MM_PILOT_MARKETS,
                      LADDER_PILOT_MARKETS, PILOT_S, REQUOTE_CANCELS_PER_MIN, BREAKER_FAIR_MOVE)
from d_ledger import Ledger, stamp, write_atomic


def read_json(path, default):
    if not os.path.exists(path):
        return default
    with open(path) as fh:
        return json.load(fh)


def arg(flag):
    return sys.argv[sys.argv.index(flag) + 1] if flag in sys.argv else None


def host_id():
    """This machine's stable name: macOS LocalHostName (never the network-derived hostname, which changes with the
    Wi-Fi), else the id in data/host_id (written once on a machine without scutil, e.g. a VM)."""
    try:
        r = subprocess.run(["scutil", "--get", "LocalHostName"], capture_output=True, text=True, timeout=5)
        if r.returncode == 0 and r.stdout.strip():
            return r.stdout.strip()
    except (OSError, subprocess.SubprocessError):
        pass
    if os.path.exists(HOST_ID_FILE):
        with open(HOST_ID_FILE) as fh:
            return fh.read().strip() or None
    return None


# ====================================================================================================== controls
LAYERS = ["position", "recycler", "mm", "ladder", "pairs", "sniper", "leaders"]
LAYER_STATES = ("off", "pilot", "full")


class Controls:
    """data/overrides.json: every flag and control setting, so a restart keeps the current stage. Edited by the engine
    (dashboard controls, automatic promotions and kills) or by hand on disk (re-read when its mtime changes).
      layers   {name: {state: off|pilot|full, since, caps: {...}, killed: {reason, at} | null}}
      paused   global "pause new risk" (exits, rebounds, unwinds and repairs keep running)
      kappa    {value, until} override of the capital discount, or null
      markets  {ex: {paused: bool, exclude: [layer, ...]}}
    Every change is an event, a line in docs/NIGHT_LOG.md, and is persisted."""

    def __init__(self, path, log=print, night=None, clock=time.time):
        self.path, self.log, self.night, self.clock = path, log, night, clock
        self.lock = threading.RLock()
        self.mtime, self.d = None, None
        self.load()

    def defaults(self):
        return dict(paused=False, read_only=False, kappa=None, markets={}, decisions=[],
                    layers={l: dict(state="full" if l in ("position", "leaders") else "off", since=self.clock(), caps={},
                                    killed=None) for l in LAYERS})

    def load(self):
        with self.lock:
            d = read_json(self.path, None) if os.path.exists(self.path) else None
            base = self.defaults()
            if d:
                base.update({k: v for k, v in d.items() if k != "layers"})
                for l, v in (d.get("layers") or {}).items():
                    base["layers"].setdefault(l, dict(state="off", since=self.clock(), caps={}, killed=None)).update(v)
            self.d = base
            self.mtime = os.path.getmtime(self.path) if os.path.exists(self.path) else None

    def reload_if_changed(self):
        m = os.path.getmtime(self.path) if os.path.exists(self.path) else None
        if m != self.mtime:
            self.load()
            self.log("CONTROL", "overrides.json changed on disk: reloaded")
            return True
        return False

    def save(self):
        with self.lock:
            write_atomic(self.path, json.dumps(self.d, indent=1))
            self.mtime = os.path.getmtime(self.path)

    def _note(self, text):
        self.log("CONTROL", text)
        if self.night:
            self.night.line(text)

    # ---- reads
    def layer(self, name):
        return self.d["layers"].get(name) or dict(state="off", caps={}, killed=None)

    def state(self, name):
        l = self.layer(name)
        return "off" if l.get("killed") else l.get("state", "off")

    def on(self, name):
        return self.state(name) != "off"

    @property
    def paused(self):
        return bool(self.d.get("paused"))

    def kappa_override(self):
        k = self.d.get("kappa")
        if k and k.get("until", 0) > self.clock():
            return float(k["value"])
        return None

    def market(self, ex):
        return self.d["markets"].get(str(ex)) or {}

    def allows(self, ex, layer):
        m = self.market(ex)
        return not m.get("paused") and layer not in (m.get("exclude") or [])

    # ---- changes (every one persisted and logged)
    def set_layer(self, name, state, reason, by="engine"):
        assert state in LAYER_STATES, state
        with self.lock:
            l = self.d["layers"].setdefault(name, dict(state="off", caps={}, killed=None))
            old = self.state(name)
            l.update(state=state, since=self.clock(), killed=None)
            self.save()
        self._note(f"layer {name}: {old} -> {state} ({by}: {reason})")

    def kill(self, name, reason):
        with self.lock:
            l = self.d["layers"].setdefault(name, dict(state="off", caps={}, killed=None))
            if l.get("killed"):
                return False
            l["killed"] = dict(reason=reason, at=self.clock())
            self.save()
        self._note(f"KILL layer {name}: {reason}")
        return True

    def set_caps(self, name, caps, by="dashboard"):
        with self.lock:
            self.d["layers"].setdefault(name, dict(state="off", caps={}, killed=None)).setdefault("caps", {}).update(caps)
            self.save()
        self._note(f"layer {name} caps {caps} ({by})")

    def set_paused(self, paused, reason, by="engine"):
        with self.lock:
            if bool(self.d.get("paused")) == bool(paused):
                return False
            self.d["paused"] = bool(paused)
            self.save()
        self._note(f"{'PAUSE new risk' if paused else 'RESUME new risk'} ({by}: {reason})")
        return True

    def set_kappa(self, value, minutes, by="dashboard"):
        with self.lock:
            self.d["kappa"] = None if value is None else dict(value=max(0.0, min(0.03, float(value))),
                                                              until=self.clock() + 60 * float(minutes))
            self.save()
        self._note(f"kappa override {self.d['kappa']} ({by})")

    def set_market(self, ex, by="dashboard", **kw):
        with self.lock:
            self.d["markets"].setdefault(str(ex), {}).update(kw)
            self.save()
        self._note(f"market {ex}: {kw} ({by})")

    def decide(self, text):
        """Something Piero must decide in the morning (shown in the NIGHT_LOG status block)."""
        with self.lock:
            if text not in self.d["decisions"]:
                self.d["decisions"].append(text)
                self.save()
                self._note(f"DECISION NEEDED: {text}")


class NightLog:
    """docs/NIGHT_LOG.md: an auto-maintained status block at the top (the morning report's five lines) and an
    append-only log below it (stage boundaries, flag changes, kills, and an entry every hour)."""
    START, END = "<!-- status:auto -->", "<!-- /status:auto -->"

    def __init__(self, path):
        self.path, self.lock = path, threading.Lock()

    def _read(self):
        if not os.path.exists(self.path):
            return f"# NIGHT_LOG\n\n{self.START}\n{self.END}\n\n## Log\n"
        with open(self.path) as fh:
            return fh.read()

    def line(self, text):
        with self.lock:
            body = self._read()
            write_atomic(self.path, body.rstrip("\n") + f"\n- {time.strftime('%H:%M')} {text}\n")

    def entry(self, title, lines):
        with self.lock:
            body = self._read()
            block = f"\n### {title}\n" + "".join(f"- {l}\n" for l in lines)
            write_atomic(self.path, body.rstrip("\n") + "\n" + block)

    def status(self, lines):
        with self.lock:
            body = self._read()
            new = self.START + "\n" + "".join(f"- {l}\n" for l in lines) + self.END
            if self.START in body and self.END in body:
                a, rest = body.split(self.START, 1)
                body = a + new + rest.split(self.END, 1)[1]
            else:
                body = body.replace("\n", "\n\n" + new + "\n", 1)
            write_atomic(self.path, body)


class Engine:
    def __init__(self, only=None):
        os.makedirs(DATA, exist_ok=True)
        api.init()
        sig = [m for m in api.tournament_markets() if m.get("exchanges")]
        self.meta = {}
        for m in sig:
            race, party = pm.race_party(m["title"]) or (m["title"], "?")
            self.meta[str(m["exchanges"][0]["id"])] = dict(mid=str(m["id"]), title=m["title"], race=race, party=party)
        by_mid = {v["mid"]: ex for ex, v in self.meta.items()}
        manual = {by_mid[str(i)] for i in MANUAL_MARKETS if str(i) in by_mid}
        held = api.positions()

        # position bots (ladder split over the size held when first seen: data/position_ref.json)
        refs = read_json(POSITION_REF_FILE, {})
        self.bots = []
        for name, cfg in POSITION_BOTS.items():
            ex = by_mid[str(cfg["market"])]
            if name not in refs:
                if abs(held.get(ex, 0)) < MIN_QTY:
                    continue
                refs[name] = dict(ref=int(abs(held[ex])), side="yes" if held[ex] > 0 else "no")
            self.bots.append(bots.PositionBot(name, ex, self.meta[ex], cfg["exits"], cfg["adds"], refs[name]["ref"],
                                              refs[name]["side"], POSITION_MAX_ADD))
        if not api.DRY_RUN:
            write_atomic(POSITION_REF_FILE, json.dumps(refs, indent=1))
        taken = {b.ex for b in self.bots} | manual

        # one market bot per market that has a Polymarket reference
        self.mp = pm.market_map(sig)
        self.bots += [bots.MarketBot(m["ex"], self.meta[m["ex"]]) for m in self.mp if m["ex"] in self.meta and m["ex"] not in taken]

        # one pair bot per race whose party markets are all free
        races = {}
        for ex, v in self.meta.items():
            races.setdefault(v["race"], []).append((v["party"], ex))
        self.pairs = [bots.PairBot(r, sorted(legs)) for r, legs in sorted(races.items())
                      if PAIR_ENABLED and len(legs) >= 2 and "?" not in dict(legs) and not {ex for _, ex in legs} & taken]

        if only:                                           # tests / dry runs; live flags come from data/overrides.json
            self.bots = [b for b in self.bots if b.kind in only or b.name in only]
            self.pairs = self.pairs if "pair" in only else []
        self.night = NightLog(NIGHT_LOG_FILE)
        self.controls = Controls(OVERRIDES_FILE, log=lambda k, t: self.event(k, t), night=self.night if not api.DRY_RUN else None)
        self.owner = {}
        for b in self.bots:
            if b.ex in self.owner:
                raise SystemExit(f"exchange {b.ex} has two owners: {self.owner[b.ex].name} and {b.name}")
            self.owner[b.ex] = b

        self.ledger = Ledger(persist=not api.DRY_RUN)
        self.journal = jx.Journal(os.path.join(DATA, "journal" if not api.DRY_RUN else "journal_dry"))
        self.ex = jx.Executor(api, self.ledger, self.journal, batch_size=BATCH_SIZE, min_qty=MIN_QTY,
                              log=lambda s: self.event(*s.strip().split(" ", 1)) if s.strip() else None)
        self.manual_ex = {b.ex for b in self.bots if b.kind == "position"} | manual
        self.last_state = None
        self.fair, self.fair_t, self.events, self.acct, self.book, self.px = {}, 0.0, [], {}, {}, {}
        self.open, self.n, self.pulled, self.summary = [], 0, False, ""
        self.focus, self.pnl_t, self.risk_view, self.t = set(), 0.0, {}, {}
        self.hour, self.started, self.errors = None, time.time(), []
        self.pm, self.px_t, self.state_t, self.alerts = {}, None, 0.0, []
        self.ft, self.mids, self.mids_t = pm.FairTracker(), read_json(os.path.join(DATA, "mids.json"), {}), 0.0
        self.breaker, self.bad_since, self.tranche_view, self.used, self.ladder_quota = {}, None, {}, {}, 0
        self.pilot_sets, self.sweep_rank = {}, {}
        self.load_pilots()
        self.leaders_t, self.scan_t, self.seen_markets, self.new_markets = 0.0, 0.0, set(), {}
        self.wake, self.rt_lock, self.rt_pending = threading.Event(), threading.Lock(), []   # realtime -> planner
        self.requotes = []                                  # times of MM/ladder requote cancels (A9 cap)
        self.sig_feed = self.pm_feed = None
        self.latency = {k: [] for k in ("fill_to_queued", "pm_to_defend", "plan_cycle")}
        self.position_names = {b.name for b in self.bots if b.kind == "position"}
        if self.ex.recovered:
            self.event("AUDIT", f"{self.ex.recovered} orders acked after the last ledger save recovered from the journal as ours")
        kinds = {k: sum(1 for b in self.bots if b.kind == k) for k in ("position", "market")}
        print(f"ROSTER {'LIVE' if not api.DRY_RUN else 'DRY RUN'}: {kinds['position']} position bots, {kinds['market']} market bots, "
              f"{len(self.pairs)} pair bots", flush=True)
        for b in self.bots:
            if b.kind == "position":
                print(f"  position {b.name:20} {b.market['title']}")

    # ------------------------------------------------------------------------------------------------ helpers
    def event(self, kind, text):
        self.event_id = getattr(self, "event_id", 0) + 1
        self.events = (self.events + [dict(id=self.event_id, t=time.strftime("%H:%M:%S"), kind=kind, text=text)])[-300:]
        if kind == "ERR":
            self.errors = [t for t in self.errors if t > time.time() - 3600] + [time.time()]
        print(f"  {kind:7}{text}", flush=True)

    def bot_of(self, ex):
        return self.owner[ex].name if ex in self.owner else "manual"

    def name(self, ex):
        m = self.meta.get(ex)
        return f"{m['race']} {m['party']}" if m else f"ex {ex}"

    def side_fair(self, ex, side):
        return None if ex not in self.fair else (self.fair[ex] if side == "yes" else 1 - self.fair[ex])

    def side_mid(self, ex, side):
        b, a, _ = self.px.get(ex, (None, None, None))
        if b is None or a is None:
            return None
        return (b + a) / 2 if side == "yes" else 1 - (b + a) / 2

    def risk(self, ex, q):
        """Exposure of a holding in SIG: at Polymarket fair, or at cost when there's no fair (never zero)."""
        if not q:
            return 0.0
        if ex in self.fair:
            return abs(bots.exposure(self.fair[ex], q))
        return abs(q) * (self.book.get(ex, {}).get("row") or {}).get("avgCost", 0.5)

    @staticmethod
    def adds(side, qty, price, pos):
        """SIG of NEW exposure a buy opens: the part beyond the shares it closes."""
        closes = max(-pos, 0) if side == "yes" else max(pos, 0)
        return max(0.0, qty - closes) * price

    MARKET_LAYERS = {"exit": "recycler", "recycle": "recycler", "take": "sniper", "touch": "mm", "mm": "mm",
                     "ladder": "ladder", "rebound": "ladder", "set": "pairs", "unwind": "pairs", "repair": "pairs"}
    REDUCING = {"exit", "recycle", "rebound", "unwind", "repair"}

    def gate(self, b, wants):
        """Layer flags (data/overrides.json): a want survives only if its layer is on and allowed on this market. Pause
        new risk drops every want that adds risk; exits, rebounds, unwinds and repairs keep running."""
        c, out = self.controls, []
        for w in wants:
            layer = ("recycler" if w["layer"] == "recycle" else "position") if b.kind == "position" else \
                self.MARKET_LAYERS.get(w["layer"], w["layer"])
            reducing = w["layer"] in self.REDUCING or w.get("reduce")
            if not c.on(layer) or not c.allows(b.ex, layer):
                if not (reducing and layer not in ("recycler",) and c.d["layers"].get(layer, {}).get("state") is not None):
                    continue                                 # a layer that is off keeps closing what it holds (not the recycler)
            if c.paused and not reducing:
                continue
            out.append(w)
        return out

    def pick_focus(self):
        """Market bots that get touch + ladder: competitive markets with the most room in the SIG book."""
        def score(b):
            bid, ask, _ = self.px.get(b.ex, (None, None, None))
            f = self.fair.get(b.ex)
            if f is None or bid is None or ask is None or not 0.04 <= f <= 0.96:
                return -1
            return (ask - bid) + 0.05 * min(f, 1 - f)
        ranked = sorted((b for b in self.bots if b.kind == "market"), key=score, reverse=True)
        self.focus = {b.ex for b in ranked[:MB_FOCUS] if score(b) >= 0}

    # ------------------------------------------------------------------------------------------------ F1-F5 one loop
    def step(self):
        now = time.time()
        self.t = {}
        # F1 read: Polymarket and every SIG read at the same time (each SIG read can take seconds under load)
        watched = set(self.owner) | {ex for pb in self.pairs for ex in pb.legs}
        self.controls.reload_if_changed()
        events = self.take_rt()                             # P2: what woke us (fills, order updates, PM moves, resyncs)
        with ThreadPoolExecutor(max_workers=6) as pool:
            # open orders FIRST, then positions, in one worker: a fill between the two reads can then only make the
            # position look smaller than the orders imply (I2 capacity understated, never overstated)
            jobs = dict(fair=pool.submit(pm.fairs, self.mp), book=pool.submit(lambda: (api.open_orders(), api.positions_raw())),
                        px=pool.submit(api.prices, watched), fills=pool.submit(api.fills, set(self.ledger.seen)), pnl=pool.submit(api.pnl))
        got = {}
        for k, j in jobs.items():
            try:
                got[k] = j.result()
            except Exception as e:
                if k in ("book", "px"):
                    raise                                       # no positions/orders/prices: no trading this loop
                self.event("ERR", f"{k}: {e!r}")
        got["open"], got["pos"] = got.pop("book")
        self.t["read"] = time.time() - now
        if "fair" in got:
            self.pm = got["fair"]
            self.ft.update(got["fair"], now)                # A6: EWMA mid, jumps confirmed, wide/illiquid dropped
            self.fair = self.ft.fairs(now)
            self.fair_t = time.time()
        if self.pm_feed and self.pm_feed.quotes:            # between REST polls: Polymarket's websocket quotes (P2)
            ws = {ex: (b, a, (self.pm.get(ex) or (0, 0, 0))[2] if len(self.pm.get(ex) or ()) > 2 else 2000.0)
                  for ex, (b, a, t) in list(self.pm_feed.quotes.items()) if t > self.fair_t - 60}
            if ws:
                self.ft.update(ws, time.time())
                self.fair = self.ft.fairs(time.time())
        self.px_t = time.time()
        fresh = time.time() - self.fair_t <= PM_MAX_AGE_S
        if not fresh and any(b.kind == "market" for b in self.bots):
            if not self.pulled:
                self.event("PM", "Polymarket stale: pulling at-risk market-bot orders; position exits stay")
                self.pull_at_risk(limit_s=20)
                self.pulled = True
            return self.write_state(fresh)
        if self.pulled:
            self.ex.resume()
        self.pulled = False

        pos, rows = {}, {}
        for p in got["pos"]:
            ex = str(p["exchangeId"])
            pos[ex], rows[ex] = pos.get(ex, 0) + api.net(p), p
        self.open, self.px = got["open"], got["px"]
        prev = {ex: (b["pos"], b["row"]) for ex, b in self.book.items()}
        fill_exs = set()
        for f in self.ledger.sync(self.open, got.get("fills", []), self.bot_of, self.side_fair, self.side_mid, as_of=now,
                                  held=lambda ex: prev.get(ex)):
            fill_exs.add(f["ex"])
            e = "" if f["edge"] is None else f" | vs PM {f['edge'] * 100:+.1f}c, vs SIG mid {(f['sig_edge'] or 0) * 100:+.1f}c"
            self.event("FILL", f"{f['bot']} [{f['layer'] or '-'}]: {f['action'] or 'trade'} {f['side'].upper()} {f['qty']:,.0f} @ {f['price']:.3f}{e}")
        self.ledger.markouts(self.side_mid)
        self.ledger.init_subbooks(pos, self.manual_ex)
        self.ledger.audit_subbooks(pos, log=lambda s: self.event("AUDIT", s.strip()[6:].strip()))
        self.ex.update(pos, self.open, as_of=now)
        if self.ex.frozen or self.journal.unknown():
            n = self.ex.resolve_unknown(self.open, got.get("fills", []), on_adopt=self.adopted)
            if n:
                self.event("AUDIT", f"resolved {n} writes with an unknown outcome; their markets are unfrozen")
        self.book = {ex: dict(pos=pos[ex], row=rows[ex]) for ex in pos}
        self.acct = got.get("pnl", self.acct)
        if self.n % 20 == 0 or not self.focus:
            self.pick_focus()

        # F2 context: free cash, kappa, SIG reference mids, tranches, the layer controller, bucket usage
        raw = (self.acct.get("totalAccountValue") or 0) - (self.acct.get("totalHoldingsValue") or 0)
        self.cash_hist = (getattr(self, "cash_hist", []) + [raw])[-3:]
        cash = min(self.cash_hist)                          # one glitchy read can't open the gates (06:12: +10.7k for a loop)
        self.update_mids(now)
        self.track_tranches(now)
        self.controller(now)
        kappa, _ = self.kappa()
        used = self.bucket_usage()
        engine_ex = {b.ex for b in self.bots if b.kind == "market"} | {ex for pb in self.pairs for ex in pb.legs}
        filled = sum(self.risk(ex, pos.get(ex, 0)) for ex in engine_ex)
        self.risk_view = dict(filled=round(filled), resting=round(used["committed"]), cash=round(cash),
                              budget=round(cash - CASH_RESERVE - used["committed"]))
        budget = self.risk_view["budget"]

        # F2a pairs first: unwinds and repairs always; new sets only with the pairs layer on and new risk allowed
        t0 = time.time()
        shots, spent = self.run_pairs(now, min(budget, self.cap("pairs", "capital") - used["pairs"]), kappa) \
            if self.controls.on("pairs") else (0, 0.0)
        self.t["pairs"] = time.time() - t0
        t0 = time.time()
        # each bot sees only its own orders: resting ones it placed (ownership record) + its queued/in-flight ones
        mine = {}
        for o in self.open:
            rec = self.ledger.orders.get(o["id"])
            if rec:
                mine.setdefault(rec["bot"], []).append(dict(o, sub=rec.get("sub", ""), layer=rec.get("layer", "")))
        for p in self.ex.pending():
            mine.setdefault(p["bot"], []).append(dict(p, id=None, price=p["price"]))
        plans = []
        self.ladder_quota = int(LADDER_CAPACITY_SHARE * self.ex.capacity * max(LOOP_S, self.t.get("read", 0) + 5) / 60)
        for b in sorted(self.bots, key=lambda b: (b.kind != "position", -self.sweep_rank.get(b.ex, (0, 0))[0],
                                                  -self.sweep_rank.get(b.ex, (0, 0))[1])):   # §5.3: most sweeps first
            s = self.bot_snapshot(b, pos, rows, kappa, cash, used)
            s["own"] = [o for o in mine.get(b.name, []) if o.get("id")]   # I4: our resting orders aren't the market
            wants = self.gate(b, b.plan(s))
            if b.kind == "market":                                  # buys against the net holding net it, and I2 caps them
                net, room_r = pos.get(b.ex, 0), abs(pos.get(b.ex, 0))   # at |net| together: share that capacity, exits and
                for w in sorted(wants, key=lambda w: not w.get("reduce")):   # rebounds first, then entries (no churn)
                    if w["action"] == "buy" and jx.reduces(w["side"], "buy", net):
                        w["qty"] = int(max(0, min(w["qty"], room_r)))
                        room_r -= w["qty"]
                        if w.get("qmax") is not None:
                            w["qmax"] = min(w["qmax"], w["qty"])
                wants = [w for w in wants if w["qty"] >= MIN_QTY]
            kept, cancels, places = bots.reconcile(wants, mine.get(b.name, []))
            if b.kind == "market":
                places = self.admit(b, places, used, cash)
            if b.kind == "position":                                # foreign sells (placed by hand) share the holding
                foreign = sum(o["qty"] for o in self.open if o["ex"] == b.ex and not self.ex.owned(o["id"])
                              and jx.reduces(o["side"], o["action"], pos.get(b.ex, 0)))
                places = bots.trim_sells(int(abs(s["pos"]) - foreign), kept, places)
            b.view = dict(s={k: v for k, v in s.items() if k in ("pos", "fair", "bid", "ask", "ref", "u")}, wants=wants)
            urgency = max((b.urgency(o) for o in cancels if o.get("id")), default=0.0)
            plans.append((b.kind != "position", not any(w.get("reduce") for w in places), -urgency, b, cancels, places))
        plans.sort(key=lambda t: t[:3])         # position bots, risk-reducing orders, quotes being picked off
        self.t["plan"] = time.time() - t0
        t0 = time.time()

        # F3 submit: cancels to the cancel lane (owned orders only), placements to the queue (I2 trims at send time)
        room, n_cancel, n_place, deferred = self.ex.admit(), 0, 0, 0
        placed_exs, cancelled_exs = set(), set()
        self.requotes = [t for t in self.requotes if t > time.time() - 60]
        for _, _, _, b, cancels, places in plans:
            if cancels:
                cancelled_exs.add(b.ex)
            if places:
                placed_exs.add(b.ex)
            sub = "manual" if b.kind == "position" else "inventory"
            held_back = False
            live = self.market_layers(b.ex) if b.kind == "market" and not self.controls.paused else {}
            for o in cancels:                               # a requote (layer still on here) is capped; a pull is not
                if o.get("id") and o.get("layer") in ("mm", "ladder") and live.get(o["layer"]) and b.urgency(o) <= 0:
                    if len(self.requotes) >= REQUOTE_CANCELS_PER_MIN:   # A9: requote cancels capped; one still at or
                        held_back = True                                # above fair (being picked off) always goes
                        continue
                    self.requotes.append(time.time())
                if o.get("id"):
                    n_cancel += self.ex.cancel(o, reason=b.name)
                else:                                       # a queued order no longer wanted: drop it before it's sent
                    self.ex.drop_queued(lambda bot, sb, layer, od, o=o, b=b: bot == b.name and od[0] == o["ex"] and
                                        od[1] == o["side"] and od[2] == o["action"] and abs(od[3] - o["price"]) < 1e-9)
            places = [w for w in places if not self.ex.is_frozen(b.ex)]
            if held_back:                                   # its old quotes stay for now: no doubled quotes
                places = [w for w in places if w["layer"] not in ("mm", "ladder")]
                deferred += 1
            if not places:
                continue
            if b.kind != "position" and len(places) > room:
                deferred += 1
                continue
            if b.kind == "position":
                for w in places:
                    self.event("PLACE", f"{b.name}: {w['action']} {w['side'].upper()} {w['qty']:,} @ {w['px']:.3f}")
            elif time.time() - self.fair_t > PM_MAX_AGE_S:
                deferred += 1
                continue
            by_ttl = {}
            for w in places:                                # one job per TTL (the executor merges same-TTL jobs)
                ttl = None if b.kind == "position" and w["layer"] in ("exit", "add") else (w.get("ttl") or MB_QUOTE_TTL_S)
                by_ttl.setdefault(ttl, []).append(w)
            for ttl, ws in by_ttl.items():
                self.ex.place([(b.name, w.get("sub") or sub, w["layer"], (b.ex, w["side"], w["action"], w["px"], w["qty"])) for w in ws],
                              ttl_s=ttl, layer_prio=jx.PRIORITY["position"] if b.kind == "position" and ttl is None else None)
            room -= len(places)
            n_place += len(places)
        self.t["orders"] = time.time() - t0
        done = time.time()                                  # A7 latency budgets, measured from the realtime event
        for t_ev, kind, exs in events:
            exs = exs or (fill_exs if kind == "fills" else set())   # a notification names no market: the fills REST found
            if kind == "fills" and exs & placed_exs:
                self.sample("fill_to_queued", done - t_ev)
            if kind == "pm_move" and exs & cancelled_exs:
                self.sample("pm_to_defend", done - t_ev)
        # F4 repair pair sets whose legs filled unevenly (runs whatever the pairs flag: repairs reduce risk)
        self.repair_baskets(now)
        self.t["loop"] = time.time() - now
        self.sample("plan_cycle", self.t["loop"])

        # F5 record
        self.n += 1
        mk = [b for b in self.bots if b.kind == "market"]
        self.summary = (f"{time.strftime('%H:%M:%S')} loop {self.n} | market bots active {sum(1 for b in mk if b.view['wants'])}/{len(mk)} "
                        f"(focus {len(self.focus)}) | cancels queued {n_cancel} | placements queued {n_place} | deferred {deferred} | pair sets {shots} | "
                        f"risk filled {filled:,.0f}, committed {used['committed']:,.0f}, budget {budget:,.0f} | executor {self.ex.stats()['queue']} | "
                        f"account {self.acct.get('totalAccountValue', 0):,.0f} | took {self.t['loop']:.0f}s "
                        f"(read {self.t['read']:.0f}, pairs {self.t['pairs']:.0f}, orders {self.t['orders']:.0f})")
        print(self.summary, flush=True)
        self.ledger.save()
        self.write_state(fresh)
        for job in (self.hourly, lambda: self.log_leaders(time.time()), lambda: self.leaders_marks_check(time.time()),
                    lambda: self.scan_new_markets(time.time())):
            try:
                job()
            except Exception as e:                          # side jobs must never stop trading
                self.event("ERR", f"side job: {e!r}")

    # ------------------------------------------------------------------------------------------------ realtime (P2)
    def rt_event(self, kind, exs, t):
        """From the realtime threads: mark markets dirty and wake the planner (REST is re-read before acting)."""
        with self.rt_lock:
            self.rt_pending.append((t, kind, set(exs)))
            if kind == "pm_move" or kind in ("fills", "orders", "resync"):
                if kind in ("fills", "orders", "resync"):
                    self.rt_sig_t = t
                else:
                    self.rt_pm_t = t
        self.wake.set()

    def start_realtime(self):
        """P2 feeds (live only): SIG user channel + Polymarket CLOB websocket for the mapped YES tokens."""
        try:
            self.sig_feed = rt.SigUserFeed(lambda: api._req("POST", "/realtime/token", json={}), self.rt_event,
                                           log=lambda k, s: self.event(k, s))
            self.sig_feed.start()
        except Exception as e:
            self.event("ERR", f"SIG realtime not started: {e!r}")
        try:
            toks = read_json(os.path.join(DATA, "clob_tokens.json"), {})
            tokens = {m["ex"]: [toks[i][0] for i in m["pm_ids"] if toks.get(i)] for m in self.mp}
            tokens = {ex: t for ex, t in tokens.items() if t and len(t) == len(next(m["pm_ids"] for m in self.mp if m["ex"] == ex))}
            if tokens:
                self.pm_feed = rt.PolyFeed(tokens, self.rt_event, log=lambda k, s: self.event(k, s))
                self.pm_feed.start()
        except Exception as e:
            self.event("ERR", f"Polymarket realtime not started: {e!r}")

    def take_rt(self):
        """Realtime events since the last loop (oldest first)."""
        with self.rt_lock:
            out, self.rt_pending = self.rt_pending, []
        return out

    def latency_view(self):
        """A7 budgets: p50/p90/max per path (seconds). cancel_to_sent comes from the executor."""
        out = {}
        for k, xs in list(self.latency.items()) + [("cancel_to_sent", list(getattr(self.ex, "cancel_lat", [])))]:
            xs = sorted(xs)
            out[k] = dict(n=len(xs), p50=xs[len(xs) // 2] if xs else None, p90=xs[int(len(xs) * 0.9)] if xs else None,
                          max=xs[-1] if xs else None)
        return out

    def sample(self, key, v):
        xs = self.latency.setdefault(key, [])
        xs.append(round(v, 3))
        del xs[:-500]

    # ------------------------------------------------------------------------------------------------ layers v2
    def pol(self, ex, side):
        """Political direction of buying `side` on ex: 'D' (D YES or R NO), 'R' (R YES or D NO), else 'other'."""
        party = (self.meta.get(ex) or {}).get("party")
        if party not in ("D", "R"):
            return "other"
        return party if side == "yes" else ("R" if party == "D" else "D")

    def update_mids(self, now):
        """SIG mid snapshots for the ladder anchor (A5): last 30 min per market; saved every 5 min to survive restarts."""
        for ex, (b, a, _) in self.px.items():
            if b is not None and a is not None:
                d = self.mids.setdefault(ex, [])
                d.append((now, (b + a) / 2, a - b))
                while d and d[0][0] < now - REF_WINDOW_S:
                    d.pop(0)
        if now - self.mids_t >= 300 and not api.DRY_RUN:
            self.mids_t = now
            write_atomic(os.path.join(DATA, "mids.json"), json.dumps(self.mids))

    def ref(self, ex):
        """30-min median SIG mid, ignoring snapshots with a spread > 10c; None until >= 6 samples over >= 10 min."""
        d = [(t, m) for t, m, sp in self.mids.get(ex, []) if sp <= REF_MAX_SPREAD]
        if len(d) < REF_MIN_SAMPLES or d[-1][0] - d[0][0] < 600:
            return None
        xs = sorted(m for _, m in d)
        return xs[len(xs) // 2]

    def track_tranches(self, now):
        """A tranche starts at its first fill (records ref, offset and fair then); the news guard hands it to the
        recycler when fair drops > off/2 below that; it closes once its sub-book is back to zero."""
        tr, self.tranche_view = self.ledger.tranches, {}
        for ex, books in list(self.ledger.subbooks.items()):
            for sub, q in list(books.items()):
                if not sub.startswith("tranche:") or abs(q) < 1 or sub in tr:
                    continue
                side = "yes" if q > 0 else "no"
                level = int(sub.split("-")[0][-1]) if sub.split("-")[0][-1].isdigit() else 0
                f, r = self.side_fair(ex, side), self.ref(ex)
                ref_side = None if r is None else (r if side == "yes" else 1 - r)
                base = LADDER_LEVELS[min(level, len(LADDER_LEVELS) - 1)][0]
                tr[sub] = dict(ex=ex, side=side, level=level, ref_side=ref_side, fair_side=f, state="open", t=stamp(),
                               off=max(0.015, base * bots.s_of(f if f is not None else 0.5)))
                self.event("TRANCHE", f"{self.name(ex)}: ladder fill -> tranche {sub} {side.upper()} {abs(q):,.0f}")
        for sub, t in tr.items():
            if t["state"] != "open":
                continue
            ex, q = t["ex"], self.ledger.subbooks.get(t["ex"], {}).get(sub, 0)
            if abs(q) < 1:
                t["state"] = "closed"
                continue
            f = self.side_fair(ex, t["side"])
            if f is not None and t.get("fair_side") is not None and f < t["fair_side"] - t["off"] / 2:
                self.ledger.move_sub(ex, sub, "inventory")
                t["state"] = "news"
                self.event("NEWS", f"{self.name(ex)}: fair {t['side'].upper()} fell {t['fair_side'] - f:+.3f} (> off/2): tranche "
                                   f"{sub} handed to the recycler")
                continue
            if t.get("ref_side") is None:
                r = self.ref(ex)
                t["ref_side"] = None if r is None else (r if t["side"] == "yes" else 1 - r)
            avg = self.ledger.tranche_avg(sub)
            if avg is not None and t.get("ref_side") is not None:
                self.tranche_view.setdefault(ex, []).append(dict(sub=sub, ref_side=t["ref_side"], avg=avg, state="open"))

    def market_layers(self, ex):
        """{layer: True} for the strategy layers that may act on this market: on, allowed here, and in the pilot set."""
        out = {}
        for l in ("recycler", "mm", "ladder", "sniper"):
            st = self.controls.state(l)
            if st == "off" or not self.controls.allows(ex, l):
                continue
            if st == "pilot" and l in self.pilot_sets and ex not in self.pilot_sets[l]:
                continue
            out[l] = True
        return out

    def blocked_sides(self, ex):
        now = time.time()
        return {side for side in ("yes", "no") if self.breaker.get(self.pol(ex, side), 0) > now}

    def bot_snapshot(self, b, pos, rows, kappa, cash, used):
        bid, ask, last = self.px.get(b.ex, (None, None, None))
        if b.kind == "position":
            s = dict(pos=self.ledger.sub(b.ex, "manual"), fair=self.fair.get(b.ex), bid=bid, ask=ask, kappa=kappa,
                     recycle={k: v for k, v in self.ledger.subbooks.get(b.ex, {}).items() if k.startswith("recycle:")}
                     if self.controls.on("recycler") and self.controls.allows(b.ex, "recycler") else {})
            return s
        f = self.fair.get(b.ex)
        held = {"yes": 0.0, "no": 0.0}
        q = pos.get(b.ex, 0)
        if f is not None and q:
            held["yes" if q > 0 else "no"] = abs(q) * (f if q > 0 else 1 - f)
        for o in self.open:
            if o["ex"] == b.ex and o["action"] == "buy":
                rec = self.ledger.orders.get(o["id"])
                if rec and rec["layer"] not in self.REDUCING and not (rec["layer"] == "mm" and jx.reduces(o["side"], "buy", q)):
                    held[o["side"]] += o["qty"] * o["price"]
        return dict(fair=f, u=self.ft.u.get(b.ex), bid=bid, ask=ask, ref=self.ref(b.ex), kappa=kappa,
                    subs=dict(self.ledger.subbooks.get(b.ex, {})), layers=self.market_layers(b.ex),
                    paused=self.controls.paused, blocked=self.blocked_sides(b.ex),
                    room={sd: max(0.0, MARKET_MAX_POS - held[sd]) for sd in ("yes", "no")},
                    tranches=self.tranche_view.get(b.ex, []),
                    ladder_ids={(sd, i): self.ledger.ladder_id(b.ex, sd, i) for sd in ("yes", "no") for i in range(len(LADDER_LEVELS))}
                    if self.market_layers(b.ex).get("ladder") else {},
                    take_notional=max(0.0, self.cap("sniper", "per_market") - self.race_takes(b.ex, used)) if
                    time.time() - self.new_markets.get(b.ex, 0) < 1800 else
                    min(TAKE_NOTIONAL, max(0.0, self.cap("sniper", "per_market") - self.race_takes(b.ex, used))))

    def bucket_usage(self):
        """What each capital bucket uses now (SIG): resting entry buys at full notional, filled inventory at cost."""
        u = dict(committed=0.0, mm=0.0, ladder_filled=0.0, ladder_resting=0.0, ladder_side={"D": 0.0, "R": 0.0, "other": 0.0},
                 sniper_market={}, sniper_day=0.0, pairs=0.0)
        for o in self.open:
            rec = self.ledger.orders.get(o["id"])
            if not rec or o["action"] != "buy" or rec["bot"] in self.position_names:
                continue
            n, l = o["qty"] * o["price"], rec["layer"]
            if l in ("mm", "take", "set") and not jx.reduces(o["side"], "buy", self.book.get(o["ex"], {}).get("pos", 0)):
                u["committed"] += n                         # the hard floor counts resting entry buys at full notional
            if l == "ladder":
                u["ladder_resting"] += n
                u["ladder_side"][self.pol(o["ex"], o["side"])] += n
            elif l == "take":
                u["sniper_market"][o["ex"]] = u["sniper_market"].get(o["ex"], 0.0) + n
        for p in self.ex.pending():                         # queued / in flight / acked but not yet in a snapshot
            if p["action"] != "buy" or p["bot"] in self.position_names:
                continue
            n, l = p["qty"] * p["price"], p["layer"]
            if l in self.FLOOR_LAYERS:
                u["committed"] += n
            if l == "ladder":
                u["ladder_resting"] += n
                u["ladder_side"][self.pol(p["ex"], p["side"])] += n
            elif l == "take":
                u["sniper_market"][p["ex"]] = u["sniper_market"].get(p["ex"], 0.0) + n
        for ex, books in self.ledger.subbooks.items():
            q = books.get("mm", 0)
            f = self.fair.get(ex)
            if q and f is not None:
                u["mm"] += abs(q) * (f if q > 0 else 1 - f)
        for sub, t in self.ledger.tranches.items():
            if t["state"] == "open":
                q = self.ledger.subbooks.get(t["ex"], {}).get(sub, 0)
                u["ladder_filled"] += abs(q) * (self.ledger.tranche_avg(sub) or 0.5)
        today = time.strftime("%Y-%m-%d")
        for f in self.ledger.fills[-3000:]:
            if f["layer"] == "take" and f["t"][:10] == today and f["action"] == "buy":
                u["sniper_day"] += f["qty"] * f["price"]
                u["sniper_market"][f["ex"]] = u["sniper_market"].get(f["ex"], 0.0) + f["qty"] * f["price"]
        for bk in self.ledger.baskets:
            if bk.get("state", "held") in ("held", "repair"):
                u["pairs"] += sum(l[3] * (l[4] or 0) for l in self.ledger.basket_fills(bk))
        self.used = {("mm", "inventory"): u["mm"], ("ladder", "filled"): u["ladder_filled"], ("ladder", "resting"): u["ladder_resting"],
                     ("pairs", "capital"): u["pairs"], ("sniper", "per_day"): u["sniper_day"],
                     ("sniper", "per_market"): max(u["sniper_market"].values(), default=0.0)}
        return u

    def race_takes(self, ex, used):
        race = (self.meta.get(ex) or {}).get("race")
        return sum(v for e_, v in used["sniper_market"].items() if (self.meta.get(e_) or {}).get("race") == race)

    def admit(self, b, places, used, cash):
        """The risk gate for new entries (night plan §4.2), in place order: hard cash floor for MM/takes, the layer's
        bucket, the ladder's per-side cash cover and its share of SIG capacity. Reducing orders always pass."""
        out = []
        for w in places:
            if w.get("reduce"):
                out.append(w)
                continue
            n, l = w["qty"] * w["px"], w["layer"]
            if l in ("mm", "take") and cash - used["committed"] - n < CASH_RESERVE:
                continue
            if l == "mm" and used["mm"] >= self.cap("mm", "inventory"):
                continue                                    # the MM bucket is inventory at cost (§4.2); resting = the floor
            race = (self.meta.get(b.ex) or {}).get("race")
            race_used = sum(v for ex, v in used["sniper_market"].items() if (self.meta.get(ex) or {}).get("race") == race)
            if l == "take" and (used["sniper_day"] + n > self.cap("sniper", "per_day")      # the per-market cap also holds
                                or race_used + n > self.cap("sniper", "per_market")):       # per race: D YES + R NO is one bet
                continue
            if l == "ladder":
                side = self.pol(b.ex, w["side"])
                cover = max(0.0, cash - CASH_RESERVE - used["committed"])
                if (self.ladder_quota <= 0 or used["ladder_filled"] >= self.cap("ladder", "filled")
                        or used["ladder_resting"] + n > self.cap("ladder", "resting") or used["ladder_side"][side] + n > cover):
                    continue
                self.ladder_quota -= 1
                used["ladder_resting"] += n
                used["ladder_side"][side] += n
            if l in ("mm", "take"):
                used["committed"] += n
            if l == "take":
                used["sniper_day"] += n
                used["sniper_market"][b.ex] = used["sniper_market"].get(b.ex, 0.0) + n
            out.append(w)
        return out

    def load_pilots(self):
        """Pilot market sets from data/market_stats.json (24 h SIG trade tape): MM = most volume, ladders = most sweeps."""
        path = os.path.join(DATA, "market_stats.json")
        stats = (read_json(path, {}) or {}).get("stats", {})
        mk = [b.ex for b in self.bots if b.kind == "market"]
        vol = sorted(mk, key=lambda ex: -(stats.get(ex, {}).get("volume") or 0))
        swp = sorted(mk, key=lambda ex: (-(stats.get(ex, {}).get("sweeps") or 0), -(stats.get(ex, {}).get("volume") or 0)))
        self.pilot_sets = {"mm": set(vol[:MM_PILOT_MARKETS]), "ladder": set(swp[:LADDER_PILOT_MARKETS])} if stats else {}
        self.sweep_rank = {ex: (v.get("sweeps") or 0, v.get("volume") or 0) for ex, v in stats.items()}

    def controller(self, now):
        """Night plan §4.4-4.5 and §5 gates, every loop: per-layer kill rules, pilot promotions, the D5 loss stop, the
        SIG 5xx pause, the correlated-fill breaker, and D1 (AK release to the recycler)."""
        c = self.controls
        night = c.d.setdefault("night", {})
        before = json.dumps(night, sort_keys=True, default=str)
        since = {l: c.layer(l).get("since") or now for l in LAYERS}
        stats = self.layer_stats(since)
        path = os.path.join(DATA, "market_stats.json")
        m = os.path.getmtime(path) if os.path.exists(path) else None
        if m != getattr(self, "pilots_mtime", None):        # new trade-tape stats: refresh the pilot market sets
            self.pilots_mtime = m
            self.load_pilots()
        for l in ("recycler", "mm", "ladder", "pairs", "sniper"):
            if not c.on(l):
                continue
            hit = [k for k in self.kill_status(l, stats.get(l, {})) if k["hit"]]
            if hit:
                c.kill(l, f"{hit[0]['rule']}: {hit[0]['value']}")
                self.alerts.append(f"{time.strftime('%H:%M')} layer {l} killed: {hit[0]['rule']} ({hit[0]['value']})")
        # promotions (pilot -> full) and pilot extensions
        for l in ("mm", "ladder", "pairs"):
            if c.state(l) != "pilot":
                continue
            st, age = stats.get(l, {}), now - since[l]
            ok, why = False, ""
            if l == "mm" and age >= PILOT_S:
                n, edge, mo = st.get("fills", 0), st.get("edge"), st.get("markout30")
                if n >= 20 and edge is not None and edge > 0.01 and (mo is None or mo >= -0.005):
                    ok, why = True, f"{n} fills, edge {edge * 100:+.2f}c, mark-out {(mo or 0) * 100:+.2f}c"
                elif now - night.get(f"{l}_ext", 0) >= PILOT_S:
                    night[f"{l}_ext"] = now
                    self.night.line(f"layer mm pilot extended 2 h: {n} fills, edge {(edge or 0) * 100:+.2f}c, "
                                    f"mark-out {(mo or 0) * 100:+.2f}c (gate: >= 20 fills, edge > 1c, mark-out >= -0.5c)")
            if l == "ladder" and age >= PILOT_S:
                trims = self.ex.trims.get("ladder", 0) - night.get("ladder_trims0", 0)
                trips = night.get("breaker_trips_since_ladder", 0)
                if trims == 0 and trips == 0:
                    ok, why = True, "2 h pilot: no I2 trims, no breaker trips"
                elif now - night.get(f"{l}_ext", 0) >= PILOT_S:
                    night[f"{l}_ext"] = now
                    self.night.line(f"layer ladder stays pilot: {trims} I2 trims, {trips} breaker trips (review)")
            if l == "pairs":
                done = [bk for bk in self.ledger.baskets if bk.get("ts", 0) >= since[l] and bk.get("state") in ("held", "unwound", "done")
                        and self.ledger.matched(bk) > 0 and not self.basket_residue(bk)]
                if len(done) >= 5:
                    ok, why = True, f"{len(done)} sets with no one-leg residue"
            if ok:
                c.set_layer(l, "full", f"pilot gate passed: {why}")
        # D5 loss stop and the SIG 5xx pause
        bot_24h = sum(self.fill_pnl_fair(f) for f in self.fills_since(now - 86400)
                      if self.strategy_of(f["bot"], f["layer"]) not in ("position", "old engine"))
        if bot_24h < -LOSS_STOP and not c.paused:
            c.set_paused(True, f"D5 loss stop: bot P&L at fair {bot_24h:+,.0f} over 24 h")
            c.decide(f"loss stop tripped at {time.strftime('%H:%M')} ({bot_24h:+,.0f}): resume new risk?")
        h = api.health(600)
        bad = h["read"]["n"] >= 20 and h["read"]["bad_rate"] > 0.5
        if bad and not c.paused:
            self.bad_since = self.bad_since or now
            if now - self.bad_since >= 600:
                c.set_paused(True, "SIG 5xx/timeouts > 50% for 10 min")
                night["paused_by_5xx"] = True
        elif not bad:
            self.bad_since = None
            if c.paused and night.pop("paused_by_5xx", False) and h["read"]["bad_rate"] < 0.1:
                c.set_paused(False, "SIG recovered (< 10% errors)")
        # correlated-fill breaker: ladder + MM entry fills in >= 5 markets in 10 min, >= 80% one political direction
        recent = [f for f in self.fills_since(now - BREAKER_WINDOW_S) if f["layer"] in ("ladder", "mm") and f["action"] == "buy"]
        lean = {}                                           # each market counts once, by its notional-weighted direction:
        for f in recent:                                    # one busy market can't fake a correlated move
            d = lean.setdefault(f["ex"], {})
            p = self.pol(f["ex"], f["side"])
            d[p] = d.get(p, 0.0) + f["qty"] * f["price"]
        market_dir = {ex: max(d, key=d.get) for ex, d in lean.items()}
        for direction in ("D", "R"):
            if self.breaker.get(direction, 0) > now:
                continue
            ms = set(market_dir)
            mine = [ex for ex, d in market_dir.items() if d == direction]
            # news moves Polymarket too: what we bought must have lost >= 1c of fair since (MM flow at a flat fair is
            # not news; without this the breaker trips on ordinary MM fills and its mass cancels eat the write budget)
            hit = [f for f in recent if f["ex"] in mine and f.get("fair") is not None]
            q = sum(f["qty"] for f in hit)
            moved = sum(((self.side_fair(f["ex"], f["side"]) or f["fair"]) - f["fair"]) * f["qty"] for f in hit) / q if q else 0.0
            if len(ms) >= BREAKER_MARKETS and len(mine) / len(ms) >= BREAKER_SHARE - 1e-9 and moved <= -BREAKER_FAIR_MOVE:
                self.breaker[direction] = now + BREAKER_PAUSE_S
                night["breaker_trips_since_ladder"] = night.get("breaker_trips_since_ladder", 0) + 1
                n = 0
                for o in self.open:
                    rec = self.ledger.orders.get(o["id"])
                    if rec and rec["layer"] in ("ladder", "mm") and o["action"] == "buy" and self.pol(o["ex"], o["side"]) == direction \
                            and not jx.reduces(o["side"], "buy", self.book.get(o["ex"], {}).get("pos", 0)):
                        n += bool(self.ex.cancel(o, reason="breaker"))
                msg = (f"BREAKER pro-{direction}: {len(mine)}/{len(ms)} markets with ladder/MM fills leaned pro-{direction} within 10 min, "
                       f"Polymarket fair {moved * 100:+.1f}c against them "
                       f"(news, not a sweep): {n} {direction}-side ladder/MM buys cancelled, entries that way paused 30 min")
                self.alerts.append(f"{time.strftime('%H:%M')} {msg}")
                self.event("BREAKER", msg)
                self.night.line(msg)
        self.restore_floor()
        self.alerts = self.alerts[-20:]
        # D1: release 25% of the AK position to the recycler once the recycler is on (night plan §1)
        if c.on("recycler") and not night.get("d1_done"):
            self.apply_d1()
        if json.dumps(night, sort_keys=True, default=str) != before:
            c.save()                                        # only on a change: a hand edit on disk is never overwritten

    def restore_floor(self):
        """§4.2 hard floor: free cash - every resting MM/take entry buy >= CASH_RESERVE. Fills spend cash while the other
        quotes stay, so it can slip: cancel resting entries, lowest ROC first, until the deficit is covered (admission
        keeps them from coming back until cash recovers)."""
        a = self.acct or {}
        cash = min(getattr(self, "cash_hist", None) or [(a.get("totalAccountValue") or 0) - (a.get("totalHoldingsValue") or 0)])
        deficit = self.committed() + CASH_RESERVE - cash
        if deficit <= 0 or not a:
            return 0
        cands = []
        for o in self.open:
            rec = self.ledger.orders.get(o["id"])
            if not rec or o["action"] != "buy" or rec["layer"] not in ("mm", "take") or o["id"] in self.ex.cancelling or \
                    jx.reduces(o["side"], "buy", self.book.get(o["ex"], {}).get("pos", 0)):
                continue
            f = self.side_fair(o["ex"], o["side"])
            cands.append((bots.roc_of(f, o["price"]) if f is not None else -1.0, o))
        freed, n = 0.0, 0
        for _, o in sorted(cands, key=lambda t: t[0]):
            if freed >= deficit:
                break
            if self.ex.cancel(o, reason="cash floor"):
                freed += o["qty"] * o["price"]
                n += 1
        if n:
            self.event("FLOOR", f"cash floor short by {deficit:,.0f}: {n} lowest-ROC entry quotes cancelled ({freed:,.0f} SIG)")
        return n

    def apply_d1(self):
        c, night = self.controls, self.controls.d.setdefault("night", {})
        ak = next((b for b in self.bots if b.kind == "position" and b.name == "AK-GOV-D"), None)
        if ak is None:
            night["d1_done"] = "no AK bot"
            return
        held = self.ledger.sub(ak.ex, "manual")
        q = self.ledger.split_sub(ak.ex, "manual", "recycle:ak-d1", int(abs(held) * 0.25))
        refs = read_json(POSITION_REF_FILE, {})
        old = ak.ref
        ak.ref = int(round(old * 0.75))
        if "AK-GOV-D" in refs:
            refs["AK-GOV-D"]["ref"] = ak.ref
            write_atomic(POSITION_REF_FILE, json.dumps(refs, indent=1))
        night["d1_done"] = stamp()
        self.night.line(f"D1 applied: {abs(q):,.0f} AK NO (25%) moved to sub-book recycle:ak-d1 (sold into the SIG bid while it is "
                        f">= PM NO fair + 0.5c); the AK exit ladder now covers the other 75% (ref {old:,} -> {ak.ref:,}); 0.10 spike bid kept")

    def log_leaders(self, now):
        """Every 10 min (night plan §5.6): the top 20 and our own row -> data/leaders.csv (2 reads)."""
        if now - self.leaders_t < 600 or not self.controls.on("leaders"):
            return
        self.leaders_t = now
        try:
            d = api.leaderboard(20)
            rows = [dict(r, ours=0) for r in d["leaderboard"]]
            if d.get("myRank"):
                me = api.leaderboard(1, offset=int(d["myRank"]) - 1)["leaderboard"]
                rows += [dict(r, ours=1) for r in me[:1]]
        except Exception as err:
            self.event("ERR", f"leaderboard: {str(err)[:120]}")
            return
        path, new = os.path.join(DATA, "leaders.csv"), not os.path.exists(os.path.join(DATA, "leaders.csv"))
        with open(path, "a", newline="") as fh:
            w = csv.writer(fh)
            if new:
                w.writerow(["time", "rank", "name", "profile", "value", "trades", "volume", "roi", "ours"])
            t = stamp()
            for r in rows:
                w.writerow([t, r.get("rank"), r.get("username") or r.get("profileId"), r.get("profileId"), r.get("pnl"),
                            r.get("tradesCount"), r.get("volume"), r.get("roi"), r["ours"]])

    def leaders_marks_check(self, now):
        """After 6 h of samples: does a leader's P&L move without trades (SIG marks catching up, not trading)?"""
        night = self.controls.d.setdefault("night", {})
        path = os.path.join(DATA, "leaders.csv")
        if night.get("marks_check") or not os.path.exists(path):
            return
        with open(path, newline="") as fh:
            rows = list(csv.DictReader(fh))
        if not rows:
            return
        ts = lambda r: time.mktime(time.strptime(r["time"], "%Y-%m-%d %H:%M:%S"))
        t1 = ts(rows[-1])
        if t1 - ts(rows[0]) < 6 * 3600:
            return
        first = {r["name"]: r for r in rows if ts(r) <= ts(rows[0]) + 60}
        last = {r["name"]: r for r in rows if ts(r) == t1}
        moved, still = [], []
        for name, r in last.items():
            if name in first:
                dv = float(r["value"]) - float(first[name]["value"])
                dn = int(float(r["trades"] or 0)) - int(float(first[name]["trades"] or 0))
                (still if dn == 0 and abs(dv) >= 1000 else moved).append(f"{name} {dv:+,.0f} on {dn} trades")
        night["marks_check"] = stamp()
        self.night.entry(f"{time.strftime('%H:%M')} PT: leaders marks check (6 h)", [
            f"P&L moved >= 1k WITHOUT trades (SIG marks catching up): {'; '.join(still) or 'none'}",
            f"P&L with trades: {'; '.join(moved[:20]) or 'none'}"])

    def scan_new_markets(self, now):
        """Sniper (night plan §5.5): every 60 s, the 20 newest tournament markets (1 read). A market not seen before is
        mapped to Polymarket strictly; without a confident match: alert, no trade. With one: it gets a MarketBot, and its
        first plans take asks far below fair (sniper caps), then MM and ladders pick it up."""
        if now - self.scan_t < 60 or not self.controls.on("sniper"):
            return
        self.scan_t = now
        try:
            ms = api.recent_markets(20)
        except Exception as err:
            self.event("ERR", f"new markets: {str(err)[:120]}")
            return
        for m in ms:
            ex = str(m["exchanges"][0]["id"]) if m.get("exchanges") else None
            if not ex or ex in self.meta or str(m["id"]) in self.seen_markets:
                continue
            self.seen_markets.add(str(m["id"]))
            race, party = pm.race_party(m["title"]) or (m["title"], "?")
            self.meta[ex] = dict(mid=str(m["id"]), title=m["title"], race=race, party=party)
            try:
                mp = pm.map_one(m)
            except Exception as err:
                mp = None
                self.event("ERR", f"map new market: {str(err)[:120]}")
            if not mp:
                msg = f"NEW MARKET {m['title']} (ex {ex}): no confident Polymarket match, not traded"
                self.alerts.append(f"{time.strftime('%H:%M')} {msg}")
                self.event("NEW", msg)
                self.night.line(msg)
                continue
            self.mp.append(mp)
            bot = bots.MarketBot(ex, self.meta[ex])
            self.bots.append(bot)
            self.owner[ex] = bot
            self.new_markets[ex] = now
            msg = f"NEW MARKET {m['title']} (ex {ex}) mapped to Polymarket {mp['pm_ids']}: sniper armed, then MM/ladders"
            self.event("NEW", msg)
            self.night.line(msg)

    def d1_status(self, b):
        """D1 (AK): the share released to the recycler, and where it stands."""
        night = self.controls.d.get("night", {})
        if b.name != "AK-GOV-D" or not night.get("d1_done"):
            return None
        q = abs(self.ledger.sub(b.ex, "recycle:ak-d1"))
        sold = sum(f["qty"] for f in self.ledger.fills if f["ex"] == b.ex and f["layer"] == "recycle")
        return dict(qty=q, state=(f"selling: {q:,.0f} left" if q >= 10 else "sold") + f" · {sold:,.0f} sold into the bid since "
                    f"{str(night['d1_done'])[11:16]}")

    def basket_residue(self, bk):
        legs = self.ledger.basket_fills(bk)
        return sum(l[3] for l in legs) - self.ledger.matched(bk) * bk["legs"] if legs else 0

    # ------------------------------------------------------------------------------------------------ F2a / F4 pairs
    def run_pairs(self, now, budget, kappa=0.0):
        """Pairs v2 (A12). Unwinds first (no ROC, no cap, not paused): a NO basket when the YES ask sum <= n-1+kappa, a
        YES set when the YES bid sum >= 1 - outsider - kappa, sent atomically. Then new sets (pairs layer on, new risk
        allowed): edge >= PAIR_EDGE (2c), EV vs fair >= 0 (>= 3c when a leg has no fair), sized from the books with our
        own orders removed, atomic. -> (sets sent, SIG committed)."""
        start, shots = budget, 0
        by_race = {pb.race: pb for pb in self.pairs}
        for bk in self.ledger.baskets:                          # early unwind
            if bk.get("state", "held") != "held" or bk["race"] not in by_race or self.ex.admit() < 1:
                continue
            m = self.ledger.matched(bk)
            if m < MIN_QTY:
                continue
            pb, n = by_race[bk["race"]], bk["legs"]
            legs = [l[1] for l in self.ledger.basket_fills(bk)]
            q = [self.px.get(ex, (None, None, None)) for ex in legs]
            if any(self.ex.is_frozen(ex) for ex in legs) or len(legs) != n:
                continue
            if bk["side"] == "no":
                asks = [x[1] for x in q]
                if None in asks or sum(asks) > n - 1 + kappa + 1e-9:
                    bk["unwind_at"], bk["unwind_value"] = round(n - 1 + kappa, 3), None if None in asks else round(sum(asks), 3)
                    continue
                orders = [(ex, "yes", "buy", a, int(m)) for ex, a in zip(legs, asks)]
            else:
                bids, out = [x[0] for x in q], pb.outsider(self.fair, PAIR_THIRD_PARTY)
                if None in bids or out is None or sum(bids) < 1 - out - kappa - 1e-9:
                    bk["unwind_at"] = None if out is None else round(1 - out - kappa, 3)
                    bk["unwind_value"] = None if None in bids else round(sum(bids), 3)
                    continue
                orders = [(ex, "no", "buy", round(1 - b_, 3), int(m)) for ex, b_ in zip(legs, bids)]
            bk["state"], bk["unwind_t"] = "unwinding", now

            def unwound(results, bk=bk, n=len(orders)):
                ok = [d.get("orderId") for _, _, ok_, d in results if ok_ and d.get("orderId") is not None]
                with self.ledger.lock:
                    bk.setdefault("unwinds", {})
                    for oid, (_, o, _, d) in zip(bk["orders"], results):
                        if d.get("orderId") is not None:
                            bk["unwinds"].setdefault(oid, []).append(str(d["orderId"]))
                    bk["state"] = "unwound" if len(ok) == n else "held"
            self.ex.place([(pb.name, f"pairset:{bk['id']}", "unwind", o) for o in orders], ttl_s=PAIR_TTL_S, atomic=True, on_done=unwound)
            self.event("UNWIND", f"{bk['race']}: {bk['side'].upper()} set x{int(m):,} unwound early at "
                                 f"{' + '.join(f'{o[3]:.3f}' for o in orders)} (keeps the locked edge, frees the capital)")
        if self.controls.paused:
            return 0, 0.0
        own = {}
        for o in self.open:
            yes_px = o["price"] if o["side"] == "yes" else round(1 - o["price"], 3)
            book = "bids" if (o["side"] == "yes") == (o["action"] == "buy") else "asks"
            own[(o["ex"], book, yes_px)] = own.get((o["ex"], book, yes_px), 0) + o["qty"]
        cands = []
        for pb in self.pairs:
            if not all(self.controls.allows(ex, "pairs") for ex in pb.legs):
                continue
            for e_, side in pb.screen(self.px, self.fair, PAIR_EDGE, PAIR_THIRD_PARTY):
                if now - pb.last_shot >= PAIR_COOLDOWN_S and not any(self.ex.is_frozen(ex) for ex in pb.legs):
                    cands.append((e_, side, pb))
        for e_, side, pb in sorted(cands, key=lambda c: -c[0]):
            if shots >= PAIR_MAX_SHOTS or self.ex.admit() < 2:
                break
            pb.last_shot = now
            books, key = {}, "bids" if side == "no" else "asks"
            try:                                                                # a bad read skips this pair, not the loop
                with ThreadPoolExecutor(max_workers=len(pb.legs)) as pool:      # every leg's book at once
                    fetched = list(pool.map(api.orderbook, pb.legs))
            except Exception as err:
                self.event("ERR", f"{pb.race} order book: {str(err)[:120]}")
                continue
            for ex, (bids, asks) in zip(pb.legs, fetched):
                lv = bids if side == "no" else asks
                books[ex] = [(p, q - own.get((ex, key, round(p, 3)), 0)) for p, q in lv if q - own.get((ex, key, round(p, 3)), 0) > 0]
            orders, qty, margin = pb.size(side, books, self.fair, PAIR_EDGE, PAIR_THIRD_PARTY, PAIR_MAX_QTY)
            if not orders:
                continue
            fairs = [self.side_fair(o[0], side) for o in orders]
            if None in fairs:
                if margin < 0.03 - 1e-9:                    # A12.1: no fair on a leg -> require >= 3c locked
                    continue
            elif sum(f - o[3] for f, o in zip(fairs, orders)) < -1e-9:
                self.event("PAIR", f"{pb.race}: {side.upper()} set skipped: locks {margin * 100:.1f}c but EV vs fair "
                                   f"{sum(f - o[3] for f, o in zip(fairs, orders)) * 100:+.1f}c (A12.1)")
                continue
            cost = sum(o[3] * o[4] for o in orders)
            if cost > budget or qty * margin < PAIR_MIN_LOCK:
                continue
            set_id = f"{pb.race}:{int(now)}:{uuid.uuid4().hex[:6]}"                 # unique even for two sets a second

            def recorded(results, pb=pb, side=side, qty=qty, margin=margin, n=len(orders), set_id=set_id, ts=now):
                ids = [str(d.get("orderId")) for _, _, ok, d in results if ok and d.get("orderId") is not None]
                if len(ids) == n or api.DRY_RUN:            # the set exists once SIG acked every leg
                    with self.ledger.lock:
                        self.ledger.baskets.append(dict(id=set_id, t=stamp(), ts=ts, race=pb.race, side=side, qty=qty,
                                                        margin=round(margin, 4), legs=n, orders=ids, repaired=False, state="held"))
            if self.ex.place([(pb.name, f"pairset:{set_id}", "set", o) for o in orders], ttl_s=PAIR_TTL_S, atomic=True,
                             on_done=recorded):
                budget -= cost
                shots += 1
                self.event("PAIR", f"{pb.race}: queued buy {side.upper()} on all {len(orders)} legs x{qty:,} at "
                                   f"{' + '.join(f'{o[3]:.3f}' for o in orders)} -> locks {margin * 100:.1f}c/set "
                                   f"(~{qty * margin:,.0f} SIG once every leg fills)")
        return shots, start - budget

    def settle_unwind(self, bk):
        """An early unwind is placed atomically but its legs fill independently. Once its orders are gone: what is left
        even stays a set ('held', it can unwind again); a leg's excess over the others is one-leg exposure and goes to
        'inventory', where the recycler sells it at >= fair (A12.4 step 4)."""
        legs = self.ledger.basket_fills(bk)
        if not legs:
            bk["state"] = "closed"
            return
        low = min(l[3] for l in legs)
        moved = []
        for oid, ex, side, held, avg in legs:
            extra = held - low
            if extra >= 1:
                self.ledger.split_sub(ex, f"pairset:{bk['id']}", "inventory", extra if side == "yes" else -extra)
                moved.append(f"{self.name(ex)} {side.upper()} {extra:,.0f}")
        bk["state"] = "held" if low >= MIN_QTY else "closed"
        if moved:
            self.event("UNWIND", f"{bk['race']}: unwind filled unevenly; one-leg residue to the recycler: {', '.join(moved)}")
        elif bk["state"] == "closed":
            self.event("UNWIND", f"{bk['race']}: set fully unwound (edge realized, capital freed)")

    def adopted(self, entry, ids):
        """A pair set whose atomic write had an UNKNOWN outcome and that the audit found on SIG: record it as a set
        (with repair tracking), as if its write had been acked."""
        items = entry.get("items", [])
        subs = {it["sub"] for it in items}
        if entry.get("kind") != "multileg" or len(subs) != 1 or not next(iter(subs)).startswith("pairset:") or len(ids) != len(items):
            return
        set_id = next(iter(subs))[len("pairset:"):]
        if any(bk["id"] == set_id for bk in self.ledger.baskets):
            return
        side, prices = items[0]["order"][1], [it["order"][3] for it in items]
        race = items[0]["bot"][len("PAIR "):]
        pb = next((p for p in self.pairs if p.race == race), None)
        out = pb.outsider(self.fair, PAIR_THIRD_PARTY) if pb and side == "yes" else 0.0
        margin = (1 - sum(prices)) if side == "no" else (1 - (out or PAIR_THIRD_PARTY) - sum(prices))
        with self.ledger.lock:
            self.ledger.baskets.append(dict(id=set_id, t=stamp(), ts=entry.get("ts", time.time()), race=race, side=side,
                                            qty=int(items[0]["order"][4]), margin=round(margin, 4), legs=len(items),
                                            orders=[str(i) for i in ids], repaired=False, state="held", adopted=True))
        self.event("AUDIT", f"pair set {set_id} adopted after an unknown write: recorded with repair tracking")

    def repair_baskets(self, now):
        """A12.4, in sequence for a set whose legs filled unevenly by its expiry:
          1 complete the short leg at the break-even price (2-leg sets; TTL PAIR_TTL_S)
          2 wait until REST shows that order gone (filled or expired)
          3 still uneven: unwind the long leg's excess at its average price (TTL PAIR_TTL_S)
          4 gone and still uneven: the residue goes to 'inventory' (the recycler)."""
        open_subs = {}
        for o in self.open:
            rec = self.ledger.orders.get(o["id"])
            if rec and rec.get("sub", "").startswith("pairset:"):
                open_subs.setdefault(rec["sub"], set()).add(rec["layer"])
        for p in self.ex.pending():
            if p["sub"].startswith("pairset:"):
                open_subs.setdefault(p["sub"], set()).add(p["layer"])
        for bk in self.ledger.baskets:
            st = bk.get("state", "held")
            if st in ("unwinding", "unwound") and now - bk.get("unwind_t", now) >= PAIR_TTL_S + 10 and \
                    "unwind" not in open_subs.get(f"pairset:{bk['id']}", set()):
                self.settle_unwind(bk)                      # its legs fill independently: even it up afterwards
                continue
            if bk.get("repaired") and st == "held" and not bk.get("repair_state"):
                continue                                    # sets from before v2: handled then
            sub = f"pairset:{bk['id']}"
            if st == "held" and not bk.get("repair_state"):
                if now - bk.get("ts", now) < PAIR_TTL_S + 10:
                    continue
                legs = self.ledger.basket_fills(bk)
                if len(legs) != bk["legs"] or max(l[3] for l in legs) - min(l[3] for l in legs) < MIN_QTY:
                    bk["repaired"] = True                   # even (or nothing filled): nothing to repair
                    continue
                full, short = max(legs, key=lambda l: l[3]), min(legs, key=lambda l: l[3])
                if bk["legs"] == 2 and full[4] is not None and not self.ex.is_frozen(short[1]):
                    limit, qty = bots.tick_down(1 - full[4]), int(full[3] - short[3])
                    if limit >= 0.01:
                        def rec(results, bk=bk, leg=short[0]):
                            for _, _, ok, d in results:
                                if ok and d.get("orderId") is not None:
                                    with self.ledger.lock:
                                        bk.setdefault("repairs", {}).setdefault(leg, []).append(str(d["orderId"]))
                        self.ex.place([(f"PAIR {bk['race']}", sub, "repair", (short[1], short[2], "buy", limit, qty))],
                                      ttl_s=PAIR_TTL_S, on_done=rec)
                        bk.update(repair_state="complete", repair_t=now, state="repair")
                        self.event("REPAIR", f"{bk['race']}: 1/3 complete the short leg {short[2].upper()} {qty:,} @ <= {limit:.3f}")
                        continue
                bk.update(repair_state="unwind_due", repair_t=now, state="repair")
                continue
            rs = bk.get("repair_state")
            if not rs or rs == "done" or "repair" in open_subs.get(sub, set()) or now - bk.get("repair_t", now) < 10:
                continue                                    # 2: wait until REST shows the repair order gone
            legs = self.ledger.basket_fills(bk)
            spread = max(l[3] for l in legs) - min(l[3] for l in legs) if legs else 0
            if spread < MIN_QTY:
                bk.update(repair_state="done", state="held", repaired=True)
                self.event("REPAIR", f"{bk['race']}: set complete after repair")
                continue
            if rs in ("complete", "unwind_due"):
                full = max(legs, key=lambda l: l[3])
                excess = int(full[3] - min(l[3] for l in legs))
                have = full[2]
                if full[4] is not None and not self.ex.is_frozen(full[1]):
                    close = "no" if have == "yes" else "yes"
                    px = bots.tick_down(1 - full[4])
                    def rec_u(results, bk=bk, leg=full[0]):
                        for _, _, ok, d in results:
                            if ok and d.get("orderId") is not None:
                                with self.ledger.lock:
                                    bk.setdefault("unwinds", {}).setdefault(leg, []).append(str(d["orderId"]))
                    self.ex.place([(f"PAIR {bk['race']}", sub, "repair", (full[1], close, "buy", px, excess))], ttl_s=PAIR_TTL_S,
                                  on_done=rec_u)
                    self.event("REPAIR", f"{bk['race']}: 3/3 unwind the long leg's excess {excess:,} at its average {full[4]:.3f}")
                bk.update(repair_state="unwind", repair_t=now)
                continue
            if rs == "unwind":                              # 4: what is left goes to the recycler
                full = max(legs, key=lambda l: l[3])
                excess = full[3] - min(l[3] for l in legs)
                sign = 1 if full[2] == "yes" else -1
                self.ledger.split_sub(full[1], sub, "inventory", sign * excess)
                bk.update(repair_state="done", state="held", repaired=True)
                self.event("REPAIR", f"{bk['race']}: residue {excess:,.0f} handed to the recycler (inventory)")

    # ------------------------------------------------------------------------------------------------ stop / outage
    def pull_at_risk(self, limit_s=60):
        """Stop / outage path. Halt placements (queued ones dropped, nothing new sent), then cancel engine-owned market
        and pair orders within 3c of the touch (the ones that get picked off first), closest first, one DELETE each
        through the cancel lane, within the write budget and limit_s. A write already on the wire when the stop began
        may land quotes at the touch: when it finishes, those jump the cancel queue. Never a tournament-wide cancel,
        never re-places anything, never touches position exits or orders the engine didn't place: they stay, and the
        engine's other quotes expire on SIG by themselves (TTL). A write whose outcome is UNKNOWN stays frozen and is
        resolved by the next start's audit; its orders (if they landed) expire by TTL."""
        dropped, end = self.ex.halt(), time.time() + limit_s
        keep = {b.name for b in self.bots if b.kind == "position"}
        try:
            orders = api.open_orders()
        except Exception as e:
            self.event("ERR", f"stop: open orders unreadable, using the last snapshot ({str(e)[:80]})")
            orders = self.open
        with self.ex.wake:                                     # one consistent view: acked since the snapshot vs on the wire
            wire_keys = set(self.ex.in_flight)
            wire = {it[3][0] for job in self.ex.in_flight.values() for it in job["items"]}
            acked = [dict(v[0]) for v in self.ex.recent.values()]
        known = {o["id"] for o in orders}
        orders = orders + [o for o in acked if o["id"] not in known]
        try:
            px = api.prices({o["ex"] for o in orders} | wire) if orders or wire else {}
        except Exception:
            px = self.px

        def at_risk(cands):
            """Engine-owned market/pair buys within 3c of their side's best bid, closest first."""
            owned = {i for i, o in self.ledger.orders.items() if o["status"] == "open" and o["bot"] not in keep and o["bot"] != "manual"}
            out = []
            for o in cands:
                if o["id"] not in owned or o["action"] != "buy" or self.ledger.orders[o["id"]]["layer"] in self.REDUCING:
                    continue                                # exits, rebounds, unwinds and repairs only reduce risk: they stay
                bid, ask, _ = px.get(o["ex"], (None, None, None))
                best = bid if o["side"] == "yes" else (None if ask is None else 1 - ask)  # best bid of the order's own side
                gap = -1.0 if best is None else best - o["price"]                          # <= 0: we are the touch
                if gap <= 0.03 + 1e-9:
                    out.append((gap, o))
            return [o for _, o in sorted(out, key=lambda t: t[0])]

        first = at_risk(orders)
        for o in first:
            self.ex.cancel(o, reason="stop")
        seen, late, waiting = {o["id"] for o in orders}, [], bool(wire_keys)
        while time.time() < end:
            with self.ex.wake:
                finished = not (wire_keys & set(self.ex.in_flight))
                acked = [dict(v[0]) for v in self.ex.recent.values() if v[0]["id"] not in seen]
            if waiting and finished:                           # the write on the wire at the stop has finished
                waiting = False
                late = at_risk(acked)
                for o in reversed(late):
                    self.ex.cancel(o, reason="stop", front=True)
            if not self.ex.cancels and not waiting:
                break
            if not self.ex.step_cancels(max_n=1):              # one at a time: late quotes can jump the queue
                time.sleep(0.2)
        risky = first + late
        done = sum(1 for o in risky if self.ledger.orders.get(o["id"], {}).get("status") == "cancelled")
        left = sum(1 for o in self.ledger.orders.values() if o["status"] == "open" and o["bot"] not in keep)
        self.event("PULL", f"dropped {dropped} queued placements; cancelled {done}/{len(risky)} engine orders within 3c of the "
                           f"touch ({len(late)} landed by a write on the wire at the stop); {left - done} other engine "
                           f"orders left to expire; {sum(1 for o in orders if (self.ledger.orders.get(o['id']) or {}).get('bot', 'manual') in keep | {'manual'} or o['id'] not in self.ledger.orders)} "
                           f"position-exit/foreign orders untouched")
        self.stop_report = dict(dropped=dropped, at_risk=[o["id"] for o in risky], cancelled=done, late=[o["id"] for o in late],
                                wire_keys=sorted(wire_keys))
        return done

    # ------------------------------------------------------------------------------------------------ night log
    def account_fair(self):
        """(account at Polymarket fair, free cash): cash + every holding at fair (SIG valuation where there is no fair).
        The score basis (plan_review §1)."""
        a = self.acct or {}
        cash = (a.get("totalAccountValue") or 0) - (a.get("totalHoldingsValue") or 0)
        hold = 0.0
        for ex, b in self.book.items():
            q, row = b["pos"], b["row"]
            f = self.side_fair(ex, "yes" if q > 0 else "no")
            hold += abs(q) * f if f is not None else float(row.get("marketValue") or 0)
        return cash + hold, cash

    def fills_since(self, since):
        out = []
        for f in self.ledger.fills:
            if f["before_engine"]:
                continue
            try:
                t = time.mktime(time.strptime(f["t"], "%Y-%m-%d %H:%M:%S"))
            except ValueError:
                continue
            if t + 1 > since:                               # fill times have 1 s resolution
                out.append(f)
        return out

    def realized_today(self):
        today = time.strftime("%Y-%m-%d")
        return sum(f.get("realized") or 0 for f in self.ledger.fills if f["t"][:10] == today)

    def hourly(self, force=False):
        """Once an hour (and at startup): an entry in docs/NIGHT_LOG.md and a refreshed status block at its top."""
        h = time.strftime("%Y-%m-%d %H")
        if api.DRY_RUN or not self.acct or not self.book or (h == self.hour and not force):
            return
        self.hour = h
        fair_now, cash = self.account_fair()
        c = self.controls
        night = c.d.setdefault("night", {})
        if "start_fair" not in night:
            night.update(start_fair=round(fair_now), start_t=stamp())
            c.save()
        per = {}
        for f in self.fills_since(time.time() - 3600):
            k = f["layer"] or f["bot"]
            per[k] = per.get(k, 0) + 1
        live = " · ".join(f"{l} {c.state(l)}" for l in LAYERS) + (" · NEW RISK PAUSED" if c.paused else "")
        kills = [f"{l}: {v['killed']['reason']}" for l, v in c.d["layers"].items() if v.get("killed")]
        st = self.ex.stats()
        self.night.entry(f"{time.strftime('%H:%M')} PT (engine, hourly)", [
            f"live: {live}",
            f"account @ fair {fair_now:,.0f} (night start {night['start_fair']:,}, {fair_now - night['start_fair']:+,.0f}) · "
            f"leaderboard {(self.acct or {}).get('totalAccountValue', 0):,.0f} (SIG marks, not the score) · free cash {cash:,.0f} · "
            f"realized today {self.realized_today():+,.0f}",
            f"fills last hour: {', '.join(f'{k} {n}' for k, n in sorted(per.items())) or 'none'}",
            "latency p50/p90 s: " + ", ".join(f"{k} {v['p50']}/{v['p90']} (n {v['n']})" for k, v in self.latency_view().items()),
            f"kills/rollbacks: {'; '.join(kills) or 'none'} · errors last hour {len(self.errors)} · loop {self.n}, "
            f"last {self.t.get('loop', 0):.0f} s · journal UNKNOWN {st['unknown']} · executor sent {st['sent_orders']}"])
        self.night.status([
            f"**Live** ({time.strftime('%H:%M')} PT): {live}",
            f"**Account at fair:** {night['start_fair']:,} at night start ({night['start_t'][11:16]}) -> {fair_now:,.0f} now "
            f"({fair_now - night['start_fair']:+,.0f})",
            f"**Realized P&L today:** {self.realized_today():+,.0f} · **free cash** {cash:,.0f}",
            f"**Kills / rollbacks:** {'; '.join(kills) or 'none'}",
            f"**For Piero to decide:** {'; '.join(c.d.get('decisions') or []) or 'nothing'}"])

    # ------------------------------------------------------------------------------------------------ F5 state
    def pnl_of(self, ex):
        """P&L of the holding on ex at SIG valuation (leaderboard), at the SIG book mid, and at Polymarket fair."""
        b = self.book.get(ex)
        if not b or not b["pos"]:
            return None, None, None
        q, row = b["pos"], b["row"]
        side = "yes" if q > 0 else "no"
        cost = abs(q) * row["avgCost"]
        mid, fair = self.side_mid(ex, side), self.side_fair(ex, side)
        return (row["marketValue"] - cost, None if mid is None else abs(q) * mid - cost, None if fair is None else abs(q) * fair - cost)

    def write_state(self, fresh, stopped=False):
        orders = self.ledger.orders
        fills = [f for f in self.ledger.fills if not f["before_engine"]]
        open_rows = []
        for o in self.open:
            known = orders.get(o["id"])
            open_rows.append(dict(o, bot=known["bot"] if known else self.bot_of(o["ex"]), layer=(known or {}).get("layer", ""),
                                  market=self.name(o["ex"]), origin="engine" if known else "not placed by engine",
                                  yes_px=o["price"] if o["side"] == "yes" else round(1 - o["price"], 3)))
        per = {}
        for o in open_rows:
            per.setdefault(o["bot"], dict(resting=0, placed=0, fills=0, filled=0.0, edge=0.0, sig_edge=0.0))["resting"] += 1
        for o in orders.values():
            per.setdefault(o["bot"], dict(resting=0, placed=0, fills=0, filled=0.0, edge=0.0, sig_edge=0.0))["placed"] += 1
        for f in fills:
            d = per.setdefault(f["bot"], dict(resting=0, placed=0, fills=0, filled=0.0, edge=0.0, sig_edge=0.0))
            d["fills"] += 1
            d["filled"] += f["qty"]
            d["edge"] += (f["edge"] or 0) * f["qty"]
            d["sig_edge"] += (f["sig_edge"] or 0) * f["qty"]

        # P&L attribution: every holding belongs to a position bot, a market bot, or is manual
        groups = {}
        def add(group, ex):
            v, bk, fa = self.pnl_of(ex)
            g = groups.setdefault(group, dict(val=0.0, book=0.0, fair=0.0, n=0))
            if v is not None:
                g["val"] += v
                g["book"] += bk if bk is not None else v
                g["fair"] += fa if fa is not None else (bk if bk is not None else v)
                g["n"] += 1
        hold = self.ledger.arb_hold()
        for ex in self.book:
            o = self.owner.get(ex)
            add(o.name if o and o.kind == "position" else "market bots" if o else "manual / other", ex)
        a = self.acct or {}
        total = (a.get("totalAccountValue") or INITIAL_BALANCE) - INITIAL_BALANCE
        open_val = sum(g["val"] for g in groups.values())
        groups["closed trades (derived)"] = dict(val=total - open_val, book=total - open_val, fair=total - open_val, n=0)
        total_book = sum(g["book"] for g in groups.values())
        total_fair = sum(g["fair"] for g in groups.values())

        rows = []
        for b in self.bots:
            s = getattr(b, "view", {}).get("s", {})
            v, bk, fa = self.pnl_of(b.ex)
            row = self.book.get(b.ex, {}).get("row") or {}
            p = per.get(b.name, {})
            rows.append(dict(name=b.name, kind=b.kind, ex=b.ex, market=b.market["title"], status=b.status, pos=self.book.get(b.ex, {}).get("pos", 0),
                             bot_pos=s.get("pos", 0), arb_pos=hold.get(b.ex, 0), avg=row.get("avgCost"), fair=s.get("fair"),
                             bid=s.get("bid"), ask=s.get("ask"), valuation=row.get("currentPrice"), focus=b.ex in self.focus,
                             pnl_val=v, pnl_book=bk, pnl_fair=fa, resting=p.get("resting", 0), placed=p.get("placed", 0),
                             fills=p.get("fills", 0), filled=p.get("filled", 0), edge=round(p.get("edge", 0), 2),
                             sig_edge=round(p.get("sig_edge", 0), 2), ladder=getattr(b, "ladder", None),
                             orders=[dict(layer=w["layer"], side=w["side"], action=w["action"], px=w["px"], qty=w["qty"]) for w in getattr(b, "view", {}).get("wants", [])]))
        pairs = []
        for pb in self.pairs:
            bks = [bk for bk in self.ledger.baskets if bk["race"] == pb.race]
            locked = sum(self.ledger.matched(bk) * bk["margin"] for bk in bks)
            one_leg = sum(sum(l[3] for l in self.ledger.basket_fills(bk)) - self.ledger.matched(bk) * bk["legs"] for bk in bks)
            pairs.append(dict(name=pb.name, race=pb.race, parties=pb.parties, status=pb.status, **pb.view,
                              sets=len(bks), locked=round(locked, 2), one_leg=round(one_leg)))
        # scorecard: is each strategy making money on SIG? edge at fill, then mark-outs (+ = SIG moved our way after the fill)
        def strategy(f):
            if f["bot"].startswith("MKT"):
                return f"market · {f['layer'] or '-'}"
            if f["bot"].startswith("PAIR"):
                return f"pair · {f['layer'] or 'set'}"
            if f["bot"].startswith("MM"):
                return "old market maker (PM ±2c)"
            if f["bot"].startswith("ARB"):
                return "old arb"
            return f"{f['bot']} · {f['layer'] or '-'}"
        card = {}
        for f in fills:
            if f["action"] not in ("buy", "sell"):
                continue
            c = card.setdefault(strategy(f), dict(fills=0, shares=0.0, sig=0.0, sig_n=0.0, pm=0.0, pm_n=0.0, mo5=0.0, mo5_n=0.0,
                                                   mo30=0.0, mo30_n=0.0, now=0.0, now_n=0.0))
            q, sgn = f["qty"], 1 if f["action"] == "buy" else -1
            c["fills"] += 1
            c["shares"] += q
            for k, v in (("sig", f["sig_edge"]), ("pm", f["edge"]), ("mo5", f.get("mo5")), ("mo30", f.get("mo30"))):
                if v is not None:
                    c[k] += v * q
                    c[k + "_n"] += q
            m = self.side_mid(f["ex"], f["side"])
            if m is not None:
                c["now"] += sgn * (m - f["price"]) * q
                c["now_n"] += q
        scorecard = [dict(strategy=k, **{x: round(y, 2) for x, y in v.items()}) for k, v in sorted(card.items(), key=lambda kv: -kv[1]["shares"])]
        for r in rows:
            r["subbooks"] = dict(self.ledger.subbooks.get(r["ex"], {}))
        state = dict(time=stamp(), live=not api.DRY_RUN, stopped=stopped, loop=self.n, pm_fresh=fresh, scorecard=scorecard,
                     executor=self.ex.stats(),
                     timing={k: round(v, 1) for k, v in self.t.items()}, loop_s=LOOP_S,
                     pm_age_s=round(time.time() - self.fair_t) if self.fair_t else None, writes_left=api.writes_left(),
                     account=a, summary=self.summary, risk=self.risk_view, initial=INITIAL_BALANCE,
                     pnl=dict(total=total, book=total_book, fair=total_fair, groups=groups),
                     bots=rows, pairs=pairs, open_orders=open_rows,
                     orders=[dict(o, market=self.name(o["ex"])) for o in sorted(orders.values(), key=lambda o: o["placed"], reverse=True)[:1500]],
                     fills=[dict(f, market=self.name(f["ex"])) for f in reversed(self.ledger.fills[-1000:])],
                     events=self.events[-200:][::-1])
        try:
            self.last_state = self.dash_state(stopped)      # read by the dashboard thread; never mutated after this
        except Exception as e:                              # the dashboard must never stop trading
            self.event("ERR", f"dashboard state: {e!r}")
        if not (stopped or time.time() - self.state_t >= STATE_EVERY_S):
            return
        self.state_t = time.time()
        write_atomic(STATE_FILE, json.dumps(state, default=str))
        if not api.DRY_RUN and time.time() - self.pnl_t >= PNL_EVERY_S and a:
            self.pnl_t = time.time()
            new = not os.path.exists(PNL_FILE)
            with open(PNL_FILE, "a", newline="") as f:
                w = csv.writer(f)
                if new:
                    w.writerow(["time", "account_value", "account_book", "account_pm", "unrealized", "roi_pct"])
                w.writerow([stamp(), a["totalAccountValue"], round(INITIAL_BALANCE + total_book, 2),
                            round(INITIAL_BALANCE + total_fair, 2), a["unrealizedPnl"], a["roi"]])
        if self.last_state:
            write_atomic(DASHBOARD_FILE, dashboard.render(self.last_state))

    # ------------------------------------------------------------------------------------------------ dashboard v2
    def strategy_of(self, bot, layer):
        """Which strategy layer an order/fill belongs to (the dashboard's rows and the kill rules)."""
        if bot in self.position_names:
            return "position"
        if bot.startswith("PAIR"):
            return "pairs"
        if bot.startswith("MKT"):
            return self.MARKET_LAYERS.get(layer, layer or "other")
        return "old engine"

    def kappa(self):
        """(kappa, why): the capital discount for selling below fair (D3). Override from the dashboard if set; 3c from
        Oct 30; else by free budget = (free cash - committed entry buys) / account at fair: >20% 0, 5-20% 1c, <5% 2c."""
        o = self.controls.kappa_override()
        if o is not None:
            return o, "dashboard override"
        if datetime.now().strftime("%m-%d") >= "10-30":
            return KAPPA_FROM_OCT30, "from Oct 30 (election-night cash)"
        fair, cash = self.account_fair()
        free = (cash - self.committed()) / fair if fair > 0 else 0.0
        k = 0.0 if free > 0.20 else 0.01 if free >= 0.05 else 0.02
        return k, f"free budget {free * 100:.1f}%"

    FLOOR_LAYERS = ("mm", "take", "set")                   # §4.2: the cash floor counts these resting buys (not ladders)

    def committed(self):
        """SIG committed by engine entry buys under the cash floor (MM, takes, pair sets): resting, queued or in flight,
        at full notional. Ladders are excluded: they have their own fill cap and breaker (night plan §4.2)."""
        out = 0.0
        for o in self.open:
            rec = self.ledger.orders.get(o["id"])
            if rec and o["action"] == "buy" and rec["layer"] in self.FLOOR_LAYERS and not jx.reduces(
                    o["side"], "buy", self.book.get(o["ex"], {}).get("pos", 0)):
                out += o["qty"] * o["price"]
        for p in self.ex.pending():
            if p["action"] == "buy" and p["layer"] in self.FLOOR_LAYERS:
                out += p["qty"] * p["price"]
        return out

    def cap(self, layer, key):
        """Current cap of a layer's bucket: the dashboard's override if set, else pilot/full by the layer's state."""
        pilot, full, mx = LAYER_CAPS[layer][key]
        v = (self.controls.layer(layer).get("caps") or {}).get(key)
        return max(0.0, min(float(mx), float(v))) if v is not None else float(full if self.controls.state(layer) == "full" else pilot)

    def cap_used(self, layer, key):
        return getattr(self, "used", {}).get((layer, key), 0.0)

    def fill_pnl_fair(self, f):
        """P&L of one fill at today's fair (SIG mid of its side where there is no fair)."""
        v = self.side_fair(f["ex"], f["side"])
        v = self.side_mid(f["ex"], f["side"]) if v is None else v
        if v is None or f["action"] not in ("buy", "sell"):
            return 0.0
        return (v - f["price"]) * f["qty"] * (1 if f["action"] == "buy" else -1)

    def layer_stats(self, since):
        """{layer: fills, shares, edge (vs fair at fill, per share), markout30, pnl_fair, realized, resting} since `since`
        (epoch seconds, a per-layer dict or one number)."""
        out = {}
        def row(l):
            return out.setdefault(l, dict(fills=0, shares=0.0, edge_w=0.0, edge_n=0.0, mo_w=0.0, mo_n=0.0, pnl_fair=0.0,
                                          realized=0.0, resting=0))
        t0 = min(since.values()) if isinstance(since, dict) and since else (since if not isinstance(since, dict) else time.time())
        for f in self.fills_since(t0):
            l = self.strategy_of(f["bot"], f["layer"])
            if isinstance(since, dict) and time.mktime(time.strptime(f["t"], "%Y-%m-%d %H:%M:%S")) + 1 <= since.get(l, t0):
                continue                                    # fill times have 1 s resolution
            r = row(l)
            r["fills"] += 1
            r["shares"] += f["qty"]
            if f["edge"] is not None:
                r["edge_w"] += f["edge"] * f["qty"]
                r["edge_n"] += f["qty"]
            if f.get("mo30") is not None:
                r["mo_w"] += f["mo30"]
                r["mo_n"] += 1
            r["pnl_fair"] += self.fill_pnl_fair(f)
            r["realized"] += f.get("realized") or 0
        for o in self.open:
            rec = self.ledger.orders.get(o["id"])
            if rec:
                row(self.strategy_of(rec["bot"], rec["layer"]))["resting"] += 1
        for r in out.values():
            r["edge"] = r["edge_w"] / r["edge_n"] if r["edge_n"] else None
            r["markout30"] = r["mo_w"] / r["mo_n"] if r["mo_n"] else None
        return out

    def leaders_payload(self):
        path = os.path.join(DATA, "leaders.csv")
        if not os.path.exists(path):
            return {}
        with open(path, newline="") as fh:
            rows = list(csv.DictReader(fh))[-21 * 300:]
        if not rows:
            return {}
        last_t = rows[-1]["time"]
        latest = [r for r in rows if r["time"] == last_t]
        names = {r["name"] for r in latest}
        series = {}
        for r in rows:
            if r["name"] in names:
                t = time.mktime(time.strptime(r["time"], "%Y-%m-%d %H:%M:%S"))
                series.setdefault(r["name"], dict(name=r["name"], ours=r["ours"] == "1", points=[]))["points"].append((t, float(r["value"])))
        return dict(series=list(series.values()), latest=[dict(rank=r["rank"], name=r["name"], value=float(r["value"]),
                                                             trades=r["trades"], ours=r["ours"] == "1") for r in latest])

    def dash_state(self, stopped=False):
        """The dashboard payload: small (well under 300 KB), built fresh each loop, never mutated after."""
        c = self.controls
        fair_now, cash = self.account_fair() if self.book else (None, None)
        night = c.d.setdefault("night", {})
        day = time.strftime("%Y-%m-%d")
        if fair_now and night.get("day") != day:
            night.update(day=day, day_fair=round(fair_now))
        k, why = self.kappa() if self.book else (None, "")
        start = night.get("start_t")
        night_t = time.mktime(time.strptime(start, "%Y-%m-%d %H:%M:%S")) if start else self.started
        since = {l: (c.layer(l).get("since") or night_t) if c.on(l) else night_t for l in LAYERS}
        stats = self.layer_stats(since)
        bot_24h = sum(self.fill_pnl_fair(f) for f in self.fills_since(time.time() - 86400)
                      if self.strategy_of(f["bot"], f["layer"]) not in ("position", "old engine"))
        layers = []
        for l in LAYERS:
            v, st = c.layer(l), stats.get(l, {})
            layers.append(dict(name=l, state=c.state(l), since=v.get("since"), killed=v.get("killed"), note=self.layer_note(l),
                               caps={key: dict(cap=self.cap(l, key), used=round(self.cap_used(l, key)), min=0, max=LAYER_CAPS[l][key][2])
                                     for key in LAYER_CAPS.get(l, {})},
                               resting=st.get("resting", 0), fills=st.get("fills", 0), edge=st.get("edge"), markout30=st.get("markout30"),
                               pnl_fair=round(st.get("pnl_fair", 0.0), 2), realized=round(st.get("realized", 0.0), 2),
                               kill=self.kill_status(l, st)))
        last = {}
        for f in self.ledger.fills[-3000:]:
            last[f["ex"]] = f"{f['t'][11:16]} {f['action'] or 'trade'} {f['side'].upper()} {f['qty']:,.0f} @ {f['price']:.3f}"
        resting = {}
        for o in self.open:
            rec = self.ledger.orders.get(o["id"])
            l = self.strategy_of(rec["bot"], rec["layer"]) if rec else "foreign"
            resting.setdefault(o["ex"], {}).setdefault(l, 0)
            resting[o["ex"]][l] += 1
        markets = []
        for b in self.bots:
            bid, ask, _ = self.px.get(b.ex, (None, None, None))
            f, q = self.fair.get(b.ex), self.pm.get(b.ex)
            mid = None if bid is None or ask is None else (bid + ask) / 2
            m = c.market(b.ex)
            markets.append(dict(ex=b.ex, title=b.market["title"], race=b.market["race"], party=b.market["party"], bot=b.name,
                                fair=None if f is None else round(f, 4), u=None if not q else round((q[1] - q[0]) / 2, 4),
                                bid=bid, ask=ask, mis=None if mid is None or f is None else round(mid - f, 4),
                                pos=self.book.get(b.ex, {}).get("pos", 0),
                                subs={k2: v2 for k2, v2 in self.ledger.subbooks.get(b.ex, {}).items() if abs(v2) >= 1},
                                resting=resting.get(b.ex, {}), last_fill=last.get(b.ex, ""), paused=bool(m.get("paused")),
                                exclude=m.get("exclude") or []))
        positions = []
        for b in self.bots:
            if b.kind == "position":
                positions.append(dict(name=b.name, market=b.market["title"], side=b.side, held=abs(self.book.get(b.ex, {}).get("pos", 0)),
                                      manual=abs(self.ledger.sub(b.ex, "manual")), ladder=b.ladder, status=b.status,
                                      tranche=self.d1_status(b)))
        sets, used, locked = [], 0.0, 0.0
        for bk in self.ledger.baskets[-200:]:
            legs = self.ledger.basket_fills(bk)
            if not any(l[3] for l in legs):
                continue
            m = self.ledger.matched(bk)
            res = sum(l[3] for l in legs) - m * bk["legs"]
            cap_ = sum(l[3] * (l[4] or 0) for l in legs)            # a leg with no fill has no average price
            used += cap_
            locked += m * bk["margin"]
            sets.append(dict(race=bk["race"], side=bk["side"], qty=m, locked=round(m * bk["margin"], 2), residue=round(res),
                             capital=round(cap_), state=bk.get("state", "held"), unwind_value=bk.get("unwind_value"),
                             threshold=bk.get("unwind_at")))
        st = self.ex.stats()
        a = self.acct or {}
        return dict(
            t=stamp(), loop=self.n, live=not api.DRY_RUN, stopped=stopped,
            overview=dict(account_fair=None if fair_now is None else round(fair_now), free_cash=None if cash is None else round(cash),
                          change_today=None if fair_now is None or "day_fair" not in night else round(fair_now - night["day_fair"]),
                          change_start=None if fair_now is None or "start_fair" not in night else round(fair_now - night["start_fair"]),
                          realized_today=round(self.realized_today(), 2), reserve=CASH_RESERVE, committed=round(self.committed()),
                          kappa=k, kappa_src=why, leaderboard=a.get("totalAccountValue"), paused=c.paused,
                          read_only=bool(c.d.get("read_only")),
                          loss_stop=dict(pnl=round(bot_24h), limit=LOSS_STOP, tripped=bot_24h < -LOSS_STOP)),
            health=dict(sig=api.health(), executor=st, loop_s=self.t.get("loop"), errors=len(self.errors), latency=self.latency_view(),
                        realtime=dict(sig=getattr(self.sig_feed, "stats", None), pm=getattr(self.pm_feed, "stats", None)),
                        feeds={"SIG prices (REST)": self.px_t, "Polymarket": self.fair_t or None,
                               "SIG realtime": getattr(self.sig_feed, "last_msg_t", None),
                               "Polymarket realtime": getattr(self.pm_feed, "last_msg_t", None)}),
            layers=layers, markets=markets, positions=positions,
            pairs=dict(sets=sets[-60:], used=round(used), cap=self.cap("pairs", "capital"), locked=round(locked, 2)),
            leaders=self.leaders_payload(), alerts=list(self.alerts) + [f"decide: {d}" for d in c.d.get("decisions") or []],
            events=[dict(e) for e in self.events[-300:][::-1]])

    def layer_note(self, layer):
        return {"position": "AK + RI exit ladders (always on)", "recycler": "sells inventory into bids >= fair + 0.5c",
                "mm": "two-sided quotes at fair +/- e(p)", "ladder": "deep bids for sweeps, rebounds after fills",
                "pairs": "atomic NO/YES sets, early unwind", "sniper": "new markets: take asks far below fair",
                "leaders": "leaderboard logger (read only)"}.get(layer, "")

    def kill_status(self, layer, st):
        """The kill rules of a layer (night_plan §4.4) with their current values."""
        if layer in ("position", "leaders"):
            return []
        full = max((LAYER_CAPS.get(layer, {}).get(k, (0, 0, 0))[1] for k in LAYER_CAPS.get(layer, {})), default=0)
        lim = -max(300.0, 0.03 * full)
        pnl, n, mo = st.get("pnl_fair", 0.0), st.get("fills", 0), st.get("markout30")
        rej = self.reject_rate(layer)
        return [dict(rule=f"P&L at fair > {lim:,.0f}", value=f"{pnl:+,.0f}", hit=pnl < lim),
                dict(rule="mark-out 30m >= -1c (after 30 fills)", value=f"{n} fills" + ("" if mo is None else f", {mo * 100:+.2f}c"),
                     hit=n >= 30 and mo is not None and mo < -0.01),
                dict(rule="rejects <= 20% of last 50", value=f"{rej * 100:.0f}%", hit=rej > 0.20)]

    def reject_rate(self, layer):
        recs = [o for o in self.ledger.orders.values() if self.strategy_of(o["bot"], o["layer"]) == layer][-50:]
        return sum(1 for o in recs if o["status"] == "rejected") / len(recs) if recs else 0.0

    def market_detail(self, ex):
        orders = sorted((o for o in self.ledger.orders.values() if o["ex"] == ex), key=lambda o: o["placed"], reverse=True)[:100]
        fills = [f for f in reversed(self.ledger.fills) if f["ex"] == ex][:100]
        return dict(orders=[{k: o.get(k) for k in ("placed", "bot", "layer", "side", "action", "price", "qty", "filled", "status")} for o in orders],
                    fills=[{k: f.get(k) for k in ("t", "bot", "layer", "side", "action", "price", "qty", "edge", "mo30")} for f in fills])

    def pull_quotes(self):
        """Dashboard 'pull engine quotes': DELETE every engine-owned order that adds risk (not position exits, not
        recycler/rebound/unwind/repair orders, never foreign ones), closest to the touch first, through the cancel lane."""
        cands = []
        for o in list(self.open) + [dict(v[0]) for v in list(self.ex.recent.values())]:
            rec = self.ledger.orders.get(o["id"])
            if not rec or rec["bot"] in self.position_names or rec["layer"] in self.REDUCING or o["action"] != "buy":
                continue
            bid, ask, _ = self.px.get(o["ex"], (None, None, None))
            best = bid if o["side"] == "yes" else (None if ask is None else 1 - ask)
            cands.append((-1.0 if best is None else best - o["price"], o))
        seen, n = set(), 0
        for _, o in sorted(cands, key=lambda t: t[0]):
            if o["id"] not in seen:
                seen.add(o["id"])
                n += bool(self.ex.cancel(o, reason="dashboard pull"))
        dropped = self.ex.drop_queued(lambda b, s, l, od: l not in self.REDUCING and b not in self.position_names)
        return n, dropped


class EngineControls:
    """Dashboard controls -> the engine. Every action goes through the flags (data/overrides.json), the risk gate in
    the plan, and the executor's cancel lane; none calls SIG directly. Bounded inputs; ValueError on anything else."""
    STRATEGY = [l for l in LAYERS if l != "position"]

    def __init__(self, eng):
        self.eng = eng

    def read_only(self):
        return bool(self.eng.controls.d.get("read_only"))

    def apply(self, action, args):
        msg = self._apply(action, args)
        s = self.eng.last_state                             # show pause / read-only at once (a new dict, never mutated)
        if s and "overview" in s:
            c = self.eng.controls
            self.eng.last_state = dict(s, overview=dict(s["overview"], paused=c.paused, read_only=bool(c.d.get("read_only"))))
        return msg

    def _apply(self, action, args):
        c = self.eng.controls
        if action == "pause":
            c.set_paused(True, "dashboard", by="dashboard")
            return "new risk paused (exits, rebounds, unwinds and repairs keep running)"
        if action == "resume":
            c.set_paused(False, "dashboard", by="dashboard")
            return "new risk resumed"
        if action == "pull_quotes":
            c.set_paused(True, "pull engine quotes", by="dashboard")
            n, dropped = self.eng.pull_quotes()
            self.eng.event("CONTROL", f"dashboard pull: {n} DELETEs queued, {dropped} queued placements dropped")
            return f"new risk paused; {n} engine quotes being cancelled, {dropped} queued placements dropped"
        if action == "layer_state":
            layer, state = args.get("layer"), args.get("state")
            if layer not in self.STRATEGY or state not in LAYER_STATES:
                raise ValueError(f"layer must be one of {self.STRATEGY} and state one of {LAYER_STATES}")
            c.set_layer(layer, state, "dashboard", by="dashboard")
            return f"{layer}: {state}"
        if action == "layer_caps":
            layer, caps = args.get("layer"), args.get("caps") or {}
            if layer not in LAYER_CAPS or not isinstance(caps, dict) or not set(caps) <= set(LAYER_CAPS[layer]):
                raise ValueError(f"caps for {layer}: keys {sorted(LAYER_CAPS.get(layer, {}))}")
            clean = {}
            for k, v in caps.items():
                v = float(v)
                if not 0 <= v <= LAYER_CAPS[layer][k][2]:
                    raise ValueError(f"{layer}.{k} must be within 0..{LAYER_CAPS[layer][k][2]:,}")
                clean[k] = v
            c.set_caps(layer, clean, by="dashboard")
            return f"{layer} caps {clean}"
        if action == "market":
            ex = str(args.get("ex"))
            if ex not in self.eng.owner:
                raise ValueError(f"unknown market {ex}")
            kw = {}
            if "paused" in args:
                kw["paused"] = bool(args["paused"])
            if "exclude" in args:
                ex_l = list(args["exclude"] or [])
                if not set(ex_l) <= set(self.STRATEGY):
                    raise ValueError(f"exclude only from {self.STRATEGY}")
                kw["exclude"] = ex_l
            if not kw:
                raise ValueError("nothing to change")
            c.set_market(ex, by="dashboard", **kw)
            return f"market {ex}: {kw}"
        if action == "kappa":
            v, minutes = args.get("value"), float(args.get("minutes") or 60)
            if v is not None and not 0 <= float(v) <= 0.03:
                raise ValueError("kappa must be within 0..3c")
            if not 1 <= minutes <= 1440:
                raise ValueError("minutes must be within 1..1440")
            c.set_kappa(v, minutes, by="dashboard")
            return "kappa override cleared" if v is None else f"kappa {float(v) * 100:.1f}c for {minutes:.0f} min"
        if action == "read_only":
            with c.lock:
                c.d["read_only"] = bool(args.get("on"))
                c.save()
            c._note(f"read-only mode {'on' if c.d['read_only'] else 'off'} (dashboard)")
            return f"read-only {'on' if c.d['read_only'] else 'off'}"
        raise ValueError(f"unknown action {action}")


def _stop(*_):
    raise KeyboardInterrupt


def main():
    if "--live" in sys.argv:
        api.DRY_RUN = False
    loops = int(arg("--loops")) if arg("--loops") else None
    only = set(arg("--only").split(",")) if arg("--only") else None
    signal.signal(signal.SIGTERM, _stop)
    os.makedirs(DATA, exist_ok=True)
    if not api.DRY_RUN and host_id() != ENGINE_HOST:
        raise SystemExit(f"refusing --live on {host_id()!r}: ENGINE_HOST is {ENGINE_HOST} (one engine across machines)")
    if not api.DRY_RUN:                                    # dry runs only read: they may run alongside
        lock = open(LOCK_FILE, "w")
        try:
            fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            raise SystemExit("another engine is already running; stop it first: kill $(cat data/engine.pid)")
        write_atomic(PID_FILE, str(os.getpid()))
    while True:                                            # SIG can be slow or down at startup: keep trying, never exit
        try:
            eng, i = Engine(only), 0
            break
        except (KeyboardInterrupt, SystemExit):
            raise
        except Exception as e:
            print(f"startup failed ({str(e)[:160]}); retrying in 30 s", flush=True)
            time.sleep(30)
    eng.ex.start()
    if not api.DRY_RUN and "--no-realtime" not in sys.argv:
        eng.start_realtime()
    dash = None
    try:
        app = dashboard.create_app(lambda: eng.last_state, EngineControls(eng), market_detail=eng.market_detail)
        dash = dashboard.serve_in_thread(app)
        if dash.started:
            write_atomic(DASHBOARD_URL_FILE, dash.url + "\n")
            os.chmod(DASHBOARD_URL_FILE, 0o600)
        print(dash, flush=True)
    except Exception as e:
        print("dashboard not started:", repr(e), flush=True)
    try:
        while loops is None or i < loops:
            t, i = time.time(), i + 1
            try:
                eng.step()
            except Exception as e:
                eng.event("ERR", repr(e))
            if loops is None or i < loops:                  # a realtime event replans early, within the read budget:
                eng.wake.wait(timeout=max(1.0, LOOP_S - (time.time() - t)))     # ~7 SIG reads a loop, 100 a minute
                eng.wake.clear()
                busy = api.health(60)["reads_last_min"] > 80    # near SIG's 100 reads/min: space the replans out
                gap = 6.0 if busy else 2.0                  # else 2 s: fill -> exit <= 3 s, PM move -> defend <= 2 s (A7)
                time.sleep(max(0.0, gap - (time.time() - t)))
    except KeyboardInterrupt:
        pass
    finally:
        try:
            eng.pull_at_risk(limit_s=60)
            print("engine stopped: at-risk engine quotes cancelled; the rest expire on SIG; position exits and manual orders untouched", flush=True)
        except Exception as e:
            print("ERR while stopping (engine quotes will expire on SIG by themselves):", repr(e), flush=True)
        eng.ex.stop()
        if dash:
            dash.stop()
        eng.ledger.save()
        eng.write_state(bool(eng.fair_t) and time.time() - eng.fair_t <= PM_MAX_AGE_S, stopped=True)


if __name__ == "__main__":
    main()
