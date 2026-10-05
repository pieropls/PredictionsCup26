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
import collections
import csv
import fcntl
from concurrent.futures import ThreadPoolExecutor
import json
import math
import os
import re
import signal
import statistics
import subprocess
import sys
import threading
import time
import uuid
from datetime import datetime, timedelta

import b_sig as api
import c_polymarket as pm
import e_bots as bots
import g_dashboard as dashboard
import i_realtime as rt
import j_executor as jx
import l_intel as intel
import m_dash4 as dash4
from a_config import (SCALP_MARKETS, SCALP_PILOT_MARKETS, SCALP_BUY_EDGE, SCALP_IMPROVE_SPREAD, SCALP_CLIP_MIN, SCALP_CLIP_MAX, SCALP_MAX_BUYS,
                      SCALP_SELL_MIN, SCALP_STEP1_S, SCALP_STEP1_GAP, SCALP_HAND_S, SCALP_INV_MARKET, SCALP_SLICE_MAX, SCALP_YIELD_RESERVE,
                      SCALP_WRITES_PER_MIN, SCALP_TTL_S, SCALP_PILOT_S, SCALP_GATE_TRADES, SCALP_GATE_PACE,
                      IDLE_USE_LOW, IDLE_USE_HIGH, IDLE_HOLD_S, IDLE_SLICE_STEP, IDLE_ROC_MIN, TICK, MB_MIN_PX, MB_MAX_PX)
from a_config import (ENGINE_HOST, LOOP_S, BATCH_SIZE, PM_MAX_AGE_S, MIN_QTY, TICK, INITIAL_BALANCE,
                      RISK_MAX_EXPOSURE, CASH_RESERVE, POSITION_BOTS, POSITION_MAX_ADD, MANUAL_MARKETS, MB_PM_MAX_SPREAD,
                      MB_FOCUS, MB_REFILL_S, MB_QUOTE_TTL_S, PAIR_ENABLED, PAIR_EDGE, PAIR_THIRD_PARTY, PAIR_MAX_QTY,
                      PAIR_MAX_SHOTS, PAIR_MIN_LOCK, PAIR_TTL_S, PAIR_COOLDOWN_S, DATA, POSITION_REF_FILE, PNL_FILE, PNL_EVERY_S,
                      STATE_FILE, DASHBOARD_FILE, PID_FILE, LOCK_FILE, OVERRIDES_FILE, NIGHT_LOG_FILE, HOST_ID_FILE,
                      DASHBOARD_URL_FILE, STATE_EVERY_S, LAYER_CAPS, KAPPA_FROM_OCT30, LOSS_STOP, MARKET_MAX_POS,
                      LADDER_LEVELS, LADDER_CAPACITY_SHARE, TAKE_NOTIONAL, REF_WINDOW_S, REF_MIN_SAMPLES, REF_MAX_SPREAD,
                      BREAKER_WINDOW_S, BREAKER_MARKETS, BREAKER_SHARE, BREAKER_PAUSE_S, MM_PILOT_MARKETS,
                      LADDER_PILOT_MARKETS, PILOT_S, REQUOTE_CANCELS_PER_MIN, BREAKER_FAIR_MOVE, AK_RELEASE,
                      OPPORTUNITY_RESERVE, ROC_MIN, ROC_MIN_TIGHT, RELEASE_EVERY_S, RELEASE_MARKETS, RELEASE_PILOT_TARGET,
                      FAT_LEVELS, FAT_SIZES, FAT_MIN_FAIR, FAT_UNMAPPED_REF, FAT_FAIR_CAP, FAT_TTL_S, FAT_QUEUE_S, FAT_REARM_S,
                      FAT_NEWS_DROP, FAT_NEWS_WINDOW_S, FAT_NEWS_HOLD_S, FAT_MOVE, FAT_CAPACITY_SHARE, FAT_BOOK_READS, QTY_TOL,
                      FAT_REFRESH_MAX_H, FILL_POLL_S, SETTLE_DATE, ROC_HOLD_MIN, RELEASE_QUIET_HOURS, LIQUIDATE, PNL_READ_S, ORDERS_READ_S, PRICE_ROTATE, LEADERS_EVERY_S, TAPE_MIN_PER_MIN,
                      BACKOFF_429_S, BACKOFF_LOOP_S, TMM_TTL_S, LADDER_TTL_S, RECYCLE_TAKE_TTL_S, TAKE_TTL_S, RECYCLE_REST_TTL_S, MM_TTL_S, DUMP_TTL_S, DUMP_MIN_FAIR, DUMP_V3_OFFSETS, DUMP_V3_WEIGHTS, DUMP_FAIR_GAP, DUMP_SLIDE, DUMP_SIDE_DIV, DUMP_SIDE_MIN,
                      DUMP_SIDE_MAX, DUMP_ARENA_MULT, DUMP_ARENA_N, DUMP_SELL_MIN, DUMP_SELL_FAIR_GAP, DUMP_STEP1_S, DUMP_STEP2_S, DUMP_STEP_MIN,
                      DUMP_STEP2_MIN, DUMP_ARENA_MAX, DUMP_SIDE_HARD_MAX, DUMP_PARTY_CAP, RATCHET_STEP, RATCHET_EVERY_S,
                      DUMP_BREAKER_SIDES, DUMP_WRITE_SHARE, DUMP_WRITES_IDLE, DUMP_MIN_SIDE_COST, DUMP_QUEUE_S, DUMP_BOOK_READS, DUMP_HOLD_S,
                      DUMP_NEWS_DROP, DUMP_BREAKER_FAIR, DUMP_PILOT_S, DUMP_CONC_CAP, RELEASE_FALLBACK_QTY, FAT_BAND_EDGES, FAT_TIER_A_ONLY, FAT_NEW_LEVEL,
                      FAT_NEW_SIZES, RELEASE_MAX_COST, LONG_MAX_FAIR, LONG_LEVELS,
                      LONG_TINY_FAIR, LONG_TINY_QTY, LONG_SIDE_MAX_COST, LONG_TTL_S, LONG_EXIT_TTL_S, LONG_EXIT_MULT, LONG_STALE_S,
                      LONG_NEWS_RISE, LONG_NEWS_WINDOW_S, LONG_NEWS_HOLD_S, LONG_BREAKER_SIDES, LONG_BREAKER_PAUSE_S, LONG_PILOT_S,
                      TMM_MID_HALF_LIFE_S, TMM_KAPPA_MAX, TMM_SIZE, TMM_CAP_SIDE, TMM_SKEW_TICKS, TMM_OFFER_MID_S, TMM_RELEASE_S,
                      INTEL_READS, INTEL_REVISIT_S, INTEL_DAILY_HOUR, HEADLINE_RACES)
from d_ledger import Ledger, stamp, write_atomic


def loop_gap(reads, ours, now):
    """Seconds between loops: 2 s on our own fills while reads are cheap, 6 s from 60 reads/min, 15 s from 75 (Oct 3 (a)),
    and 20 s for 2 min after any 429 (day 4 §4)."""
    gap = 15.0 if reads >= 75 else 6.0 if reads >= 60 or not ours else 2.0
    if now - (getattr(api, "LAST_429", [0.0])[0]) < BACKOFF_429_S:
        gap = max(gap, BACKOFF_LOOP_S)
    return gap


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
LAYERS = ["position", "recycler", "release", "mm", "scalp", "ladder", "pairs", "sniper", "fatfinger", "dump", "longshot", "leaders"]
TMM_ENABLED = True              # afternoon D-C: the engine's MM is the turnover MM (False: classic MM v2, tests only)
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
    DATA, CASH_RESERVE, OPPORTUNITY_RESERVE = DATA, CASH_RESERVE, OPPORTUNITY_RESERVE   # for m_dash4
    api, jx, read_json = api, jx, staticmethod(read_json)
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
        self.cash_msgs, self.neg_cash_n, self.through, self.cash_short = [], 0, {}, None   # afternoon D-B
        self.vanish_seen, self.cash_ok_n, self.resume_check_t = set(), 0, 0.0   # day 4 §1
        self.ex.on_cash_reject = self.cash_msgs.append
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
        self.release_now, self.release_t, self.depth_cache = {}, 0.0, {}
        self.px_rot, self.open_read_t, self.open_wsig, self.pnl_read_t, self.last_fill_exs = 0, None, None, 0.0, set()   # day 4 §4
        self.release_demand = None
        self.loop_end_t, self.cap_use_hist = None, collections.deque(maxlen=2000)   # dashboard v4
        self.release_name = os.path.basename(os.path.realpath(os.path.join(os.path.dirname(DATA), "run"))) if os.path.exists(os.path.join(os.path.dirname(DATA), "run")) else ""
        self.fat_book, self.fat_anchor, self.fat_block, self.fair_hist, self.fat_now = {}, {}, {}, {}, {}
        self.fat_sent = collections.deque(maxlen=5000)      # placement times (pilot gate: write usage)
        self.dump_now, self.dump_book, self.dump_block, self.dump_view, self.dump_ref = {}, {}, {}, {}, {}   # dump catcher
        self.scalp_now, self.scalp_view, self.scalp_inv, self.vol24, self.scalp_note, self.pause_t = {}, {}, collections.deque(maxlen=5000), {}, {}, 0.0   # day 5 §2
        try:
            self.load_vol24()
        except Exception as err:
            print(f"tape volume load: {err!r}")
        self.dump_frozen, self.dump_arena_t, self.dump_arena_set = {}, 0.0, set()   # v3 (day 4 §2)
        self.dump_quota, self.cross_log = 0, {}
        self.dump_track = read_json(os.path.join(DATA, "intel", "dump_track.json"), {}) or {}
        self.tmm_mid, self.mid24, self.mm_since, self.tmm_inv = {}, {}, {}, collections.deque(maxlen=5000)   # afternoon D-C
        self.cov_seq, self.cov_seq_t, self.truth_t = None, None, None   # evening §1: fills projection / order-truth health
        self.truth_prev, self.truth_carry, self.truth_live_t, self.blind_t = None, {}, None, collections.deque(maxlen=50)
        self.fills_blind, self.pending_backfill, self.last_booked_t = None, None, time.time()
        self.truth_read_t, self.truth_prev_cur, self.pos_fail, self.faults_pulled = None, {}, 0, None
        self.order_moved_t, self.audit_first = {}, {}       # night §1.2
        self.truth_filled = {}                              # Oct 3: booked shares per open order at the last snapshot
        self.pending_fixes = []                             # day 3 §1: subbook_fix
        self.long_now, self.long_view, self.long_block, self.long_quota = {}, {}, {}, 0   # day 3 §2: longshot catcher
        self.long_hist = {}
        self.coverage, self.unmapped_new, self.unmapped_markets, self.cov_logged = None, set(), {}, set()
        self.load_pilots()
        self.leaders_t, self.scan_t, self.seen_markets, self.new_markets = 0.0, 0.0, set(), {}
        self.wake, self.rt_lock, self.rt_pending = threading.Event(), threading.Lock(), []   # realtime -> planner
        self.fill_retry, self.uncov_since = {}, {}
        self.tape, self.intel_px, self.intel_t, self.fair_series, self.intel_s = None, {}, {}, {}, {}   # day 2 §5
        self.tape_reads_t = collections.deque(maxlen=200)
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
                     "ladder": "ladder", "rebound": "ladder", "set": "pairs", "unwind": "pairs", "repair": "pairs",
                     "release": "release", "fatfinger": "fatfinger", "dump": "dump", "dumpexit": "dump",
                     "longshot": "longshot", "longexit": "longshot", "scalp": "scalp", "scalpexit": "scalp"}
    REDUCING = {"exit", "recycle", "rebound", "unwind", "repair", "release", "dumpexit", "longexit", "scalpexit"}

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
        self.controls.reload_if_changed()
        events = self.take_rt()                             # P2: what woke us (fills, order updates, PM moves, resyncs)
        # day 4 §4 read budget: prices for markets with our orders or holdings every loop, the rest rotated (one read of
        # 100 a loop); open orders after our own writes / fills, else every 30 s; P&L every 60 s
        all_ex = sorted(set(self.owner) | {ex for pb in self.pairs for ex in pb.legs})
        core = {o["ex"] for o in self.open} | {ex for ex, b in self.book.items() if abs(b.get("pos", 0)) >= 1} | set(self.focus)
        if self.controls.on("scalp"):                       # day 5 §2: the scalper requotes on the touch, so its markets read every loop
            core |= set(self.scalp_rank(SCALP_MARKETS if self.controls.state("scalp") == "full" else SCALP_PILOT_MARKETS))
        rest = [ex for ex in all_ex if ex not in core]
        if rest:
            k = self.px_rot % max(1, -(-len(rest) // PRICE_ROTATE))
            self.px_rot += 1
            core |= set(rest[k * PRICE_ROTATE:(k + 1) * PRICE_ROTATE])
        watched = set(all_ex) if not self.px else (core or set(all_ex))   # first loop after a start: every market
        wsig = (self.ex.sent_orders, len(self.ex.gone), len(self.ex.cancelling))
        need_orders = (self.open_read_t is None or now - self.open_read_t >= ORDERS_READ_S or wsig != self.open_wsig
                       or any(k in ("fills", "orders", "resync") for _, k, _ in events) or bool(self.last_fill_exs))
        low = ((self.acct or {}).get("totalAccountValue") or 0) - ((self.acct or {}).get("totalHoldingsValue") or 0) < CASH_RESERVE + 5_000
        need_pnl = not self.acct or low or now - self.pnl_read_t >= PNL_READ_S   # cash near the floor: read it every loop
        with ThreadPoolExecutor(max_workers=6) as pool:
            # open orders FIRST, then positions, in one worker: a fill between the two reads can then only make the
            # position look smaller than the orders imply (I2 capacity understated, never overstated)
            jobs = dict(fair=pool.submit(pm.fairs, self.mp),
                        book=pool.submit(lambda: ((api.open_orders() if need_orders else list(self.open)), api.positions_raw())),
                        px=pool.submit(api.prices, watched), fills=pool.submit(api.fills, set(self.ledger.seen)))
            if need_pnl:
                jobs["pnl"] = pool.submit(api.pnl)
        got = {}
        for k, j in jobs.items():
            try:
                got[k] = j.result()
            except Exception as e:
                if k in ("book", "px"):
                    raise                                       # no positions/orders/prices: no trading this loop
                self.event("ERR", f"{k}: {e!r}")
        got["open"], got["pos"] = got.pop("book")
        if need_orders:
            self.open_read_t, self.open_wsig = now, (self.ex.sent_orders, len(self.ex.gone), len(self.ex.cancelling))
        if "pnl" in got:
            self.pnl_read_t = now
        got["px"] = {**{ex: v for ex, v in self.px.items() if ex not in got["px"]}, **got["px"]}   # rotated markets keep their last price
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
        catch_open = {oid for oid, o in self.ledger.orders.items() if o["status"] == "open" and o.get("layer") in self.CATCHERS}
        fill_exs, new_orders, proj = set(), set(), collections.Counter()
        for f in self.ledger.sync(self.open, got.get("fills", []), self.bot_of, self.side_fair, self.side_mid, as_of=now,
                                  held=lambda ex: prev.get(ex)):
            fill_exs.add(f["ex"])
            new_orders.add(f["order"])
            if f.get("action") in ("buy", "sell"):
                proj[f["ex"]] += f["qty"] * (1 if f["side"] == "yes" else -1) * (1 if f["action"] == "buy" else -1)
            e = "" if f["edge"] is None else f" | vs PM {f['edge'] * 100:+.1f}c, vs SIG mid {(f['sig_edge'] or 0) * 100:+.1f}c"
            self.event("FILL", f"{f['bot']} [{f['layer'] or '-'}]: {f['action'] or 'trade'} {f['side'].upper()} {f['qty']:,.0f} @ {f['price']:.3f}{e}")
        try:                                                # evening §1.2-1.4: fills from our order records while blind
            for r in self.order_truth(pos, prev, proj, now, bool(fill_exs)):
                fill_exs.add(r["ex"])
                new_orders.add(r["order"])
        except Exception as err:
            self.event("ERR", f"order truth: {err!r}")
        while self.pending_fixes:                           # day 3 §1: subbook_fix, inside the loop (the engine owns the ledger)
            fix = self.pending_fixes.pop(0)
            try:
                self.apply_subbook_fix(fix)
            except Exception as err:
                self.event("ERR", f"subbook_fix: {err!r}")
        if self.pending_backfill:
            job, self.pending_backfill = self.pending_backfill, None
            try:
                self.backfill_fills(job["since"], pos, prev, dry_run=job.get("dry_run", True), only=job.get("orders"))
            except Exception as err:
                self.event("ERR", f"backfill: {err!r}")
        self.new_fill_orders = new_orders
        self.last_fill_exs = set(fill_exs)                  # §4: a fill -> the next loop re-reads open orders
        # §6: SIG's fill notification can beat its REST fills page; a notification that found nothing re-polls in 2 s (x2)
        if not fill_exs:
            retry = [(t_ev, k, x) for t_ev, k, x in events if k == "fills" and self.fill_retry.get(t_ev, 0) < 2]
            for t_ev, k, x in retry:
                self.fill_retry[t_ev] = self.fill_retry.get(t_ev, 0) + 1
            if retry:
                with self.rt_lock:
                    self.rt_pending = retry + self.rt_pending
                self.wake.set()
        self.fill_retry = {t_: n for t_, n in self.fill_retry.items() if t_ > now - 120}
        self.ledger.markouts(self.side_mid)
        try:
            if getattr(self, "new_fill_orders", None):
                new_f = [f for f in self.ledger.fills[-200:] if f["order"] in self.new_fill_orders]
                self.fat_fill_events(new_f)
                self.dump_fill_events(new_f)
                self.longshot_fill_events(new_f)
                self.scalp_fill_events(new_f)
                for f in new_f:                             # an order filled out before any snapshot showed it: no longer "acked, resting"
                    rec = self.ledger.orders.get(str(f.get("order") or ""))
                    if rec and rec.get("filled", 0) >= rec.get("qty", 0) - QTY_TOL:
                        with self.ex.wake:
                            self.ex.recent.pop(str(f["order"]), None)
        except Exception as err:
            self.event("ERR", f"fat-finger events: {err!r}")
        self.ledger.init_subbooks(pos, self.manual_ex)
        self.ledger.audit_subbooks(pos, log=lambda s: self.event("AUDIT", s.strip()[6:].strip()), skip=self.audit_skip(pos, now))
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
        try:
            self.cash_short_check(raw, catch_open, now)
        except Exception as err:
            self.event("ERR", f"cash-short check: {err!r}")
        self.cash_hist = (getattr(self, "cash_hist", []) + [raw])[-3:]
        cash = min(self.cash_hist)                          # one glitchy read can't open the gates (06:12: +10.7k for a loop)
        self.update_mids(now)
        try:
            self.update_tmm(now)
        except Exception as err:
            self.event("ERR", f"turnover MM state: {err!r}")
        self.track_tranches(now)
        try:
            self.track_dumps(now)
        except Exception as err:
            self.event("ERR", f"dump tranches: {err!r}")
        try:
            self.track_longshots(now)
        except Exception as err:
            self.event("ERR", f"longshot exits: {err!r}")
        try:
            self.track_scalps(now)
        except Exception as err:
            self.event("ERR", f"scalper exits: {err!r}")
        self.controller(now)
        kappa, _ = self.kappa()
        used = self.bucket_usage()
        try:
            self.release_now = self.release_plan(cash, now, used)
        except Exception as err:                            # never let it stop the loop
            self.release_now = {}
            self.event("ERR", f"release plan: {err!r}")
        try:
            self.fat_now = self.fat_plan(now)
        except Exception as err:
            self.fat_now = {}
            self.event("ERR", f"fat-finger plan: {err!r}")
        try:
            self.dump_now = self.dump_plan(now, cash, used)
        except Exception as err:
            self.dump_now = {}
            self.event("ERR", f"dump plan: {err!r}")
        try:
            self.long_now = self.longshot_plan(now)
        except Exception as err:
            self.long_now = {}
            self.event("ERR", f"longshot plan: {err!r}")
        try:
            self.scalp_now = self.scalp_plan(now, cash, used)
        except Exception as err:
            self.scalp_now = {}
            self.event("ERR", f"scalper plan: {err!r}")
        self.long_quota = int(0.15 * self.ex.capacity * max(LOOP_S, self.t.get("read", 0) + 5) / 60)
        self.dump_quota = int(DUMP_WRITE_SHARE * self.ex.capacity * max(LOOP_S, self.t.get("read", 0) + 5) / 60) \
            if api.writes_left() >= DUMP_WRITES_IDLE else 0     # day 4 §2: place only while writes are idle
        self.fat_quota = int(FAT_CAPACITY_SHARE * self.ex.capacity * max(LOOP_S, self.t.get("read", 0) + 5) / 60)
        engine_ex = {b.ex for b in self.bots if b.kind == "market"} | {ex for pb in self.pairs for ex in pb.legs}
        filled = sum(self.risk(ex, pos.get(ex, 0)) for ex in engine_ex)
        self.risk_view = dict(filled=round(filled), resting=round(used["committed"]), cash=round(cash),
                              budget=round(cash - CASH_RESERVE - used["committed"]))
        budget = self.risk_view["budget"]

        # F2a pairs first: unwinds and repairs always; new sets only with the pairs layer on and new risk allowed. A loop
        # woken by our own fills skips new sets (their book reads take 1-3 s): exits go out first (day 2 §6)
        t0 = time.time()
        fill_loop = any(k == "fills" for _, k, _ in events)
        shots, spent = self.run_pairs(now, min(budget - OPPORTUNITY_RESERVE, self.cap("pairs", "capital") - used["pairs"]), kappa,
                                      new_sets=not fill_loop) if self.controls.on("pairs") else (0, 0.0)
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
        tier_a = self.fat_tiers() if self.fat_now else set()
        for b in sorted(self.bots, key=lambda b: (b.kind != "position", b.ex not in tier_a, -self.sweep_rank.get(b.ex, (0, 0))[0],
                                                  -self.sweep_rank.get(b.ex, (0, 0))[1])):   # tier A, then most sweeps first
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
                subs_now = self.ledger.subbooks.get(b.ex, {})
                for w in wants:                             # night §1.1: an exit sells only its own sub-book's shares
                    if w.get("reduce") and w.get("sub") in subs_now and w["layer"] != "release":
                        w["qty"] = int(min(w["qty"], abs(subs_now[w["sub"]])))
                        if w.get("qmax") is not None:
                            w["qmax"] = min(w["qmax"], w["qty"])
                wants = [w for w in wants if w["qty"] >= MIN_QTY]
            kept, cancels, places = bots.reconcile(wants, mine.get(b.name, []))
            places = self.no_self_cross(b, places, cancels)
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
        self.loop_end_t = time.time()
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
                    lambda: self.scan_new_markets(time.time()), lambda: self.intel_step(time.time()),
                    lambda: self.intel_daily(time.time())):
            try:
                job()
            except Exception as e:                          # side jobs must never stop trading
                self.event("ERR", f"side job: {e!r}")

    # ------------------------------------------------------------------------------------------------ intel (day 2 §5)
    def intel_write(self, name, row):
        os.makedirs(os.path.dirname(os.path.join(DATA, "intel", name)), exist_ok=True)
        with open(os.path.join(DATA, "intel", name), "a") as fh:
            fh.write(json.dumps(row, default=str) + "\n")

    def fair_at(self, ex, ts):
        """The engine's Polymarket fair of `ex` at time ts (sampled every 30 s, kept 3 h); None before the samples."""
        s = self.fair_series.get(ex)
        if not s:
            return None
        i = max((k for k, (t, _) in enumerate(s) if t <= ts + 1e-6), default=None)
        if i is None:
            return None if s[0][0] - ts > 60 else s[0][1]
        return s[i][1] if ts - s[i][0] <= 1800 else None

    def intel_step(self, now):
        """Big-event detector, live (morning_plan 4.3). SIG's market channels deliver nothing, so the tape is read
        incrementally (l_intel.TapeCollector): up to INTEL_READS markets a loop and only with spare reads, markets whose
        last price changed first, every market at least every INTEL_REVISIT_S. Prints get the fair of their time."""
        if now - self.intel_s.get("fair", 0) >= 30:
            self.intel_s["fair"] = now
            for ex, f in self.fair.items():
                self.fair_series.setdefault(ex, collections.deque(maxlen=360)).append((now, f))
        if self.coverage is not None and now - self.intel_s.get("cov", 0) >= 60:
            self.intel_s["cov"] = now
            c = self.coverage
            self.intel_write("coverage.jsonl", dict(t=now, pct=round(100 * c["covered"] / c["eligible"], 1) if c["eligible"] else None,
                                                    eligible=c["eligible"], covered=c["covered"], uncovered=len(c["reasons"])))
        for k, d in list(self.dump_track.items()):          # dump tracker: SIG mid of the gaining side, every minute, 2 h
            b_, a_, _ = self.px.get(d["ex"], (None, None, None))
            m = (now - d["t"]) / 60
            if b_ is not None and a_ is not None and (not d["path"] or m - d["path"][-1][0] >= 1):
                mid = (b_ + a_) / 2
                d["path"].append((round(m, 1), round(mid if d["side"] == "yes" else 1 - mid, 4)))
            if m >= 120:
                self.intel_write("dumps.jsonl", d)
                del self.dump_track[k]
            if now - self.intel_s.get("track_save", 0) >= 60:
                self.intel_s["track_save"] = now
                self.save_dump_track()
        if not self.controls.on("leaders") or api.DRY_RUN:
            return
        if self.tape is None:
            os.makedirs(os.path.join(DATA, "intel", "trades"), exist_ok=True)
            self.tape = intel.TapeCollector(lambda path, p: api._req("GET", path, params=p), api.T["id"],
                                            os.path.join(DATA, "intel", "state.json"))
            head = [ex for ex, m in self.meta.items() if m.get("race") in HEADLINE_RACES]
            with open(os.path.join(DATA, "intel", "names.json"), "w") as fh:
                json.dump(dict(names={b.ex: self.name(b.ex) for b in self.bots if b.kind == "market"}, headline=head), fh)
        exs = [b.ex for b in self.bots if b.kind == "market"]
        last = lambda ex: (self.px.get(ex) or (None, None, None))[2]
        changed = sorted((ex for ex in exs if last(ex) is not None and last(ex) != self.intel_px.get(ex)), key=lambda ex: self.intel_t.get(ex, 0))
        stale = sorted((ex for ex in exs if now - self.intel_t.get(ex, 0) >= INTEL_REVISIT_S), key=lambda ex: self.intel_t.get(ex, 0))
        for ex in list(dict.fromkeys(changed + stale))[:INTEL_READS]:
            tape_min = sum(1 for t_ in self.tape_reads_t if t_ >= now - 60)
            if tape_min >= TAPE_MIN_PER_MIN and not api.spare_reads(40):   # §4: 6/min guaranteed, more only with spare reads
                break
            if tape_min < TAPE_MIN_PER_MIN and api.reads_last_min() >= 95:
                break
            self.tape_reads_t.append(now)
            new = self.tape.visit(ex)
            self.intel_t[ex], self.intel_px[ex] = now, last(ex)
            if not new:
                continue
            for r in new:
                r["fair"] = self.fair_at(ex, r["ts"])
                self.intel_write(f"trades/{datetime.fromtimestamp(r['ts']).strftime('%Y-%m-%d')}.jsonl", r)
                self.note_volume(ex, r["ts"], r["p"] * r["q"])
            for e in intel.detect(new, self.fair_at, {ex: self.name(ex)}):
                self.intel_event(e)

    def save_dump_track(self):
        try:
            os.makedirs(os.path.join(DATA, "intel"), exist_ok=True)
            with open(os.path.join(DATA, "intel", "dump_track.json"), "w") as fh:
                json.dump(self.dump_track, fh)
        except OSError:
            pass

    def intel_event(self, e):
        """One big event: what we caught, what our current levels would have caught (missed), logged everywhere."""
        side, ex = e["gainer_side"], e["ex"]
        caught_q = caught_v = 0.0
        if side:
            sf = e["fair"] if side == "yes" else 1 - e["fair"]
            for f in self.ledger.fills[-500:]:
                if f["ex"] == ex and f["action"] == "buy" and f["side"] == side and \
                        e["t"] - 5 <= datetime.strptime(f["t"], "%Y-%m-%d %H:%M:%S").timestamp() <= e["t_end"] + 120:
                    caught_q += f["qty"]
                    caught_v += (sf - f["price"]) * f["qty"]
            levels = [(o["price"], o["qty"]) for o in self.open if o["ex"] == ex and o["action"] == "buy" and o["side"] == side]
            have = {round(p, 3) for p, _ in levels}
            levels += [(w["px"], w["qty"]) for w in self.fat_now.get(ex) or [] if w["side"] == side and round(w["px"], 3) not in have]
            _, would = intel.missed_value(e, levels)
            e["missed_value"] = round(max(0.0, would - caught_v), 2)
            deep = e["p_lo"] if side == "yes" else round(1 - e["p_hi"], 3)
            e["missed_why"] = (f"nothing resting or planned on {side.upper()}" if not levels else
                               f"our {side.upper()} levels ({max(p for p, _ in levels):.3f} and below) were under the sweep's "
                               f"deepest print ({deep:.3f})" if not would else "levels now resting would have caught it")
        e.update(captured_shares=round(caught_q), captured_value=round(caught_v, 2), had_orders=caught_q > 0)
        self.intel_write("events.jsonl", e)
        r = self.dump_ref.get(ex) or self.ref(ex) or (self.tmm_mid.get(ex) or (0, None))[1]   # pre-dump SIG level
        if side and e.get("fair") is not None and r is not None:     # afternoon §4.1: follow the price for 2 h
            pre = r if side == "yes" else 1 - r
            extreme = e["p_lo"] if side == "yes" else round(1 - e["p_hi"], 3)
            self.dump_track[f"{ex}:{e['first_id']}"] = dict(ex=ex, market=e["market"], side=side, t=e["t"], pre=round(pre, 4),
                                                             extreme=extreme, shares=e["shares"], walk=e.get("walk"), path=[])
            self.save_dump_track()
        msg = (f"BIG EVENT {e['market']}: {e['shares']:,} shares at {e['p_lo']:.3f}-{e['p_hi']:.3f}"
               + (f" (fair {e['fair']:.3f}), value {e['value']:,.0f}" if e["fair"] is not None else f", cost {e['cost']:,.0f}")
               + f" [{', '.join(e['kinds'])}] · we caught {caught_v:,.0f}"
               + (f" · missed {e['missed_value']:,.0f}" if e.get("missed_value") else ""))
        self.event("EVENT", msg)
        if (e.get("value") or 0) >= 10_000 or "Max sweep" in e["kinds"]:
            self.alerts.append(f"{time.strftime('%H:%M')} {msg}")
            self.night.line(msg)

    def intel_daily(self, now):
        """07:00 PT: docs/daily/<date>.md, docs/leaders/<date>.md, tier A refresh (local files only, no SIG reads)."""
        day = time.strftime("%Y-%m-%d")
        if time.localtime(now).tm_hour < INTEL_DAILY_HOUR or self.intel_s.get("daily") == day or api.DRY_RUN:
            return
        night = self.controls.d.setdefault("night", {})
        if night.get("intel_daily") == day:
            self.intel_s["daily"] = day
            return
        self.intel_s["daily"] = night["intel_daily"] = day
        self.controls.save()
        dpath, lpath, (add, drop) = intel.run_daily(DATA, os.path.join(os.path.dirname(DATA), "docs"), now)
        self.night.line(f"daily intel: {os.path.relpath(dpath, os.path.dirname(DATA))}, {os.path.relpath(lpath, os.path.dirname(DATA))}; "
                        f"tier A +{len(add)} −{len(drop)} (applied)")

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
            if t["state"] != "open" or t.get("kind") == "dump":
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
                     liquidate=LIQUIDATE.get(b.name),
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
        free = cash - CASH_RESERVE                          # §3.4: below the free-cash target, entries need ROC >= 6%
        rel = self.release_now.get(b.ex)
        return dict(fair=f, u=self.ft.u.get(b.ex), bid=bid, ask=ask, ref=self.ref(b.ex), kappa=kappa,
                    roc_min=ROC_MIN_TIGHT if free < self.release_target()[0] else ROC_MIN, roc_hold=self.roc_hold(time.time()), held=q, release=rel,
                    fatfinger=self.fat_now.get(b.ex) or [], dump=self.dump_now.get(b.ex) or [],
                    dump_tranches=self.dump_view.get(b.ex, []), tmm=self.tmm_view(b.ex, f, bid, ask),
                    longshot=self.long_now.get(b.ex) or [], long_exits=self.long_view.get(b.ex, []),
                    scalp=self.scalp_now.get(b.ex) or [], scalp_exits=self.scalp_view.get(b.ex, []),
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
                 sniper_market={}, sniper_day=0.0, pairs=0.0, fat_resting=0.0, fat_filled=0.0, dump_resting=0.0, long_resting=0.0)
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
            elif l == "fatfinger":
                u["fat_resting"] += n
            elif l == "dump":
                u["dump_resting"] += n
            elif l == "longshot":
                u["long_resting"] += n
        for ex, books in self.ledger.subbooks.items():
            for sub, v in books.items():
                if sub.startswith("fatfinger:"):
                    u["fat_filled"] += abs(v) * float(sub.rsplit(":", 1)[1])
        for p in self.ex.pending():                         # queued / in flight / acked but not yet in a snapshot
            if p["action"] != "buy" or p["bot"] in self.position_names:
                continue
            n, l = p["qty"] * p["price"], p["layer"]
            if l in self.FLOOR_LAYERS and not jx.reduces(p["side"], "buy", self.book.get(p["ex"], {}).get("pos", 0)):
                u["committed"] += n                         # an exit in flight is not an entry (restart: 12.7k for a loop)
            if l == "ladder":
                u["ladder_resting"] += n
                u["ladder_side"][self.pol(p["ex"], p["side"])] += n
            elif l == "take":
                u["sniper_market"][p["ex"]] = u["sniper_market"].get(p["ex"], 0.0) + n
            elif l == "fatfinger":
                u["fat_resting"] += n
            elif l == "dump":
                u["dump_resting"] += n
            elif l == "longshot":
                u["long_resting"] += n
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
                     ("fatfinger", "resting"): u["fat_resting"], ("fatfinger", "filled"): u["fat_filled"], ("dump", "resting"): u["dump_resting"], ("longshot", "resting"): u["long_resting"],
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
            if l == "fatfinger":
                funded = max(0.0, min(float(OPPORTUNITY_RESERVE), cash - CASH_RESERVE - used["committed"]))
                worst = sum(x["qty"] * x["px"] for x in (self.fat_now.get(b.ex) or []) if x["side"] == w["side"])
                if (self.fat_quota <= 0 or used["fat_resting"] + n > self.cap("fatfinger", "resting")
                        or used["fat_filled"] >= self.cap("fatfinger", "filled") or worst > funded):
                    self.cover_why(b.ex, w["side"], "reserve not funded" if worst > funded else
                                   "resting cap reached" if used["fat_resting"] + n > self.cap("fatfinger", "resting") else
                                   "filled cap reached" if used["fat_filled"] >= self.cap("fatfinger", "filled") else
                                   "being placed (paced at 30% of SIG capacity)")
                    continue
                self.fat_quota -= 1
                self.fat_sent.append(time.time())
                used["fat_resting"] += n
                out.append(w)
                continue
            if l == "longshot":                             # day 3 §2: total resting cap, quota, breaker, reserve fit
                funded = max(0.0, min(float(OPPORTUNITY_RESERVE), cash - CASH_RESERVE - used["committed"]))
                worst = sum(x["qty"] * x["px"] for x in (self.long_now.get(b.ex) or []) + (self.fat_now.get(b.ex) or [])
                            + (self.dump_now.get(b.ex) or []) if x["side"] == w["side"])
                if self.fills_blind or self.long_quota <= 0 or used["long_resting"] + n > self.cap("longshot", "resting") \
                        or worst > funded or self.breaker.get(("long", "all"), 0) > time.time():
                    continue
                self.long_quota -= 1
                used["long_resting"] += n
                out.append(w)
                continue
            if l == "dump" and self.fills_blind:           # evening §1.4: no new dump levels while fills are blind
                continue
            if l == "mm" and self.fills_blind:             # ... and no new MM entries (exits passed above)
                continue
            if l == "dump":                                 # afternoon D-A: a full fill of the side's levels + its fat-finger
                funded = max(0.0, min(float(OPPORTUNITY_RESERVE), cash - CASH_RESERVE - used["committed"]))   # levels fits
                worst = sum(x["qty"] * x["px"] for x in (self.dump_now.get(b.ex) or []) + (self.fat_now.get(b.ex) or [])
                            if x["side"] == w["side"])
                if self.dump_quota <= 0 or used["dump_resting"] + n > self.cap("dump", "resting") or worst > funded:
                    continue
                self.dump_quota -= 1
                used["dump_resting"] += n
                out.append(w)
                continue
            need = CASH_RESERVE if self.reserve_ok(b.ex, w) else CASH_RESERVE + OPPORTUNITY_RESERVE    # §3.2 two tiers
            if l in ("mm", "take") and cash - used["committed"] - n < need:
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
                cover = max(0.0, cash - CASH_RESERVE - OPPORTUNITY_RESERVE - used["committed"])   # ladders: above the reserve
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
        # promotions (pilot -> full) and pilot extensions; evening §1.5: none while the hold flag is set or fills are blind
        healthy, why_bad = self.fills_health(now)
        hold = bool(c.d.get("hold_gates")) or not healthy
        gate_free = set(c.d.get("gate_free") or [])         # day 4: layers Piero let promote while the hold is on
        if hold and now - night.get("hold_logged", 0) >= 1800:
            night["hold_logged"] = now
            self.night.line(f"pilot gates held: {'hold flag set' if c.d.get('hold_gates') else ''}"
                            f"{'; ' if c.d.get('hold_gates') and not healthy else ''}{'' if healthy else why_bad}")
        for l in ("mm", "scalp", "ladder", "pairs", "release", "fatfinger", "dump", "longshot"):
            if c.state(l) != "pilot" or (hold and not (l in gate_free and healthy)):
                continue
            st, age = stats.get(l, {}), now - since[l]
            ok, why = False, ""
            if l == "mm" and age >= PILOT_S:              # afternoon D-C gate for the turnover MM
                n, mo, real, vol = st.get("fills", 0), st.get("markout30"), st.get("realized", 0.0), st.get("volume", 0.0)
                inv = [c_ for t_, c_ in self.tmm_inv if t_ >= since[l]]
                avg_inv = max(1_000.0, sum(inv) / len(inv)) if inv else 1_000.0
                pace = vol / avg_inv * 86400 / max(age, 1)
                trims = self.ex.trims.get("mm", 0)
                if real > 0 and pace >= 2.0 and (mo is None or mo >= -0.005) and trims == 0:
                    ok, why = True, (f"{n} fills, realized {real:+,.0f}, turnover {pace:.1f}x/day pace, mark-out "
                                     f"{(mo or 0) * 100:+.2f}c, no I2 trims")
                elif now - night.get(f"{l}_ext", 0) >= PILOT_S:
                    night[f"{l}_ext"] = now
                    self.night.line(f"layer mm (turnover) stays pilot: {n} fills, realized {real:+,.0f}, turnover {pace:.1f}x/day, "
                                    f"mark-out {(mo or 0) * 100:+.2f}c, {trims} I2 trims (gate: realized > 0, >= 2x/day, >= -0.5c, 0)")
            if l == "ladder" and age >= PILOT_S:
                trims = self.ex.trims.get("ladder", 0) - night.get("ladder_trims0", 0)
                trips = night.get("breaker_trips_since_ladder", 0)
                if trims == 0 and trips == 0:
                    ok, why = True, "2 h pilot: no I2 trims, no breaker trips"
                elif now - night.get(f"{l}_ext", 0) >= PILOT_S:
                    night[f"{l}_ext"] = now
                    self.night.line(f"layer ladder stays pilot: {trims} I2 trims, {trips} breaker trips (review)")
            if l == "release":
                a_ = self.acct or {}
                free = (a_.get("totalAccountValue") or 0) - (a_.get("totalHoldingsValue") or 0) - CASH_RESERVE
                r = self.released_today_view()
                trims = self.ex.trims.get("release", 0) - night.get("release_trims0", 0)
                if (free >= RELEASE_PILOT_TARGET or age >= PILOT_S) and trims == 0 and (r["avg_cost"] is None or
                                                                                   r["avg_cost"] <= self.release_max_cost() + 0.005):
                    ok, why = True, (f"pilot target reached (free cash {free:,.0f})" if free >= RELEASE_PILOT_TARGET else "2 h pilot") + \
                        f", freed {r['sig']:,.0f} at {(r['avg_cost'] or 0) * 100:.1f}% average cost, no I2 trims"
            if l == "fatfinger" and age >= PILOT_S:         # morning_plan 1.10: pilot tier A 2 h -> full tiers A + B
                fills = [f for f in self.fills_since(since[l]) if f["layer"] == "fatfinger" and f["action"] == "buy"]
                close = [f for f in fills if f.get("fair") is not None and f["fair"] - f["price"] < 0.25]   # ledger: survives restarts
                trims = self.ex.trims.get("fatfinger", 0)
                thr = api.health(PILOT_S).get("throttled", 0)
                late = sum(1 for t in self.fat_sent if t >= now - 3600)   # after 2 h the initial placement is older than this
                if not close and trims == 0 and thr == 0 and late <= FAT_REFRESH_MAX_H:
                    ok, why = True, (f"2 h pilot: {len(fills)} fills, none within 25c of fair; no I2 trims; no HTTP 429s; "
                                     f"{late} placements in the last hour")
                elif now - night.get(f"{l}_ext", 0) >= PILOT_S:
                    night[f"{l}_ext"] = now
                    self.night.line(f"layer fatfinger stays pilot: {len(close)} fills within 25c of fair, {trims} I2 trims, "
                                    f"{thr} HTTP 429s, {late} placements last hour (review)")
            if l == "dump" and age >= DUMP_PILOT_S:         # day 4 §2 gate: 4 h pilot at half size on every side
                g = self.dump_gate(since[l], now)
                if g["ok"]:
                    ok, why = True, g["why"]
                elif now - night.get(f"{l}_ext", 0) >= 1800:
                    night[f"{l}_ext"] = now
                    self.night.line(f"layer dump stays pilot: {g['why']}")
            if l == "scalp" and age >= SCALP_PILOT_S:       # day 5 §2 gate: 2 h pilot on 10 markets
                g = self.scalp_gate(since[l], now)
                if g["ok"]:
                    ok, why = True, g["why"]
                elif now - night.get(f"{l}_ext", 0) >= 1800:
                    night[f"{l}_ext"] = now
                    self.night.line(f"layer scalp stays pilot: {g['why']}")
            if l == "longshot" and age >= LONG_PILOT_S:     # day 3 §2: 24 h pilot -> full
                fills = [f for f in self.fills_since(since[l]) if f["layer"] == "longshot" and f["action"] == "buy"]
                late = [f for f in fills if not self.long_exit_ok(f)]
                trims = self.ex.trims.get("longshot", 0)
                short = self.cash_short is not None and self.cash_short["t"] >= since[l]
                if not late and trims == 0 and not short:
                    ok, why = True, f"24 h pilot: {len(fills)} fills booked, exits placed, no I2 trims, no CASH SHORT"
            if l == "pairs":
                done = [bk for bk in self.ledger.baskets if bk.get("ts", 0) >= since[l] and bk.get("state") in ("held", "unwound", "done")
                        and self.ledger.matched(bk) > 0 and not self.basket_residue(bk)]
                if len(done) >= 5:
                    ok, why = True, f"{len(done)} sets with no one-leg residue"
            if ok:
                c.set_layer(l, "full", f"pilot gate passed: {why}")
        # Piero's unattended rule: writes/cancels failing with 5xx / UNKNOWN repeatedly -> stop and pull at-risk orders
        try:
            self.ratchet("dump", now)
            self.ratchet("scalp", now)
            self.idle_rule(now)
        except Exception as err:
            self.event("ERR", f"ratchet: {err!r}")
        hw = api.health(600).get("write") or {}
        if hw.get("n", 0) >= 10 and hw.get("bad_rate", 0) >= 0.5:   # day 4 §1.2: >= 10 writes in 10 min and >= 50% failed
            self.stop_and_pull(f"SIG writes failing: {hw.get('bad_rate', 0):.0%} 5xx/timeouts of {hw.get('n', 0)} in 10 min")
        self.auto_resume(now)
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
        if c.on("recycler") and night.get("ak_released", 0.25 if night.get("d1_done") else 0.0) < AK_RELEASE - 1e-9:
            self.apply_d1()
        if json.dumps(night, sort_keys=True, default=str) != before:
            c.save()                                        # only on a change: a hand edit on disk is never overwritten

    def restore_floor(self):
        """§4.2 hard floor: free cash - every resting MM/take entry buy >= CASH_RESERVE. Fills spend cash while the other
        quotes stay, so it can slip: cancel resting entries, lowest ROC first, until the deficit is covered (admission
        keeps them from coming back until cash recovers)."""
        a = self.acct or {}
        if not a:
            return 0
        cash = min(getattr(self, "cash_hist", None) or [(a.get("totalAccountValue") or 0) - (a.get("totalHoldingsValue") or 0)])
        cands, res_n, other_n = [], 0.0, 0.0
        for o in self.open:
            rec = self.ledger.orders.get(o["id"])
            if not rec or o["action"] != "buy" or rec["layer"] not in ("mm", "take") or o["id"] in self.ex.cancelling or \
                    jx.reduces(o["side"], "buy", self.book.get(o["ex"], {}).get("pos", 0)):
                continue
            f = self.side_fair(o["ex"], o["side"])
            r = self.reserve_ok(o["ex"], dict(layer=rec["layer"], side=o["side"], px=o["price"]))
            n_ = o["qty"] * o["price"]
            res_n, other_n = res_n + (n_ if r else 0), other_n + (0 if r else n_)
            cands.append((bots.roc_of(f, o["price"]) if f is not None else -1.0, r, o))
        # tier 2: entries that may not use the opportunity reserve must fit above floor + reserve; tier 1: all above floor
        deficit_other = other_n + CASH_RESERVE + OPPORTUNITY_RESERVE - cash
        deficit_all = res_n + other_n + CASH_RESERVE - cash
        freed, n = 0.0, 0
        for _, r, o in sorted(cands, key=lambda t: (t[1], t[0])):     # non-reserve entries first, lowest ROC first
            if freed >= max(deficit_all, 0) and (r or freed >= max(deficit_other, 0)):
                break
            if self.ex.cancel(o, reason="cash floor"):
                freed += o["qty"] * o["price"]
                n += 1
        if n:
            self.event("FLOOR", f"cash short of floor + reserve by {max(deficit_other, deficit_all):,.0f}: {n} lowest-ROC entry quotes "
                                f"cancelled ({freed:,.0f} SIG)")
        return n

    # ------------------------------------------------------------------------------------------------ fat-finger (day 2 §4)
    def fat_tiers(self):
        t = read_json(os.path.join(DATA, "intel", "tiers.json"), {})
        return set(t.get("tier_a") or [])

    def fat_eligible(self, ex):
        """[(side, fair or ref, size scale)] for the sides of a market the fat-finger catcher covers (morning_plan §1.2)."""
        b = self.owner.get(ex)
        if b is None or b.kind != "market" or ex in self.manual_ex:
            return []
        out = []
        f = self.fair.get(ex)
        if f is not None:
            for side, fs in (("yes", f), ("no", 1 - f)):
                if fs >= FAT_MIN_FAIR:
                    out.append((side, fs, 1.0))
            return out
        if ex in {m["ex"] for m in self.mp}:                # mapped but no fair right now (wide / stale): not covered
            return []
        r = self.ref(ex)
        new = time.time() - self.new_markets.get(ex, 0) < 3 * 3600 or ex in self.unmapped_new
        if new and r is None:                               # a brand-new unmapped market: 0.05 on both sides, half size
            return [("yes", 0.5, 0.5), ("no", 0.5, 0.5)]
        if r is not None:
            for side, rs in (("yes", r), ("no", 1 - r)):
                if rs >= FAT_UNMAPPED_REF:
                    out.append((side, rs, 0.5))
        return out

    def fat_levels(self, ex, side, value, scale, tier):
        """[(level, price, shares)]: levels <= 25% of the side's value, one tick above the best resting bid <= the level
        (from the last queue read), never above the level or the cap."""
        sizes = FAT_SIZES[tier]
        book = self.fat_book.get(ex)
        bids = []
        if book:
            _, yb, ya = book
            bids = [p for p, _ in yb] if side == "yes" else [round(1 - p, 3) for p, _ in ya]
        out, used = [], set()
        new_unmapped = scale == 0.5 and value == 0.5
        if new_unmapped:                                    # Oct 3: brand-new unmapped market: 0.05 at half size, both sides
            rivals = [p for p in bids if p <= FAT_NEW_LEVEL + 1e-9]
            px = bots.tick_down(min(FAT_NEW_LEVEL, max(rivals) + TICK)) if rivals else FAT_NEW_LEVEL
            q = int(FAT_NEW_SIZES[tier] * scale)
            return [(FAT_NEW_LEVEL, px, q)] if q >= MIN_QTY and px >= 0.01 else []
        bands = sorted(set(FAT_BAND_EDGES) | set(FAT_LEVELS))   # Oct 3: the original bands, whatever is placed
        for lvl in FAT_LEVELS:
            if lvl not in sizes or (new_unmapped and lvl != 0.05):
                continue
            cap = min(max(FAT_LEVELS), FAT_FAIR_CAP * value) if not new_unmapped else lvl   # never above 0.12, 25% of fair
            if min(lvl, cap) < 0.01:
                continue
            floor_ = max([b for b in bands if b < lvl], default=0.0)   # this level's band: (next lower level, level]
            rivals = [p for p in bids if floor_ < p <= lvl + 1e-9]
            px = bots.tick_down(min(cap, max(rivals) + TICK)) if rivals else bots.tick_down(min(lvl, cap))
            if px < 0.01 or px in used:
                continue
            used.add(px)
            q = int(sizes[lvl] * scale)
            if q >= MIN_QTY:
                out.append((lvl, px, q))
        return out

    def new_unmapped(self, ex):
        """A brand-new market (seen < 3 h ago, or still unmapped since) with no Polymarket mapping."""
        return ex not in {m["ex"] for m in self.mp} and (time.time() - self.new_markets.get(ex, 0) < 3 * 3600 or ex in self.unmapped_new)

    def fat_plan(self, now):
        """{ex: [want-spec]} for the fat-finger layer; also refreshes queue reads (<= FAT_BOOK_READS per loop, tier A first,
        every FAT_QUEUE_S) and the news guard; records coverage reasons."""
        if not self.controls.on("fatfinger"):
            self.coverage = None
            return {}
        tier_a = self.fat_tiers()
        st = self.controls.state("fatfinger")
        exs = [b.ex for b in self.bots if b.kind == "market"]
        if st == "pilot" or FAT_TIER_A_ONLY:               # Oct 3: tier A only (plus brand-new unmapped markets)
            exs = [ex for ex in exs if ex in tier_a or self.new_unmapped(ex)]
        exs.sort(key=lambda ex: (ex not in tier_a, -self.sweep_rank.get(ex, (0, 0))[1]))
        # queue reads: oldest first, a few per loop
        stale = [ex for ex in exs if now - (self.fat_book.get(ex) or (0,))[0] > FAT_QUEUE_S and self.fat_eligible(ex)]
        for ex in stale[:FAT_BOOK_READS]:
            if not api.spare_reads():                       # §6: fills and exits first; the queue read waits
                break
            try:
                bids, asks = api.orderbook(ex, depth=50)
                mine = collections.Counter()                # I4: our own resting orders are not the queue
                for o in self.open:
                    if o["ex"] == ex:
                        yes_px = o["price"] if o["side"] == "yes" else 1 - o["price"]
                        mine[("b" if (o["side"] == "yes") == (o["action"] == "buy") else "a", round(yes_px, 3))] += o["qty"]
                def strip(rows, k):
                    out = []
                    for p, q in rows:
                        cut = min(q, mine[(k, round(p, 3))])
                        mine[(k, round(p, 3))] -= cut       # drawn down once, even if a price shows on several rows
                        if q - cut > QTY_TOL:
                            out.append((p, q - cut))
                    return out
                self.fat_book[ex] = (now, strip([r for r in bids if r[0] <= 0.15], "b"), strip([r for r in asks if r[0] >= 0.85], "a"))
            except Exception as err:
                self.event("ERR", f"fat-finger queue read {self.name(ex)}: {str(err)[:80]}")
                break
        # news guard: a side's fair dropping > 15c within 10 min cancels its levels until fair is stable for 30 min
        for ex in exs:
            f = self.fair.get(ex)
            if f is None:
                continue
            h = self.fair_hist.setdefault(ex, [])
            h.append((now, f))
            while h and h[0][0] < now - FAT_NEWS_WINDOW_S:
                h.pop(0)
            for side in ("yes", "no"):
                cur = f if side == "yes" else 1 - f
                top = max((v if side == "yes" else 1 - v) for _, v in h)
                if top - cur > FAT_NEWS_DROP:
                    if self.fat_block.get((ex, side), 0) < now:
                        self.event("NEWS", f"{self.name(ex)} {side.upper()} fair fell {top - cur:.2f} in 10 min: fat-finger levels pulled 30 min")
                    self.fat_block[(ex, side)] = now + FAT_NEWS_HOLD_S
        plan, cov = {}, dict(eligible=0, covered=0, reasons={})
        resting = {}
        for o in list(self.open) + [dict(p, id=None) for p in self.ex.pending()]:
            rec = self.ledger.orders.get(o["id"]) if o.get("id") else o
            if rec and rec.get("layer") == "fatfinger":
                resting.setdefault((o["ex"], o["side"]), []).append(o)
        recent_fill = {}
        for fl in self.fills_since(now - FAT_REARM_S):
            if fl["layer"] == "fatfinger":
                recent_fill[(fl["ex"], fl["side"])] = True
        for ex in exs:
            for side, value, scale in self.fat_eligible(ex):
                cov["eligible"] += 1
                key = (ex, side)
                if resting.get(key):
                    cov["covered"] += 1
                why = None
                if self.fat_block.get(key, 0) > now:
                    why = "news guard (fair fell > 15c)"
                elif self.breaker.get(("fat", self.pol(ex, side)), 0) > now:
                    why = "fat-finger breaker"
                elif self.ex.is_frozen(ex):
                    why = "frozen (unknown write)"
                if why:
                    cov["reasons"][f"{ex}|{side}"] = why
                    continue
                anchor = self.fat_anchor.get(key)
                if anchor is None or abs(anchor - value) > FAT_MOVE or (anchor >= FAT_MIN_FAIR) != (value >= FAT_MIN_FAIR):
                    self.fat_anchor[key] = anchor = value
                tier = "A" if ex in tier_a else "B"
                for lvl, px, q in self.fat_levels(ex, side, anchor, scale, tier):
                    sub = f"fatfinger:{ex}:{side}:{lvl}"
                    if key in recent_fill and not any(abs(o["price"] - px) < 1e-9 for o in resting.get(key, [])):
                        continue                            # re-arm a filled level only after 10 min
                    plan.setdefault(ex, []).append(dict(side=side, px=px, qty=q, sub=sub, level=lvl,
                                                        qtol=1.0 if key in recent_fill else QTY_TOL))
                if not resting.get(key) and f"{ex}|{side}" not in cov["reasons"]:
                    cov["reasons"][f"{ex}|{side}"] = "being placed (paced at 30% of SIG capacity)"
        self.coverage = cov
        return plan

    def fat_fill_events(self, fills):
        for f in fills:
            if f["layer"] != "fatfinger" or f["action"] != "buy":
                continue
            fv = self.side_fair(f["ex"], f["side"])
            cap = None if fv is None else (fv - f["price"]) * f["qty"]
            msg = (f"FATFINGER FILL {self.name(f['ex'])}: bought {f['qty']:,.0f} {f['side'].upper()} at {f['price']:.3f}"
                   + ("" if fv is None else f" (fair {fv:.3f}): value captured {cap:+,.0f}"))
            self.event("FATFINGER", msg)
            self.alerts.append(f"{time.strftime('%H:%M')} {msg}")
            self.night.line(msg)
        recent = [f for f in self.fills_since(time.time() - BREAKER_WINDOW_S) if f["layer"] == "fatfinger" and f["action"] == "buy"]
        now = time.time()
        for direction in ("D", "R"):
            ms = {f["ex"] for f in recent if self.pol(f["ex"], f["side"]) == direction}
            if len(ms) >= BREAKER_MARKETS and self.breaker.get(("fat", direction), 0) < now:
                self.breaker[("fat", direction)] = now + BREAKER_PAUSE_S
                n = 0
                for o in self.open:
                    rec = self.ledger.orders.get(o["id"])
                    if rec and rec["layer"] == "fatfinger" and self.pol(o["ex"], o["side"]) == direction:
                        n += bool(self.ex.cancel(o, reason="fat-finger breaker"))
                msg = (f"FAT-FINGER BREAKER pro-{direction}: fat-finger fills in {len(ms)} markets within 10 min on one side "
                       f"(news): {n} {direction}-side fat-finger bids pulled for 30 min")
                self.alerts.append(f"{time.strftime('%H:%M')} {msg}")
                self.event("BREAKER", msg)
                self.night.line(msg)

    def fills_health(self, now):
        """Evening §1.5: fills are healthy when the projection's sequence advanced within 5 min, or order-truth booking
        succeeded within 5 min. -> (healthy, why not)."""
        cov = getattr(api, "FILL_COVERAGE", {}) or {}
        seq = cov.get("seq")
        if seq is not None and seq != self.cov_seq:
            self.cov_seq, self.cov_seq_t = seq, now
        fresh = self.cov_seq_t is not None and now - self.cov_seq_t <= 300
        truth = self.truth_t is not None and now - self.truth_t <= 300
        if self.fills_blind:
            return False, f"FILLS BLIND since {time.strftime('%H:%M', time.localtime(self.fills_blind))}"
        if fresh or truth:
            return True, ""
        age = "never seen" if self.cov_seq_t is None else f"unchanged for {(now - self.cov_seq_t) / 60:.0f} min"
        return False, f"fills projection sequence {self.cov_seq} {age} (newest fill {cov.get('newest')}), order-truth booking not live"

    def audit_skip(self, pos, now):
        """Night §1.2: a market where one of our orders vanished or shrank in the last 2 min carries its sub-book difference
        (SIG's late fill is probably on its way) instead of pushing it into inventory; only a difference still there after
        2 min goes to inventory."""
        skip = set()
        for ex in set(pos) | set(self.ledger.subbooks):
            diff = pos.get(ex, 0) - sum(self.ledger.subbooks.get(ex, {}).values())
            if abs(diff) <= 1e-6:
                self.audit_first.pop(ex, None)
                continue
            first = self.audit_first.setdefault(ex, now)
            if now - self.order_moved_t.get(ex, 0) <= 120 and now - first < 120:
                skip.add(ex)
        return skip

    LAYER_TTL = {"mm": TMM_TTL_S, "fatfinger": FAT_TTL_S, "dump": DUMP_TTL_S, "dumpexit": DUMP_TTL_S, "ladder": LADDER_TTL_S,
                 "rebound": LADDER_TTL_S, "release": RECYCLE_TAKE_TTL_S, "take": TAKE_TTL_S, "recycle": RECYCLE_REST_TTL_S,
                 "set": PAIR_TTL_S, "unwind": PAIR_TTL_S, "repair": PAIR_TTL_S, "touch": MM_TTL_S,
                 "longshot": LONG_TTL_S, "longexit": LONG_EXIT_TTL_S, "scalp": SCALP_TTL_S, "scalpexit": SCALP_TTL_S}

    def vanished_fill(self, oid, rec, q0, now):
        """Shares an order filled since the previous snapshot: it shrank, or it left the book while we didn't cancel it
        and before its TTL (evening §1.2: the fat-finger vanished-order rule; §1.4 fallback)."""
        q1 = self.truth_prev_cur.get(oid)
        if q1 is not None:
            return q0 - q1
        if rec.get("status") == "cancelled" or oid in self.ex.cancelling:
            return 0.0
        ttl = rec.get("ttl") or self.LAYER_TTL.get(rec.get("layer"), RECYCLE_REST_TTL_S)
        if ttl and now - rec.get("ts", now) >= ttl - 90:
            return 0.0                                     # expired at its TTL
        return q0

    def order_truth(self, pos, prev, proj, now, projection_booked):
        """Evening §1.2-1.4 while SIG's fills projection is stalled (no live fill source; /orders/{id}/fills 503): an engine
        order that shrank since the last open-orders snapshot, or left it without our cancel before its TTL, filled at its
        limit (source 'synthetic'). Booked only when the market's position moved by the same amount (a change seen one loop
        early, between the orders read and the positions read, is carried one loop and its audit undone). Runs before the
        audit, so booked fills never land in inventory. Unexplained moves left after the carry raise FILLS BLIND."""
        cur = {o["id"]: o["qty"] for o in self.open if o["id"] in self.ledger.orders}
        before, self.truth_prev = self.truth_prev, cur
        self.truth_prev_cur = cur
        filled_prev = self.truth_filled                    # each order's booked shares at the previous snapshot
        prev_t, self.truth_read_t = self.truth_read_t, now
        if before is None or not prev:
            self.truth_filled = {oid: (self.ledger.orders.get(oid) or {}).get("filled") or 0.0 for oid in cur}
            return []
        if self.truth_live_t is None:
            self.truth_live_t = now
        book = self.controls.d.get("truth_mode", "shadow") == "book"   # shadow: compute and log, book nothing (validation)
        before = dict(before)
        for oid, rec in self.ledger.orders.items():        # acked after the last snapshot and gone before this one
            if oid not in before and oid not in cur and rec.get("ts", 0) >= (prev_t or now) - 2 and rec.get("status") in ("open", "done"):
                before[oid] = rec["qty"] - (rec.get("filled") or 0)
        cand = collections.defaultdict(list)
        for oid, q0 in before.items():
            rec = self.ledger.orders.get(oid)
            if not rec or rec.get("action") not in ("buy", "sell"):
                continue
            d = self.vanished_fill(oid, rec, q0, now)
            # Oct 3 (13:24 840 double count): SIG's own fill for this order may have been booked by this loop's sync already
            # (its trade fell between our orders and positions reads); count only what isn't booked yet
            d -= max(0.0, (rec.get("filled") or 0) - filled_prev.get(oid, 0.0 if oid not in (before or {}) else (rec.get("filled") or 0)))
            if d > 0.5:
                self.order_moved_t[rec["ex"]] = now           # night §1.2: the audit waits up to 2 min for its late fill
                sgn = (1 if rec["side"] == "yes" else -1) * (1 if rec["action"] == "buy" else -1)
                cand[rec["ex"]].append((oid, d, sgn))
        carry, self.truth_carry = self.truth_carry, {}
        booked, unexplained, shadow_rows = [], {}, []
        exs = set(cand) | set(carry) | {ex for ex in set(pos) | set(prev) if abs(pos.get(ex, 0) - (prev.get(ex) or (0,))[0]) >= 1}
        for ex in exs:
            dpos = pos.get(ex, 0) - (prev.get(ex) or (0,))[0] - proj.get(ex, 0)
            c = carry.get(ex, 0.0)
            exp = sum(s * d for _, d, s in cand.get(ex, []))
            gross = sum(d for _, d, _ in cand.get(ex, []))
            tol = max(1.0, 0.02 * gross)
            if cand.get(ex) and (abs(dpos - exp) <= tol or abs(dpos + c - exp) <= tol):
                use_carry = abs(dpos - exp) > tol
                shadow_rows.append(dict(ex=ex, dpos=round(dpos, 2), carry=round(c, 2), orders=round(exp, 2), n=len(cand[ex]), match=True))
                if book:
                    for oid, d, s in cand[ex]:
                        r = self.ledger.book_fill(oid, d, self.ledger.orders[oid]["price"], "order_shrink", self.side_fair, self.side_mid,
                                                  held=prev.get(ex))
                        if r:
                            booked.append(r)
                            self.event("FILL", f"{r['bot']} [{r['layer'] or '-'}] (order_shrink: order left the book): {r['action']} "
                                               f"{r['side'].upper()} {d:,.0f} @ {r['price']:.3f}")
                    if use_carry:                          # last loop's audit put that move into inventory: take it back
                        books = self.ledger.subbooks.setdefault(ex, {})
                        books["inventory"] = books.get("inventory", 0) - c
            elif abs(dpos) >= 1:
                if c:                                      # carried and still not explained: unexplained for good
                    unexplained[ex] = c
                self.truth_carry[ex] = dpos
                shadow_rows.append(dict(ex=ex, dpos=round(dpos, 2), carry=round(c, 2), orders=round(exp, 2), n=len(cand.get(ex, [])),
                                        match=False, carried=True))
            elif c:
                unexplained[ex] = c
            elif cand.get(ex):                             # orders left the book but the position didn't move: not fills
                shadow_rows.append(dict(ex=ex, dpos=round(dpos, 2), carry=0, orders=round(exp, 2), n=len(cand[ex]), match=False,
                                        no_move=True))
        if shadow_rows or unexplained:
            self.intel_write("truth_shadow.jsonl", dict(t=round(now, 1), mode="book" if book else "shadow", rows=shadow_rows,
                                                        unexplained={ex: round(q, 2) for ex, q in unexplained.items()}))
        if booked or projection_booked:
            self.last_booked_t = now
        if unexplained:
            self.blind_t.append(now)
            self.event("AUDIT", "unexplained position moves (no fill, no order that left the book): "
                                + ", ".join(f"{self.name(ex)} {q:+,.0f}" for ex, q in unexplained.items()))
        recent = [t for t in self.blind_t if t >= now - 300]
        if book and len(recent) >= 2 and now - self.last_booked_t >= 300 and not self.fills_blind:
            self.fills_blind = now
            self.auto_pause("fills_blind", "FILLS BLIND: no fill booked while positions move", now)
            msg = "FILLS BLIND: position moves in >= 2 loops within 5 min and no fill booked: gates held, no new dump levels, release paused, no new MM entries"
            self.alerts.append(f"{time.strftime('%H:%M')} {msg}")
            self.event("BLIND", msg)
            self.night.line(msg)
        elif self.fills_blind and not recent and now - self.last_booked_t < 300:
            self.event("BLIND", "fills flow again: FILLS BLIND lifted")
            self.night.line("FILLS BLIND lifted: fills booked again and position moves explained")
            self.fills_blind = None
        if not unexplained and book:
            self.truth_t = now
        self.truth_filled = {oid: (self.ledger.orders.get(oid) or {}).get("filled") or 0.0 for oid in cur}
        return booked

    def apply_subbook_fix(self, fix):
        """Move shares between one market's sub-books (sum unchanged), close a dump tranche whose sub-book reaches 0, cancel
        the listed engine orders; logged before/after (event FIX, RUN_LOG)."""
        ex = fix["ex"]
        with self.ledger.lock:
            books = self.ledger.subbooks.setdefault(ex, {})
            before = {k: round(v, 2) for k, v in books.items()}
            total = sum(books.values())
            for sub, d in fix["moves"].items():
                books[sub] = books.get(sub, 0) + d
            for sub in [k for k, v in books.items() if abs(v) < 1e-6 and k != "inventory"]:
                del books[sub]
                t = self.ledger.tranches.get(sub)
                if t and t.get("state") == "open":
                    t.update(state="closed", note=f"subbook_fix {stamp()}: {fix['reason']}")
            assert abs(sum(books.values()) - total) < 1e-6
            after = {k: round(v, 2) for k, v in books.items()}
        n = 0
        for o in self.open:
            if o["id"] in fix["cancel"] and o["id"] not in self.ex.cancelling:
                n += bool(self.ex.cancel(o, reason="subbook_fix", front=True))
        self.ledger.save()
        msg = f"SUBBOOK FIX {self.name(ex)} (ex {ex}): {before} -> {after}; {n} order(s) cancelled {fix['cancel']}; {fix['reason']}"
        self.event("FIX", msg)
        self.night.line(msg)

    def backfill_fills(self, t0, pos, prev, dry_run=True, only=None):
        """Evening §1.3, once: fills since t0 that only the audit saw (position moves pushed into inventory). Per market, the
        audit's moves logged in engine.log between t0 and the start of live order-truth booking must equal the orders that
        left the book without our cancel before their TTL; then those orders are booked (source 'backfill', at their limit)
        and the audit's inventory moves are taken back, so the shares return to their owners (dump -> tranches, mm -> mm,
        release -> realized). Mismatched markets are reported, not booked. Sub-books before/after go to RUN_LOG."""
        end = self.truth_live_t or time.time()
        t0s, ends = t0, time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(end))
        aud, day, prev_s, cur_t = collections.defaultdict(float), None, None, None
        lp = re.compile(r"^(\d\d):(\d\d):(\d\d) loop ")
        au = re.compile(r"AUDIT  ex (\S+): sub-books were off by ([+-][\d,\.]+) shares -> inventory")
        today = datetime.now().date()
        lines = open(os.path.join(DATA, "engine.log"), errors="replace").read().splitlines()
        days_back = 0                                       # walk backwards so "today" anchors the clock times
        stamps = [None] * len(lines)
        for i in range(len(lines) - 1, -1, -1):
            m = lp.match(lines[i])
            if m:
                sec = int(m.group(1)) * 3600 + int(m.group(2)) * 60 + int(m.group(3))
                if prev_s is not None and sec > prev_s + 3600:
                    days_back += 1
                prev_s = sec
                d = today - timedelta(days=days_back)
                cur_t = f"{d} {m.group(1)}:{m.group(2)}:{m.group(3)}"
            stamps[i] = cur_t
        last = None
        for i, line in enumerate(lines):
            if lp.match(line):
                last = stamps[i]
                continue
            a = au.search(line)
            if a and last and t0s < last < ends:
                aud[a.group(1)] += float(a.group(2).replace(",", ""))
        cand = collections.defaultdict(list)
        for oid, o in self.ledger.orders.items():
            if o.get("status") != "done" or not o.get("closed") or not (t0s < o["closed"] < ends) or o.get("truth_q"):
                continue
            if only and oid not in only:
                continue
            ttl = o.get("ttl") or self.LAYER_TTL.get(o.get("layer"), RECYCLE_REST_TTL_S)
            age = time.mktime(time.strptime(o["closed"], "%Y-%m-%d %H:%M:%S")) - o.get("ts", 0)
            if ttl and age >= ttl - 90:
                continue
            q = o["qty"] - (o.get("filled") or 0)
            if q > 0.5 and o.get("action") in ("buy", "sell"):
                sgn = (1 if o["side"] == "yes" else -1) * (1 if o["action"] == "buy" else -1)
                cand[o["ex"]].append((oid, q, sgn, o["closed"]))
        lines_out, mism, moved = [], [], collections.Counter()
        for ex in sorted(set(cand) if only else set(aud) | set(cand)):
            exp = sum(s * q for _, q, s, _ in cand.get(ex, []))
            a = aud.get(ex, 0.0)
            if not cand.get(ex) or abs(exp - a) > max(10.0, 0.02 * abs(a)):
                mism.append(f"{self.name(ex)} (ex {ex}): audit {a:+,.0f} vs orders {exp:+,.0f} ({len(cand.get(ex, []))})")
                continue
            before = dict(self.ledger.subbooks.get(ex, {}))
            if dry_run:
                lines_out.append(f"{self.name(ex)} (ex {ex}): {len(cand[ex])} orders, {exp:+,.0f} shares would move from inventory to "
                                 + ", ".join(sorted({self.ledger.orders[o]['layer'] or '-' for o, _, _, _ in cand[ex]})))
                for oid, q, s, closed in cand[ex]:
                    moved[self.ledger.orders[oid].get("layer") or "-"] += q
                continue
            for oid, q, s, closed in cand[ex]:
                r = self.ledger.book_fill(oid, q, self.ledger.orders[oid]["price"], "backfill", self.side_fair, self.side_mid,
                                          held=None, t=closed)
                if r:
                    moved[r["layer"] or "-"] += q
            books = self.ledger.subbooks.setdefault(ex, {})
            books["inventory"] = books.get("inventory", 0) - exp
            after = {k: v for k, v in books.items() if abs(v) >= 1}
            lines_out.append(f"{self.name(ex)} (ex {ex}): {len(cand[ex])} orders, {exp:+,.0f} shares; sub-books "
                             f"{ {k: round(v) for k, v in before.items() if abs(v) >= 1} } -> { {k: round(v) for k, v in after.items()} }")
        self.ledger.save()
        msg = (f"BACKFILL{' (DRY RUN, nothing booked)' if dry_run else ''}{' of ' + ','.join(only) if only else ''} {t0s} -> {ends}: "
               f"{len(lines_out)} markets {'would be ' if dry_run else ''}booked from order records ({', '.join(f'{l} {q:,.0f}' for l, q in moved.items())} "
               f"shares by layer), {len(mism)} not matched")
        self.event("BACKFILL", msg)
        self.night.line(msg)
        for l in lines_out:
            self.night.line(("  would book: " if dry_run else "  booked: ") + l)
        for l in mism:
            self.night.line("  NOT booked (audit vs orders differ, left in inventory): " + l)
        self.backfill_report = dict(t=stamp(), dry_run=dry_run, only=only, booked=lines_out, mismatched=mism, moved=dict(moved))
        self.intel_write("backfill.jsonl", self.backfill_report)

    # ------------------------------------------------------------------------------------------------ turnover MM (D-C)
    def update_tmm(self, now):
        """Per loop: the SIG mid EWMA (30 s half-life), a 5-min sample of it for the 24 h median, MM inventory age (offered
        at SIG mid after 2 h; handed to the release layer after 6 h) and the MM inventory at cost (turnover)."""
        for ex, (b, a, _) in self.px.items():
            if b is None or a is None or a <= b:
                continue
            m = (b + a) / 2
            t0, v = self.tmm_mid.get(ex, (now, m))
            v = v + (1 - 0.5 ** (max(0.0, now - t0) / TMM_MID_HALF_LIFE_S)) * (m - v)
            self.tmm_mid[ex] = (now, v)
            h = self.mid24.setdefault(ex, collections.deque(maxlen=288))
            if not h or now - h[-1][0] >= 300:
                h.append((now, v))
        inv = 0.0
        for ex, books in self.ledger.subbooks.items():
            q = books.get("mm", 0)
            if abs(q) < MIN_QTY:
                self.mm_since.pop(ex, None)
                continue
            self.mm_since.setdefault(ex, now)
            mid = (self.tmm_mid.get(ex) or (0, 0.5))[1]
            inv += abs(q) * (mid if q > 0 else 1 - mid)
            if now - self.mm_since[ex] > TMM_RELEASE_S and self.controls.on("mm"):
                self.ledger.move_sub(ex, "mm", "inventory")
                self.mm_since.pop(ex, None)
                self.event("MM", f"{self.name(ex)}: MM inventory {abs(q):,.0f} older than 6 h handed to the release layer")
        self.tmm_inv.append((now, inv))

    def tmm_view(self, ex, f, bid, ask):
        """The turnover MM's inputs for one market (None while the MM layer is off for it)."""
        if not TMM_ENABLED or f is None or not self.market_layers(ex).get("mm") or ex not in self.tmm_mid:
            return None
        mid = self.tmm_mid[ex][1]
        h = self.mid24.get(ex) or []
        if len(h) >= 12:                                    # >= 1 h of samples: SIG's own level over (up to) 24 h
            xs = sorted(v for _, v in h)
            med = xs[len(xs) // 2]
        else:
            med = self.ref(ex) if self.ref(ex) is not None else mid
        q = self.ledger.subbooks.get(ex, {}).get("mm", 0)
        cost = q * mid if q > 0 else q * (1 - mid)          # signed: + YES, - NO
        tier = "A" if ex in self.fat_tiers() else "B"
        return dict(mid=mid, kappa_mm=min(TMM_KAPPA_MAX, abs(med - f) + 0.01), size=TMM_SIZE[tier],
                    room={"yes": max(0.0, TMM_CAP_SIDE - max(cost, 0.0)), "no": max(0.0, TMM_CAP_SIDE - max(-cost, 0.0))},
                    skew=TMM_SKEW_TICKS * max(-1.0, min(1.0, cost / (0.5 * TMM_CAP_SIDE))),
                    offer_mid=ex in self.mm_since and time.time() - self.mm_since[ex] > TMM_OFFER_MID_S,
                    max_cost=self.release_max_cost())

    # ------------------------------------------------------------------------------------------------ dump catcher v3 (day 4 §2)
    def dump_arena(self, now):
        """{ex} of the DUMP_ARENA_N markets with the most big events in the last 48 h (intel events + tracked dumps),
        recomputed every 30 min (arena sides get 1.5x size)."""
        if self.dump_arena_t and now - self.dump_arena_t < 1800:
            return self.dump_arena_set
        n = collections.Counter()
        for row in intel._jsonl(os.path.join(DATA, "intel", "events.jsonl"), now - 48 * 3600):
            n[row["ex"]] += 1
        for row in intel._jsonl(os.path.join(DATA, "intel", "dumps.jsonl"), now - 48 * 3600):
            n[row["ex"]] += 1
        self.dump_arena_t, self.dump_arena_set = now, {ex for ex, _ in n.most_common(DUMP_ARENA_N)}
        return self.dump_arena_set

    def dump_ref_v3(self, ex):
        """(ref, sliding): ref = the SIG 30-min median (the pre-dump price); sliding while it sits > 2c under the 6-h
        median (a slide in progress: the ref is frozen at its last value and levels are never lowered)."""
        r30 = self.ref(ex)
        if r30 is None:
            return None, False
        h = [v for t, v in (self.mid24.get(ex) or []) if t >= time.time() - 6 * 3600]
        sliding = False
        if len(h) >= 12:
            xs = sorted(h)
            sliding = r30 < xs[len(xs) // 2] - DUMP_SLIDE
        if sliding and ex in self.dump_frozen:
            return self.dump_frozen[ex], True
        if sliding:
            self.dump_frozen[ex] = r30
            return r30, True
        self.dump_frozen.pop(ex, None)
        return r30, False

    def dump_eligible(self, ex):
        """[(side, fair_side, ref_side)]: mapped sides with a fresh Polymarket fair_side >= 0.80 and a SIG 30-min
        reference; never a manual market; never a side whose opposite we hold (the bid would net against it)."""
        b = self.owner.get(ex)
        f, (r, _) = self.fair.get(ex), self.dump_ref_v3(ex)
        if b is None or b.kind != "market" or ex in self.manual_ex or f is None or r is None:
            return []
        held = self.book.get(ex, {}).get("pos", 0)
        return [(side, fs, rs) for side, fs, rs in (("yes", f, r), ("no", 1 - f, 1 - r))
                if fs >= DUMP_MIN_FAIR and not (held <= -MIN_QTY if side == "yes" else held >= MIN_QTY)]

    def side_cost(self, ex, side, bids=True):
        """Position held on `side` at cost (SIG avgCost), plus (bids=True) our resting dump bids on it."""
        b = self.book.get(ex) or {}
        pos, row = b.get("pos", 0), b.get("row") or {}
        held = pos if side == "yes" else -pos
        cost = max(0.0, held) * float(row.get("avgCost") or 0) if held > 0 else 0.0
        for o in self.open if bids else []:
            rec = self.ledger.orders.get(o["id"])
            if rec and rec.get("layer") == "dump" and o["ex"] == ex and o["side"] == side:
                cost += o["qty"] * o["price"]
        return cost

    def dump_levels(self, ex, side, fs, rs, cost):
        """[(i, price, shares)]: L1/L2/L3 = ref - 1 / 2 / 3.5c, each <= fair_side - 3c and <= best bid + 1 tick (join or
        improve, never cross the ask); shares by weight 40 / 35 / 25 of `cost`."""
        bid_s, ask_s = bots.side_book(*self.px.get(ex, (None, None, None))[:2], side)
        out, used = [], set()
        for i, (off, w) in enumerate(zip(DUMP_V3_OFFSETS, DUMP_V3_WEIGHTS)):
            px = min(rs - off, fs - DUMP_FAIR_GAP)
            if bid_s is not None:
                px = min(px, bid_s + TICK)
            if ask_s is not None:
                px = min(px, ask_s - TICK)
            px = bots.tick_down(px)
            if px < 0.01 or px in used:
                continue
            used.add(px)
            q = int(cost * w / px)
            if q >= MIN_QTY:
                out.append((i, px, q))
        return out

    def dump_plan(self, now, cash, used):
        """{ex: [want-spec]} for dump catcher v3: every eligible side, size = funded reserve / 8 (1.5k..6k, arena x 1.5,
        pilot x 0.5), concentration cap 12k per side, news guard, breaker, reserve fit (admission). A level whose
        tranche is still being sold back is not re-placed; it re-arms as soon as the sell-back fills. After a restart
        L1 goes out on every side first, then L2, then L3."""
        if not self.controls.on("dump"):
            return {}
        half = 0.5 if self.controls.state("dump") == "pilot" else 1.0
        arena = self.dump_arena(now)
        cands = []
        for b in self.bots:
            if b.kind != "market":
                continue
            r = self.ref(b.ex)
            if r is not None:
                self.dump_ref[b.ex] = r                     # the pre-dump reference for the next loop's fills
            for side, fs, rs in self.dump_eligible(b.ex):
                cands.append((b.ex, side, fs, rs))
        # news guard: a side's fair down > 3c within 10 min pulls its levels for 30 min
        for ex in {c[0] for c in cands}:
            h = [x for x in self.fair_hist.get(ex, []) if x[0] >= now - FAT_NEWS_WINDOW_S]
            f = self.fair.get(ex)
            if f is None:
                continue
            h.append((now, f))
            self.fair_hist[ex] = h
            for side in ("yes", "no"):
                cur = f if side == "yes" else 1 - f
                top = max((v if side == "yes" else 1 - v) for _, v in h)
                if top - cur > DUMP_NEWS_DROP:
                    if self.dump_block.get((ex, side), 0) < now:
                        self.event("NEWS", f"{self.name(ex)} {side.upper()} fair fell {top - cur:.2f} in 10 min: dump levels pulled 30 min")
                    self.dump_block[(ex, side)] = now + FAT_NEWS_HOLD_S
        funded = max(0.0, min(float(OPPORTUNITY_RESERVE), cash - CASH_RESERVE - used["committed"]))
        mult = self.ratchet_mult("dump")                   # day 5 §1.7: x1.25 per passing 2-h gate
        per_side = max(DUMP_SIDE_MIN, min(DUMP_SIDE_MAX, funded / DUMP_SIDE_DIV)) * mult
        party = dash4.party_exposure(self)                  # day 5 §1.5: held dump inventory per party side <= 40k
        resting, rest_px = {}, {}
        for o in self.open:
            rec = self.ledger.orders.get(o["id"])
            if rec and rec.get("layer") == "dump":
                tail = str(rec.get("sub", "")).rsplit(":", 1)[-1]
                resting.setdefault((o["ex"], o["side"]), set()).add(int(tail) if tail.isdigit() else 0)
                rest_px[rec.get("sub")] = o["price"]
        plan, rows = {}, []
        for ex, side, fs, rs in cands:
            key = (ex, side)
            if self.dump_block.get(key, 0) > now or self.breaker.get(("dump", self.pol(ex, side)), 0) > now or self.ex.is_frozen(ex):
                continue
            if party.get(self.pol(ex, side), 0.0) >= DUMP_PARTY_CAP:
                continue                                    # that party side is at its 40k exposure cap: no new levels
            room = DUMP_CONC_CAP - self.side_cost(ex, side, bids=False)   # position + our levels <= 12k (levels sized here)
            cost = min(per_side * (DUMP_ARENA_MULT if ex in arena else 1.0), DUMP_ARENA_MAX if ex in arena else DUMP_SIDE_HARD_MAX,
                       funded / 2) * half
            cost = min(cost, room)
            if cost < DUMP_MIN_SIDE_COST:
                continue
            sliding = self.dump_ref_v3(ex)[1]
            books = self.ledger.subbooks.get(ex, {})
            for i, px, q in self.dump_levels(ex, side, fs, rs, cost):
                sub = f"dump:{ex}:{side}:{i}"
                if abs(books.get(sub, 0)) >= MIN_QTY:
                    continue                                # its tranche is being sold back: re-arms when that fills
                if sliding and sub in rest_px and px < rest_px[sub] - 1e-9:
                    px = rest_px[sub]                       # a slide in progress: never lower a resting level
                    q = int(cost * DUMP_V3_WEIGHTS[i] / px)
                rows.append((i, i in resting.get(key, set()), ex, dict(side=side, px=px, qty=q, sub=sub, level=i)))
        # L1 everywhere first: a new L2 goes out only when no side is missing L1, a new L3 only when none is missing L2
        missing = {lvl: any(r[0] == lvl and not r[1] for r in rows) for lvl in range(3)}
        for lvl, is_resting, ex, w in rows:
            if is_resting or lvl == 0 or (lvl == 1 and not missing[0]) or (lvl == 2 and not missing[0] and not missing[1]):
                plan.setdefault(ex, []).append(w)
        return plan

    def track_dumps(self, now):
        """A filled level becomes a tranche: sell-back for the whole tranche at max(pre-dump ref_side, fill + 1.5c), never
        above fair_side - 0.5c (the A2 exception); unfilled after 60 min it steps down to fill + 1c; after 6 h the
        tranche goes to inventory (held to settlement: every fill was >= 3c under fair)."""
        tr, self.dump_view = self.ledger.tranches, {}
        for ex, books in list(self.ledger.subbooks.items()):
            for sub, q in list(books.items()):
                if not sub.startswith("dump:") or abs(q) < 1 or (sub in tr and tr[sub].get("state") == "open"):
                    continue
                side = "yes" if q > 0 else "no"
                if side != sub.split(":")[2]:                  # only a dump entry fill makes a tranche, never an exit
                    self.event("DUMP", f"{self.name(ex)}: {sub} holds {q:+,.0f} on the wrong side (an exit oversold): no tranche")
                    continue
                r = self.dump_ref.get(ex, self.ref(ex))
                tr[sub] = dict(ex=ex, side=side, kind="dump", pre_ref_side=None if r is None else (r if side == "yes" else 1 - r),
                               t=stamp(), ts=now, state="open")
                self.event("DUMP", f"{self.name(ex)}: dump fill -> tranche {sub} {side.upper()} {abs(q):,.0f}, pre-dump ref "
                                   f"{tr[sub]['pre_ref_side'] if tr[sub]['pre_ref_side'] is None else round(tr[sub]['pre_ref_side'], 3)}")
        for sub, t in tr.items():
            if t.get("kind") != "dump" or t["state"] != "open":
                continue
            ex = t["ex"]
            q = self.ledger.subbooks.get(ex, {}).get(sub, 0)
            if abs(q) < 1:
                t.update(state="closed", closed_ts=now, hold_min=round((now - t.get("ts", now)) / 60, 1))
                self.event("DUMP", f"{self.name(ex)}: tranche {sub} sold back after {t['hold_min']:.0f} min (round trip done)")
                for o in self.open:                         # its exit must not keep filling
                    rec = self.ledger.orders.get(o["id"])
                    if rec and rec.get("sub") == sub and rec.get("layer") == "dumpexit" and o["id"] not in self.ex.cancelling:
                        self.ex.cancel(o, reason="tranche done: exit no longer owns shares", front=True)
                continue
            age = now - t.get("ts", now)
            if age > DUMP_HOLD_S:
                self.ledger.move_sub(ex, sub, "inventory")
                t["state"] = "aged"
                self.event("DUMP", f"{self.name(ex)}: sell-back for {sub} not filled in 6 h: {abs(q):,.0f} shares to inventory")
                continue
            since = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(t.get("ts", now) - 300))
            avg = self.ledger.tranche_avg(sub, "dump", ex=ex, side=t["side"], since=since) or t.get("avg")
            if avg is None:
                continue
            t["avg"] = round(avg, 5)
            pre, fs = t.get("pre_ref_side"), self.side_fair(ex, t["side"])
            r_now = self.ref(ex)
            ref_side = None if r_now is None else (r_now if t["side"] == "yes" else 1 - r_now)
            if age <= DUMP_STEP1_S:                          # day 5 §1.4: 0-20 min max(pre-dump ref, fill + 1.5c)
                target, floor_ = max(avg + DUMP_SELL_MIN, pre or 0.0), avg + DUMP_STEP_MIN
            elif age <= DUMP_STEP2_S:                        # 20-60 min: max(fill + 1c, ref - 0.5c)
                target, floor_ = max(avg + DUMP_STEP_MIN, (ref_side - 0.005) if ref_side is not None else 0.0), avg + DUMP_STEP_MIN
            else:                                            # > 60 min: fill + 0.5c
                target, floor_ = avg + DUMP_STEP2_MIN, avg + DUMP_STEP2_MIN
            if fs is not None:
                target = min(target, fs - DUMP_SELL_FAIR_GAP)
            rebound = bots.tick_up(round(max(target, floor_), 6))
            self.dump_view.setdefault(ex, []).append(dict(sub=sub, rebound=min(rebound, 0.99), avg=avg))

    def dump_fill_events(self, fills):
        for f in fills:
            if f["layer"] != "dump" or f["action"] != "buy":
                continue
            fv = self.side_fair(f["ex"], f["side"])
            msg = (f"DUMP FILL {self.name(f['ex'])}: bought {f['qty']:,.0f} {f['side'].upper()} at {f['price']:.3f}"
                   + ("" if fv is None else f" (fair {fv:.3f}, edge {(fv - f['price']) * f['qty']:+,.0f} at fair)"))
            self.event("DUMP", msg)
            self.alerts.append(f"{time.strftime('%H:%M')} {msg}")
            self.night.line(msg)
        now = time.time()
        recent = [f for f in self.fills_since(now - BREAKER_WINDOW_S) if f["layer"] == "dump" and f["action"] == "buy"]
        for direction in ("D", "R"):
            fs = [f for f in recent if self.pol(f["ex"], f["side"]) == direction]
            sides = {(f["ex"], f["side"]) for f in fs}
            moved = [f["fair"] - self.side_fair(f["ex"], f["side"]) for f in fs
                     if f.get("fair") is not None and self.side_fair(f["ex"], f["side"]) is not None]
            if len(sides) >= DUMP_BREAKER_SIDES and moved and sum(moved) / len(moved) >= DUMP_BREAKER_FAIR \
                    and self.breaker.get(("dump", direction), 0) < now:
                self.breaker[("dump", direction)] = now + BREAKER_PAUSE_S
                n = 0
                for o in self.open:
                    rec = self.ledger.orders.get(o["id"])
                    if rec and rec["layer"] == "dump" and self.pol(o["ex"], o["side"]) == direction:
                        n += bool(self.ex.cancel(o, reason="dump breaker"))
                msg = (f"DUMP BREAKER pro-{direction}: dump fills on {len(sides)} sides within 10 min and Polymarket moved "
                       f"{sum(moved) / len(moved) * 100:+.1f}c against them (news): {n} {direction}-side dump bids pulled 30 min")
                self.alerts.append(f"{time.strftime('%H:%M')} {msg}")
                self.event("BREAKER", msg)
                self.night.line(msg)

    # ------------------------------------------------------------------------------------------------ longshot catcher (day 3 §2)
    def longshot_eligible(self, ex):
        """[(side, fair_side)]: mapped sides with Polymarket fair_side <= 0.05; never manual; not where we hold the other side."""
        b, f = self.owner.get(ex), self.fair.get(ex)
        if b is None or b.kind != "market" or ex in self.manual_ex or f is None:
            return []
        held = self.book.get(ex, {}).get("pos", 0)
        return [(side, fs) for side, fs in (("yes", f), ("no", 1 - f))
                if fs <= LONG_MAX_FAIR + 1e-9 and not (held <= -MIN_QTY if side == "yes" else held >= MIN_QTY)]

    def longshot_levels(self, fs):
        """[(price, shares)]: 0.005 x 20k and 0.010 x 10k; only 0.005 x 10k where fair_side < 0.005 (worst case <= 200)."""
        lv = [(LONG_LEVELS[0][0], LONG_TINY_QTY)] if fs < LONG_TINY_FAIR else list(LONG_LEVELS)
        out, cost = [], 0.0
        for px, q in lv:
            if cost + px * q <= LONG_SIDE_MAX_COST + 1e-9:
                out.append((px, q))
                cost += px * q
        return out

    def longshot_plan(self, now):
        """{ex: [want-spec]}: resting longshot bids (TTL 24 h, re-placed only when gone), with the news guard."""
        if not self.controls.on("longshot"):
            return {}
        plan = {}
        for b in self.bots:
            if b.kind != "market":
                continue
            ex, f = b.ex, self.fair.get(b.ex)
            if f is not None:                               # news guard: a longshot side up > 3c in 10 min
                h = [x for x in self.long_hist.get(ex, []) if x[0] >= now - LONG_NEWS_WINDOW_S] + [(now, f)]
                self.long_hist[ex] = h
                for side in ("yes", "no"):
                    cur = f if side == "yes" else 1 - f
                    low = min((v if side == "yes" else 1 - v) for _, v in h)
                    if cur - low > LONG_NEWS_RISE and low <= LONG_MAX_FAIR:
                        if self.long_block.get((ex, side), 0) < now:
                            self.event("NEWS", f"{self.name(ex)} {side.upper()} fair rose {cur - low:.2f} in 10 min: longshot levels pulled 30 min")
                        self.long_block[(ex, side)] = now + LONG_NEWS_HOLD_S
            for side, fs in self.longshot_eligible(ex):
                if self.long_block.get((ex, side), 0) > now or self.ex.is_frozen(ex):
                    continue
                for px, q in self.longshot_levels(fs):
                    plan.setdefault(ex, []).append(dict(side=side, px=px, qty=q, sub=f"longshot:{ex}:{side}:{px:.3f}"))
        return plan

    def longshot_fill_events(self, fills):
        now = time.time()
        for f in fills:
            if f["layer"] != "longshot" or f["action"] != "buy":
                continue
            fv = self.side_fair(f["ex"], f["side"])
            msg = (f"LONGSHOT FILL {self.name(f['ex'])}: bought {f['qty']:,.0f} {f['side'].upper()} at {f['price']:.3f}"
                   + ("" if fv is None else f" (fair {fv:.3f})") + f"; exits at {LONG_EXIT_MULT:.0f}x entry and the SIG 6-h median")
            self.event("LONGSHOT", msg)
            self.alerts.append(f"{time.strftime('%H:%M')} {msg}")
            self.night.line(msg)
        recent = {(f["ex"], f["side"]) for f in self.fills_since(now - BREAKER_WINDOW_S) if f["layer"] == "longshot" and f["action"] == "buy"}
        if len(recent) >= LONG_BREAKER_SIDES and self.breaker.get(("long", "all"), 0) < now:
            self.breaker[("long", "all")] = now + LONG_BREAKER_PAUSE_S
            msg = f"LONGSHOT BREAKER: {len(recent)} longshot sides filled within 10 min: no new longshot levels for 30 min (report)"
            self.alerts.append(f"{time.strftime('%H:%M')} {msg}")
            self.event("BREAKER", msg)
            self.night.line(msg)

    def track_longshots(self, now):
        """Filled longshot levels -> exits: half at 3x entry, half at max(3x entry, the side's SIG 6-h median); never below
        3x entry (A2 exception, as the dump rebound); unsold after 72 h: kept, flagged (dashboard)."""
        self.long_view = {}
        for ex, books in self.ledger.subbooks.items():
            for sub, q in books.items():
                if not sub.startswith("longshot:") or abs(q) < MIN_QTY:
                    continue
                side = sub.split(":")[2]
                if (q > 0) != (side == "yes"):
                    continue                                # wrong side (an exit oversold): no exit of its own
                ids = {i for i, o in self.ledger.orders.items() if o.get("sub") == sub and o.get("layer") == "longshot"}
                fs = [f for f in self.ledger.fills if f["order"] in ids and f["action"] == "buy"]
                tq = sum(f["qty"] for f in fs)
                if not tq:
                    continue
                avg = sum(f["qty"] * f["price"] for f in fs) / tq
                first = min(time.mktime(time.strptime(f["t"], "%Y-%m-%d %H:%M:%S")) for f in fs)
                h = [v for t, v in (self.mid24.get(ex) or []) if t >= now - 6 * 3600]
                med = None
                if h:
                    xs = sorted(h)
                    med = xs[len(xs) // 2] if side == "yes" else 1 - xs[len(xs) // 2]
                p1 = bots.tick_up(LONG_EXIT_MULT * avg)
                p2 = bots.tick_up(max(LONG_EXIT_MULT * avg, med or 0.0))
                self.long_view.setdefault(ex, []).append(dict(sub=sub, side=side, avg=round(avg, 4), qty=abs(q), p1=p1, p2=p2,
                                                              stale=now - first > LONG_STALE_S))

    def long_exit_ok(self, f):
        """Gate check: a longshot fill got its exit order within 30 s (the ledger's exit orders on that sub)."""
        t0 = time.mktime(time.strptime(f["t"], "%Y-%m-%d %H:%M:%S"))
        sub = (self.ledger.orders.get(f["order"]) or {}).get("sub")
        return any(o.get("sub") == sub and o.get("layer") == "longexit" and t0 - 5 <= o.get("ts", 0) <= t0 + 30
                   for o in self.ledger.orders.values())

    # ------------------------------------------------------------------------------------------------ I4 self-cross guard
    @staticmethod
    def yes_quote(side, action, px):
        """An order in YES terms: ('bid' | 'ask', YES price). A NO buy at p is a YES ask at 1 - p."""
        return ("bid" if (side == "yes") == (action == "buy") else "ask", px if side == "yes" else round(1 - px, 3))

    def self_cross(self, w, own):
        """The first of our own orders `w` would trade against (I4: no self-trades), or None."""
        k, p = self.yes_quote(w["side"], w.get("action", "buy"), w["px"])
        for o in own:
            k2, p2 = self.yes_quote(o["side"], o["action"], o["price"])
            if k != k2 and ((k == "bid" and p >= p2 - 1e-9) or (k == "ask" and p <= p2 + 1e-9)):
                return o
        return None

    def no_self_cross(self, b, places, cancels):
        """I4 for every placement: drop a want that would trade against one of our own resting / queued orders. When an
        exit (reducing want) meets one of our own entries, the entry is cancelled so the exit goes next loop."""
        going = {o.get("id") for o in cancels if o.get("id")} | set(self.ex.cancelling)
        own = [o for o in self.open if o["ex"] == b.ex and o["id"] not in going] + \
              [p for p in self.ex.pending() if p["ex"] == b.ex]
        if not own:
            return places
        out = []
        for w in places:
            o = self.self_cross(w, own)
            if o is None:
                out.append(w)
                continue
            rec = self.ledger.orders.get(o.get("id") or "") or {}
            if (w["layer"] in self.REDUCING or w.get("reduce")) and o.get("id") and rec and rec.get("bot") not in self.position_names \
                    and not (rec.get("layer") in self.REDUCING) and not jx.reduces(o["side"], o["action"], self.book.get(b.ex, {}).get("pos", 0)):
                self.ex.cancel(o, reason="self-cross: an exit needs this price", front=True)
            k = (b.ex, w["side"], w["layer"])
            if time.time() - self.cross_log.get(k, 0) > 600:
                self.cross_log[k] = time.time()
                self.event("I4", f"{self.name(b.ex)}: {w['layer']} {w.get('action', 'buy')} {w['side'].upper()} @ {w['px']:.3f} not sent: "
                                 f"it would trade with our own {o['action']} {o['side'].upper()} @ {o['price']:.3f}")
        return out

    def stop_and_pull(self, reason):
        """Pause new risk and pull every at-risk engine order (never exits, never manual orders), once per incident."""
        if self.faults_pulled and time.time() - self.faults_pulled < 1800:
            return
        self.faults_pulled = time.time()
        self.auto_pause("stop_pull", f"STOP AND PULL: {reason}", time.time())
        try:
            n, dropped = self.pull_quotes()
        except Exception as err:
            n, dropped = 0, 0
            self.event("ERR", f"pull: {err!r}")
        msg = f"STOP AND PULL: {reason} -> new risk paused, {n} at-risk engine orders being cancelled, {dropped} queued dropped"
        self.alerts.append(f"{time.strftime('%H:%M')} {msg}")
        self.event("STOP", msg)
        self.night.line(msg)

    AUTO_KINDS = ("cash_short", "stop_pull", "fills_blind")

    def auto_pause(self, kind, reason, now):
        """Day 4 §1.3: a safety pause the engine lifts itself once its condition is gone (manual pauses wait for Piero).
        The same kind 3x in 2 h stays paused and raises an alert."""
        c = self.controls
        self.pause_t = now
        with c.lock:
            ap = c.d.setdefault("auto_pause", {})
            hist = [t for t in ap.get("history", {}).get(kind, []) if t >= now - 7200] + [now]
            ap.setdefault("history", {})[kind] = hist
            ap.update(kind=kind, reason=reason[:200], t=now, stuck=len(hist) >= 3)
            c.save()
        c.set_paused(True, reason)
        if len(hist) >= 3:
            msg = f"{kind} paused 3x within 2 h: staying paused until Piero resumes"
            self.alerts.append(f"{time.strftime('%H:%M')} {msg}")
            self.event("PAUSE", msg)
            self.night.line(msg)

    def auto_resume(self, now):
        """Every 5 min: lift an automatic pause whose condition is gone (cash >= floor + 5k for 2 loops; SIG writes >= 80%
        OK over the last 20; fills healthy). Also logs any pause > 10 min at the top of RUN_LOG (hourly status)."""
        c = self.controls
        ap = c.d.get("auto_pause") or {}
        if not c.paused or not ap.get("kind"):
            return
        a = self.acct or {}
        cash = (a.get("totalAccountValue") or 0) - (a.get("totalHoldingsValue") or 0)
        self.cash_ok_n = self.cash_ok_n + 1 if cash >= CASH_RESERVE + 5_000 else 0
        if ap.get("stuck") or now - (ap.get("t") or now) < 300:   # re-checked every loop (local data) from 5 min after the pause
            return
        kind = ap["kind"]
        ok = {"cash_short": self.cash_ok_n >= 2,
              "stop_pull": api.recent_writes_ok(20) >= 0.8,
              "fills_blind": not self.fills_blind and self.fills_health(now)[0]}.get(kind, False)
        if not ok:
            return
        with c.lock:
            c.d["auto_pause"] = dict(history=ap.get("history", {}))
            c.d["decisions"] = [d for d in c.d.get("decisions", []) if not d.startswith(("CASH SHORT", "STOP AND PULL"))]
            c.save()
        if kind == "cash_short":
            self.cash_short = None
        c.set_paused(False, f"auto-resume: {kind} condition gone after {(now - ap['t']) / 60:.0f} min")
        msg = f"AUTO-RESUME after {kind}: paused {(now - ap['t']) / 60:.0f} min ({ap.get('reason', '')[:100]})"
        self.event("PAUSE", msg)
        self.night.line(msg)

    def ratchet_mult(self, layer):
        r = (self.controls.d.get("ratchet") or {}).get(layer) or {}
        return RATCHET_STEP ** int(r.get("level", 0))

    def ratchet(self, layer, now):
        """Day 5 §1.7: every 2 h a full layer passes its gate, raise its size one notch (x1.25); the first failed gate steps
        back one notch and holds 2 h. Caps are applied where the size is used. Every step is logged."""
        c = self.controls
        if c.state(layer) != "full" or c.paused:
            return
        with c.lock:
            rr = c.d.setdefault("ratchet", {})
            r = rr.setdefault(layer, dict(level=0, since=now, hold_until=0.0, history=[]))
        if now < r.get("hold_until", 0) or now - r.get("since", now) < RATCHET_EVERY_S:
            return
        g = self.dump_gate(r.get("since", now - RATCHET_EVERY_S), now) if layer == "dump" else self.scalp_gate(r.get("since", now), now)
        with c.lock:
            if g["ok"]:
                r["level"] = int(r.get("level", 0)) + 1
                step = f"{layer} ratchet up to level {r['level']} (x{RATCHET_STEP ** r['level']:.2f})"
            else:
                r["level"] = max(0, int(r.get("level", 0)) - 1)
                r["hold_until"] = now + RATCHET_EVERY_S
                step = f"{layer} ratchet back to level {r['level']} (x{RATCHET_STEP ** r['level']:.2f}), holding 2 h"
            r["since"] = now
            r.setdefault("history", []).append(dict(t=stamp(), level=r["level"], ok=g["ok"]))
            r["history"] = r["history"][-50:]
            c.save()
        msg = f"RATCHET {step}: {g['why']}"
        self.event("RATCHET", msg)
        self.night.line(msg)

    # ------------------------------------------------------------------------------------------------ scalper (day 5 §2, Mode A)
    def note_volume(self, ex, ts, notional):
        """Our own tape -> traded value per market over the last 24 h (the scalper's market ranking)."""
        d = self.vol24.setdefault(str(ex), collections.deque())
        d.append((ts, notional))
        cut = time.time() - 86400
        while d and d[0][0] < cut:
            d.popleft()

    def load_vol24(self):
        """Startup: the last 24 h of data/intel/trades/<day>.jsonl (the tape the engine itself collects)."""
        now, cut = time.time(), time.time() - 86400
        for day in (cut, now):
            path = os.path.join(DATA, "intel", "trades", time.strftime("%Y-%m-%d", time.localtime(day)) + ".jsonl")
            if not os.path.exists(path):
                continue
            with open(path) as fh:
                for line in fh:
                    try:
                        r = json.loads(line)
                    except ValueError:
                        continue
                    if (r.get("ts") or 0) >= cut and r.get("p") is not None:
                        self.note_volume(r["ex"], r["ts"], float(r["p"]) * float(r.get("q") or 0))

    def scalp_rank(self, n):
        """The n highest-traded mapped markets over 24 h (traded value on our tape); never a manual market."""
        vols = {ex: sum(v for _, v in d) for ex, d in self.vol24.items()}
        mk = [b.ex for b in self.bots if b.kind == "market" and vols.get(b.ex) and b.ex not in self.manual_ex]
        return sorted(mk, key=lambda ex: -vols[ex])[:n]

    def scalp_slice(self):
        """The scalper's inventory slice at cost: the layer cap x the ratchet, plus the idle-cash rule's bonus, <= 20k."""
        bonus = float((self.controls.d.get("idle") or {}).get("scalp_bonus") or 0)
        return min(float(SCALP_SLICE_MAX), self.cap("scalp", "inventory") * self.ratchet_mult("scalp") + bonus)

    def scalp_cost(self, ex=None):
        """Scalper inventory at cost (its tranche's average entry), one market or all."""
        out = 0.0
        for e, books in self.ledger.subbooks.items():
            if ex is not None and e != ex:
                continue
            for sub, q in books.items():
                if sub.startswith("scalp:") and abs(q) >= MIN_QTY:
                    avg = (self.ledger.tranches.get(sub) or {}).get("avg") or self.sig_mark(e, "yes" if q > 0 else "no") or 0.0
                    out += abs(q) * float(avg)
        return out

    def scalp_plan(self, now, cash, used):
        """{ex: [want-spec]} (day 5 §2): on the 20 busiest markets (10 in the pilot) join the best bid, or improve it by a
        tick when the spread is >= 2 ticks, only at <= fair_side - 2c; clips 300-500 shares; <= 2 resting buys per market;
        inventory 3k per market and the slice in total (resting buys count against the room only for new placements, so
        a resting order never cancels itself); no new buys while the dump catcher's funded reserve < 15k, new risk is
        paused, or fills are blind; <= 6 placements/min for the layer (requotes and new sides wait for budget)."""
        c = self.controls
        if not c.on("scalp") or c.paused or self.fills_blind:
            return {}
        funded = self.dump_demand(cash, used)["funded"]
        if funded < SCALP_YIELD_RESERVE:
            if now - self.scalp_note.get("yield", 0) >= 1800:
                self.scalp_note["yield"] = now
                self.event("SCALP", f"scalper yields to the dump catcher: funded reserve {funded:,.0f} < {SCALP_YIELD_RESERVE:,.0f} (no new buys)")
            return {}
        budget = SCALP_WRITES_PER_MIN - sum(1 for o in self.ledger.orders.values()
                                            if o.get("layer") in ("scalp", "scalpexit") and (o.get("ts") or 0) >= now - 60)
        existing, resting_cost = {}, 0.0
        for o in self.open:
            rec = self.ledger.orders.get(o["id"])
            if rec and rec.get("layer") == "scalp" and o["id"] not in self.ex.cancelling:
                existing[(o["ex"], o["side"])] = o
                resting_cost += o["qty"] * o["price"]
        n = SCALP_MARKETS if c.state("scalp") == "full" else SCALP_PILOT_MARKETS
        room_total = self.scalp_slice() - self.scalp_cost() - resting_cost
        plan = {}
        for ex in self.scalp_rank(n):
            f, px = self.fair.get(ex), self.px.get(ex)
            if f is None or px is None or px[0] is None or px[1] is None or px[1] <= px[0] or self.ex.is_frozen(ex):
                continue
            bid_y, ask_y = px[0], px[1]
            held = self.book.get(ex, {}).get("pos", 0)
            room_m = SCALP_INV_MARKET - self.scalp_cost(ex) - sum(o["qty"] * o["price"] for (e, _), o in existing.items() if e == ex)
            n_rest = 0
            for side in ("yes", "no"):
                if n_rest >= SCALP_MAX_BUYS:
                    break
                fs = f if side == "yes" else 1 - f
                best = bid_y if side == "yes" else round(1 - ask_y, 3)
                p = round(best + TICK, 3) if round((ask_y - bid_y) / TICK) >= SCALP_IMPROVE_SPREAD else best
                exits = [x["px"] for x in self.scalp_view.get(ex, []) if x["side"] == side]
                if exits:                                                  # I4: the bid stays a tick under our own exit
                    p = min(p, round(min(exits) - TICK, 3))
                cur = existing.get((ex, side))
                if cur is not None and exits and cur["price"] > min(exits) - TICK + 1e-9:
                    if budget <= 0:                                        # it would trade with our exit: pull it (a cancel, no placement)
                        continue
                elif cur is not None and abs(cur["price"] - p) < 1e-9:    # resting at the right price: keep (no write)
                    plan.setdefault(ex, []).append(dict(side=side, px=cur["price"], qty=int(cur["qty"]), sub=f"scalp:{ex}:{side}"))
                    n_rest += 1
                    continue
                if (p > fs - SCALP_BUY_EDGE + 1e-9 or not (MB_MIN_PX <= p <= MB_MAX_PX)
                        or (held <= -MIN_QTY if side == "yes" else held >= MIN_QTY)      # never bid against our own other side
                        or self.dump_block.get((ex, side), 0) > now):                   # news guard (3c in 10 min)
                    if cur is not None and budget <= 0:
                        plan.setdefault(ex, []).append(dict(side=side, px=cur["price"], qty=int(cur["qty"]), sub=f"scalp:{ex}:{side}"))
                        n_rest += 1
                    continue
                if cur is not None:                                        # the touch moved: requote when there is budget
                    if budget <= 0:
                        plan.setdefault(ex, []).append(dict(side=side, px=cur["price"], qty=int(cur["qty"]), sub=f"scalp:{ex}:{side}"))
                    else:
                        plan.setdefault(ex, []).append(dict(side=side, px=p, qty=int(cur["qty"]), sub=f"scalp:{ex}:{side}"))
                        budget -= 1
                    n_rest += 1
                    continue
                room = min(room_m, room_total)
                q = min(SCALP_CLIP_MAX, int(room / p)) if room > 0 else 0
                if q < SCALP_CLIP_MIN or budget <= 0:
                    continue
                plan.setdefault(ex, []).append(dict(side=side, px=p, qty=q, sub=f"scalp:{ex}:{side}"))
                room_m -= q * p
                room_total -= q * p
                budget -= 1
                n_rest += 1
        return plan

    def track_scalps(self, now):
        """Scalper inventory -> exits: ask at best ask - 1 tick, never below entry + 0.5c; after 30 min entry + 0 when that
        is still >= fair_side - 4c; after 2 h the shares are handed to the dump catcher's tranche rules (sell-back, then
        inventory). A tranche per market and side; a sold-out tranche closes (round trip)."""
        tr, self.scalp_view = self.ledger.tranches, {}
        for ex, books in list(self.ledger.subbooks.items()):
            for sub, q in list(books.items()):
                if not sub.startswith("scalp:") or abs(q) < MIN_QTY:
                    continue
                side = sub.split(":")[2]
                if (q > 0) != (side == "yes"):
                    continue                                 # wrong side (an exit oversold): the audit's business
                t = tr.get(sub)
                if t is None or t.get("state") != "open":
                    t = tr[sub] = dict(ex=ex, side=side, kind="scalp", t=stamp(), ts=now, state="open")
                since = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(t.get("ts", now) - 300))
                avg = self.ledger.tranche_avg(sub, "scalp", ex=ex, side=side, since=since) or t.get("avg")
                if avg is None:
                    continue
                t["avg"] = round(avg, 5)
                age = now - t.get("ts", now)
                if age > SCALP_HAND_S:
                    dsub = f"dump:{ex}:{side}:scalp"
                    self.ledger.move_sub(ex, sub, dsub)
                    t["state"] = "handed"
                    r = self.dump_ref.get(ex, self.ref(ex))
                    tr[dsub] = dict(ex=ex, side=side, kind="dump", pre_ref_side=None if r is None else (r if side == "yes" else 1 - r),
                                    t=stamp(), ts=now, state="open", avg=t["avg"], from_="scalp")
                    self.event("SCALP", f"{self.name(ex)}: {abs(q):,.0f} {side.upper()} unsold after 2 h (entry {avg:.3f}): handed to the dump catcher's tranche rules")
                    continue
                px = self.px.get(ex)
                if px is None or px[0] is None or px[1] is None:
                    continue
                best_ask = px[1] if side == "yes" else round(1 - px[0], 3)
                fs = self.side_fair(ex, side)
                floor_ = avg + SCALP_SELL_MIN
                if age > SCALP_STEP1_S and fs is not None and avg >= fs - SCALP_STEP1_GAP - 1e-9:
                    floor_ = avg
                p = bots.tick_up(round(max(round(best_ask - TICK, 3), floor_), 6))
                self.scalp_view.setdefault(ex, []).append(dict(sub=sub, side=side, px=min(p, 0.99), avg=avg, age_min=round(age / 60, 1)))
        for sub, t in tr.items():
            if t.get("kind") == "scalp" and t.get("state") == "open" and abs(self.ledger.subbooks.get(t["ex"], {}).get(sub, 0)) < MIN_QTY:
                t.update(state="closed", closed_ts=now, hold_min=round((now - t.get("ts", now)) / 60, 1))
                self.event("SCALP", f"{self.name(t['ex'])}: scalp {sub} sold after {t['hold_min']:.0f} min (round trip done)")
        self.scalp_inv.append((now, self.scalp_cost()))

    def scalp_fill_events(self, fills):
        for f in fills:
            if f["layer"] not in ("scalp", "scalpexit"):
                continue
            fs = self.side_fair(f["ex"], f["side"])
            what = "bought" if f["layer"] == "scalp" else "sold (closing buy)"
            self.event("SCALP", f"SCALP {what} {f['qty']:,.0f} {f['side'].upper()} on {self.name(f['ex'])} at {f['price']:.3f}"
                                + ("" if fs is None else f" (fair {fs:.3f})") + ("" if f["layer"] == "scalp" else f", realized {f.get('realized') or 0:+,.0f}"))

    def scalp_stats(self, since):
        """Scalper KPIs since `since`: trades (buys + sells), realized (sells), mark-out 30m of the buys, turnover pace of
        the slice (traded value / average inventory, per day)."""
        buys = [f for f in self.fills_since(since) if f["layer"] == "scalp"]
        sells = [f for f in self.fills_since(since) if f["layer"] == "scalpexit"]
        mo = [f["mo30"] for f in buys if f.get("mo30") is not None]
        vol = sum(f["qty"] * f["price"] for f in buys + sells)
        inv = [v for t, v in self.scalp_inv if t >= since]
        avg_inv = max(1_000.0, sum(inv) / len(inv)) if inv else 1_000.0
        age = max(time.time() - since, 1.0)
        return dict(trades=len(buys) + len(sells), buys=len(buys), sells=len(sells), volume=round(vol, 2),
                    realized=round(sum(f.get("realized") or 0 for f in sells), 2),
                    markout30=round(sum(mo) / len(mo), 4) if mo else None, n_mo=len(mo),
                    avg_inv=round(avg_inv), pace=round(vol / avg_inv * 86400 / age, 2))

    def scalp_gate(self, since, now):
        s = self.scalp_stats(since)
        trims = self.ex.trims.get("scalp", 0) + self.ex.trims.get("scalpexit", 0)
        ok = (s["realized"] > 0 and (s["markout30"] is None or s["markout30"] >= 0) and s["trades"] >= SCALP_GATE_TRADES
              and s["pace"] >= SCALP_GATE_PACE and trims == 0)
        why = (f"{s['trades']} trades ({s['buys']} buys, {s['sells']} sells), realized {s['realized']:+,.0f}, mark-out 30m {s['markout30']}, "
               f"turnover {s['pace']:.1f}x/day on {s['avg_inv']:,.0f} average inventory, {trims} I2 trims (gate: realized > 0, "
               f"mark-out >= 0, >= {SCALP_GATE_TRADES} trades, >= {SCALP_GATE_PACE:.0f}x/day, no trims)")
        return dict(ok=ok, why=why, stats=s)

    # ------------------------------------------------------------------------------------------------ no idle cash (day 5 §3)
    def roc_hold(self, now):
        """The minimum return to settlement for takes: 8% while the idle-cash rule's second step is on, else 10%."""
        return IDLE_ROC_MIN if ((self.controls.d.get("idle") or {}).get("roc_until") or 0) > now else ROC_HOLD_MIN

    def idle_rule(self, now):
        """Capital use < 40% for 2 h with no pause -> first the scalper slice +5k (max 20k), then takes at ROC >= 8% for 2 h;
        one step per 2 h, each logged; reversed as soon as use > 70%."""
        c = self.controls
        hist = [(t, u) for t, u in self.cap_use_hist if t >= now - IDLE_HOLD_S and u is not None]
        if len(hist) < 2 or hist[0][0] > now - IDLE_HOLD_S + 600:
            return                                          # less than 2 h of samples
        idle = c.d.get("idle") or {}
        use_now, use_max = hist[-1][1], max(u for _, u in hist)
        if use_now > IDLE_USE_HIGH and (idle.get("scalp_bonus") or (idle.get("roc_until") or 0) > now):
            with c.lock:
                c.d["idle"] = dict(scalp_bonus=0, roc_until=0, t=now)
                c.save()
            msg = f"IDLE RULE reversed: capital use {use_now:.0%} > {IDLE_USE_HIGH:.0%}: scalper bonus 0, takes back to ROC >= {ROC_HOLD_MIN:.0%}"
        elif use_max < IDLE_USE_LOW and not c.paused and self.pause_t < now - IDLE_HOLD_S and now - (idle.get("t") or 0) >= IDLE_HOLD_S \
                and c.on("scalp"):
            if self.scalp_slice() < SCALP_SLICE_MAX - 1e-9:
                add = min(float(IDLE_SLICE_STEP), SCALP_SLICE_MAX - self.scalp_slice())
                with c.lock:
                    c.d["idle"] = dict(idle, scalp_bonus=float(idle.get("scalp_bonus") or 0) + add, t=now)
                    c.save()
                msg = f"IDLE RULE step 1: capital use {use_max:.0%} max over 2 h < {IDLE_USE_LOW:.0%}: scalper slice +{add:,.0f} -> {self.scalp_slice():,.0f}"
            elif (idle.get("roc_until") or 0) < now:
                with c.lock:
                    c.d["idle"] = dict(idle, roc_until=now + IDLE_HOLD_S, t=now)
                    c.save()
                msg = f"IDLE RULE step 2: capital use {use_max:.0%} max over 2 h < {IDLE_USE_LOW:.0%}: takes allowed at ROC >= {IDLE_ROC_MIN:.0%} for 2 h"
            else:
                return
        else:
            return
        self.event("IDLE", msg)
        self.night.line(msg)

    def dump_stats(self, since):
        """Dump v3 KPIs since `since`: fills, round trips, realized, edge vs fair at fill, median hold (min), mark-out 30m."""
        fills = [f for f in self.fills_since(since) if f["layer"] == "dump" and f["action"] == "buy"]
        exits = [f for f in self.fills_since(since) if f["layer"] == "dumpexit"]
        holds = [t.get("hold_min") for t in self.ledger.tranches.values()
                 if t.get("kind") == "dump" and t.get("state") == "closed" and (t.get("closed_ts") or 0) >= since and t.get("hold_min") is not None]
        mo = [f["mo30"] for f in fills if f.get("mo30") is not None]
        return dict(fills=len(fills), shares=sum(f["qty"] for f in fills), cost=round(sum(f["qty"] * f["price"] for f in fills)),
                    edge_fair=round(sum(f["qty"] * ((f.get("fair") or f["price"]) - f["price"]) for f in fills)),
                    round_trips=len(holds), realized=round(sum(f.get("realized") or 0 for f in exits), 2),
                    median_hold_min=statistics.median(holds) if holds else None,
                    markout30=round(sum(mo) / len(mo), 4) if mo else None, n_mo=len(mo))

    def dump_gate(self, since, now):
        s = self.dump_stats(since)
        trims = self.ex.trims.get("dump", 0)
        short = self.cash_short is not None and self.cash_short["t"] >= since
        ok = (s["fills"] >= 20 and s["median_hold_min"] is not None and s["median_hold_min"] <= 60 and s["realized"] > 0
              and (s["markout30"] is None or s["markout30"] >= 0) and trims == 0 and not short)
        why = (f"{s['fills']} fills, {s['round_trips']} round trips, median hold {s['median_hold_min']} min, realized {s['realized']:+,.0f}, "
               f"mark-out 30m {s['markout30']}, {trims} I2 trims, cash short {short} (gate: >= 20 fills, hold <= 60, realized > 0, "
               f"mark-out >= 0, no trims, no CASH SHORT)")
        return dict(ok=ok, why=why, stats=s)

    def pause_view(self):
        """{kind, reason, since, minutes, auto} while new risk is paused (dashboard banner; status block)."""
        c = self.controls
        if not c.paused:
            return None
        ap = c.d.get("auto_pause") or {}
        t0 = ap.get("t") or c.d.get("paused_t") or time.time()
        return dict(kind=ap.get("kind") or "manual", reason=ap.get("reason") or "paused from the dashboard", auto=bool(ap.get("kind")),
                    since=time.strftime("%H:%M", time.localtime(t0)), minutes=round((time.time() - t0) / 60, 1), stuck=bool(ap.get("stuck")))

    CATCHERS = ("fatfinger", "dump", "longshot")           # resting catch levels whose fills are paid from the reserve

    def cash_short_check(self, raw, catch_open, now):
        """Afternoon D-B: we don't know what SIG does when a fill exceeds cash (reject, partial fill, negative balance).
        Detected three ways: the balance below 0 on two reads in a row; a write refused for funds (executor hook); a
        catcher order partly filled whose remainder vanished before its TTL (we didn't cancel it), or that still rests
        partly filled while the side's best bid sits below it (two loops in a row: the price traded through it)."""
        why = []
        self.neg_cash_n = self.neg_cash_n + 1 if raw < 0 else 0
        if self.neg_cash_n >= 2:
            why.append(f"balance {raw:,.0f} below 0 on {self.neg_cash_n} reads")
        msgs, self.cash_msgs[:] = list(self.cash_msgs), []
        why += [f"write refused: {m[:140]}" for m in msgs]
        # Day 4 §1.1: a vanished remainder is never evidence on its own. At its TTL (- 120 s) it expired. Otherwise one
        # GET /orders/{id} is logged as evidence, and only the balance (the hard triggers above) can pause.
        for oid in catch_open:
            o = self.ledger.orders.get(oid) or {}
            age = now - (o.get("ts") or now)
            if not (o.get("status") == "done" and 0 < (o.get("filled") or 0) < o["qty"] - MIN_QTY) or oid in self.vanish_seen:
                continue
            self.vanish_seen.add(oid)
            ttl = o.get("ttl") or self.LAYER_TTL.get(o.get("layer"), RECYCLE_REST_TTL_S)
            if ttl and age >= ttl - 120:
                continue                                    # expired at its TTL
            try:
                so = api.order(oid)
                ev = f"SIG: open={so.get('open')} quantity={so.get('quantity')}"
            except Exception as err:
                ev = f"order read failed: {str(err)[:80]}"
            self.event("CASHCHECK", f"{self.name(o['ex'])} {o['side'].upper()} {o['layer']} bid at {o['price']:.3f}: filled "
                                    f"{o['filled']:,.0f} of {o['qty']:,.0f}, the rest gone after {age / 60:.0f} min (TTL {ttl / 60:.0f}); "
                                    f"{ev}; balance {raw:,.0f}: {'SHORT' if raw < 0 else 'fine, no pause'}")
        if len(self.vanish_seen) > 2000:
            self.vanish_seen = set(list(self.vanish_seen)[-1000:])
        if why:
            self.cash_short_trip(why, raw, now)

    def cash_short_trip(self, why, raw, now):
        """At once: cancel every engine entry order (cancel lane), drop queued entries, pause new risk (exits and the
        release layer keep running: release refills cash), log CASH SHORT; the first time, record it in RECAP."""
        first = self.cash_short is None
        self.cash_short = dict(t=now, why=why, cash=raw)
        dropped = self.ex.drop_queued(lambda b, s, l, o: o[2] == "buy" and not jx.reduces(o[1], "buy", self.book.get(o[0], {}).get("pos", 0)))
        n = 0
        for o in self.open:
            rec = self.ledger.orders.get(o["id"])
            if rec and o["action"] == "buy" and rec["bot"] not in self.position_names and o["id"] not in self.ex.cancelling \
                    and not jx.reduces(o["side"], "buy", self.book.get(o["ex"], {}).get("pos", 0)):
                n += bool(self.ex.cancel(o, reason="CASH SHORT", front=True))
        if not self.controls.paused:
            self.auto_pause("cash_short", f"CASH SHORT: {why[0][:120]}", now)
        msg = f"CASH SHORT: {'; '.join(why)[:300]} -> {n} entry orders cancelled, {dropped} queued dropped, new risk paused"
        self.event("CASHSHORT", msg)
        self.alerts.append(f"{time.strftime('%H:%M')} {msg}")
        self.night.line(msg)
        rec = dict(t=stamp(), why=why, cash=raw, acct={k: (self.acct or {}).get(k) for k in ("totalAccountValue", "totalHoldingsValue")},
                   cancelled=n, dropped=dropped)
        with open(os.path.join(DATA, "cash_short.jsonl"), "a") as fh:
            fh.write(json.dumps(rec, default=str) + "\n")
        night = self.controls.d.setdefault("night", {})
        if first and not night.get("cash_short_recap"):
            night["cash_short_recap"] = stamp()
            self.controls.save()
            try:
                with open(os.path.join(os.path.dirname(DATA), "docs", "RECAP.md"), "a") as fh:
                    fh.write(f"\n- **{stamp()[:16]} PT, first CASH SHORT (afternoon D-B), engine-recorded:** {'; '.join(why)[:400]}. "
                             f"Cash read {raw:,.0f}. Evidence: `data/cash_short.jsonl`. (Claude Code to confirm SIG's behaviour.)\n")
            except OSError:
                pass

    RELEASE_SUBS = ("inventory", "mm", "sniper")            # never: pair sets, manual (AK/RI), ladder tranches, fresh fat-finger

    def bid_depth(self, ex, side, now):
        """Shares at the best SIG bid of `side` (order book read, cached 60 s)."""
        c = self.depth_cache.get(ex)
        if not c or now - c[0] > 60:
            if not api.spare_reads():                       # Oct 3 (b): no spare read -> size from the prices read: <= 500
                return float(RELEASE_FALLBACK_QTY)          # shares, still a limit at the bid
            bids, asks = api.orderbook(ex, depth=1)
            c = self.depth_cache[ex] = (now, bids, asks)
        _, bids, asks = c
        if side == "yes":
            return bids[0][1] if bids else 0.0
        return asks[0][1] if asks else 0.0

    def dump_demand(self, cash, used):
        """Day 4 §3: is the dump catcher cash-constrained now? (funded reserve < what its live levels need) and its measured
        return per SIG per day over the last 24 h (realized + edge vs fair at fill, per SIG of catches)."""
        funded = max(0.0, min(float(OPPORTUNITY_RESERVE), cash - CASH_RESERVE - used.get("committed", 0.0)))
        need = sum(w["px"] * w["qty"] for ws in self.dump_now.values() for w in ws)
        s = self.dump_stats(time.time() - 86400)
        ret = (s["realized"] + s["edge_fair"]) / max(s["cost"], 1.0) if s["fills"] else 0.0
        return dict(constrained=need > funded + 1e-6, need=round(need), funded=round(funded), return_per_day=ret, shortfall=max(0.0, need - funded))

    def release_plan(self, cash, now, used=None):
        """Day 4 §3 (Piero's rule), release on demand: a holding is sold into the SIG bid only if (1) the bid is >= its average
        entry cost (a realized profit) with the price still >= fair_side - 5%, or (2) the dump catcher is cash-constrained now,
        the holding returns less per day to settlement than the dump catcher made per day over 24 h, and the cost vs fair is
        <= 5% (then only up to the shortfall). Never while new risk is paused; not 07:00-08:00 PT. Lowest return per day
        first; a limit at the bid, sized to the top of book (or 500 without a spare read)."""
        st = self.controls.state("release")
        if st == "off" or self.fills_blind or self.controls.paused:
            return {}
        if RELEASE_QUIET_HOURS[0] <= time.localtime(now).tm_hour < RELEASE_QUIET_HOURS[1]:
            return {}
        if now - self.release_t < RELEASE_EVERY_S and self.release_now:
            return {ex: r for ex, r in self.release_now.items() if ex in self.book}
        self.release_t = now
        dd = self.dump_demand(cash, used or {})
        self.release_demand = dd
        pending = sum(o["qty"] * (1 - o["price"]) for o in self.open
                      if (self.ledger.orders.get(o["id"]) or {}).get("layer") == "release")
        budget = max(0.0, dd["shortfall"] - pending)         # branch (2) only up to the dump catcher's shortfall
        maxc, cands = self.release_max_cost(), []
        for ex, b in self.book.items():
            q = b["pos"]
            if not q or ex not in self.owner or self.owner[ex].kind != "market" or self.ex.is_frozen(ex):
                continue
            side = "yes" if q > 0 else "no"
            ctf = self.cost_to_free(ex, side)
            if ctf is None or ctf < 0 or ctf > maxc:        # a bid above fair is the recycler's job; > 5% never
                continue
            bid = bots.side_book(*self.px.get(ex, (None, None, None))[:2], side)[0]
            avg = float((b.get("row") or {}).get("avgCost") or 0)
            profit = bid is not None and avg > 0 and bid >= avg - 1e-9
            rpd = self.return_per_day(ctf)
            demand = dd["constrained"] and budget > 0 and rpd < dd["return_per_day"]
            if not (profit or demand):
                continue
            subs = self.ledger.subbooks.get(ex, {})
            avail = sum(abs(v) for k, v in subs.items() if k in self.RELEASE_SUBS and v * q > 0)
            if avail >= MIN_QTY:
                cands.append((rpd, ctf, ex, side, avail, "profit" if profit else "demand"))
        plan = {}
        for rpd, ctf, ex, side, avail, why in sorted(cands):      # lowest return per day first
            if len(plan) >= RELEASE_MARKETS:
                break
            bid = bots.side_book(*self.px.get(ex, (None, None, None))[:2], side)[0]
            depth = self.bid_depth(ex, side, now)
            qty = int(min(avail, depth, (budget / bid) if why == "demand" else avail))
            if qty < MIN_QTY:
                continue
            subs, left, split = self.ledger.subbooks.get(ex, {}), qty, {}
            for k in self.RELEASE_SUBS:
                v = subs.get(k, 0)
                if left > 0 and v * (1 if side == "yes" else -1) > 0:
                    take = int(min(abs(v), left))
                    if take >= MIN_QTY:
                        split[k] = take
                        left -= take
            if split:
                plan[ex] = dict(side=side, px=bid, cost=round(ctf, 4), subs=split, why=why)
                if why == "demand":
                    budget -= sum(split.values()) * bid
        if plan:
            self.event("RELEASE", "; ".join(f"{self.name(ex)} {r['side'].upper()} {sum(r['subs'].values()):,} @ {r['px']:.3f} "
                                             f"(cost {r['cost'] * 100:.1f}%, {r['why']})" for ex, r in plan.items()))
        return plan

    def return_per_day(self, ctf):
        """Afternoon §2.2: expected return to settlement per SIG held, per day: (fair - bid)/bid over the days left."""
        days = max(1.0, (time.mktime(time.strptime(SETTLE_DATE, "%Y-%m-%d")) - time.time()) / 86400)
        return ctf / days

    def released_today_view(self):
        """Cash the release layer freed today and its average cost vs fair."""
        today = time.strftime("%Y-%m-%d")
        sig, w = 0.0, 0.0
        for f in self.ledger.fills:
            if f["layer"] == "release" and f["t"][:10] == today and f["action"] == "buy":
                sale = 1 - f["price"]                       # a complement buy at p sells the held side at 1 - p
                held_fair = None if f.get("fair") is None else 1 - f["fair"]
                sig += sale * f["qty"]
                if held_fair is not None and sale > 0:
                    w += (held_fair - sale) / sale * sale * f["qty"]
        rel = [f for f in self.ledger.fills if f["layer"] == "release" and f["t"][:10] == today]
        return dict(sig=round(sig, 2), avg_cost=round(w / sig, 4) if sig else None, fills=len(rel),
                    realized=round(sum(f.get("realized") or 0 for f in rel), 2), vs_fair=round(-w, 2),
                    demand=getattr(self, "release_demand", None))

    def cover_why(self, ex, side, why):
        if self.coverage is not None and f"{ex}|{side}" in self.coverage["reasons"]:
            self.coverage["reasons"][f"{ex}|{side}"] = why  # the specific reason replaces "being placed"

    def reserve_ok(self, ex, w):
        """Day 2 §3.2: only these may spend the opportunity reserve (down to the floor): fat-finger levels, sniper takes on
        a new market (< 30 min old), and takes >= 8c through fair."""
        if w.get("layer") in ("fatfinger", "dump", "longshot"):
            return True
        if w.get("layer") != "take":
            return False
        f = self.side_fair(ex, w["side"])
        return time.time() - self.new_markets.get(ex, 0) < 1800 or (f is not None and f - w["px"] >= 0.08 - 1e-9)

    def apply_d1(self):
        """D1 / D1b (night plan §1, day 2 §1): release a cumulative AK_RELEASE share of the original AK NO position to the
        recycler (sub-book recycle:ak-d1, sold into the SIG bid while it is >= PM NO fair + 0.5c). The exit ladder is
        scaled to the share that stays manual; the 0.10 spike bid is kept."""
        night = self.controls.d.setdefault("night", {})
        ak = next((b for b in self.bots if b.kind == "position" and b.name == "AK-GOV-D"), None)
        if ak is None:
            night["d1_done"], night["ak_released"] = "no AK bot", AK_RELEASE
            return
        done = night.get("ak_released", 0.25 if night.get("d1_done") else 0.0)
        if done >= AK_RELEASE - 1e-9:
            return
        held = self.ledger.sub(ak.ex, "manual")
        q = self.ledger.split_sub(ak.ex, "manual", "recycle:ak-d1", int(round(abs(held) * (AK_RELEASE - done) / (1 - done))))
        refs = read_json(POSITION_REF_FILE, {})
        old = ak.ref
        ak.ref = int(round(old * (1 - AK_RELEASE) / (1 - done)))
        if "AK-GOV-D" in refs:
            refs["AK-GOV-D"]["ref"] = ak.ref
            write_atomic(POSITION_REF_FILE, json.dumps(refs, indent=1))
        night["d1_done"], night["ak_released"] = night.get("d1_done") or stamp(), AK_RELEASE
        self.night.line(f"AK release to {AK_RELEASE:.0%} (D1{'b' if done else ''}): {abs(q):,.0f} AK NO moved to sub-book recycle:ak-d1 "
                        f"(sold into the SIG bid while it is >= PM NO fair + 0.5c); the exit ladder now covers the remaining "
                        f"{1 - AK_RELEASE:.0%} (ref {old:,} -> {ak.ref:,}); 0.10 spike bid kept")

    def log_leaders(self, now):
        """Every 10 min (night plan §5.6): the top 20 and our own row -> data/leaders.csv (2 reads)."""
        if now - self.leaders_t < LEADERS_EVERY_S or not self.controls.on("leaders") or not api.spare_reads():
            return
        self.leaders_t = now
        try:
            d = api.leaderboard(50)                         # top 50 (day 2 §5.1), #100, and our own row: 3 reads / 10 min
            rows = [dict(r, ours=0) for r in d["leaderboard"]]
            total = d.get("total")
            if total and total >= 100:
                rows += [dict(r, ours=0) for r in api.leaderboard(1, offset=99)["leaderboard"][:1] if r.get("rank") == 100]
            if d.get("myRank"):
                me = api.leaderboard(1, offset=int(d["myRank"]) - 1)["leaderboard"]
                rows += [dict(r, ours=1) for r in me[:1]]
            if not api.DRY_RUN:
                write_atomic(os.path.join(DATA, "leaders_meta.json"), json.dumps(dict(total=total, t=stamp(), my_rank=d.get("myRank"))))
                try:                                        # dashboard v4: the daily board (SIG day) top 25 + our daily rank: 1 read
                    dd = api.leaderboard(25, period="1d")
                    write_atomic(os.path.join(DATA, "leaders_day.json"), json.dumps(dict(
                        t=stamp(), my_rank=dd.get("myRank"), total=dd.get("total"),
                        top=[dict(rank=r.get("rank"), name=r.get("username") or r.get("profileId"), pnl=r.get("pnl"), trades=r.get("tradesCount"))
                             for r in dd.get("leaderboard", [])[:25]])))
                except Exception as err:
                    self.event("ERR", f"daily leaderboard: {str(err)[:80]}")
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
        if now - self.scan_t < 60 or not (self.controls.on("sniper") or self.controls.on("fatfinger")) or not api.spare_reads(15):
            return
        self.scan_t = now
        if now - getattr(self, "remap_t", 0) >= 600 and self.unmapped_new:      # retry unmapped new markets every 10 min
            self.remap_t = now
            for ex in list(self.unmapped_new):
                try:
                    mp = pm.map_one(self.unmapped_markets[ex])
                except Exception:
                    mp = None
                if mp:
                    self.mp.append(mp)
                    self.unmapped_new.discard(ex)
                    self.night.line(f"NEW MARKET {self.name(ex)} now mapped to Polymarket {mp['pm_ids']}: full coverage")
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
                msg = (f"NEW MARKET UNMAPPED {m['title']} (ex {ex}): no confident Polymarket match; only 0.05 fat-finger bids, half size, "
                       f"both sides; mapping retried every 10 min")
                self.alerts.append(f"{time.strftime('%H:%M')} {msg}")
                self.event("NEW", msg)
                self.night.line(msg)
                bot = bots.MarketBot(ex, self.meta[ex])
                self.bots.append(bot)
                self.owner[ex] = bot
                self.unmapped_new.add(ex)
                self.unmapped_markets[ex] = m
                self.new_markets[ex] = now
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
    def run_pairs(self, now, budget, kappa=0.0, new_sets=True):
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
        if self.controls.paused or not new_sets:
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
            if shots >= PAIR_MAX_SHOTS or self.ex.admit() < 2 or not api.spare_reads(20):
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
        if night.get("hourly_h") != h or force:             # a restart within the hour refreshes the status block only
            night["hourly_h"] = h
            c.save()
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
            (lambda mv: f"**P&L today (SIG marks):** {mv['today']['pnl']:+,.0f} = realized {mv['today']['realized']:+,.0f} + "
                        f"unrealized {mv['today']['unrealized_change']:+,.0f} · **free cash** {cash - CASH_RESERVE:,.0f} above the floor"
             if (mv or {}).get("today") and mv["today"].get("pnl") is not None
             else f"**Realized P&L today:** {self.realized_today():+,.0f} · **free cash** {cash:,.0f}")(self.money_view()),
            f"**Kills / rollbacks:** {'; '.join(kills) or 'none'}",
            (lambda p: f"**PAUSED {p['minutes']:.0f} min** ({p['kind']}, since {p['since']}): {p['reason']}; blocked: every new entry "
                       f"(dump, fat-finger, longshot, MM, pairs, ladders); exits and sell-backs keep running"
             if p and p["minutes"] >= 10 else "**Pauses > 10 min:** none")(self.pause_view()),
            f"**For Piero to decide (engine):** {'; '.join(c.d.get('decisions') or []) or 'nothing from the engine'}"
            " · Claude Code's items are in the summary below"])

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
        for p in self.ex.pending():                         # queued / in flight: same rule (an exit is not an entry)
            if p["action"] == "buy" and p["layer"] in self.FLOOR_LAYERS and not jx.reduces(
                    p["side"], "buy", self.book.get(p["ex"], {}).get("pos", 0)):
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
            r["volume"] = r.get("volume", 0.0) + f["qty"] * f["price"]
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
            rows = list(csv.DictReader(fh))[-52 * 300:]
        if not rows:
            return {}
        last_t = rows[-1]["time"]
        latest = [r for r in rows if r["time"] == last_t and (r["ours"] == "1" or int(r["rank"] or 999) <= 20)]
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
        try:
            v3 = self.v3_view(stats)
        except Exception as err:                            # the dashboard must never stop trading
            self.event("ERR", f"dashboard v3: {err!r}")
            v3 = {}
        out = dict(
            t=stamp(), loop=self.n, live=not api.DRY_RUN, stopped=stopped,
            overview=dict(pause=self.pause_view(), account_fair=None if fair_now is None else round(fair_now), free_cash=None if cash is None else round(cash),
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
        th = v3.pop("leader_thresholds", None)
        out.update(v3)
        if isinstance(out.get("leaders"), dict):
            out["leaders"] = dict(out["leaders"], thresholds=th)
        return out

    # ------------------------------------------------------------------------------------------------ dashboard v3
    LABELS = {"position": ("Position exits (AK, RI)", "Resting exit ladders on the manual AK and RI positions"),
              "recycler": ("Recycler", "Sells older inventory into bids above the Polymarket price"),
              "release": ("Capital release", "Sells holdings where freeing the cash is cheap"),
              "mm": ("Market maker", "Quotes both sides around the Polymarket price"),
              "scalp": ("Scalper", "Joins the best bid 2c+ under fair on the busiest markets; sells a tick inside the ask"),
              "ladder": ("Deep ladders", "Deep bids that catch sweeps, then sell back"),
              "pairs": ("Pair sets", "Buys every side of a race when the set costs less than it pays"),
              "sniper": ("Sniper", "Buys big gaps below fair, and new markets"),
              "fatfinger": ("Fat-finger catcher", "Cheap bids that catch wrong-side panic orders"),
              "dump": ("Dump catcher", "Bids 7-20c under fair on favourites; sells the rebound for a round trip"),
              "longshot": ("Longshot catcher", "0.5-1c bids on sides Polymarket prices <= 5%; sells at 3x entry or more"),
              "leaders": ("Leaderboard logger", "Records the top accounts every 10 minutes")}

    @staticmethod
    def sub_layer(sub):
        """Which layer owns a sub-book (I3 names)."""
        if sub == "manual":
            return "position"
        for pre, layer in (("recycle:", "recycler"), ("tranche:", "ladder"), ("pairset:", "pairs"), ("fatfinger", "fatfinger"),
                           ("dump:", "dump"), ("longshot:", "longshot"), ("release", "release")):
            if sub.startswith(pre):
                return layer
        return {"mm": "mm", "sniper": "sniper", "inventory": "inventory"}.get(sub, "inventory")

    def sig_mark(self, ex, side):
        """SIG valuation of one share of `side` (the leaderboard's mark): the position's currentPrice, else the SIG mid."""
        row = (self.book.get(ex) or {}).get("row") or {}
        held = str(row.get("option") or "").lower()
        if row.get("currentPrice") is not None and held in ("yes", "no"):
            p = float(row["currentPrice"])
            return p if side == held else 1 - p
        return self.side_mid(ex, side)

    def pnl_rows(self):
        path = PNL_FILE
        if not os.path.exists(path):
            return []
        with open(path, newline="") as fh:
            rows = list(csv.DictReader(fh))
        out = []
        for r in rows:
            try:
                out.append(dict(t=time.mktime(time.strptime(r["time"], "%Y-%m-%d %H:%M:%S")), date=r["time"][:10],
                                sig=float(r["account_value"]), fair=float(r["account_pm"] or r["account_value"]),
                                unreal=float(r["unrealized"] or 0)))
            except (ValueError, KeyError):
                continue
        return out

    def layer_pnl(self, fills):
        """{layer: realized, pnl at SIG marks, pnl at fair} for a list of fills (pnl = realized + unrealized)."""
        out = {}
        for f in fills:
            l = self.strategy_of(f["bot"], f["layer"])
            if l == "old engine":
                continue
            r = out.setdefault(l, dict(realized=0.0, sig=0.0, fair=0.0, fills=0))
            sgn = 1 if f["action"] == "buy" else -1 if f["action"] == "sell" else 0
            m = self.sig_mark(f["ex"], f["side"])
            fv = self.side_fair(f["ex"], f["side"])
            r["realized"] += f.get("realized") or 0
            r["sig"] += sgn * ((m if m is not None else f["price"]) - f["price"]) * f["qty"]
            r["fair"] += sgn * ((fv if fv is not None else (m if m is not None else f["price"])) - f["price"]) * f["qty"]
            r["fills"] += 1
        return out

    def daily_rows(self, money):
        """Daily P&L since Oct 1 (SIG marks; fair alongside): data/daily_pnl.csv for closed days (rebuilt from pnl.csv when
        missing) plus today's partial row. realized = P&L - unrealized change (SIG's own split)."""
        pts = self.pnl_rows()
        days = sorted({p["date"] for p in pts})
        rows, prev = [], dict(sig=float(INITIAL_BALANCE), fair=float(INITIAL_BALANCE), unreal=0.0)
        today = time.strftime("%Y-%m-%d")
        for d in days:
            ps = [p for p in pts if p["date"] == d]
            close = ps[-1]
            if d == today and money:
                close = dict(sig=money["account_sig"], fair=money["fair"]["account"] or close["fair"], unreal=money["total"]["unrealized"])
            pnl = close["sig"] - prev["sig"]
            du = close["unreal"] - prev["unreal"]
            fills = [f for f in self.ledger.fills if f["t"][:10] == d and not f["before_engine"]]
            lp = self.layer_pnl(fills)
            rows.append(dict(date=d, account_sig=round(close["sig"], 2), account_fair=round(close["fair"], 2),
                             cash=round(money["cash"], 2) if d == today and money else None,
                             holdings_sig=round(money["holdings_sig"], 2) if d == today and money else None,
                             realized=round(pnl - du, 2), unrealized_change=round(du, 2), pnl_sig=round(pnl, 2),
                             pnl_fair=round(close["fair"] - prev["fair"], 2), partial=d == today,
                             layers={l: dict(realized=round(v["realized"], 2), unrealized=round(v["sig"] - v["realized"], 2))
                                     for l, v in lp.items()}))
            prev = close
        if not any(r["date"] == today for r in rows) and money:
            rows.append(dict(date=today, account_sig=money["account_sig"], account_fair=money["fair"]["account"], cash=money["cash"],
                             holdings_sig=money["holdings_sig"], realized=money["today"]["realized"],
                             unrealized_change=money["today"]["unrealized_change"], pnl_sig=money["today"]["pnl"], pnl_fair=None,
                             partial=True, layers={}))
        return rows

    def write_daily(self, rows):
        """data/daily_pnl.csv: one line per closed day (written once the day is over)."""
        path = os.path.join(DATA, "daily_pnl.csv")
        have = set()
        if os.path.exists(path):
            with open(path, newline="") as fh:
                have = {r["date"] for r in csv.DictReader(fh)}
        new = [r for r in rows if not r["partial"] and r["date"] not in have]
        if not new or api.DRY_RUN:
            return
        layers = ["position", "recycler", "release", "mm", "ladder", "pairs", "sniper", "fatfinger", "dump", "longshot"]
        head = ["date", "account_sig", "account_fair", "cash", "holdings_sig", "realized", "unrealized_change", "pnl_sig", "pnl_fair"]
        head += [f"{l}_{k}" for l in layers for k in ("realized", "unrealized")]
        with open(path, "a", newline="") as fh:
            w = csv.writer(fh)
            if not have:
                w.writerow(head)
            for r in new:
                w.writerow([r.get(h) for h in head[:9]] + [((r["layers"].get(l) or {}).get(k)) for l in layers for k in ("realized", "unrealized")])

    def money_view(self):
        a = self.acct or {}
        acct = a.get("totalAccountValue")
        if acct is None:
            return None
        hold = a.get("totalHoldingsValue") or 0.0
        cash = acct - hold
        rows = [b["row"] for b in self.book.values()]
        unreal = sum(float(r.get("unrealizedPnl") or 0) for r in rows)
        cost = sum(float(r.get("costBasis") or 0) for r in rows)
        mv = sum(float(r.get("marketValue") or 0) for r in rows)
        realized = cash + cost - INITIAL_BALANCE
        diff = (realized + unreal) - (acct - INITIAL_BALANCE)
        fair_now, _ = self.account_fair()
        pts = self.pnl_rows()
        today = time.strftime("%Y-%m-%d")
        before = [p for p in pts if p["date"] < today]
        start = before[-1] if before else (pts[0] if pts else None)
        lb = self.leader_meta()
        ours = lb.get("ours") or {}
        mine = ours.get("value")
        to = {k: (None if lb.get("thresholds", {}).get(k) is None or mine is None else round(lb["thresholds"][k] - mine, 2))
              for k in ("10", "100")}
        return dict(account_sig=round(acct, 2), start=INITIAL_BALANCE, vs_start=round(acct - INITIAL_BALANCE, 2),
                    rank=ours.get("rank"), n_traders=lb.get("total"), to_rank=to,
                    today=dict(pnl=None if not start else round(acct - start["sig"], 2),
                               unrealized_change=None if not start else round(unreal - start["unreal"], 2),
                               realized=None if not start else round((acct - start["sig"]) - (unreal - start["unreal"]), 2),
                               since=f"{today} 00:00:00"),
                    total=dict(realized=round(realized, 2), unrealized=round(unreal, 2), sum=round(realized + unreal, 2),
                               account_minus_start=round(acct - INITIAL_BALANCE, 2), diff=round(diff, 2), ok=abs(diff) <= 50,
                               positions_value=round(mv, 2)),
                    fair=dict(account=round(fair_now, 2), vs_sig=round(fair_now - acct, 2)),
                    velocity=self.velocity_view(acct, None if not start else acct - start["sig"], pts, today),
                    cash=round(cash, 2), holdings_sig=round(hold, 2))

    def velocity_view(self, acct, pnl_today, pts, today):
        """Afternoon §2.4: return on capital today = P&L at SIG today / average account value today; turnover today =
        volume traded today (our fills, SIG) / the same average capital."""
        caps = [p["sig"] for p in pts if p["date"] == today] + [acct]
        avg = sum(caps) / len(caps)
        vol = sum(f["qty"] * f["price"] for f in self.ledger.fills if f["t"][:10] == today and not f["before_engine"])
        return dict(avg_capital=round(avg, 2), volume_today=round(vol, 2),
                    roc_today=None if pnl_today is None else round(pnl_today / avg, 5), turnover_today=round(vol / avg, 3))

    def leader_meta(self):
        """Latest leaderboard sample: our row, #10/#25/#50/#100 P&L, number of traders (data/leaders.csv + meta)."""
        meta = read_json(os.path.join(DATA, "leaders_meta.json"), {})
        path = os.path.join(DATA, "leaders.csv")
        if not os.path.exists(path):
            return meta
        with open(path, newline="") as fh:
            rows = list(csv.DictReader(fh))[-80:]
        if not rows:
            return meta
        last = [r for r in rows if r["time"] == rows[-1]["time"]]
        by_rank = {int(r["rank"]): float(r["value"]) for r in last if r["rank"] and r["ours"] != "1"}
        ours = next((r for r in last if r["ours"] == "1"), None)
        th = {k: by_rank.get(int(k)) for k in ("10", "25", "50", "100")}
        th.update({k: v for k, v in (meta.get("thresholds") or {}).items() if th.get(k) is None})
        return dict(total=meta.get("total"), thresholds=th,
                    ours=None if not ours else dict(rank=int(ours["rank"]), value=float(ours["value"])))

    def capital_view(self, money):
        if not money:
            return None
        buckets = {}
        for ex, b in self.book.items():
            row, q = b["row"], b["pos"]
            if not q:
                continue
            side = "yes" if q > 0 else "no"
            mark = self.sig_mark(ex, side) or 0.0
            fv = self.side_fair(ex, side)
            subs = self.ledger.subbooks.get(ex, {}) or {"inventory": q}
            tot = sum(abs(v) for v in subs.values() if v * q > 0) or abs(q)
            for sub, v in subs.items():
                if v * q <= 0:
                    continue
                share = abs(q) * abs(v) / tot
                key = "manual" if self.sub_layer(sub) == "position" else self.sub_layer(sub)
                if key == "recycler":
                    key = "inventory"
                bk = buckets.setdefault(key, [0.0, 0.0])
                bk[0] += share * mark
                bk[1] += share * (fv if fv is not None else mark)
        labels = {"manual": "AK / RI manual positions", "mm": "Market-maker inventory", "ladder": "Ladder tranches", "pairs": "Pair sets",
                  "sniper": "Sniper / takes", "fatfinger": "Fat-finger catches", "dump": "Dump catches (rebound pending)", "longshot": "Longshot catches (exits resting)", "release": "Being released",
                  "inventory": "Older inventory (recycler)"}
        order = ["manual", "mm", "ladder", "pairs", "sniper", "fatfinger", "dump", "longshot", "release", "inventory"]
        out = [dict(key=k, label=labels[k], sig=round(buckets.get(k, [0, 0])[0], 2), fair=round(buckets.get(k, [0, 0])[1], 2)) for k in order]
        out.append(dict(key="cash", label="Cash", sig=money["cash"], fair=money["cash"]))
        committed = self.committed()
        reserve_used = max(0.0, min(float(OPPORTUNITY_RESERVE), CASH_RESERVE + OPPORTUNITY_RESERVE - (money["cash"] - committed)))
        tgt, tdate = self.release_target()
        rel = self.released_today_view()
        idle = self.idle_view()
        return dict(cash=money["cash"], free_cash=round(money["cash"] - CASH_RESERVE, 2), idle=idle, holdings_sig=money["holdings_sig"], holdings_fair=round(money["fair"]["account"] - money["cash"], 2),
                    floor=CASH_RESERVE, reserve=OPPORTUNITY_RESERVE, reserve_used=round(reserve_used, 2), committed=round(committed, 2),
                    free_to_trade=round(money["cash"] - CASH_RESERVE - OPPORTUNITY_RESERVE - committed, 2),
                    target=tgt, target_date=tdate, released_today=rel, buckets=out)

    def idle_view(self):
        """Afternoon §2.4: capital idle = holdings older than 3 days with no exit order resting (SIG marks); plus all
        holdings with no exit order, whatever their age. Age: since the position last went from flat to held (ledger)."""
        since, net = {}, collections.defaultdict(float)
        for f in self.ledger.fills:                         # oldest first
            q = f["qty"] * (1 if (f["side"] == "yes") == (f["action"] == "buy") else -1)
            was = net[f["ex"]]
            net[f["ex"]] = was + q
            if abs(was) < 1 <= abs(net[f["ex"]]) or was * net[f["ex"]] < 0:
                since[f["ex"]] = f["t"]
        exits = set()
        for o in self.open:
            p = self.book.get(o["ex"], {}).get("pos", 0)
            if p and jx.reduces(o["side"], o["action"], p):
                exits.add(o["ex"])
        cutoff = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(time.time() - 3 * 86400))
        no_exit, idle3 = [0, 0.0], [0, 0.0]
        for ex, b in self.book.items():
            if abs(b["pos"]) < 1 or ex in exits:
                continue
            v = float((b.get("row") or {}).get("marketValue") or 0)
            no_exit[0] += 1
            no_exit[1] += v
            if since.get(ex, "2026-09-01 00:00:00") <= cutoff:
                idle3[0] += 1
                idle3[1] += v
        return dict(idle_3d=dict(n=idle3[0], sig=round(idle3[1], 2)), no_exit=dict(n=no_exit[0], sig=round(no_exit[1], 2)))

    def release_target(self):
        """Day 4 §3: release is on demand (no free-cash target); the dashboard's target is the opportunity reserve."""
        return OPPORTUNITY_RESERVE, "on demand"

    def cost_to_free(self, ex, side):
        bid = bots.side_book(*self.px.get(ex, (None, None, None))[:2], side)[0]
        fv = self.side_fair(ex, side)
        if bid is None or fv is None or bid <= 0:
            return None
        return (fv - bid) / bid

    def holdings_view(self):
        out = []
        for ex, b in self.book.items():
            row, q = b["row"], b["pos"]
            if not q:
                continue
            side = "yes" if q > 0 else "no"
            shares = abs(q)
            mark = self.sig_mark(ex, side)
            fv = self.side_fair(ex, side)
            bid = bots.side_book(*self.px.get(ex, (None, None, None))[:2], side)[0]
            cost = float(row.get("costBasis") or 0) or shares * float(row.get("avgCost") or 0)
            subs = self.ledger.subbooks.get(ex, {}) or {}
            owner = max(subs.items(), key=lambda kv: abs(kv[1]) if kv[1] * q > 0 else -1)[0] if subs else "inventory"
            ctf = self.cost_to_free(ex, side)
            out.append(dict(ex=ex, market=self.name(ex), side=side, shares=shares, avg_cost=round(cost / shares, 4) if shares else None,
                            sig_mark=None if mark is None else round(mark, 4), sig_bid=bid, fair=None if fv is None else round(fv, 4),
                            value_sig=round(float(row.get("marketValue") or 0), 2),
                            value_fair=None if fv is None else round(shares * fv, 2),
                            unreal_sig=round(float(row.get("unrealizedPnl") or 0), 2),
                            unreal_fair=None if fv is None else round(shares * fv - cost, 2),
                            cost_to_free=None if ctf is None else round(ctf, 4), layer=self.sub_layer(owner),
                            release_candidate=bool(self.release_ok(ex, side, owner, ctf))))
        return sorted(out, key=lambda r: -r["value_sig"])

    def release_ok(self, ex, side, owner, ctf):
        """Would the release layer sell this holding now? (§3.3; the layer itself decides; this is the dashboard badge)."""
        return ctf is not None and 0 <= ctf <= self.release_max_cost() and self.sub_layer(owner) not in ("pairs", "position", "fatfinger", "dump", "longshot")

    def release_max_cost(self):
        d = time.strftime("%m-%d")
        return RELEASE_MAX_COST                             # Oct 3: 10% flat (was 5%, 6% from Oct 30)

    def fill_sentence(self, f):
        l = self.strategy_of(f["bot"], f["layer"])
        label = self.LABELS.get(l, (l,))[0] if l in self.LABELS else "Old engine" if l == "old engine" else l
        side, px, fair = f["side"], f["price"], f.get("fair")
        verb = "bought" if f["action"] == "buy" else "sold" if f["action"] == "sell" else "traded"
        if f["action"] == "buy" and f["layer"] in self.REDUCING:      # a complement buy closes what we held: say "sold"
            side, px, fair, verb = ("no" if side == "yes" else "yes"), 1 - px, None if fair is None else 1 - fair, "sold"
        edge = None if fair is None else ((fair - px) if verb == "bought" else (px - fair)) * 100
        tail = "" if fair is None else f" (fair {fair:.3f}, {edge:+.1f}c)"
        return f"{label} {verb} {f['qty']:,.0f} {side.upper()} on {self.name(f['ex'])} at {px:.3f}{tail}"

    def strategies_view(self, stats):
        today = time.strftime("%Y-%m-%d")
        tf = [f for f in self.ledger.fills if f["t"][:10] == today and not f["before_engine"]]
        night = self.controls.d.get("night", {})
        start = night.get("start_t") or "2026-10-01 00:00:00"
        allf = [f for f in self.ledger.fills if f["t"] >= start and not f["before_engine"]]
        pt, pa = self.layer_pnl(tf), self.layer_pnl(allf)
        cap = {b["key"]: b["sig"] for b in (getattr(self, "last_capital", None) or {}).get("buckets", [])}
        vol = collections.Counter()
        for f in tf:
            vol[self.strategy_of(f["bot"], f["layer"])] += f["qty"] * f["price"]
        last = {}
        for f in self.ledger.fills[-600:]:
            last[self.strategy_of(f["bot"], f["layer"])] = f"{f['t'][11:16]} " + self.fill_sentence(f).split(" ", 1)[1] \
                if " " in self.fill_sentence(f) else ""
        out = []
        for l in ["position", "recycler", "release", "mm", "ladder", "pairs", "sniper", "fatfinger", "dump", "longshot", "leaders"]:
            label, what = self.LABELS[l]
            v = self.controls.layer(l)
            t_, a_ = pt.get(l, {}), pa.get(l, {})
            out.append(dict(key=l, label=label, what=what, state=self.controls.state(l), killed=v.get("killed"),
                            today=dict(realized=round(t_.get("realized", 0), 2), unrealized_sig=round(t_.get("sig", 0) - t_.get("realized", 0), 2),
                                       unrealized_fair=round(t_.get("fair", 0) - t_.get("realized", 0), 2)),
                            total=dict(realized=round(a_.get("realized", 0), 2), unrealized_sig=round(a_.get("sig", 0) - a_.get("realized", 0), 2),
                                       unrealized_fair=round(a_.get("fair", 0) - a_.get("realized", 0), 2)),
                            fills_today=t_.get("fills", 0), capital_used=round(cap.get("manual" if l == "position" else l, 0.0), 2),
                            volume_today=round(vol.get(l, 0.0), 2),
                            turnover_today=round(vol.get(l, 0.0) / cap.get("manual" if l == "position" else l, 0.0), 2)
                            if cap.get("manual" if l == "position" else l, 0.0) >= 100 else None,
                            resting=stats.get(l, {}).get("resting", 0), last_action=last.get(l)))
        return out

    def activity_view(self):
        out = []
        for i, f in enumerate(self.ledger.fills[-300:]):
            if f["before_engine"]:
                continue
            out.append(dict(id=len(self.ledger.fills) - 300 + i if len(self.ledger.fills) > 300 else i, t=f["t"],
                            layer=self.strategy_of(f["bot"], f["layer"]), text=self.fill_sentence(f)))
        return out[::-1]

    def series_view(self):
        pts = self.pnl_rows()[-1500:]
        rank_t, rank = [], []
        path = os.path.join(DATA, "leaders.csv")
        if os.path.exists(path):
            with open(path, newline="") as fh:
                for r in csv.DictReader(fh):
                    if r["ours"] == "1" and r["rank"]:
                        rank_t.append(time.mktime(time.strptime(r["time"], "%Y-%m-%d %H:%M:%S")))
                        rank.append(int(r["rank"]))
        return dict(t=[p["t"] for p in pts], account_sig=[p["sig"] for p in pts], account_fair=[p["fair"] for p in pts],
                    rank_t=rank_t[-300:], rank=rank[-300:])

    def v3_view(self, stats):
        """Dashboard v3 keys (docs/dashboard_v3_contract.md)."""
        money = self.money_view()
        capital = self.capital_view(money)
        self.last_capital = capital
        daily = self.daily_rows(money)
        try:
            self.write_daily(daily)
        except OSError as e:
            self.event("ERR", f"daily_pnl.csv: {e!r}")
        lm = self.leader_meta()
        cov = None
        if self.coverage is not None:
            c_ = self.coverage
            now_ = time.time()                              # C1: how long each side has been uncovered
            self.uncov_since = {k: self.uncov_since.get(k, now_) for k in c_["reasons"]}
            un = [dict(ex=k.split("|")[0], market=self.name(k.split("|")[0]), side=k.split("|")[1], reason=v,
                       minutes=round((now_ - self.uncov_since[k]) / 60, 1))
                  for k, v in sorted(c_["reasons"].items(), key=lambda kv: self.uncov_since[kv[0]])]
            cov = dict(pct=round(100 * c_["covered"] / c_["eligible"], 1) if c_["eligible"] else None, eligible=c_["eligible"],
                       covered=c_["covered"], over_5_min=sum(1 for u in un if u["minutes"] > 5), uncovered=un[:200])
            for ex, t0 in self.new_markets.items():
                if ex not in self.cov_logged and not any(u["ex"] == ex for u in un):
                    self.cov_logged.add(ex)
                    self.night.line(f"NEW MARKET {self.name(ex)} covered {time.time() - t0:.0f} s after it was seen")
                    self.intel_write("new_markets.jsonl", dict(t=time.time(), ex=ex, market=self.name(ex), seen=t0,
                                                                covered_s=round(time.time() - t0, 1)))
        strategies = self.strategies_view(stats)
        if time.time() - self.intel_s.get("layers", 0) >= 60:    # today's layer P&L for the files-only daily report
            self.intel_s["layers"] = time.time()
            try:
                os.makedirs(os.path.join(DATA, "intel"), exist_ok=True)
                with open(os.path.join(DATA, "intel", "layers_today.json"), "w") as fh:
                    json.dump(dict(date=time.strftime("%Y-%m-%d"), t=stamp(), layers={
                        s["key"]: round((s["today"].get("realized") or 0) + (s["today"].get("unrealized_sig") or 0), 2)
                        for s in strategies if s.get("today")}), fh)
            except OSError:
                pass
        try:
            v4 = dash4.v4_view(self, time.time(), capital, self.holdings_view())
        except Exception as err:
            self.event("ERR", f"dashboard v4 payload: {err!r}")
            v4 = {}
        return dict(sigday=v4.get("sigday"), status=v4.get("status"), daily4=v4.get("daily"), feed=v4.get("feed"), strategies4=v4.get("strategies"),
                    capital4=v4.get("capital"), holdings4=v4.get("holdings"), leaders4=v4.get("leaders"), admin4=v4.get("admin"),
                    money=money, daily=daily, series=self.series_view(), capital=capital, strategies=strategies,
                    coverage=cov,
                    holdings=self.holdings_view(), activity=self.activity_view(), leader_thresholds=lm.get("thresholds"))

    def layer_note(self, layer):
        return {"position": "AK + RI exit ladders (always on)", "recycler": "sells inventory into bids >= fair + 0.5c",
                "mm": "two-sided quotes at fair +/- e(p)", "ladder": "deep bids for sweeps, rebounds after fills",
                "pairs": "atomic NO/YES sets, early unwind", "sniper": "new markets: take asks far below fair",
                "leaders": "leaderboard logger (read only)"}.get(layer, "")

    def kill_status(self, layer, st):
        """The kill rules of a layer (night_plan §4.4) with their current values."""
        if layer in ("position", "leaders"):
            return []
        if layer == "release":                              # it sells below fair by design: judge its cost, not its P&L
            r = self.released_today_view()
            mx = self.release_max_cost() + 0.01
            return [dict(rule=f"average cost <= {mx * 100:.0f}%", value="-" if r["avg_cost"] is None else f"{r['avg_cost'] * 100:.1f}%",
                         hit=r["avg_cost"] is not None and r["avg_cost"] > mx)]
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
        if action == "clear_decisions":
            with c.lock:
                c.d["decisions"] = []
                c.save()
            return "decisions cleared"
        if action == "pause":
            with c.lock:
                c.d["auto_pause"] = dict(history=(c.d.get("auto_pause") or {}).get("history", {}))
                c.d["paused_t"] = time.time()
                c.save()
            c.set_paused(True, "dashboard", by="dashboard")
            return "new risk paused (exits, rebounds, unwinds and repairs keep running)"
        if action == "resume":
            with c.lock:
                c.d["auto_pause"] = dict(history=(c.d.get("auto_pause") or {}).get("history", {}))
                c.save()
            self.eng.cash_short = None
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
        if action == "backfill":                           # evening §1.3: once, inside the engine loop (it owns the ledger)
            since = str(args.get("since") or "")
            if not re.match(r"^\d{4}-\d\d-\d\d \d\d:\d\d:\d\d$", since):
                raise ValueError("since: 'YYYY-MM-DD HH:MM:SS' (PT)")
            orders = [str(o) for o in (args.get("orders") or [])] or None
            self.eng.pending_backfill = dict(since=since, dry_run=bool(args.get("dry_run", True)), orders=orders)
            return f"backfill from {since} queued for the next loop ({'dry run' if args.get('dry_run', True) else 'BOOKING'}" \
                   f"{', orders ' + ','.join(orders) if orders else ''})"
        if action == "subbook_fix":                        # day 3 §1: an audited correction of one market's sub-books
            ex = str(args.get("ex"))
            moves = {str(k): float(v) for k, v in (args.get("moves") or {}).items()}
            cancel = [str(o) for o in (args.get("cancel") or [])]
            if ex not in self.eng.owner or not moves:
                raise ValueError("subbook_fix needs a known market (ex) and moves {sub: delta}")
            if abs(sum(moves.values())) > 1e-6:
                raise ValueError(f"moves must add up to 0 (the position doesn't change): they add up to {sum(moves.values()):+,.2f}")
            for oid in cancel:
                rec = self.eng.ledger.orders.get(oid)
                if not rec or rec.get("ex") != ex or rec.get("bot") in self.eng.position_names:
                    raise ValueError(f"order {oid}: only engine market orders on ex {ex} can be cancelled here")
            self.eng.pending_fixes.append(dict(ex=ex, moves=moves, cancel=cancel, reason=str(args.get("reason") or "")[:160]))
            return f"subbook_fix for ex {ex} queued for the next loop: {moves}, cancel {cancel}"
        if action == "truth_mode":                         # evening §1.2: shadow (compute + log) or book
            mode = args.get("mode")
            if mode not in ("shadow", "book"):
                raise ValueError("mode: shadow | book")
            with c.lock:
                c.d["truth_mode"] = mode
                c.save()
            self.eng.event("CONTROL", f"order-truth fills: {mode} (dashboard)")
            return f"order-truth fills: {mode}"
        if action == "gate_free":                          # day 4: layers that may promote although the hold is on
            layers = [str(l) for l in (args.get("layers") or [])]
            if not set(layers) <= set(self.STRATEGY):
                raise ValueError(f"layers must be among {self.STRATEGY}")
            with c.lock:
                c.d["gate_free"] = layers
                c.save()
            self.eng.event("CONTROL", f"gates free to promote: {layers or 'none'} (dashboard)")
            return f"gates free to promote: {layers or 'none'}"
        if action == "hold_gates":                         # evening §1.5: no pilot promotes while set
            on = bool(args.get("on", True))
            with c.lock:
                c.d["hold_gates"] = on
                c.save()
            self.eng.event("CONTROL", f"pilot gates {'held' if on else 'released'} (dashboard)")
            return f"pilot gates {'held' if on else 'released'}"
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
                eng.pos_fail = eng.pos_fail + 1 if "position" in repr(e).lower() else eng.pos_fail
                if eng.pos_fail >= 2:                       # Piero's unattended rule: the positions read failing -> stop and pull
                    eng.stop_and_pull(f"positions read failed {eng.pos_fail} loops in a row ({repr(e)[:80]})")
            else:
                eng.pos_fail = 0
            if loops is None or i < loops:                  # a realtime event replans early, within the read budget:
                eng.wake.wait(timeout=max(1.0, min(LOOP_S, FILL_POLL_S) - (time.time() - t)))   # fills read <= 10 s apart (§6)
                eng.wake.clear()
                reads = api.health(60)["reads_last_min"]    # ~8 SIG reads a loop; SIG allows 100 a minute
                with eng.rt_lock:
                    ours = any(k != "pm_move" for _, k, _ in eng.rt_pending)
                gap = loop_gap(reads, ours, time.time())
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
