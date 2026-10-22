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
import hashlib
import itertools
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
import n_election as el
import e_bots as bots
import g_dashboard as dashboard
import i_realtime as rt
import q_dash5 as dash5
import j_executor as jx
import l_intel as intel
import m_dash4 as dash4
from a_config import (SCALP_MARKETS, SCALP_PILOT_MARKETS, SCALP_BUY_EDGE, SCALP_IMPROVE_SPREAD, SCALP_CLIP_MIN, SCALP_CLIP_MAX, SCALP_MAX_BUYS,
                      SCALP_SELL_MIN, SCALP_STEP1_S, SCALP_STEP1_GAP, SCALP_HAND_S, SCALP_INV_MARKET, SCALP_SLICE_MAX, SCALP_YIELD_RESERVE,
                      SCALP_WRITES_PER_MIN, SCALP_TTL_S, SCALP_PILOT_S, SCALP_GATE_TRADES, SCALP_GATE_PACE,
                      IDLE_USE_LOW, IDLE_USE_HIGH, IDLE_HOLD_S, IDLE_SLICE_STEP, IDLE_ROC_MIN, TICK, MB_MIN_PX, MB_MAX_PX,
                      RATCHET_MIN_FILLS_DAY, RATCHET_MIN_FILLS_NIGHT, DAY_WINDOW, DUMP_CONC_CAPS, DUMP_V3_OFFSETS_DAY, DUMP_V3_WEIGHTS_DAY,
                      DUMP_RECYCLE_FAST_S, CONSTRAINED_FREE, CONSTRAINED_FILLS_30M, RELEASE_FUND_PER_H, RELEASE_MIN_AGE_S, SCALP_DAY_OFF,
                      DUMP_REFRESH_S, DUMP_REFRESH_JITTER_S, LEADERS_DAY_EVERY_S, JUMP_MIN, TOUCH_MIN_FAIR, TOUCH_GAP, TOUCH_CLIP, TOUCH_RACE_CAP, TOUCH_TTL_S, TOUCH_SELL_MIN,
                      TOUCH_NEWS_FAIR, TOUCH_NEWS_SIG, TOUCH_NEWS_HOLD_S, TOUCH_YIELD_RESERVE, TOUCH_PILOT_S, TOUCH_GATE_FILLS, TOUCH_RATCHET_MAX,
                      RULE0_MARGIN, DAY1_CUTOFF, CASH_T_START, CASH_T_STEP, CASH_T_MIN, CASH_T_MAX, CASH_T_CONSTRAINED_MIN, CASH_T_PEAK_UP,
                      CASH_T_PEAK_DOWN, CASH_T_IDLE_H, RELEASE_TIER0, RELEASE_TIER1, RELEASE_TIER2, RELEASE_TIER2_RETURN,
                      RI_RACES, RI_ENGINE_CAP, DUMP_L0_MIN_FAIR, DUMP_L0_GAP, DUMP_L0_CLIP, DUMP_L0_REFILLS, DUMP_L0_WINDOW_S, UNCOVERED_LOG_S,
                      ENTRY_CASH_MULT, ENTRY_NEAR_TOUCH, ENTRY_NEAR_WEIGHT, ENTRY_MIN_FREE, DUMP_NEAR_TTL_S, DUMP_NEAR_GAP, DUMP_L0_BUDGET, ENTRY_WEIGHTS, REBUILD_EXC_FREE,
                     DUMP_REACH, BUDGET_K_MAX, BUDGET_K_MIN, BUDGET_K_STEP, BUDGET_K_DOWN_S, BUDGET_K_UP_S, BUDGET_K_PEAK_DOWN, BUDGET_K_PEAK_UP,
                     DUMP_WANT_BAND_LO, DUMP_WANT_BAND_HI, DUMP_GAP_LOG_S, DUMP_LADDER_FIRST, DUMP_LADDER_STEP_S, DUMP_LADDER_TICK,
                     BUDGET_K_TABLE, BUDGET_K_TIER_HYST, RI_SENATE_RACE, RI_SENATE_ADD, RI_SENATE_MIN_EDGE,
                     RULES_V2, HOLD_IDLE_GAP, BUFFER_ADD, BUFFER_WINDOW_S, BUFFER_P, BUFFER_DEFAULT, BUFFER_MIN, BUFFER_MAX, L3_BUDGET_SHARE, RI_TURNOVER, RI_MIN_EDGE,
                     TM_E_MIN, TM_IMPROVE_SPREAD, TM_CLIP, TM_CLIP_PILOT, TM_DEPTH_SHARE, TM_MIN_QTY, TM_INV_MARKET, TM_INV_MARKET_PILOT, TM_CAP_COST, TM_CAP_COST_PILOT,
                     TM_SIDES_PILOT, TM_TTL_S, TM_REPEG_TICKS, TM_EXIT_FIRST, TM_EXIT_STEP_S, TM_EXIT_LATE, TM_WRITES_PER_MIN, TM_NEWS_C, TM_NEWS_WINDOW_S, TM_REQUOTE_S,
                     TM_GLIDE_START, TM_DEPTH_CACHE_S, TM_GATE_MIN_FILLS, TM_E_MIN_LOW, TM_CAP_LOG_STEP, TM_RT_HI, TM_RT_LO, TM_RT_GRACE_S, TM_CLIP_HI, TM_INV_HI, TM_INV_LO,
                     ROTATION_EDGE_LEFT, ROTATION_FUND_EDGE, ROTATION_MARKETS, ROTATION_RET_LEFT, ROTATION_STOP_ABOVE,
                     SM_CLIP, SM_CLIP_PILOT, SM_DEPTH_SHARE, SM_MIN_QTY, SM_CAP_SH, SM_CAP_SH_PILOT, SM_CAP_COST, SM_CAP_COST_PILOT, SM_MARKETS, SM_MARKETS_PILOT,
                     SM_FAIR_LO, SM_FAIR_HI, SM_TTL_S, SM_REPEG_TICKS, SM_WRITES_PER_MIN, SM_NEWS_C, SM_NEWS_WINDOW_S, SM_REQUOTE_S, SM_SWEEP_C, SM_SWEEP_HOLD_S,
                     SM_GLIDE_START, SM_DEPTH_CACHE_S, SM_GATE_RT, SM_GATE_PER_RT, SM_GATE_MO30, SWING_BAND, RESERVOIRS, ENTRY_RESUME_FREE, RING_WINDOWS, FEED_MODE_DEFAULT, FEED_BOOK_MAX_AGE_S, FEED_REPORT_S, FEED_SWEEP_TICKS, FEED_SWEEP_SHARES, FEED_SWEEP_WINDOW_S, FEED_SPOT_EVERY,
                     SLEEVE_CAP, SLEEVE_MIN_FREE, SLEEVE_CLIP, SLEEVE_GAP_S, SLEEVE_TTL_S, SLEEVE_MIN_FAIR, SLEEVE_MIN_EDGE, SLEEVE_NEWS_C, SLEEVE_NEWS_WINDOW_S,
                     SLEEVE_NEWS_HOLD_S, SLEEVE_TARGETS, SWING, SWING_TTL_S, WALL_HOLD_MIN_FREE, WALL_RECYCLE_EXIT_C,
                     REBUILD_MAX_GAP, DUMP_BIG_SWEEP_SH, DUMP_BIG_CLIP, DUMP_V3_WEIGHTS_DAY_BIG,
                     FM_TARGETS, FM_CLIP, FM_CAP_SH, FM_CAP_COST, FM_TTL_S, FM_WRITES_PER_MIN, FM_SKEW_PER_1K, FM_NEWS_C, FM_NEWS_WINDOW_S, FM_REQUOTE_S,
                     FM_AGE_LADDER_S, FM_OFF_REALIZED, FM_OFF_MO30, FM_OFF_MIN_FILLS, FM_GLIDE_START, FM_POLL_PULL_S,
                     FAST_PATH_ON, FAST_REARMS_PER_10M, FAST_WRITES_RESERVE, FAST_SLOW_S, FAST_PM_MOVE_C,
                     TM_BUFFER_RESUME, TM_PAUSE_FREE, D5_EXCLUDE_LAYERS, TM_ENTRY_STOPS, SM_REGIME_WIN_S, SM_REGIME_EVERY_S, SM_REGIME, SM_QUOTE_REGIMES, SM_LOSS_MARKET_DAY, SM_LOSS_24H,
                     SM_TUNE_AT, SM_TUNE_BOUNDS, SM_TUNE_MIN_GAIN, SM_TUNE_MIN_FILLS, RETIRED_LAYERS, DUMP_DEEP_OFF, DUMP_L0_CAP_RESTING, DUMP_THIN_SIDES,
                     CORE_CAP_COST, CORE_SWAP_C, CORE_NEAR_FAIR_C, TRADING_HOLD_C, CORE_EARLY_CLOSE_PT, CORE_EARLY_BONUS, CORE_EVERY_S, CORE_SKIP_SUBS, PAUSE_KINDS,
                     PM_STALE_S, ENGINE_ERRORS_10M, API_429_STORM_10M, I2_RESUME_S, CASH_FLOOR_RESUME, BOOK_SCAN_EVERY_S, RECYCLE_DEPTH_HOUR, SCOREBOARD_EVERY_S, SCOREBOARD_TARGET_DAY,
                     TRADING_CASH_TARGET, TRADING_CASH_HYST, CORE_LOCK_RET, TRADING_INV_CAP, ROTATION_MARKETS_C89,
                     EDGE_AB_CAP, EDGE_PROMOTE_H, EDGE_ROLLBACK_H, H1_TTL_S, H1_BEHIND_TICKS, H1_BEHIND_S, H1_GATE, PAPER_HALF_AT_PRICE, PAPER_MAX_AGE_S, H1_SM_UNIVERSE, H3, H4, CASH_PLAN_1735, H1_JUDGE_WINDOW, DEDUP_SINCE, HOLD_RET, NOEDGE, GTC_TTL_S, REPEG_AWAY_TICKS, REPEG_AWAY_S, H1_TEST, ROTATION, ROT_TTL_S, MANUAL_RESERVE, FMM, DASH_EVERY_S, ALLOC, FUND, CARRY, NIGHT_SELL, SALE_REPEG_S, GAPMM, SELL_HOLD, SELL_KEEP_LAYERS, CARRY_BIDSELL, SELL_EV_MAX, FMM_CF, ENGINE_ERR_EXTERNAL, ENGINE_ERR_RESUME_S, ELECTION, ELECTION_RESERVE, WIND_DOWN, FMM_ROOM, CARRY_FIRST,
                      REBUILD_FREE, REBUILD_GAP1, REBUILD_GAP2, REBUILD_MARGIN, REBUILD_MARGIN_GAP,
                      WALL_TTL_S, WALL_HOLD_S, WALL_NEWS_C, WALL_NEWS_WINDOW_S, WALL_MIN_FREE)
TM_BUFFER_GATE = True             # C87: module switch for the TM BUFFER pause (the test rigs turn it off; C87Tests turn it on)
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
                      FAT_REFRESH_MAX_H, FILL_POLL_S, SETTLE_DATE, ROC_HOLD_MIN, RELEASE_QUIET_HOURS, LIQUIDATE, PNL_READ_S, ORDERS_READ_S, PRICE_ROTATE, LEADERS_EVERY_S, TAPE_MIN_PER_MIN, FILLS_READ_S, POSITIONS_READ_S, SAMPLER_EVERY_S, SAMPLER_PER_LOOP, SAMPLER_DEPTH, LIFECYCLE_LAYERS, LIFECYCLE_MARKS_S, SPARE_KEEP,
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
STARTUP_HOLD_S = 120          # optional reads (sampler, depth, fat-finger queues) wait 2 min after a start: every restart's burst gave a 429 (Oct 8)
LAYERS = ["position", "recycler", "release", "mm", "scalp", "tm", "sm", "rot", "fmm", "carry", "touch", "wall", "sleeve", "swing", "fm", "ladder", "pairs", "sniper", "fatfinger", "dump", "longshot", "leaders"]
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
    LIVE_DATA = os.path.join(os.path.dirname(os.path.abspath(__file__)), "data")

    def __init__(self, only=None):
        # day 6 §5.2: only `f_engine.py --live` (deploy.sh, SIG_DATA set) may touch the repo's data/; a test or a script
        # that builds an Engine without its own SIG_DATA stops here instead of writing fake rows into the live files
        if os.path.realpath(DATA) == os.path.realpath(self.LIVE_DATA) and "--live" not in sys.argv:
            raise RuntimeError(f"refusing to run an engine on the live data dir {DATA}: set SIG_DATA to a scratch dir (tests do) or run --live")
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
        self.ex.on_noop = lambda m: self.fmm_noops.append(m) if hasattr(self, "fmm_noops") else None
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
        self.fills_read_t, self.pos_read_t, self.last_pos_raw, self.reads_report_t = None, None, [], time.time()   # ideas_backlog Oct 8: polling cuts + the hourly reads-by-purpose line
        self.sample_t, self.lifecycle, self.lifecycle_seen_ts, self.lifecycle_done = {}, {}, time.time(), collections.deque(maxlen=20000)   # the book sampler and the order-lifecycle log (orders placed from now on; C84 backfilled the old ones with wrong marks)
        self.release_demand = None
        self.loop_end_t, self.cap_use_hist = None, collections.deque(maxlen=2000)   # dashboard v4
        self.release_name = os.path.basename(os.path.realpath(os.path.join(os.path.dirname(DATA), "run"))) if os.path.exists(os.path.join(os.path.dirname(DATA), "run")) else ""
        self.fat_book, self.fat_anchor, self.fat_block, self.fair_hist, self.fat_now = {}, {}, {}, {}, {}
        self.fat_sent = collections.deque(maxlen=5000)      # placement times (pilot gate: write usage)
        self.dump_now, self.dump_book, self.dump_block, self.dump_view, self.dump_ref = {}, {}, {}, {}, {}   # dump catcher
        self.scalp_now, self.scalp_view, self.scalp_inv, self.vol24, self.scalp_note, self.pause_t = {}, {}, collections.deque(maxlen=5000), {}, {}, 0.0   # day 5 §2
        self.constrained, self.constrained_t, self.constrained_min = False, None, collections.deque(maxlen=600)   # day 6 §3.3 (minutes, per hour)
        self.release_fund_log, self.dump_refresh_t = [], {}   # day 6 §3.3 funding per hour; §4.1 level refresh times
        self.release_why = {}                               # ex -> (t, branch) of the last release plan (fill labels)
        self.touch_now, self.touch_view, self.touch_block, self.touch_note = {}, {}, {}, {}   # day 6 §2
        self.wall_now, self.wall_view = {}, {}               # Piero's manual walls (control `wall`)
        self.sleeve_now, self.sleeve_last, self.sleeve_block, self.sleeve_log = {}, {}, {}, []   # day 9 sleeve (settlement holds)
        self.swing_now, self.swing_block, self.swing_note = {}, 0.0, {}   # day 10 §3: the RI swing
        self.big_sweep, self.big_sweep_t = set(), 0.0                    # day 10 §4.2: big-sweep sides (clips to 4k)
        self.fm_now, self.fm_block, self.fm_fills, self.fm_note, self.fm_poll = {}, {}, collections.deque(maxlen=5000), {}, None   # mm_plan §1: the fair maker
        self.tm_now, self.tm_view, self.tm_block, self.tm_fills, self.tm_note, self.tm_depth, self.off24, self.tm_cov, self.tm_wpeak = {}, {}, {}, collections.deque(maxlen=5000), {}, {}, {}, {}, 0   # day 11 §2: the touch maker
        self.tm_buf_paused, self.tm_stop_logged = False, None   # C87: the BUFFER pause and the pre-ring entry stop
        self.pnl_alert_t, self.trims_seen, self.trim_t, self.book_scan_t, self.book_scan_rows, self.reads_before, self.reads_after_t = {}, 0, None, 0.0, [], None, None   # C88
        self.core_t, self.core_set, self.scoreboard_t, self.scoreboard = 0.0, set(), 0.0, {}   # C88: the two books and the scoreboard
        self.trading_inv_stopped = None                     # C89: the 30k trading-inventory stop
        self.dedup_pending = False
        self.drift_since, self.h1_test_stopped, self.h1_shortfall_done, self.noedge_listed = {}, False, False, False   # C93
        self.lotavg_logged = set()                          # C93d: one LOT AVG CHECK line per lot
        self.rot_now, self.rot_view, self.rot_posted, self.rot_logged = {}, {}, {}, set()   # C94: the coin-flip rotation layer
        self.inv_new_t = {}                                 # C95c: ex -> when the audit last booked unexplained inventory
        self.fit_log = {}                                   # C95d: (ex, layer) -> last 'exit not sent' line
        self.rot_post_log = {}                              # C95f: (ex:side, 'posted') -> last ROT POSTED line
        self.fmm_now, self.fmm_view, self.fmm_sweep, self.fmm_lat, self.fmm_logged = {}, {}, {}, [], set()   # FMM (fmm_spec)
        self.fmm_paper = dict(orders={}, asks=[], lots=[], realized=0.0, fills=0, rt=0, takes=0, locks=[], saved_t=0.0)
        self.fmm_unread, self.fmm_noops, self.fmm_cov = [], [], {}           # v2 §2.5: buys not yet in a cash read; SIG no-op buys; coverage
        self.fmm_cash_seen, self.rot_seen_fills = set(), set()               # each fill once (the loop re-hands an order's earlier fills)
        self.carry_now, self.carry_sell, self.carry_t, self.carry_seen, self.carry_view = {}, {}, 0.0, set(), {}   # C98: the carry book
        self.sale_anchor, self.sale_stats, self.foreign_actions = {}, collections.defaultdict(collections.Counter), {}   # C99: sale asks keep their place; foreign fills' direction
        self.sell_undercut, self.fmm_hour_prev, self.churn_t = {}, None, stamp()
        self.election, self.el_mode_cached, self.el_snap = None, "off", dict(cash=0.0, pos={})   # C102: election night (n_election v2)
        self.el_logq, self.el_shadowq, self.el_hour = collections.deque(maxlen=5000), collections.deque(maxlen=20000), collections.Counter()
        self.drift_now, self.drift_view, self.drift_logged = {}, {}, {}
        self.callexit_now, self.callexit_logged = {}, set()
        self.el_sent, self.el_fill_seen = {}, set()
        self.fmm_refill_wait, self.fmm_lat_plan = {}, []   # C100: per sell order, since when the others' best ask sits below ours; the FMM hour; churn window
        self.gap_paper = dict(asks={}, realized=0.0, fills=0, done=0)                                 # C99c: the gap-median exit on paper
        self.tm_behind, self.sm_behind, self.paper, self.paper_seen, self.edge_t, self.edge_hour, self.h4_used = {}, {}, {}, set(), 0.0, None, 0.0   # C90: the edge program
        self.tm_cap_logged, self.tm_unfunded, self.dump_cut_rows, self.rotation_log, self.rotation_logged, self.rotation_on = None, [], [], [], {}, False   # Piero 22:15 Oct 7 (C82) / 06:35 Oct 8 (C84b): the rotation
        self.sm_now, self.sm_block, self.sm_fills, self.sm_depth, self.sm_cov, self.sm_wpeak, self.sm_note = {}, {}, collections.deque(maxlen=5000), {}, {}, 0, {}   # turnover_plan §1: the spread maker
        self.prints, self.print_ids, self.sm_regime, self.sm_regime_t, self.sm_exits_only, self.sm_tune_mtime = {}, {}, {}, 0.0, None, None   # C87: print ring, live regimes, loss stop, re-tune
        self.entries_off, self.res_p10, self.res_p10_t, self.boot_t, self.ring_on = False, {}, {}, time.time(), None
        self.mkt_feed, self.feed_cmp, self.feed_report_t, self.feed_samples, self.feed_sweep_log_t = None, collections.deque(maxlen=5000), time.time(), 0, 0.0   # C86: the markets feed   # C84c: the entry guard's hysteresis; the reservoir's 24-h p10 of RI NO prints; the start time (optional reads wait 2 min)
        self.fast_seen, self.fast_rearms, self.fast_slow, self.fast_orders, self.fast_stats = set(), collections.defaultdict(list), {}, {}, collections.Counter()   # mm_plan §2: the fast path
        self.in_use_hist, self.use_peak, self.rule0_log, self.rule0_blocked, self.lots_cache = collections.deque(maxlen=3000), 0.0, {}, collections.deque(maxlen=5000), (None, {})   # §9
        self.uncovered_since, self.uncovered_log, self.uncovered_n, self.refreshed = {}, {}, collections.deque(maxlen=2000), {}   # day 7 §1.3
        try:
            self.load_vol24()
        except Exception as err:
            print(f"tape volume load: {err!r}")
        try:
            self.day7_migrations()
        except Exception as err:
            print(f"day 7 migrations: {err!r}")
        try:
            self.day8_migrations()
        except Exception as err:
            print(f"day 8 migrations: {err!r}")
        try:
            self.day9_migrations()
        except Exception as err:
            print(f"day 9 migrations: {err!r}")
        try:
            self.day10_migrations()
        except Exception as err:
            print(f"day 10 migrations: {err!r}")
        try:
            self.mm_migrations()
        except Exception as err:
            print(f"mm plan migrations: {err!r}")
        try:
            self.day11_migrations()
        except Exception as err:
            print(f"day 11 migrations: {err!r}")
        try:
            self.tm_migrations()
        except Exception as err:
            print(f"touch maker migrations: {err!r}")
        try:
            self.sm_migrations()
        except Exception as err:
            print(f"spread maker migrations: {err!r}")
        try:
            self.c87_migrations()                           # C87 (Piero 19:56 Oct 8): TM full, SM gated, FM / scalper deleted
        except Exception as err:
            print(f"C87 migrations: {err!r}")
        try:
            self.core_set = {(str(m["ex"]), m["sub"]) for m in ((self.controls.d.get("core") or {}).get("lots") or {}).values()}   # C88: the books from the last run
            self.c88_migration(time.time())
            self.c90_migration(time.time())
            self.c91_migration(time.time())
            self.c92_migration(time.time())
            self.c93_migration(time.time())
            self.c93b_migration(time.time())
            self.c93c_migration(time.time())
            self.c93e_migration(time.time())
            self.c94_migration(time.time())
            self.c95_migration(time.time())
            self.c96_migration(time.time())
            self.c97_migration(time.time())
            self.c101_migration(time.time())
            if not (self.controls.d.get("c95c") or {}).get("since"):
                with self.controls.lock:
                    self.controls.d["c95c"] = dict(since=stamp()); self.controls.save()
                self.night.line("C95c: Piero's own fills go to the 'manual' sub-book from now on (never sold by the engine); new unexplained inventory is left alone 10 min; "
                                "the dashboard header shows a paused banner with its reason and a Resume now button")
        except Exception as err:
            print(f"C88 / C90 migration: {err!r}")
        try:
            self.band_credit_migration()
        except Exception as err:
            print(f"band credit migration: {err!r}")
        try:
            self.reservoir_migration()
        except Exception as err:
            print(f"reservoir migration: {err!r}")
        try:                                                # Oct 5 02:30: undo synthetic fills that duplicated a SIG fill
            fixed = self.ledger.dedup_synthetic()          # booked <= quantity per order + phantom netting (no time-window pairing)
            if fixed["removed"] or fixed["netted"]:
                msg = (f"DEDUP: {len(fixed['removed'])} synthetic (order_shrink) fills that duplicated SIG's own fill removed; "
                       f"{len(fixed['netted'])} phantom sub-book pairs netted against inventory: "
                       + "; ".join(f"{self.name(ex)} {sub.split(':')[0]} {old:+,.0f} -> {new:+,.0f}" for ex, sub, old, new in fixed["netted"][:12])
                       + (" …" if len(fixed["netted"]) > 12 else ""))
                self.event("AUDIT", msg)
                self.night.line(msg)
                self.ledger.save()
        except Exception as err:
            print(f"dedup: {err!r}")
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
        self.dump_cov = None                                # day 8 §2: dump coverage (sides with >= 1 level / eligible sides)
        self.dump_gap_since, self.dump_gap_log, self.dump_cut_t = {}, {}, {}   # day 9 §4: gaps on funded sides, with cause
        self.ri_senate_base = None                          # day 9 §9.4: RI Senate holdings at cost when C73 started (set by day9_migrations)
        self.ri_held_cap = None                             # day 11 §1.4: RI held at cost at the C81c deploy (recorded once prices are in); the engine may add RI_TURNOVER in flight
        self.held_idle = {}                                 # day 11 §1.2: {sub: edge} of the lots held while cash is idle (free >= BUFFER)
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

    MARKET_LAYERS = {"exit": "recycler", "recycle": "recycler", "take": "sniper", "mm": "mm",   # "touch" is the day-6 layer (the old MM touch quote is gone)
                     "ladder": "ladder", "rebound": "ladder", "set": "pairs", "unwind": "pairs", "repair": "pairs",
                     "release": "release", "fatfinger": "fatfinger", "dump": "dump", "dumpexit": "dump",
                     "longshot": "longshot", "longexit": "longshot", "scalp": "scalp", "scalpexit": "scalp", "touch": "touch", "touchexit": "touch", "wall": "wall", "wallexit": "wall", "sleeve": "sleeve", "swing": "swing", "swingsell": "swing", "fm": "fm", "fmexit": "fm", "tm": "tm", "tmexit": "tm", "rot": "rot", "rotexit": "rot", "fmm": "fmm", "fmmtake": "fmm", "fmmexit": "fmm", "fmmhedge": "fmm", "carry": "carry", "sm": "sm", "smexit": "sm",
                     "callsnipe": "election", "callexit": "election", "settle": "election", "drift": "election", "driftexit": "election"}   # C102
    REDUCING = {"exit", "recycle", "rebound", "unwind", "repair", "release", "dumpexit", "longexit", "scalpexit", "touchexit", "wallexit", "tmexit", "fmexit", "swingsell", "smexit", "rotexit", "fmmexit", "callexit", "driftexit"}

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
            if c.paused and not reducing and not (layer == "longshot" and (c.d.get("travel") or {}).get("on")):
                continue                                     # day 6 §4.2: the travel pause keeps the longshot bids resting
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
        if self.loop_end_t and time.time() - self.loop_end_t > 300:   # day 6 §4.2: a wake after a sleep / an outage
            self.wake_t = time.time()
            self.event("WAKE", f"loop gap {(time.time() - self.loop_end_t) / 60:.0f} min: resuming; the travel pause (if set) lifts after 2 healthy min")
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
        # ideas_backlog (Oct 8): the realtime feed wakes us on fills / order updates, so fills are read on such an event (or
        # every 60 s as a fallback) and positions after fills or order reads (or every 120 s); prices stay every loop
        need_fills = (self.fills_read_t is None or now - self.fills_read_t >= FILLS_READ_S or bool(self.last_fill_exs) or self.fills_blind
                      or any(k in ("fills", "orders", "resync") for _, k, _ in events))
        need_pos = (self.pos_read_t is None or now - self.pos_read_t >= POSITIONS_READ_S or need_fills or need_orders or low or not self.last_pos_raw)
        with ThreadPoolExecutor(max_workers=6) as pool:
            # open orders FIRST, then positions, in one worker: a fill between the two reads can then only make the
            # position look smaller than the orders imply (I2 capacity understated, never overstated)
            jobs = dict(fair=pool.submit(pm.fairs, self.mp),
                        book=pool.submit(lambda: ((api.open_orders() if need_orders else list(self.open)), (api.positions_raw() if need_pos else list(self.last_pos_raw)))),
                        px=pool.submit(api.prices, watched))
            if need_fills:
                jobs["fills"] = pool.submit(api.fills, set(self.ledger.seen))
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
        if "fills" in got:
            self.fills_read_t = now
        if need_pos:
            self.pos_read_t, self.last_pos_raw = now, list(got["pos"])
        if now - self.reads_report_t >= 3600:               # the hourly reads-by-purpose line (before / after the polling cuts)
            self.reads_report_t = now
            rb = api.reads_by_purpose(600) if hasattr(api, "reads_by_purpose") else {}   # a 10-min window: the call log holds ~2,000 entries, under an hour at 77/min
            msg = f"READS last 10 min: {sum(rb.values()):.0f}/min of the {getattr(api, 'READ_BUDGET', 95)} budget — " + (", ".join(f"{k} {v:g}" for k, v in rb.items()) or "n/a")
            self.event("READS", msg)
            self.night.line(msg)
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
        if got.get("fills"):
            dash5.archive_append(got["fills"], self.ledger.orders)   # dashboard v5 §0: the append-only fills archive (complete since Oct 1)
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
                self.touch_fill_events(new_f)
                self.wall_fill_events(new_f)
                for f_ in new_f:                            # C93 item 3: FILLED is logged from SIG's fills only (cash freed counts here, never at POSTED)
                    if f_.get("layer") == "release" and f_.get("action") == "buy":
                        k_ = self.fill_key(f_)
                        if k_ in self.rot_seen_fills:
                            continue                        # the loop re-hands an order's earlier fills: each FILLED line once
                        self.rot_seen_fills.add(k_)
                        px_s = round(1 - f_["price"], 3)
                        tag = self.rot_sale_fill(f_, px_s)  # C94: a rotation sale's fill feeds the rotation's cash pool
                        msg = f"FILLED {tag}release {self.name(f_['ex'])} {('NO' if f_['side'] == 'yes' else 'YES')} {f_['qty']:,.0f} at {px_s:.3f}: cash freed {f_['qty'] * px_s:,.0f}, realized {(f_.get('realized') or 0):+,.0f}"
                        self.event("RELEASE", msg); self.night.line(msg)
                self.rot_fill_events(new_f)
                self.fmm_fill_events(new_f)
                self.carry_fill_events(new_f)
                self.el_fill_events(new_f)                      # C102 item 9: detection -> sent -> filled, per election order
                self.sleeve_fill_events(new_f)
                self.swing_fill_events(new_f)
                self.rule0_fill_check(new_f)
                self.fm_fill_events(new_f)
                self.tm_fill_events(new_f)
                self.sm_fill_events(new_f)
                for f in new_f:                             # an order filled out before any snapshot showed it: no longer "acked, resting"
                    rec = self.ledger.orders.get(str(f.get("order") or ""))
                    if rec and rec.get("filled", 0) >= rec.get("qty", 0) - QTY_TOL:
                        with self.ex.wake:
                            self.ex.recent.pop(str(f["order"]), None)
        except Exception as err:
            self.event("ERR", f"fat-finger events: {err!r}")
        self.ledger.init_subbooks(pos, self.manual_ex)
        fixed_ = self.ledger.audit_subbooks(pos, log=lambda s: self.event("AUDIT", s.strip()[6:].strip()), skip=self.audit_skip(pos, now))
        for ex_, diff_ in fixed_ or ():
            if abs(diff_) >= 1:
                self.inv_new_t[str(ex_)] = now               # C95c: new unexplained inventory (maybe Piero's trade, its fill not read yet): no seller for 10 min
        try:
            self.foreign_fill_actions()                     # C99: SIG's fills carry no buy / sell; a foreign order's direction comes from its record
            self.manual_book(now)                           # C95c: Piero's own fills -> the 'manual' sub-book (never sold by the engine)
            self.az06_audit()                               # C100 item 2: once, SIG's record of Piero's four Oct 17 AZ-06 orders
        except Exception as err:
            self.event("ERR", f"manual book: {err!r}")
        try:
            self.el_step(now)                               # C102: election night — the snapshot for the feed thread, the pulls, the queued snipes
        except Exception as err:
            self.event("ERR", f"election: {err!r}")
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
        self.cash_raw_hist = (getattr(self, "cash_raw_hist", []) + [raw])[-3:]
        mr = self.manual_reserve_left()                     # C95 (Piero 15:05 Oct 16): the engine never spends below floor + the manual reserve
        er = self.el_reserve(now)                           # C103: the election reserve ramp (Oct 28 20k usable -> Nov 3 60k): no entry layer spends it
        self.engine_cash = min(self.cash_raw_hist) - mr - er
        claim = self.rot_claim() + self.fmm_priority()      # C95: the rotation's own cash (its sales) is not the other layers'; C97 item 2: FMM's first 10k
        self.cash_hist = [x - mr - er - claim for x in self.cash_raw_hist]
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
        try:
            self.track_tm(now)
        except Exception as err:
            self.event("ERR", f"touch maker exits: {err!r}")
        try:
            snap = dash4.take_snapshot(self, now)           # dashboard v4 rule 3: the 17:00 PT position snapshot (once per SIG day)
            if snap:
                self.event("SIGDAY", f"SIG day {dash4.sig_day_label(snap['start'])} snapshot: {len(snap['positions'])} positions, "
                                     f"account {snap['account']:,.0f}" + (" (late: taken " + time.strftime('%H:%M', time.localtime(now)) + ")" if snap["late"] else ""))
        except Exception as err:
            self.event("ERR", f"sigday snapshot: {err!r}")
        try:
            self.fast_reconcile(now)                        # mm_plan §2: a fast order neither resting nor filled -> slow path 10 min
        except Exception as err:
            self.event("ERR", f"fast reconcile: {err!r}")
        try:
            if RULES_V2:
                self.buffer_calc(now)                       # day 11 §1.3: BUFFER recomputed once a day
        except Exception as err:
            self.event("ERR", f"buffer: {err!r}")
        try:
            self.track_lifecycle(now)                       # ideas_backlog: the order-lifecycle log (SM / TM / L0)
        except Exception as err:
            self.event("ERR", f"lifecycle: {err!r}")
        try:
            self.feed_report(now)                           # C86: the markets feed's 30-min line
        except Exception as err:
            self.event("ERR", f"feed report: {err!r}")
        try:
            self.run_sell_now(now)                          # Piero 17:15: an immediate fill-or-cancel sale, when one is queued
        except Exception as err:
            self.event("ERR", f"sell_now: {err!r}")
        self.controller(now)
        kappa, _ = self.kappa()
        used = self.bucket_usage()
        try:
            self.release_now = self.release_plan(cash, now, used)
        except Exception as err:                            # never let it stop the loop
            self.release_now = {}
            self.event("ERR", f"release plan: {err!r}")
        try:
            if RULES_V2:
                for ex_, r_ in self.rotation_plan(cash, now).items():   # Piero 22:15 Oct 7 (C82): the rotation sells through the release path
                    self.release_now.setdefault(ex_, r_)
        except Exception as err:
            self.event("ERR", f"rotation plan: {err!r}")
        self.entry_left = self.entry_budget(cash)             # Oct 5 23:35: the 2 x free budget, allocated dump L1-L4 -> L0 -> scalper -> fat-finger
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
            self.track_walls(now)
            self.wall_now = self.wall_plan(now, cash)
            self.sleeve_now = self.sleeve_plan(now, cash)
            self.swing_now = self.swing_plan(now)
        except Exception as err:
            self.wall_now = {}
            self.event("ERR", f"wall plan: {err!r}")
        try:
            self.fm_now = self.fm_plan(now, cash)          # mm_plan §1: after L0 (the dump plan), before the scalper
            self.tm_now = self.tm_plan(now, cash, used)    # day 11 §2: the touch maker (its cap is reserved off the top like the FM's)
            self.sm_now = self.sm_plan(now, cash)          # turnover_plan §1: the spread maker
            self.rot_now = self.rot_plan(now, cash)        # C94: the coin-flip rotation's bids (funded by its own sales only)
            try:
                self.c98_migration(now)                    # once, with positions and fair in
            except Exception as err:
                self.event("ERR", f"C98 migration: {err!r}")
            try:
                if self.gapmm_paper_on(now):
                    self.gapmm_paper_step(now)             # C99c: the gap-median exit on paper, 06:00-10:00
                self.gapmm_gate(now)
            except Exception as err:
                self.event("ERR", f"gapmm: {err!r}")
            try:
                self.fmm_gate(now)                         # fmm_spec §3.3 every loop: paper -> live by its gates, the step-up, the stop
            except Exception as err:
                self.event("ERR", f"FMM gate: {err!r}")
            try:
                self.fmm_now = self.fmm_plan(now)          # fmm_spec: favourite-side MM (paper: simulated, nothing sent)
            except Exception as err:
                self.fmm_now = {}
                self.event("ERR", f"FMM plan: {err!r}")
            try:
                self.carry_now = self.carry_plan(now)      # C98 alpha_spec §2: the carry book's bids (held to settlement)
            except Exception as err:
                self.carry_now = {}
                self.event("ERR", f"carry plan: {err!r}")
            self.scalp_now = self.scalp_plan(now, cash, used)
        except Exception as err:
            self.scalp_now = {}
            self.event("ERR", f"scalper plan: {err!r}")
        try:
            self.fat_now = self.fat_plan(now)               # last in line for the entry budget
        except Exception as err:
            self.fat_now = {}
            self.event("ERR", f"fat-finger plan: {err!r}")
        try:
            self.track_touch(now)
            self.touch_now = self.touch_plan(now, cash, used)
        except Exception as err:
            self.touch_now = {}
            self.event("ERR", f"touch plan: {err!r}")
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
            if rec and not (rec.get("layer") == "dump" and o["id"] in self.ex.cancelling):   # day 9 §4: a dump level being cancelled
                mine.setdefault(rec["bot"], []).append(dict(o, sub=rec.get("sub", ""), layer=rec.get("layer", "")))   # is not resting: its replacement stays
        for p in self.ex.pending():
            mine.setdefault(p["bot"], []).append(dict(p, id=None, price=p["price"]))
        plans = []
        try:
            self.sell_undercut_update(now)                  # C100 item 3: keep-place clocks for every sell order
        except Exception as err:
            self.event("ERR", f"sell keep-place: {err!r}")
        self.ladder_quota = int(LADDER_CAPACITY_SHARE * self.ex.capacity * max(LOOP_S, self.t.get("read", 0) + 5) / 60)
        tier_a = self.fat_tiers() if self.fat_now else set()
        for b in sorted(self.bots, key=lambda b: (b.kind != "position", b.ex not in tier_a, -self.sweep_rank.get(b.ex, (0, 0))[0],
                                                  -self.sweep_rank.get(b.ex, (0, 0))[1])):   # tier A, then most sweeps first
            s = self.bot_snapshot(b, pos, rows, kappa, cash, used)
            s["own"] = [o for o in mine.get(b.name, []) if o.get("id")]   # I4: our resting orders aren't the market
            wants = self.rule0(b, self.gate(b, b.plan(s)), s)
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
                if self.wd_after("entries_stop", now):          # C104 (C13): catcher / fat-finger / longshot / wall / scalper entries stop Nov 2 17:00
                    wants = [w for w in wants if w.get("reduce") or w["layer"] not in WIND_DOWN["entries_stop_layers"]]
                if self.election is not None and self.el_mode_cached == "live" and not self.election.entries_allowed(str(b.ex), now):
                    wants = [w for w in wants if w.get("reduce") or w["layer"] not in el.PULL_LAYERS]   # C102 C14 / C15: none from the first close - 10 min
                hold = SELL_HOLD.get(str(b.ex))
                if hold:                                    # C100 item 1: no layer sells (or nets with a buy of the other side) the held side
                    wants = [w for w in wants if not ((w["action"] == "buy" and w["side"] != hold) or (w["action"] == "sell" and w["side"] == hold))]
            if self.drift_since:                            # C93: a timer only lives while its order rests
                open_ids = {o["id"] for o in self.open}
                for oid in list(self.drift_since):
                    if oid not in open_ids:
                        self.drift_since.pop(oid, None)
            for w in (wants if GTC_TTL_S else ()):          # C93 item 3: GTC — every seller, exit and TM / SM quote rests until re-pegged
                if w.get("reduce") or w.get("layer") in ("tm", "sm", "release", "rot", "fmm", "fmmhedge", "carry"):   # (not fmmtake: a sweep take keeps its 120 s)
                    w["ttl"] = GTC_TTL_S
            kept, cancels, places = bots.reconcile(wants, mine.get(b.name, []))
            cancels, places = self.repeg_grace(b, cancels, places, now)
            places = self.no_self_cross(b, places, cancels)
            places = self.fit_reducing(b, places)           # C95d: exits sized to the position's room at planning, so the executor never trims (= no I2 pause)
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
        for job in (self.hourly, lambda: self.log_leaders(time.time()), lambda: self.log_leaders_day(time.time()), lambda: self.leaders_marks_check(time.time()),
                    lambda: self.scan_new_markets(time.time()), lambda: self.intel_step(time.time()),
                    lambda: self.intel_daily(time.time())):
            try:
                job()
            except Exception as e:                          # side jobs must never stop trading
                self.event("ERR", f"side job: {e!r}")

    # ------------------------------------------------------------------------------------------------ intel (day 2 §5)
    def sampler_targets(self):
        """The 20 SM markets (when the SM is on) + RI Senate R."""
        out = list(self.sm_universe(time.time())) if self.controls.on("sm") else []
        ri = str((self.controls.d.get("swing") or {}).get("ex") or SWING.get("ex") or "")
        if ri and ri in self.owner and ri not in out:
            out.append(ri)
        return out

    def book_sampler(self, now):
        """ideas_backlog (Oct 8): depth at the touch +/- 2 ticks on the SM universe + RI Senate R, every ~60 s each, with spare
        reads only, to data/intel/books/<day>.jsonl; the samples also feed the SM / TM depth caches (no second read)."""
        if not self.controls.on("sm") or now - getattr(self, "boot_t", 0) < STARTUP_HOLD_S:
            return 0                                        # the sampler serves the spread maker's universe (+ RI Senate R); not in the first 2 min after a start (the restart burst gave a 429 every time)
        due = sorted((ex for ex in self.sampler_targets() if now - self.sample_t.get(ex, 0) >= SAMPLER_EVERY_S), key=lambda e: self.sample_t.get(e, 0))
        n = 0
        for ex in due[:SAMPLER_PER_LOOP]:
            fb = self.feed_book(ex)                         # C86 live: the feed's book, one REST spot check per FEED_SPOT_EVERY samples
            self.feed_samples += 1
            src = "feed"
            if fb is not None and self.feed_samples % FEED_SPOT_EVERY:
                bids, asks = fb[0][:SAMPLER_DEPTH], fb[1][:SAMPLER_DEPTH]
            else:
                if not api.spare_reads(keep=max(1, SPARE_KEEP // 2)):   # the sampler is the freed budget's first consumer: it runs to READ_BUDGET - 10
                    break
                try:
                    bids, asks = api.orderbook(ex, depth=SAMPLER_DEPTH)
                except Exception as err:
                    self.event("ERR", f"book sampler {self.name(ex)}: {err!r}")
                    self.sample_t[ex] = now
                    continue
                src = "rest"
                try:
                    self.feed_compare(ex, bids, asks, now)  # C86: every REST sample is a comparison with the feed
                except Exception as err:
                    self.event("ERR", f"feed compare: {err!r}")
            self.sample_t[ex] = now
            self.sm_depth[ex] = self.tm_depth[ex] = (now, bids, asks)
            px = self.px.get(ex) or (None, None, None)
            self.intel_write(f"books/{datetime.fromtimestamp(now).strftime('%Y-%m-%d')}.jsonl",
                             dict(t=round(now, 3), ex=ex, fair=self.fair.get(ex), bid=px[0], ask=px[1], last=px[2], bids=bids[:SAMPLER_DEPTH], asks=asks[:SAMPLER_DEPTH], src=src))
            n += 1
        return n

    def feed_mode(self):
        return str(self.controls.d.get("feed_mode") or FEED_MODE_DEFAULT)

    def feed_book(self, ex):
        """C86 live mode: (bids, asks) from the markets feed when fresh and from the current connection, else None (REST then)."""
        if self.feed_mode() != "live" or self.mkt_feed is None:
            return None
        return self.mkt_feed.book(ex, FEED_BOOK_MAX_AGE_S)

    def on_feed_sweep(self, sw):
        """A sweep on the feed: logged (data/intel/feed_sweeps.jsonl) and, in live mode, the loop wakes for that market."""
        try:
            self.intel_write(f"feed_sweeps/{datetime.fromtimestamp(sw['t']).strftime('%Y-%m-%d')}.jsonl", dict(sw, name=self.name(sw["ex"]), mode=self.feed_mode()))
        except Exception:
            pass
        try:
            self.fmm_note_sweep(sw)                         # fmm_spec §2.2: a sweep that drops a favourite side arms the refill
        except Exception:
            pass
        if self.feed_mode() == "live":
            self.rt_event("sweep", {sw["ex"]}, sw["recv"])

    def feed_compare(self, ex, bids, asks, now):
        """C86: the feed's book against a REST read of the same exchange: touch prices equal, touch sizes equal, and the shares at each of
        our resting SM / TM levels (queue-ahead) — the log-only window's agreement numbers."""
        if self.mkt_feed is None:
            return
        fb = self.mkt_feed.book(ex, 60.0)
        if fb is None:
            self.feed_cmp.append(dict(t=now, ex=ex, have=False))
            return
        fbids, fasks = fb
        top = lambda xs: (xs[0][0], xs[0][1]) if xs else (None, None)
        rb, ra, fbb, fba = top(bids), top(asks), top(fbids), top(fasks)
        qa = []
        for o in self.open:
            rec = self.ledger.orders.get(o["id"]) or {}
            if o["ex"] != ex or rec.get("layer") not in ("sm", "tm", "smexit", "tmexit"):
                continue
            lvl, book_r, book_f = (o["price"], bids, fbids) if o["side"] == "yes" else (round(1 - o["price"], 3), asks, fasks)
            r_q = next((q for p_, q in book_r if abs(p_ - lvl) < 1e-9), 0.0)
            f_q = next((q for p_, q in book_f if abs(p_ - lvl) < 1e-9), 0.0)
            qa.append(abs(r_q - f_q))
        self.feed_cmp.append(dict(t=now, ex=ex, have=True, px_ok=(rb[0] == fbb[0] and ra[0] == fba[0]), size_ok=(rb == fbb and ra == fba), qa=qa))

    def feed_report(self, now):
        """Every 30 min: the feed's health and the REST comparisons, one RUN_LOG line."""
        if self.mkt_feed is None or now - self.feed_report_t < FEED_REPORT_S:
            return
        since, self.feed_report_t = self.feed_report_t, now
        f = self.mkt_feed
        st = dict(f.stats)
        prev = getattr(self, "feed_prev_stats", {}) or {}
        self.feed_prev_stats = st
        d = {k: st.get(k, 0) - prev.get(k, 0) for k in st}
        mins = max(1.0, (now - since) / 60)
        cmp_ = [x for x in self.feed_cmp if x["t"] >= since]
        have = [x for x in cmp_ if x["have"]]
        qa = sorted(q for x in have for q in x.get("qa") or [])
        big = []
        try:
            for line in open(os.path.join(DATA, "intel", "events.jsonl")):
                e = json.loads(line)
                if e.get("t", 0) >= since and (e.get("value") or 0) >= 1000:
                    big.append(e)
        except Exception:
            pass
        sw = [x for x in f.sweeps if x["recv"] >= since]
        seen = sum(1 for e in big if any(x["ex"] == str(e.get("ex")) and abs(x["t"] - e.get("t", 0)) <= 600 for x in sw))
        msg = (f"FEED ({self.feed_mode()}) last {mins:.0f} min: connected {f.connected()}, {d.get('messages', 0) / mins:.1f} batches/min, {d.get('books', 0):,} books, {d.get('trades', 0):,} trades, "
               f"gaps {d.get('gaps', 0)}, duplicates {d.get('duplicates', 0)}, reconnects {d.get('reconnects', 0)}, errors {d.get('errors', 0)}; book asOf lag p50 {f.lag_pct(0.5)} s / p90 {f.lag_pct(0.9)} s; "
               f"sweeps detected {len(sw)} vs REST big events {len(big)} ({seen} of them also seen by the feed within 10 min); REST spot checks {len(cmp_)}: feed book present {len(have)}, "
               f"touch prices equal {sum(1 for x in have if x['px_ok'])}, touch sizes equal {sum(1 for x in have if x['size_ok'])}, queue-ahead |REST - feed| at our levels p50 "
               f"{qa[len(qa) // 2] if qa else 'n/a'} / p90 {qa[int(0.9 * (len(qa) - 1))] if qa else 'n/a'} shares over {len(qa)}")
        self.event("FEED", msg)
        self.night.line(msg)

    def queue_ahead(self, ex, side, price):
        """Shares resting at the order's own level on its side, from the latest book sample (None without a fresh sample)."""
        fb = self.feed_book(ex)                             # C86 live: the feed's current book
        if fb is not None:
            bids, asks = fb
            if side == "yes":
                return next((q for p, q in bids if abs(p - price) < 1e-9), 0.0)
            return next((q for p, q in asks if abs(p - round(1 - price, 3)) < 1e-9), 0.0)
        c = self.sm_depth.get(ex) or self.tm_depth.get(ex)
        if not c or time.time() - c[0] > 2 * SAMPLER_EVERY_S:
            return None
        _, bids, asks = c
        if side == "yes":
            return next((q for p, q in bids if abs(p - price) < 1e-9), 0.0)
        return next((q for p, q in asks if abs(p - round(1 - price, 3)) < 1e-9), 0.0)

    def track_lifecycle(self, now):
        """ideas_backlog (Oct 8): for every SM / TM order and dump L0 level: queue ahead at placement, time to fill or expiry, mid and
        fair at +1 / 5 / 30 min; written to data/intel/orders/<day>.jsonl once the order is closed and the 30-min mark is in."""
        for oid, o in self.ledger.orders.items():
            ts = float(o.get("ts") or 0)
            if ts < self.lifecycle_seen_ts - 1 or oid in self.lifecycle or oid in self.lifecycle_done or (o.get("layer") not in LIFECYCLE_LAYERS and not (o.get("layer") == "dump" and str(o.get("sub", "")).endswith(":L0"))):
                continue
            px = self.px.get(o["ex"]) or (None, None, None)
            mid = None if px[0] is None or px[1] is None else round((px[0] + px[1]) / 2, 4)
            self.lifecycle[oid] = dict(id=oid, ex=o["ex"], name=self.name(o["ex"]), layer=o["layer"], sub=o.get("sub"), side=o["side"], price=o["price"], qty=o["qty"],
                                       t_place=round(ts, 3), queue_ahead=self.queue_ahead(o["ex"], o["side"], o["price"]), mid0=mid, fair0=self.fair.get(o["ex"]), marks={})
        self.lifecycle_seen_ts = now
        done = []
        for oid, r in self.lifecycle.items():
            o = self.ledger.orders.get(oid) or {}
            for m in LIFECYCLE_MARKS_S:
                k = str(m)
                if k not in r["marks"] and now >= r["t_place"] + m:
                    px = self.px.get(r["ex"]) or (None, None, None)
                    r["marks"][k] = dict(mid=None if px[0] is None or px[1] is None else round((px[0] + px[1]) / 2, 4), fair=self.fair.get(r["ex"]))
            full = (o.get("filled") or 0) >= (o.get("qty") or float("inf")) - 1e-9
            if "t_close" not in r and (o.get("closed") or full):
                r["t_close"] = o.get("closed") or stamp(); r["outcome"] = "filled" if full else o.get("status"); r["filled"] = o.get("filled") or 0
                try:
                    r["life_s"] = round(time.mktime(time.strptime(r["t_close"], "%Y-%m-%d %H:%M:%S")) - r["t_place"])
                except Exception:
                    r["life_s"] = None
            if "t_close" in r and len(r["marks"]) == len(LIFECYCLE_MARKS_S) or now - r["t_place"] > 3 * 3600:
                self.intel_write(f"orders/{datetime.fromtimestamp(r['t_place']).strftime('%Y-%m-%d')}.jsonl", r)
                done.append(oid)
        for oid in done:
            self.lifecycle.pop(oid, None)
            self.lifecycle_done.append(oid)
        if len(self.lifecycle) > 5000:
            for oid in sorted(self.lifecycle, key=lambda i: self.lifecycle[i]["t_place"])[:1000]:
                self.lifecycle.pop(oid, None)

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
        if self.dump_cov and now - self.intel_s.get("dcov", 0) >= 60:      # day 8 §2: dump coverage every minute
            self.intel_s["dcov"] = now
            self.intel_write("dump_coverage.jsonl", dict(t=now, **self.dump_cov))
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
        try:
            self.book_sampler(now)                          # ideas_backlog: the freed reads go to the book sampler first
        except Exception as err:
            self.event("ERR", f"book sampler: {err!r}")
        for fn in (self.book_scan, self.recycle_depth_snapshot, self.range_candidates_daily):   # C88: zero-read jobs from the feed and the tape
            try:
                fn(now)
            except Exception as err:
                self.event("ERR", f"{fn.__name__}: {err!r}")
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
                self.note_print(ex, r.get("id"), r["ts"], r["p"], r.get("q"), r["fair"])
                self.intel_write(f"trades/{datetime.fromtimestamp(r['ts']).strftime('%Y-%m-%d')}.jsonl", r)
                self.note_volume(ex, r["ts"], r["p"] * r["q"], off=self.print_off_fair(r))
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
            if self.feed_mode() != "off":                   # C86: books and sweeps from the tournament markets topic
                self.mkt_feed = rt.SigMarketFeed(lambda: api._req("POST", "/realtime/token", json={}), api.T["id"], on_sweep=self.on_feed_sweep,
                                                 log=lambda k, s: self.event(k, s), sweep_ticks=FEED_SWEEP_TICKS, sweep_shares=FEED_SWEEP_SHARES,
                                                 sweep_window_s=FEED_SWEEP_WINDOW_S)
                self.mkt_feed.start()
                self.event("FEED", f"SIG markets feed started (mode {self.feed_mode()}): tournament:{api.T['id']}:markets")
        except Exception as e:
            self.event("ERR", f"SIG markets feed not started: {e!r}")
        try:
            toks = read_json(os.path.join(DATA, "clob_tokens.json"), {})
            tokens = {m["ex"]: [toks[i][0] for i in m["pm_ids"] if toks.get(i)] for m in self.mp}
            tokens = {ex: t for ex, t in tokens.items() if t and len(t) == len(next(m["pm_ids"] for m in self.mp if m["ex"] == ex))}
            if tokens:
                self.pm_feed = rt.PolyFeed(tokens, self.rt_event, log=lambda k, s: self.event(k, s))
                self.pm_feed.on_quote = self.el_quote           # C102: every PM quote -> the call detector (feed thread, detection only)
                self.el_lane_ev = threading.Event()             # C104 item 7: the call lane (live engine only)
                threading.Thread(target=self.el_lane_loop, name="call-lane", daemon=True).start()
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
                    touch=self.touch_now.get(b.ex) or [], touch_exits=self.touch_view.get(b.ex, []),
                    wall=self.wall_now.get(b.ex) or [], wall_exits=self.wall_view.get(b.ex, []), sleeve=self.sleeve_now.get(b.ex) or [], swing=self.swing_now.get(b.ex) or {}, fm=self.fm_now.get(b.ex) or [],
                    tm=self.tm_now.get(b.ex) or [], tm_exits=self.tm_view.get(b.ex, []), sm=self.sm_now.get(b.ex) or [],
                    rot=self.rot_now.get(b.ex) or [], rot_exits=self.rot_view.get(b.ex, []),
                    fmm=self.fmm_now.get(b.ex) or [], fmm_exits=self.fmm_view.get(b.ex, []), carry=self.carry_now.get(b.ex) or [],
                    drift=self.drift_now.get(str(b.ex)) or [], drift_exits=self.drift_view.get(str(b.ex), []), callexit=self.callexit_now.get(str(b.ex)) or [],
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
            if hit:                                         # C88 (Piero 01:55): P&L / mark-out rules are ALERTS; only the reject rate (an API signal) still kills
                rej = [k for k in hit if k["rule"].startswith("rejects")]
                if rej:
                    c.kill(l, f"{rej[0]['rule']}: {rej[0]['value']}")
                    self.alerts.append(f"{time.strftime('%H:%M')} layer {l} killed: {rej[0]['rule']} ({rej[0]['value']})")
                elif now - self.pnl_alert_t.get(l, 0) >= 3600:
                    self.pnl_alert_t[l] = now
                    msg = f"ALERT {l}: {hit[0]['rule']} ({hit[0]['value']}) — alert only, no stop (Piero 01:55)"
                    self.alerts.append(f"{time.strftime('%H:%M')} {msg}"); self.event("ALERT", msg)
        # promotions (pilot -> full) and pilot extensions; evening §1.5: none while the hold flag is set or fills are blind
        healthy, why_bad = self.fills_health(now)
        hold = bool(c.d.get("hold_gates")) or not healthy
        gate_free = set(c.d.get("gate_free") or [])         # day 4: layers Piero let promote while the hold is on
        if hold and now - night.get("hold_logged", 0) >= 1800:
            night["hold_logged"] = now
            self.night.line(f"pilot gates held: {'hold flag set' if c.d.get('hold_gates') else ''}"
                            f"{'; ' if c.d.get('hold_gates') and not healthy else ''}{'' if healthy else why_bad}")
        for l in ("mm", "scalp", "touch", "ladder", "pairs", "release", "fatfinger", "dump", "longshot"):
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
                else:                                       # day 6 §3.1: a failed scalper gate -> off, its slice goes to the touch layer
                    c.set_layer("scalp", "off", f"pilot gate failed: {g['why']}")
                    with c.lock:
                        tl = c.d["layers"].setdefault("touch", dict(state="off", caps={}, killed=None))
                        caps = tl.setdefault("caps", {})
                        caps["budget"] = float(caps.get("budget") or LAYER_CAPS["touch"]["budget"][0]) + LAYER_CAPS["scalp"]["inventory"][0]
                        c.save()
                    self.night.line(f"layer scalp off (gate failed: {g['why']}); its 10k slice goes to the touch layer (budget {caps['budget']:,.0f})")
                    continue
            if l == "touch" and age >= TOUCH_PILOT_S:       # day 6 §2 gate: 4 h pilot at half budget
                g = self.touch_gate(since[l], now)
                if g["ok"]:
                    ok, why = True, g["why"]
                elif now - night.get(f"{l}_ext", 0) >= 1800:
                    night[f"{l}_ext"] = now
                    self.night.line(f"layer touch stays pilot: {g['why']}")
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
            self.ratchet("touch", now)
            self.idle_rule(now)
            self.cash_target(now)
            if now - getattr(self, "phantom_t", 0) >= 600:    # Oct 5: an impossible long-tranche / short-inventory pair is netted out
                self.phantom_t = now
                fixed = self.ledger.net_phantoms()
                if fixed:
                    self.event("AUDIT", "phantom sub-book pairs netted against inventory: " + "; ".join(f"{self.name(ex)} {sub.split(':')[0]} {old:+,.0f} -> {new:+,.0f}" for ex, sub, old, new in fixed))
        except Exception as err:
            self.event("ERR", f"ratchet: {err!r}")
        hw = api.health(600).get("write") or {}
        if hw.get("n", 0) >= 10 and hw.get("bad_rate", 0) >= 0.5:   # day 4 §1.2: >= 10 writes in 10 min and >= 50% failed
            self.stop_and_pull(f"SIG writes failing: {hw.get('bad_rate', 0):.0%} 5xx/timeouts of {hw.get('n', 0)} in 10 min")
        self.auto_resume(now)
        # D5 loss stop and the SIG 5xx pause
        bot_24h = sum(self.fill_pnl_fair(f) for f in self.fills_since(now - 86400)
                      if self.strategy_of(f["bot"], f["layer"]) not in ("position", "old engine") and f["layer"] not in D5_EXCLUDE_LAYERS)   # C87: Piero's RI / wall orders are not bot P&L
        if bot_24h < -LOSS_STOP and now - self.pnl_alert_t.get("D5", 0) >= 3600:   # C88: the D5 loss stop is an ALERT, never a pause
            self.pnl_alert_t["D5"] = now
            msg = f"ALERT D5: bot P&L at fair {bot_24h:+,.0f} over 24 h (< {-LOSS_STOP:+,}) — alert only, no pause (Piero 01:55)"
            self.alerts.append(f"{time.strftime('%H:%M')} {msg}"); self.event("ALERT", msg)
        self.hard_pauses(now)
        for fn in (self.core_update, self.scoreboard_update, self.paper_scan, self.edge_controller):
            try:
                fn(now)
            except Exception as err:
                self.event("ERR", f"{fn.__name__}: {err!r}")
        if self.reads_after_t and now >= self.reads_after_t:
            self.reads_after_t = None
            after = api.reads_by_purpose(600)
            self.night.line("C88 reads/min by purpose AFTER the feed switch (last 10 min): " + ", ".join(f"{k} {v:.1f}" for k, v in sorted(after.items(), key=lambda kv: -kv[1])[:10])
                            + f"; total {sum(after.values()):.0f}/min")
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
            if not api.spare_reads() or now - getattr(self, "boot_t", 0) < STARTUP_HOLD_S:   # §6: fills and exits first; not in the first 2 min after a start (C86)
                break
            try:
                fb = self.feed_book(ex)                     # C88: the feed's book (59 levels) first; REST only without a fresh one
                bids, asks = fb if fb is not None else api.orderbook(ex, depth=50)
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
        left = getattr(self, "entry_left", float("inf"))
        if self.entry_budget(min(self.cash_hist) if self.cash_hist else 0.0) <= 0:   # Oct 5 22:50 guard (b): under 2k free, no levels
            left = 0.0
        if left < float("inf"):                             # Oct 5 23:35: last in line for the entry budget (deep levels, 1x)
            cut, spent = {}, 0.0
            for ex in plan:
                for w in plan[ex]:
                    c_ = w["px"] * w["qty"] * ENTRY_WEIGHTS["fatfinger"]   # day 8 §2: 0.05 (0 real fills since Oct 2)
                    if spent + c_ <= left + 1e-6:
                        cut.setdefault(ex, []).append(w)
                        spent += c_
                    else:
                        cov["reasons"][f"{ex}|{w['side']}"] = "reserve not funded"
            cov["covered"] = sum(len(v) for v in cut.values())
            for k in list(cov["reasons"]):
                if cov["reasons"][k].startswith("being placed") and not any(f"{ex}|{w['side']}" == k for ex in cut for w in cut[ex]):
                    cov["reasons"][k] = "reserve not funded"
            plan = cut
            self.entry_left = max(0.0, left - spent)
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
                 "set": PAIR_TTL_S, "unwind": PAIR_TTL_S, "repair": PAIR_TTL_S,
                 "longshot": LONG_TTL_S, "longexit": LONG_EXIT_TTL_S, "scalp": SCALP_TTL_S, "scalpexit": SCALP_TTL_S, "touch": TOUCH_TTL_S, "touchexit": TOUCH_TTL_S, "wall": WALL_TTL_S, "wallexit": WALL_HOLD_S, "sleeve": SLEEVE_TTL_S, "swing": SWING_TTL_S, "swingsell": SWING_TTL_S, "fm": FM_TTL_S, "fmexit": FM_TTL_S, "tm": TM_TTL_S, "tmexit": TM_TTL_S, "sm": SM_TTL_S, "smexit": SM_TTL_S}

    def vanished_fill(self, oid, rec, q0, now):
        """Shares an order filled since the previous snapshot: it shrank, or it left the book while we didn't cancel it
        and before its TTL (evening §1.2: the fat-finger vanished-order rule; §1.4 fallback)."""
        q1 = self.truth_prev_cur.get(oid)
        if q1 is not None:
            return q0 - q1
        if rec.get("status") == "cancelled" or oid in self.ex.cancelling or now - self.ex.recent_cancels.get(oid, 0) < 600:
            return 0.0                                     # Oct 5 03:55: an order we asked to cancel in the last 10 min that vanished is not a fill
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
            # Oct 5 02:30 (143 double bookings since Oct 3): SIG's fills page and its positions lag each other by a few
            # seconds either way, so the only safe bound is the order's own arithmetic: shares that left the book minus
            # shares already booked for it from any source. Never book a synthetic fill beyond that.
            d = min(d, max(0.0, rec["qty"] - cur.get(oid, 0.0) - (rec.get("filled") or 0)))
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
            if cand.get(ex) and abs(abs(exp) - gross) > tol:   # Oct 5 22:50: both sides vanished at once (SIG pulled them): not fills
                shadow_rows.append(dict(ex=ex, dpos=round(dpos, 2), carry=round(c, 2), orders=round(exp, 2), n=len(cand[ex]), match=False, netted=True))
                if abs(dpos) >= 1:
                    self.truth_carry[ex] = dpos
                continue
            if cand.get(ex) and abs(dpos) >= 1 and (abs(dpos - exp) <= tol or abs(dpos + c - exp) <= tol):
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

    def entry_budget(self, cash):
        """Oct 5 22:50 hard guard (b) + day 8 §2: sum(resting entry cost x weight) <= 1.5 x free cash above the floor
        (weights: L0, scalper, walls 1.0; L1 0.5; L2 0.3; L3-L4 0.15; fat-finger 0.05); below 2k free no entry bids at all."""
        free = cash - CASH_RESERVE
        if free < ENTRY_MIN_FREE:
            self.entries_off = True
        elif self.entries_off and free >= ENTRY_RESUME_FREE:
            self.entries_off = False                        # C84c: entries resume only at 3k free (hysteresis against the 2k line)
        return 0.0 if (free < ENTRY_MIN_FREE or self.entries_off) else self.budget_k() * free

    def budget_k(self):
        """Day 9 §2: the live multiplier k (k_max from the replay; -0.5 / +0.5 steps by adjust_budget_k)."""
        bk = self.controls.d.get("budget_k") or {}
        return float(bk.get("k") or BUDGET_K_MAX)

    def k_table(self):
        """Day 11 §1.3 (rules v2): the k_max table hangs off BUFFER — >= BUFFER + 5k: 3.0; BUFFER..+5k: 3.0 (the replay value);
        under BUFFER entries shrink instead of anything being sold: 2.0 down to BUFFER/2, then the k floor."""
        if not RULES_V2:
            return list(BUDGET_K_TABLE)
        B = self.buffer()
        return [(B + 5_000, 3.0), (B, 3.0), (B / 2, 2.0), (0, float(BUDGET_K_MIN))]

    def buffer(self):
        """Day 11 §1.3: BUFFER = the catcher's p95 cash need over the last 72 h + 5k (buffer_calc), else the 15k default."""
        b = (self.controls.d.get("buffer") or {}).get("value")
        return float(b) if b else float(BUFFER_DEFAULT)

    def buffer_calc(self, now, force=False):
        """Once a day: BUFFER = p95 of the catcher's rolling-1-h entry cost (dump buys, 5-min samples over 72 h) + 5k, clamped
        10k..30k; logged with its derivation."""
        c = self.controls
        bf = c.d.get("buffer") or {}
        day = time.strftime("%Y-%m-%d", time.localtime(now))
        if not force and bf.get("day") == day:
            return self.buffer()
        ev = []
        for f in self.fills_since(now - BUFFER_WINDOW_S):
            if f["layer"] == "dump" and f["action"] == "buy":
                ev.append((time.mktime(time.strptime(f["t"], "%Y-%m-%d %H:%M:%S")), f["qty"] * f["price"]))
        ev.sort()
        vals, t = [], now - BUFFER_WINDOW_S
        while t <= now + 1e-6:
            vals.append(sum(v for t_, v in ev if t - 3600 < t_ <= t))
            t += 300
        vals.sort()
        p95 = vals[min(len(vals) - 1, int(BUFFER_P * (len(vals) - 1)))] if vals else 0.0
        value = float(min(BUFFER_MAX, max(BUFFER_MIN, p95 + BUFFER_ADD)))
        with c.lock:
            c.d["buffer"] = dict(value=round(value), p95=round(p95), fills=len(ev), day=day, t=stamp(),
                                 hist=(bf.get("hist") or [])[-30:] + [dict(t=stamp(), value=round(value), p95=round(p95), fills=len(ev))])
            c.save()
        msg = (f"BUFFER {value:,.0f} = the catcher's p95 rolling-1-h entry cost over the last 72 h {p95:,.0f} ({len(ev)} dump buys, 5-min samples) + {BUFFER_ADD:,}"
               f" (clamped {BUFFER_MIN:,}..{BUFFER_MAX:,}); above it catch / touch lots >= {HOLD_IDLE_GAP * 100:.0f}c under fair are held, below it the ladder offers them lowest-edge first and k falls")
        self.event("CASH", msg)
        self.night.line(msg)
        return value

    def k_max_for(self, free, cur_tier=None):
        """Day 9 §9.2: k_max from the free-cash table; a tier is entered upward only once free is 1k past its line."""
        tiers = sorted(self.k_table(), key=lambda t: -t[0])
        pick = tiers[-1][0]
        for lo, _ in tiers:
            if free >= lo + (BUDGET_K_TIER_HYST if (cur_tier is None or lo > cur_tier) and lo > 0 else 0):
                pick = lo
                break
        return pick, dict(self.k_table())[pick]

    def adjust_budget_k(self, now, free=None):
        """Day 9 §2 live auto-adjust inside the §9.2 table: k_max = f(free cash); k -0.5 after 30 min with bounded peak use
        > 70% or any constrained minute; k +0.5 after 2 h with peak use < 40%, never above k_max; every change logged."""
        c = self.controls
        bk = c.d.get("budget_k") or {}
        if free is None:
            free = (min(self.cash_hist) if self.cash_hist else 0.0) - CASH_RESERVE
        tier, kmax = self.k_max_for(free, bk.get("tier"))
        if not bk or bk.get("tier") is None:
            bk = dict(k=float(kmax), k_max=float(kmax), tier=tier, t=now, hist=(bk.get("hist") or [])[-48:] + [dict(t=stamp(), k=float(kmax), why=f"k_max table, free {free:,.0f}")])
            with c.lock:
                c.d["budget_k"] = bk
                c.save()
            self.event("CASH", f"BUDGET k = {bk['k']:g} x free cash above the floor (k_max {kmax:g} for free {free:,.0f}; table {self.k_table()}; -0.5 after 30 min > {BUDGET_K_PEAK_DOWN:.0%} "
                               f"peak use or a constrained minute, +0.5 after 2 h < {BUDGET_K_PEAK_UP:.0%})")
            return
        if tier != bk.get("tier") or float(bk.get("k_max") or 0) != float(kmax):
            k_old = float(bk["k"])
            k_new = min(k_old, float(kmax)) if kmax < float(bk.get("k_max") or kmax) else k_old   # a lower tier caps k at once; a higher one lets the +0.5 rule climb
            with c.lock:
                bk.update(k=k_new, k_max=float(kmax), tier=tier, hist=(bk.get("hist") or [])[-48:] + [dict(t=stamp(), k=k_new, why=f"k_max {kmax:g} (free {free:,.0f})")])
                c.d["budget_k"] = bk
                c.save()
            msg = f"BUDGET k_max -> {kmax:g} (free cash {free:,.0f}); k {k_old:g} -> {k_new:g}"
            self.event("CASH", msg)
            self.night.line(msg)
        k, since = float(bk["k"]), now - float(bk.get("t") or now)
        use30 = max((u for t, u in self.cap_use_hist if t >= now - BUDGET_K_DOWN_S and u is not None), default=0.0)
        use2h = max((u for t, u in self.cap_use_hist if t >= now - BUDGET_K_UP_S and u is not None), default=0.0)
        cm = self.constrained_minutes(now - BUDGET_K_DOWN_S)
        k2, why = k, None
        if since >= BUDGET_K_DOWN_S and (use30 > BUDGET_K_PEAK_DOWN or cm > 0) and k > BUDGET_K_MIN + 1e-9:
            k2, why = max(BUDGET_K_MIN, k - BUDGET_K_STEP), f"last 30 min: peak use {use30:.0%}, constrained {cm} min"
        elif since >= BUDGET_K_UP_S and use2h < BUDGET_K_PEAK_UP and k < float(bk["k_max"]) - 1e-9:
            k2, why = min(float(bk["k_max"]), k + BUDGET_K_STEP), f"2 h peak use {use2h:.0%} < {BUDGET_K_PEAK_UP:.0%}"
        if why:
            with c.lock:
                bk.update(k=k2, t=now, hist=(bk.get("hist") or [])[-48:] + [dict(t=stamp(), k=k2, why=why)])
                c.d["budget_k"] = bk
                c.save()
            msg = f"BUDGET k {k:g} -> {k2:g} ({why}); entry budget now {k2:g} x free"
            self.event("CASH", msg)
            self.night.line(msg)

    def other_entry_cost(self):
        """Resting entry bids of the other layers (fat-finger, scalper, longshot) at cost, weighted like the dump levels."""
        out = 0.0
        for o in self.open:
            rec = self.ledger.orders.get(o["id"])
            if not rec or rec.get("layer") not in ("fatfinger", "scalp", "longshot") or o["action"] != "buy":
                continue
            out += o["qty"] * o["price"] * ENTRY_WEIGHTS.get(rec.get("layer"), 1.0)   # day 8 §2 weights
        return out

    def l0_resting(self):
        """Our resting L0 bids at notional across all sides (Piero 21:20: the L0 cap counts only these)."""
        out = 0.0
        for o in self.open:
            rec = self.ledger.orders.get(o["id"])
            if rec and rec.get("layer") == "dump" and str(rec.get("sub", "")).endswith(":L0") and o["id"] not in self.ex.cancelling:
                out += o["qty"] * o["price"]
        return out

    def l0_cap_used(self):
        """What counts against DUMP_L0_BUDGET: resting L0 bids (catcher fix #1), else the held L0 tranches (before C87)."""
        return self.l0_resting() if DUMP_L0_CAP_RESTING else self.l0_held()

    def l0_held(self):
        """L0 tranches held at cost across all sides (the 8k L0 budget)."""
        return sum(abs(q) * float((self.ledger.tranches.get(sub) or {}).get("avg") or self.sig_mark(ex, "yes" if q > 0 else "no") or 0)
                   for ex, books in self.ledger.subbooks.items() for sub, q in books.items() if sub.endswith(":L0") and abs(q) >= 1)

    def race_room(self, ex, side, fs):
        """Day 7 §2.2-2.3: the room under the race cap (20k at fair >= 0.95, 15k at 0.90-0.95, 12k below) for new levels on
        (ex, side): the cap minus what every engine layer holds at cost on the race's markets and the dump levels resting
        on the race's other sides. On the Rhode Island races the engine's tranches get their own 10k: the old inventory
        (the Oct 2 RI Senate R NO book) and the manual position do not count."""
        race = (self.meta.get(ex) or {}).get("race")
        ri = race in RI_RACES
        if ri and self.controls.d.get("ri_freeze"):
            return -1.0                                     # day 10 D1: RI frozen — no new engine entries on either race
        if ri and RULES_V2 and self.ri_held_cap is not None:
            return self.ri_held_cap + RI_TURNOVER - self.ri_held_cost(resting_except=(ex, side))   # day 11 §1.4: RI unfrozen for turnover — held capped at the deploy level, +10k in flight
        senate = race == RI_SENATE_RACE and self.ri_senate_base is not None   # day 9 §9.4: +10k beyond the Oct 6 holdings, all subs count
        cap = (self.ri_senate_base + RI_SENATE_ADD) if senate else RI_ENGINE_CAP if ri else next(c for lo, c in DUMP_CONC_CAPS if fs >= lo - 1e-9)
        used = 0.0
        for e, m in self.meta.items():
            if m.get("race") != race:
                continue
            books = self.ledger.subbooks.get(e, {})
            for sub, q in books.items():
                if abs(q) < 1 or sub.startswith(("fmm:", "fmmlock:", "carry:")) or (not senate and (sub == "manual" or (ri and not sub.startswith(("dump:", "touch:", "scalp:", "sniper"))))):
                    continue                                # Piero 21:15 item 2: FMM lots are outside the RI cap
                sd = "yes" if q > 0 else "no"
                avg = (self.ledger.tranches.get(sub) or {}).get("avg") or ((self.book.get(e) or {}).get("row") or {}).get("avgCost") or self.sig_mark(e, sd) or 0.0
                used += abs(q) * float(avg)
            for o in self.open:
                rec = self.ledger.orders.get(o["id"])
                if o["ex"] == e and rec and rec.get("layer") == "dump" and not (e == ex and o["side"] == side):
                    used += o["qty"] * o["price"]
                elif o["ex"] == e and rec and rec.get("layer") == "wall" and o["action"] == "buy":   # C76: a resting wall uses the race's room
                    used += o["qty"] * o["price"]
        return cap - used

    def l0_refills(self, sub, now):
        """Fills of an L0 level in the last 10 min (the sweep-follow limit)."""
        ids = {i for i, o in self.ledger.orders.items() if o.get("sub") == sub and o.get("layer") == "dump"}
        return sum(1 for f in self.fills_since(now - DUMP_L0_WINDOW_S) if f["order"] in ids and f["action"] == "buy")

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

    def dump_levels(self, ex, side, fs, rs, cost, room=None, l0_ok=True):
        """[(i, price, shares)]: L1/L2/L3 = ref - 1 / 2 / 3.5c, each <= fair_side - 3c and <= best bid + 1 tick (join or
        improve, never cross the ask); shares by weight 40 / 35 / 25 of `cost`. Catcher fix #1 (Piero 21:20): L3 / L4 off;
        on the top-20 thin sides L1 joins the touch (its own order next to L0, <= fair_side - 3c) with L3 / L4's weight added."""
        bid_s, ask_s = bots.side_book(*self.px.get(ex, (None, None, None))[:2], side)
        out, used = [], set()
        day = self.day_window(time.time())                  # day 6 §1.3: L4 (ref - 5c) and 45/25/15/15 in the day window
        big = (ex, side) in self.big_sweep_sides(time.time())   # day 10 §4.2: 72-h median sweep >= 50k shares
        offs, ws = (DUMP_V3_OFFSETS_DAY, (DUMP_V3_WEIGHTS_DAY_BIG if big else DUMP_V3_WEIGHTS_DAY)) if day else (DUMP_V3_OFFSETS, DUMP_V3_WEIGHTS)
        clip = DUMP_BIG_CLIP if big else DUMP_L0_CLIP
        if l0_ok and fs >= DUMP_L0_MIN_FAIR - 1e-9 and bid_s is not None and bid_s <= fs - DUMP_L0_GAP + 1e-9 and MB_MIN_PX <= bid_s <= MB_MAX_PX:
            q0 = min(clip, int(room / bid_s)) if room is not None else clip   # day 7 §2.1: L0 joins the best bid (4k on big-sweep sides)
            if q0 >= MIN_QTY:
                out.append((4, bid_s, q0))
                used.add(bid_s)
        thin = (str(ex), side) in self.thin_sides()
        for i, (off, w) in enumerate(zip(offs, ws)):
            if DUMP_DEEP_OFF and i >= 2:
                continue                                    # catcher fix #1: L3 / L4 off
            touch = thin and i == 0 and bid_s is not None
            if touch:                                       # catcher fix #1: L1 at the touch of a thin side, with L3 / L4's weight
                px = min(bid_s, fs - DUMP_FAIR_GAP)
                w = w + (sum(ws[2:]) if DUMP_DEEP_OFF else 0.0)
            else:
                px = min(rs - off, fs - DUMP_FAIR_GAP)
                if bid_s is not None:
                    px = min(px, bid_s + TICK)
            if ask_s is not None:
                px = min(px, ask_s - TICK)
            px = bots.tick_down(px)
            if px < 0.01 or (px in used and not touch):
                continue
            used.add(px)
            q = int(cost * w / px)
            if big and i in (0, 1):
                q = max(q, min(DUMP_BIG_CLIP, int(room / px) if room is not None else DUMP_BIG_CLIP))   # L1/L2 clips up to 4k on big-sweep sides
            if q >= MIN_QTY:
                out.append((i, px, q))
        return out

    def vwap6(self, now):
        """{ex: trailing 6-h VWAP of SIG prints, YES terms} from the tape files (re-read every 30 min) — SIG's mark for a position."""
        c = getattr(self, "_vwap6", None)
        if c and now - c[0] < 1800:
            return c[1]
        pq, qq, cut = collections.Counter(), collections.Counter(), now - 6 * 3600
        for d in range(2):
            fn = os.path.join(DATA, "intel", "trades", time.strftime("%Y-%m-%d", time.localtime(now - 86400 * d)) + ".jsonl")
            try:
                with open(fn) as fh:
                    for line in fh:
                        try:
                            r = json.loads(line)
                        except ValueError:
                            continue
                        if (r.get("ts") or 0) >= cut and r.get("p") is not None:
                            q = float(r.get("q") or 0)
                            pq[str(r.get("ex"))] += float(r["p"]) * q; qq[str(r.get("ex"))] += q
            except OSError:
                continue
        self._vwap6 = (now, {ex: pq[ex] / qq[ex] for ex in qq if qq[ex] > 0})
        return self._vwap6[1]

    def catcher_ok(self, ex, side, level, px, fs):
        """C97 item 4a (Piero 15:55 Oct 17): the catcher's slice buys L0 / L1 catches only, each >= 3c under the 6-h VWAP mark and below PM fair."""
        if not ALLOC.get("on"):
            return True
        if level not in (4, 0) or fs is None or px >= fs - 1e-9:
            return False
        v = self.vwap6(time.time()).get(str(ex))
        if v is None:
            return False
        mark = v if side == "yes" else 1 - v
        return px <= mark - float(ALLOC["catcher_mark_c"]) + 1e-9

    def catcher_new_cost(self):
        """C97 item 4a: the catcher's lots opened since the allocation (at cost) — the older catcher lots are holdings."""
        out = 0.0
        for ex, books in self.ledger.subbooks.items():
            for sub, q in books.items():
                if not sub.startswith("dump:") or abs(q) < 1:
                    continue
                t = self.ledger.tranches.get(sub) or {}
                if str(t.get("t") or "") < ALLOC["since"]:
                    continue
                side = "yes" if q > 0 else "no"
                avg = float(t.get("avg") or 0) or float(((self.book.get(ex) or {}).get("row") or {}).get("avgCost") or 0) or float(self.sig_mark(ex, side) or 0)
                out += abs(q) * avg
        return out

    def thin_sides(self):
        """The top-20 thin sides (a control 'thin_sides' [[ex, side], ...] overrides the config list)."""
        ov = self.controls.d.get("thin_sides")
        return {(str(a), str(b)) for a, b in (ov or DUMP_THIN_SIDES)}

    def dump_plan(self, now, cash, used):
        """{ex: [want-spec]} for dump catcher v3: every eligible side, size = funded reserve / 8 (1.5k..6k, arena x 1.5,
        pilot x 0.5), concentration cap 12k per side, news guard, breaker, reserve fit (admission). A level whose
        tranche is still being sold back is not re-placed; it re-arms as soon as the sell-back fills. After a restart
        L1 goes out on every side first, then L2, then L3."""
        if not self.controls.on("dump"):
            return {}
        if self.trading_inv_gate(now):
            self.dump_cov = dict(eligible=0, covered=0, levels=0, weighted=0, budget=0, nominal=0, k=self.budget_k(), by_l={}, cut=0, inv_stop=True)
            return {}                                       # C89: trading inventory over 30k at cost: no new catcher bids
        if self.entry_budget(cash) <= 0:                    # Oct 5 22:50 guard (b): under 2k free above the floor: no entry bids
            if now - self.scalp_note.get("nocash", 0) >= 1800:
                self.scalp_note["nocash"] = now
                self.event("CASH", f"free cash {cash - CASH_RESERVE:,.0f} < {ENTRY_MIN_FREE:,}: no entry bids (dump, fat-finger, scalper)")
            return {}
        self.adjust_budget_k(now)                           # day 9 §2
        if self.ri_senate_base is None:
            self.record_ri_base()                           # day 9 §9.4 (C74): once, on the first loop with prices and positions in
        if RULES_V2 and self.ri_held_cap is None:
            self.record_ri_cap()                            # day 11 §1.4 (C81c): once, on the first priced loop after the deploy
        half = 0.5 if self.controls.state("dump") == "pilot" else 1.0
        arena = self.dump_arena(now)
        l0_ok = True if DUMP_L0_CAP_RESTING else self.l0_held() < DUMP_L0_BUDGET   # catcher fix #1: the cap counts resting L0 bids (enforced in allocate_levels)
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
        resting, rest_px, rest_q = {}, {}, {}
        for o in self.open:
            rec = self.ledger.orders.get(o["id"])
            if rec and rec.get("layer") == "dump" and o["id"] not in self.ex.cancelling:
                tail = str(rec.get("sub", "")).rsplit(":", 1)[-1]
                resting.setdefault((o["ex"], o["side"]), set()).add(4 if tail == "L0" else int(tail) if tail.isdigit() else 0)
                rest_px[rec.get("sub")] = o["price"]
                rest_q[rec.get("sub")] = o["qty"]
        plan, rows = {}, []
        for ex, side, fs, rs in cands:
            key = (ex, side)
            if self.dump_block.get(key, 0) > now or self.breaker.get(("dump", self.pol(ex, side)), 0) > now or self.ex.is_frozen(ex):
                continue
            if party.get(self.pol(ex, side), 0.0) >= DUMP_PARTY_CAP:
                continue                                    # that party side is at its 40k exposure cap: no new levels
            room = self.race_room(ex, side, fs)                             # day 7 §2.2: cap per race, position + resting, all engine layers
            cost = min(per_side * (DUMP_ARENA_MULT if ex in arena else 1.0), DUMP_ARENA_MAX if ex in arena else DUMP_SIDE_HARD_MAX,
                       funded / 2) * half
            cost = min(cost, room)
            if cost < DUMP_MIN_SIDE_COST:
                continue
            sliding = self.dump_ref_v3(ex)[1]
            books = self.ledger.subbooks.get(ex, {})
            best_s = bots.side_book(*self.px.get(ex, (None, None, None))[:2], side)[0]
            for i, px, q in self.dump_levels(ex, side, fs, rs, cost, room=room, l0_ok=l0_ok):
                if self.ri_edge_short(ex, px, fs):
                    continue                                # day 9 §9.4 / day 11 §1.4: on the RI races only levels >= 15c under fair
                if not self.catcher_ok(ex, side, i, px, fs):
                    continue                                # C97 item 4a: L0 / L1 only, >= 3c under the 6-h VWAP mark and below PM fair
                sub = f"dump:{ex}:{side}:{'L0' if i == 4 else i}"
                if abs(books.get(sub, 0)) >= MIN_QTY:
                    if i != 4 or self.l0_refills(sub, now) >= DUMP_L0_REFILLS + 1 or (str(ex), sub) in self.core_set:   # the first fill + 3 refills per 10 min
                        continue                            # its tranche is being sold back: re-arms when that fills (L0: 3 refills / 10 min); C88c: never into a core sub (the 50k cap)
                if sliding and sub in rest_px and px < rest_px[sub] - 1e-9:
                    px = rest_px[sub]                       # a slide in progress: never lower a resting level
                    q = int(cost * DUMP_V3_WEIGHTS[i] / px)
                if sub in rest_q and 0.5 * rest_q[sub] <= q <= 2.0 * rest_q[sub]:
                    q = int(rest_q[sub])                    # C72: quantity hysteresis — a resting level keeps its size within x0.5..x2 (no re-size churn)
                near = i == 4 or (best_s is not None and px >= best_s - DUMP_NEAR_GAP - 1e-9)
                at_touch = i == 0 and best_s is not None and px >= best_s - 1e-9 and (str(ex), side) in self.thin_sides()
                rows.append((i, i in resting.get(key, set()), ex, dict(side=side, px=px, qty=q, sub=sub, level=i, edge=fs - px, arena=ex in arena, l0=i == 4, touch=at_touch,
                                                                        ttl=DUMP_NEAR_TTL_S if near else DUMP_TTL_S,
                                                                        weight=ENTRY_WEIGHTS["L0" if i == 4 else f"L{i + 1}"], lname="L0" if i == 4 else f"L{i + 1}",
                                                                        px_rest=rest_px.get(sub))))   # day 8 §2 / C72: the resting price, if any
        # day 7 §1.3 make-before-break: a level due for its refresh is placed again first, then the old one is cancelled
        # (resting orders lock no cash; the overlap is seconds). A side is never left without its level.
        wanted = {w["sub"]: (ex, w) for _, _, ex, w in rows}
        for o in self.open:
            rec = self.ledger.orders.get(o["id"])
            if not rec or rec.get("layer") != "dump" or o["id"] in self.ex.cancelling or o["id"] in self.refreshed:
                continue
            age = now - (rec.get("ts") or now)
            ttl = rec.get("ttl") or DUMP_TTL_S
            jitter = ((hash(rec.get("sub", "")) % (2 * DUMP_REFRESH_JITTER_S)) - DUMP_REFRESH_JITTER_S) * (ttl / DUMP_TTL_S)
            w = wanted.get(rec.get("sub"))
            due = age >= 0.75 * ttl + jitter
            reprice = (w is not None and not w[1]["l0"] and not (w[1]["px"] - DUMP_WANT_BAND_LO - 1e-9 <= o["price"] <= w[1]["px"] + DUMP_WANT_BAND_HI + 1e-9))
            reprice = reprice or (w is not None and w[1]["l0"] and not (w[1]["px"] - 0.005 - 1e-9 <= o["price"] <= w[1]["px"] + 1e-9))
            if not (due or reprice):
                continue
            if api.writes_left() < DUMP_WRITES_IDLE:        # C72: no writes to spare: the old level stays (the want keeps its price)
                if reprice:
                    w[1]["px"] = o["price"]
                continue
            if w is not None:                               # day 9 §4: a re-price is also make-before-break (place first, then cancel)
                ex_, spec = w
                self.ex.place([(self.owner[ex_].name, spec["sub"], "dump", (ex_, spec["side"], "buy", spec["px"], int(spec["qty"])))], ttl_s=spec.get("ttl") or DUMP_TTL_S)
            self.refreshed[o["id"]] = now
            self.dump_cut_t[(o["ex"], o["side"])] = (now, "refresh (make-before-break)" if due else "re-price (make-before-break)")
            self.ex.cancel(o, reason="refresh (make-before-break)" if due else "re-price (make-before-break)")
        if len(self.refreshed) > 3000:
            self.refreshed = {k: v for k, v in self.refreshed.items() if v >= now - 7200}
        # Day 9 §2-§3: the budget is k x free cash above the floor (minus Piero's walls); levels are allocated resting-first
        # (hysteresis: a resting level loses its slot only when the resting set alone no longer fits), L0 first within its
        # 8k cap (held + resting), then by expected edge = (fair - price) x reach / weight.
        budget = max(0.0, self.entry_budget(cash) - self.wall_resting() - self.fm_reserve() - self.tm_reserve() - self.sm_reserve() - self.reservoir_resting() - self.h1_sleeve_reserve(now) - self.carry_bid_notional())   # C93 item 4: the H1 sleeve ahead of the catcher; C95: the rotation's cash and the manual reserve are already off `cash`   # C84c: the reservoir's rebuys come first   # C70: Piero's walls, (C81b) the FM's cap and (C83) the TM's cap come off the top (weight 1.0)
        if ALLOC.get("on"):
            budget = min(budget, max(0.0, float(ALLOC["catcher_slice"]) - self.catcher_new_cost()))   # C97 item 4a: a fixed 10k slice (new lots at cost + resting levels)
        out, cut, spent = self.allocate_levels(rows, budget, float(DUMP_L0_BUDGET) if DUMP_L0_CAP_RESTING else max(0.0, DUMP_L0_BUDGET - self.l0_held()))   # fix #1: the L0 rows themselves (resting + new) fit the cap
        self.entry_spent = round(spent)
        self.entry_left = max(0.0, budget - spent)
        for lvl, is_resting, ex, w in out:
            plan.setdefault(ex, []).append(w)
        self.dump_cut_rows = [(w["edge"], w["px"] * w["qty"], w["px"]) for _, _, _, w in cut]   # Piero 22:15 (C82): the rotation funds cut entries with >= 8c edge
        for lvl, is_resting, ex, w in cut:
            if is_resting:
                self.dump_cut_t[(ex, w["side"])] = (now, "cut for budget")
        elig = {(ex, side) for ex, side, _, _ in cands}
        funded = {(ex, w["side"]) for _, _, ex, w in out}
        sides = funded | (set(resting) & elig)
        by_l = collections.Counter(w["lname"] for _, _, _, w in out)
        self.dump_cov = dict(eligible=len(elig), covered=len(sides & elig), levels=len(out), weighted=round(spent), budget=round(budget),
                             nominal=round(sum(w["px"] * w["qty"] for _, _, _, w in out)), k=self.budget_k(),
                             by_l={k_: by_l.get(k_, 0) for k_ in ("L0", "L1", "L2", "L3", "L4")}, cut=len(cut))   # day 8 §2 / day 9 §3
        # Day 9 §4: a FUNDED side without a resting level (and nothing queued) for > 30 s is a gap: logged with its cause
        covered = set(resting) | {(p["ex"], p["side"]) for p in self.ex.pending() if p.get("layer") == "dump"}
        for key in list(self.dump_gap_since):
            if key not in funded or key in covered:
                self.dump_gap_since.pop(key, None)
        for key in funded:
            if key in covered:
                continue
            t0 = self.dump_gap_since.setdefault(key, now)
            if now - t0 > DUMP_GAP_LOG_S and now - self.dump_gap_log.get(key, 0) > 600:
                self.dump_gap_log[key] = now
                ex, side = key
                cut_t, why = self.dump_cut_t.get(key, (0, None))
                if cut_t < now - 600:
                    why = None
                if why is None:
                    cut_off = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(now - 600))
                    last = max((r for r in self.ledger.orders.values() if r.get("layer") == "dump" and r["ex"] == ex and r["side"] == side
                                and (r.get("closed") or "") >= cut_off), key=lambda r: r["closed"], default=None)
                    why = ({"done": "filled: its tranche is selling back", "expired": "TTL expired without a replacement",
                            "cancelled": "cancelled (plan change)"}.get((last or {}).get("status"), "placement lag (queued / write pacing)"))
                self.uncovered_n.append((now, ex, side, round(now - t0)))
                self.event("DUMP", f"{self.name(ex)} {side.upper()}: funded but no level resting for {now - t0:.0f} s: {why}")
        return plan

    def ri_held_cost(self, resting_except=None):
        """Day 11 §1.4: everything held on the two RI races at cost, every sub-book but manual (walls, swing, inventory, tranches),
        plus the engine's resting RI entry bids (dump / wall buys) except the level being planned."""
        used = 0.0
        for e, m in self.meta.items():
            if m.get("race") not in RI_RACES:
                continue
            for sub, q in self.ledger.subbooks.get(e, {}).items():
                if abs(q) < 1 or sub == "manual" or sub.startswith(("fmm:", "fmmlock:", "carry:")):
                    continue                                # Piero 21:15 item 2: FMM lots are outside the RI cap
                sd = "yes" if q > 0 else "no"
                avg = (self.ledger.tranches.get(sub) or {}).get("avg") or ((self.book.get(e) or {}).get("row") or {}).get("avgCost") or self.sig_mark(e, sd) or 0.0
                used += abs(q) * float(avg)
            for o in self.open:
                rec = self.ledger.orders.get(o["id"])
                if o["ex"] != e or not rec or o["action"] != "buy" or o["id"] in self.ex.cancelling:
                    continue
                if rec.get("layer") == "dump" and (e, o["side"]) != resting_except or rec.get("layer") in ("wall", "touch", "scalp"):
                    used += o["qty"] * o["price"]
        return used

    def record_ri_cap(self):
        """Day 11 §1.4 (C81c): record RI held at cost at the deploy (both races, every sub-book but manual) once every held sub
        has a price; from then on RI held is capped there and the engine may add RI_TURNOVER in flight on levels >= 15c under fair."""
        base, ok = 0.0, True
        for e, m in self.meta.items():
            if m.get("race") not in RI_RACES:
                continue
            for sub, q in self.ledger.subbooks.get(e, {}).items():
                if abs(q) < 1 or sub == "manual":
                    continue
                sd = "yes" if q > 0 else "no"
                avg = (self.ledger.tranches.get(sub) or {}).get("avg") or ((self.book.get(e) or {}).get("row") or {}).get("avgCost") or self.sig_mark(e, sd)
                if not avg:
                    ok = False
                    break
                base += abs(q) * float(avg)
            if not ok:
                break
        if not ok or not self.book:
            return
        c = self.controls
        with c.lock:
            c.d.setdefault("day11", {})["ri_held_cap"] = round(base, 2)
            c.save()
        self.ri_held_cap = base
        msg = (f"DAY 11 §1.4: RI held at cost {base:,.0f} recorded as the cap (both RI races, every sub-book but manual); the catcher, L0 and the touch maker may "
               f"trade RI again on levels >= {RI_MIN_EDGE * 100:.0f}c under fair with up to {RI_TURNOVER:,} in flight; any RI lot above the cap is recycled by the ladder, never held")
        self.event("DUMP", msg)
        self.night.line(msg)

    def ri_edge_short(self, ex, px, fs):
        """True when (ex, px) is an RI level under the 15c rule: RI Senate once its base is recorded (day 9 §9.4); both RI races under rules v2."""
        race = (self.meta.get(ex) or {}).get("race")
        if RULES_V2 and race in RI_RACES:
            return px > fs - RI_MIN_EDGE + 1e-9
        return self.ri_senate_base is not None and race == RI_SENATE_RACE and px > fs - RI_SENATE_MIN_EDGE + 1e-9

    def record_ri_base(self):
        """Day 9 §9.4 (C74): record the RI Senate holdings at cost (every sub-book) once every held sub has a price; until
        then the race keeps the day-7 10k engine cap and the 15c filter does not apply."""
        base, ok = 0.0, True
        for e, m in self.meta.items():
            if m.get("race") != RI_SENATE_RACE:
                continue
            for sub, q in self.ledger.subbooks.get(e, {}).items():
                if abs(q) < 1:
                    continue
                sd = "yes" if q > 0 else "no"
                avg = (self.ledger.tranches.get(sub) or {}).get("avg") or ((self.book.get(e) or {}).get("row") or {}).get("avgCost") or self.sig_mark(e, sd)
                if not avg:
                    ok = False
                    break
                base += abs(q) * float(avg)
            if not ok:
                break
        if not ok or not self.book:
            return
        c = self.controls
        with c.lock:
            c.d.setdefault("day9", {})["ri_senate_base"] = round(base, 2)
            c.save()
        self.ri_senate_base = base
        msg = f"DAY 9 §9.4: RI Senate holdings at cost {base:,.0f} recorded; the engine may add up to {RI_SENATE_ADD:,} beyond that, on levels >= {RI_SENATE_MIN_EDGE * 100:.0f}c under fair"
        self.event("DUMP", msg)
        self.night.line(msg)

    # ------------------------------------------------------------------------------------------------ day 9 sleeve (settlement holds)
    def sleeve_targets(self):
        return self.controls.d.get("sleeve_targets") or SLEEVE_TARGETS

    def sleeve_held(self, ex=None, side=None):
        """Sleeve holdings at cost (all, or one target's)."""
        out = 0.0
        for e, books in self.ledger.subbooks.items():
            for sub, q in books.items():
                if not sub.startswith("sleeve:") or abs(q) < 1:
                    continue
                sd = "yes" if q > 0 else "no"
                if ex is not None and (e != ex or sd != side):
                    continue
                avg = (self.ledger.tranches.get(sub) or {}).get("avg") or self.sig_mark(e, sd) or 0.0
                out += abs(q) * float(avg)
        return out

    def sleeve_cost(self):
        return self.sleeve_held()

    def sleeve_plan(self, now, cash):
        """{ex: [want-spec]}: one limit clip per target when the touch is at or under the limit, our side's Polymarket fair
        >= 0.98 and the edge >= 10c; clips <= 1,500 shares, >= 2 min apart, never past the limit; only cash above 20k free
        (the catcher keeps its k x free budget on >= 20k); 10k at cost in total and per-target caps; party and race caps;
        a 2c / 10 min fall of our side's fair pauses new clips 30 min; held to settlement (rule 0: never sold at a loss)."""
        c = self.controls
        if not c.on("sleeve") or c.paused or self.fills_blind:
            return {}
        free = cash - CASH_RESERVE
        held_all = self.sleeve_held()
        plan, pending = {}, 0.0
        for o in self.open:                                 # a clip in flight stays wanted until it fills or its TTL ends
            rec = self.ledger.orders.get(o["id"])
            if rec and rec.get("layer") == "sleeve" and o["id"] not in self.ex.cancelling:
                plan.setdefault(o["ex"], []).append(dict(side=o["side"], px=o["price"], qty=int(o["qty"]), sub=rec.get("sub"), ttl=SLEEVE_TTL_S))
                pending += o["qty"] * o["price"]
        pending += sum((p.get("qty") or 0) * (p.get("price") or 0) for p in self.ex.pending() if p.get("layer") == "sleeve")
        for t in self.sleeve_targets():
            ex, side, limit = str(t["ex"]), t["side"], float(t["limit"])
            key = (ex, side)
            if ex not in self.owner or self.ex.is_frozen(ex) or pending > 0:
                continue                                        # one clip in flight at a time
            fs = self.side_fair(ex, side)
            if fs is None:
                continue
            h = [x for x in self.fair_hist.get(ex, []) if x[0] >= now - SLEEVE_NEWS_WINDOW_S]
            f = self.fair.get(ex)
            if f is not None and (not h or now - h[-1][0] >= 5):
                h.append((now, f))
                self.fair_hist[ex] = h
            top = max(((v if side == "yes" else 1 - v) for _, v in h), default=fs)
            if top - fs >= SLEEVE_NEWS_C - 1e-9:
                if self.sleeve_block.get(key, 0) < now:
                    self.event("SLEEVE", f"SLEEVE {t.get('name', ex)}: fair fell {(top - fs) * 100:.1f}c in 10 min: clips paused 30 min")
                self.sleeve_block[key] = now + SLEEVE_NEWS_HOLD_S
            if self.sleeve_block.get(key, 0) > now or now - self.sleeve_last.get(key, 0) < SLEEVE_GAP_S:
                continue
            ask = bots.side_book(*self.px.get(ex, (None, None, None))[:2], side)[1]
            if ask is None or ask > limit + 1e-9:
                continue                                        # take only: the touch must be at or under the limit
            if fs < SLEEVE_MIN_FAIR - 1e-9 or fs - ask < SLEEVE_MIN_EDGE - 1e-9:
                continue
            room = min(float(t["cap"]) - self.sleeve_held(ex, side), SLEEVE_CAP - held_all, free - SLEEVE_MIN_FREE,
                       self.race_room(ex, side, fs), DUMP_PARTY_CAP - dash4.party_exposure(self).get(self.pol(ex, side), 0.0))
            q = int(min(SLEEVE_CLIP, room / ask))
            if q < MIN_QTY:
                continue
            if any(o["ex"] == ex and jx.reduces(o["side"], o["action"], self.book.get(ex, {}).get("pos", 0)) and o["side"] == side and o["action"] == "sell" and o["price"] <= ask + 1e-9
                   for o in self.open):
                continue                                        # never cross our own resting sell on that side (I4)
            self.sleeve_last[key] = now
            sub = f"sleeve:{ex}:{side}"
            plan.setdefault(ex, []).append(dict(side=side, px=ask, qty=q, sub=sub, ttl=SLEEVE_TTL_S))
            msg = f"SLEEVE clip {t.get('name', self.name(ex))}: buy {q:,} {side.upper()} at {ask:.3f} (limit {limit:.3f}, fair {fs:.3f}, edge {(fs - ask) * 100:.1f}c; held {held_all:,.0f} of {SLEEVE_CAP:,}, free {free:,.0f})"
            self.event("SLEEVE", msg)
            self.alerts.append(f"{time.strftime('%H:%M')} {msg}")
            self.night.line(msg)
            pending += q * ask
        return plan

    def sleeve_fill_events(self, fills):
        for f in fills:
            if f["layer"] != "sleeve":
                continue
            sub = f"sleeve:{f['ex']}:{f['side']}"
            t = self.ledger.tranches.setdefault(sub, dict(ex=f["ex"], side=f["side"], kind="sleeve", t=stamp(), ts=time.time(), state="held", avg=f["price"]))
            avg = self.ledger.tranche_avg(sub, "sleeve", ex=f["ex"], side=f["side"], since="2026-10-06 00:00:00") or f["price"]
            t["avg"] = round(avg, 5)
            fs = self.side_fair(f["ex"], f["side"])
            msg = (f"SLEEVE FILL {self.name(f['ex'])}: bought {f['qty']:,.0f} {f['side'].upper()} at {f['price']:.3f}"
                   + ("" if fs is None else f" (fair {fs:.3f}, EV at settlement {(fs - f['price']) * f['qty']:+,.0f})") + f"; sleeve held {self.sleeve_held():,.0f} at cost")
            self.event("SLEEVE", msg)
            self.alerts.append(f"{time.strftime('%H:%M')} {msg}")
            self.night.line(msg)

    def sleeve_view(self):
        """Fills, average price and EV at settlement per target (the report)."""
        out = []
        for t in self.sleeve_targets():
            ex, side = str(t["ex"]), t["side"]
            q = self.ledger.subbooks.get(ex, {}).get(f"sleeve:{ex}:{side}", 0.0)
            avg = (self.ledger.tranches.get(f"sleeve:{ex}:{side}") or {}).get("avg")
            fs = self.side_fair(ex, side)
            out.append(dict(name=t.get("name", self.name(ex)), ex=ex, side=side, limit=t["limit"], cap=t["cap"], shares=abs(q), avg=avg, cost=round(abs(q) * (avg or 0), 2),
                            fair=fs, ev=None if (fs is None or avg is None) else round(abs(q) * (fs - avg), 2)))
        return out

    def allocate_levels(self, rows, budget, l0_room):
        """Day 9 §3 allocation: -> (out, cut, spent). rows = (lvl, is_resting, ex, spec). Resting levels first (hysteresis),
        then new ones; within each group L0 first (its cost capped by l0_room), then by expected edge (fair - price) x reach /
        weight (+0.5c for arena markets). A level that does not fit is cut (a resting one gets cancelled by the reconcile)."""
        def xe(w):                                          # catcher fix #1: an L1 at the touch of a thin side reaches like L0
            return w["edge"] * DUMP_REACH["L0" if w.get("touch") else w["lname"]] / w["weight"] + (0.005 if w.get("arena") else 0.0)
        # C72 (§3 target >= 60 sides): among NEW levels, the best level of a side that has no level yet goes before any
        # second level of a side that already has one
        has = {(ex, w["side"]) for _, is_resting, ex, w in rows if is_resting}
        best_new = {}
        for lvl, is_resting, ex, w in rows:
            if not is_resting and (ex, w["side"]) not in has:
                k_ = (ex, w["side"])
                if k_ not in best_new or xe(w) > xe(best_new[k_]):
                    best_new[k_] = w
        first = {id(w) for w in best_new.values()}
        ring = self.ring_window(time.time())
        order = sorted(rows, key=lambda r: (not r[1], not r[3]["l0"], ring and r[3]["lname"] not in ("L0", "L1"), id(r[3]) not in first, -xe(r[3])))   # (1c) L0 / L1 first in a ring window
        out, cut, spent, l0_spent, deep_spent = [], [], 0.0, 0.0, 0.0
        deep_room = L3_BUDGET_SHARE * budget if RULES_V2 else float("inf")   # day 11 §1.5: L3 / L4 levels take at most 20% of the weighted budget
        for lvl, is_resting, ex, w in order:
            c_ = w["px"] * w["qty"] * w["weight"]
            deep = w["lname"] in ("L3", "L4")
            if w["l0"] and l0_spent + w["px"] * w["qty"] > l0_room + 1e-6:
                cut.append((lvl, is_resting, ex, w))
                continue
            if deep and deep_spent + c_ > deep_room + 1e-6:
                cut.append((lvl, is_resting, ex, w))
                continue
            if spent + c_ <= budget + 1e-6:
                out.append((lvl, is_resting, ex, w))
                spent += c_
                if w["l0"]:
                    l0_spent += w["px"] * w["qty"]
                if deep:
                    deep_spent += c_
            else:
                cut.append((lvl, is_resting, ex, w))
        return out, cut, spent

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
        live = []
        for sub, t in tr.items():
            if t.get("kind") != "dump" or t["state"] != "open":
                continue
            ex = t["ex"]
            q = self.ledger.subbooks.get(ex, {}).get(sub, 0)
            if abs(q) < 1:
                t.update(state="closed", closed_ts=now, hold_min=round((now - t.get("ts", now)) / 60, 1))
                t.pop("held", None)
                self.event("DUMP", f"{self.name(ex)}: tranche {sub} sold back after {t['hold_min']:.0f} min (round trip done)")
                for o in self.open:                         # its exit must not keep filling
                    rec = self.ledger.orders.get(o["id"])
                    if rec and rec.get("sub") == sub and rec.get("layer") == "dumpexit" and o["id"] not in self.ex.cancelling:
                        self.ex.cancel(o, reason="tranche done: exit no longer owns shares", front=True)
                continue
            since = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(t.get("ts", now) - 300))
            avg = self.ledger.tranche_avg(sub, "dump", ex=ex, side=t["side"], since=since) or t.get("avg")
            if avg is None:
                continue
            t["avg"] = round(avg, 5)
            live.append((sub, t, ex, q, avg, self.side_fair(ex, t["side"])))
        held = self.hold_when_idle(live, now) if RULES_V2 else set()
        for sub, t, ex, q, avg, fs in live:
            if sub in held or self.release_resting(ex, sub):   # C90: a release ask already covers the lot -> no second exit (the I2 trims of Oct 10-11)
                if not t.get("held"):
                    t["held"] = now
                    self.event("DUMP", f"{self.name(ex)}: {sub} {abs(q):,.0f} {t['side'].upper()} at {avg:.3f} is {(fs - avg) * 100:.1f}c under fair and cash is idle: held, not offered (day 11 §1.2)")
                continue                                    # held: no sell-back on the ladder (and no ageing to inventory while held)
            if t.get("held"):
                t["held_s"] = float(t.get("held_s") or 0.0) + max(0.0, now - float(t.pop("held")))   # the held time does not count towards the 6-h ageing or the ladder steps
                self.event("DUMP", f"{self.name(ex)}: {sub} back on the ladder (free cash under BUFFER or the lot no longer >= {HOLD_IDLE_GAP * 100:.0f}c under fair)")
            age = now - t.get("ts", now) - float(t.get("held_s") or 0.0)
            if age > DUMP_HOLD_S:
                self.ledger.move_sub(ex, sub, "inventory")
                t["state"] = "aged"
                self.event("DUMP", f"{self.name(ex)}: sell-back for {sub} not filled in 6 h: {abs(q):,.0f} shares to inventory")
                continue
            pre = t.get("pre_ref_side")
            r_now = self.ref(ex)
            ref_side = None if r_now is None else (r_now if t["side"] == "yes" else 1 - r_now)
            # day 9 §9.1 (Piero, C73): first ask at fill + 1c, one tick lower every 10 min, floor at entry + 0.5c (rule 0);
            # the same ladder for every dump tranche (L0 included); never above fair_side - 0.5c (the A2 exception), never under the floor
            floor_ = avg + RULE0_MARGIN
            cyc = (self.reservoirs().get(ex) or {}).get("catcher_cycle_c")
            if cyc:
                floor_ = avg + float(cyc)                   # Piero 17:40: catcher lots on RI are offered back at >= entry + 2c (a cycle, not a hold)
            pxs = self.px.get(ex)
            ask_s = bots.side_book(pxs[0], pxs[1], t["side"])[1] if pxs and pxs[0] is not None and pxs[1] is not None else None
            if ask_s is not None:                           # C88 (Piero 01:55): a trading lot under 3c of edge exits at max(entry + 0.5c, the touch)
                target = max(floor_, ask_s)
            else:
                target = max(floor_, avg + DUMP_LADDER_FIRST - DUMP_LADDER_TICK * int(age // DUMP_LADDER_STEP_S))
            rebound = bots.tick_up(round(max(target, floor_), 6))
            self.dump_view.setdefault(ex, []).append(dict(sub=sub, rebound=min(rebound, 0.99), avg=avg))

    def hold_when_idle(self, live, now, merge=False):
        """C88 (Piero 01:55): the set of tranche subs held off the exits — CORE lots (held to settlement) and TRADING lots still
        >= 3c under fair (no +0.5c exits on them). Under 3c a trading lot exits at max(entry + 0.5c, the touch). An RI lot above
        RI's cap is never held; a cycling reservoir market's catcher lots cycle."""
        held = {}
        for sub, t, ex, q, avg, fs in live:
            if (self.meta.get(ex) or {}).get("race") in RI_RACES and (self.ri_held_cap is None or self.ri_held_cost() > self.ri_held_cap + 1):
                continue                                    # §1.4: RI above its cap is never held
            side = t.get("side") if isinstance(t, dict) and t.get("side") else ("yes" if q > 0 else "no")
            if self.lot_policy(ex, side, sub, avg, fs) in ("core", "hold"):
                held[sub] = (fs - avg) if (fs is not None and avg is not None) else 0.0
        if merge:
            self.held_idle.update(held)
        else:
            self.held_idle = held
        return set(held)

    # ------------------------------------------------------------------------------------------------ C88: the two books (Piero 01:55 Oct 9)
    def lot_policy(self, ex, side, sub, avg, fs):
        """'core' (held to settlement), 'hold' (a trading lot >= 3c under fair: no +0.5c exit) or 'exit' (max(entry + 0.5c, touch))."""
        ex = str(ex)
        if (ex, sub) in self.core_set:
            return "core"
        if f"{ex}|{sub}" in ((self.controls.d.get("core") or {}).get("swap_out") or {}):
            return "exit"                                   # a core lot swapped out or within 1c of fair: sold at the touch, never below entry + 0.5c
        if (self.reservoirs().get(ex) or {}).get("catcher_cycle_c"):
            return "exit"                                   # Piero 17:40: RI catcher lots cycle at entry + 2c
        if fs is not None and avg is not None and fs - float(avg) >= TRADING_HOLD_C - 1e-9:
            return "hold"
        return "exit"

    def poll_close_early(self):
        """{ex} whose first poll close is by CORE_EARLY_CLOSE_PT (data/election/poll_close.json), cached."""
        if getattr(self, "_early_close", None) is None:
            out = set()
            try:
                with open(os.path.join(DATA, "election", "poll_close.json")) as fh:
                    for m in json.load(fh).get("markets") or []:
                        if str(m.get("first_close_pt") or "99:99") <= CORE_EARLY_CLOSE_PT:
                            out.add(str(m.get("ex")))
            except Exception:
                pass
            self._early_close = out
        return self._early_close

    def engine_lots(self):
        """Every engine-owned lot: [(ex, side, sub, qty, avg, fs, cost)] — tranche subs and 'inventory' per market; never manual,
        wall, swing, sleeve or pair-set lots, never the RI races, a reservoir market or a manual market."""
        out = []
        for ex, books in list(self.ledger.subbooks.items()):
            ex = str(ex)
            if ex not in self.owner or (self.meta.get(ex) or {}).get("race") in RI_RACES or ex in self.reservoirs() or ex in self.manual_ex:
                continue
            row = (self.book.get(ex) or {}).get("row") or {}
            for sub, q in list(books.items()):
                if abs(q) < MIN_QTY or sub.startswith(CORE_SKIP_SUBS):
                    continue
                side = "yes" if q > 0 else "no"
                avg = (self.ledger.tranches.get(sub) or {}).get("avg")
                if not avg:
                    lots = [l for l in self.lots(ex, side, sub) if l[0] > 0]
                    tot = sum(l[0] for l in lots)
                    avg = (sum(l[0] * l[1] for l in lots) / tot) if tot else float(row.get("avgCost") or 0)
                if not avg:
                    continue
                out.append((ex, side, sub, float(abs(q)), float(avg), self.side_fair(ex, side), float(abs(q)) * float(avg)))
        return out

    def core_update(self, now):
        """Every minute: CORE = the highest edge-% lots vs fair (edge % = (fair - entry) / entry; +0.5 pp for races whose polls close by
        17:00 PT), filled to 50k at cost from lots >= 3c under fair, sticky: a core lot leaves only when within 1c of fair or swapped
        (one swap per minute) for a lot with >= 3c more edge; the lot that leaves is sold at the touch, never below entry + 0.5c."""
        c = self.controls
        if now - self.core_t < CORE_EVERY_S or not self.fair:
            return
        self.core_t = now
        core = c.d.get("core") or {}
        lots = {f"{ex}|{sub}": (ex, side, sub, q, avg, fs, cost) for ex, side, sub, q, avg, fs, cost in self.engine_lots()}
        members = {k: dict(v) for k, v in (core.get("lots") or {}).items()}
        swap_out = dict(core.get("swap_out") or {})
        sold = list(core.get("sold") or [])[-200:]
        changes = []
        for k in list(members):
            l = lots.get(k)
            if l is None:
                sold.append(dict(k=k, t=stamp(), **{x: members[k].get(x) for x in ("ex", "sub", "qty", "avg")}))
                members.pop(k); swap_out.pop(k, None)
                changes.append(f"{k} left core (sold or gone)")
                continue
            ex, side, sub, q, avg, fs, cost = l
            members[k].update(qty=q, avg=avg, fs=fs, cost=cost, edge_c=None if fs is None else round(fs - avg, 4))
            if fs is not None and fs - avg < CORE_NEAR_FAIR_C - 1e-9:
                members.pop(k); swap_out[k] = dict(t=now, why="within 1c of fair")
                changes.append(f"{self.name(ex)} {sub} leaves core: {(fs - avg) * 100:.1f}c from fair -> exit at max(entry + 0.5c, touch)")
                continue
            ask_s = bots.side_book(*self.px.get(ex, (None, None, None))[:2], side)[1] if self.px.get(ex) else None
            if fs is not None and ask_s and ask_s > avg + RULE0_MARGIN and (fs - ask_s) / ask_s < CORE_LOCK_RET - 1e-9:   # C89 (Piero 16:55 item 2): lock the gain
                members.pop(k); swap_out[k] = dict(t=now, why=f"gain lock: {(fs - ask_s) / ask_s:.1%} return left at the touch {ask_s:.3f}")
                changes.append(f"CORE LOCK {self.name(ex)} {sub} {q:,.0f} {side.upper()}: bought {avg:.3f}, touch {ask_s:.3f}, fair {fs:.3f} ({(fs - ask_s) / ask_s:.1%} return left < {CORE_LOCK_RET:.0%}) -> sold at the touch; its slot goes to the next candidate")
        for k in list(swap_out):
            if k not in lots:
                swap_out.pop(k)
        used = sum(float(m.get("cost") or 0) for m in members.values())
        early = self.poll_close_early()
        score = lambda l: (l[5] - l[4]) / l[4] + (CORE_EARLY_BONUS if l[0] in early else 0.0)
        cands = sorted((l for k, l in lots.items() if k not in members and k not in swap_out and l[5] is not None and l[5] - l[4] >= TRADING_HOLD_C - 1e-9),
                       key=score, reverse=True)
        swaps = 0
        for l in cands:
            ex, side, sub, q, avg, fs, cost = l
            k = f"{ex}|{sub}"
            m = dict(ex=ex, side=side, sub=sub, qty=q, avg=avg, fs=fs, cost=cost, edge_c=round(fs - avg, 4), edge_pct=round((fs - avg) / avg, 4), t=now, early=ex in early)
            if used + cost <= CORE_CAP_COST + 1e-6:
                members[k] = m; used += cost
                changes.append(f"{self.name(ex)} {sub} {q:,.0f} {side.upper()} at {avg:.3f} joins core ({(fs - avg) * 100:.1f}c = {(fs - avg) / avg:.1%}{', early close' if ex in early else ''})")
                continue
            if swaps >= 1 or not members:
                continue
            wk = min(members, key=lambda kk: members[kk].get("edge_c") if members[kk].get("edge_c") is not None else 9.0)
            w = members[wk]
            if w.get("edge_c") is not None and fs - avg >= float(w["edge_c"]) + CORE_SWAP_C - 1e-9 and used - float(w["cost"]) + cost <= CORE_CAP_COST + 1e-6:   # C88b: the cap is exact
                members.pop(wk); swap_out[wk] = dict(t=now, why=f"swapped for {k}")
                members[k] = m; used += cost - float(w["cost"]); swaps += 1
                changes.append(f"SWAP: {self.name(w['ex'])} {w['sub']} ({float(w['edge_c']) * 100:.1f}c) out at the touch, never below entry + 0.5c, for {self.name(ex)} {sub} ({(fs - avg) * 100:.1f}c)")
        self.core_set = {(str(m["ex"]), m["sub"]) for m in members.values()}
        with c.lock:
            c.d["core"] = dict(lots=members, swap_out=swap_out, sold=sold, cost=round(used, 2), cap=CORE_CAP_COST, t=stamp())
            c.save()
        if changes:
            if len(changes) > 8:
                msg = f"CORE: {len(changes)} changes — {len(members)} lots, {used:,.0f} of {CORE_CAP_COST:,} at cost; " + "; ".join(changes[:4]) + " …"
                self.event("CORE", msg); self.night.line(msg)
            else:
                for ch in changes:
                    self.event("CORE", ch); self.night.line("CORE " + ch)

    # ------------------------------------------------------------------------------------------------ C90: docs/edge_program.md
    def edge_state(self, h):
        return (self.controls.d.get("edge") or {}).get(h) or {}

    def edge_arm(self, h, ex, layer=None):
        """'A' (the hypothesis) or 'B' (control) for a market: arm A = half the universe by a stable hash while the hypothesis is in
        its A/B, every market once promoted, none once rolled back / off. H1's spread-maker quotes use H5's ranked universe instead
        (controls edge.H1.sm_universe / sm_arm_a: the top 10 by time-to-front, split by rank parity); SM markets outside it stay control."""
        st_d = self.edge_state(h)
        st = st_d.get("state", "off")
        if st == "full":
            return "A"
        if st != "ab":
            return "B"
        if h == "H1" and layer == "sm" and st_d.get("sm_universe"):
            return "A" if str(ex) in set(map(str, st_d.get("sm_arm_a") or [])) else "B"
        return "A" if int(hashlib.md5(str(ex).encode()).hexdigest(), 16) % 2 == 0 else "B"

    def h1_in_study(self, ex, layer):
        """A quote counts in H1's A/B stats: TM quotes everywhere; SM quotes only inside H5's universe (when set)."""
        uni = self.edge_state("H1").get("sm_universe")
        return layer != "sm" or not uni or str(ex) in set(map(str, uni))

    def h1_on(self, ex, layer="tm"):
        return self.edge_arm("H1", ex, layer) == "A"

    def release_resting(self, ex, sub):
        """True when a release / rotation ask already rests for this sub-book (the layer's own exit must not double it)."""
        for o in self.open:
            rec = self.ledger.orders.get(o["id"]) or {}
            if o["ex"] == ex and rec.get("sub") == sub and rec.get("layer") == "release" and o["id"] not in self.ex.cancelling:
                return True
        return False

    def own_exit_resting(self, ex, sub):
        """True when the sub-book's own layer (dumpexit / tmexit / smexit / fmexit ...) already rests an exit for it."""
        for o in self.open:
            rec = self.ledger.orders.get(o["id"]) or {}
            if o["ex"] == ex and rec.get("sub") == sub and rec.get("layer") in self.REDUCING and rec.get("layer") != "release":
                return True                                 # C93b (Piero 16:15 item 3): an exit being re-pegged (cancelling) still covers the lot: its layer re-posts it this loop
        if sub.startswith("tm:") and any(v.get("sub") == sub for v in (self.tm_view.get(ex) or [])):
            return True                                     # C93b: the TM plans an exit for this lot this loop (the 15:59 I2 trim: release ask vs the TM exit's re-peg)
        return False

    def h1_keep(self, which, ex, side, behind_ticks, now):
        """H1 (arm A): a quote keeps its queue place while the touch is < 2 ticks away, and for 30 min once it is >= 2 ticks away;
        only then is it re-pegged. -> True = keep the resting order."""
        d = self.tm_behind if which == "tm" else self.sm_behind
        key = (ex, side)
        if behind_ticks < H1_BEHIND_TICKS:
            d.pop(key, None)
            return True
        t0 = d.setdefault(key, now)
        return now - t0 < H1_BEHIND_S

    def paper_scan(self, now):
        """Control-arm TM / SM entries that expired unfilled become paper orders (as if they had rested, H1 §0.3); each is followed
        against the tape ring for 12 h: prints under our price count in full, prints at our price for half (the queue); when the
        counted volume reaches the order's size it is a paper fill, with its 30-min mark-out written 30 min later.
        -> data/intel/paper/H1-<day>.jsonl."""
        for oid, o in list(self.ledger.orders.items()):
            if oid in self.paper_seen or o.get("layer") not in ("tm", "sm") or o.get("status") != "done" or o.get("action") != "buy":
                continue
            left = float(o.get("qty") or 0) - float(o.get("filled") or 0)
            ts, ttl = float(o.get("ts") or 0), float(o.get("ttl") or TM_TTL_S)
            if left < MIN_QTY or not ts or now - ts < 0.8 * ttl or now - ts > ttl + 900:
                continue
            self.paper_seen.add(oid)
            if self.edge_arm("H1", o["ex"], o["layer"]) != "B" or not self.h1_in_study(o["ex"], o["layer"]):
                continue                                    # arm A's quotes rest for real; SM quotes outside H5's universe are not in the study
            self.paper[oid] = dict(h="H1", ex=o["ex"], side=o["side"], px=float(o["price"]), qty=left, placed=ts, expired=now, last=now, cum=0.0,
                                   layer=o["layer"], fill_t=None, mo30=None, fair=self.side_fair(o["ex"], o["side"]))
        if len(self.paper_seen) > 20000:
            self.paper_seen = set(list(self.paper_seen)[-10000:])
        day = time.strftime("%Y-%m-%d", time.localtime(now))
        for oid, po in list(self.paper.items()):
            if po["fill_t"] is None:
                if now - po["expired"] > PAPER_MAX_AGE_S:
                    self.intel_write(f"paper/H1-{day}.jsonl", dict(po, oid=oid, event="unfilled", t=now))
                    self.paper.pop(oid)
                    continue
                for ts_, p, q, _ in list(self.prints.get(po["ex"], ())):
                    if ts_ <= po["last"]:
                        continue
                    yes_px = po["px"] if po["side"] == "yes" else round(1 - po["px"], 3)
                    better = (p < yes_px - 1e-9) if po["side"] == "yes" else (p > yes_px + 1e-9)
                    if better:
                        po["cum"] += q
                    elif abs(p - yes_px) < 1e-9:
                        po["cum"] += PAPER_HALF_AT_PRICE * q
                    if po["cum"] >= po["qty"]:
                        po["fill_t"] = ts_
                        self.intel_write(f"paper/H1-{day}.jsonl", dict(po, oid=oid, event="fill", t=now, rest_h=round((ts_ - po["placed"]) / 3600, 2)))
                        break
                po["last"] = now
            elif po["mo30"] is None and now - po["fill_t"] >= 1800:
                mid = self.side_mid(po["ex"], po["side"])
                po["mo30"] = None if mid is None else round(mid - po["px"], 4)
                self.intel_write(f"paper/H1-{day}.jsonl", dict(po, oid=oid, event="mo30", t=now))
                self.paper.pop(oid)

    def h1_arm_stats(self, now, hours=12):
        """Per arm over the last `hours`: resting-hours of TM / SM entry quotes, fills, fills per resting-hour, 30-min mark-out, TM edge
        at fill; control also gets its paper fills per resting-hour."""
        cut = now - hours * 3600
        st = {a: dict(rest_h=0.0, fills=0, shares=0.0, mo=[], edge=[], paper_fills=0, paper_rest_h=0.0) for a in ("A", "B")}
        for o in self.ledger.orders.values():
            if o.get("layer") not in ("tm", "sm") or o.get("action") != "buy":
                continue
            ts = float(o.get("ts") or 0)
            if not ts:
                continue
            end = now
            if o.get("status") in ("done", "cancelled", "rejected") and o.get("closed"):
                try:
                    end = time.mktime(time.strptime(o["closed"], "%Y-%m-%d %H:%M:%S"))
                except ValueError:
                    pass
            lo, hi = max(ts, cut), min(end, now)
            if hi > lo and self.h1_in_study(o["ex"], o["layer"]):
                st[self.edge_arm("H1", o["ex"], o["layer"])]["rest_h"] += (hi - lo) / 3600
        for f in self.fills_since(cut):
            if f.get("layer") in ("tm", "sm") and f.get("action") == "buy" and self.h1_in_study(f["ex"], f["layer"]):
                a = st[self.edge_arm("H1", f["ex"], f["layer"])]
                a["fills"] += 1; a["shares"] += f["qty"]
                if f.get("mo30") is not None:
                    a["mo"].append(f["mo30"])
                if f.get("layer") == "tm" and f.get("edge") is not None:
                    a["edge"].append(f["edge"])
        for po in self.paper.values():
            if po.get("fill_t") and po["fill_t"] >= cut:
                st["B"]["paper_fills"] += 1
        out = {}
        for a, v in st.items():
            out[a] = dict(rest_h=round(v["rest_h"], 1), fills=v["fills"], shares=round(v["shares"]), per_h=round(v["fills"] / v["rest_h"], 3) if v["rest_h"] > 0 else None,
                          mo30=round(sum(v["mo"]) / len(v["mo"]), 4) if v["mo"] else None, n_mo=len(v["mo"]),
                          tm_edge=round(sum(v["edge"]) / len(v["edge"]), 4) if v["edge"] else None, paper_fills=v["paper_fills"])
        return out

    def h3_arm_stats(self, now, hours=12):
        """Per arm over the last `hours`, on the arm's markets: edge bought on entries, edge given back on release sales, realized on
        those sales, net = bought - given back (the scoreboard's measure for H3)."""
        cut = now - hours * 3600
        st = {a: dict(bought=0.0, given=0.0, realized=0.0, sales=0, entries=0) for a in ("A", "B")}
        for f in self.fills_since(cut):
            if f.get("edge") is None or f.get("action") not in ("buy", "sell"):
                continue
            a = st[self.edge_arm("H3", f["ex"])]
            e = float(f["edge"]) * float(f["qty"])
            if f.get("layer") == "release":
                a["given"] += -e; a["realized"] += float(f.get("realized") or 0.0); a["sales"] += 1
            elif f.get("layer") in ("dump", "tm", "sm") and f.get("action") == "buy":
                a["bought"] += e; a["entries"] += 1
        return {a: dict({k: round(v, 2) for k, v in d.items()}, net=round(d["bought"] - d["given"], 2)) for a, d in st.items()}

    def h1_morning_verdict(self, now):
        """Piero 17:35 item 3: H1 is judged on the 04:00-08:00 PT window — fills per resting-hour vs control and the 30-min mark-out.
        Once, at the first controller run after 08:00: win -> promoted to every market, lose -> off, tie -> the A/B goes on; H4 resumes."""
        c = self.controls
        h1 = (c.d.get("edge") or {}).get("H1") or {}
        day = time.strftime("%Y-%m-%d", time.localtime(now))
        hm = time.strftime("%H:%M", time.localtime(now))
        if h1.get("state") != "ab" or h1.get("judged") == day or not (H1_JUDGE_WINDOW[1] <= hm < "09:00"):
            return                                          # once, in the hour after the window closes (never on a start-up later in the day)
        lt = time.localtime(now)
        t0 = time.mktime((lt.tm_year, lt.tm_mon, lt.tm_mday, int(H1_JUDGE_WINDOW[0][:2]), int(H1_JUDGE_WINDOW[0][3:]), 0, 0, 0, -1))
        t1 = time.mktime((lt.tm_year, lt.tm_mon, lt.tm_mday, int(H1_JUDGE_WINDOW[1][:2]), int(H1_JUDGE_WINDOW[1][3:]), 0, 0, 0, -1))
        st = self.h1_window_stats(t0, t1)
        A, B = st["A"], st["B"]
        enough = A["fills"] + B["fills"] >= 10 and A["per_h"] is not None and B["per_h"] is not None
        win = enough and A["per_h"] >= H1_GATE["fill_rate_x"] * max(B["per_h"], 1e-6) and (A["mo30"] is None or A["mo30"] >= H1_GATE["mo30_min"] - 1e-9)
        lose = enough and (A["per_h"] < B["per_h"] or (A["mo30"] is not None and A["mo30"] < -0.005))
        verdict = "win" if win else "lose" if lose else "tie (not enough fills)" if not enough else "tie"
        with c.lock:
            h1["judged"] = day
            h1["morning"] = dict(window=list(H1_JUDGE_WINDOW), A=A, B=B, verdict=verdict)
            if win:
                h1.update(state="full", promoted=stamp())
            elif lose:
                h1.update(state="off", rolled_back=stamp())
            c.d["edge"]["h4_paused"] = False
            c.save()
        msg = (f"H1 MORNING VERDICT ({H1_JUDGE_WINDOW[0]}-{H1_JUDGE_WINDOW[1]} PT): {verdict} — A (GTC) {A['fills']} fills / {A['rest_h']:.1f} resting-h = {A['per_h']}/h, mark-out {A['mo30']}; "
               f"control {B['fills']} / {B['rest_h']:.1f} = {B['per_h']}/h, mark-out {B['mo30']}, paper fills {B['paper_fills']} -> "
               + ("PROMOTED to every market" if win else "ROLLED BACK (off)" if lose else "the A/B goes on") + "; H4 pair locks resume")
        self.event("CONTROL", msg); self.night.line(msg); self.alerts.append(f"{time.strftime('%H:%M')} {msg}")

    def h1_window_stats(self, t0, t1):
        """h1_arm_stats over an absolute window [t0, t1]."""
        st = {a: dict(rest_h=0.0, fills=0, shares=0.0, mo=[], edge=[], paper_fills=0) for a in ("A", "B")}
        for o in self.ledger.orders.values():
            if o.get("layer") not in ("tm", "sm") or o.get("action") != "buy" or not self.h1_in_study(o["ex"], o["layer"]):
                continue
            ts = float(o.get("ts") or 0)
            if not ts:
                continue
            end = t1
            if o.get("status") in ("done", "cancelled", "rejected") and o.get("closed"):
                try:
                    end = time.mktime(time.strptime(o["closed"], "%Y-%m-%d %H:%M:%S"))
                except ValueError:
                    pass
            lo, hi = max(ts, t0), min(end, t1)
            if hi > lo:
                st[self.edge_arm("H1", o["ex"], o["layer"])]["rest_h"] += (hi - lo) / 3600
        for f in self.fills_since(t0):
            try:
                ft = time.mktime(time.strptime(f["t"], "%Y-%m-%d %H:%M:%S"))
            except ValueError:
                continue
            if ft > t1 or f.get("layer") not in ("tm", "sm") or f.get("action") != "buy" or not self.h1_in_study(f["ex"], f["layer"]):
                continue
            a = st[self.edge_arm("H1", f["ex"], f["layer"])]
            a["fills"] += 1; a["shares"] += f["qty"]
            if f.get("mo30") is not None:
                a["mo"].append(f["mo30"])
        for po in self.paper.values():
            if po.get("fill_t") and t0 <= po["fill_t"] <= t1:
                st["B"]["paper_fills"] += 1
        return {a: dict(rest_h=round(v["rest_h"], 1), fills=v["fills"], shares=round(v["shares"]), per_h=round(v["fills"] / v["rest_h"], 3) if v["rest_h"] > 0 else None,
                        mo30=round(sum(v["mo"]) / len(v["mo"]), 4) if v["mo"] else None, n_mo=len(v["mo"]), paper_fills=v["paper_fills"]) for a, v in st.items()}

    def edge_controller(self, now):
        """Every 10 min: the H1 A/B verdict for the hour (A wins when fills per resting-hour >= 3x control, 30-min mark-out >= 0, TM edge
        at fill >= 4c, on >= 10 fills); promotion to every market after >= 12 consecutive winning hours, automatic rollback (off) after
        6 consecutive losing hours; an 'H*' RUN_LOG line at the top of each hour (H1, H4, and H2 / H3 when live)."""
        if now - self.edge_t < 600:
            return
        self.edge_t = now
        c = self.controls
        edge = c.d.setdefault("edge", {})
        h1 = edge.setdefault("H1", dict(state="off", since=stamp(), hist=[]))
        st = self.h1_arm_stats(now)
        A, B = st["A"], st["B"]
        verdict = "n/a"
        if h1.get("state") == "ab" and A["fills"] + B["fills"] >= 10 and A["per_h"] is not None and B["per_h"] is not None:
            win = (A["per_h"] >= H1_GATE["fill_rate_x"] * max(B["per_h"], 1e-6) and (A["mo30"] is None or A["mo30"] >= H1_GATE["mo30_min"] - 1e-9)
                   and (A["tm_edge"] is None or A["tm_edge"] >= H1_GATE["tm_edge_min"] - 1e-9))
            lose = A["per_h"] < B["per_h"] or (A["mo30"] is not None and A["mo30"] < -0.005)
            verdict = "win" if win else "lose" if lose else "tie"
        try:
            self.h1_test_check(now)
            self.rot_report(now)                            # C95 item 6: the rotation report at 18:00 and 08:00
        except Exception as err:
            self.event("ERR", f"H1 test: {err!r}")
        if getattr(self, "dedup_pending", False) and getattr(self, "position_names", None) is not None:
            self.dedup_pending = False
            try:
                self.dedup_vs_sig(now, since=None if (self.controls.d.get("c93") or {}).get("dedup_all") else DEDUP_SINCE)
            except Exception as err:
                self.event("ERR", f"dedup vs SIG: {err!r}")
        try:
            self.h1_morning_verdict(now)
        except Exception as err:
            self.event("ERR", f"H1 morning verdict: {err!r}")
        hour = time.strftime("%Y-%m-%d %H", time.localtime(now))
        changed = False
        if hour != self.edge_hour:
            self.edge_hour = hour
            if h1.get("state") in ("ab", "full"):
                h1.setdefault("hist", []).append(dict(h=hour, v=verdict, A=A, B=B))
                h1["hist"] = h1["hist"][-48:]
                changed = True
                hist = [x["v"] for x in h1["hist"]]
                if h1["state"] == "ab" and len(hist) >= EDGE_PROMOTE_H and all(v == "win" for v in hist[-EDGE_PROMOTE_H:]):
                    h1.update(state="full", promoted=stamp())
                    msg = f"H1 PROMOTED to every market after {EDGE_PROMOTE_H} winning hours: A {A} vs control {B}"
                    self.event("CONTROL", msg); self.night.line(msg); self.alerts.append(f"{time.strftime('%H:%M')} {msg}")
                elif h1["state"] in ("ab", "full") and len(hist) >= EDGE_ROLLBACK_H and all(v == "lose" for v in hist[-EDGE_ROLLBACK_H:]):
                    h1.update(state="off", rolled_back=stamp())
                    msg = f"H1 ROLLED BACK (off) after {EDGE_ROLLBACK_H} losing hours: A {A} vs control {B}"
                    self.event("CONTROL", msg); self.night.line(msg); self.alerts.append(f"{time.strftime('%H:%M')} {msg}")
            fmt = lambda v: (f"{v['fills']} fills / {v['rest_h']:.0f} resting-h = {v['per_h'] if v['per_h'] is not None else 'n/a'}/h, mark-out 30m "
                             f"{'n/a' if v['mo30'] is None else f'{v['mo30'] * 100:+.2f}c'} (n {v['n_mo']}), TM edge {'n/a' if v['tm_edge'] is None else f'{v['tm_edge'] * 100:.1f}c'}")
            rest_now = collections.Counter()
            for o in self.open:
                rec = self.ledger.orders.get(o["id"]) or {}
                if rec.get("layer") in ("tm", "sm") and o["action"] == "buy" and not jx.reduces(o["side"], "buy", self.book.get(o["ex"], {}).get("pos", 0)) and self.h1_in_study(o["ex"], rec.get("layer")):
                    rest_now[self.edge_arm("H1", o["ex"], rec.get("layer"))] += 1
            self.night.line(f"H1 ({h1.get('state')}) last 12 h — A (GTC): {fmt(A)}; control: {fmt(B)}, paper fills as-if-GTC {B['paper_fills']}; resting now A {rest_now['A']} / control {rest_now['B']} quotes, "
                            f"free cash {self.free_cash():,.0f} (Piero 17:35: >= 12k by 03:30); verdict this hour: {verdict}; "
                            f"hours won/lost in a row: {sum(1 for _ in itertools.takewhile(lambda v: v == 'win', reversed([x['v'] for x in h1.get('hist', [])])))}/"
                            f"{sum(1 for _ in itertools.takewhile(lambda v: v == 'lose', reversed([x['v'] for x in h1.get('hist', [])])))}")
            h3 = edge.get("H3") or {}
            if h3.get("state") in ("ab", "full"):
                s3 = self.h3_arm_stats(now)
                v3 = "n/a"
                if h3["state"] == "ab" and s3["A"]["entries"] + s3["B"]["entries"] >= 10:
                    v3 = "win" if s3["A"]["net"] > s3["B"]["net"] else "lose"
                h3.setdefault("hist", []).append(dict(h=hour, v=v3, A=s3["A"], B=s3["B"])); h3["hist"] = h3["hist"][-48:]; changed = True
                hist3 = [x["v"] for x in h3["hist"]]
                if h3["state"] == "ab" and len(hist3) >= EDGE_PROMOTE_H and all(v == "win" for v in hist3[-EDGE_PROMOTE_H:]):
                    h3.update(state="full", promoted=stamp()); msg = f"H3 PROMOTED to every market after {EDGE_PROMOTE_H} winning hours: A {s3['A']} vs control {s3['B']}"
                    self.event("CONTROL", msg); self.night.line(msg); self.alerts.append(f"{time.strftime('%H:%M')} {msg}")
                elif len(hist3) >= EDGE_ROLLBACK_H and all(v == "lose" for v in hist3[-EDGE_ROLLBACK_H:]):
                    h3.update(state="off", rolled_back=stamp()); msg = f"H3 ROLLED BACK (off) after {EDGE_ROLLBACK_H} losing hours: A {s3['A']} vs control {s3['B']}"
                    self.event("CONTROL", msg); self.night.line(msg); self.alerts.append(f"{time.strftime('%H:%M')} {msg}")
                self.night.line(f"H3 ({h3.get('state')}) last 12 h — A (swap-only sales): bought {s3['A']['bought']:+,.0f}, given back {-s3['A']['given']:+,.0f}, net {s3['A']['net']:+,.0f}, "
                                f"{s3['A']['sales']} sales / {s3['A']['entries']} entries, realized {s3['A']['realized']:+,.0f}; control (C89 seller): bought {s3['B']['bought']:+,.0f}, given back "
                                f"{-s3['B']['given']:+,.0f}, net {s3['B']['net']:+,.0f}, {s3['B']['sales']} sales / {s3['B']['entries']} entries, realized {s3['B']['realized']:+,.0f}; verdict: {v3}")
            if ROTATION.get("on") and (self.controls.d.get("rotation") or {}).get("since"):
                self.night.line(self.rot_status(now))      # C94: the hourly rotation line
            try:
                if ROTATION.get("party_cap"):
                    self.night.line(self.party_line())     # alpha_spec §5.5: net party exposure and its 1 sigma, hourly
                    self.night.line(self.manual_line())    # Piero 01:05 item 7: his manual tickets next to it
                    self.party_list(time.time())           # the Nov 1 18:00 decision list (once)
                self.night.line(self.sale_churn_line())    # Piero 01:05 item 4: reposts / re-pegs of the sale asks
            except Exception as err:
                self.event("ERR", f"party line: {err!r}")
            try:
                if CARRY.get("on") and (self.controls.d.get("c98") or {}).get("done"):
                    self.night.line(self.carry_status())   # alpha_spec §2 / §6: the carry book's EV/$ hourly
            except Exception as err:
                self.event("ERR", f"carry status: {err!r}")
            try:
                if self.fmm_mode(now) in ("paper", "live", "stopped"):
                    self.night.line(self.fmm_status(now, hourly=True))  # fmm_spec §4: the hourly FMM line (C100 item 4: + the last hour)
                if self.election is not None and self.el_mode_cached != "off":
                    self.night.line(self.el_hour_line())  # C102: the election detector / sniper, hourly
                if ELECTION_RESERVE.get("on"):
                    self.night.line(self.election_cash_line(now))   # C103: cash_calendar §3 item 4
                if GAPMM.get("on") and (self.gapmm_paper_on(time.time()) or self.gapmm_live() or self.gap_paper["fills"]):
                    self.night.line(self.gapmm_line(time.time()))
            except Exception as err:
                self.event("ERR", f"FMM status: {err!r}")
            if self.h1_test_day(now) and (self.controls.d.get("h1_test") or {}).get("opened"):
                fl = self.h1_test_fills(now)
                ent = [f for f in fl if f.get("layer") == "tm"]; ex_ = [f for f in fl if f.get("layer") == "tmexit"]
                self.night.line(f"H1 TEST hour: {len(ent)} entry fills / {len(ex_)} exit fills so far, {self.h1_test_pnl_fair(now):+,.0f} at fair, realized {sum(f.get('realized') or 0 for f in ex_):+,.0f}, "
                                f"cost in use {self.h1_cost():,.0f} of {H1_TEST['sleeve']:,.0f}, quotes resting {sum(1 for o in self.open if o['ex'] in set(self.h1_test_markets()) and (self.ledger.orders.get(o['id']) or {}).get('layer') == 'tm')}, "
                                f"{'STOPPED' if self.h1_test_stopped else 'running'}, {'ring-stop hours (lifted for the sleeve)' if self.tm_entry_stop(now) or self.ring_window(now) else 'outside the ring stops'}")
            h4 = [bk for bk in self.ledger.baskets if bk.get("h4")]
            self.night.line(f"H4 pair locks: {len(h4)} sets, {sum(bk['qty'] for bk in h4):,} per leg, locked {sum(bk['qty'] * bk['margin'] for bk in h4):,.0f} at settlement, "
                            f"{self.h4_used:,.0f} of {H4['cap']:,} used" if H4.get("on") else "H4 off")
        if changed:
            with c.lock:
                c.save()

    def c90_migration(self, now):
        """Once (C90, docs/edge_program.md): H1 starts as a live A/B (arm A = half the universe by hash); H4 on; logged."""
        c = self.controls
        d = c.d.setdefault("c90", {})
        if d.get("done") or os.environ.get("SIG_C90", "1") != "1":
            return
        with c.lock:
            c.d.setdefault("edge", {})["H1"] = dict(state="ab", since=stamp(), hist=[])
            d.update(done=stamp())
            c.save()
        n_a = sum(1 for b in self.bots if b.kind == "market" and self.edge_arm("H1", b.ex) == "A")
        msg = (f"C90 (edge program): H1 GTC quotes live as an A/B — arm A = {n_a} markets by hash (TM / SM entries with a 24-h TTL, kept within 1 tick of the touch and for 30 min at >= 2 ticks; "
               f"control = the rest on the 20-min TTL, with paper fills as if they had rested); gate: fills per resting-hour >= 3x control, 30-min mark-out >= 0, TM edge >= 4c; promote after "
               f"{EDGE_PROMOTE_H} winning hours, roll back after {EDGE_ROLLBACK_H} losing hours. H4 pair locks on: both NO at total cost <= 0.96 on two-candidate races, <= {H4['per_race']:,} per race, "
               f"<= {H4['cap']:,} in all, only with free > {H4['idle_free']:,} and no entry >= {H4['waiting_edge']:.0%} waiting; held to settlement. Also: a release ask and a layer's own exit never both rest for one lot (the I2 trims of Oct 10-11); the cash seller's ask keeps its place within a tick.")
        self.event("CONTROL", msg); self.night.line(msg)

    def c91_migration(self, now):
        """Once (C91): H1's spread-maker arm on H5's universe (top 10 by time-to-front, rank parity); H3 (swap-only sales) live as an A/B."""
        c = self.controls
        d = c.d.setdefault("c91", {})
        if d.get("done") or os.environ.get("SIG_C91", "1") != "1":
            return
        with c.lock:
            h1 = c.d.setdefault("edge", {}).setdefault("H1", dict(state="ab", since=stamp(), hist=[]))
            h1["sm_universe"], h1["sm_arm_a"] = list(H1_SM_UNIVERSE), list(H1_SM_UNIVERSE[0::2])
            c.d["edge"]["H3"] = dict(state="ab", since=stamp(), hist=[])
            d.update(done=stamp())
            c.save()
        msg = (f"C91 (edge program): H1's spread-maker arm now follows H5's universe — top 10 by time-to-front {list(H1_SM_UNIVERSE)}, arm A = ranks 1/3/5/7/9 {list(H1_SM_UNIVERSE[0::2])}, "
               f"the other five control; TM quotes keep the hash split everywhere. H3 live as an A/B: on arm-A markets (hash) the cash seller sells a trading lot only for a waiting entry "
               f"with >= {H3['swap_c'] * 100:.0f}c more edge, sized to that entry's cost, converged lots (< {H3['converged_ret']:.0%} left) first; arm B stays on C89's lowest-return-first seller; "
               f"judged hourly on net edge (bought - given back) per arm, promote after {EDGE_PROMOTE_H} winning hours, roll back after {EDGE_ROLLBACK_H} losing hours.")
        self.event("CONTROL", msg); self.night.line(msg)

    def c93_migration(self, now):
        """Once (C93, Piero 15:30 Oct 12): the 12k target and the H3 swap seller are retired (HOLD the favourites; release the no-edge
        lots as GTC asks); orders keep their place; the whole-ledger dedup; the H1 decisive test is armed for Oct 13."""
        c = self.controls
        d = c.d.setdefault("c93", {})
        if os.environ.get("SIG_C93", "1") != "1":
            return
        if d.get("done"):
            if not (c.d.get("c92") or {}).get("dedup_all_done"):
                self.dedup_pending = True
            return
        with c.lock:
            c.d.pop("cash_target_1735", None); c.d.pop("cash_low_ret", None); c.d.pop("sell_subs", None)
            e = c.d.setdefault("edge", {})
            if (e.get("H3") or {}).get("state") in ("ab", "full"):
                e["H3"].update(state="off", retired=stamp(), why="Piero 15:30: HOLD the favourites — no seller touches a lot with >= 6% return left")
            d.update(done=stamp(), dedup_all=True)
            c.save()
        self.dedup_pending = True
        msg = (f"C93 (Piero 15:30): (1) HOLD THE FAVOURITES — the C89 cash-target seller and the H3 swap seller are retired (H3 A/B closed: arm A ahead in every hourly read; "
               f"the lesson is 'do not churn'); no seller touches a lot with >= {HOLD_RET:.0%} return left; the catcher's / TM's round-trip exits and core's rule stay. "
               f"(2) RELEASE THE NO-EDGE LOTS: one GTC ask at max(lot average + 0.5c, touch ask) on every non-core lot with < {NOEDGE['ret']:.0%} return left at that price or on a side with fair < {NOEDGE['low_fair']} and < {NOEDGE['low_edge_c'] * 100:.0f}c of edge (RI-GOV-D and the RI reservoir excluded); the list follows. "
               f"(3) ORDERS KEEP THEIR PLACE: every seller, exit and TM / SM quote is GTC ({GTC_TTL_S // 3600} h); a re-peg waits until the touch has sat >= {REPEG_AWAY_TICKS} tick away for > {REPEG_AWAY_S // 60} min; re-size only beyond 25%; POSTED and FILLED logged apart (cash freed counts from SIG fills only). "
               f"(4) H1 DECISIVE TEST armed: {H1_TEST['day']} {H1_TEST['start']}-{H1_TEST['end']} PT on {', '.join(self.name(e) for e in self.h1_test_markets())}: a {H1_TEST['sleeve']:,.0f} sleeve ahead of the catcher from {H1_TEST['reserve_from']}, "
               f"shortfall sold at the best bid at {H1_TEST['shortfall_at']} (<= ~{H1_TEST['shortfall_max_cost']:,.0f} of settlement value), GTC quotes of {H1_TEST['clip']} with the TM pause and the SM 24-h stop lifted there, stop {H1_TEST['stop_fair']:+,.0f} at fair. "
               f"(5) the whole-ledger dedup against SIG's fills runs on the first controller pass (before / after line follows). H4 stays as it is.")
        self.event("CONTROL", msg); self.night.line(msg)

    # ------------------------------------------------------------------ C94: coin-flip rotation (Piero 20:30 Oct 13)
    # ------------------------------------------------------------------ C95: coin-flip rotation at full size + the manual reserve (Piero 15:05 Oct 16)
    def rot_state(self):
        return self.controls.d.get("rotation") or {}

    @staticmethod
    def fill_dq(f):
        """Change in net YES shares from one ledger fill."""
        q = float(f["qty"])
        if f.get("action") == "sell":
            return -q if f.get("side") == "yes" else q
        return q if f.get("side") == "yes" else -q

    def manual_spent(self):
        """C95 item 1: net cash Piero's own fills (orders the engine did not place: no layer) took since the reserve was set; a sale
        that frees cash counts back. Positions are walked back from now so each fill is split into its reducing and adding part."""
        cfg = self.controls.d.get("manual_reserve") or {}
        since = cfg.get("since")
        if not since:
            return 0.0
        key = (len(self.ledger.fills), since)
        if getattr(self, "_manual_key", None) == key:
            return self._manual_spent
        later = sorted((f for f in self.ledger.fills if str(f.get("t", "")) >= since), key=lambda f: f["t"])
        pos = {str(ex): float(b.get("pos") or 0) for ex, b in self.book.items()}
        for f in reversed(later):
            pos[str(f["ex"])] = pos.get(str(f["ex"]), 0.0) - self.fill_dq(f)
        spent = 0.0
        for f in later:
            ex = str(f["ex"]); p0 = pos.get(ex, 0.0); d = self.fill_dq(f); q = float(f["qty"]); p = float(f["price"])
            if not (f.get("layer") or ""):
                red = min(q, abs(p0)) if (p0 > 0 > d) or (p0 < 0 < d) else 0.0
                add = q - red
                cash = (red * p - add * (1 - p)) if f.get("action") == "sell" else (red * (1 - p) - add * p)
                spent -= cash
            pos[ex] = p0 + d
        self._manual_key, self._manual_spent = key, spent
        return spent

    def fit_reducing(self, b, places):
        """C95d (15:44 Oct 16: the recycler's 120 Tennessee Senate R YES vs the funding asks already covering the position, cut by the
        executor every loop -> I2 pause x3 -> stuck): every reducing placement is cut, at planning and in executor priority, to what the
        position still allows after our resting / in-flight / queued exits and any foreign ones; a cut to under 10 is dropped (logged
        once per market and layer per 10 min). I2 itself is unchanged — the executor still trims anything that slips through."""
        ex = b.ex
        pos = float((self.book.get(ex) or {}).get("pos") or 0)
        if not places or not pos:
            return places
        if not any(jx.reduces(w["side"], w.get("action", "buy"), pos) for w in places):
            return places
        try:
            cap = float(self.ex._reducing_capacity(ex))
            cap -= sum(p["qty"] for p in self.ex.pending() if p.get("state") == "queued" and p["ex"] == ex and jx.reduces(p["side"], p["action"], pos))
        except Exception:
            return places
        out = []
        for w in sorted(places, key=lambda w: jx.PRIORITY.get(w.get("layer"), 9)):
            if jx.reduces(w["side"], w.get("action", "buy"), pos):
                q = int(max(0.0, min(float(w["qty"]), cap)))
                if q < MIN_QTY:
                    k_ = (ex, w.get("layer"))
                    if time.time() - self.fit_log.get(k_, 0) > 600:
                        self.fit_log[k_] = time.time()
                        self.event("I2", f"{self.name(ex)}: {w.get('layer')} exit {w['side'].upper()} {int(w['qty']):,} not sent: our resting exits already cover the position ({pos:+,.0f}); room {max(0.0, cap):,.0f}")
                    continue
                if q < w["qty"]:
                    w = dict(w, qty=q, qmax=min(int(w.get("qmax") or q), q))
                cap -= q
            out.append(w)
        return out

    def foreign_fill_actions(self, max_reads=6):
        """C99 (Piero 01:05 Oct 18, AZ-06): SIG's fill rows have no direction (a YES sale and a YES buy look the same), and a fill of an order
        the engine did not place has no record of its own — it was booked as a buy. Each new foreign fill's order is read once (GET /orders/{id},
        cached) and its action set before the manual book uses it."""
        n = 0
        for f in self.ledger.fills[-400:]:
            if f.get("action") or (f.get("layer") or "") or f.get("manual_booked"):
                continue
            oid = str(f.get("order") or "")
            if not oid or oid in self.ledger.orders:
                continue
            act = self.foreign_actions.get(oid)
            if act is None:
                if n >= max_reads or not api.spare_reads(10):
                    break
                n += 1
                try:
                    act = str((api.order(oid) or {}).get("action") or "").lower() or None
                except Exception as err:
                    self.event("ERR", f"foreign order {oid} read: {err!r}")
                    break
                self.foreign_actions[oid] = act
            if act in ("buy", "sell"):
                f["action"] = act
                if act == "sell":
                    msg = f"AUDIT {self.name(f['ex'])}: a fill of order {oid} (not the engine's) is a SALE of {f['qty']:,.0f} {f['side'].upper()} at {f['price']:.3f} (SIG's order record)"
                    self.event("AUDIT", msg); self.night.line(msg)

    def manual_book(self, now):
        """C95c (Piero 15:27 Oct 16: the no-edge release sold 8,356 of his 25,000 SC-01 D YES bought by hand): a fill of an order the
        engine did not place moves its shares into the 'manual' sub-book, which no engine seller touches; a manual sale reduces it.
        A manual fill that reduces an engine holding (manual book empty, engine inventory on the other side) stays with the engine's
        books. Each fill is booked once (flag on the fill), from C95c on."""
        since = (self.controls.d.get("c95c") or {}).get("since")
        if not since:
            return
        moved = []
        with self.ledger.lock:
            for f in self.ledger.fills[-400:]:
                if f.get("manual_booked") or (f.get("layer") or "") or str(f.get("t", "")) < since:
                    continue
                f["manual_booked"] = True
                ex = str(f["ex"]); d = self.fill_dq(f)
                books = self.ledger.subbooks.setdefault(ex, {})
                man, inv = float(books.get("manual", 0.0)), float(books.get("inventory", 0.0))
                if man * d < 0:
                    mv = max(d, -man) if man > 0 else min(d, -man)   # closing his own: at most what the manual book holds
                elif man == 0 and inv * d < 0:
                    mv = 0.0                                 # he reduced an engine holding: the engine's books carry it
                else:
                    mv = d                                   # he opened / added: his shares
                if abs(mv) >= 1:
                    books["manual"] = man + mv
                    books["inventory"] = inv - mv
                    if abs(books["manual"]) < 1:
                        books.pop("manual", None)
                    moved.append((ex, mv))
        for ex, mv in moved:
            msg = f"MANUAL {self.name(ex)}: Piero's own fill {mv:+,.0f} YES booked to the manual sub-book (manual {float(self.ledger.subbooks.get(ex, {}).get('manual', 0)):+,.0f}); no engine seller touches it"
            self.event("AUDIT", msg); self.night.line(msg)

    def manual_reserve_left(self):
        """C95 item 1: what is left of Piero's manual reserve (his sales refill it up to the amount)."""
        amt = float((self.controls.d.get("manual_reserve") or {}).get("amount") or 0.0)
        if amt <= 0:
            return 0.0
        return max(0.0, min(amt, amt - self.manual_spent()))

    def engine_cash_now(self):
        if getattr(self, "engine_cash", None) is not None:
            return self.engine_cash
        a = self.acct or {}
        return ((a.get("totalAccountValue") or 0) - (a.get("totalHoldingsValue") or 0)) - self.manual_reserve_left()

    def rot_lots(self):
        """{sub: (ex, side, shares, avg entry)} of the rotation's inventory."""
        out = {}
        for ex, books in self.ledger.subbooks.items():
            for sub, q in books.items():
                if sub.startswith("rot:") and abs(q) >= 1:
                    side = "yes" if q > 0 else "no"
                    avg = float((self.ledger.tranches.get(sub) or {}).get("avg") or 0) or float(self.sig_mark(ex, side) or 0.5)
                    out[sub] = (str(ex), side, abs(q), avg)
        return out

    def rot_bids(self):
        return [o for o in self.open if o["action"] == "buy" and (self.ledger.orders.get(o["id"]) or {}).get("layer") == "rot"]

    def rot_race(self, ex):
        ex = str(ex)
        if ex in (ROTATION.get("senate_exs") or ()):
            return "U.S. Senate control"
        return (self.meta.get(ex) or {}).get("race") or ex

    def rot_expo(self, ex, side):
        """(race, exposure): in a two-candidate D/R race (and Senate control, 842 D / 843 R) a buy's exposure is the party it wins with
        (D YES = R NO: the same exposure in the cheaper book); elsewhere the contract and side."""
        ex = str(ex)
        race = self.rot_race(ex)
        senate = ROTATION.get("senate_exs") or ()
        party = ("D" if ex == senate[0] else "R") if ex in senate else str((self.meta.get(ex) or {}).get("party") or "")
        two = ex in senate or sorted(str(m.get("party") or "") for m in self.meta.values() if m.get("race") == race) == ["D", "R"]
        if two and party in ("D", "R"):
            return race, (party if side == "yes" else ("R" if party == "D" else "D"))
        return race, f"{ex}:{side}"

    def rot_cost(self, race=None):
        """Rotation inventory at cost plus its resting bids (one race, or all)."""
        out = 0.0
        for sub, (ex, side, q, avg) in self.rot_lots().items():
            if race is None or self.rot_race(ex) == race:
                out += q * avg
        for o in self.rot_bids():
            if race is None or self.rot_race(o["ex"]) == race:
                out += o["qty"] * o["price"]
        return out

    def rot_flows(self):
        """C95: the rotation's cash since it began, from the ledger — the sales (every reducing fill on a sale market, up to its
        target: the release asks and the lots' own exits alike), its exits and its buys. Recomputed, so a restart never counts twice."""
        st = self.rot_state()
        frozen = st.get("frozen") or {}                     # C97 item 1: a sale market moved to the fund sale keeps only what the rotation sold
        sells = {str(k): min(int(v), int(frozen.get(str(k), v))) for k, v in (ROTATION.get("sells") or {}).items()}
        key = (len(self.ledger.fills), st.get("since"), tuple(sorted(frozen.items())))
        if getattr(self, "_rot_flows_key", None) == key:
            return self._rot_flows
        t0 = st.get("since") or "9999"
        sold = collections.Counter(); cash_m = collections.Counter()
        freed = exits = buys = rs = rx = 0.0
        for f in self.ledger.fills:
            if str(f.get("t", "")) < t0:
                continue
            ex, lay = str(f.get("ex")), f.get("layer")
            if lay == "rot":
                buys += f["qty"] * f["price"]
            elif lay == "rotexit":
                exits += f["qty"] * (1 - f["price"]); rx += f.get("realized") or 0.0
            elif ex in sells and lay in self.REDUCING and f.get("action") == "buy" and sold[ex] < sells[ex]:
                q = min(float(f["qty"]), sells[ex] - sold[ex])
                sold[ex] += q; cash_m[ex] += q * (1 - f["price"]); freed += q * (1 - f["price"])
                rs += (f.get("realized") or 0.0) * q / float(f["qty"])
        out = dict(sold=dict(sold), cash=dict(cash_m), freed=freed, exits=exits, buys=buys, realized_sales=rs, realized_exits=rx, net=freed + exits - buys)
        self._rot_flows_key, self._rot_flows = key, out
        return out

    def rot_pool(self):
        """C95 (and C94e): -> (pool, rotation cash left, engine room): the rotation's net cash (sales + exits - buys) minus its resting
        bids, never more than the engine's free cash (SIG cash - the 5,000 floor - the manual reserve left) minus its resting bids."""
        if not ROTATION.get("on") or not self.rot_state():
            return 0.0, 0.0, 0.0
        resting = sum(o["qty"] * o["price"] for o in self.rot_bids())
        left = self.rot_flows()["net"] - resting
        room = (self.engine_cash_now() - CASH_RESERVE - self.fmm_priority()) - resting   # C97 item 2: behind FMM's first 10k
        return min(left, room), left, room

    def rot_claim(self):
        """The rotation's claim on the engine's cash (unspent + resting bids): taken off what every other layer sees. C97: never more
        than its cap leaves (at or over its 30k cap it can't buy, so its cash is not held back) nor what FMM's first 10k leaves."""
        if not ROTATION.get("on") or not self.rot_state():
            return 0.0
        inv = sum(q * avg for (ex, side, q, avg) in self.rot_lots().values())
        return max(0.0, min(self.rot_flows()["net"], self.engine_cash_now() - CASH_RESERVE - self.fmm_priority(), max(0.0, self.rot_total_cap() - inv)))

    def fmm_priority(self):
        """C97 item 2 (Piero 15:55 Oct 17): cash goes to FMM first, until its free cash reaches 10k — the other layers (the rotation, the
        catcher, TM / SM, locks) see only free cash above FMM's first 10k while FMM is live."""
        if not ALLOC.get("on") or self.fmm_mode(time.time()) != "live":
            return 0.0
        first = self.fmm_claim_first(self.fmm_free())
        return first + sum(n for _, n in self.fmm_unread)   # C99f: + the buys since the last cash read — fmm_free nets them, the others' cash read
                                                             # doesn't yet (07:59 Oct 18: 1,967 of unread FMM fills became a 1,966 RI carry bid)

    def fmm_claim_first(self, free):
        """C101 item B (Piero 15:15 Oct 21): FMM claims min(15k, what it can deploy under its caps) of the free cash; the rest goes to the
        carry bids, then the all-NO sets (idle cash above floor + reserve ~0)."""
        dep = getattr(self, "fmm_deploy", None)
        return min(max(0.0, free), float(ALLOC["fmm_priority_free"]), float("inf") if dep is None else max(0.0, dep))

    def rot_reserve(self):
        return max(0.0, self.rot_pool()[0])

    def rot_sale_fill(self, f, px_s):
        if FUND.get("on") and str(f["ex"]) in (FUND.get("sells") or {}) and str(f.get("t", "")) >= FUND["since"]:
            return "fund "
        return "rotation " if str(f["ex"]) in (ROTATION.get("sells") or {}) else ""

    # ------------------------------------------------------------------ C97 item 1: the fund sale (Piero 15:55 Oct 17)
    def fund_flows(self):
        """What the fund sale sold per market since it began (every reducing fill on a fund market, up to its target), and the cash."""
        sells = {str(k): int(v) for k, v in (FUND.get("sells") or {}).items()}
        key = (len(self.ledger.fills), FUND.get("since"))
        if getattr(self, "_fund_key", None) == key:
            return self._fund_flows
        sold, cash = collections.Counter(), collections.Counter(); rs = 0.0
        for f in self.ledger.fills:
            ex = str(f.get("ex"))
            if ex not in sells or str(f.get("t", "")) < FUND["since"] or f.get("layer") not in self.REDUCING or f.get("action") != "buy" or sold[ex] >= sells[ex]:
                continue
            q = min(float(f["qty"]), sells[ex] - sold[ex])
            sold[ex] += q; cash[ex] += q * (1 - f["price"]); rs += (f.get("realized") or 0.0) * q / float(f["qty"])
        self._fund_key, self._fund_flows = key, dict(sold=dict(sold), cash=dict(cash), freed=sum(cash.values()), realized=rs)
        return self._fund_flows

    def fund_sync(self):
        """-> {ex: shares left to sell} (>= MIN_QTY), saved when it changes."""
        if not FUND.get("on"):
            return {}
        st = self.controls.d.get("fund")
        if not st:
            return {}
        fl = self.fund_flows()
        left = {str(ex): max(0, int(v) - int(fl["sold"].get(str(ex), 0))) for ex, v in (FUND.get("sells") or {}).items()}
        if left != st.get("sell_left"):
            with self.controls.lock:
                st.update(sell_left=left, cash_freed=round(fl["freed"], 2)); self.controls.save()
        return {ex: v for ex, v in left.items() if v >= MIN_QTY}

    def rot_sync(self):
        """Left-to-sell per sale market from the ledger (rot_flows); saved when it changes."""
        st = self.rot_state()
        sells = ROTATION.get("sells") or {}
        if not st or not sells:
            return st
        fl = self.rot_flows()
        frozen = st.get("frozen") or {}
        new_left = {str(ex): max(0, min(int(v), int(frozen.get(str(ex), v))) - int(fl["sold"].get(str(ex), 0))) for ex, v in sells.items()}
        if new_left != st.get("sell_left") or abs(fl["freed"] - float(st.get("cash_freed") or 0)) > 0.5:
            with self.controls.lock:
                st.update(sell_left=new_left, cash_freed=round(fl["freed"], 2), sold=float(sum(fl["sold"].values())))
                self.controls.d["rotation"] = st; self.controls.save()
        return st

    def rot_touch(self, ex, side):
        """(bid, ask, fair) on `side`: the touch bid we would join, the ask, PM fair in side terms."""
        pxs = self.px.get(ex)
        f = self.fair.get(ex)
        if not pxs or pxs[0] is None or pxs[1] is None or f is None:
            return None, None, None
        bid, ask = bots.side_book(pxs[0], pxs[1], side)[:2]
        return bid, ask, (f if side == "yes" else 1 - f)

    def rot_vol48(self, now):
        """{ex: shares printed in the last 48 h} from the tape files (re-read every 30 min)."""
        c = getattr(self, "_vol48", None)
        if c and now - c[0] < 1800:
            return c[1]
        vol, cut = collections.Counter(), now - 2 * 86400
        for d in range(3):
            fn = os.path.join(DATA, "intel", "trades", time.strftime("%Y-%m-%d", time.localtime(now - 86400 * d)) + ".jsonl")
            try:
                with open(fn) as fh:
                    for line in fh:
                        try:
                            r = json.loads(line)
                        except ValueError:
                            continue
                        if (r.get("ts") or 0) >= cut:
                            vol[str(r.get("ex"))] += float(r.get("q") or 0)
            except OSError:
                continue
        self._vol48 = (now, dict(vol))
        return self._vol48[1]

    def rot_skips(self, now):
        sk = self.rot_state().get("skipped") or {}
        return {k for k, v in sk.items() if isinstance(v, (int, float)) and v > now}

    def rot_candidates(self, now):
        """C95 item 3: every coin-flip side (fair 0.25-0.75) with a gap to PM fair >= 3c at the touch bid and >= 100k shares printed in
        48 h; per race one exposure (the race's held exposure if it has one), the best gap % in the cheaper book; sorted >= 4c first,
        then the >= 3c extension, each by gap %."""
        vol = self.rot_vol48(now)
        skipped = self.rot_skips(now)
        rows = []
        for ex, b in self.owner.items():
            ex = str(ex)
            if b.kind != "market" or ex in self.manual_ex or ex in dash5.RI_EX or self.ex.is_frozen(ex) or ex in self.reservoirs():
                continue
            if vol.get(ex, 0.0) < ROTATION["min_vol48"]:
                continue
            pos_ = float((self.book.get(ex) or {}).get("pos") or 0)
            for side in ("yes", "no"):
                if f"{ex}:{side}" in skipped:
                    continue
                if (side == "yes" and pos_ <= -1) or (side == "no" and pos_ >= 1):
                    continue                                # C95f: we hold the other side here; a buy would net against it (sell it under its floor) — Iowa D NO vs our 500 YES
                bid, ask, fs = self.rot_touch(ex, side)
                if bid is None or fs is None or not (ROTATION["fair_lo"] <= fs <= ROTATION["fair_hi"]) or not (MB_MIN_PX <= bid <= MB_MAX_PX):
                    continue
                gap = fs - bid
                if gap < ROTATION["gap_ext"] - 1e-9:
                    continue
                race, expo = self.rot_expo(ex, side)
                rows.append(dict(ex=ex, side=side, px=bid, fs=fs, gap=gap, pct=gap / bid, race=race, expo=expo,
                                 tier=0 if gap >= ROTATION["gap_min"] - 1e-9 else 1, vol=vol.get(ex, 0.0)))
        held = {}
        for sub, (ex, side, q, avg) in self.rot_lots().items():
            held[self.rot_race(ex)] = self.rot_expo(ex, side)[1]
        for o in self.rot_bids():
            held.setdefault(self.rot_race(o["ex"]), self.rot_expo(o["ex"], o["side"])[1])
        best = {}
        for r in rows:
            if r["race"] in held and held[r["race"]] != r["expo"]:
                continue                                    # one exposure per race
            k = r["race"]
            if k not in best or (r["tier"], -r["pct"]) < (best[k]["tier"], -best[k]["pct"]):
                best[k] = r
        self.rot_cands = sorted(best.values(), key=lambda r: (r["tier"], -r["pct"]))
        return self.rot_cands

    def engine_bid_room(self):
        """fmm_spec §0.1: what the engine's resting entry bids (rotation + FMM) may total: cash - the 5,000 floor - the manual reserve left."""
        return self.engine_cash_now() - CASH_RESERVE

    def rot_widened(self):
        """fmm_spec §0.3: {race: (gap at entry, gap now)} for rotation lots whose gap to PM fair has widened by >= 2c since entry
        (mark = SIG mid; no stop-loss — rule 0 holds; the race gets nothing more)."""
        out = {}
        for sub, (ex, side, q, avg) in self.rot_lots().items():
            tr = self.ledger.tranches.get(sub) or {}
            f0 = tr.get("fair0"); pxs = self.px.get(ex); fs = self.side_fair(ex, side)
            if f0 is None or fs is None or not pxs or pxs[0] is None or pxs[1] is None:
                continue
            mid = (pxs[0] + pxs[1]) / 2; mark = mid if side == "yes" else 1 - mid
            g0, g1 = float(f0) - avg, fs - mark
            if g1 - g0 >= float(ROTATION.get("widen_flag_c", 0.02)) - 1e-9:
                out[self.rot_race(ex)] = (round(g0, 4), round(g1, 4))
        for race, (g0, g1) in out.items():
            k_ = (race, "widened")
            if k_ not in self.rot_logged:
                self.rot_logged.add(k_)
                msg = f"ROT FLAG {race}: the gap to PM fair widened from {g0 * 100:.1f}c at entry to {g1 * 100:.1f}c; nothing more is added to this race (no stop-loss: rule 0 holds)"
                self.event("ROT", msg); self.night.line(msg)
        return out

    def rot_caps(self):
        """(race cost {race: cost}, total cost) of inventory at cost + resting bids."""
        rc = collections.defaultdict(float)
        for sub, (ex, side, q, avg) in self.rot_lots().items():
            rc[self.rot_race(ex)] += q * avg
        for o in self.rot_bids():
            rc[self.rot_race(o["ex"])] += o["qty"] * o["price"]
        return rc, sum(rc.values())

    def rot_cap(self, race):
        return float(ROTATION["senate_cap"] if race == "U.S. Senate control" else ROTATION["race_cap"])

    def rot_plan(self, now, cash):
        """C95 items 3-4: resting bids keep their size and place (never pulled because the pool shrank; pulled only when the gap at
        their price falls under 3c or PM fair moved >= 2c against them); a re-peg (touch moved up, after the 10-min grace) carries
        only what the pool funds; new bids go to the candidates in order, each for what the pool funds at that moment (>= 500 shares),
        within 8k per race (12k Senate control) and 65k in all; the >= 3c tier only once the >= 4c one is full. -> {ex: [want-spec]}"""
        c = self.controls
        if not ROTATION.get("on") or self.fills_blind:
            return {}
        cands = self.rot_candidates(now)
        self.track_rot(now)                                 # the exits keep running in a pause (C94f)
        if c.paused or c.state("rot") == "off":
            return {}
        if self.wd_after("rot_buys_stop", now):
            return {}                                        # C103: the rotation buys nothing from Oct 27 (its exits keep running)
        st = self.rot_sync()
        if not st:
            return {}
        pool = self.rot_pool()[0]
        rc, total = self.rot_caps()
        clip = int(ROTATION["min_clip"])
        skips = self.rot_skips(now)
        widened = self.rot_widened()
        vol = self.rot_vol48(now)
        race_vol = collections.Counter()
        for ex_, v_ in vol.items():
            race_vol[self.rot_race(ex_)] += v_
        race_sh = collections.Counter()
        for sub, (ex_, side_, q_, avg_) in self.rot_lots().items():
            race_sh[self.rot_race(ex_)] += q_
        for o in self.rot_bids():
            race_sh[self.rot_race(o["ex"])] += o["qty"]
        plan, races, ages = {}, set(), []                 # ages: (age key, ex, index in plan[ex], notional) for the reserve guard
        pcap = float(ROTATION.get("party_cap") or 0)
        net = self.party_view()["net"] if pcap else 0.0      # alpha_spec §5.5: net D - R at cost on close races
        def party_ok(ex, side, notional):
            nonlocal net
            if not pcap:
                return True
            p_ = self.party_dir(ex, side)
            delta = notional if p_ == "D" else -notional if p_ == "R" else 0.0
            if (delta > 0 and net + delta > pcap + 1e-6) or (delta < 0 and net + delta < -pcap - 1e-6):
                k_ = ("party", ex, side, int(net // 1000))
                if k_ not in self.rot_logged:
                    self.rot_logged.add(k_)
                    msg = (f"ROT PARTY CAP {self.name(ex)} {side.upper()} ({p_}): net {'D' if net >= 0 else 'R'} {abs(net):,.0f} at cost on close races; "
                           f"this bid ({notional:,.0f}) would pass the {pcap:,.0f} cap — not posted (alpha_spec §5.5)")
                    self.event("ROT", msg); self.night.line(msg)
                return False
            net += delta
            return True
        for o in self.rot_bids():
            ex, side = str(o["ex"]), o["side"]
            k = f"{ex}:{side}"
            bid, ask, fs = self.rot_touch(ex, side)
            if bid is None or fs is None or k in skips or self.rot_race(ex) in widened:
                continue
            posted = self.rot_posted.get(k)
            if posted is not None and fs < posted - ROTATION["pm_skip_c"] + 1e-9:
                with c.lock:
                    st.setdefault("skipped", {})[k] = now + ROTATION["pm_skip_s"]; c.d["rotation"] = st; c.save()
                self.rot_posted.pop(k, None)
                msg = f"ROT SKIP {self.name(ex)} {side.upper()}: PM fair moved {(posted - fs) * 100:.1f}c against the bid after posting ({posted:.3f} -> {fs:.3f}); pulled, no bid for 24 h"
                self.event("ROT", msg); self.night.line(msg)
                continue
            if fs - o["price"] < ROTATION["keep_gap"] - 1e-9:
                continue                                    # the gap at its price closed under 3c: pulled (no edge)
            px, qty = o["price"], int(o["qty"])
            if bid > o["price"] + 1e-9 and fs - bid >= ROTATION["keep_gap"] - 1e-9:
                q2 = int(min(o["qty"], (pool + o["qty"] * o["price"]) / bid))   # the re-peg carries only what the pool funds now
                if q2 >= clip:
                    pool -= q2 * bid - o["qty"] * o["price"]
                    px, qty = bid, q2
            if not party_ok(ex, side, qty * px):
                continue                                    # alpha_spec §5.5: a resting bid that would pass the party cap is pulled
            plan.setdefault(ex, []).append(dict(side=side, px=px, qty=qty, sub=f"rot:{ex}:{side}", ttl=ROT_TTL_S))
            ages.append((int(o["id"]) if str(o["id"]).isdigit() else 0, ex, len(plan[ex]) - 1, qty * px))
            races.add(self.rot_race(ex))
        if pcap and net > pcap:                              # §5.5: while net D > the cap, R-side coin-flip gaps (>= 4c) first
            cands = sorted([cd for cd in cands if self.party_dir(cd["ex"], cd["side"]) != "R" or cd["gap"] >= float(ROTATION.get("party_r_gap") or 0) - 1e-9],
                           key=lambda cd: (self.party_dir(cd["ex"], cd["side"]) != "R",))
        for cd in cands:
            race = cd["race"]
            if race in races or f"{cd['ex']}:{cd['side']}" in skips or race in widened:
                continue
            room = min(self.rot_cap(race) - rc[race], self.rot_total_cap() - total)
            sh_room = float(ROTATION.get("vol_cap_share", 0.10)) * race_vol.get(race, 0.0) - race_sh[race]   # fmm_spec §0.2: <= 10% of the race's 48-h shares
            room = min(room, max(0.0, sh_room) * cd["px"])
            if room < clip * cd["px"]:
                continue                                    # this race (or the whole book) is full
            if pool < clip * cd["px"]:
                break                                       # post only what the pool funds; the next candidates (and the 3c tier) wait
            qty = int(min(room, pool) / cd["px"])
            if not party_ok(cd["ex"], cd["side"], qty * cd["px"]):
                continue
            pool -= qty * cd["px"]; rc[race] += qty * cd["px"]; total += qty * cd["px"]; races.add(race)
            k = f"{cd['ex']}:{cd['side']}"
            self.rot_posted[k] = cd["fs"]
            k_log = (k, "posted")
            quiet = time.time() - self.rot_post_log.get(k_log, 0) < 600   # C95f: one POSTED line per bid per 10 min
            self.rot_post_log[k_log] = self.rot_post_log.get(k_log, 0) if quiet else time.time()
            msg = (f"ROT POSTED {self.name(cd['ex'])} {cd['side'].upper()}: bid {qty:,} at {cd['px']:.3f} (touch), PM fair {cd['fs']:.3f}, gap {cd['gap'] * 100:.1f}c "
                   f"({cd['pct']:.1%}{'' if cd['tier'] == 0 else ', the >= 3c tier'}), 48-h volume {cd['vol']:,.0f}; race {race} {rc[race]:,.0f} of {self.rot_cap(race):,.0f}, book {total:,.0f} of {ROTATION['total_cap']:,.0f}; pool left {pool:,.0f}")
            if not quiet:
                self.event("ROT", msg); self.night.line(msg)
            plan.setdefault(cd["ex"], []).append(dict(side=cd["side"], px=cd["px"], qty=qty, sub=f"rot:{cd['ex']}:{cd['side']}", ttl=ROT_TTL_S))
            ages.append((10 ** 18, cd["ex"], len(plan[cd["ex"]]) - 1, qty * cd["px"]))   # a new bid is the newest
            race_sh[race] += qty
        return self.rot_reserve_guard(plan, ages)

    def party_dir(self, ex, side):
        """alpha_spec §5.5: the party a lot wins with — D YES / R NO = 'D', R YES / D NO = 'R'; a non-D/R book (Alaska, independents) counts
        by the party it is against: the race's major party (the leading one when both are listed)."""
        p = self.pol(ex, side)
        if p in ("D", "R"):
            return p
        race = (self.meta.get(ex) or {}).get("race")
        majors = {}
        for e2, m in self.meta.items():
            if m.get("race") == race and m.get("party") in ("D", "R"):
                majors[m["party"]] = self.fair.get(e2) if self.fair.get(e2) is not None else 0.5
        if not majors:
            return "other"
        against = max(majors, key=lambda k: majors[k]) if len(majors) == 2 else next(iter(majors))
        other = "R" if against == "D" else "D"
        return other if side == "yes" else against           # YES on a book against R wins when R loses (counts D); its NO wins when R wins

    def party_view(self):
        """alpha_spec §5.5 (Piero 16:45 / 17:20 Oct 17): correlated close-race risk. Every SIG position on a close race (side fair 0.20-0.80) at
        cost, by the party it wins with; net = D - R. 1 sigma of the settlement value (each lot pays q x Bernoulli(fair)): independent, at the
        configured rho (0.5) between same-party lots (and -rho across), and fully correlated."""
        lo, hi = ROTATION.get("party_band") or (0.20, 0.80)
        rho = float(ROTATION.get("party_rho") or 0.5)
        d = r = 0.0; s_sum = 0.0; var = 0.0; rows = []
        for ex, b in self.book.items():
            q = float(b.get("pos") or 0)
            if abs(q) < 1:
                continue
            side = "yes" if q > 0 else "no"
            fs = self.side_fair(ex, side)
            if fs is None or not (lo <= fs <= hi):
                continue
            pol = self.party_dir(ex, side)
            if pol not in ("D", "R"):
                continue
            avg = float(((b.get("row") or {}).get("avgCost")) or 0) or float(self.sig_mark(ex, side) or fs)
            cost = abs(q) * avg; sig = abs(q) * math.sqrt(fs * (1 - fs))
            if pol == "D":
                d += cost; s_sum += sig
            else:
                r += cost; s_sum -= sig
            var += sig * sig
            rows.append((pol, self.name(ex), side, abs(q), cost))
        return dict(d=d, r=r, net=d - r, sigma_ind=math.sqrt(var), sigma_rho=math.sqrt(max(0.0, (1 - rho) * var + rho * s_sum * s_sum)),
                    sigma_corr=abs(s_sum), rho=rho, rows=rows)

    def party_line(self):
        v = self.party_view()
        cap = float(ROTATION.get("party_cap") or 0)
        lo, hi = ROTATION.get("party_band") or (0.20, 0.80)
        top = sorted(v["rows"], key=lambda x: -x[4])[:5]
        return (f"PARTY close races (side fair {lo:.2f}-{hi:.2f}): D {v['d']:,.0f} / R {v['r']:,.0f} at cost, net {'D' if v['net'] >= 0 else 'R'} {abs(v['net']):,.0f} (cap {cap:,.0f}); "
                f"1 sigma at settlement {v['sigma_ind']:,.0f} independent / {v['sigma_rho']:,.0f} at rho {v['rho']:.1f} / {v['sigma_corr']:,.0f} fully correlated; largest: "
                + ", ".join(f"{n} {sd.upper()} {c:,.0f} ({p})" for p, n, sd, q, c in top))

    def party_list(self, now):
        """alpha_spec §5.5: the Nov 1 18:00 decision list for Piero — every lot in a race unlikely to be called by Nov 4 09:00 PT (Alaska; close
        races in AZ / NV / CA / ME / OR / WA; the control markets), net party exposure and its 1 sigma, the gap to #50 and #25. Once."""
        c = self.controls
        at = ROTATION.get("party_list_at")
        if not at or time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(now)) < at or (c.d.get("party_list") or {}).get("done"):
            return
        slow = tuple(ROTATION.get("party_slow") or ())
        lo, hi = ROTATION.get("party_band") or (0.20, 0.80)
        rows = []
        for ex, b in self.book.items():
            q = float(b.get("pos") or 0)
            if abs(q) < 1:
                continue
            side = "yes" if q > 0 else "no"
            race = str((self.meta.get(ex) or {}).get("race") or "")
            fs = self.side_fair(ex, side)
            control = race.startswith("U.S. ") or str(ex) in (ROTATION.get("senate_exs") or ())
            close = fs is not None and lo <= fs <= hi
            if race.startswith("Alaska") or control or (close and race.startswith(slow)):
                avg = float(((b.get("row") or {}).get("avgCost")) or 0)
                rows.append(f"{self.name(ex)} {side.upper()} {abs(q):,.0f} @ {avg:.3f} (fair {fs if fs is None else round(fs, 3)}, {self.party_dir(ex, side)})")
        gaps = "n/a"
        try:
            with open(os.path.join(DATA, "leaders.csv")) as fh:
                lines = fh.readlines()[-200:]
            rr = [r for r in csv.reader(lines) if r and r[0] != "time"]
            last_t = max(r[0] for r in rr)
            cur = {r[1]: float(r[4]) for r in rr if r[0] == last_t}
            ours = next((float(r[4]) for r in rr if r[0] == last_t and r[-1] == "1"), None)
            if ours is not None:
                gaps = ", ".join(f"#{k} {cur[k]:+,.0f} ({ours - cur[k]:+,.0f})" for k in ("50", "25") if k in cur)
        except Exception:
            pass
        with c.lock:
            c.d["party_list"] = dict(done=stamp(), n=len(rows)); c.save()
        msg = f"NOV 1 DECISION LIST (alpha_spec §5.5): {len(rows)} lots in races unlikely to be called by Nov 4 09:00 PT — " + "; ".join(rows) + f". {self.party_line()}. Gap to {gaps}."
        self.event("PARTY", msg); self.night.line(msg); self.alerts.append(f"{time.strftime('%H:%M')} NOV 1 DECISION LIST in RUN_LOG ({len(rows)} lots)")

    def rot_reserve_guard(self, plan, ages):
        """fmm_spec §0.1 (the only exception to no-churn): every loop the rotation's (+ FMM's) resting and new bids must fit in
        cash - 5,000 - the manual reserve left; if they don't, the newest are pulled until they do."""
        room = self.engine_bid_room() - self.fmm_priority()   # fmm_spec v2 §2.5: FMM bids lock no cash; C97 item 2: the rotation is behind FMM's first 10k
        total = sum(a[3] for a in ages)
        if total <= room + 1e-6:
            return plan
        drop = set()
        for age, ex, i, notional in sorted(ages, key=lambda a: -a[0]):
            if total <= room + 1e-6:
                break
            drop.add((ex, i)); total -= notional
        out = {}
        for ex, ws in plan.items():
            keep = [w for i, w in enumerate(ws) if (ex, i) not in drop]
            if keep:
                out[ex] = keep
        k_ = ("guard", round(room, -2))
        if k_ not in self.rot_logged:
            self.rot_logged.add(k_)
            msg = (f"ROT GUARD: rotation bids pulled to fit the room (cash - 5,000 - manual reserve left = {room + self.fmm_bid_notional():,.0f}, FMM bids {self.fmm_bid_notional():,.0f}): "
                   + ", ".join(f"{self.name(ex)} #{i}" for ex, i in sorted(drop)))
            self.event("ROT", msg); self.night.line(msg)
        return out

    FMM_LAYERS = ("fmm", "fmmtake", "fmmhedge")              # FMM bids: not a cash lock (fmm_spec v2 §2.5), capped at K x free
    FMM_CFG = FMM

    def fmm_bids(self):
        return [o for o in self.open if o["action"] == "buy" and (self.ledger.orders.get(o["id"]) or {}).get("layer") in self.FMM_LAYERS]

    def fmm_bid_notional(self):
        """Resting FMM entry / take / hedge bids (notional)."""
        return sum(o["qty"] * o["price"] for o in self.fmm_bids())

    def rot_total_cap(self):
        """fmm_spec v2 §3.3: the rotation's 65k shrinks to 50k while FMM is live (its 15k inventory cap comes from the rotation pool)."""
        return float(FMM["rot_cap_live"]) if self.fmm_mode(time.time()) == "live" else float(ROTATION["total_cap"])

    # ------------------------------------------------------------------ FMM: favourite-side market maker + two-book locks (fmm_spec v2, Oct 16 18:45)
    def fmm_state(self):
        return self.controls.d.get("fmm") or {}

    def fmm_mode(self, now):
        """off (before Sat 06:00) -> paper (06:00-10:00, nothing sent) -> live / off by the 10:00 gate; stopped: no auto-restart."""
        st = self.fmm_state()
        if not FMM.get("on"):
            return "off"
        if st.get("stopped"):
            return "stopped"
        t = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(now))
        if t < FMM["paper_from"]:
            return "off"
        if t < FMM["live_at"]:
            return "paper"
        return st.get("decision") or "pending"

    def fmm_k(self):
        return float(self.fmm_state().get("K") or FMM["k_live"])

    def fmm_inv_cap(self):
        cap = float(self.fmm_state().get("inv_cap") or FMM["inv_cap_live"])
        if self.fmm_room_on():
            cap = max(cap, float(FMM_ROOM["inv_cap"]))       # C106: 40k until Oct 28 23:59
        return min(cap, self.wd_fmm_cap(time.time()))      # C103: the cash calendar's FMM wind-down (15k Oct 29, 5k Oct 31)

    def fmm_room_on(self, now=None):
        """C106: the larger FMM caps until Oct 28 23:59."""
        return bool(FMM_ROOM.get("on") and time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(now or time.time())) <= FMM_ROOM["until"])

    def fmm_race_cap(self, race):
        base = float(FMM_ROOM["race_cap"]) if self.fmm_room_on() else float(FMM["race_cap"])   # C106: 5k -> 8k a race (RI Senate stays 15k)
        return float((FMM.get("race_caps") or {}).get(race, base))

    def fmm_reserve_release(self):
        """Piero 18:30 item 4: once FMM runs (paper simulates it), his reserve counts as 2,000 for FMM only — the other 8,000 goes to FMM, not
        to the rotation or any other layer (they keep the full reserve)."""
        amt = float((self.controls.d.get("manual_reserve") or {}).get("amount") or 0.0)
        if amt <= 0 or self.fmm_mode(time.time()) not in ("paper", "live"):
            return 0.0
        live = max(0.0, min(float(FMM["manual_reserve_live"]), float(FMM["manual_reserve_live"]) - self.manual_spent()))
        return max(0.0, self.manual_reserve_left() - live)

    def fmm_free(self):
        """Free cash for FMM: cash - 5,000 - Piero's reserve left (2,000 for FMM: item 4), less the buys filled since the last cash read."""
        t0 = self.pnl_read_t
        self.fmm_unread = [(t, n) for t, n in self.fmm_unread if t >= t0]
        return self.engine_bid_room() + self.fmm_reserve_release() - sum(n for _, n in self.fmm_unread)

    def fmm_refill_measure(self):
        """C106 (Piero 02:50 Oct 22 item 2): the refill latency = the sweep's receipt on the feed -> the refill order placed (the ledger's
        placement time), plus the planning part (sweep -> planned)."""
        if not self.fmm_refill_wait:
            return
        recent = list(itertools.islice(reversed(self.ledger.orders.values()), 60))
        for ex, (recv, planned) in list(self.fmm_refill_wait.items()):
            o = next((r for r in recent if r.get("layer") == "fmmtake" and str(r.get("ex")) == str(ex) and float(r.get("ts") or 0) >= planned - 1), None)
            if o is not None:
                self.fmm_lat.append(float(o["ts"]) - recv); self.fmm_lat = self.fmm_lat[-500:]
                self.fmm_lat_plan.append(planned - recv); self.fmm_lat_plan = self.fmm_lat_plan[-500:]
                self.fmm_refill_wait.pop(ex, None)
            elif time.time() - planned > 60:
                self.fmm_refill_wait.pop(ex, None)

    def fmm_ring(self, now):
        hm = time.strftime("%H:%M", time.localtime(now))
        return any(a <= hm < b for a, b in FMM["ring"])

    def fmm_two(self, race):
        """{party: ex} when the race has exactly a D and an R book (locks / hedges)."""
        senate = ROTATION.get("senate_exs") or ()
        if race == "U.S. Senate control" and len(senate) == 2:
            return {"D": str(senate[0]), "R": str(senate[1])}
        d = {}
        for ex, m in self.meta.items():
            if self.rot_race(ex) == race:
                d.setdefault(str(m.get("party") or ""), []).append(str(ex))
        return {"D": d["D"][0], "R": d["R"][0]} if set(d) == {"D", "R"} and len(d["D"]) == len(d["R"]) == 1 else None

    def fmm_lots(self):
        """{sub: (ex, side, shares, avg)} FMM inventory, one sub-book per fill price ('fmm:<ex>:<side>:<px>'); locks excluded."""
        out = {}
        for ex, books in self.ledger.subbooks.items():
            for sub, q in books.items():
                if sub.startswith("fmm:") and abs(q) >= 1:
                    side = "yes" if q > 0 else "no"
                    tr = self.ledger.tranches.get(sub) or {}
                    avg = float(tr.get("avg") or 0) or float(sub.rsplit(":", 1)[-1]) if sub.count(":") >= 3 else float(tr.get("avg") or 0) or float(self.sig_mark(ex, side) or 0.5)
                    out[sub] = (str(ex), side, abs(q), avg)
        return out

    def fmm_lock_capital(self):
        """Cash in FMM locks: pair sets (a NO set costs 1 - margin a share) and hedge-made locks (shares - profit locked)."""
        st = (self.fmm_state().get("live") or {})
        sets = sum(bk["qty"] * float(bk.get("cost") or (1 - bk["margin"])) for bk in self.ledger.baskets if bk.get("fmm") and bk.get("state", "held") == "held")
        return sets + max(0.0, float(st.get("locks_sh") or 0) - float(st.get("locks_profit") or 0))

    def fmm_lock_room(self):
        """Piero 21:15 item 1: new two-book locks take only cash FMM isn't using — free less the guard's need (the two largest race ladders
        resting) and the resting hedges — and at most 30% of engine free cash (free before the locks) in all."""
        if self.wd_after("locks_stop", time.time()):
            return 0.0                                       # C103: no new locks / all-NO sets from Oct 29
        free = self.fmm_free()
        sets = sum(bk["qty"] * float(bk.get("cost") or (1 - bk["margin"])) for bk in self.ledger.baskets if bk.get("fmm") and bk.get("state", "held") == "held")
        by_race, hedge = collections.Counter(), 0.0
        if ALLOC.get("on"):                                  # C97 item 4b: every lock <= 10k at cost in all, only from idle cash (above FMM's first 10k and its ladders' need)
            for o in self.fmm_bids():
                if (self.ledger.orders.get(o["id"]) or {}).get("layer") == "fmmhedge":
                    hedge += o["qty"] * o["price"]
                else:
                    by_race[self.rot_race(o["ex"])] += o["qty"] * o["price"]
            need = sum(sorted(by_race.values(), reverse=True)[:2]) + hedge + self.carry_bid_notional()   # alpha_spec §4: after FMM's and carry's bids
            idle = free - self.fmm_claim_first(free) - need   # C101: FMM's claim is what it can deploy, <= 15k
            return max(0.0, min(idle, float(ALLOC["lock_cap"]) - self.fmm_lock_capital()))
        for o in self.fmm_bids():
            if (self.ledger.orders.get(o["id"]) or {}).get("layer") == "fmmhedge":
                hedge += o["qty"] * o["price"]
            else:
                by_race[self.rot_race(o["ex"])] += o["qty"] * o["price"]
        need = sum(sorted(by_race.values(), reverse=True)[:2]) + hedge
        return max(0.0, min(free - need, float(FMM["lock_free_share"]) * max(0.0, free + sets) - sets))

    def fmm_race_locked(self, race, legs=()):
        """Shares locked in a race (pair sets + hedge locks), for the 10,000-share cap."""
        sets = sum(bk["qty"] for bk in self.ledger.baskets if bk.get("fmm") and bk["race"] == race and bk.get("state", "held") == "held")
        exs = [str(e) for e in legs] or [str(ex) for ex in self.meta if self.rot_race(ex) == race]
        sub = sum(abs(float(q)) for ex in exs for s_, q in (self.ledger.subbooks.get(ex) or {}).items() if s_.startswith("fmmlock:"))
        return sets + sub / 2

    def fmm_others_ask(self, ex, side):
        """The best ask on `side` without our own resting asks (the feed's depth less our quantity at each level), else the touch."""
        own = collections.Counter()
        for o in self.open:
            if str(o["ex"]) == str(ex) and o["action"] == "buy" and o["side"] != side and self.ex.owned(o["id"]):
                own[round(1 - o["price"], 3)] += o["qty"]
        fb = self.feed_book(ex)
        if fb is not None:
            lv = fb[1] if side == "yes" else [(round(1 - p, 3), q) for p, q in fb[0]]
            for p, q in lv:
                if q - own.get(round(p, 3), 0) >= 1:
                    return round(p, 3)
            return None
        return self.rot_touch(ex, side)[1]

    def fmm_targets(self, now, paper):
        """fmm_spec v2 §2.1: -> [(race, ex, side, px, qty, layer, sub)]. Every eligible favourite-side book (fair >= 0.75, touch <= fair - 2c)
        gets the touch; the race's cheaper book the ladder (touch, -1, -2, -3c). Race resting budget = K x free x race share of 48-h shares,
        clip max(500, budget / 4); total resting <= K x free; no clip bigger than free (the fill guard's rule); resting + inventory inside the
        race cap (5k at cost) and the inventory cap (15k / 30k), so a fill never breaks a cap. Kept bids keep their place; the largest go
        first when they no longer fit. Paper: the same plan against the live feed, with the paper inventory."""
        K, cap, edge = self.fmm_k(), self.fmm_inv_cap(), float(FMM["edge"])
        rcap = self.fmm_race_cap                             # per race: 5k, Rhode Island Senate 15k (Piero 21:15 item 2)
        P = self.fmm_paper
        free = self.fmm_free() - (sum(q * p for _, _, q, p in P["lots"]) if paper else 0.0)
        allow = K * max(0.0, free)
        vol = self.rot_vol48(now)
        hand = {str(o["ex"]) for o in self.open if not self.ex.owned(o["id"])}   # Piero's own resting orders (RI-GOV-D): FMM stays out of those books
        books = collections.defaultdict(list)
        cf = set()                                           # C101 item B: the coin-flip books (side fair 0.25-0.75): the touch only, <= fair - 3c, 500 a clip, 3k a race
        party = dict(net=self.party_view()["net"]) if (ROTATION.get("party_cap") and FMM_CF.get("on") and not paper) else None
        for ex, b in self.owner.items():
            ex = str(ex)
            if b.kind != "market" or ex in self.manual_ex or ex in hand or self.ex.is_frozen(ex) or ex in SELL_HOLD:
                continue
            f = self.fair.get(ex)
            if f is None:
                continue
            side = "yes" if f >= FMM["fav_min"] else "no" if f <= 1 - FMM["fav_min"] else None
            pos_ = float((self.book.get(ex) or {}).get("pos") or 0)
            if side is None:
                if not FMM_CF.get("on"):
                    continue
                best = None
                for sd in ("yes", "no"):
                    if not paper and ((sd == "yes" and pos_ <= -1) or (sd == "no" and pos_ >= 1)):
                        continue                             # C95f
                    bid_, ask_, fs_ = self.rot_touch(ex, sd)
                    if (bid_ is None or fs_ is None or not (FMM_CF["band"][0] - 1e-9 <= fs_ <= FMM_CF["band"][1] + 1e-9) or bid_ > fs_ - FMM_CF["edge"] + 1e-9
                            or not (0.01 <= bid_ <= 0.99)):
                        continue
                    if best is None or fs_ - bid_ > best[2] - best[3]:
                        best = (sd, ask_, fs_, bid_)
                if best is None or not self.carry_party_ok(ex, best[0], FMM_CF["clip"] * best[3], party):
                    continue                                 # the party cap holds (no new D-side close-race buy while net D > 10k)
                side, ask, fs, bid = best
                cf.add((ex, side))
                books[self.rot_race(ex)].append((ex, side, fs, bid, ask))
                continue
            if not paper and ((side == "yes" and pos_ <= -1) or (side == "no" and pos_ >= 1)):
                continue                                     # C95f: we hold the other side here; a buy would net against it
            bid, ask, fs = self.rot_touch(ex, side)
            if bid is None or fs is None or bid > fs - edge + 1e-9 or not (0.01 <= bid <= 0.99):
                continue
            books[self.rot_race(ex)].append((ex, side, fs, bid, ask))
        rvol = collections.Counter()
        for ex_, v_ in vol.items():
            rvol[self.rot_race(ex_)] += v_
        tot = sum(rvol[r] for r in books) or 1.0
        inv, inv_cf = collections.Counter(), collections.Counter()
        lo_cf, hi_cf = FMM_CF["band"]
        if paper:
            for ex, side, q, px in P["lots"]:
                inv[self.rot_race(ex)] += q * px
        else:
            for sub, (ex, side, q, avg) in self.fmm_lots().items():
                inv[self.rot_race(ex)] += q * avg
                sf_ = self.side_fair(ex, side)
                if sf_ is not None and lo_cf - 1e-9 <= sf_ <= hi_cf + 1e-9:
                    inv_cf[self.rot_race(ex)] += q * avg
        inv_tot = sum(inv.values())
        rcf = float(FMM_ROOM["cf_race_cap"]) if self.fmm_room_on(now) else float(FMM_CF["race_cap"])   # C106: 3k -> 4k a coin-flip race
        mclip = float(FMM_ROOM["min_clip"]) if self.fmm_room_on(now) else float(FMM["min_clip"])      # C106: clips scale with the caps
        cfclip = float(FMM_ROOM["cf_clip"]) if self.fmm_room_on(now) else float(FMM_CF["clip"])
        # C101 item B: what FMM can still deploy under its caps (its claim on free cash is min(15k, this))
        self.fmm_deploy = max(0.0, min(cap - inv_tot, sum(max(0.0, (rcap(r) if any((l[0], l[1]) not in cf for l in legs) else min(rcap(r), rcf - inv_cf[r] + inv[r])) - inv[r])
                                                            for r, legs in books.items())))
        cur = collections.defaultdict(dict)                  # (ex, side) -> {px: (qty, age key)}
        if paper:
            for (ex, side, px), o in P["orders"].items():
                cur[(ex, side)][px] = (o["qty"] - o["filled"], o["placed"])
        else:
            for o in self.open:
                rec = self.ledger.orders.get(o["id"]) or {}
                if o["action"] == "buy" and rec.get("layer") == "fmm":
                    cur[(str(o["ex"]), o["side"])][round(o["price"], 3)] = (o["qty"], int(o["id"]) if str(o["id"]).isdigit() else 0)
        # 1. what each book wants (levels), the kept bids first
        plan, kept = [], []
        for race, legs in books.items():
            fav = [l for l in legs if (l[0], l[1]) not in cf]
            ladder = ([l for l in fav if len(cur.get((l[0], l[1]), {})) > 1] or [max(fav, key=lambda l: l[2] - l[3])]) if fav else [(None,)]   # the cheaper book, sticky while its ladder rests
            sw = self.fmm_sweep.get(race)
            for ex, side, fs, bid, ask in legs:
                lv = list(FMM["levels"]) if ex == ladder[0][0] else [0]   # coin-flip books: the touch only
                refill = sw is not None and sw.get("ex") == ex and now - sw["t"] <= FMM["refill_s"]
                if refill:                                   # §2.2 (b): every level the sweep cleared, down to fair - 2c
                    lv = list(range(0, max(lv[-1], int((bid - (fs - edge)) / FMM["level_step"] + 1e-9)) + 1))
                own = None if paper else self.own_ask(ex, side)
                for k in lv:
                    px = round(bid - k * FMM["level_step"], 3)
                    if px > fs - edge + 1e-9 or px < 0.01 or (own is not None and px >= own - 1e-9):
                        continue                             # never above fair - 2c; never at or through our own ask in the same book
                    if (ex, side) in cf and px > fs - FMM_CF["edge"] + 1e-9:
                        continue                             # C101: a coin-flip bid sits <= PM fair - 3c
                    have = cur.get((ex, side), {}).get(px)
                    (kept if have else plan).append(dict(race=race, ex=ex, side=side, px=px, k=k, fs=fs, ask=ask, have=have, refill=refill and (ex, side) not in cf,
                                                         cf=(ex, side) in cf))
        # 2. kept bids that no longer fit go, the largest first (the planner applies the fill guard's rule every loop)
        kept.sort(key=lambda w: -w["have"][0] * w["px"])
        rest_tot = sum(w["have"][0] * w["px"] for w in kept)
        rest_race, rest_cf = collections.Counter(), collections.Counter()
        for w in kept:
            rest_race[w["race"]] += w["have"][0] * w["px"]
            if w.get("cf"):
                rest_cf[w["race"]] += w["have"][0] * w["px"]
        out, cut = [], []
        other = lambda rc: max([v for r_, v in rest_race.items() if r_ != rc] + [0.0])   # a burst spans <= 2 races (replay): the two largest ladders must fit in free
        for w in kept:
            n = w["have"][0] * w["px"]
            if (n > max(0.0, free) + 1e-6 or rest_race[w["race"]] + other(w["race"]) > max(0.0, free) + 1e-6 or rest_tot > allow + 1e-6
                    or inv[w["race"]] + rest_race[w["race"]] > rcap(w["race"]) + 1e-6 or inv_tot + rest_tot > cap + 1e-6
                    or (w.get("cf") and inv_cf[w["race"]] + rest_cf[w["race"]] > rcf + 1e-6)):
                rest_tot -= n; rest_race[w["race"]] -= n; cut.append(w)
                if w.get("cf"):
                    rest_cf[w["race"]] -= n
                continue
            out.append((w["race"], w["ex"], w["side"], w["px"], int(w["have"][0]), "fmm", f"fmm:{w['ex']}:{w['side']}:{w['px']:.3f}"))
        if cut and not paper:
            self.fmm_note_guard(f"FMM GUARD (plan): {len(cut)} bids / {sum(w['have'][0] * w['px'] for w in cut):,.0f} pulled, largest first, to fit free {free:,.0f} "
                                f"(K {K:g} x free = {allow:,.0f}; caps {FMM['race_cap']:,.0f} a race (RI Senate {rcap(RI_SENATE_RACE):,.0f}) / {cap:,.0f})")
        # hedges (§2.4 b): a NO lot gets a bid on the other book's NO at <= 0.990 - its cost (live only); stuck lots (held >= 2 h) first,
        # before any new bid, so they turn into riskless locks instead of more cash going out (Piero 21:15 item 1)
        lock_box = [float(ALLOC["lock_cap"]) - self.fmm_lock_capital() if ALLOC.get("on") else 1e18]   # C97 item 4b: hedge locks count in the 10k

        def hedges(stuck):
            nonlocal rest_tot
            if not FMM.get("hedge") or paper:
                return
            for sub, (lex, lside, lq, lavg) in sorted(self.fmm_lots().items(), key=lambda kv: float((self.ledger.tranches.get(kv[0]) or {}).get("ts") or now)):
                age = now - float((self.ledger.tranches.get(sub) or {}).get("ts") or now)
                if lside != "no" or (age >= FMM["hedge_after_s"]) != stuck:
                    continue
                race = self.rot_race(lex); two = self.fmm_two(race)
                if not two or lex not in two.values():
                    continue
                other = two["R"] if lex == two["D"] else two["D"]
                if other in hand or self.ex.is_frozen(other) or float((self.book.get(other) or {}).get("pos") or 0) >= 1:
                    continue
                hpx = bots.tick_down(round(FMM["lock_cost"] - lavg, 6))
                hq = int(min(lq, FMM["lock_cap_sh"] - self.fmm_race_locked(race, (two["D"], two["R"])), (allow - rest_tot) / max(hpx, 0.01), max(0.0, free) / max(hpx, 0.01),
                             max(0.0, lock_box[0]) / max(lavg + hpx, 0.01)))
                oa = self.own_ask(other, "no")
                if 0.01 <= hpx <= 0.99 and hq >= MIN_QTY and not (oa is not None and hpx >= oa - 1e-9):
                    out.append((race, other, "no", hpx, hq, "fmmhedge", f"fmmlock:{race}:{lex}:{lavg:.3f}"))
                    rest_tot += hq * hpx; lock_box[0] -= hq * (lavg + hpx)
        hedges(stuck=True)
        # 3. sweep takes (§2.2 a) and new bids, the races with the most volume first
        allow_new = paper or api.writes_left() > int(FMM["fast_writes"])   # §2.5: 5 writes / min kept for the fast lane (refill, guard, exits)
        order = sorted(plan, key=lambda w: (w["k"], -rvol[w["race"]], w["race"]))   # every eligible book's touch first, then the deeper levels
        taken = set()
        touch_new = [w for w in order if w["k"] == 0]          # C104 (Piero 15:15 Oct 21 item B: >= 80 books in day hours): when 500-share touch clips
        room0 = min(cap - inv_tot - rest_tot, allow - rest_tot)   # can't all fit the room, every new touch bid gets an equal share of it (>= 100 shares)
        breadth = (room0 / len(touch_new) / (sum(w["px"] for w in touch_new) / len(touch_new))) if touch_new and room0 > 0 else None
        for w in order:
            race, ex, side, px, fs = w["race"], w["ex"], w["side"], w["px"], w["fs"]
            budget = min(K * max(0.0, free) * rvol[race] / tot, rcap(race) - inv[race])   # the race's resting budget, never more than its cap leaves
            room = min(rcap(race) - inv[race] - rest_race[race], cap - inv_tot - rest_tot, allow - rest_tot, max(0.0, free) - rest_race[race] - other(race))   # a sweep takes a race's whole ladder at once: the two largest must fit in free
            sw = self.fmm_sweep.get(race)
            if w["refill"] and sw is not None and not sw.get("taken") and (race, ex) not in taken and w["ask"] is not None and w["ask"] <= fs - edge + 1e-9 and room > 0:
                taken.add((race, ex)); sw["taken"] = True
                q = int(min(room, budget if budget > 0 else room) / w["ask"])
                fb = self.feed_book(ex)
                if fb is not None:
                    lv_ = fb[1] if side == "yes" else [(round(1 - p, 3), q_) for p, q_ in fb[0]]
                    q = int(min(q, sum(q_ for p, q_ in lv_ if p <= fs - edge + 1e-9)))
                if q >= MIN_QTY:
                    out.append((race, ex, side, round(min(fs - edge, w["ask"]), 3), q, "fmmtake", f"fmm:{ex}:{side}:{round(min(fs - edge, w['ask']), 3):.3f}"))
                    rest_tot += q * w["ask"]; rest_race[race] += q * w["ask"]; room -= q * w["ask"]
                    self.fmm_refill_wait[ex] = (float(sw["t"]), time.time())   # C106: measured to the order placed (was: to the loop's start, < 0 mid-loop)
                    msg = f"FMM REFILL {self.name(ex)} {side.upper()}: take {q:,} at <= {min(fs - edge, w['ask']):.3f} (fair {fs:.3f}) {now - sw['t']:.1f} s after the sweep ({'paper' if paper else 'live'})"
                    self.event("FMM", msg); self.night.line(msg)
            if not allow_new:
                continue
            clip = max(mclip, budget / 4 / px)
            if w["k"] == 0 and breadth is not None and breadth < mclip:
                clip = max(float(FMM["min_new"]), breadth)      # C104: breadth first
            lim = [clip, room / px]
            if w.get("cf"):
                lim += [cfclip, (rcf - inv_cf[race] - rest_cf[race]) / px]   # C101 / C106: the coin-flip clip and race cap
            if w["k"] > 0:
                lim.append((budget - rest_race[race]) / px)  # the touch is every eligible book's floor; deeper levels within the race budget
            q = int(min(lim))
            if q < FMM["min_new"]:
                continue
            out.append((race, ex, side, px, q, "fmm", f"fmm:{ex}:{side}:{px:.3f}"))
            rest_tot += q * px; rest_race[race] += q * px
            if w.get("cf"):
                rest_cf[race] += q * px
        hedges(stuck=False)
        quoted = {(t[1], t[2]) for t in out if t[5] == "fmm"}
        self.fmm_cov = dict(eligible=sum(len(v) for v in books.values()), quoted=len(quoted), rest=round(rest_tot), allow=round(allow), free=round(free), K=K,
                            cf_eligible=len(cf), cf_quoted=len({k_ for k_ in quoted if k_ in cf}), deploy=round(self.fmm_deploy))
        return out

    def fmm_plan(self, now):
        """-> {ex: [want-spec]} in live mode; in paper mode the same targets become simulated orders and nothing is sent."""
        mode = self.fmm_mode(now)
        self.track_fmm(now)                                  # exits keep running whatever the mode (live lots only)
        if mode not in ("paper", "live") or self.fills_blind or (mode == "live" and (self.controls.paused or self.controls.state("fmm") == "off")):
            return {}
        if self.wd_after("fmm_no_bids", now):
            return {}                                        # C103: no new FMM bids from Nov 1 12:00 (exits only)
        tg = self.fmm_targets(now, paper=(mode == "paper"))
        self.fmm_refill_measure()
        if mode == "paper":
            self.fmm_paper_place(now, tg)
            return {}
        plan = {}
        for race, ex, side, px, q, layer, sub in tg:
            plan.setdefault(ex, []).append(dict(side=side, px=px, qty=q, sub=sub, ttl=FMM["take_ttl_s"] if layer == "fmmtake" else 86400, layer=layer))
        return plan

    def track_fmm(self, now):
        """§2.3 v2: every FMM lot (one per fill price) rests a GTC ask at max(fill + 0.5c (ring hours + 1c), the best ask), never below
        lot avg + 0.5c; re-pegged down to a new best ask, never below fill + 0.5c (exempt from the 10-min keep-place grace)."""
        self.fmm_view = {}
        ring = self.fmm_ring(now)
        cur = {}
        for o in self.open:
            rec = self.ledger.orders.get(o["id"]) or {}
            if rec.get("layer") == "fmmexit":
                cur[rec.get("sub")] = round(1 - o["price"], 3)
        for sub, (ex, side, q, avg) in self.fmm_lots().items():
            if q < MIN_QTY:
                continue
            lots = [l for l in self.lots(ex, side, sub) if l[0] >= 1 and l[2] == "engine"]
            top = max([l[1] for l in lots] + [avg])
            floor_ = max(bots.tick_up(round(top + FMM["exit_c"], 6)), bots.tick_up(round(avg + RULE0_MARGIN, 6)))
            first = max(floor_, bots.tick_up(round(top + (FMM["exit_ring_c"] if ring else FMM["exit_c"]), 6)))
            oa = self.fmm_others_ask(ex, side)
            have = cur.get(sub)
            if self.gapmm_live():                            # C99c: the gap-median exit, live after the 10:00 gate (docs/gapmm.md §7)
                gpx = self.gap_exit_px(ex, side, top, avg, now)
                gfl = self.gap_floor(top, avg)
                if have is None:
                    px = gpx if gpx is not None else gfl
                else:
                    px = self.gap_keep(("live", sub), have, gpx, gfl, now)
            elif have is not None and have >= floor_ - 1e-9:
                px = min(have, max(first, oa)) if oa is not None and oa < have - 1e-9 else have   # C100: down to what a new post would take (ring hours: fill + 1c),
            else:                                            # never up: IA-01 07:18-08:01 Oct 18 flipped 0.825 (new) <-> 0.820 (re-peg) each loop, 166 cancels
                px = max(first, oa) if oa is not None else first
            self.fmm_view.setdefault(ex, []).append(dict(sub=sub, side=side, px=min(px, 0.99), avg=avg))

    def gap6(self, now):
        """{ex: median of (print price - PM fair at the print), YES terms, over the last 6 h} from the tape files (every 10 min)."""
        c = getattr(self, "_gap6", None)
        if c and now - c[0] < GAPMM["refresh_s"]:
            return c[1]
        gaps, cut = collections.defaultdict(list), now - GAPMM["window_s"]
        for d in range(2):
            fn = os.path.join(DATA, "intel", "trades", time.strftime("%Y-%m-%d", time.localtime(now - 86400 * d)) + ".jsonl")
            try:
                with open(fn) as fh:
                    for line in fh:
                        try:
                            r = json.loads(line)
                        except ValueError:
                            continue
                        if (r.get("ts") or 0) >= cut and r.get("p") is not None and r.get("fair") is not None:
                            gaps[str(r.get("ex"))].append(float(r["p"]) - float(r["fair"]))
            except OSError:
                continue
        self._gap6 = (now, {ex: statistics.median(v) for ex, v in gaps.items() if len(v) >= int(GAPMM.get("min_prints") or 1)})   # docs/gapmm.md §7: >= 10 prints
        return self._gap6[1]

    def gap_floor(self, top, avg):
        return max(bots.tick_up(round(top + FMM["exit_c"], 6)), bots.tick_up(round(avg + RULE0_MARGIN, 6)))

    def gap_exit_px(self, ex, side, top, avg, now):
        """F1 (docs/gapmm.md §7): max(fill + 0.5c, lot avg + 0.5c, side fair + the 6-h median gap on that side), the target rounded up to the
        tick; None without a fair or a median (< 10 prints)."""
        fs = self.side_fair(ex, side)
        g = self.gap6(now).get(str(ex))
        if fs is None or g is None:
            return None
        g_side = g if side == "yes" else -g
        return min(max(self.gap_floor(top, avg), bots.tick_up(round(fs + g_side, 6))), 0.99)

    def gap_keep(self, key, have, target, floor_, now):
        """§7.6 keep-place: the ask moves to the target only after the target has differed from it by >= 1 tick for > 10 min without a
        break (either direction, never below the floor); without a target it stays where it is."""
        away = self._gap_away = getattr(self, "_gap_away", {})
        if target is None or abs(target - have) < 0.005 - 1e-9:
            away.pop(key, None)
            return max(have, floor_)
        t0 = away.setdefault(key, now)
        if now - t0 > float(GAPMM.get("repeg_s") or 600):
            away.pop(key, None)
            return max(target, floor_)
        return max(have, floor_)

    def gapmm_live(self):
        return bool(GAPMM.get("on") and (self.fmm_state().get("gapmm") or {}).get("live"))

    def gapmm_paper_on(self, now):
        t = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(now))
        return bool(GAPMM.get("on")) and GAPMM["paper_from"] <= t < GAPMM["live_at"] and self.fmm_mode(now) == "live"

    def gapmm_paper_step(self, now):
        """06:00-10:00: every live FMM lot gets a paper ask at the F1 price (depth ahead from the feed's book when placed; re-placed when the
        price moves a tick); fills come from the prints (fmm_paper_print's half rate). Nothing is sent."""
        P = self.gap_paper
        live_subs = set()
        for sub, (ex, side, q, avg) in self.fmm_lots().items():
            if q < MIN_QTY:
                continue
            live_subs.add(sub)
            lots = [l for l in self.lots(ex, side, sub) if l[0] >= 1 and l[2] == "engine"]
            top = max([l[1] for l in lots] + [avg])
            target = self.gap_exit_px(ex, side, top, avg, now)
            a = P["asks"].get(sub)
            if a is None:
                px = target if target is not None else self.gap_floor(top, avg)   # at the fill: max(floor, target), the floor without one
            else:
                px = self.gap_keep(("paper", sub), a["px"], target, self.gap_floor(top, avg), now)
                if abs(a["px"] - px) < 1e-9:
                    continue
            fb = self.fmm_paper_book(ex)
            ahead = 0.0
            if fb is not None:
                lv = fb[1] if side == "yes" else [(round(1 - p_, 3), q_) for p_, q_ in fb[0]]
                ahead = sum(q_ for p_, q_ in lv if abs(p_ - px) < 1e-9)
            sold = a["sold"] if a else 0.0
            P["asks"][sub] = dict(ex=ex, side=side, px=px, qty=(a["qty"] if a else q), cost=avg, ahead=ahead, cum=0.0, sold=sold, placed=now)
        for sub in [s_ for s_, a in P["asks"].items() if s_ not in live_subs and a["sold"] < a["qty"] - 1]:
            P["asks"].pop(sub, None)                         # the lot left FMM (sold or locked): no paper ask without a lot

    def gapmm_paper_print(self, ex, ts, p, q):
        P = self.gap_paper
        for sub, a in P["asks"].items():
            if a["ex"] != ex or ts < a["placed"] or a["sold"] >= a["qty"] - 1e-9:
                continue
            ps = p if a["side"] == "yes" else 1 - p
            if ps >= a["px"] - 1e-9:
                a["cum"] += q
                new = min(a["qty"], max(0.0, a["cum"] - a["ahead"]) * 0.5) - a["sold"]
                if new >= 1:
                    a["sold"] += new; P["fills"] += 1; P["realized"] += new * (a["px"] - a["cost"])
                    if a["sold"] >= a["qty"] - 1:
                        P["done"] += 1
                    try:
                        self.intel_write(f"paper/gapmm-{time.strftime('%Y-%m-%d')}.jsonl", dict(t=stamp(), ev="exit", ex=ex, name=self.name(ex), side=a["side"], px=a["px"],
                                                                                              qty=round(new), cost=a["cost"], realized=round(new * (a["px"] - a["cost"]), 2)))
                    except Exception:
                        pass

    def gapmm_gate(self, now):
        """10:00: live on FMM exits if the replay passed (docs/gapmm.md) and paper realized >= 0; logged once."""
        st = self.controls.d.setdefault("fmm", {})
        g = st.setdefault("gapmm", {})
        if g.get("decided") or not GAPMM.get("on") or time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(now)) < GAPMM["live_at"]:
            return
        P = self.gap_paper
        ok = bool(GAPMM.get("replay_ok")) and P["realized"] >= -1e-9 and self.fmm_mode(now) == "live"
        with self.controls.lock:
            g.update(decided=stamp(), live=ok, paper=dict(realized=round(P["realized"], 2), fills=P["fills"], done=P["done"])); self.controls.save()
        msg = (f"GAPMM GATE {time.strftime('%H:%M', time.localtime(now))}: {'LIVE on FMM exits' if ok else 'stays on paper'} — {GAPMM['replay_note']}; "
               f"paper 06:00-10:00: {P['fills']} fills, {P['done']} lots out, realized {P['realized']:+,.1f}" + ("" if self.fmm_mode(now) == "live" else f"; FMM is {self.fmm_mode(now)}"))
        self.event("FMM", msg); self.night.line(msg)

    def gapmm_line(self, now):
        P = self.gap_paper
        mode = "live" if self.gapmm_live() else "paper" if self.gapmm_paper_on(now) else "off"
        return f"GAPMM ({mode}) exits at max(fill + 0.5c, fair + 6-h median gap): paper asks {len(P['asks'])}, fills {P['fills']}, lots out {P['done']}, paper realized {P['realized']:+,.1f}"

    def fmm_note_guard(self, msg):
        k = ("guard", msg[:40], int(time.time() // 300))
        if k not in self.fmm_logged:
            self.fmm_logged.add(k)
            self.event("FMM", msg); self.night.line(msg)

    def fmm_guard(self, now, why):
        """§2.5 fill guard, run when the loop books an FMM fill (docs/latency.md: ~6 s p50 / ~12 s p90 after SIG's match): if free (cash - floor
        - reserve, less buys not yet in a cash read) is under the largest resting FMM clip or the two largest race ladders, or resting > K x free,
        pull FMM bids, the largest first, until the rest fit."""
        free = self.fmm_free(); K = self.fmm_k()
        cash0 = self.engine_bid_room()
        bids = sorted(self.fmm_bids(), key=lambda o: -o["qty"] * o["price"])
        total = sum(o["qty"] * o["price"] for o in bids)
        by_race = collections.Counter()
        for o in bids:
            if (self.ledger.orders.get(o["id"]) or {}).get("layer") != "fmmhedge":
                by_race[self.rot_race(o["ex"])] += o["qty"] * o["price"]
        pulled = []
        for o in bids:                                       # the replay: a sweep fills a race's whole ladder in one burst, <= 2 races inside the guard's ~6-12 s
            n = o["qty"] * o["price"]; rc = self.rot_race(o["ex"])
            hedge = (self.ledger.orders.get(o["id"]) or {}).get("layer") == "fmmhedge"
            two = by_race[rc] + max([v for r_, v in by_race.items() if r_ != rc] + [0.0])
            if n > max(0.0, free) + 1e-6 or total > K * max(0.0, free) + 1e-6 or (not hedge and two > max(0.0, free) + 1e-6):
                if self.ex.cancel(o, reason="FMM GUARD", front=True):
                    pulled.append(n); total -= n
                    if not hedge:
                        by_race[rc] -= n
        if pulled:
            msg = (f"FMM GUARD ({why}): free {cash0:,.0f} at the last cash read -> {free:,.0f} after the fills; pulled {len(pulled)} bids / {sum(pulled):,.0f} "
                   f"(largest first), {total:,.0f} left resting (<= K {K:g} x free; every clip and the two largest race ladders <= free): the 5,000 floor and Piero's reserve hold")
            self.event("FMM", msg); self.night.line(msg)
        return pulled

    def fmm_funds(self, what, raw=None):
        """§2.5: a SIG funds / balance event (a write refused for funds, a buy accepted as a no-op, a bid SIG shrank or cancelled, a balance < 0)."""
        c = self.controls
        with c.lock:
            st = c.d.setdefault("fmm", {})
            ev = st.setdefault("funds", [])
            ev.append(dict(t=stamp(), what=str(what)[:200], cash=None if raw is None else round(raw, 2)))
            st["funds"] = ev[-50:]
            neg = raw is not None and raw < 0
            if neg and self.fmm_k() > 1:
                st["K"] = 1.0
            c.save()
        msg = f"FMM FUNDS: {str(what)[:200]}" + ("" if raw is None else f"; SIG cash {raw:,.0f}") + ("; cash below 0: K -> 1 at once (report to Piero)" if raw is not None and raw < 0 else "")
        self.event("FMM", msg); self.night.line(msg)
        if raw is not None and raw < 0:
            self.alerts.append(f"{time.strftime('%H:%M')} {msg}")

    @staticmethod
    def fill_key(f):
        return str(f.get("key") or f"{f.get('t')}|{f.get('order')}|{f.get('qty')}|{f.get('price')}")

    def fmm_fill_events(self, fills):
        """Live FMM fills: per-price lots (tranche avg), exits' realized and round trips, hedge fills -> locks; then the fill guard.
        Each fill once: the loop hands every recent fill of an order that filled again (MI-07, Oct 16 21:31) — seen keys persist."""
        st = self.controls.d.setdefault("fmm", {})
        lv = st.setdefault("live", dict(fills=0, realized=0.0, rt=0, locks_sh=0.0, locks_profit=0.0))
        seen = st.setdefault("seen", [])
        seen_set = set(seen)
        changed, entry = False, False
        for f in fills:
            lay = f.get("layer")
            if lay in ("fmm", "fmmtake", "fmmexit", "fmmhedge"):
                k = self.fill_key(f)
                if k in seen_set:
                    continue
                seen.append(k); seen_set.add(k)
            elif self.fill_key(f) in self.fmm_cash_seen:
                continue
            if lay not in ("fmm", "fmmtake", "fmmexit", "fmmhedge"):
                self.fmm_cash_seen.add(self.fill_key(f))
                if f.get("action") == "buy" and lay not in self.REDUCING:
                    self.fmm_unread.append((time.time(), f["qty"] * f["price"]))   # any entry buy spends cash the fill guard must see
                continue
            changed = True
            ex = str(f["ex"]); lv["fills"] += 1
            rec = self.ledger.orders.get(str(f.get("order") or "")) or {}
            sub = rec.get("sub") or ""
            if lay in ("fmm", "fmmtake"):
                entry = True
                self.fmm_unread.append((time.time(), f["qty"] * f["price"]))
                sub = sub or f"fmm:{ex}:{f['side']}:{f['price']:.3f}"
                tr = self.ledger.tranches.get(sub)
                if tr is None or tr.get("state") != "open":
                    tr = self.ledger.tranches[sub] = dict(ex=ex, side=f["side"], kind="fmm", t=stamp(), ts=time.time(), state="open", avg=f["price"], qty=0.0)
                qa = float(tr.get("qty") or 0.0)
                tr["avg"] = round((float(tr["avg"]) * qa + f["price"] * f["qty"]) / (qa + f["qty"]), 5) if qa + f["qty"] > 0 else f["price"]
                tr["qty"] = qa + f["qty"]
                q = abs(float((self.ledger.subbooks.get(ex) or {}).get(sub, 0.0)))
                msg = f"FMM FILL {self.name(ex)} {f['side'].upper()} {f['qty']:,.0f} at {f['price']:.3f} ({lay}; fair {self.side_fair(ex, f['side']) or 0:.3f}); lot {q:,.0f} at {tr['avg']:.3f}"
            elif lay == "fmmexit":
                lv["realized"] += f.get("realized") or 0.0
                left = abs(float((self.ledger.subbooks.get(ex) or {}).get(sub, 0.0))) if sub else 0.0
                msg = f"FMM EXIT {self.name(ex)} {('NO' if f['side'] == 'yes' else 'YES')} {f['qty']:,.0f} at {1 - f['price']:.3f}: realized {(f.get('realized') or 0):+,.0f}; lot left {left:,.0f}"
                if sub and left < MIN_QTY:
                    lv["rt"] += 1
                    tr = self.ledger.tranches.get(sub)
                    if tr is not None:
                        tr.update(state="closed", closed_ts=time.time())
                    msg += " (round trip done)"
            else:                                            # a hedge filled: the NO lot on the other book and this NO are a lock
                parts = sub.split(":")                       # fmmlock:<race>:<lot ex>:<lot avg>
                race = self.rot_race(ex)
                lot_ex = parts[-2] if len(parts) >= 4 else None
                lot_avg = float(parts[-1]) if len(parts) >= 4 else 0.0
                lot_sub = f"fmm:{lot_ex}:no:{lot_avg:.3f}" if lot_ex else None
                lot_q = abs(float((self.ledger.subbooks.get(lot_ex) or {}).get(lot_sub, 0.0))) if lot_ex else 0.0
                mq = min(lot_q, f["qty"])
                if mq >= 1:
                    self.ledger.split_sub(lot_ex, lot_sub, f"fmmlock:{race}", -mq)
                    lv["locks_sh"] += mq; lv["locks_profit"] += mq * (1 - (lot_avg + f["price"]))
                if f["qty"] - mq >= 1:                       # the lot sold meanwhile: the hedge's excess is an FMM lot of its own (it gets an exit)
                    xs = f"fmm:{ex}:no:{f['price']:.3f}"
                    self.ledger.split_sub(ex, sub, xs, -(f["qty"] - mq))
                    tr = self.ledger.tranches.get(xs)
                    if tr is None or tr.get("state") != "open":
                        tr = self.ledger.tranches[xs] = dict(ex=ex, side="no", kind="fmm", t=stamp(), ts=time.time(), state="open", avg=f["price"], qty=0.0)
                    qa = float(tr.get("qty") or 0.0); qn = f["qty"] - mq
                    tr["avg"] = round((float(tr["avg"]) * qa + f["price"] * qn) / (qa + qn), 5); tr["qty"] = qa + qn
                msg = (f"FMM LOCK {race}: hedge NO {f['qty']:,.0f} at {f['price']:.3f} + NO lot at {lot_avg:.3f} = {lot_avg + f['price']:.3f} -> {mq:,.0f} locked "
                       f"(+{mq * (1 - lot_avg - f['price']):,.0f} at settlement; cost {mq * (lot_avg + f['price']):,.0f}), held")
            self.event("FMM", msg); self.night.line(msg)
        if len(self.fmm_cash_seen) > 5000:
            self.fmm_cash_seen = set(list(self.fmm_cash_seen)[-2000:])
        if changed:
            st["seen"] = seen[-3000:]
            with self.controls.lock:
                self.controls.save()
        if entry and self.fmm_mode(time.time()) == "live":
            self.fmm_guard(time.time(), "fill")

    def fmm_note_sweep(self, sw):
        """§2.2: a feed sweep that drops a favourite side arms that book's refill for 60 s (detection time kept for the latency log)."""
        ex = str(sw["ex"]); f = self.fair.get(ex)
        if f is None:
            return
        if (f >= FMM["fav_min"] and sw.get("direction") == "down") or (f <= 1 - FMM["fav_min"] and sw.get("direction") == "up"):
            self.fmm_sweep[self.rot_race(ex)] = dict(t=float(sw.get("recv") or time.time()), ex=ex, taken=False)

    # ---- paper mode (fmm_spec §3.2)
    def fmm_paper_book(self, ex):
        """The feed's current book for paper: the feed sends a book only when it changes, so an older book is still the current one
        (feed_book's 10-s freshness would skip every quiet book — live orders don't need a fresh book, paper shouldn't either)."""
        if self.feed_mode() != "live" or self.mkt_feed is None:
            return None
        return self.mkt_feed.book(ex, 6 * 3600)

    def fmm_paper_place(self, now, targets):
        """Simulated orders for the targets: depth ahead from the feed's book at placement; a take crosses at once (from the asks at or
        under it); an order not wanted for > 10 min goes; exits re-peg down to the best ask (never below fill + 0.5c)."""
        P = self.fmm_paper
        if not P.get("restored"):                            # a restart in the paper window keeps the counters (not the simulated queue)
            P["restored"] = True
            old = self.fmm_state().get("paper") or {}
            if str(old.get("t") or "")[:10] == time.strftime("%Y-%m-%d", time.localtime(now)):
                P["realized"], P["fills"], P["rt"], P["takes"] = float(old.get("realized") or 0), int(old.get("fills") or 0), int(old.get("rt") or 0), int(old.get("takes") or 0)
        want = {}
        for race, ex, side, px, q, layer, sub in targets:
            want[(ex, side, round(px, 3))] = (q, race, layer)
        for key, (q, race, layer) in want.items():
            ex, side, px = key
            o = P["orders"].get(key)
            if o is not None:
                o["unwanted"] = None
                continue
            fb = self.fmm_paper_book(ex)
            if fb is None:
                continue
            bids_s = fb[0] if side == "yes" else [(round(1 - p_, 3), q_) for p_, q_ in fb[1]]
            asks_s = fb[1] if side == "yes" else [(round(1 - p_, 3), q_) for p_, q_ in fb[0]]
            if asks_s and px >= asks_s[0][0] - 1e-9:         # a take: filled at once from the asks at or under it
                got = min(float(q), sum(q_ for p_, q_ in asks_s if p_ <= px + 1e-9))
                if got >= 1:
                    self.fmm_paper_fill(ex, side, px, got, now, take=True)
                continue
            ahead = sum(q_ for p_, q_ in bids_s if abs(p_ - px) < 1e-9)
            if bids_s and px > bids_s[0][0] + 1e-9:
                ahead = 0.0
            P["orders"][key] = dict(qty=float(q), ahead=ahead, cum=0.0, filled=0.0, placed=now, unwanted=None, race=race)
        for key, o in list(P["orders"].items()):
            if key not in want:
                o["unwanted"] = o["unwanted"] or now
                if now - o["unwanted"] > 600:
                    P["orders"].pop(key, None)
        for a in P["asks"]:                                  # exits: down to a new best ask, never below fill + 0.5c
            fb = self.fmm_paper_book(a["ex"])
            if fb is None:
                continue
            asks_s = fb[1] if a["side"] == "yes" else [(round(1 - p_, 3), q_) for p_, q_ in fb[0]]
            if asks_s and asks_s[0][0] < a["px"] - 1e-9:
                npx = max(a["floor"], asks_s[0][0])
                if npx < a["px"] - 1e-9:
                    a["px"] = npx; a["ahead"] = sum(q_ for p_, q_ in asks_s if abs(p_ - npx) < 1e-9); a["cum"] = 0.0; a["placed"] = now
        if now - P["saved_t"] > 60:
            P["saved_t"] = now
            with self.controls.lock:
                self.controls.d.setdefault("fmm", {})["paper"] = dict(realized=round(P["realized"], 2), fills=P["fills"], rt=P["rt"], takes=P["takes"],
                                                                      inv_cost=round(sum(q * p for _, _, q, p in P["lots"]), 2), t=stamp())
                self.controls.save()

    def fmm_paper_fill(self, ex, side, px, q, ts, take=False):
        P = self.fmm_paper
        P["lots"].append((ex, side, q, px)); P["fills"] += 1; P["takes"] += 1 if take else 0
        ring = self.fmm_ring(ts)
        floor_ = bots.tick_up(round(px + FMM["exit_c"], 6))
        ask = max(floor_, bots.tick_up(round(px + (FMM["exit_ring_c"] if ring else FMM["exit_c"]), 6)))
        fb = self.fmm_paper_book(ex)
        ahead = 0.0
        if fb is not None:
            lv = fb[1] if side == "yes" else [(round(1 - p_, 3), q_) for p_, q_ in fb[0]]
            if lv:
                ask = max(ask, lv[0][0])
            ahead = sum(q_ for p_, q_ in lv if abs(p_ - ask) < 1e-9)
        P["asks"].append(dict(ex=ex, side=side, px=ask, floor=floor_, qty=q, cost=px, ahead=ahead, cum=0.0, sold=0.0, placed=ts))
        self.fmm_paper_write(dict(t=stamp(), ev="fill", ex=ex, name=self.name(ex), side=side, px=px, qty=round(q), take=take, ask=ask, fair=self.side_fair(ex, side)))

    def fmm_paper_print(self, ex, ts, p, q):
        """A print on `ex`: simulated bid fills (half rate beyond the depth ahead) and exit fills."""
        P = self.fmm_paper
        for key, o in list(P["orders"].items()):
            oex, side, px = key
            if oex != ex or ts < o["placed"]:
                continue
            ps = p if side == "yes" else 1 - p
            if ps <= px + 1e-9:
                o["cum"] += q
                new = min(o["qty"], max(0.0, o["cum"] - o["ahead"]) * 0.5) - o["filled"]
                if new >= 1:
                    o["filled"] += new
                    self.fmm_paper_fill(ex, side, px, new, ts)
                if o["filled"] >= o["qty"] - 1:
                    P["orders"].pop(key, None)
        for a in P["asks"]:
            if a["ex"] != ex or ts < a["placed"] or a["sold"] >= a["qty"] - 1e-9:
                continue
            ps = p if a["side"] == "yes" else 1 - p
            if ps >= a["px"] - 1e-9:
                a["cum"] += q
                new = min(a["qty"], max(0.0, a["cum"] - a["ahead"]) * 0.5) - a["sold"]
                if new >= 1:
                    a["sold"] += new; P["realized"] += new * (a["px"] - a["cost"])
                    left = new
                    for i, (lex, lside, lq, lpx) in enumerate(P["lots"]):
                        if lex == ex and lside == a["side"] and abs(lpx - a["cost"]) < 1e-9 and lq > 0:
                            tk = min(lq, left); P["lots"][i] = (lex, lside, lq - tk, lpx); left -= tk
                            if left <= 0:
                                break
                    P["lots"] = [l for l in P["lots"] if l[2] >= 1]
                    if a["sold"] >= a["qty"] - 1:
                        P["rt"] += 1
                    self.fmm_paper_write(dict(t=stamp(), ev="exit", ex=ex, name=self.name(ex), side=a["side"], px=a["px"], qty=round(new), realized=round(new * (a["px"] - a["cost"]), 2)))
        P["asks"] = [a for a in P["asks"] if a["sold"] < a["qty"] - 1e-9]

    def fmm_paper_write(self, rec):
        try:
            self.intel_write(f"paper/fmm-{time.strftime('%Y-%m-%d')}.jsonl", rec)
        except Exception:
            pass

    def fmm_locks_view(self):
        """Live FMM locks: (shares, profit locked) from the hedge locks and the FMM pair sets."""
        st = (self.fmm_state().get("live") or {})
        sets = [bk for bk in self.ledger.baskets if bk.get("fmm")]
        sh = float(st.get("locks_sh") or 0) + sum(bk["qty"] for bk in sets)
        pr = float(st.get("locks_profit") or 0) + sum(bk["qty"] * bk["margin"] for bk in sets)
        return sh, pr

    def fmm_pnl_fair(self):
        st = (self.fmm_state().get("live") or {})
        open_fair = 0.0
        for sub, (ex, side, q, avg) in self.fmm_lots().items():
            fs = self.side_fair(ex, side)
            if fs is not None:
                open_fair += q * (fs - avg)
        return float(st.get("realized") or 0) + open_fair

    def fmm_gate(self, now):
        """§3.3 v2: Sat 10:00 — live (K 3, inventory cap 15k at cost, 5k a race; Piero's reserve cut to 2,000: 18:30 item 4) if the replay was
        positive on >= 6 of 9 days and paper realized >= 0, else off; Sun 10:00 — inventory cap 30k and K 5 if live realized > 0, every cap
        held, no funds event and the replay's 5-s measure supports it; stop and report (no auto-restart) on live P&L at fair < -150, a cap
        break, or an I2 trim from FMM."""
        if not FMM.get("on"):
            return
        c = self.controls; st = c.d.setdefault("fmm", {})
        for m in list(self.fmm_noops):                      # buys SIG accepted as no-ops (buying power committed on that exchange)
            self.fmm_funds(m)
        self.fmm_noops[:] = []
        t = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(now))
        if t >= FMM["live_at"] and not st.get("decision") and not st.get("stopped"):
            paper = st.get("paper") or {}
            pr = float(paper.get("realized") or 0) if paper else float(self.fmm_paper["realized"])
            ok = bool(FMM.get("replay_ok")) and pr >= -1e-9
            with c.lock:
                st["decision"] = "live" if ok else "off"; st["decided"] = stamp()
                if ok:
                    st["K"] = float(FMM["k_live"]); st["inv_cap"] = float(FMM["inv_cap_live"])
                    st["trims0"] = sum(v for k, v in self.ex.trims.items() if k in ("fmm", "fmmtake", "fmmexit", "fmmhedge"))
                    st["reserve_release"] = round(self.fmm_reserve_release(), 2)   # item 4: FMM's 8,000 of the reserve (the other layers keep the full reserve)
                    c.d["layers"].setdefault("fmm", dict(state="off", since=now, caps={}, killed=None))["state"] = "full"
                c.save()
            msg = (f"FMM GATE {t[11:16]}: {'LIVE' if ok else 'stays OFF'} — replay {'positive on >= 6 of 9 days' if FMM.get('replay_ok') else 'not positive enough'} "
                   f"({FMM.get('replay_note', '')}); paper realized {pr:+,.1f} ({(paper or {}).get('fills', self.fmm_paper['fills'])} fills, {(paper or {}).get('rt', self.fmm_paper['rt'])} round trips)"
                   + (f"; K {FMM['k_live']:g} (resting <= K x free), inventory cap {FMM['inv_cap_live']:,.0f} at cost ({FMM['race_cap']:,.0f} a race) from the rotation pool "
                      f"(its cap {ROTATION['total_cap']:,.0f} -> {FMM['rot_cap_live']:,.0f}); Piero's manual reserve counts as {FMM['manual_reserve_live']:,.0f} for FMM: "
                      f"{st.get('reserve_release', 0):,.0f} of it goes to FMM only, the other layers keep the full reserve (item 4)" if ok else ""))
            self.event("FMM", msg); self.night.line(msg); self.alerts.append(f"{time.strftime('%H:%M')} {msg}")
        if st.get("decided") and not st.get("status10") and now - time.mktime(time.strptime(st["decided"], "%Y-%m-%d %H:%M:%S")) >= 600:
            with c.lock:
                st["status10"] = stamp(); c.save()
            msg = self.fmm_status10(now)                     # Piero 21:15 item 4: the 10:00 status line (10 min after the decision, the ladder resting)
            self.event("FMM", msg); self.night.line(msg)
        if st.get("decision") != "live" or st.get("stopped"):
            return
        lv = st.get("live") or {}
        why = None
        pf = self.fmm_pnl_fair()
        if pf < FMM["stop_fair"] - 1e-9:
            why = f"live P&L at fair {pf:+,.0f} < {FMM['stop_fair']:+,.0f}"
        trims = sum(v for k, v in self.ex.trims.items() if k in ("fmm", "fmmtake", "fmmexit", "fmmhedge"))
        if trims > int(st.get("trims0") or 0):
            why = why or f"an I2 trim came from FMM ({trims - int(st.get('trims0') or 0)})"
        lots = self.fmm_lots()
        inv_tot = sum(v[2] * v[3] for v in lots.values())
        by_race = collections.Counter()
        for v in lots.values():
            by_race[self.rot_race(v[0])] += v[2] * v[3]
        cap = self.fmm_inv_cap(); tol = float(FMM["cap_tol"])
        if inv_tot > cap + tol:
            why = why or f"inventory cap broken: {inv_tot:,.0f} at cost > {cap:,.0f}"
        big = [r for r, v in by_race.items() if v > self.fmm_race_cap(r) + tol]
        if big:
            why = why or f"race cap broken: {big[0]} {by_race[big[0]]:,.0f} at cost > {self.fmm_race_cap(big[0]):,.0f}"
        raw = min(self.cash_raw_hist) if getattr(self, "cash_raw_hist", None) else None
        if raw is not None and raw < CASH_RESERVE - 1e-6:
            fmm_spent = sum(f["qty"] * f["price"] for f in self.ledger.fills[-400:] if f.get("layer") in ("fmm", "fmmtake", "fmmhedge")
                            and str(f.get("t", "")) >= time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(now - 900)))
            if raw + fmm_spent >= CASH_RESERVE:              # FMM's own fills of the last 15 min took cash under the floor
                why = why or f"cash {raw:,.0f} under the {CASH_RESERVE:,} floor after FMM fills ({fmm_spent:,.0f} in 15 min)"
            elif ("floor-other", int(now // 3600)) not in self.fmm_logged:
                self.fmm_logged.add(("floor-other", int(now // 3600)))   # Piero's AZ-06 buy, Oct 17 15:54: not FMM's — the engine's cash-floor pause handles it
                msg = f"FMM: cash {raw:,.0f} under the {CASH_RESERVE:,} floor, not from FMM's fills ({fmm_spent:,.0f} in 15 min): FMM keeps its quotes (the fill guard sizes them to free cash); the cash-floor pause handles new risk"
                self.event("FMM", msg); self.night.line(msg)
        if why:
            with c.lock:
                st["stopped"] = dict(t=stamp(), why=why); c.save()
            msg = f"FMM STOPPED: {why}; FMM bids pulled, exits keep running; no auto-restart (report to Piero)"
            self.event("FMM", msg); self.night.line(msg); self.alerts.append(f"{time.strftime('%H:%M')} {msg}")
            for o in self.fmm_bids():
                self.ex.cancel(o, reason="FMM STOPPED", front=True)
            return
        if t >= FMM["step_at"] and not st.get("stepped"):
            funds = [e for e in st.get("funds") or [] if str(e.get("t") or "") >= str(st.get("decided") or "")]
            ok = float(lv.get("realized") or 0) > 0 and not funds and bool(FMM.get("k5_ok"))
            with c.lock:
                st["stepped"] = stamp()
                if ok:
                    st["inv_cap"] = float(FMM["inv_cap_step"]); st["K"] = float(FMM["k_step"])
                c.save()
            msg = (f"FMM STEP {t[11:16]}: " + (f"inventory cap -> {FMM['inv_cap_step']:,.0f}, K -> {FMM['k_step']:g}" if ok else f"held at cap {cap:,.0f}, K {self.fmm_k():g}")
                   + f" (live realized {float(lv.get('realized') or 0):+,.0f}, caps held, funds events {len(funds)}, replay 5-s measure {'supports' if FMM.get('k5_ok') else 'does not support'} K 5)")
            self.event("FMM", msg); self.night.line(msg)

    def fmm_status10(self, now):
        """FMM live or not and why; locks taken; the Rhode Island Senate ladder resting; the fast path's p50."""
        st = self.fmm_state()
        paper = st.get("paper") or {}
        why = (f"replay {'passed' if FMM.get('replay_ok') else 'failed'} (>= 6 of 9 days > 0), paper realized {float(paper.get('realized') or 0):+,.1f} "
               f"({paper.get('fills', 0)} fills, {paper.get('rt', 0)} round trips)")
        mode = "LIVE" if st.get("decision") == "live" and not st.get("stopped") else "STOPPED" if st.get("stopped") else "OFF"
        if st.get("stopped"):
            why += f"; stopped: {st['stopped'].get('why')}"
        sets = [bk for bk in self.ledger.baskets if bk.get("fmm")]
        lsh, lpr = self.fmm_locks_view()
        ri = [o for o in self.fmm_bids() if self.rot_race(o["ex"]) == RI_SENATE_RACE and (self.ledger.orders.get(o["id"]) or {}).get("layer") == "fmm"]
        lat = self.latency_view()
        fm, ff = lat.get("fast_match_to_sent") or {}, lat.get("fast_fill_to_sent") or {}
        f2 = lambda v: "n/a" if v is None else f"{v:.1f} s"
        return (f"FMM STATUS {time.strftime('%H:%M', time.localtime(now))}: {mode} — {why}. Locks: {len(sets)} pair set(s) + hedges, {lsh:,.0f} shares, +{lpr:,.0f} locked "
                f"(new-lock room {self.fmm_lock_room():,.0f}: <= 30% of free, from cash the ladders don't need). RI Senate ladder: "
                + (", ".join(f"{self.name(o['ex'])} {o['side'].upper()} {o['qty']:,.0f} @ {o['price']:.3f}" for o in sorted(ri, key=lambda o: -o["price"])) + f" ({sum(o['qty'] * o['price'] for o in ri):,.0f})"
                   if ri else "none resting") +
                f" (cap {self.fmm_race_cap(RI_SENATE_RACE):,.0f} at cost). Fast path p50: SIG match -> sent {f2(fm.get('p50'))} (n {fm.get('n', 0)}), event -> sent {f2(ff.get('p50'))} (n {ff.get('n', 0)}). "
                + self.fmm_status(now))

    def fmm_unread_since_live(self):
        return bool(self.fmm_unread) or bool(self.fmm_bids())

    def fmm_status(self, now, hourly=False):
        mode = self.fmm_mode(now)
        lat = sorted(self.fmm_lat)
        p50 = lat[len(lat) // 2] if lat else None; p90 = lat[int(len(lat) * 0.9)] if lat else None
        cv = getattr(self, "fmm_cov", None) or {}
        cov = (f"coverage {cv.get('quoted', 0)} of {cv.get('eligible', 0)} books quoted (coin flips {cv.get('cf_quoted', 0)} of {cv.get('cf_eligible', 0)}), "
               f"resting {cv.get('rest', 0):,} of K {cv.get('K', self.fmm_k()):g} x free {cv.get('allow', 0):,} (free {cv.get('free', 0):,}; deployable under the caps {cv.get('deploy', 0):,}, "
               f"claim {self.fmm_claim_first(cv.get('free', 0)):,.0f})")
        lp = sorted(getattr(self, "fmm_lat_plan", []) or [])
        latx = (f"refill latency (sweep received -> refill order placed, n {len(lat)}) p50 {'n/a' if p50 is None else f'{p50:.1f} s'} p90 {'n/a' if p90 is None else f'{p90:.1f} s'}"
                + (f", of which sweep -> planned p50 {lp[len(lp) // 2]:.1f} s" if lp else ""))
        if mode == "paper":
            P = self.fmm_paper
            n = len(P["orders"]); notional = sum((o["qty"] - o["filled"]) * k[2] for k, o in P["orders"].items())
            inv = sum(q * p for _, _, q, p in P["lots"])
            return (f"FMM (paper) quotes {n} / {notional:,.0f}; fills {P['fills']} (takes {P['takes']}), round trips {P['rt']}, realized {P['realized']:+,.1f}; "
                    f"inventory at cost {inv:,.0f}; {cov}; {latx}")
        lv = self.fmm_state().get("live") or {}
        bids = self.fmm_bids()
        inv = sum(v[2] * v[3] for v in self.fmm_lots().values())
        lsh, lpr = self.fmm_locks_view()
        hour = ""
        if hourly:                                          # C100 item 4 (Piero 17:15 Oct 18): realized / round trips per hour and the inventory change
            cur = (now, float(lv.get("realized") or 0), int(lv.get("rt") or 0), inv)
            pv = self.fmm_hour_prev
            if pv and now - pv[0] >= 600:
                h = (now - pv[0]) / 3600
                hour = (f"; last {h:.1f} h: realized {(cur[1] - pv[1]) / h:+,.1f}/h, round trips {(cur[2] - pv[2]) / h:,.1f}/h, inventory {cur[3] - pv[3]:+,.0f} "
                        f"(resting {sum(o['qty'] * o['price'] for o in bids):,.0f})")
            self.fmm_hour_prev = cur
        return (f"FMM ({mode}) quotes {len(bids)} / {sum(o['qty'] * o['price'] for o in bids):,.0f}; fills {lv.get('fills', 0)}, round trips {lv.get('rt', 0)}, "
                f"realized {float(lv.get('realized') or 0):+,.0f}, P&L at fair {self.fmm_pnl_fair():+,.0f}; inventory at cost {inv:,.0f} of {self.fmm_inv_cap():,.0f}; "
                f"locks {lsh:,.0f} shares / +{lpr:,.0f} locked; {cov}; {latx}; funds events {len(self.fmm_state().get('funds') or [])}{hour}")

    def track_rot(self, now):
        """C95 item 5: each rotation lot rests an ask at max(entry + 0.5c, PM fair - 1.5c) (GTC); it sells at the touch instead (a swap)
        when another race's candidate, with room under its cap, has a gap >= 3c bigger than what this lot has left."""
        self.rot_view = {}
        rc, total = self.rot_caps()
        cands = [cd for cd in (getattr(self, "rot_cands", None) or []) if self.rot_cap(cd["race"]) - rc[cd["race"]] >= ROTATION["min_clip"] * cd["px"]]
        for sub, (ex, side, q, avg) in self.rot_lots().items():
            if q < MIN_QTY:
                continue
            bid, ask, fs = self.rot_touch(ex, side)
            if fs is None:
                continue
            floor_ = bots.tick_up(round(avg + RULE0_MARGIN, 6))
            px, why = max(floor_, bots.tick_up(round(fs - ROTATION["exit_fair_c"], 6))), "fair - 1.5c"
            if self.wd_after("rot_exit_touch", now) and ask is not None:
                px, why = max(floor_, ask), "from Oct 30: max(lot avg + 0.5c, touch ask) (cash calendar)"   # C103
            dl = (ROTATION.get("exit_touch") or {}).get(str(ex))
            if dl and ask is not None and time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(now)) < dl:
                px, why = max(floor_, ask), f"out before {dl[5:16]} (alpha_spec §5.5): max(lot avg + 0.5c, touch ask)"
            left = fs - (ask if ask is not None else px)
            better = [cd for cd in cands if cd["race"] != self.rot_race(ex) and cd["gap"] >= left + ROTATION["swap_gap_c"] - 1e-9]
            if better and ask is not None and ask >= floor_ - 1e-9:
                px, why = max(ask, floor_), f"swap: {self.name(better[0]['ex'])} {better[0]['side'].upper()} has {better[0]['gap'] * 100:.1f}c vs {left * 100:.1f}c left here"
                k_ = (ex, "swap", better[0]["ex"])
                if k_ not in self.rot_logged:
                    self.rot_logged.add(k_)
                    msg = f"ROT SWAP {self.name(ex)} {side.upper()} {q:,.0f}: ask at the touch {px:.3f} ({why})"
                    self.event("ROT", msg); self.night.line(msg)
            self.rot_view.setdefault(ex, []).append(dict(sub=sub, side=side, px=min(px, 0.99), avg=avg, why=why))

    def rot_fill_events(self, fills):
        """Rotation fills: the lot's tranche (average entry and the fair at entry), FILLED lines, round trips."""
        for f in fills:
            if f.get("layer") not in ("rot", "rotexit"):
                continue
            k = self.fill_key(f)
            if k in self.rot_seen_fills:
                continue                                    # the loop re-hands an order's earlier fills when it fills again (MI-07, Oct 16 21:31)
            self.rot_seen_fills.add(k)
            if len(self.rot_seen_fills) > 5000:
                self.rot_seen_fills = set(list(self.rot_seen_fills)[-2000:])
            ex = f["ex"]
            sub = (self.ledger.orders.get(str(f.get("order") or "")) or {}).get("sub") or f"rot:{ex}:{f['side'] if f['layer'] == 'rot' else ('no' if f['side'] == 'yes' else 'yes')}"
            q = float((self.ledger.subbooks.get(ex) or {}).get(sub, 0.0))
            tr = self.ledger.tranches.get(sub)
            if f["layer"] == "rot":
                fs = self.side_fair(ex, f["side"])
                if tr is None or tr.get("state") != "open":
                    tr = self.ledger.tranches[sub] = dict(ex=ex, side=f["side"], kind="rot", t=stamp(), ts=time.time(), state="open", avg=f["price"], qty=0.0, fair0=fs)
                qa = float(tr.get("qty") or 0.0)
                tr["avg"] = round((float(tr.get("avg") or f["price"]) * qa + f["price"] * f["qty"]) / (qa + f["qty"]), 5) if qa + f["qty"] > 0 else f["price"]
                if fs is not None:
                    tr["fair0"] = round((float(tr.get("fair0") or fs) * qa + fs * f["qty"]) / (qa + f["qty"]), 5) if qa + f["qty"] > 0 else fs
                tr["qty"] = qa + f["qty"]
                msg = f"FILLED rot {self.name(ex)} {f['side'].upper()} {f['qty']:,.0f} at {f['price']:.3f}" + ("" if fs is None else f" (PM fair {fs:.3f}, gap {(fs - f['price']) * 100:.1f}c)") + f"; lot {abs(q):,.0f} at {tr['avg']:.3f}"
            else:
                px_s = round(1 - f["price"], 3)
                msg = f"FILLED rotexit {self.name(ex)} {('NO' if f['side'] == 'yes' else 'YES')} {f['qty']:,.0f} at {px_s:.3f}: cash back {f['qty'] * px_s:,.0f}, realized {(f.get('realized') or 0):+,.0f}; lot left {abs(q):,.0f}"
                if tr is not None and abs(q) < MIN_QTY:
                    tr.update(state="closed", closed_ts=time.time())
                    msg += " (round trip done)"
            self.event("ROT", msg); self.night.line(msg)

    def rot_lot_rows(self):
        """[(name, side, shares, entry, mark (SIG mid), fair, gap closed, P&L on the board, P&L at fair)] of the rotation lots."""
        out = []
        for sub, (ex, side, q, avg) in self.rot_lots().items():
            pxs = self.px.get(ex); fs = self.side_fair(ex, side)
            if not pxs or pxs[0] is None or pxs[1] is None or fs is None:
                continue
            mid = (pxs[0] + pxs[1]) / 2; mark = mid if side == "yes" else 1 - mid
            f0 = float((self.ledger.tranches.get(sub) or {}).get("fair0") or fs)
            g0, g1 = f0 - avg, fs - mark
            closed = (1 - g1 / g0) if g0 > 0.001 else None
            out.append((self.name(ex), side, q, avg, mark, fs, closed, q * (mark - avg), q * (fs - avg)))
        return out

    def rot_status(self, now):
        st = self.rot_state(); fl = self.rot_flows()
        sells = ROTATION.get("sells") or {}
        asks = [o for o in self.open if str(o["ex"]) in sells and (self.ledger.orders.get(o["id"]) or {}).get("layer") == "release"]
        bids = self.rot_bids()
        rows = self.rot_lot_rows()
        pool, left, room = self.rot_pool()
        a = self.acct or {}
        return (f"ROT status: funding sold {sum(fl['sold'].values()):,.0f} of {sum(int(v) for v in sells.values()):,} shares (cash {fl['freed']:,.0f}), "
                f"asks resting {len(asks)} / {sum(o['qty'] for o in asks):,.0f} shares; bids resting {len(bids)} (" +
                ", ".join(f"{self.name(o['ex'])} {o['side'].upper()} {o['qty']:,.0f} @ {o['price']:.3f}" for o in bids) + f"); lots {len(rows)} / {sum(r[2] for r in rows):,.0f} shares at cost "
                f"{sum(r[2] * r[3] for r in rows):,.0f}, open {sum(r[7] for r in rows):+,.0f} on the board / {sum(r[8] for r in rows):+,.0f} at fair; realized exits {fl['realized_exits']:+,.0f}; "
                f"pool {max(0.0, pool):,.0f} (rotation cash left {left:,.0f}; engine free cash - resting bids {room:,.0f}); cash {((a.get('totalAccountValue') or 0) - (a.get('totalHoldingsValue') or 0)):,.0f}, "
                f"manual reserve left {self.manual_reserve_left():,.0f}")

    def rank_vs_100(self):
        """(our rank, total, our P&L, #100's P&L) from the last leaderboard read (data/leaders.csv / leaders_meta.json)."""
        try:
            meta = read_json(os.path.join(DATA, "leaders_meta.json"), {}) or {}
            with open(os.path.join(DATA, "leaders.csv")) as fh:
                lines = fh.readlines()[-80:]
            rows = list(csv.reader(lines))
            last_t = max(r[0] for r in rows if r and r[0] != "time")
            cur = [r for r in rows if r and r[0] == last_t]
            ours = next((float(r[4]) for r in cur if r[-1] == "1"), None)
            r100 = next((float(r[4]) for r in cur if r[1] == "100"), None)
            return meta.get("my_rank"), meta.get("total"), ours, r100
        except Exception:
            return None, None, None, None

    def rot_report(self, now):
        """C95 item 6: at 18:00 and 08:00 PT once a day — funding asks filled, the rotation lots (entry / mark / fair / gap closed),
        realized, open P&L on the board and at fair, cash, manual reserve left, rank vs #100."""
        if not ROTATION.get("on") or not self.rot_state():
            return
        hm = time.strftime("%H:%M", time.localtime(now)); day = time.strftime("%Y-%m-%d", time.localtime(now))
        slot = "08:00" if "08:00" <= hm < "08:30" else "18:00" if "18:00" <= hm < "18:30" else None
        if slot is None:
            return
        st = self.rot_state()
        if (st.get("reports") or {}).get(slot) == day:
            return
        with self.controls.lock:
            st.setdefault("reports", {})[slot] = day; self.controls.d["rotation"] = st; self.controls.save()
        msg = self.rot_report_text(now, slot)
        self.event("ROT", msg); self.night.line(msg)

    def rot_report_text(self, now, slot="now"):
        fl = self.rot_flows(); sells = ROTATION.get("sells") or {}
        rows = self.rot_lot_rows()
        rank, total, ours, r100 = self.rank_vs_100()
        a = self.acct or {}
        cash = (a.get("totalAccountValue") or 0) - (a.get("totalHoldingsValue") or 0)
        fund = "; ".join(f"{self.name(ex)} {int(fl['sold'].get(str(ex), 0)):,}/{int(v):,}" for ex, v in sells.items())
        lots = "; ".join(f"{n} {sd.upper()} {q:,.0f} @ {e:.3f} / {m:.3f} / {f_:.3f}" + ("" if c_ is None else f" ({c_:.0%} closed)") + f" {pb:+,.0f} / {pf:+,.0f}" for n, sd, q, e, m, f_, c_, pb, pf in rows)
        wid = self.rot_widened()
        flag = ("" if not wid else " FLAGGED (gap widened >= 2c, no more adds): " + "; ".join(f"{r} {g0 * 100:.1f}c -> {g1 * 100:.1f}c" for r, (g0, g1) in wid.items()) + ".")
        rk = "rank n/a" if rank is None else (f"rank {rank} of {total}" + ("" if ours is None or r100 is None else f", P&L {ours:+,.0f} vs #100 {r100:+,.0f} ({ours - r100:+,.0f})"))
        return (f"ROT REPORT {slot} (Piero 15:05 Oct 16 item 6): FUNDING sold {sum(fl['sold'].values()):,.0f} of {sum(int(v) for v in sells.values()):,} shares, cash {fl['freed']:,.0f} "
                f"(realized {fl['realized_sales']:+,.0f} vs lot avg) — {fund}. LOTS {len(rows)} (entry / mark / fair, gap closed, board / fair): {lots or 'none'}. "
                f"OPEN {sum(r[7] for r in rows):+,.0f} on the board / {sum(r[8] for r in rows):+,.0f} at fair; REALIZED exits {fl['realized_exits']:+,.0f}; buys {fl['buys']:,.0f}, exits {fl['exits']:,.0f}.{flag} "
                f"CASH {cash:,.0f}, manual reserve left {self.manual_reserve_left():,.0f}, engine free {self.engine_cash_now() - CASH_RESERVE:,.0f}, pool {self.rot_reserve():,.0f}. {rk}."
                + ("" if self.fmm_mode(now) == "off" else " " + self.fmm_status(now))
                + (" " + self.alloc_line(now) if ALLOC.get("on") else "")
                + (" " + self.carry_status() if CARRY.get("on") and (self.controls.d.get("c98") or {}).get("done") else "")
                + (" " + self.party_line() if ROTATION.get("party_cap") else ""))

    def alloc_line(self, now):
        """C97 item 7: the allocation by bucket (at cost; holdings at SIG's marks), FMM realized and at fair, the fund sale, rank vs #100."""
        a = self.acct or {}
        cash = (a.get("totalAccountValue") or 0) - (a.get("totalHoldingsValue") or 0)
        mm = sum(v[2] * v[3] for v in self.fmm_lots().values()); mm_rest = self.fmm_bid_notional()
        rc, rot_total = self.rot_caps()
        cat = self.catcher_new_cost()
        cat_rest = sum(o["qty"] * o["price"] for o in self.open if o["action"] == "buy" and (self.ledger.orders.get(o["id"]) or {}).get("layer") == "dump")
        locks = self.fmm_lock_capital()
        hold = max(0.0, float(a.get("totalHoldingsValue") or 0) - mm - (rot_total - sum(o["qty"] * o["price"] for o in self.rot_bids())) - cat - locks)
        lv = self.fmm_state().get("live") or {}
        ff = self.fund_flows() if FUND.get("on") else dict(sold={}, freed=0.0, realized=0.0)
        rank, total, ours, r100 = self.rank_vs_100()
        rk = f"rank {rank} of {total}, P&L {ours:+,.0f} vs #100 {r100:+,.0f} ({ours - r100:+,.0f})" if rank else "rank n/a"
        return (f"ALLOCATION (Piero 15:55 Oct 17): MM {mm:,.0f} held of {self.fmm_inv_cap():,.0f} (+ {mm_rest:,.0f} resting); coin flips {rot_total:,.0f} of {self.rot_total_cap():,.0f} (held + resting); "
                f"catcher {cat:,.0f} new lots + {cat_rest:,.0f} resting of {ALLOC['catcher_slice']:,.0f}; locks {locks:,.0f} of {ALLOC['lock_cap']:,.0f}; holdings ~{hold:,.0f} at SIG marks; cash {cash:,.0f} "
                f"(FMM free {self.fmm_free():,.0f}, its priority {self.fmm_priority():,.0f}). FMM realized {float(lv.get('realized') or 0):+,.1f}, at fair {self.fmm_pnl_fair():+,.0f}. "
                f"FUND sold {sum(ff['sold'].values()):,.0f} shares, cash {ff['freed']:,.0f}, realized {ff['realized']:+,.0f}. {rk}.")

    def c101_migration(self, now):
        """Once (C101, Piero 15:15 Oct 21): FMM's inventory cap 30k -> 35k (the race caps stay)."""
        c = self.controls
        st = c.d.get("fmm") or {}
        if (c.d.get("c101") or {}).get("done") or st.get("decision") != "live":
            return
        with c.lock:
            old = float(st.get("inv_cap") or 0)
            st["inv_cap"] = max(old, float(FMM["inv_cap_live"]))
            c.d["fmm"] = st
            c.d["c101"] = dict(done=stamp(), inv_cap_was=old)
            c.save()
        msg = (f"C101 (Piero 15:15 Oct 21): FMM inventory cap {old:,.0f} -> {st['inv_cap']:,.0f}; coin-flip sides (side fair {FMM_CF['band'][0]:.2f}-{FMM_CF['band'][1]:.2f}) at the "
               f"touch <= PM fair - {FMM_CF['edge'] * 100:.0f}c, {FMM_CF['clip']:,} a clip, {FMM_CF['race_cap']:,.0f} a race, the party cap holding; FMM's claim = min(15k, what it can "
               f"deploy under its caps); sales only at EV/$ held <= {SELL_EV_MAX:.0%}; engine_errors counts the engine's own errors and resumes after 3 clean minutes")
        self.event("FMM", msg); self.night.line(msg)

    def c97_migration(self, now):
        """Once (C97, Piero 15:55 Oct 17): the capital allocation — the fund sale, FMM's 30k cap, the rotation's 30k, the reserve at 2,000."""
        c = self.controls
        if (c.d.get("c97") or {}).get("done") or not (ALLOC.get("on") or FUND.get("on")):
            return
        rot = c.d.get("rotation") or {}
        fl = self.rot_flows()
        with c.lock:
            c.d["c97"] = dict(done=stamp())
            mr = c.d.get("manual_reserve") or {}
            mr["amount"] = float(MANUAL_RESERVE); c.d["manual_reserve"] = mr          # item 5: 2,000 for every layer
            st = c.d.setdefault("fmm", {})
            if st.get("decision") == "live":
                st["inv_cap"] = float(FMM["inv_cap_live"])                         # item 2: 30k now
            if FUND.get("on"):
                frozen = rot.setdefault("frozen", {})
                for ex in FUND.get("sells") or {}:
                    if str(ex) in (ROTATION.get("sells") or {}):
                        frozen[str(ex)] = int(fl["sold"].get(str(ex), 0))           # the rotation keeps what it sold; the rest is the fund's
                c.d["rotation"] = rot
                c.d["fund"] = dict(since=FUND["since"], sell_left={str(k): int(v) for k, v in FUND["sells"].items()}, cash_freed=0.0)
                res = c.d.get("reservoirs") or {}
                for ex in FUND.get("sell_all_subs") or ():
                    if ex in res:
                        c.d.setdefault("reservoirs_retired", {})[ex] = dict(res.pop(ex), retired=stamp(), why="Piero 15:55 Oct 17 item 1: every lot sold (no rebuy)")
            c.save()
        msg = (f"C97 (Piero 15:55 Oct 17): CAPITAL ALLOCATION ~30k market making / ~30k coin flips / ~20k other / holdings only what can't be sold at >= lot avg + 0.5c. "
               f"(1) FUND: GTC asks at max(lot avg + 0.5c, touch), place kept, core lots included: " + ", ".join(f"{self.name(k)} {'all' if v >= 10 ** 6 else format(v, ',')}" for k, v in FUND["sells"].items()) +
               f"; Delaware's reservoir retired; the rotation's own Delaware sale frozen at what it sold; the fund's cash is not the rotation's. (2) FMM inventory cap {FMM['inv_cap_live']:,.0f} now, K 3 until "
               f"Sun 10:00 then 5 by the gate; FMM gets cash first until its free cash reaches {ALLOC['fmm_priority_free']:,.0f} (the other layers see free above it). (3) Rotation cap {ROTATION['total_cap']:,.0f} at cost "
               f"(no new buys until exits bring it under; at its cap its cash is not held back). (4a) Catcher: a {ALLOC['catcher_slice']:,.0f} slice (new lots at cost + resting), L0 / L1 only, "
               f">= {ALLOC['catcher_mark_c'] * 100:.0f}c under the 6-h VWAP mark and below PM fair. (4b) Locks <= {ALLOC['lock_cap']:,.0f} at cost, walked cost <= {FMM['lock_cost']:.3f}, idle cash only. "
               f"(5) Manual reserve {MANUAL_RESERVE:,.0f} for every layer. Invariants unchanged.")
        self.event("CONTROL", "C97 (Piero 15:55 Oct 17): the capital allocation (the full text is in the night log)"); self.night.line(msg)

    def c96_migration(self, now):
        """Once (C96, fmm_spec v2 Oct 16 18:45 + Piero 18:30 item 4): the favourite-side market maker's schedule and gates."""
        c = self.controls
        if (c.d.get("c96") or {}).get("done") or not FMM.get("on"):
            return
        with c.lock:
            c.d["c96"] = dict(done=stamp())
            c.d["layers"].setdefault("fmm", dict(state="off", since=now, caps={}, killed=None))
            c.save()
        msg = (f"C96 (fmm_spec v2, Piero 18:50 Oct 16): FAVOURITE MM — paper {FMM['paper_from'][5:16]} to {FMM['live_at'][11:16]} (planner on the live feed, nothing sent, "
               f"simulated fills to data/intel/paper/fmm-<date>.jsonl); live at {FMM['live_at'][11:16]} if the replay was positive on >= 6 of 9 days ({FMM['replay_note']}) and paper "
               f"realized >= 0: every eligible favourite-side book (fair >= {FMM['fav_min']:.2f}, touch <= fair - {FMM['edge'] * 100:.0f}c) quoted GTC at the touch, the race's cheaper book "
               f"also -1 / -2 / -3c; resting bids lock no cash, total <= K x (cash - 5,000 - manual reserve left), K {FMM['k_live']:g}; race budget K x free x 48-h volume share, clip "
               f"max({FMM['min_clip']}, budget / 4), no clip above free; inventory cap {FMM['inv_cap_live']:,.0f} at cost ({FMM['race_cap']:,.0f} a race; resting + inventory inside both) "
               f"from the rotation pool (rotation cap {ROTATION['total_cap']:,.0f} -> {FMM['rot_cap_live']:,.0f}); Piero's manual reserve counts as {FMM['manual_reserve_live']:,.0f} for FMM only "
               f"(item 4). Fill guard on every FMM fill: pull bids, largest first, until each clip AND the two largest race ladders fit free and resting <= K x free (FMM GUARD; "
               f"the replay's worst burst is one race's whole ladder, 5,010, <= 2 races: with this rule cash never went under floor + reserve at guard latencies 2-15 s). Exits at max(fill + 0.5c "
               f"(ring hours + 1c), best ask), re-pegged down to the best ask, never below fill + 0.5c. Sweep refill: take asks <= fair - 2c, re-post the cleared levels. "
               f"Locks: NO + NO at a walked cost <= {FMM['lock_cost']:.3f} (atomic, <= {FMM['lock_cap_sh']:,} shares a race, held) and hedges of NO lots, replacing H4 while live. "
               f"FMM FUNDS on every SIG funds / balance event (SIG: resting buys escrow nothing; it shrinks or cancels resting buys a falling balance cannot cover; a buy is a "
               f"no-op when that exchange's resting buys commit the whole buying power); cash below 0 -> K 1. Sun {FMM['step_at'][11:16]}: cap {FMM['inv_cap_step']:,.0f}, K {FMM['k_step']:g} "
               f"if live realized > 0, caps held, no funds event and the replay's 5-s measure supports it. STOP (no auto-restart) on live P&L at fair < {FMM['stop_fair']:+,.0f}, "
               f"a cap break or an I2 trim from FMM. Invariants unchanged.")
        self.event("CONTROL", "C96 (fmm_spec v2): favourite MM — paper Sat 06:00-10:00, live by its gates (the full text is in the night log)"); self.night.line(msg)

    def c95_migration(self, now):
        """Once (C95, Piero 15:05 Oct 16): the manual reserve, the rotation's new phase (sales, universe, caps), RI Governor R's
        reservoir retired (it is sold in full)."""
        c = self.controls
        if (c.d.get("c95") or {}).get("done"):
            return
        with c.lock:
            c.d["c95"] = dict(done=stamp())
            c.d["manual_reserve"] = dict(amount=float(MANUAL_RESERVE), since=stamp())
            c.d["rotation_c94"] = c.d.get("rotation")
            c.d["rotation"] = dict(since=stamp(), sell_left={str(k): int(v) for k, v in ROTATION["sells"].items()}, cash_freed=0.0, sold=0.0, skipped={}, reports={})
            res = c.d.get("reservoirs") or {}
            for ex in ROTATION.get("sell_all_subs") or ():
                if ex in res:
                    c.d.setdefault("reservoirs_retired", {})[ex] = dict(res.pop(ex), retired=stamp(), why="Piero 15:05 Oct 16: sold in full for the rotation (no rebuy)")
            c.d["layers"].setdefault("rot", dict(state="off", since=now, caps={}, killed=None))["state"] = "full"
            c.save()
        msg = (f"C95 (Piero 15:05 Oct 16): (1) MANUAL RESERVE {MANUAL_RESERVE:,.0f}: the engine never spends below the {CASH_RESERVE:,} floor + the reserve left "
               f"(engine free cash = cash - {CASH_RESERVE + MANUAL_RESERVE:,.0f}); Piero's own fills (orders the engine did not place) draw from it and pause nothing above the floor; "
               f"shown on the dashboard Overview. (2) FUND: GTC asks at max(lot avg + 0.5c, touch), place kept, cash counted on SIG fills only: " +
               ", ".join(f"{self.name(k)} {v:,}" for k, v in ROTATION["sells"].items()) +
               "; KEEP Rhode Island Senate R NO, Delaware's reservoir / swing half and every lot below lot avg + 0.5c; Rhode Island Governor R's reservoir retired (no rebuy). "
               f"Every dollar the sales free goes to the rotation (its cash is off the other layers'). (3) BUY: every coin-flip side (fair 0.25-0.75) with a gap >= 4c at the touch and >= 100k shares in 48 h, "
               f"biggest gap % first, one exposure per race in the cheaper book; caps at cost 8k per race, 12k Senate control (842 YES / 843 NO), 65k in all; the >= 3c tier once the 4c one is full. "
               f"(4) NO BID CHURN: a bid only for what the pool funds (>= 500 shares); a resting bid is never pulled because the pool shrank; GTC, place kept. "
               f"(5) EXIT at max(entry + 0.5c, PM fair - 1.5c); swap when another gap is >= 3c bigger; exempt from the no-edge release above 2% left. "
               f"(6) REPORT at 18:00 and 08:00. Invariants unchanged.")
        self.event("CONTROL", "C95 (Piero 15:05 Oct 16): manual reserve + the coin-flip rotation at full size (the full text is in RUN_LOG)"); self.night.line(msg)

    def c94_migration(self, now):
        """Once (C94, Piero 20:30 Oct 13): the coin-flip rotation — four sales, five targets, exits, the hourly line."""
        c = self.controls
        if (c.d.get("c94") or {}).get("done"):
            return
        with c.lock:
            c.d["c94"] = dict(done=stamp())
            c.d["rotation"] = dict(since=stamp(), sell_left={k: int(v) for k, v in ROTATION["sells"].items()}, cash_freed=0.0, sold=0.0, skipped={})
            c.d["layers"].setdefault("rot", dict(state="off", since=now, caps={}, killed=None))["state"] = "full"
            c.save()
        msg = (f"C94 (Piero 20:30): COIN-FLIP ROTATION. (1) SELL as GTC asks at max(lot avg + 0.5c, touch), place kept: " + ", ".join(f"{self.name(k)} {v:,}" for k, v in ROTATION["sells"].items()) +
               f" (~29k; cash counted on SIG fills only). (2) BUY only from that cash, GTC bids at min(touch, cap) while the gap to PM fair >= {ROTATION['gap_min'] * 100:.0f}c, a target closed if PM moves >= {ROTATION['pm_skip_c'] * 100:.0f}c against it after posting: " +
               "; ".join(f"{b['name']} <= {' / '.join(f'{cp:.3f}' for _, _, cp in b['legs'])} cap {b['cap']:,.0f}" for b in ROTATION["buys"]) +
               f". (3) EXIT: an ask at max(entry + 0.5c, PM fair - {ROTATION['exit_fair_c'] * 100:.1f}c) per lot, a swap into a gap >= {ROTATION['swap_gap_c'] * 100:.0f}c bigger; rotation lots are exempt from the no-edge release above {ROTATION['release_exempt_ret']:.0%} left. "
               f"Invariants unchanged: never below lot average + 0.5c, the 5,000 floor, I2 / I3, Piero's manual positions untouched (swing / pairset / manual sub-books are never sold).")
        self.event("CONTROL", msg); self.night.line(msg)

    def c93e_migration(self, now):
        """Once (C93e, Piero 14:58 Oct 13): the cash_floor auto-resume at floor + 1,000; the H1 window reopened to 18:00; the shortfall per-sub cap."""
        c = self.controls
        if (c.d.get("c93e") or {}).get("done"):
            return
        with c.lock:
            c.d["c93e"] = dict(done=stamp())
            st = c.d.get("h1_test") or {}
            if st.get("opened") and st.get("closed") and self.h1_test_day(now):
                st["reopened"] = stamp(); st["closed_1500"] = st.pop("closed")
            c.save()
        msg = (f"C93e (Piero 14:58): (1) resumed by hand at 15:3x (the cash_floor pause from 12:11 after the manual NY-13 buy; the auto-resume wanted cash >= floor + 5,000). "
               f"(2) a cash_floor / cash_short pause now lifts at cash >= floor + {CASH_FLOOR_RESUME:,} for 2 loops; the {CASH_RESERVE:,} floor and its pause are unchanged. "
               f"(3) the H1 window is extended to {H1_TEST['end']} today (the 12:11-15:2x pause is not the sleeve's and is left out of resting time); the sleeve reserve and the touch-maker quotes resume; report at 18:30. "
               f"Also in this release: the 03:15 shortfall sale's per-market cap is shared by the market's sub-books (Oklahoma sold 4,891 for a 2,505 shortfall).")
        self.event("CONTROL", msg); self.night.line(msg)

    def c93c_migration(self, now):
        """Once (C93c, Piero 16:30 Oct 12): the ring stops lifted inside the H1 sleeve only; no self-trade; the report split by ring hours."""
        c = self.controls
        if (c.d.get("c93c") or {}).get("done"):
            return
        with c.lock:
            c.d["c93c"] = dict(done=stamp()); c.save()
        msg = (f"C93c (Piero 16:30): (1) RING STOPS LIFTED FOR THE H1 SLEEVE ONLY: inside the {H1_TEST['sleeve']:,.0f} sleeve on H5's ten the TM quotes {H1_TEST['start']}-{H1_TEST['end']} continuously, "
               f"03:00-06:00 and 12:00-14:00 included (the sleeve is reserved apart from the catcher); everywhere else the TM ring stops stay; the {H1_TEST['stop_fair']:+,.0f}-at-fair stop stays. "
               f"(2) NO SELF-TRADE: a sleeve bid is never placed at or through one of our own resting asks on the same market (release / exit asks, re-pegging ones included): a tick below it or the market is skipped, logged ('H1 SKIP'). "
               f"(3) the H1 report splits fills, mark-out and P&L by ring-stop hours vs outside.")
        self.event("CONTROL", msg); self.night.line(msg)

    def c93b_migration(self, now):
        """Once (C93b, Piero 16:15 Oct 12): the H1 sleeve is touch-maker quotes only; the gate; the 15:59 I2 trim; the release-ask averages."""
        c = self.controls
        if (c.d.get("c93b") or {}).get("done"):
            return
        with c.lock:
            c.d["c93b"] = dict(done=stamp()); c.save()
        msg = (f"C93b (Piero 16:15): (1) the H1 sleeve is TOUCH-MAKER quotes only (the side below fair) on H5's ten markets ({', '.join(self.name(e) for e in self.h1_test_markets())}), "
               f"inside {H1_TEST['sleeve']:,.0f}, clip {H1_TEST['clip']}, GTC; the spread maker stays exits-only; the TM 10k pause and the 30k inventory stop are lifted inside the sleeve only; "
               f"the ring stops (03:00-06:00, 12:00-14:00) and every other rule stay. (2) GATE: round trips >= 0.5 per sleeve share inside 04:00-15:00, realized on round trips > 0, 30-min mark-out >= 0; "
               f"fills per resting-hour and per quoted share-hour vs control (1.40 / 1.14), time to the front, fill rate at the front and P&L per 1k of sleeve per hour reported; I2 pause minutes left out of resting time. "
               f"(3) the 15:59 I2 trim: the release ask Illinois Governor D YES 201 at 0.960 was posted in the loop in which the TM re-pegged its own exit for the same lot (NO 0.04 = YES 0.96, 201; the exit was in 'cancelling'); "
               f"the own-exit guard now counts an exit being re-pegged or planned this loop; the release ask was the one trimmed. "
               f"(4) a release ask takes SIG's own position average when the lot is the whole position (the tranche average had mixed both sides' entries): "
               f"U.S. House D NO 502 (0.61 -> 0.12), CO-03 House race D YES 492 (0.545 -> 0.33); Kansas Senate D NO 1,355 keeps its lots' 0.618 (SIG's 0.598 is average-cost accounting after the TM's 1,000 at 0.57 were sold at 0.582; rule 0 holds the lots): asks at max(avg + 0.5c, touch) (POSTED lines follow).")
        self.event("CONTROL", msg); self.night.line(msg)

    def c92_migration(self, now):
        """Once (C92, Piero 17:35 Oct 11): the cash plan for H1's morning flow and the H1 morning verdict."""
        c = self.controls
        d = c.d.setdefault("c92", {})
        if os.environ.get("SIG_C92", "1") != "1":
            return
        if d.get("done"):
            if not d.get("dedup_done"):
                self.dedup_pending = True                   # C92's start-up failed before the dedup (position names not built yet): run it now
            return
        with c.lock:
            c.d["cash_target_1735"] = CASH_PLAN_1735["target"]
            c.d["cash_low_ret"] = CASH_PLAN_1735["low_ret"]
            c.d["sell_subs"] = list(CASH_PLAN_1735["sell_subs"])
            c.d.setdefault("edge", {})["h4_paused"] = bool(CASH_PLAN_1735["h4_paused"])
            d.update(done=stamp())
            c.save()
        self.dedup_pending = True                           # the dedup needs the roster (position names): it runs on the first controller pass
        msg = (f"C92 (Piero 17:35): cash for H1's 04:00-07:00 flow — by 03:30 free >= {CASH_PLAN_1735['target']:,}: (a) trading lots with < {CASH_PLAN_1735['low_ret']:.0%} return left sell first, lowest first, on both arms; "
               f"(b) the NM Senate R wall lots (3,791 NO, entry 0.915) are offered at the touch while short ({', '.join(k.split('|')[1] for k in CASH_PLAN_1735['sell_subs'])}); (c) H4 pair locks paused until H1's verdict; "
               f"RI's 8,900 held; never below entry + 0.5c. H1 is judged on the {H1_JUDGE_WINDOW[0]}-{H1_JUDGE_WINDOW[1]} PT window (fills per resting-hour vs control, 30-min mark-out) at 08:00.")
        self.event("CONTROL", msg); self.night.line(msg)

    def dedup_vs_sig(self, now, since=DEDUP_SINCE):
        """Piero 17:35 item 2 (bookkeeping only): a synthetic (order_shrink) fill is a duplicate when the order is booked for more shares
        than SIG's own fills for it (data/fills_archive.jsonl, complete since Oct 1). Drop those synthetic fills newest-first until the
        booking matches SIG, repair the sub-books, recompute the scoreboard; log before / after."""
        truth = collections.defaultdict(float)
        try:
            with open(os.path.join(DATA, "fills_archive.jsonl")) as fh:
                for l in fh:
                    try:
                        f = json.loads(l)
                        truth[str(f.get("orderId"))] += abs(float(f.get("quantity") or 0))
                    except Exception:
                        pass
        except OSError:
            self.event("ERR", "dedup vs SIG: no fills archive")
            return
        if not truth:
            return
        before = dict(fills=len(self.ledger.fills), syn=sum(1 for f in self.ledger.fills if f.get("source") == "order_shrink"))
        self.scoreboard_t = 0; self.scoreboard_update(now)
        sb0 = dict(self.scoreboard or {})
        cat0 = (sb0.get("since") or {}).get("strategy", {}).get("catcher") or {}
        fixed = self.ledger.dedup_vs_truth(truth, since=since)   # C92: the H3 window; C93 (Piero 15:30 item 5): the whole ledger
        self.lots_cache = (None, {})
        self.scoreboard_t = 0; self.scoreboard_update(now)
        sb1 = dict(self.scoreboard or {})
        cat1 = (sb1.get("since") or {}).get("strategy", {}).get("catcher") or {}
        msg = (f"DEDUP vs SIG's own fills (Piero 17:35 item 2): {len(fixed['removed'])} synthetic fills removed ({fixed['shares']:,.0f} shares on {fixed['orders']} orders over-booked vs the archive), "
               f"{len(fixed['netted'])} sub-book pairs netted, {len(fixed.get('trimmed') or [])} sub-books trimmed to their fills; ledger fills {before['fills']} -> {len(self.ledger.fills)} "
               f"(synthetic {before['syn']} -> {sum(1 for f in self.ledger.fills if f.get('source') == 'order_shrink')}). Scoreboard since Oct 1, catcher: bought {cat0.get('bought', 0):+,.0f} -> {cat1.get('bought', 0):+,.0f}, "
               f"given back {-cat0.get('given', 0):+,.0f} -> {-cat1.get('given', 0):+,.0f}, net {cat0.get('net', 0):+,.0f} -> {cat1.get('net', 0):+,.0f}, realized {cat0.get('realized', 0):+,.0f} -> {cat1.get('realized', 0):+,.0f}; "
               f"all books today net {sb0.get('today_net', 0):+,.0f} -> {sb1.get('today_net', 0):+,.0f}. The A/B verdicts read the repaired ledger.")
        self.event("AUDIT", msg); self.night.line(msg)
        if fixed["removed"] or fixed["netted"]:
            self.ledger.save()
        with self.controls.lock:
            self.controls.d.setdefault("c92", {})["dedup_done"] = stamp()
            if since is None:
                self.controls.d["c92"]["dedup_all_done"] = stamp()
            self.controls.save()

    def book_scan(self, now):
        """C88 (reads 3a): every 2 min, every book from the feed (zero REST reads): gap of the SIG mid vs fair, depth at the touch
        and within 3 ticks, ranked by |gap| -> data/intel/book_scan/latest.json and self.book_scan_rows."""
        if now - self.book_scan_t < BOOK_SCAN_EVERY_S or self.mkt_feed is None or self.feed_mode() != "live":
            return
        self.book_scan_t = now
        rows = []
        for b in self.bots:
            if b.kind != "market":
                continue
            ex, f = b.ex, self.fair.get(b.ex)
            fb = self.mkt_feed.book(ex, 120.0)
            if f is None or fb is None or not fb[0] or not fb[1]:
                continue
            bids, asks = fb
            bid, ask = bids[0][0], asks[0][0]
            mid = (bid + ask) / 2
            d3 = lambda lv, p0, up: sum(q for p_, q in lv if abs(p_ - p0) <= 3 * TICK + 1e-9)
            rows.append(dict(ex=ex, name=self.name(ex), fair=round(f, 4), bid=bid, ask=ask, gap_c=round((mid - f) * 100, 1),
                             bid_q=bids[0][1], ask_q=asks[0][1], bid_q3=d3(bids, bid, False), ask_q3=d3(asks, ask, True),
                             cheap=("yes" if bid <= f - 0.02 else "no" if ask >= f + 0.02 else None)))
        rows.sort(key=lambda r: -abs(r["gap_c"]))
        self.book_scan_rows = rows
        try:
            os.makedirs(os.path.join(DATA, "intel", "book_scan"), exist_ok=True)
            tmp = os.path.join(DATA, "intel", "book_scan", "latest.json.tmp")
            with open(tmp, "w") as fh:
                json.dump(dict(t=stamp(), n=len(rows), rows=rows), fh)
            os.replace(tmp, os.path.join(DATA, "intel", "book_scan", "latest.json"))
        except OSError:
            pass

    def recycle_depth_snapshot(self, now):
        """Election night 5a: once a day (first loop after 12:00 PT) the resting bid depth at >= 0.98 and >= 0.99 per contract and side,
        from the feed's full books -> data/intel/recycle_depth/<day>.json (our capacity to recycle winners into those bids)."""
        day = time.strftime("%Y-%m-%d", time.localtime(now))
        if time.localtime(now).tm_hour < RECYCLE_DEPTH_HOUR or self.intel_s.get("recycle_depth") == day or self.mkt_feed is None:
            return
        path = os.path.join(DATA, "intel", "recycle_depth", day + ".json")
        if os.path.exists(path):
            self.intel_s["recycle_depth"] = day
            return
        out = {}
        for b in self.bots:
            if b.kind != "market":
                continue
            fb = self.mkt_feed.book(b.ex, 600.0)
            if fb is None:
                continue
            bids, asks = fb
            out[b.ex] = dict(name=self.name(b.ex), yes_98=sum(q for p_, q in bids if p_ >= 0.98 - 1e-9), yes_99=sum(q for p_, q in bids if p_ >= 0.99 - 1e-9),
                             no_98=sum(q for p_, q in asks if p_ <= 0.02 + 1e-9), no_99=sum(q for p_, q in asks if p_ <= 0.01 + 1e-9))
        if len(out) < 100:
            return                                          # the feed has not filled in yet: try next loop
        self.intel_s["recycle_depth"] = day
        try:
            os.makedirs(os.path.dirname(path), exist_ok=True)
            with open(path, "w") as fh:
                json.dump(dict(t=stamp(), markets=out), fh)
            tot98 = sum(v["yes_98"] + v["no_98"] for v in out.values()); tot99 = sum(v["yes_99"] + v["no_99"] for v in out.values())
            self.night.line(f"RECYCLE DEPTH snapshot {day}: {len(out)} contracts; bids >= 0.98: {tot98:,.0f} shares, >= 0.99: {tot99:,.0f} (data/intel/recycle_depth/{day}.json)")
        except OSError:
            pass

    def range_candidates_daily(self, now):
        """C88 (reads 3b): once a day, sell-high / buy-back-low candidates per market from our tape's last 24 h: the held or favourite
        side's share-weighted p10 / p90 print prices, the number of visits to each, the range in cents -> data/intel/range_candidates.json."""
        day = time.strftime("%Y-%m-%d", time.localtime(now))
        if self.intel_s.get("ranges") == day:
            return
        path = os.path.join(DATA, "intel", "range_candidates.json")
        try:
            if os.path.exists(path) and json.load(open(path)).get("day") == day:
                self.intel_s["ranges"] = day
                return
        except Exception:
            pass
        self.intel_s["ranges"] = day
        cut = now - 86400
        rows = collections.defaultdict(list)
        for d in (cut, now):
            fp = os.path.join(DATA, "intel", "trades", time.strftime("%Y-%m-%d", time.localtime(d)) + ".jsonl")
            if not os.path.exists(fp):
                continue
            with open(fp) as fh:
                for line in fh:
                    try:
                        r = json.loads(line)
                    except ValueError:
                        continue
                    if (r.get("ts") or 0) >= cut and r.get("p") is not None:
                        rows[str(r["ex"])].append((float(r["ts"]), round(float(r["p"]), 3), float(r.get("q") or 0)))
        out = []
        for ex, pr in rows.items():
            f = self.fair.get(ex)
            if f is None or len(pr) < 20:
                continue
            side = "yes" if f >= 0.5 else "no"             # the favourite side (what we would hold)
            vals = sorted(((p if side == "yes" else round(1 - p, 3)), q) for _, p, q in pr)
            tot = sum(q for _, q in vals)
            if tot <= 0:
                continue
            acc, p10, p90 = 0.0, None, None
            for p_, q in vals:
                acc += q
                if p10 is None and acc >= 0.10 * tot:
                    p10 = p_
                if p90 is None and acc >= 0.90 * tot:
                    p90 = p_
            seq = [(p if side == "yes" else round(1 - p, 3)) for _, p, _ in sorted(pr)]
            hi_visits = sum(1 for a, b2 in zip(seq, seq[1:]) if a < p90 - 1e-9 <= b2)
            lo_visits = sum(1 for a, b2 in zip(seq, seq[1:]) if a > p10 + 1e-9 >= b2)
            out.append(dict(ex=ex, name=self.name(ex), side=side, fair_side=round(f if side == "yes" else 1 - f, 4), p10=p10, p90=p90, range_c=round((p90 - p10) * 100, 1),
                            prints=len(pr), shares=round(tot), hi_visits=hi_visits, lo_visits=lo_visits, round_trips=min(hi_visits, lo_visits)))
        out.sort(key=lambda r: -(r["range_c"] * r["round_trips"]))
        try:
            os.makedirs(os.path.dirname(path), exist_ok=True)
            with open(path, "w") as fh:
                json.dump(dict(day=day, t=stamp(), rows=out), fh)
            top = out[:5]
            self.night.line(f"RANGE candidates {day} (24-h tape, favourite side, sell at p90 / buy back at p10): " + "; ".join(f"{r['name']} {r['side'].upper()} {r['p10']:.3f}-{r['p90']:.3f} ({r['range_c']:.1f}c, {r['round_trips']} round trips)" for r in top))
        except OSError:
            pass

    def scoreboard_update(self, now):
        """C88 (Piero 01:55 item 4): per strategy and per book, today (SIG day) and since Oct 1: edge bought (entry fills, at the fill's
        fair), edge given back (exit fills: fair - sale price), realized, fair drift (held lots: fair now - fair at entry). Every 5 min
        -> self.scoreboard and data/intel/scoreboard.json. Target: bought - given back >= 2,500 a day."""
        if now - self.scoreboard_t < SCOREBOARD_EVERY_S:
            return
        self.scoreboard_t = now
        day0 = now - (now % 86400)                          # 00:00 UTC
        core_subs = {(ex, sub) for ex, sub in self.core_set}
        sold_core = {(str(x.get("ex")), x.get("sub")) for x in ((self.controls.d.get("core") or {}).get("sold") or [])}
        blank = lambda: dict(bought=0.0, given=0.0, realized=0.0, drift=0.0, n_in=0, n_out=0)
        out = {"today": dict(strategy=collections.defaultdict(blank), book=collections.defaultdict(blank)),
               "since": dict(strategy=collections.defaultdict(blank), book=collections.defaultdict(blank))}
        fair_at = collections.defaultdict(lambda: [0.0, 0.0])
        for f in list(self.ledger.fills):
            if f.get("before_engine") or f.get("action") not in ("buy", "sell") or f.get("edge") is None:
                continue
            try:
                t = time.mktime(time.strptime(f["t"], "%Y-%m-%d %H:%M:%S"))
            except ValueError:
                continue
            rec = self.ledger.orders.get(str(f.get("order") or "")) or {}
            sub = rec.get("sub") or "inventory"
            strat = dash5.bucket_of_layer(f.get("layer"), sub, f["ex"])
            apart = strat in ("ri", "walls") or sub.startswith(CORE_SKIP_SUBS) or f["ex"] in dash5.RI_EX or f.get("bot") in self.position_names
            book = "apart" if apart else "core" if ((f["ex"], sub) in core_subs or (f["ex"], sub) in sold_core) else "trading"   # Piero's position orders are neither book
            exit_ = f.get("action") == "sell" or f.get("layer") in self.REDUCING
            e = float(f["edge"]) * float(f["qty"])
            for scope in (("since",) + (("today",) if t >= day0 else ())):
                for key, grp in ((strat, "strategy"), (book, "book")):
                    r = out[scope][grp][key]
                    if exit_:
                        r["given"] += -e; r["realized"] += float(f.get("realized") or 0.0); r["n_out"] += 1
                    else:
                        r["bought"] += e; r["n_in"] += 1
            if not exit_ and f.get("fair") is not None:
                fa = fair_at[(f["ex"], sub)]
                fa[0] += float(f["fair"]) * float(f["qty"]); fa[1] += float(f["qty"])
        for ex, side, sub, q, avg, fs, cost in self.engine_lots():
            fa = fair_at.get((ex, sub))
            if fs is None or not fa or fa[1] <= 0:
                continue
            drift = (fs - fa[0] / fa[1]) * q
            strat = dash5.bucket_of_sub(sub, ex)
            book = "core" if (ex, sub) in core_subs else "trading"
            for scope in ("today", "since"):
                out[scope]["strategy"][strat]["drift"] += drift
                out[scope]["book"][book]["drift"] += drift
        res = {}
        for scope in ("today", "since"):
            res[scope] = {}
            for grp in ("strategy", "book"):
                res[scope][grp] = {k: dict((x, round(y, 2)) for x, y in v.items()) | {"net": round(v["bought"] - v["given"], 2)} for k, v in out[scope][grp].items()}
        today_net = sum(v["net"] for k, v in res["today"]["book"].items() if k != "apart")   # the target is for the two books; Piero's own orders are apart
        frac = max(0.05, (now - day0) / 86400)
        res.update(t=stamp(), target_day=SCOREBOARD_TARGET_DAY, today_net=round(today_net, 2), pace_day=round(today_net / frac, 2), core_cost=(self.controls.d.get("core") or {}).get("cost"))
        self.scoreboard = res
        try:
            os.makedirs(os.path.join(DATA, "intel"), exist_ok=True)
            tmp = os.path.join(DATA, "intel", "scoreboard.json.tmp")
            with open(tmp, "w") as fh:
                json.dump(res, fh)
            os.replace(tmp, os.path.join(DATA, "intel", "scoreboard.json"))
        except OSError:
            pass

    def c88_migration(self, now):
        """Once (C88): the markets feed goes live for books; the reads-by-purpose line before the switch (the 'after' line follows 30 min later)."""
        c = self.controls
        d = c.d.setdefault("c88", {})
        if d.get("done") or os.environ.get("SIG_C88", "1") != "1":
            return
        before = api.reads_by_purpose(600)
        with c.lock:
            c.d["feed_mode"] = "live"
            d.update(done=stamp(), reads_before=before)
            c.save()
        self.reads_after_t = now + 1800
        msg = (f"C88 (Piero 01:55): books from the SIG realtime feed (mode live; REST books only on resync, gaps or asOf > 10 s); CORE / TRADING books on; P&L stops are alerts; "
               f"reads/min by purpose BEFORE: " + ", ".join(f"{k} {v:.1f}" for k, v in sorted(before.items(), key=lambda kv: -kv[1])[:10]))
        self.event("CONTROL", msg); self.night.line(msg)

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

    AUTO_KINDS = PAUSE_KINDS

    def errors_10m(self, now, window=600):
        """The engine's own ERR events in the last `window` s (the events carry a same-day HH:MM:SS stamp), one per message a minute.
        C101 item D: an external service's error (HTTP / URL / timeout) is not one — Polymarket's are pm_stale's, SIG's api_errors' /
        stop_pull's (08:27 Oct 21: 55 Polymarket 404s in 85 s paused new risk 20 min while the fair stayed fresh)."""
        n, lt, seen = 0, time.localtime(now), set()
        for e in self.events[-400:]:
            txt = str(e.get("text", ""))
            if e.get("kind") != "ERR" or "429" in txt or "RATE_LIMITED" in txt or any(x in txt for x in ENGINE_ERR_EXTERNAL):
                continue                                    # rate-limit answers count under api_errors, not here
            try:
                hh, mm, ss = (int(x) for x in str(e.get("t", "")).split(":"))
            except ValueError:
                continue
            t_ = time.mktime((lt.tm_year, lt.tm_mon, lt.tm_mday, hh, mm, ss, 0, 0, -1))
            if 0 <= now - t_ <= window and (txt[:80], hh, mm) not in seen:
                seen.add((txt[:80], hh, mm))
                n += 1
        return n

    def hard_pauses(self, now):
        """C88 (Piero 01:55): the only automatic pauses — cash under the 5k floor, an I2 breach, SIG API errors / a 429 storm, the
        Polymarket fair stale > 2 min, engine errors (>= 5 ERR in 10 min). Each lifts itself in auto_resume once its condition is gone."""
        c = self.controls
        if c.paused:
            return
        a = self.acct or {}
        cash = (a.get("totalAccountValue") or 0) - (a.get("totalHoldingsValue") or 0)
        h = api.health(600)
        trims = sum(self.ex.trims.values())
        if trims > self.trims_seen:
            self.trims_seen, self.trim_t = trims, now
        reasons = []
        if a and cash < CASH_RESERVE:
            reasons.append(("cash_floor", f"cash {cash:,.0f} under the {CASH_RESERVE:,} floor"))
        if self.trim_t and now - self.trim_t < 60:
            reasons.append(("i2_breach", f"I2 trim (orders cut to fit the position): {trims} in all"))
        if (h["read"]["n"] >= 20 and h["read"]["bad_rate"] > 0.5) or (h.get("throttled") or 0) >= API_429_STORM_10M:
            reasons.append(("api_errors", f"SIG API: {h['read']['bad_rate']:.0%} of {h['read']['n']} reads failed, {h.get('throttled') or 0} rate-limit answers in 10 min"))
        if self.fair_t and now - self.fair_t > PM_STALE_S:
            reasons.append(("pm_stale", f"Polymarket fair {now - self.fair_t:.0f} s old"))
        n_err = self.errors_10m(now)
        if n_err >= ENGINE_ERRORS_10M:
            reasons.append(("engine_errors", f"{n_err} engine errors in 10 min"))
        if reasons:
            kind, why = reasons[0]
            self.auto_pause(kind, f"{kind}: {why}", now)
            msg = f"PAUSE ({kind}): {why} — new risk paused; exits keep running; resumes itself when clear"
            self.alerts.append(f"{time.strftime('%H:%M')} {msg}"); self.event("PAUSE", msg); self.night.line(msg)

    def travel(self, on, now=None):
        """Day 6 §4.2 `./travel.sh`: on -> pull every engine order except position exits and longshot bids (exits and
        sell-backs stay), pause new risk; the engine lifts the pause itself once SIG and Polymarket have answered for 2 min
        after the wake. off -> resume now."""
        now = now or time.time()
        c = self.controls
        if on:
            self.auto_pause("travel", "travel: lid about to close (./travel.sh)", now)
            n = 0
            for o in list(self.open):
                rec = self.ledger.orders.get(o["id"])
                if not rec or rec["bot"] in self.position_names or rec["layer"] in self.REDUCING or rec["layer"] == "longshot":
                    continue
                n += bool(self.ex.cancel(o, reason="travel"))
            dropped = self.ex.drop_queued(lambda b, s_, l, od: l not in self.REDUCING and l != "longshot" and b not in self.position_names)
            with c.lock:
                c.d["travel"] = dict(on=True, t=now)
                c.save()
            self.wake_t = None
            msg = (f"TRAVEL on: {n} engine orders being cancelled, {dropped} queued dropped; position exits, sell-backs and longshot bids stay; "
                   f"new risk paused until SIG and Polymarket answer for 2 min after the wake")
        else:
            with c.lock:
                c.d["travel"] = dict(on=False, t=now)
                c.d["auto_pause"] = dict(history=(c.d.get("auto_pause") or {}).get("history", {}))
                c.save()
            c.set_paused(False, "travel off", by="travel.sh")
            msg = "TRAVEL off: new risk resumed by hand"
        self.event("TRAVEL", msg)
        self.night.line(msg)
        return msg

    def travel_ok(self, now):
        """The travel pause lifts 2 min after a wake (a loop gap > 5 min) once SIG reads and the Polymarket feed are healthy."""
        if not (self.controls.d.get("travel") or {}).get("on"):
            return True                                     # travel.sh off: nothing holds it
        if not getattr(self, "wake_t", None):
            return False
        h = api.health(120)
        bad = (h.get("read") or {}).get("bad_rate")
        return (now - self.wake_t >= 120 and (bad is None or bad == 0) and self.fills_health(now)[0] and bool(self.fair)
                and (not self.pm_feed or self.pm_feed.last_msg_t is None or now - self.pm_feed.last_msg_t < 120))

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
        """Every 5 min: lift an automatic pause whose condition is gone (cash >= floor + 1k for 2 loops; SIG writes >= 80%
        OK over the last 20; fills healthy). Also logs any pause > 10 min at the top of RUN_LOG (hourly status)."""
        c = self.controls
        ap = c.d.get("auto_pause") or {}
        if not c.paused or not ap.get("kind"):
            return
        a = self.acct or {}
        cash = (a.get("totalAccountValue") or 0) - (a.get("totalHoldingsValue") or 0)
        self.cash_ok_n = self.cash_ok_n + 1 if cash >= CASH_RESERVE + CASH_FLOOR_RESUME else 0   # C93e (Piero 14:58): floor + 1,000 for 2 loops (was + 5,000)
        if ap.get("stuck") or now - (ap.get("t") or now) < 300:   # re-checked every loop (local data) from 5 min after the pause
            return
        kind = ap["kind"]
        h = api.health(600)
        ok = {"cash_short": self.cash_ok_n >= 2,
              "cash_floor": self.cash_ok_n >= 2,
              "stop_pull": api.recent_writes_ok(20) >= 0.8,
              "fills_blind": not self.fills_blind and self.fills_health(now)[0],
              "travel": self.travel_ok(now),
              "i2_breach": not self.trim_t or now - self.trim_t >= I2_RESUME_S,
              "api_errors": (h["read"]["bad_rate"] < 0.1) and (h.get("throttled") or 0) < 3,
              "pm_stale": bool(self.fair_t) and now - self.fair_t < 60,
              "engine_errors": self.errors_10m(now, ENGINE_ERR_RESUME_S) == 0}.get(kind, False)   # C101 item D: 3 clean minutes
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
        every = RATCHET_EVERY_S if layer == "dump" else 2 * 3600       # day 6: dump hourly; scalper / touch every 2 h
        if now < r.get("hold_until", 0) or now - r.get("since", now) < every:
            return
        since = r.get("since", now - every)
        if layer == "dump":
            need = RATCHET_MIN_FILLS_DAY if self.day_window(now) else RATCHET_MIN_FILLS_NIGHT
            g = self.dump_gate(since, now, min_fills=need)   # the hourly gate judges on the hour's fills (not the pilot's 20)
            if g["stats"]["fills"] < need:                  # day 6 §1.1: too few fills to judge -> hold the level (not a fail)
                with c.lock:
                    r["since"] = now
                    r.setdefault("history", []).append(dict(t=stamp(), level=r.get("level", 0), ok=None))
                    r["history"] = r["history"][-50:]
                    c.save()
                self.event("RATCHET", f"dump ratchet held at level {r.get('level', 0)}: {g['stats']['fills']} fills in the hour (< {need})")
                return
        elif layer == "touch":
            g = self.touch_gate(since, now)
        else:
            g = self.scalp_gate(since, now)
        with c.lock:
            if g["ok"]:
                r["level"] = int(r.get("level", 0)) + 1
                step = f"{layer} ratchet up to level {r['level']} (x{RATCHET_STEP ** r['level']:.2f})"
            else:
                r["level"] = max(0, int(r.get("level", 0)) - 1)
                r["hold_until"] = now + every
                step = f"{layer} ratchet back to level {r['level']} (x{RATCHET_STEP ** r['level']:.2f}), holding {every // 3600} h"
            r["since"] = now
            r.setdefault("history", []).append(dict(t=stamp(), level=r["level"], ok=g["ok"]))
            r["history"] = r["history"][-50:]
            c.save()
        msg = f"RATCHET {step}: {g['why']}"
        self.event("RATCHET", msg)
        self.night.line(msg)

    # ------------------------------------------------------------------------------------------------ scalper (day 5 §2, Mode A)
    def note_volume(self, ex, ts, notional, off=False):
        """Our own tape -> traded value per market over the last 24 h (the scalper's market ranking) and, day 11 §2, the value
        printed >= 2c off fair (the touch maker's priority = volume x the off-fair share)."""
        d = self.vol24.setdefault(str(ex), collections.deque())
        d.append((ts, notional))
        cut = time.time() - 86400
        while d and d[0][0] < cut:
            d.popleft()
        if off:
            o = self.off24.setdefault(str(ex), collections.deque())
            o.append((ts, notional))
            while o and o[0][0] < cut:
                o.popleft()

    @staticmethod
    def print_off_fair(r):
        """A tape print (YES price p, fair) is >= 2c off fair on one side or the other."""
        f = r.get("fair")
        return f is not None and r.get("p") is not None and abs(float(r["p"]) - float(f)) >= TM_E_MIN - 1e-9

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
                        self.note_volume(r["ex"], r["ts"], float(r["p"]) * float(r.get("q") or 0), off=self.print_off_fair(r))

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
        """C87 (Piero 19:56 d): the scalper v1 is deleted (releases/archive/scalp_layer.py); its leftover shares went to 'held'."""
        return {}

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
                hh = time.localtime(now).tm_hour + time.localtime(now).tm_min / 60
                if age > SCALP_HAND_S or (SCALP_DAY_OFF and SCALP_DAY_OFF[1] <= hh < SCALP_DAY_OFF[3]):   # 2 h old: the tranche rules
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
                floor_ = avg + SCALP_SELL_MIN                # day 6 §9 rule 0: never below entry + 0.5c (the day-5 "entry + 0 after 30 min" step is gone)
                p = bots.tick_up(round(max(round(best_ask - TICK, 3), floor_), 6))
                self.scalp_view.setdefault(ex, []).append(dict(sub=sub, side=side, px=min(p, 0.99), avg=avg, age_min=round(age / 60, 1)))
        for sub, t in tr.items():
            if t.get("kind") == "scalp" and t.get("state") in ("open", "closed") and not t.get("closed_ts") \
                    and abs(self.ledger.subbooks.get(t["ex"], {}).get(sub, 0)) < MIN_QTY:
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

    def day7_migrations(self):
        """Once (flags in overrides): the touch book becomes dump L0 tranches under rule 0 (same entry, the dump sell-back
        ladder), the touch layer goes off; the cash target restarts at 25k on the bounded rule; the dump ratchet replays
        today's hourly history with the >= 10 / >= 5 fills rule and restores the level it gives."""
        c = self.controls
        d7 = c.d.setdefault("day7", {})
        now = time.time()
        if not d7.get("touch_to_l0"):
            moved = []
            for ex, books in list(self.ledger.subbooks.items()):
                for sub, q in list(books.items()):
                    if sub.startswith("touch:") and abs(q) >= MIN_QTY:
                        side = sub.split(":")[2]
                        dsub = f"dump:{ex}:{side}:L0"
                        t = self.ledger.tranches.get(sub) or {}
                        self.ledger.move_sub(ex, sub, dsub)
                        self.ledger.tranches[dsub] = dict(ex=ex, side=side, kind="dump", pre_ref_side=None, t=t.get("t") or stamp(),
                                                          ts=t.get("ts") or now, state="open", avg=t.get("avg"), from_="touch")
                        if t:
                            t["state"] = "handed"
                        moved.append((self.name(ex), side, abs(q), t.get("avg")))
            with c.lock:
                d7["touch_to_l0"] = stamp()
                if c.d["layers"].get("touch", {}).get("state") != "off":
                    c.d["layers"].setdefault("touch", dict(state="off", caps={}, killed=None))["state"] = "off"
                c.save()
            if moved:
                msg = "DAY 7: the touch book became dump L0 tranches (same entry, the dump sell-back ladder, rule 0): " + "; ".join(
                    f"{n} {sd.upper()} {q:,.0f} at {a if a is None else round(a, 3)}" for n, sd, q, a in moved) + "; touch layer off"
                self.event("DUMP", msg)
                self.night.line(msg)
            self.ledger.save()
        if not d7.get("cash_target_v2"):
            with c.lock:
                c.d["cash_target"] = dict(T=float(CASH_T_START), hour=int(now // 3600), idle=0, hist=[])
                d7["cash_target_v2"] = stamp()
                c.save()
            self.event("CASH", "DAY 7: cash target reset to 25,000; rises only after >= 15 constrained min or bounded peak use >= 70%")
        if not d7.get("phantom_2217"):                     # the same pattern at 22:17:45 (booked under C64, before the both-sides rule)
            bad = [f for f in self.ledger.fills if f["t"] == "2026-10-05 22:17:45" and f.get("source") == "order_shrink" and f.get("layer") == "fatfinger"]
            with self.ledger.lock:
                for f in bad:
                    order = self.ledger.orders.get(str(f.get("order"))) or {}
                    if order:                               # the sub-books were already zeroed by subbook_fix at 23:03; fix the records only
                        order["filled"] = max(0.0, (order.get("filled") or 0) - f["qty"])
                        order["cost"] = max(0.0, (order.get("cost") or 0) - f["qty"] * f["price"])
                keys = {f["key"] for f in bad}
                self.ledger.fills = [f for f in self.ledger.fills if f.get("key") not in keys]
            with c.lock:
                d7["phantom_2217"] = stamp()
                c.save()
            if bad:
                self.event("AUDIT", f"PHANTOM: the six 22:17:45 fat-finger order_shrink fill records removed (sub-books were zeroed by subbook_fix at 23:03): "
                                    + "; ".join(f"{self.name(f['ex'])} {f['side'].upper()} {f['qty']:,.0f}" for f in bad))
            self.ledger.save()
        if not d7.get("phantom_1815"):
            bad = [f for f in self.ledger.fills if f["t"] == "2026-10-05 18:15:04" and f.get("source") == "order_shrink" and f.get("layer") == "fatfinger"]
            with self.ledger.lock:
                for f in bad:
                    order = self.ledger.orders.get(str(f.get("order"))) or {}
                    delta = (1 if f["action"] == "buy" else -1) * (f["qty"] if f["side"] == "yes" else -f["qty"])
                    books = self.ledger.subbooks.setdefault(f["ex"], {})
                    if order.get("sub"):
                        books[order["sub"]] = books.get(order["sub"], 0) - delta
                        if abs(books[order["sub"]]) < 1:
                            books.pop(order["sub"], None)
                    if order:
                        order["filled"] = max(0.0, (order.get("filled") or 0) - f["qty"])
                        order["cost"] = max(0.0, (order.get("cost") or 0) - f["qty"] * f["price"])
                keys = {f["key"] for f in bad}
                self.ledger.fills = [f for f in self.ledger.fills if f.get("key") not in keys]
            with c.lock:
                d7["phantom_1815"] = stamp()
                c.save()
            if bad:
                msg = ("PHANTOM: the six 18:15:04 fat-finger order_shrink fills removed (both sides of FL-22 D, VA-01 D, NY-17 R vanished at once "
                       "when SIG pulled them at the cash floor; SIG's position 0, cash unchanged): " + "; ".join(f"{self.name(f['ex'])} {f['side'].upper()} {f['qty']:,.0f}" for f in bad))
                self.event("AUDIT", msg)
                self.night.line(msg)
            self.ledger.save()
        if not d7.get("ratchet_reeval"):
            r = (c.d.get("ratchet") or {}).get("dump") or {}
            hist = r.get("history") or []
            if hist:
                t0 = time.mktime(time.strptime(hist[0]["t"], "%Y-%m-%d %H:%M:%S"))
                level, hold_until, t, rows = 0, 0.0, t0, []
                while t <= now:
                    if t >= hold_until:
                        need = RATCHET_MIN_FILLS_DAY if self.day_window(t) else RATCHET_MIN_FILLS_NIGHT
                        g = self.dump_gate(t - RATCHET_EVERY_S, t, min_fills=need)
                        if g["stats"]["fills"] < need:
                            rows.append((time.strftime("%H:%M", time.localtime(t)), level, None))
                        elif g["ok"]:
                            level += 1
                            rows.append((time.strftime("%H:%M", time.localtime(t)), level, True))
                        else:
                            level = max(0, level - 1)
                            hold_until = t + RATCHET_EVERY_S
                            rows.append((time.strftime("%H:%M", time.localtime(t)), level, False))
                    t += RATCHET_EVERY_S
                with c.lock:
                    rr = c.d.setdefault("ratchet", {})
                    rr["dump"] = dict(level=level, since=now, hold_until=0.0,
                                      history=(hist[-20:] + [dict(t=stamp(), level=level, ok=None, note="day 7 re-evaluation")]))
                    d7["ratchet_reeval"] = stamp()
                    c.save()
                msg = (f"DAY 7: dump ratchet re-evaluated over today's hours with the >= 10 (day) / >= 5 (night) rule: level {level} "
                       f"(x{RATCHET_STEP ** level:.2f}); hourly: " + " ".join(f"{h}:{'hold' if ok is None else 'up' if ok else 'back'}" for h, lv, ok in rows[-12:]))
                self.event("RATCHET", msg)
                self.night.line(msg)

    # ------------------------------------------------------------------------------------------------ manual walls (Piero's pilot)
    def walls(self):
        return list(self.controls.d.get("walls") or [])

    def wall_resting(self):
        """Day 8 §2 (C70): Piero's walls resting or queued, at cost x their weight (1.0): they come off the top of the entry
        budget before the dump levels are allocated (the walls are his orders, the engine never cuts them for its own levels)."""
        out = sum(o["qty"] * o["price"] for o in self.open
                  if (self.ledger.orders.get(o["id"]) or {}).get("layer") == "wall" and o["action"] == "buy" and o["id"] not in self.ex.cancelling)
        out += sum((p.get("qty") or 0) * (p.get("price") or 0) for p in self.ex.pending() if p.get("layer") == "wall")
        return out * ENTRY_WEIGHTS["wall"]

    def wall_cost(self):
        return sum(abs(q) * float((self.ledger.tranches.get(sub) or {}).get("avg") or self.sig_mark(ex, "yes" if q > 0 else "no") or 0)
                   for ex, books in self.ledger.subbooks.items() for sub, q in books.items() if sub.startswith("wall:") and abs(q) >= 1)

    def wall_plan(self, now, cash):
        """{ex: [want-spec]}: each active wall rests as one order at its price; its quantity is cut so that a full fill leaves
        >= 2k free above the floor (none if that is < MIN_QTY); it counts against the entry budget; the news guard pulls it
        when Polymarket's fair for the market moves >= 2c against it within 10 min; it expires at its TTL."""
        c = self.controls
        plan = {}
        if not c.on("wall") or c.paused:
            return {}
        free = cash - CASH_RESERVE
        changed = False
        for w in self.walls():
            if w.get("state") != "active":
                continue
            ex, side = str(w["ex"]), w["side"]
            if now - w["t"] >= w.get("ttl_s", WALL_TTL_S):
                w["state"], changed = "expired", True
                self.event("WALL", f"WALL {w['id']} {self.name(ex)} {side.upper()} expired at its TTL ({w.get('ttl_s', WALL_TTL_S) / 3600:.0f} h), filled {w.get('filled', 0):,.0f} of {w['qty']:,}")
                self.night.line(f"WALL {w['id']} expired: filled {w.get('filled', 0):,.0f} of {w['qty']:,}")
                continue
            f = self.fair.get(ex)
            if f is not None:                               # news guard: fair for the side we are SELLING rises >= 2c in 10 min
                h = [x for x in self.fair_hist.get(ex, []) if x[0] >= now - WALL_NEWS_WINDOW_S]
                if not h or now - h[-1][0] >= 5:
                    h.append((now, f))
                    self.fair_hist[ex] = h
                sold = "no" if side == "yes" else "yes"      # buying NO = selling YES: a YES rise is against us
                cur = f if sold == "yes" else 1 - f
                low = min((v if sold == "yes" else 1 - v) for _, v in h + [(now, f)])
                if cur - low >= w.get("news_c", WALL_NEWS_C) - 1e-9:
                    w["state"], changed = "pulled", True
                    self.event("WALL", f"WALL {w['id']} {self.name(ex)}: Polymarket {sold.upper()} rose {(cur - low) * 100:.1f}c in 10 min: wall pulled")
                    self.night.line(f"WALL {w['id']} pulled by the news guard ({sold.upper()} +{(cur - low) * 100:.1f}c in 10 min); filled {w.get('filled', 0):,.0f}")
                    continue
            fs_w = self.side_fair(ex, side)
            if w.get("min_edge") is not None and fs_w is not None:   # C76 (Piero): rests only while the edge vs fair >= the wall's floor
                if fs_w - w["px"] < w["min_edge"] - 1e-9:
                    if not w.get("edge_off"):
                        w["edge_off"], changed = now, True
                        self.event("WALL", f"WALL {w['id']} {self.name(ex)}: edge {(fs_w - w['px']) * 100:.1f}c < {w['min_edge'] * 100:.0f}c floor: pulled until it returns")
                    continue
                if w.get("edge_off"):
                    w["edge_off"], changed = None, True
                    self.event("WALL", f"WALL {w['id']} {self.name(ex)}: edge {(fs_w - w['px']) * 100:.1f}c >= {w['min_edge'] * 100:.0f}c again: re-placed")
            if w.get("once") and (w.get("filled") or 0) >= 1:
                if not w.get("once_done"):
                    w["once_done"], changed = now, True
                    self.event("WALL", f"WALL {w['id']} {self.name(ex)}: filled {w['filled']:,.0f} of {w['qty']:,} — one fill only, not re-rested")
                continue
            min_free = float(w.get("min_free") or WALL_MIN_FREE)
            left_q = int(w["qty"] - (w.get("filled") or 0))
            others = sum((x["qty"] - (x.get("filled") or 0)) * x["px"] for x in self.walls()   # C78: the cut is netted across the resting walls
                         if x is not w and x.get("state") == "active" and not (x.get("once") and (x.get("filled") or 0) >= 1) and not x.get("edge_off")
                         and x["id"] < w["id"])                 # earlier walls keep their size; later ones are cut by what the earlier could still fill
            cap_q = int(max(0.0, free - min_free - others) / w["px"])
            q = min(left_q, cap_q)
            if q < MIN_QTY:
                if now - w.get("nocash_t", 0) >= 1800:
                    w["nocash_t"], changed = now, True
                    self.event("WALL", f"WALL {w['id']} {self.name(ex)}: a full fill would leave < {min_free:,.0f} free (free {free:,.0f}): not resting")
                continue
            if q < left_q and now - w.get("cut_t", 0) >= 1800:
                w["cut_t"], changed = now, True
                self.event("WALL", f"WALL {w['id']} {self.name(ex)}: cut to {q:,} of {left_q:,} so a full fill leaves >= {min_free:,.0f} free")
            plan.setdefault(ex, []).append(dict(side=side, px=w["px"], qty=q, sub=f"wall:{ex}:{side}:{w['id']}", ttl=w.get("ttl_s", WALL_TTL_S)))
            self.entry_left = max(0.0, getattr(self, "entry_left", float("inf")) - q * w["px"] * ENTRY_WEIGHTS["wall"])
        if changed:
            with c.lock:
                c.save()
        return plan

    def track_walls(self, now):
        """A filled wall rests its buy-back (the exit price Piero gave, in YES terms) for the filled quantity; after the hold
        time it is held to settlement (no exit). Rule 0 applies to the buy-back like to every sale."""
        self.wall_view = {}
        c = self.controls
        changed = False
        for w in self.walls():
            ex, side = str(w["ex"]), w["side"]
            sub = f"wall:{ex}:{side}:{w['id']}"
            q = self.ledger.subbooks.get(ex, {}).get(sub, 0)
            if abs(q) < MIN_QTY:
                continue
            if (q > 0) != (side == "yes"):
                continue
            t = self.ledger.tranches.get(sub)
            if t is not None and t.get("state") == "held":
                continue                                    # held to settlement: no buy-back, ever
            if f"{ex}|{sub}" in set(self.controls.d.get("sell_subs") or []):
                continue                                    # Piero 17:35: the seller offers this lot at the touch; no second exit
            if t is None or t.get("state") != "open":
                t = self.ledger.tranches[sub] = dict(ex=ex, side=side, kind="wall", t=stamp(), ts=now, state="open", avg=w["px"])
            avg = self.ledger.tranche_avg(sub, "wall", ex=ex, side=side, since=time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(t["ts"] - 300))) or t.get("avg")
            t["avg"] = round(avg, 5)
            if now - t["ts"] > w.get("hold_s", WALL_HOLD_S):
                if t.get("state") != "held":
                    t["state"], changed = "held", True
                    self.event("WALL", f"WALL {w['id']} {self.name(ex)}: {abs(q):,.0f} {side.upper()} not bought back within {w.get('hold_s', WALL_HOLD_S) / 3600:.0f} h: held to settlement")
                    self.night.line(f"WALL {w['id']}: {abs(q):,.0f} {side.upper()} held to settlement (fair {self.side_fair(ex, side)})")
                continue
            if w.get("exit_min_free") is not None:            # C77 (Piero): held to settlement while free >= exit_min_free
                free_now = (min(self.cash_hist) if self.cash_hist else 0.0) - CASH_RESERVE
                if free_now >= float(w["exit_min_free"]):
                    if w.get("exit_on"):
                        w["exit_on"], changed = False, True
                        self.event("WALL", f"WALL {w['id']} {self.name(ex)}: free {free_now:,.0f} >= {w['exit_min_free']:,.0f}: buy-back pulled, {abs(q):,.0f} {side.upper()} held")
                    continue
                if not w.get("exit_on"):
                    w["exit_on"], changed = True, True
                    self.event("WALL", f"WALL {w['id']} {self.name(ex)}: free {free_now:,.0f} < {w['exit_min_free']:,.0f}: buy-back at YES {float(w['exit_px']):.3f} rests for {abs(q):,.0f} {side.upper()}")
            exit_yes = float(w["exit_px"])                    # the buy-back price in YES terms
            sell_px = (1 - exit_yes) if side == "no" else exit_yes   # what the held side fetches
            sell_px = max(sell_px, avg + RULE0_MARGIN)        # never below entry + 0.5c (rule 0)
            q_exit = abs(q)
            if sub in self.reservoir_subs(ex) and self.controls.on("swing"):
                q_exit = 0.0                                # C84c/g: a reservoir owns these lots (its asks replace the wall buy-backs)
            elif sub in (self.swing_cfg().get("wall_subs") or []) and self.controls.on("swing"):   # day 10 §3: the swing owns the slice; the wall exit covers only the hold part
                q_exit = max(0.0, abs(q) - self.swing_slice_alloc().get(sub, 0.0))
            if q_exit < MIN_QTY:
                continue
            self.wall_view.setdefault(ex, []).append(dict(sub=sub, side=side, sell_px=round(sell_px, 3), avg=avg, qty=int(q_exit)))
        if changed:
            with c.lock:
                c.save()

    def wall_fill_events(self, fills):
        c = self.controls
        changed = False
        for f in fills:
            if f["layer"] not in ("wall", "wallexit"):
                continue
            sub = (self.ledger.orders.get(f["order"]) or {}).get("sub", "")
            wid = sub.rsplit(":", 1)[-1]
            for w in self.walls():
                if str(w["id"]) == wid and f["layer"] == "wall":
                    w["filled"] = (w.get("filled") or 0) + f["qty"]
                    w["fill_t"] = w.get("fill_t") or f["t"]
                    changed = True
            fs = self.side_fair(f["ex"], f["side"])
            if f["layer"] == "wall":
                msg = f"WALL fill {self.name(f['ex'])}: bought {f['qty']:,.0f} {f['side'].upper()} at {f['price']:.3f}" + ("" if fs is None else f" (fair {fs:.3f}, {(fs - f['price']) * 100:.1f}c under)")
            else:
                msg = f"WALL buy-back {self.name(f['ex'])}: {f['qty']:,.0f} closed at {1 - f['price']:.3f} (YES bid {f['price']:.3f}), realized {f.get('realized') or 0:+,.0f}"
            self.event("WALL", msg)
            self.night.line(msg)
        if changed:
            with c.lock:
                c.save()

    # ------------------------------------------------------------------------------------------------ touch layer (day 6 §2)
    def touch_budget(self):
        """The layer's budget at cost: the cap (pilot half, full 15k; a failed scalper gate adds its slice) x the ratchet, <= 30k."""
        return min(float(TOUCH_RATCHET_MAX), self.cap("touch", "budget") * self.ratchet_mult("touch"))

    def touch_cost(self, ex=None):
        """Touch inventory at cost (its tranche's average entry), one market or all."""
        out = 0.0
        for e, books in self.ledger.subbooks.items():
            if ex is not None and e != ex:
                continue
            for sub, q in books.items():
                if sub.startswith("touch:") and abs(q) >= MIN_QTY:
                    avg = (self.ledger.tranches.get(sub) or {}).get("avg") or self.sig_mark(e, "yes" if q > 0 else "no") or 0.0
                    out += abs(q) * float(avg)
        return out

    def race_cost(self, ex):
        """What we hold at cost on every market of ex's race (RI Senate R NO counts for RI Senate)."""
        race = (self.meta.get(ex) or {}).get("race")
        out = 0.0
        for e, m in self.meta.items():
            if m.get("race") != race:
                continue
            b = self.book.get(e) or {}
            q = b.get("pos", 0)
            if abs(q) >= 1:
                out += abs(q) * float((b.get("row") or {}).get("avgCost") or self.sig_mark(e, "yes" if q > 0 else "no") or 0.0)
        return out

    def touch_news(self, ex, side, now):
        """§2 news guard: fair_side down 2c in 10 min, or SIG down 3c in 10 min while Polymarket also moved against us."""
        f = self.fair.get(ex)
        if f is None:
            return False
        h = [x for x in self.fair_hist.get(ex, []) if x[0] >= now - FAT_NEWS_WINDOW_S]
        if not h or now - h[-1][0] >= 5:
            h.append((now, f))
            self.fair_hist[ex] = h                          # kept here too: dump_plan only samples while the dump layer is on
        cur = f if side == "yes" else 1 - f
        top = max((v if side == "yes" else 1 - v) for _, v in h + [(now, f)])
        pm_drop = top - cur
        mids = [(t, m) for t, m, _ in (self.mids.get(ex) or []) if t >= now - 600]
        sig_drop = 0.0
        if len(mids) >= 2:
            s_top = max((m if side == "yes" else 1 - m) for _, m in mids)
            s_cur = mids[-1][1] if side == "yes" else 1 - mids[-1][1]
            sig_drop = s_top - s_cur
        return pm_drop >= TOUCH_NEWS_FAIR - 1e-9 or (sig_drop >= TOUCH_NEWS_SIG - 1e-9 and pm_drop > 0.004)

    def touch_plan(self, now, cash, used):
        """{ex: [want-spec]} (day 6 §2): favourite sides (fair_side >= 0.93) whose SIG best bid sits >= 6c under fair: join
        the best bid (never improve, never cross), 2,000 shares, one order per side, requoted only when the touch moves;
        <= 8k per race at cost (holdings included), the layer budget at cost, the 40k party cap shared with the dump
        catcher; no new buys while the dump catcher's funded reserve < 15k, new risk is paused or fills are blind."""
        c = self.controls
        if not c.on("touch") or c.paused or self.fills_blind:
            return {}
        funded = max(0.0, min(float(OPPORTUNITY_RESERVE), cash - CASH_RESERVE - used.get("committed", 0.0)))
        existing = {}
        for o in self.open:
            rec = self.ledger.orders.get(o["id"])
            if rec and rec.get("layer") == "touch" and o["id"] not in self.ex.cancelling:
                existing[(o["ex"], o["side"])] = o
        if funded < TOUCH_YIELD_RESERVE:                    # yields to the dump catcher: resting bids stay, no new ones
            if now - self.touch_note.get("yield", 0) >= 1800:
                self.touch_note["yield"] = now
                self.event("TOUCH", f"touch yields to the dump catcher: funded reserve {funded:,.0f} < {TOUCH_YIELD_RESERVE:,.0f} (no new bids)")
            return {ex: [dict(side=sd, px=o["price"], qty=int(o["qty"]), sub=f"touch:{ex}:{sd}")] for (ex, sd), o in existing.items()}
        party = dash4.party_exposure(self)
        budget_left = self.touch_budget() - self.touch_cost() - sum(o["qty"] * o["price"] for o in existing.values())
        plan = {}
        for b in self.bots:
            if b.kind != "market" or b.ex in self.manual_ex or self.ex.is_frozen(b.ex):
                continue
            ex, f, px = b.ex, self.fair.get(b.ex), self.px.get(b.ex)
            if f is None or px is None or px[0] is None or px[1] is None:
                continue
            held = self.book.get(ex, {}).get("pos", 0)
            for side in ("yes", "no"):
                fs = f if side == "yes" else 1 - f
                if fs < TOUCH_MIN_FAIR:
                    continue
                best = px[0] if side == "yes" else round(1 - px[1], 3)
                cur = existing.get((ex, side))
                if self.touch_news(ex, side, now):
                    if self.touch_block.get((ex, side), 0) < now:
                        self.event("NEWS", f"{self.name(ex)} {side.upper()}: fair or SIG fell against the touch bid: pulled 30 min")
                    self.touch_block[(ex, side)] = now + TOUCH_NEWS_HOLD_S
                if self.touch_block.get((ex, side), 0) > now or self.breaker.get(("dump", self.pol(ex, side)), 0) > now:
                    continue                                # news guard / the correlated breaker: no bid (a resting one is pulled)
                if best > fs - TOUCH_GAP + 1e-9 or not (MB_MIN_PX <= best <= MB_MAX_PX):
                    continue                                # the bid must sit >= 6c under fair
                if held <= -MIN_QTY if side == "yes" else held >= MIN_QTY:
                    continue                                # never bid against our own other side
                if cur is not None:                         # resting: keep it; requote only when the touch moved
                    if self.race_cost(ex) + cur["qty"] * cur["price"] > TOUCH_RACE_CAP + 1e-6:
                        continue                            # a fill elsewhere on the race took it to the 8k cap: the bid goes
                    plan.setdefault(ex, []).append(dict(side=side, px=best if abs(cur["price"] - best) >= TICK - 1e-9 else cur["price"],
                                                        qty=int(cur["qty"]), sub=f"touch:{ex}:{side}"))
                    continue
                if party.get(self.pol(ex, side), 0.0) >= DUMP_PARTY_CAP:
                    continue
                room = min(TOUCH_RACE_CAP - self.race_cost(ex), budget_left)
                q = min(TOUCH_CLIP, int(room / best)) if room > 0 else 0
                if q < MIN_QTY * 10:
                    continue
                plan.setdefault(ex, []).append(dict(side=side, px=best, qty=q, sub=f"touch:{ex}:{side}"))
                budget_left -= q * best
        return plan

    def track_touch(self, now):
        """Touch inventory -> exits at max(entry + 1.5c, best ask - 1 tick); a sold-out tranche is a round trip; unsold
        shares are simply held (6c+ under fair on a >= 93% race: settlement money)."""
        tr, self.touch_view = self.ledger.tranches, {}
        for ex, books in list(self.ledger.subbooks.items()):
            for sub, q in list(books.items()):
                if not sub.startswith("touch:") or abs(q) < MIN_QTY:
                    continue
                side = sub.split(":")[2]
                if (q > 0) != (side == "yes"):
                    continue
                t = tr.get(sub)
                if t is None or t.get("state") != "open":
                    t = tr[sub] = dict(ex=ex, side=side, kind="touch", t=stamp(), ts=now, state="open")
                since = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(t.get("ts", now) - 300))
                avg = self.ledger.tranche_avg(sub, "touch", ex=ex, side=side, since=since) or t.get("avg")
                if avg is None:
                    continue
                t["avg"] = round(avg, 5)
                px = self.px.get(ex)
                if px is None or px[0] is None or px[1] is None:
                    continue
                best_ask = px[1] if side == "yes" else round(1 - px[0], 3)
                p = bots.tick_up(round(max(round(best_ask - TICK, 3), avg + TOUCH_SELL_MIN), 6))
                self.touch_view.setdefault(ex, []).append(dict(sub=sub, side=side, px=min(p, 0.99), avg=avg, age_min=round((now - t["ts"]) / 60, 1)))
        for sub, t in tr.items():
            if t.get("kind") == "touch" and t.get("state") in ("open", "closed") and not t.get("closed_ts") \
                    and abs(self.ledger.subbooks.get(t["ex"], {}).get(sub, 0)) < MIN_QTY:   # the ledger may close it first
                t.update(state="closed", closed_ts=now, hold_min=round((now - t.get("ts", now)) / 60, 1))
                self.event("TOUCH", f"{self.name(t['ex'])}: touch {sub} sold after {t['hold_min']:.0f} min (round trip done)")

    def touch_fill_events(self, fills):
        for f in fills:
            if f["layer"] not in ("touch", "touchexit"):
                continue
            fs = self.side_fair(f["ex"], f["side"])
            if f["layer"] == "touch":
                msg = (f"TOUCH FILL {self.name(f['ex'])}: bought {f['qty']:,.0f} {f['side'].upper()} at {f['price']:.3f}"
                       + ("" if fs is None else f" (fair {fs:.3f}, {(fs - f['price']) * 100:.1f}c under, edge {(fs - f['price']) * f['qty']:+,.0f})"))
                self.night.line(msg)
            else:
                msg = f"TOUCH sold {f['qty']:,.0f} on {self.name(f['ex'])} at {1 - f['price']:.3f}, realized {f.get('realized') or 0:+,.0f}"
            self.event("TOUCH", msg)

    def touch_stats(self, since):
        buys = [f for f in self.fills_since(since) if f["layer"] == "touch"]
        sells = [f for f in self.fills_since(since) if f["layer"] == "touchexit"]
        gaps = [((f.get("fair") if f.get("fair") is not None else f["price"]) - f["price"]) for f in buys]
        now_gap = [(self.side_fair(f["ex"], f["side"]) or f["price"]) - f["price"] for f in buys]   # mark-out at fair (now vs entry)
        held = self.touch_cost()
        ev = sum(abs(q) * ((self.side_fair(ex, "yes" if q > 0 else "no") or 0) - ((self.ledger.tranches.get(sub) or {}).get("avg") or 0))
                 for ex, books in self.ledger.subbooks.items() for sub, q in books.items() if sub.startswith("touch:") and abs(q) >= MIN_QTY)
        trips = sum(1 for t in self.ledger.tranches.values() if t.get("kind") == "touch" and t.get("state") == "closed" and (t.get("closed_ts") or 0) >= since)
        return dict(fills=len(buys), shares=sum(f["qty"] for f in buys), cost=round(sum(f["qty"] * f["price"] for f in buys)),
                    avg_gap=round(sum(gaps) / len(gaps), 4) if gaps else None, markout_fair=round(sum(now_gap) / len(now_gap), 4) if now_gap else None,
                    round_trips=trips, realized=round(sum(f.get("realized") or 0 for f in sells), 2), held_cost=round(held), ev_settle=round(ev))

    def touch_gate(self, since, now):
        s = self.touch_stats(since)
        trims = self.ex.trims.get("touch", 0) + self.ex.trims.get("touchexit", 0)
        ok = s["fills"] >= TOUCH_GATE_FILLS and (s["markout_fair"] is None or s["markout_fair"] >= 0) and trims == 0
        why = (f"{s['fills']} fills ({s['shares']:,.0f} sh, cost {s['cost']:,}), avg gap to fair {(s['avg_gap'] or 0) * 100:.1f}c, mark-out at fair "
               f"{(s['markout_fair'] or 0) * 100:+.1f}c, {s['round_trips']} round trips, realized {s['realized']:+,.0f}, held {s['held_cost']:,} "
               f"(EV at settlement {s['ev_settle']:+,}), {trims} I2 trims (gate: >= {TOUCH_GATE_FILLS} fills, mark-out >= 0, no trims)")
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

    def dump_gate(self, since, now, min_fills=20):
        """The pilot gate (4 h, >= 20 fills) and, with min_fills, the hourly ratchet gate (day 6 §1.1: >= 10 fills in the day
        window, >= 5 at night; fewer is a hold, decided by the caller)."""
        s = self.dump_stats(since)
        trims = self.ex.trims.get("dump", 0)
        short = self.cash_short is not None and self.cash_short["t"] >= since
        ok = (s["fills"] >= min_fills and s["median_hold_min"] is not None and s["median_hold_min"] <= 60 and s["realized"] > 0
              and (s["markout30"] is None or s["markout30"] >= 0) and trims == 0 and not short)
        why = (f"{s['fills']} fills, {s['round_trips']} round trips, median hold {s['median_hold_min']} min, realized {s['realized']:+,.0f}, "
               f"mark-out 30m {s['markout30']}, {trims} I2 trims, cash short {short} (gate: >= {min_fills} fills, hold <= 60, realized > 0, "
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
        if FMM.get("on") and self.fmm_mode(now) in ("paper", "live", "stopped"):
            for m in msgs:
                self.fmm_funds(f"write refused for funds: {m[:160]}", raw)
            if raw < 0:
                self.fmm_funds("SIG balance below 0", raw)
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
        fb = self.feed_book(ex)                             # C88: the feed's book first
        if fb is not None:
            c = self.depth_cache[ex] = (now, fb[0][:1], fb[1][:1])
        elif not c or now - c[0] > 60:
            if not api.spare_reads():                       # Oct 3 (b): no spare read -> size from the prices read: <= 500
                return float(RELEASE_FALLBACK_QTY)          # shares, still a limit at the bid
            bids, asks = api.orderbook(ex, depth=1)
            c = self.depth_cache[ex] = (now, bids, asks)
        _, bids, asks = c
        if side == "yes":
            return bids[0][1] if bids else 0.0
        return asks[0][1] if asks else 0.0

    @staticmethod
    def day_window(now):
        """Day 6: the day window (06:00-15:00 PT) has 10x the dumps of the night."""
        h = time.localtime(now).tm_hour
        return DAY_WINDOW[0] <= h < DAY_WINDOW[1]

    def cash_constrained(self, cash, now):
        """Day 6 §3.3: constrained = free cash above the floor < 10k AND dump fills >= 3k at cost in the last 30 min. Tracks
        the minutes constrained (the hourly line)."""
        free = cash - CASH_RESERVE
        recent = sum(f["qty"] * f["price"] for f in self.fills_since(now - 1800) if f["layer"] == "dump" and f["action"] == "buy")
        on = (free < CONSTRAINED_FREE and recent >= CONSTRAINED_FILLS_30M and free >= ENTRY_MIN_FREE) or free < ENTRY_MIN_FREE   # day 8 §1.3; C84c (Piero 10:10 a): a starved engine (free < 2k, no entries) counts as constrained
        if on and not self.constrained:
            self.constrained_t = now
            self.event("CASH", f"cash-constrained: free {free:,.0f} < {CONSTRAINED_FREE:,} and dump fills {recent:,.0f} in 30 min (funding branch on)")
        elif not on and self.constrained:
            self.event("CASH", f"cash-constrained over after {(now - (self.constrained_t or now)) / 60:.0f} min (free {free:,.0f})")
        if on and (not self.constrained_min or now - self.constrained_min[-1] >= 60):
            self.constrained_min.append(now)
        self.constrained = on
        return on

    def constrained_minutes(self, since):
        return sum(1 for t in self.constrained_min if t >= since)

    def dump_demand(self, cash, used):
        """Day 4 §3 + day 6 §3.3: is the dump catcher cash-constrained now (free < 10k and >= 3k of dump fills in 30 min)
        and its measured return per SIG per day over the last 24 h (realized + edge vs fair at fill, per SIG of catches).
        The funding branch may free at most 10k an hour."""
        now = time.time()
        funded = max(0.0, min(float(OPPORTUNITY_RESERVE), cash - CASH_RESERVE - used.get("committed", 0.0)))
        need = sum(w["px"] * w["qty"] for ws in self.dump_now.values() for w in ws)
        s = self.dump_stats(now - 86400)
        ret = (s["realized"] + s["edge_fair"]) / max(s["cost"], 1.0) if s["fills"] else 0.0
        on = self.cash_constrained(cash, now)
        self.release_fund_log = [(t, v) for t, v in self.release_fund_log if t >= now - 3600]
        hour_left = max(0.0, RELEASE_FUND_PER_H - sum(v for _, v in self.release_fund_log))
        return dict(constrained=on, need=round(need), funded=round(funded), return_per_day=ret,
                    shortfall=min(hour_left, max(0.0, CONSTRAINED_FREE - (cash - CASH_RESERVE))) if on else 0.0)

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
        T = self.cash_target(now)
        free = cash - CASH_RESERVE
        pending = sum(o["qty"] * (1 - o["price"]) for o in self.open
                      if (self.ledger.orders.get(o["id"]) or {}).get("layer") == "release")
        if free + pending < REBUILD_FREE and not RULES_V2:   # Oct 5 22:50 (Piero): rebuild free cash to 15k first — gone with day 11 rules v2: nothing is sold to raise cash, k falls instead
            return self.rebuild_plan(cash, now, free, pending)
        self.release_fund_log = [(t_, v) for t_, v in self.release_fund_log if t_ >= now - 3600]
        budget = max(0.0, min(T - free - pending, RELEASE_FUND_PER_H - sum(v for _, v in self.release_fund_log)))   # tiers 1-2: up to T, <= 10k/h
        tier2_ok = bool(self.controls.d.get("release_tier2")) and self.trading_return_24h(now) >= RELEASE_TIER2_RETURN   # day 8 §1.4: tier 2 needs Piero (release_tier2 flag)
        cands = []
        for ex, b in self.book.items():
            q = b["pos"]
            if not q or ex not in self.owner or self.owner[ex].kind != "market" or self.ex.is_frozen(ex):
                continue
            side = "yes" if q > 0 else "no"
            bid = bots.side_book(*self.px.get(ex, (None, None, None))[:2], side)[0]
            fs = self.side_fair(ex, side)
            if bid is None or fs is None:
                continue
            ctf = fs - bid                                    # the gap to fair at the SIG bid, in price (negative: the bid is above fair)
            if ctf > RELEASE_TIER2 + 1e-9:
                continue                                    # never > 7c under fair for funding (settlement money)
            tier = 0 if ctf <= RELEASE_TIER0 + 1e-9 else 1 if ctf <= RELEASE_TIER1 + 1e-9 else 2
            if tier == 2 and not tier2_ok:
                continue
            if tier > 0 and (budget <= 0 or now - self.first_fill_t(ex) < RELEASE_MIN_AGE_S):
                continue                                    # tiers 1-2 fund T only, from holdings >= 24 h old
            subs = self.ledger.subbooks.get(ex, {})
            avail = 0
            for k, v in subs.items():                       # never touch-owned, manual, tranches or pair sets; rule 0 per lot
                if k in self.RELEASE_SUBS and v * q > 0 and (str(ex), k) not in self.core_set:   # C88: never a core lot
                    avail += self.rule0_allowed(ex, side, k, bid, int(abs(v)))
            if avail >= MIN_QTY:
                cands.append((tier, self.return_per_day(max(ctf, 0.0)), ctf, ex, side, avail, bid))
        plan = {}
        for tier, rpd, ctf, ex, side, avail, bid in sorted(cands):   # tier 0 first, then the lowest carry first
            if len(plan) >= RELEASE_MARKETS:
                break
            depth = self.bid_depth(ex, side, now)
            qty = int(min(avail, depth, avail if tier == 0 else budget / bid))
            if qty < MIN_QTY:
                continue
            subs, left, split = self.ledger.subbooks.get(ex, {}), qty, {}
            for k in self.RELEASE_SUBS:
                v = subs.get(k, 0)
                if left > 0 and v * (1 if side == "yes" else -1) > 0:
                    take = int(min(self.rule0_allowed(ex, side, k, bid, int(abs(v))), left))
                    if take >= MIN_QTY:
                        split[k] = take
                        left -= take
            if split:
                why = f"tier{tier}"
                plan[ex] = dict(side=side, px=bid, cost=round(ctf, 4), subs=split, why=why, tier=tier,
                                ev=round(max(ctf, 0.0) * sum(split.values()), 2))
                self.release_why[ex] = (now, why)
                if tier > 0:
                    budget -= sum(split.values()) * bid
                    self.release_fund_log.append((now, sum(split.values()) * bid))
        if plan:
            self.event("RELEASE", "; ".join(f"{self.name(ex)} {r['side'].upper()} {sum(r['subs'].values()):,} @ {r['px']:.3f} "
                                             f"({r['why']}, {r['cost'] * 100:.1f}c under fair, EV given up {r['ev']:+,.0f})" for ex, r in plan.items()))
        return plan

    # ------------------------------------------------------------------------------------------------ capital policy v2 (day 6 §9)
    def lots(self, ex, side, sub):
        """FIFO lots held on (ex, side) in a sub-book: [(qty left, entry price, origin)], origin 'day1' (bought Oct 1 PT or
        before the engine) or 'engine'. Sells on the same sub consume the oldest lots first. Fills whose order record is
        gone (pruned) count as 'inventory' lots."""
        key = (len(self.ledger.fills), id(self.ledger.fills[-1]) if self.ledger.fills else 0, len(getattr(self.ledger, "moves", [])))
        if self.lots_cache[0] != key:
            book = {}
            for f in sorted(self.ledger.fills, key=lambda f: f["t"]):
                rec = self.ledger.orders.get(str(f.get("order") or ""))
                fsub = (rec or {}).get("sub") or ("manual" if f.get("bot") in self.position_names else "inventory")
                fside = f["side"] if f["action"] == "buy" else ("no" if f["side"] == "yes" else "yes")   # a sell of YES = a NO buy
                buys = (f["action"] == "buy") == (f["side"] == fside)
                lst = book.setdefault((f["ex"], fside, fsub), [])
                opp = book.setdefault((f["ex"], "no" if fside == "yes" else "yes", fsub), [])
                if buys:
                    origin = "day1" if (f.get("before_engine") or f["t"] < DAY1_CUTOFF) else "engine"
                    left = f["qty"]
                    for lot in opp:                          # a buy of the other side nets the oldest lots we hold first
                        take = min(lot[0], left)
                        lot[0] -= take
                        left -= take
                        if left <= 0:
                            break
                    if left >= 1:
                        lst.append([left, f["price"], origin])
                book[(f["ex"], fside, fsub)] = [l for l in lst if l[0] >= 1]
                book[(f["ex"], "no" if fside == "yes" else "yes", fsub)] = [l for l in opp if l[0] >= 1]
            for ex_, src, dst, q, _t in getattr(self.ledger, "moves", []):   # a sub-book move carries its lots (FIFO) with it
                side_ = "yes" if q > 0 else "no"
                left = abs(q)
                src_lots = book.get((ex_, side_, src), [])
                dst_lots = book.setdefault((ex_, side_, dst), [])
                for lot in src_lots:
                    take = min(lot[0], left)
                    if take >= 1:
                        dst_lots.append([take, lot[1], lot[2]])
                        lot[0] -= take
                        left -= take
                    if left <= 0:
                        break
                book[(ex_, side_, src)] = [l for l in src_lots if l[0] >= 1]
            self.lots_cache = (key, book)
        return [tuple(l) for l in self.lots_cache[1].get((ex, side, sub), [])]

    def rule0_allowed(self, ex, side, sub, sale_px, qty, margin=RULE0_MARGIN):
        """How many of `qty` shares of (ex, side) in `sub` may be sold at `sale_px` under rule 0: every day-1 lot, and an
        engine lot only at >= its entry + 0.5c. Shares beyond the lots we can account for are engine lots at SIG's
        average cost (the safe side)."""
        if sale_px is None:
            return 0
        lots = self.lots(ex, side, sub)
        known = sum(l[0] for l in lots)
        ok = sum(l[0] for l in lots if l[2] == "day1" or l[1] + margin <= sale_px + 1e-9)
        if qty > known:                                     # shares the lot history can't place: the tranche's own entry (the
            t = self.ledger.tranches.get(sub) or {}          # engine's record, e.g. a hand-over sub), else SIG's average cost —
            avg = float(t.get("avg") or 0) if sub.startswith(("dump:", "touch:", "scalp:", "wall:", "tm:")) and t.get("avg") else \
                float(((self.book.get(ex) or {}).get("row") or {}).get("avgCost") or 0)
            top = max((l[1] for l in lots if l[2] == "engine"), default=0.0)   # C80 (§9.4 b): never below the highest known engine entry of the sub
            avg = max(avg, top)
            if avg and avg + margin <= sale_px + 1e-9:
                ok += qty - known
        return int(max(0, min(qty, ok)))

    def rule0(self, b, wants, s):
        """Piero's rule (day 6 §9.0): the engine never sells its own trades at a loss. Every reducing want of a market bot
        (release, recycler, exits, unwinds) is clipped to the shares rule 0 allows at its sale price; position bots (the
        manual AK / RI book) are not the engine's trades."""
        if b.kind != "market":
            return wants
        out = []
        for w in wants:
            reducing = w["layer"] in self.REDUCING or w.get("reduce")
            if not reducing or w["action"] not in ("buy", "sell"):
                out.append(w)
                continue
            held_side = ("no" if w["side"] == "yes" else "yes") if w["action"] == "buy" else w["side"]
            sale_px = round(1 - w["px"], 3) if w["action"] == "buy" else w["px"]
            sub = w.get("sub") or "inventory"
            margin = w.get("rule0_margin", RULE0_MARGIN)
            pair = None
            if str(sub).startswith("pairset:"):             # Piero 15:35 (item 2): a pair-set leg is judged with the pair's net lock
                pair = self.pair_lock(str(sub)[8:])
                if pair is not None:
                    margin = margin - pair["lock"]          # a +2.6c lock lets the leg go 2.6c under its own entry
            allowed = self.rule0_allowed(b.ex, held_side, sub, sale_px, int(w["qty"]), margin=margin)
            if allowed < w["qty"]:
                blocked = int(w["qty"]) - allowed
                self.rule0_blocked.append((time.time(), b.ex, held_side, blocked, sale_px))
                k = (b.ex, held_side, w["layer"])
                if time.time() - self.rule0_log.get(k, 0) > 1800:
                    self.rule0_log[k] = time.time()
                    lots = self.lots(b.ex, held_side, sub)
                    ent = [l[1] for l in lots if l[2] == "engine" and l[1] + RULE0_MARGIN > sale_px + 1e-9]
                    t_avg = (self.ledger.tranches.get(sub) or {}).get("avg")
                    if pair is not None:
                        self.event("RULE0", f"{self.name(b.ex)} {held_side.upper()}: {w['layer']} sale of {blocked:,} at {sale_px:.3f} held back on pair {sub[8:]}: "
                                            f"legs {pair['legs']}, pair net {pair['lock'] * 100:+.2f}c per share — judged as a whole")
                    self.event("RULE0", f"{self.name(b.ex)} {held_side.upper()}: {w['layer']} sale of {blocked:,} at {sale_px:.3f} held back: engine lots at "
                                        f"{min(ent):.3f}-{max(ent):.3f}" if ent else f"{self.name(b.ex)} {held_side.upper()}: {w['layer']} sale of {blocked:,} at {sale_px:.3f} held back "
                                        f"(unknown lots at {'tranche entry ' + format(t_avg, '.3f') if t_avg else 'SIG average cost'})")
                if allowed < MIN_QTY:
                    continue
                w = dict(w, qty=allowed, qmax=min(w.get("qmax") or allowed, allowed))
            out.append(w)
        return out

    def pair_lock(self, set_id):
        """{lock: 1 - (yes avg + no avg) per share, legs: text} for a pair-set basket, or None."""
        for bk in self.ledger.baskets:
            if str(bk.get("id")) != str(set_id):
                continue
            legs = self.ledger.basket_fills(bk)
            if len(legs) < 2 or any(l[4] is None for l in legs):
                return None
            lock = 1.0 - sum(float(l[4]) for l in legs)
            return dict(lock=lock, legs=", ".join(f"{self.name(l[1])} {str(l[2]).upper()} {l[3]:,.0f} @ {float(l[4]):.3f}" for l in legs))
        return None

    def rule0_blocked_value(self, since):
        best = {}
        for t, ex, side, q, p in self.rule0_blocked:
            if t >= since:
                best[(ex, side)] = max(best.get((ex, side), 0.0), q * p)
        return round(sum(best.values()))

    def trading_return_24h(self, now):
        """The trading layers' (dump + touch + scalper) realized return per day on their average capital at cost over 24 h."""
        realized = sum(f.get("realized") or 0 for f in self.fills_since(now - 86400) if f["layer"] in ("dumpexit", "touchexit", "scalpexit"))
        inv = [v for t, v in self.in_use_hist if t >= now - 86400]
        avg = sum(inv) / len(inv) if inv else 0.0
        return realized / avg if avg >= 1000 else 0.0

    def cash_target(self, now):
        """§9.2 + day 8 §1: T starts at 25k; +10k after an hour in which the trading layers were cash-constrained >= 15 min
        or peaked at >= 70% of free cash (max 35k); -10k after 2 idle hours (bounded peak use < 60% or no constrained
        minute; min 25k). Logged."""
        c = self.controls
        ct = c.d.get("cash_target") or {}
        T = min(float(CASH_T_MAX), float(ct.get("T") or CASH_T_START))
        hour = int(now // 3600)
        if ct.get("hour") is None:
            with c.lock:
                c.d["cash_target"] = dict(T=T, hour=hour, idle=0, hist=[])
                c.save()
            return T
        if hour > ct["hour"]:
            minutes = self.constrained_minutes(now - 3600)
            peak, self.use_peak = self.use_peak, 0.0
            idle = int(ct.get("idle") or 0)
            msg = None
            if minutes >= CASH_T_CONSTRAINED_MIN or peak >= CASH_T_PEAK_UP:
                T2 = min(float(CASH_T_MAX), T + CASH_T_STEP)
                idle = 0
                if T2 != T:                                  # at the cap nothing changes: no line
                    msg = f"CASH TARGET {T:,.0f} -> {T2:,.0f}: last hour constrained {minutes} min, peak use {peak:.0%}"
                T = T2
            elif peak < CASH_T_PEAK_DOWN or minutes == 0:   # day 8 §1.2: an idle hour = bounded peak < 60% or no constrained minute
                idle += 1
                if idle >= CASH_T_IDLE_H:
                    T2 = max(float(CASH_T_MIN), T - CASH_T_STEP)
                    if T2 != T:
                        msg = f"CASH TARGET {T:,.0f} -> {T2:,.0f}: {idle} idle hours (peak use < {CASH_T_PEAK_DOWN:.0%} or no constrained minute)"
                    T, idle = T2, 0
            else:
                idle = 0
            with c.lock:
                ct = dict(T=T, hour=hour, idle=idle, hist=(ct.get("hist") or [])[-48:] + [dict(t=stamp(), T=T, minutes=minutes, peak=round(peak, 3))])
                c.d["cash_target"] = ct
                c.save()
            if msg:
                self.event("CASH", msg)
                self.night.line(msg)
        return T

    REBUILD_SKIP = ("manual", "pairset:", "recycle:", "sleeve:", "wall:", "swing:", "fm:")   # day 9/10: sleeve, wall, swing and FM lots have their own exits

    def rebuild_plan(self, cash, now, free, pending):
        """Oct 5 22:50 (Piero): while free cash above the floor is under 15k, sell engine lots at the SIG bid: lots the bid
        clears at entry + 0.5c (rule 0), and — Piero's exception — lots <= 5c under fair down to entry - 1c; lowest gap
        to fair first, <= 7c under fair first, then 7-10c only if still short. Every sub-book but manual, pair sets and
        recycle lots; no hourly cap, no age rule. Each sale is logged with its gap, origin and EV given up."""
        need = REBUILD_FREE - free - pending
        cands = []
        for ex, b in self.book.items():
            q = b["pos"]
            if not q or ex not in self.owner or self.owner[ex].kind != "market" or self.ex.is_frozen(ex):
                continue
            side = "yes" if q > 0 else "no"
            bid = bots.side_book(*self.px.get(ex, (None, None, None))[:2], side)[0]
            fs = self.side_fair(ex, side)
            if bid is None or fs is None:
                continue
            gap = fs - bid
            if gap >= REBUILD_MAX_GAP - 1e-9:
                continue                                    # day 10 §9.4: never sell a lot >= 8c under fair; the catcher shrinks instead
            margin = REBUILD_MARGIN if (gap <= REBUILD_MARGIN_GAP + 1e-9 and free < REBUILD_EXC_FREE) else RULE0_MARGIN   # day 8 §1.5: the -1c exception only under 10k free
            split = {}
            for sub, v in self.ledger.subbooks.get(ex, {}).items():
                if v * q <= 0 or sub.startswith(self.REBUILD_SKIP):
                    continue
                take = self.rule0_allowed(ex, side, sub, bid, int(abs(v)), margin=margin)
                if take >= MIN_QTY:
                    split[sub] = take
            if split:
                cands.append((2 if gap > REBUILD_GAP2 + 1e-9 else 0, self.return_per_day(max(gap, 0.0)), ex, side, bid, split, margin))   # day 10 §4.3: lowest return per day first; >= 10c under fair last
        plan = {}
        for band, rpd, ex, side, bid, split, margin in sorted(cands):
            gap = self.side_fair(ex, side) - bid
            if need <= 0 or len(plan) >= RELEASE_MARKETS:
                break
            depth = self.bid_depth(ex, side, now)
            qty = int(min(sum(split.values()), depth, need / bid))
            if qty < MIN_QTY:
                continue
            left, take = qty, {}
            for sub, v in sorted(split.items(), key=lambda kv: -kv[1]):
                t_ = int(min(v, left))
                if t_ >= MIN_QTY:
                    take[sub] = t_
                    left -= t_
            if not take:
                continue
            ev = round(max(gap, 0.0) * sum(take.values()), 2)
            plan[ex] = dict(side=side, px=bid, cost=round(gap, 4), subs=take, why="rebuild", tier=0, ev=ev, rule0_margin=margin)
            self.release_why[ex] = (now, "rebuild")
            need -= sum(take.values()) * bid
            lots = self.lots(ex, side, next(iter(take)))
            origin = "day1" if lots and lots[0][2] == "day1" else "engine"
            msg = (f"REBUILD sale {self.name(ex)} {side.upper()} {sum(take.values()):,} at the bid {bid:.3f}: {gap * 100:.1f}c under fair, "
                   f"{origin} lots, EV given up {ev:+,.0f}" + (" (rule-0 exception: <= 5c under fair, down to entry - 1c)" if margin < 0 else "")
                   + f"; free cash {free:,.0f} -> target {REBUILD_FREE:,}")
            self.event("RELEASE", msg)
            self.night.line(msg)
        return plan

    def first_fill_t(self, ex):
        """When our current holding on ex was opened (the fill that took the net position away from flat), else 0."""
        net, first = 0.0, 0.0
        for f in self.ledger.fills:
            if f["ex"] != ex:
                continue
            q = f["qty"] * (1 if (f["side"] == "yes") == (f["action"] == "buy") else -1)
            was, net = net, net + q
            if abs(was) < 1 <= abs(net) or was * net < 0:
                first = time.mktime(time.strptime(f["t"], "%Y-%m-%d %H:%M:%S"))
        return first

    def return_per_day(self, ctf):
        """Afternoon §2.2: expected return to settlement per SIG held, per day: (fair - bid)/bid over the days left."""
        days = max(1.0, (time.mktime(time.strptime(SETTLE_DATE, "%Y-%m-%d")) - time.time()) / 86400)
        return ctf / days

    def ev_given_up(self, since_t):
        """Day 8 §1.5: the expected value given up by release / rebuild sales since `since_t` (held fair - sale price, per
        share, only where the fill carries a fair)."""
        start = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(since_t))
        ev = 0.0
        for f in self.ledger.fills:
            if f["layer"] == "release" and f["action"] == "buy" and f["t"] >= start and f.get("fair") is not None:
                ev += max(0.0, (1 - f["fair"]) - (1 - f["price"])) * f["qty"]
        return round(ev, 2)

    # ------------------------------------------------------------------------------------------------ day 10 §3: the RI swing
    def swing_cfg(self):
        return self.controls.d.get("swing") or {}

    def swing_cost(self):
        cfg = self.swing_cfg()
        if not cfg:
            return 0.0
        ex, side = str(cfg["ex"]), cfg["side"]
        q = abs(self.ledger.subbooks.get(ex, {}).get(f"swing:{ex}:{side}", 0.0))
        avg = (self.ledger.tranches.get(f"swing:{ex}:{side}") or {}).get("avg") or cfg["buy_px"]
        return q * float(avg)

    def swing_state(self):
        """(slice_held, sold_out, per-sub holdings of the slice) for the configured slice."""
        cfg = self.swing_cfg()
        ex, side = str(cfg["ex"]), cfg["side"]
        books = self.ledger.subbooks.get(ex, {})
        wall_total = sum(abs(books.get(s, 0.0)) for s in cfg["wall_subs"])
        swing_q = abs(books.get(f"swing:{ex}:{side}", 0.0))
        slice_in_walls = max(0.0, wall_total - float(cfg.get("hold_qty", 0.0)))
        slice_held = min(float(cfg["slice"]), slice_in_walls + swing_q)
        sold_out = max(0.0, float(cfg["slice"]) - slice_held)
        return slice_held, sold_out, slice_in_walls, swing_q

    def swing_slice_alloc(self):
        """{wall sub: shares of the slice that sit in it} (largest sub first, like the sell leg) — the rest of each sub is the hold part."""
        cfg = self.swing_cfg()
        if not cfg:
            return {}
        ex = str(cfg["ex"])
        books = self.ledger.subbooks.get(ex, {})
        _, _, slice_in_walls, _ = self.swing_state()
        out, left = {}, slice_in_walls
        for sub in sorted(cfg["wall_subs"], key=lambda s: -abs(books.get(s, 0.0))):
            take = min(abs(books.get(sub, 0.0)), left)
            out[sub] = take
            left -= take
        return out

    def swing_plan(self, now):
        """{ex: {sells: [...], buy: {...}}}: the sell leg rests a NO ask at sell_px for the part of the slice we hold (swing lots
        first, then the wall lots; never below entry + 0.5c: rule 0); the buy leg rests a NO bid at buy_px for what was sold,
        only while >= min_edge under fair and never more than was sold (race-cap neutral). Both legs are pulled while fair YES
        > max_fair_yes or after a >= 2c Polymarket rise in 10 min (30 min)."""
        c = self.controls
        if self.reservoirs():
            if not c.on("swing") or self.fills_blind:
                return {}
            out = self.reservoirs_plan(now)                 # C84g: the reservoirs replace the swing legs and the band
            if c.paused:                                    # C87: a pause stops new risk only; the reservoir's asks (reductions) keep resting
                out = {ex: dict(p, buys=[], buy=None) for ex, p in out.items() if p.get("sells")}
            return out
        cfg = self.swing_cfg()
        if not cfg or not c.on("swing") or c.paused or self.fills_blind:
            return {}
        ex, side = str(cfg["ex"]), cfg["side"]
        if ex not in self.owner or self.ex.is_frozen(ex):
            return {}
        f = self.fair.get(ex)
        if f is None:
            return {}
        fair_yes = f if side == "no" else 1 - f
        h = [x for x in self.fair_hist.get(ex, []) if x[0] >= now - cfg.get("news_window_s", 600)]
        if not h or now - h[-1][0] >= 5:
            h.append((now, f))
            self.fair_hist[ex] = h
        low = min((v if side == "no" else 1 - v) for _, v in h)
        if fair_yes - low >= cfg.get("news_c", 0.02) - 1e-9 and self.swing_block < now:
            self.swing_block = now + cfg.get("news_hold_s", 1800)
            self.event("SWING", f"SWING {self.name(ex)}: Polymarket YES rose {(fair_yes - low) * 100:.1f}c in 10 min: both legs pulled 30 min")
        if fair_yes > cfg["max_fair_yes"] + 1e-9:
            if now - self.swing_note.get("fair", 0) >= 1800:
                self.swing_note["fair"] = now
                self.event("SWING", f"SWING {self.name(ex)}: fair YES {fair_yes:.3f} > {cfg['max_fair_yes']}: legs pulled")
            return {}
        if self.swing_block > now:
            return {}
        if cfg.get("band"):
            return self.swing_band_plan(now, cfg, f)
        slice_held, sold_out, slice_in_walls, swing_q = self.swing_state()
        books = self.ledger.subbooks.get(ex, {})
        sells, left = [], slice_held
        wall_left = slice_in_walls                          # the wall lots beyond the slice (hold_qty) are never sold
        order = [f"swing:{ex}:{side}"] + sorted(cfg["wall_subs"], key=lambda s: -abs(books.get(s, 0.0)))
        for sub in order:
            avail = abs(books.get(sub, 0.0)) if sub.startswith("swing:") else min(abs(books.get(sub, 0.0)), wall_left)
            take = int(min(avail, left))
            if take < MIN_QTY:
                continue
            if not sub.startswith("swing:"):
                wall_left -= take
            avg = (self.ledger.tranches.get(sub) or {}).get("avg")
            px = max(float(cfg["sell_px"]), (float(avg) + RULE0_MARGIN) if avg else 0.0)
            sells.append(dict(side=side, px=round(px, 3), qty=take, sub=sub, ttl=SWING_TTL_S))
            left -= take
        plan = dict(sells=sells)
        fs = self.side_fair(ex, side)
        if sold_out >= MIN_QTY and fs is not None and fs - float(cfg["buy_px"]) >= float(cfg["min_edge"]) - 1e-9:
            plan["buy"] = dict(side=side, px=float(cfg["buy_px"]), qty=int(sold_out), sub=f"swing:{ex}:{side}", ttl=SWING_TTL_S)
        return {ex: plan}

    def reservoirs(self):
        return self.controls.d.get("reservoirs") or {}

    def reservoir_subs(self, ex):
        r = self.reservoirs().get(str(ex))
        return ({sub for sub, _ in r["offers"]} | set(r.get("hold_subs") or [])) if r else set()   # held subs: no wall buy-back either

    def reservoir_avail(self, r):
        """{sub: shares offerable} — the core_sub keeps core_qty back (the highest-cost lots stay; rule 0 picks the cheap ones)."""
        books = self.ledger.subbooks.get(str(r["ex"]), {})
        out = {}
        for sub, _ in r["offers"]:
            q = abs(books.get(sub, 0.0)) if (books.get(sub, 0.0) < 0) == (r["side"] == "no") else 0.0
            if sub == r["core_sub"]:
                q = max(0.0, q - float(r.get("core_qty") or 0))
            out[sub] = q
        return out

    def reservoir_total(self, r):
        books = self.ledger.subbooks.get(str(r["ex"]), {})
        subs = {sub for sub, _ in r["offers"]} | {r["core_sub"]} | set(r.get("hold_subs") or [])
        return sum(abs(books.get(sub, 0.0)) for sub in subs)

    def reservoir_p10(self, now, r):
        """The 24-h p10 of the market's NO (held-side) print prices from our own tape, share-weighted, refreshed every 10 min."""
        ex = str(r["ex"])
        if now - self.res_p10_t.get(ex, 0.0) < 600:
            return self.res_p10.get(ex)
        cut, vals = now - 86400, []
        for day in (cut, now):
            path = os.path.join(DATA, "intel", "trades", time.strftime("%Y-%m-%d", time.localtime(day)) + ".jsonl")
            if not os.path.exists(path):
                continue
            with open(path) as fh:
                for line in fh:
                    try:
                        x = json.loads(line)
                    except ValueError:
                        continue
                    if str(x.get("ex")) == ex and (x.get("ts") or 0) >= cut and x.get("p") is not None:
                        ps = round(1 - float(x["p"]), 3) if r["side"] == "no" else round(float(x["p"]), 3)
                        vals += [ps] * max(1, int(float(x.get("q") or 1) // 1000))
        vals.sort()
        self.res_p10[ex] = vals[int(0.10 * (len(vals) - 1))] if vals else None
        self.res_p10_t[ex] = now
        return self.res_p10[ex]

    def reservoir_save(self, r):
        c = self.controls
        with c.lock:
            c.d.setdefault("reservoirs", {})[str(r["ex"])] = r
            c.save()

    def reservoir_check_triggers(self, now, r, f):
        """Rebuild triggers: 24-h p10 of NO prints >= trig_p10, PM fair moved >= 2c from the deploy, or Oct 30 00:00 PT."""
        if r.get("mode") == "rebuild" or r.get("no_rebuild"):
            return
        if r.get("fair0") is None and f is not None:        # the deploy ran before the first fair was in — pin the reference now
            r["fair0"] = f
            self.reservoir_save(r)
            self.event("SWING", f"RESERVOIR {self.name(r['ex'])}: fair reference pinned at {f:.4f}; the 2c-move trigger is live from here")
        why = None
        p10 = self.reservoir_p10(now, r)
        if p10 is not None and p10 >= r["trig_p10"] - 1e-9:
            why = f"24-h p10 of NO prints {p10:.3f} >= {r['trig_p10']}"
        elif f is not None and r.get("fair0") is not None and abs(f - float(r["fair0"])) >= r["trig_fair_move"] - 1e-9:
            why = f"PM fair moved {abs(f - float(r['fair0'])) * 100:.1f}c from the deploy ({float(r['fair0']):.3f} -> {f:.3f})"
        elif now >= time.mktime(time.strptime(r["trig_date"], "%Y-%m-%d %H:%M:%S")):
            why = f"{r['trig_date']} reached"
        if why:
            r["mode"] = "rebuild"; r["rebuild_t"] = stamp(); r["rebuild_why"] = why
            self.reservoir_save(r)
            msg = f"RESERVOIR {self.name(r['ex'])} -> REBUILD: {why}; selling stops, rebuying up to {r['full_qty']:,} at <= {r['rebuild_px']}"
            self.event("SWING", msg); self.night.line(msg); self.alerts.append(f"{time.strftime('%H:%M')} {msg}")

    def reservoir_resting(self):
        """The reservoirs' resting rebuy bids at cost — reserved first among entries inside the ring windows only (Piero 16:10)."""
        rs = self.reservoirs()
        if not rs or not self.ring_window(time.time()):
            return 0.0
        return sum(o["qty"] * o["price"] for o in self.open if o["ex"] in rs and o["action"] == "buy" and o["side"] == rs[o["ex"]]["side"]
                   and (self.ledger.orders.get(o["id"]) or {}).get("layer") == "swing" and o["id"] not in self.ex.cancelling)

    def reservoir_out(self, r):
        st = r.get("stats") or {}
        return max(0.0, float(st.get("sold_sh") or 0) - float(st.get("bought_sh") or 0))

    def run_sell_now(self, now):
        """Piero 17:15 Oct 8: run a 'sell_now' instruction — cancel the market's resting reservoir asks, send the clips (limit buy of the
        complement at 1 - px: never worse than px), cancel whatever did not fill once its TTL passed (not chased, not left resting), then log
        the fills with SIG's position average before / after and the free cash right after. Piero's order: the per-lot rule 0 is his call
        here (the sale is at the position average)."""
        c = self.controls
        sn = c.d.get("sell_now") or {}
        if sn.get("state") not in ("pending", "sent"):
            return
        ex, side = str(sn["ex"]), sn["side"]
        close = "yes" if side == "no" else "no"
        buy_px = round(1 - float(sn["px"]), 3)
        if sn["state"] == "pending":
            r = self.reservoirs().get(ex) or {}
            subs = sn.get("subs") or [sub for sub, _ in (r.get("offers") or [])] or ["inventory"]
            books = self.ledger.subbooks.get(ex, {})
            for o in list(self.open):                       # the resting reservoir asks go first (no oversell of the same lots)
                rec = self.ledger.orders.get(o["id"]) or {}
                if o["ex"] == ex and rec.get("layer") == "swingsell" and o["id"] not in self.ex.cancelling:
                    self.ex.cancel(o, reason="sell_now: resting asks pulled first", front=True)
            self.ex.step_cancels()
            left, items = int(sn["qty"]), []
            for sub in subs:
                q = int(abs(books.get(sub, 0.0))) if (books.get(sub, 0.0) < 0) == (side == "no") else 0
                while q >= MIN_QTY and left >= MIN_QTY:
                    take = int(min(q, left, sn["clip"]))
                    items.append((self.owner[ex].name, sub, "swingsell", (ex, close, "buy", buy_px, take)))
                    q -= take; left -= take
            row = (self.book.get(ex) or {}).get("row") or {}
            sn.update(state="sent", sent_t=now, avg_before=row.get("avgCost"), held_before=(self.book.get(ex) or {}).get("pos"),
                      free_before=self.free_cash(), sent_qty=sum(it[3][4] for it in items))
            with c.lock:
                c.d["sell_now"] = sn; c.save()
            if items:
                self.ex.place(items, ttl_s=max(5.0, float(sn["ttl_s"])))
                self.ex.step_placements()
            msg = (f"SELL NOW {self.name(ex)}: {sn['sent_qty']:,} {side.upper()} at {float(sn['px']):.3f} sent in {len(items)} clips (limit buy {close.upper()} at {buy_px:.3f}, "
                   f"fill-or-cancel after {sn['ttl_s']:.0f} s); position {sn['held_before']} at average {sn['avg_before']}, free {sn['free_before']:,.0f}")
            self.event("SWING", msg); self.night.line(msg)
            return
        if now - float(sn.get("sent_t") or now) < float(sn["ttl_s"]) + 5:
            return
        mine = [o for o in self.open if o["ex"] == ex and o["action"] == "buy" and o["side"] == close and abs(o["price"] - buy_px) < 1e-9
                and (self.ledger.orders.get(o["id"]) or {}).get("layer") == "swingsell" and float((self.ledger.orders.get(o["id"]) or {}).get("ts") or 0) >= float(sn["sent_t"]) - 2]
        for o in mine:
            if o["id"] not in self.ex.cancelling:
                self.ex.cancel(o, reason="sell_now: the unfilled rest is cancelled, not chased", front=True)
        self.ex.step_cancels()
        t0 = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(float(sn["sent_t"]) - 2))
        fills = [f for f in self.ledger.fills if f["ex"] == ex and f["layer"] == "swingsell" and f["t"] >= t0 and f["side"] == close and abs(f["price"] - buy_px) < 0.0051]
        q = sum(f["qty"] for f in fills)
        avg_px = (sum((1 - f["price"]) * f["qty"] for f in fills) / q) if q else None
        row = (self.book.get(ex) or {}).get("row") or {}
        sn.update(state="done", done_t=stamp(), filled=q, avg_fill=avg_px, avg_after=row.get("avgCost"), held_after=(self.book.get(ex) or {}).get("pos"),
                  free_after=self.free_cash(), cancelled=len(mine), realized=round(sum(f.get("realized") or 0 for f in fills), 2))
        with c.lock:
            c.d["sell_now"] = sn; c.save()
        msg = (f"SELL NOW {self.name(ex)} done: filled {q:,.0f} of {sn['sent_qty']:,} {side.upper()}" + (f" at an average {avg_px:.4f}" if avg_px else "")
               + f" ({len(fills)} fills, realized {sn['realized']:+,.0f}); {len(mine)} unfilled clip(s) cancelled; position {sn['held_before']} -> {sn['held_after']}, "
               f"SIG average {sn['avg_before']} -> {sn['avg_after']}; free cash {sn['free_before']:,.0f} -> {sn['free_after']:,.0f}")
        self.event("SWING", msg); self.night.line(msg); self.alerts.append(f"{time.strftime('%H:%M')} {msg}")

    def reservoirs_plan(self, now):
        out = {}
        for ex, r in self.reservoirs().items():
            if ex not in self.owner or self.ex.is_frozen(ex):
                continue
            try:
                p = self.reservoir_plan(now, r)
            except Exception as err:
                self.event("ERR", f"reservoir {self.name(ex)}: {err!r}")
                continue
            if p:
                out[ex] = p
        return out

    def reservoir_plan(self, now, r):
        """{sells, buys} for one reservoir (Piero 10:10 / 16:10 Oct 8). Trade mode: every offerable share rests at its sub's price in clips
        <= clip (several at once; rule 0 per lot — a tranche lot whose entry + 0.5c is above the price waits, inventory lots are counted
        by rule0_allowed); rebuys at three levels for 40/30/30 of the shares out, never above the last sell - 3c, within free - the 2k
        guard and the race room. Rebuild mode: nothing offered; one bid at min(touch, rebuild_px) for the shares missing to full_qty."""
        ex, side = str(r["ex"]), r["side"]
        f = self.fair.get(ex)
        self.reservoir_check_triggers(now, r, f)
        px = self.px.get(ex)
        if px is None or px[0] is None or px[1] is None or px[1] <= px[0]:
            return None
        no_bid, no_ask = bots.side_book(px[0], px[1], side)
        fs = self.side_fair(ex, side)
        cash = (min(self.cash_hist) if getattr(self, "cash_hist", None) else 0.0)
        free = cash - CASH_RESERVE
        plan = dict(sells=[], buys=[])
        if r.get("mode", "trade") == "trade":
            avail = self.reservoir_avail(r)
            out_room = (float(r["max_out"]) - self.reservoir_out(r)) if r.get("max_out") is not None else float("inf")   # Piero 16:50: at most 20,000 out at a time
            if r.get("rest_qty") is not None:
                out_room = min(out_room, float(r["rest_qty"]))   # Piero 17:15: at most 15,000 resting at a time, whatever is out
            sn = self.controls.d.get("sell_now") or {}
            if str(sn.get("ex")) == ex and sn.get("state") in ("pending", "sent"):
                out_room = 0.0                              # an immediate sale is running on this market: no resting asks meanwhile
            resting = collections.defaultdict(list)
            for o in self.open:
                rec = self.ledger.orders.get(o["id"]) or {}
                if o["ex"] == ex and rec.get("layer") == "swingsell" and o["id"] not in self.ex.cancelling:
                    resting[rec.get("sub")].append(round(1 - o["price"], 3) if o["side"] != side else o["price"])
            pos_avg = float(((self.book.get(ex) or {}).get("row") or {}).get("avgCost") or 0.0)
            for sub, sp in r["offers"]:
                q = int(min(avail.get(sub, 0.0), max(0.0, out_room), float((r.get("sell_caps") or {}).get(sub, float("inf")))))
                if q < MIN_QTY:
                    continue
                t_avg = (self.ledger.tranches.get(sub) or {}).get("avg")
                if r.get("avg_floor"):
                    if pos_avg and float(sp) < round(pos_avg, 3) - 1e-9:
                        continue                            # Piero 17:40: the floor is the position average (never below it)
                elif sub.startswith(("wall:", "swing:")) and t_avg and float(t_avg) + RULE0_MARGIN > float(sp) + 1e-9:
                    continue                                # rule 0 per lot: this lot waits for a higher price
                elif sub == "inventory" or not t_avg:
                    q = min(q, self.rule0_allowed(ex, side, sub, float(sp), q))
                ask_px = float(sp)
                if r.get("join_touch") and no_ask is not None and no_ask > ask_px + 1e-9:
                    ask_px = float(no_ask)                  # Piero 16:50: join the touch when it sits above the level (a better price, passive)
                if no_bid is not None and ask_px <= no_bid + 1e-9:
                    ask_px = round(no_bid + TICK, 3)        # C84h: never cross — a level under the market's bid rests one tick above the bid (a better price)
                cur = resting.get(sub)
                if cur:                                     # queue kept: a resting price within a tick of the target, >= the level and not crossing, stays
                    p_cur = max(set(cur), key=cur.count)
                    if p_cur >= float(sp) - 1e-9 and ask_px - TICK - 1e-9 <= p_cur <= ask_px + 1e-9 and (no_bid is None or p_cur > no_bid + 1e-9):   # kept only when the touch moved up past it (it stays the best ask); above the touch it re-pegs down
                        ask_px = p_cur
                out_room -= q
                while q >= MIN_QTY:
                    take = int(min(q, r["clip"]))
                    x_ = dict(side=side, px=ask_px, qty=take, sub=sub, ttl=float(r.get("ask_ttl_s") or SWING_TTL_S))   # C87: Piero's 'queue kept' asks live 12 h
                    if r.get("avg_floor"):
                        x_["rule0_margin"] = -1.0           # the engine's rule-0 filter lets these lots go at the position-average floor (Piero's order)
                    plan["sells"].append(x_)
                    q -= take
            out_sh = self.reservoir_out(r)
            cap_px = (float(r["last_sell_px"]) - r["rebuy_under_sell"]) if r.get("last_sell_px") else 1.0
            budget = max(0.0, free - ENTRY_MIN_FREE)
            room = self.race_room(ex, side, fs if fs is not None else 0.99)
            if out_sh >= MIN_QTY and budget > 0 and room > 0 and self.entry_budget(cash) > 0:
                for lvl_px, share in r["rebuy"]:
                    p = min(float(lvl_px), cap_px)
                    if no_ask is not None and p >= no_ask - 1e-9:
                        p = round(no_ask - TICK, 3)
                    q = int(min(out_sh * float(share), budget / p, room / p))
                    if q < MIN_QTY:
                        continue
                    plan["buys"].append(dict(side=side, px=round(p, 3), qty=q, sub=f"swing:{ex}:{side}", ttl=SWING_TTL_S))
                    budget -= q * p; room -= q * p
        else:
            missing = int(float(r["full_qty"]) - self.reservoir_total(r))
            budget = max(0.0, free - ENTRY_MIN_FREE)
            room = self.race_room(ex, side, fs if fs is not None else 0.99)
            if missing >= MIN_QTY and no_bid is not None and budget > 0 and room > 0 and self.entry_budget(cash) > 0:
                p = min(no_bid, float(r["rebuild_px"]))
                q = int(min(missing, budget / p, room / p))
                if q >= MIN_QTY:
                    plan["buys"].append(dict(side=side, px=round(p, 3), qty=q, sub=f"swing:{ex}:{side}", ttl=SWING_TTL_S))
        return plan if (plan["sells"] or plan["buys"]) else None

    def reservoir_migration(self):
        """Once per config version (C84g, Piero 16:10 Oct 8): the three reservoirs; the RI one keeps its stats, mode and fair reference."""
        c = self.controls
        if os.environ.get("SIG_RESERVOIR", "1") != "1":
            return
        rs = c.d.get("reservoirs") or {}
        old = c.d.get("reservoir")
        changed = []
        for ex, base in RESERVOIRS.items():                 # a new config version updates the static fields, keeps the live state
            if ex in rs and base.get("ver") and rs[ex].get("ver") != base.get("ver"):
                keep = {k: rs[ex][k] for k in ("mode", "fair0", "since", "last_sell_px", "stats", "rebuild_t", "rebuild_why") if k in rs[ex]}
                rs[ex] = dict(json.loads(json.dumps(base)), **keep)
                changed.append(ex)
        if changed:
            with c.lock:
                c.d["reservoirs"] = rs
                c.save()
            for ex in changed:
                r = rs[ex]
                msg = (f"RESERVOIR {self.name(ex)} re-configured ({r['ver']}): offers " + ", ".join(f"{sub.split(':')[-1]} @{sp}" for sub, sp in r["offers"])
                       + (f", at most {r['max_out']:,} out" if r.get("max_out") else "") + (", joining the touch" if r.get("join_touch") else "")
                       + (f"; held: {', '.join(x.split(':')[-1] for x in r.get('hold_subs') or [])}" if r.get("hold_subs") else "")
                       + f"; rebuys {' / '.join(str(p) for p, _ in r['rebuy'])}; out now {self.reservoir_out(r):,.0f}")
                self.event("SWING", msg); self.night.line(msg)
        retired = c.d.get("reservoirs_retired") or {}
        if any(ex in rs for ex in retired):                 # C95d: a retired reservoir that an older release re-added is removed again
            with c.lock:
                for ex in list(rs):
                    if ex in retired:
                        rs.pop(ex, None)
                        self.night.line(f"RESERVOIR {self.name(ex)} removed again (retired {retired[ex].get('retired', '')}: {retired[ex].get('why', '')}); no rebuy")
                c.d["reservoirs"] = rs
                c.save()
        if rs and all(ex in rs or ex in retired for ex in RESERVOIRS):
            return
        for ex, base in RESERVOIRS.items():
            if ex in rs or ex in retired:                   # C95b: a reservoir Piero sold in full stays retired (no rebuy)
                continue
            r = json.loads(json.dumps(base)); r["mode"] = "trade"; r["fair0"] = self.fair.get(ex); r["since"] = stamp()
            r["stats"] = dict(sold_sh=0.0, bought_sh=0.0, realized=0.0, cycles=0, sold_val=0.0, bought_val=0.0, legs=[])
            if old and str(old.get("ex")) == ex:            # carry the live RI state over (out shares, last sell, mode, fair0)
                for k in ("mode", "fair0", "since", "last_sell_px", "stats", "rebuild_t", "rebuild_why"):
                    if old.get(k) is not None:
                        r[k] = old[k]
            rs[ex] = r
        with c.lock:
            c.d["reservoirs"] = rs
            c.d.pop("reservoir", None)
            sw = c.d.get("swing") or {}
            sw.pop("band", None); c.d["swing"] = sw
            c.d["layers"].setdefault("swing", dict(state="full", caps={}, killed=None))["state"] = "full"
            c.save()
        for ex, r in rs.items():
            avail = self.reservoir_avail(r)
            msg = (f"RESERVOIR {self.name(ex)} (C84g): core {float(r.get('core_qty') or 0):,.0f} held, offerable {sum(avail.values()):,.0f} ("
                   + ", ".join(f"{sub.split(':')[-1]} {q:,.0f} @{sp}" for (sub, sp), q in zip(r["offers"], [avail.get(s_, 0.0) for s_, _ in r["offers"]]))
                   + f") in clips <= {r['clip']:,}, rule 0 per lot; rebuys {' / '.join(str(p) for p, _ in r['rebuy'])} (40/30/30 of what was sold, <= last sell - {r['rebuy_under_sell'] * 100:.0f}c), "
                   f"first among entries in the ring windows; rebuild on p10 >= {r['trig_p10']}, PM move >= 2c, or {r['trig_date'][:10]}; out now {self.reservoir_out(r):,.0f}")
            self.event("SWING", msg); self.night.line(msg)

    def swing_band_plan(self, now, cfg, f):
        """turnover_plan §2.1: the RI Senate R NO inventory (the wall lots + the swing sub) trades inside [lo, hi] shares at the touch:
        a NO ask at the NO touch ask (= 1 - YES bid) while held > lo and the price is >= entry + 1c (rule 0 anyway), a NO bid at the
        NO touch bid (= 1 - YES ask) while held < hi and >= 14c under NO fair and the RI room allows; clips of 1k; the same queue
        rule as the spread maker (keep within a tick, re-peg >= 2 ticks); every fill re-places the leg."""
        band = cfg["band"]
        ex, side = str(cfg["ex"]), cfg["side"]
        px = self.px.get(ex)
        if px is None or px[0] is None or px[1] is None or px[1] <= px[0]:
            return {}
        books = self.ledger.subbooks.get(ex, {})
        swing_sub = f"swing:{ex}:{side}"
        subs = [swing_sub] + sorted(cfg["wall_subs"], key=lambda s_: -abs(books.get(s_, 0.0)))
        held = sum(abs(books.get(s_, 0.0)) for s_ in subs)
        no_bid, no_ask = bots.side_book(px[0], px[1], side)         # the NO book from the YES book
        fs = self.side_fair(ex, side)
        existing = {}
        for o in self.open:
            rec = self.ledger.orders.get(o["id"])
            if rec and rec.get("layer") in ("swing", "swingsell") and o["ex"] == ex and o["id"] not in self.ex.cancelling:
                existing[rec["layer"]] = o
        def keep(layer, target):
            cur = existing.get(layer)
            if cur is None:
                return target
            p_cur = cur["price"] if layer == "swing" else round(1 - cur["price"], 3)   # a swingsell rests as the complement buy
            return p_cur if abs(p_cur - target) <= (SM_REPEG_TICKS - 1) * TICK + 1e-9 else target
        plan = dict(sells=[])
        if held > band["lo"] + MIN_QTY and no_ask is not None:
            left = int(min(band["clip"], held - band["lo"]))
            for s_ in subs:
                avail = int(abs(books.get(s_, 0.0)))
                take = min(avail, left)
                if take < MIN_QTY:
                    continue
                avg = (self.ledger.tranches.get(s_) or {}).get("avg")
                floor_ = (float(avg) + band["sell_over_entry"]) if avg else 0.0
                p = keep("swingsell", no_ask)
                if p < floor_ - 1e-9:
                    continue                                # the touch ask is under entry + 1c: this lot waits
                plan["sells"].append(dict(side=side, px=round(p, 3), qty=take, sub=s_, ttl=SWING_TTL_S))
                left -= take
                if left < MIN_QTY:
                    break
        free = self.free_cash()
        cash = (min(self.cash_hist) if getattr(self, "cash_hist", None) else 0.0)
        credit = float(band.get("credit") or 0.0)
        sold_q, sold_val = float(band.get("sold_q") or 0.0), float(band.get("sold_val") or 0.0)
        sell_avg = (sold_val / sold_q) if sold_q >= 1 else None
        if held < band["hi"] - MIN_QTY and no_bid is not None and fs is not None and self.entry_budget(cash) > 0:   # the 2k guard stops every entry
            p = keep("swing", no_bid)
            if no_ask is not None and p >= no_ask - 1e-9:
                p = no_bid                                  # a kept price that now meets our own ask is re-pegged to the touch
            if sell_avg is not None:
                p = min(p, round(sell_avg - band.get("buy_under_sell", 0.01), 3))   # C84b (Piero 06:35): each buy at most the cycle's sell price - 1c
            if fs - p >= band["buy_min_edge"] - 1e-9 and p < (no_ask if no_ask is not None else 1.0) - 1e-9 and p >= MB_MIN_PX:
                room = self.race_room(ex, side, fs)
                q = int(min(band["clip"], band["hi"] - held, room / p if room > 0 else 0))
                if credit < p * MIN_QTY and free < self.buffer():
                    q = 0                                   # no band credit and no idle cash: the buy leg waits (cash-neutral band)
                elif free < self.buffer():
                    q = int(min(q, credit / p))             # within the band's own sale proceeds
                if q >= MIN_QTY:
                    plan["buy"] = dict(side=side, px=round(p, 3), qty=q, sub=swing_sub, ttl=SWING_TTL_S)
        return {ex: plan} if (plan["sells"] or plan.get("buy")) else {}

    def rule0_fill_check(self, fills):
        """C80 (§9.4 b): after every reducing fill of the engine's layers, compare the sale price with the FIFO lot entries it
        consumed (day-1 lots exempt); a sale under entry + 0.5c is logged as RULE0 BROKEN with the shares and the gap."""
        for f in fills:
            if f["layer"] not in self.REDUCING and f["layer"] not in self.ledger.EXIT_LAYERS:
                continue
            if f["action"] != "buy":
                continue
            held = "no" if f["side"] == "yes" else "yes"
            sub = (self.ledger.orders.get(str(f.get("order") or "")) or {}).get("sub") or "inventory"
            sale = round(1 - f["price"], 3)
            bad = 0
            try:
                for q, entry, origin in self.lots_before(f["ex"], held, sub, f):
                    if origin == "engine" and sale < entry + RULE0_MARGIN - 1e-9:
                        bad += q
            except Exception:
                continue
            if bad >= MIN_QTY and f["layer"] == "callexit":
                msg = f"RULE0-CALLED {self.name(f['ex'])} {held.upper()} {sub}: {bad:,.0f} of {f['qty']:,.0f} sold at {sale:.3f} after a strict call against it (Piero's §5.4 exemption)"
                self.event("RULE0", msg)
                self.night.line(msg)
            elif bad >= MIN_QTY:
                msg = f"RULE0 BROKEN {self.name(f['ex'])} {held.upper()} {sub}: {bad:,.0f} of {f['qty']:,.0f} sold at {sale:.3f} under their lot entry + 0.5c ({f['layer']})"
                self.event("RULE0", msg)
                self.night.line(msg)

    def lots_before(self, ex, side, sub, fill):
        """The FIFO lots of (ex, side, sub) that `fill` (a reducing fill) consumed: lots as of just before it."""
        key_t = fill["t"]
        book = collections.deque()
        for g in sorted(self.ledger.fills, key=lambda x: x["t"]):
            if g["ex"] != ex or g["t"] > key_t or g is fill:
                continue
            gsub = (self.ledger.orders.get(str(g.get("order") or "")) or {}).get("sub") or ("manual" if g.get("bot") in self.position_names else "inventory")
            if gsub != sub:
                continue
            if g["action"] == "buy" and g["side"] == side:
                book.append([g["qty"], g["price"], "day1" if g["t"] < DAY1_CUTOFF else "engine"])
            elif g["action"] == "buy" and g["side"] != side:
                q = g["qty"]
                while q > 1e-9 and book:
                    lot = book[0]; take = min(q, lot[0]); lot[0] -= take; q -= take
                    if lot[0] <= 1e-9:
                        book.popleft()
        out, q = [], fill["qty"]
        for lot in book:
            if q <= 1e-9:
                break
            take = min(q, lot[0]); out.append((take, lot[1], lot[2])); q -= take
        return out

    def big_sweep_sides(self, now):
        """Day 10 §4.2: (ex, side) whose median sweep in the last 72 h is >= 50k shares (from the intel events)."""
        if now - self.big_sweep_t < 1800:
            return self.big_sweep
        self.big_sweep_t = now
        by = collections.defaultdict(list)
        try:
            for e in intel._jsonl(os.path.join(DATA, "intel", "events.jsonl"), now - 72 * 3600):
                if (e.get("value") or 0) >= 1000:
                    side = "no" if e.get("wrong") == "YES buyer" else "yes"
                    by[(str(e["ex"]), side)].append(float(e.get("shares") or 0))
        except Exception:
            return self.big_sweep
        self.big_sweep = {k for k, v in by.items() if len(v) >= 3 and statistics.median(v) >= DUMP_BIG_SWEEP_SH}
        return self.big_sweep

    def swing_fill_events(self, fills):
        cfg = self.swing_cfg()
        if not cfg:
            return
        c = self.controls
        st = cfg.setdefault("stats", dict(sold=0.0, bought=0.0, realized=0.0, cycles=0, legs=[]))
        changed = False
        for f in fills:
            if f["layer"] not in ("swing", "swingsell"):
                continue
            changed = True
            r = self.reservoirs().get(str(f["ex"]))
            if r:                                           # C84c/g: the reservoir's stats, per-fill log and cycle c/share
                st_r = r.setdefault("stats", dict(sold_sh=0.0, bought_sh=0.0, realized=0.0, cycles=0, sold_val=0.0, bought_val=0.0, legs=[]))
                sub_f = (self.ledger.orders.get(f["order"]) or {}).get("sub")
                if f["layer"] == "swingsell":
                    sale_px = round(1 - f["price"], 3) if f["side"] != r["side"] else f["price"]
                    st_r["sold_sh"] += f["qty"]; st_r["sold_val"] += f["qty"] * sale_px; st_r["realized"] += f.get("realized") or 0.0
                    r["last_sell_px"] = sale_px
                    bk_ = self.book.get(f["ex"]) or {}
                    held_ = abs(bk_.get("pos") or 0)
                    line = (f"RESERVOIR {self.name(f['ex'])} sold {f['qty']:,.0f} {r['side'].upper()} at {sale_px:.3f} from {sub_f} (out {st_r['sold_sh'] - st_r['bought_sh']:,.0f}), realized {(f.get('realized') or 0):+,.0f}; "
                            f"position {held_:,.0f} at SIG average {((bk_.get('row') or {}).get('avgCost'))}, exposure {held_ * sale_px:,.0f} SIG at {sale_px:.3f}; free cash {self.free_cash():,.0f}")
                else:
                    st_r["bought_sh"] += f["qty"]; st_r["bought_val"] += f["qty"] * f["price"]
                    line = f"RESERVOIR {self.name(f['ex'])} rebought {f['qty']:,.0f} {r['side'].upper()} at {f['price']:.3f} (out {st_r['sold_sh'] - st_r['bought_sh']:,.0f})"
                    if st_r["bought_sh"] >= st_r["sold_sh"] - 1 and st_r["sold_sh"] >= 1:
                        sq, sv, bq, bv = st_r["sold_sh"], st_r["sold_val"], st_r["bought_sh"], st_r["bought_val"]
                        st_r["cycles"] += 1
                        line += f" — cycle {st_r['cycles']}: sold {sq:,.0f} at {sv / sq:.3f}, rebought {bq:,.0f} at {bv / bq:.3f} = {(sv / sq - bv / bq) * 100:+.2f}c per share"
                        st_r.update(sold_sh=0.0, bought_sh=0.0, sold_val=0.0, bought_val=0.0)
                st_r["legs"] = (st_r.get("legs") or [])[-300:] + [dict(t=f["t"], layer=f["layer"], qty=f["qty"], px=f["price"], sub=sub_f, realized=f.get("realized"))]
                self.event("SWING", line); self.night.line(line)
                self.reservoir_save(r)
            band = cfg.get("band")
            if band is not None:                            # C84b: the band's cash-neutral credit and its cycle log
                if f["layer"] == "swingsell":
                    sale_px = round(1 - f["price"], 3) if f["side"] != cfg["side"] else f["price"]
                    band["credit"] = float(band.get("credit") or 0.0) + f["qty"] * sale_px
                    band["sold_q"] = float(band.get("sold_q") or 0.0) + f["qty"]; band["sold_val"] = float(band.get("sold_val") or 0.0) + f["qty"] * sale_px
                else:
                    band["credit"] = float(band.get("credit") or 0.0) - f["qty"] * f["price"]
                    band["bought_q"] = float(band.get("bought_q") or 0.0) + f["qty"]; band["bought_val"] = float(band.get("bought_val") or 0.0) + f["qty"] * f["price"]
                    if band["bought_q"] >= float(band.get("sold_q") or 0.0) - 1 and float(band.get("sold_q") or 0.0) >= 1:
                        sq, sv, bq, bv = float(band["sold_q"]), float(band["sold_val"]), band["bought_q"], band["bought_val"]
                        cyc = (f"BAND cycle {self.name(f['ex'])}: sold {sq:,.0f} NO at {sv / sq:.3f}, bought back {bq:,.0f} at {bv / bq:.3f} = {(sv / sq - bv / bq) * 100:+.2f}c per share "
                               f"({sv - bv * (sq / bq):+,.0f} on the cycle); credit left {band['credit']:,.0f}")
                        self.event("SWING", cyc); self.night.line(cyc)
                        band["cycles"] = int(band.get("cycles") or 0) + 1
                        band.update(sold_q=0.0, sold_val=0.0, bought_q=0.0, bought_val=0.0)
            if f["layer"] == "swingsell":
                st["sold"] += f["qty"]; st["realized"] += f.get("realized") or 0.0
                msg = f"SWING sell leg {self.name(f['ex'])}: {f['qty']:,.0f} {cfg['side'].upper()} sold at {1 - f['price'] if f['side'] != cfg['side'] else f['price']:.3f}, realized {(f.get('realized') or 0):+,.0f}"
            else:
                st["bought"] += f["qty"]
                sub = f"swing:{f['ex']}:{f['side']}"
                t = self.ledger.tranches.setdefault(sub, dict(ex=f["ex"], side=f["side"], kind="swing", t=stamp(), ts=time.time(), state="open", avg=f["price"]))
                t["avg"] = round(self.ledger.tranche_avg(sub, "swing", ex=f["ex"], side=f["side"], since="2026-10-07 00:00:00") or f["price"], 5)
                msg = f"SWING buy leg {self.name(f['ex'])}: {f['qty']:,.0f} {f['side'].upper()} bought back at {f['price']:.3f}"
                if st["bought"] >= st["sold"] - 1 and st["sold"] > 0:
                    st["cycles"] += 1
                    msg += f" — cycle {st['cycles']} complete"
            st["legs"] = (st.get("legs") or [])[-200:] + [dict(t=f["t"], layer=f["layer"], qty=f["qty"], px=f["price"], realized=f.get("realized"))]
            self.event("SWING", msg)
            self.alerts.append(f"{time.strftime('%H:%M')} {msg}")
            self.night.line(msg)
        if changed:
            with c.lock:
                c.d["swing"] = cfg
                c.save()

    def swing_view(self):
        cfg = self.swing_cfg()
        if not cfg:
            return None
        held, sold, in_walls, swing_q = self.swing_state()
        st = cfg.get("stats") or {}
        return dict(slice=cfg["slice"], held=round(held), sold_out=round(sold), in_walls=round(in_walls), swing_sub=round(swing_q),
                    sold_total=round(st.get("sold", 0)), bought_total=round(st.get("bought", 0)), realized=round(st.get("realized", 0), 2), cycles=st.get("cycles", 0))

    # ------------------------------------------------------------------------------------------------ mm_plan §2 / §8.1: the fast path
    def fast_step(self, now):
        """Between a realtime wake and the full loop: act on our new fills and on Polymarket moves within ~1 s.
        L0 sweep-follow: our L0 level filled -> one book read -> re-bid at the new best bid while it is >= 5c under fair
        (<= 3 fast re-arms per side per 10 min, under the L0 held cap and the refill cap). FM: the filled side is re-quoted;
        a PM move >= 2c pulls that market's FM quotes (per-order cancels). 5 writes/min stay reserved. The next full loop
        reconciles: a fast order that is neither resting nor filled sends its market to the slow path for 10 min."""
        if not FAST_PATH_ON or api.DRY_RUN or self.controls.paused or self.fills_blind:
            return 0
        sent_el = 0
        if self.election is not None and self.el_mode_cached != "off" and self.election.queue:
            sent_el = self.election.run_queue(now, self.el_snap["cash"], self.el_snap["pos"])   # C102: a call's snipe, within the wake
        with self.rt_lock:
            events = list(self.rt_pending)
        sig_ev = [e for e in events if e[1] in ("fills", "orders")]
        pm_ev = [e for e in events if e[1] == "pm_move"]
        sent = 0
        if pm_ev and self.controls.on("fm"):
            sent += self.fast_pull(pm_ev, now)
        if not sig_ev:
            return sent
        if api.writes_left() <= FAST_WRITES_RESERVE:
            self.fast_stats["skipped_writes"] += 1
            return sent
        try:
            known = {f["key"] for f in self.ledger.fills} | self.fast_seen
            new = [f for f in api.fills(known=known, max_pages=1) if f"{f['id']}:{f.get('orderId')}" not in known]
        except Exception as err:
            self.event("ERR", f"fast path fills read: {err!r}")
            return sent
        t_ev = min(e[0] for e in sig_ev)
        for f in new:
            key = f"{f['id']}:{f.get('orderId')}"
            self.fast_seen.add(key)
            rec = self.ledger.orders.get(str(f.get("orderId") or ""))
            if not rec:
                continue
            try:
                self.fast_match_t = datetime.fromisoformat(str(f.get("filledAt")).replace("Z", "+00:00")).timestamp()
            except Exception:
                self.fast_match_t = None
            ex = rec["ex"]
            if self.fast_slow.get(ex, 0) > now:
                self.fast_stats["slow_skipped"] += 1
                continue
            if rec.get("layer") == "dump" and str(rec.get("sub", "")).endswith(":L0"):
                sent += self.fast_l0(ex, rec, t_ev, now, filled_id=str(f.get("orderId") or ""))
            elif rec.get("layer") in ("fm", "fmexit"):
                sent += self.fast_fm(ex, rec, t_ev, now)
            elif rec.get("layer") in ("sm", "smexit"):
                sent += self.fast_sm(ex, rec, t_ev, now)   # turnover_plan §1: refill the filled side at once
        if len(self.fast_seen) > 5000:
            self.fast_seen = set(list(self.fast_seen)[-2000:])
        return sent

    def fast_l0(self, ex, rec, t_ev, now, filled_id=""):
        side = rec["side"]
        key = (ex, side)
        self.fast_rearms[key] = [t for t in self.fast_rearms[key] if t >= now - 600]
        if len(self.fast_rearms[key]) >= FAST_REARMS_PER_10M or self.l0_cap_used() >= DUMP_L0_BUDGET or (str(ex), rec.get("sub")) in self.core_set or self.l0_refills(rec.get("sub"), now) >= DUMP_L0_REFILLS + 1:
            self.fast_stats["l0_capped"] += 1
            return 0
        fs = self.side_fair(ex, side)
        if fs is None or ex in self.manual_ex or not self.controls.on("dump") or (self.controls.d.get("ri_freeze") and (self.meta.get(ex) or {}).get("race") in RI_RACES):
            return 0
        fb = self.feed_book(ex)                             # C86 live: the feed's book saves the fast path's REST read (~0.3 s)
        try:
            bids, asks = fb if fb is not None else api.orderbook(ex, depth=5)
        except Exception as err:
            self.event("ERR", f"fast path book read {self.name(ex)}: {err!r}")
            return 0
        self.px[ex] = (bids[0][0] if bids else None, asks[0][0] if asks else None, self.px.get(ex, (None, None, None))[2])
        bid_s = bots.side_book(self.px[ex][0], self.px[ex][1], side)[0]
        if bid_s is None or bid_s > fs - DUMP_L0_GAP + 1e-9 or not (MB_MIN_PX <= bid_s <= MB_MAX_PX) or self.ri_edge_short(ex, bid_s, fs) or not self.catcher_ok(ex, side, 4, bid_s, fs):
            self.fast_stats["l0_no_edge"] += 1
            return 0
        if ALLOC.get("on") and self.catcher_new_cost() + bid_s * DUMP_L0_CLIP > float(ALLOC["catcher_slice"]):
            self.fast_stats["l0_capped"] += 1                 # C97 item 4a: the catcher's 10k slice
            return 0
        room = self.race_room(ex, side, fs)
        q = int(min(DUMP_L0_CLIP if (ex, side) not in self.big_sweep else DUMP_BIG_CLIP, room / bid_s if room > 0 else 0))
        if q < MIN_QTY:
            return 0
        sub = rec["sub"]
        if any(o["ex"] == ex and o["id"] != filled_id and (self.ledger.orders.get(o["id"]) or {}).get("sub") == sub and o["id"] not in self.ex.cancelling for o in self.open):
            return 0                                        # another order of this level already rests (the filled one is still in the stale open list)
        t_fast = self.fast_orders.get((ex, sub))
        if (t_fast is not None and (self.open_read_t is None or t_fast >= self.open_read_t - 1)) or \
                any(p.get("ex") == ex and p.get("sub") == sub and p.get("action") == "buy" for p in self.ex.pending()):
            self.fast_stats["l0_dup"] += 1                  # Piero 21:15 item 3: a re-bid already went out since the last orders read (Florida Senate D, Oct 15: one per partial fill)
            return 0
        self.ex.place([(self.owner[ex].name, sub, "dump", (ex, side, "buy", bid_s, q))], ttl_s=DUMP_NEAR_TTL_S)
        n = self.ex.step_placements()
        self.fast_rearms[key].append(now)
        self.fast_orders[(ex, sub)] = now
        self.sample("fast_fill_to_sent", time.time() - t_ev)
        if getattr(self, "fast_match_t", None):
            self.sample("fast_match_to_sent", time.time() - self.fast_match_t)   # SIG's match -> sent (docs/latency.md: the event clock can start before the fill)
        self.fast_stats["l0_rearms"] += 1
        self.event("FAST", f"FAST L0 re-bid {self.name(ex)} {side.upper()}: {q:,} at {bid_s:.3f} ({(fs - bid_s) * 100:.1f}c under fair), {time.time() - t_ev:.2f} s after the fill event")
        return n

    def fast_fm(self, ex, rec, t_ev, now):
        if not self.controls.on("fm") or self.fm_block.get(ex, 0) > now:
            return 0
        plan = self.fm_plan(now).get(ex) or []
        side_filled = rec["side"]
        wants = [q for q in plan if q["side"] == side_filled]   # the side that just filled is re-quoted (the other still rests)
        if not wants:
            return 0
        items = []
        for q in wants:
            px = q["px"] if not q.get("reduce") else round(1 - q["sell_px"], 3) if q["held"] == "yes" else q["sell_px"]
            items.append((self.owner[ex].name, q["sub"], "fmexit" if q.get("reduce") else "fm", (ex, q["side"], "buy", px, int(q["qty"]))))
        self.ex.place(items, ttl_s=FM_TTL_S)
        n = self.ex.step_placements()
        self.fast_orders[(ex, "fm")] = now
        self.sample("fast_fill_to_sent", time.time() - t_ev)
        if getattr(self, "fast_match_t", None):
            self.sample("fast_match_to_sent", time.time() - self.fast_match_t)   # SIG's match -> sent (docs/latency.md: the event clock can start before the fill)
        self.fast_stats["fm_requotes"] += 1
        self.event("FAST", f"FAST FM re-quote {self.name(ex)} {side_filled.upper()}: {len(items)} quote(s), {time.time() - t_ev:.2f} s after the fill event")
        return n

    def fast_pull(self, pm_ev, now):
        targets = {str(t["ex"]) for t in self.fm_targets()}
        n = 0
        for t_ev, _, exs in pm_ev:
            for ex in exs:
                if ex not in targets or self.fast_slow.get(ex, 0) > now:
                    continue
                h = [x for x in self.fair_hist.get(ex, []) if x[0] >= now - FM_NEWS_WINDOW_S]
                f = self.fair.get(ex)
                if f is None:
                    continue
                move = max(abs(f - v) for _, v in h) if h else 0.0
                if move < FAST_PM_MOVE_C - 1e-9:
                    continue
                pulled = 0
                for o in list(self.open):
                    r = self.ledger.orders.get(o["id"])
                    if o["ex"] == ex and r and r.get("layer") in ("fm", "fmexit") and o["id"] not in self.ex.cancelling:
                        self.ex.cancel(o, reason="fast pull on a Polymarket move", front=True); pulled += 1
                if pulled:
                    self.ex.step_cancels()
                    self.fm_block[ex] = now + FM_REQUOTE_S
                    self.sample("fast_pm_to_pull", time.time() - t_ev)
                    self.fast_stats["pm_pulls"] += 1
                    self.event("FAST", f"FAST pull {self.name(ex)}: Polymarket moved {move * 100:.1f}c, {pulled} FM quote(s) cancelled {time.time() - t_ev:.2f} s after the event")
                    n += pulled
        return n

    def fast_reconcile(self, now):
        """In the full loop, after the REST state is in: a fast order placed >= 20 s ago must be resting, filled or cancelled by us;
        otherwise the market goes to the slow path for 10 min and the event is logged."""
        for (ex, sub), t0 in list(self.fast_orders.items()):
            if now - t0 < 20:
                continue
            del self.fast_orders[(ex, sub)]
            same = lambda r: r.get("sub") == sub or sub == "fm" or (sub == "sm" and r.get("layer") in ("sm", "smexit"))   # Piero 21:15 item 3: the SM key is "sm", its subs "sm:<ex>..."
            ok = any(o["ex"] == ex and same(self.ledger.orders.get(o["id"]) or {}) for o in self.open) or \
                 any(r["ex"] == ex and same(r) and (r.get("ts") or 0) >= t0 - 1 and (r.get("status") in ("done", "cancelled") or (r.get("filled") or 0) > 0) for r in self.ledger.orders.values())
            if not ok:
                self.fast_slow[ex] = now + FAST_SLOW_S
                self.fast_stats["mismatch"] += 1
                self.event("FAST", f"FAST mismatch {self.name(ex)} ({sub}): the fast order is neither resting nor filled: slow path for 10 min")

    # ------------------------------------------------------------------------------------------------ mm_plan §1/§4: the fair maker (FM)
    def fm_targets(self):
        """C87: the fair maker is deleted (releases/archive/fm_layer.py): no targets; its markets are open to the TM and the SM."""
        return []

    def fm_inventory(self, ex):
        """Signed YES inventory of the FM sub (YES buys +, NO buys -)."""
        return float(self.ledger.subbooks.get(ex, {}).get(f"fm:{ex}", 0.0))

    def fm_cost(self):
        """The FM's leftover lots at cost (its fm:{ex} sub-books)."""
        out = 0.0
        for ex, books in self.ledger.subbooks.items():
            q = books.get(f"fm:{ex}", 0.0)
            if abs(q) >= 1:
                avg = (self.ledger.tranches.get(f"fm:{ex}") or {}).get("avg") or self.sig_mark(ex, "yes" if q > 0 else "no") or 0.5
                out += abs(q) * float(avg)
        return out

    def fm_reserve(self):
        """C87: the FM is deleted: nothing reserved."""
        return 0.0

    def fm_writes_last_min(self, now):
        """FM writes in the last minute = distinct placement seconds (one batch write places up to 50 orders)."""
        return len({int(o["ts"]) for o in self.ledger.orders.values() if o.get("layer") in ("fm", "fmexit") and (o.get("ts") or 0) >= now - 60})

    def fm_poll_block(self, ex, now):
        """mm_plan §1 election glide: no quotes on a market from its state's last poll close - 10 min (data/election/poll_close.json)."""
        if self.fm_poll is None:
            try:
                rows = json.load(open(os.path.join(DATA, "election", "poll_close.json")))["markets"]
                self.fm_poll = {str(r["ex"]): r["last_close_et"] for r in rows}
            except Exception:
                self.fm_poll = {}
        lc = self.fm_poll.get(ex)
        if not lc:
            return False
        hh, mm = int(lc[:2]), int(lc[3:5])
        day = 4 if lc.endswith("+1") else 3
        close = time.mktime(time.strptime(f"2026-11-{day:02d} {hh:02d}:{mm:02d}:00", "%Y-%m-%d %H:%M:%S")) - 3 * 3600   # ET -> PT (server clock = PT)
        return now >= close - FM_POLL_PULL_S

    def fm_plan(self, now, cash):
        """C87 (Piero 19:56 c): the fair maker is deleted (its quoting code: releases/archive/fm_layer.py). What is left are its
        lots: each is offered at max(entry + 0.5c, the SIG touch ask on its side) — never below cost (rule 0) — one exit of the
        whole lot at a time, kept in its queue place while the target moves by a tick or less, until it is sold."""
        plan = {}
        resting = {}
        for o in self.open:
            rec = self.ledger.orders.get(o["id"])
            if rec and rec.get("layer") == "fmexit" and o["id"] not in self.ex.cancelling:
                resting[o["ex"]] = o
        for ex, books in list(self.ledger.subbooks.items()):
            inv = float(books.get(f"fm:{ex}", 0.0))
            if abs(inv) < MIN_QTY or ex not in self.owner or self.ex.is_frozen(ex):
                continue
            sub = f"fm:{ex}"
            held = "yes" if inv > 0 else "no"
            avg = (self.ledger.tranches.get(sub) or {}).get("avg")
            if not avg or self.lot_policy(ex, held, sub, avg, self.side_fair(ex, held)) != "exit" or self.release_resting(ex, sub):
                continue                                    # C88: a core lot or a lot >= 3c under fair is held; C90: or a release ask covers it
            floor_ = max(float(avg), max((l[1] for l in self.lots(ex, held, sub) if l[2] == "engine" and l[0] > 0), default=0.0)) + RULE0_MARGIN
            pxs = self.px.get(ex)
            ask_s = bots.side_book(pxs[0], pxs[1], held)[1] if pxs and pxs[0] is not None and pxs[1] is not None else None
            sell = bots.tick_up(round(max(floor_, ask_s if ask_s is not None else floor_), 6))
            cur = resting.get(ex)
            if cur is not None:
                cur_sell = round(1 - cur["price"], 3)
                if abs(cur_sell - sell) <= TICK + 1e-9 and cur_sell >= floor_ - 1e-9:
                    sell = cur_sell                         # keep the queue place
            if sell > MB_MAX_PX:
                continue
            plan[ex] = [dict(reduce=True, held=held, sell_px=round(sell, 3), qty=int(abs(inv)), sub=sub, ttl=TM_TTL_S,
                             side="no" if held == "yes" else "yes", px=round(1 - sell, 3))]
        return plan

    def fm_fill_events(self, fills):
        c = self.controls
        st = c.d.setdefault("fm_stats", dict(fills=0, realized=0.0, edge=0.0, since=stamp()))
        changed = False
        for f in fills:
            if f["layer"] not in ("fm", "fmexit"):
                continue
            changed = True
            ex = f["ex"]; sub = f"fm:{ex}"
            fv = self.side_fair(ex, f["side"])
            edge = 0.0 if fv is None else (fv - f["price"]) * f["qty"]   # buying `side` at price vs that side's fair
            st["fills"] += 1; st["realized"] += f.get("realized") or 0.0; st["edge"] += edge
            self.fm_fills.append(dict(t=time.time(), ex=ex, side=f["side"], px=f["price"], qty=f["qty"], layer=f["layer"]))
            q = self.fm_inventory(ex)
            tr = self.ledger.tranches.get(sub)
            if abs(q) >= 1:
                if tr is None or tr.get("state") != "open" or (tr.get("side") != ("yes" if q > 0 else "no")):
                    tr = self.ledger.tranches[sub] = dict(ex=ex, side="yes" if q > 0 else "no", kind="fm", t=stamp(), ts=time.time(), state="open", avg=f["price"])
                tr["avg"] = round(self.ledger.tranche_avg(sub, "fm", ex=ex, side=tr["side"], since=time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(tr["ts"] - 300))) or f["price"], 5)
            elif tr is not None:
                tr["state"] = "closed"
            msg = (f"FM fill {self.name(ex)}: {'bought' if f['layer'] == 'fm' else 'closed'} {f['qty']:,.0f} {f['side'].upper()} at {f['price']:.3f}"
                   + ("" if fv is None else f" (fair {fv:.3f}, edge {edge:+,.0f})") + f", realized {(f.get('realized') or 0):+,.0f}; inventory {q:+,.0f} YES")
            self.event("FM", msg)
            self.night.line(msg)
        if changed:
            with c.lock:
                c.d["fm_stats"] = st
                c.save()
        # auto-off (mm_plan §4)
        if c.on("fm") and st["fills"]:
            mo = self.fm_markout(1800)
            why = None
            if st["realized"] < FM_OFF_REALIZED:
                why = f"realized {st['realized']:+,.0f} < {FM_OFF_REALIZED:+,.0f}"
            elif mo["n"] >= FM_OFF_MIN_FILLS and mo["avg"] < FM_OFF_MO30:
                why = f"30-min mark-out vs the moving fair {mo['avg'] * 100:+.2f}c over {mo['n']} fills < {FM_OFF_MO30 * 100:+.1f}c"
            if why:
                with c.lock:
                    c.d["layers"].setdefault("fm", dict(state="off", caps={}, killed=None))["state"] = "off"
                    c.save()
                msg = f"FM AUTO-OFF: {why}; quotes pulled (Piero decides whether it comes back)"
                self.event("FM", msg)
                self.night.line(msg)
                self.alerts.append(f"{time.strftime('%H:%M')} {msg}")

    def fm_markout(self, horizon_s):
        """Average mark-out vs the MOVING Polymarket fair `horizon_s` after each FM fill (fills old enough only)."""
        now = time.time(); vals = []
        for f in self.fm_fills:
            if now - f["t"] < horizon_s:
                continue
            fh = self.fair_at(f["ex"], f["t"] + horizon_s)
            if fh is None:
                continue
            fs = fh if f["side"] == "yes" else 1 - fh
            vals.append(fs - f["px"])                       # we bought `side` at px: fair of that side later minus px
        return dict(n=len(vals), avg=(sum(vals) / len(vals)) if vals else 0.0)

    def fm_view(self):
        st = self.controls.d.get("fm_stats") or {}
        inv = {ex: float(books.get(f"fm:{ex}", 0.0)) for ex, books in self.ledger.subbooks.items() if abs(books.get(f"fm:{ex}", 0.0)) >= 1}
        return dict(fills=st.get("fills", 0), realized=round(st.get("realized", 0.0), 2), edge=round(st.get("edge", 0.0), 2),
                    inventory=inv, cost=round(self.fm_cost(), 2), mo30=self.fm_markout(1800), writes_last_min=self.fm_writes_last_min(time.time()), on=False, retired=True)

    # ------------------------------------------------------------------------------------------------ day 11 §2: the touch maker (TM, scalper v2)
    def ring_window(self, now):
        """Piero 15:35 Oct 8 (1c): True inside a ring window (19:30-21:00, 03:30-06:00 PT); the switch is logged."""
        lt = time.localtime(now); hm = lt.tm_hour * 60 + lt.tm_min
        on = any((int(a[:2]) * 60 + int(a[3:])) <= hm < (int(b[:2]) * 60 + int(b[3:])) for a, b in RING_WINDOWS)
        if on != self.ring_on:
            self.ring_on = on
            msg = ("RING WINDOW on: no new TM / SM entries (exits keep running, reserves drop to holdings); cash to the reservoir's rebuys first, then L0 / L1, then the rest"
                   if on else "RING WINDOW off: TM / SM entries and reserves back to normal")
            self.event("CASH", msg); self.night.line(msg)
        return on

    def tm_pilot(self):
        return self.controls.state("tm") != "full"

    def tm_sizes(self):
        p = self.tm_pilot()
        return dict(clip=TM_CLIP_PILOT if p else TM_CLIP, inv=TM_INV_MARKET_PILOT if p else TM_INV_MARKET,
                    cap=TM_CAP_COST_PILOT if p else TM_CAP_COST, sides=TM_SIDES_PILOT if p else None)

    def tm_inventory(self, ex, side):
        """Shares of `side` held by the TM sub on ex (0 when the sub is flat or on the other side)."""
        q = float(self.ledger.subbooks.get(ex, {}).get(f"tm:{ex}:{side}", 0.0))
        return abs(q) if abs(q) >= 1 and (q > 0) == (side == "yes") else 0.0

    def tm_cost(self, ex=None):
        out = 0.0
        for e, books in self.ledger.subbooks.items():
            if ex is not None and e != ex:
                continue
            for sub, q in books.items():
                if sub.startswith("tm:") and abs(q) >= 1:
                    avg = (self.ledger.tranches.get(sub) or {}).get("avg") or self.sig_mark(e, "yes" if q > 0 else "no") or 0.0
                    out += abs(q) * float(avg)
        return out

    def tm_reserve(self):
        """The TM's room in the entry budget, reserved off the top (weight 1.0, after L0, before the FM and L1-L4)."""
        now = time.time()
        if not self.controls.on("tm") or self.ring_window(now) or self.tm_entry_stop(now) or (TM_BUFFER_GATE and getattr(self, "tm_buf_paused", False)):
            return 0.0                                      # (1c / C87) no entries: nothing reserved off the top
        return max(0.0, min(self.tm_cap(now, log=False) - self.tm_cost(), self.free_cash() - TM_PAUSE_FREE))   # Piero 21:37: never the catcher's last 10k

    def tm_entry_stop(self, now):
        """C87 (Piero 19:56 a): no TM entries from 60 min before each ring (20:00, 04:00, 13:00 PT) to the end of its window."""
        hm = time.strftime("%H:%M", time.localtime(now))
        return any((a <= hm < b) if a <= b else (hm >= a or hm < b) for a, b in TM_ENTRY_STOPS)

    def tm_buffer_gate(self, now):
        """C87 (Piero 19:56 a): TM entries pause while free < BUFFER and resume at BUFFER + 1k; logged on each change. True = paused."""
        if not TM_BUFFER_GATE:
            return False
        free, buf = self.free_cash(), float(TM_PAUSE_FREE)   # Piero 21:37: the floor is 10k (was BUFFER)
        was = getattr(self, "tm_buf_paused", False)
        now_paused = free < buf if not was else free < buf + TM_BUFFER_RESUME
        if now_paused != was:
            self.tm_buf_paused = now_paused
            msg = (f"TM entries PAUSED: free {free:,.0f} < {buf:,.0f} (resting TM bids cancelled; exits keep running)" if now_paused
                   else f"TM entries resume: free {free:,.0f} >= {buf:,.0f} + {TM_BUFFER_RESUME:,}")
            self.event("TM", msg); self.night.line(msg)
        return now_paused

    def free_cash(self):
        return (min(self.cash_hist) if getattr(self, "cash_hist", None) else 0.0) - CASH_RESERVE

    def tm_e_min(self):
        """C87 (Piero 19:56 a): every side >= 2c cheap vs fair (the C82 4c-under-BUFFER rule ended with the 10k entry floor)."""
        return TM_E_MIN

    def tm_cap(self, now, log=True):
        """Piero 22:15 Oct 7 (C82 item 2): TM cap = base (8k pilot / 20k full) + max(0, free - BUFFER), recomputed each loop, logged on change."""
        base = float(self.tm_sizes()["cap"])
        cap = base + max(0.0, self.free_cash() - self.buffer())
        step = int(cap // TM_CAP_LOG_STEP)
        if log and step != self.tm_cap_logged:
            if self.tm_cap_logged is not None:
                self.event("TM", f"TM cap -> {cap:,.0f} (base {base:,.0f} + max(0, free {self.free_cash():,.0f} - BUFFER {self.buffer():,.0f}))")
            self.tm_cap_logged = step
        return cap

    def tm_market_profile(self, now):
        """Piero 22:15 Oct 7 (C82 item 3): {ex: (clip, inventory cap)} from the market's 24-h TM round-trip share (shares sold / shares
        bought, buys older than 30 min): >= 50% -> clip 1k and cap 3k; < 20% -> cap 1k; else the pilot / full defaults."""
        sz = self.tm_sizes()
        bought, sold = collections.Counter(), collections.Counter()
        for f in self.fills_since(now - 86400):
            if f["layer"] == "tm":
                t = time.mktime(time.strptime(f["t"], "%Y-%m-%d %H:%M:%S"))
                if t <= now - TM_RT_GRACE_S:
                    bought[f["ex"]] += f["qty"]
            elif f["layer"] == "tmexit":
                sold[f["ex"]] += f["qty"]
        out = {}
        for ex, b in bought.items():
            if b <= 0:
                continue
            share = sold.get(ex, 0.0) / b
            if share >= TM_RT_HI:
                out[ex] = (TM_CLIP_HI, TM_INV_HI, share)
            elif share < TM_RT_LO:
                out[ex] = (sz["clip"], TM_INV_LO, share)
            else:
                out[ex] = (sz["clip"], sz["inv"], share)
        return out

    def tm_writes_last_min(self, now):
        """TM writes in the last minute: distinct placement seconds (a batch is one write) + cancels (one write each)."""
        secs, cancels = set(), 0
        cut = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(now - 60))
        for o in self.ledger.orders.values():
            if o.get("layer") not in ("tm", "tmexit"):
                continue
            if (o.get("ts") or 0) >= now - 60:
                secs.add(int(o["ts"]))
            if o.get("status") == "cancelled" and (o.get("closed") or "") >= cut:
                cancels += 1
        return len(secs) + cancels

    def tm_score(self, ex):
        """Priority = the 24-h SIG value printed >= 2c off fair on the market (volume x the off-fair share), from our own tape."""
        return sum(v for _, v in self.off24.get(str(ex), ()))

    def tm_candidates(self):
        """[(score, ex, side, fair_side, best, spread_ticks)] on every mapped market (never manual, frozen or an FM target): the
        side that is cheap vs fair — YES when the SIG bid <= fair - e_min, NO when the SIG ask >= fair + e_min — best first."""
        fm_ex = {str(t["ex"]) for t in self.fm_targets()}
        sm_ex = set(self.sm_universe(time.time())) if self.controls.on("sm") else set()   # ideas_backlog #4: the touch maker steps aside in every SM market
        e_min = self.tm_e_min()
        out = []
        for b in self.bots:
            if b.kind != "market" or b.ex in self.manual_ex or b.ex in fm_ex or b.ex in sm_ex or b.ex in self.reservoirs() or self.ex.is_frozen(b.ex):
                continue                                    # (Piero 17:40) a reservoir market is bought only by the catcher
            ex, f, px = b.ex, self.fair.get(b.ex), self.px.get(b.ex)
            if f is None or px is None or px[0] is None or px[1] is None or px[1] <= px[0]:
                continue
            sp = int(round((px[1] - px[0]) / TICK))
            for side in ("yes", "no"):
                fs = f if side == "yes" else 1 - f
                best = px[0] if side == "yes" else round(1 - px[1], 3)
                if best > fs - e_min + 1e-9 or not (MB_MIN_PX <= best <= MB_MAX_PX):
                    continue
                out.append(((fs - best) / best, ex, side, fs, best, sp))   # Piero 22:15 (C82 item 1): ranked by edge / price (the off-fair value breaks ties)
        out.sort(key=lambda r: (-r[0], -self.tm_score(r[1]), r[1], r[2]))
        return out

    def own_ask(self, ex, side):
        """The lowest price at which one of our own resting exits sells `side` on ex (I4: a TM bid must stay under it), else None."""
        best = None
        for o in self.open:
            rec = self.ledger.orders.get(o["id"])
            if o["ex"] != ex or o["action"] != "buy" or o["side"] == side or not rec or rec.get("layer") not in self.REDUCING:
                continue                                    # C93c: an ask being re-pegged still counts (its layer re-posts it)
            px = round(1 - o["price"], 3)
            best = px if best is None else min(best, px)
        return best

    def tm_touch_depth(self, ex, side, now):
        """Shares at the SIG touch of `side` (order-book read cached 5 min, only with spare reads), else None."""
        fb = self.feed_book(ex)                             # C86 live: the feed's book, no REST read
        if fb is not None:
            return (fb[0][0][1] if fb[0] else 0.0) if side == "yes" else (fb[1][0][1] if fb[1] else 0.0)
        c = self.tm_depth.get(ex)
        if not c or now - c[0] > TM_DEPTH_CACHE_S:
            if not api.spare_reads() or now - getattr(self, "boot_t", 0) < STARTUP_HOLD_S:
                return None
            try:
                bids, asks = api.orderbook(ex, depth=1)
            except Exception as err:
                self.event("ERR", f"TM depth read {self.name(ex)}: {err!r}")
                return None
            c = self.tm_depth[ex] = (now, bids, asks)
        _, bids, asks = c
        if side == "yes":
            return bids[0][1] if bids else 0.0
        return asks[0][1] if asks else 0.0

    def tm_plan(self, now, cash, used):
        """{ex: [want-spec]} (day 11 §2): on the ranked cheap sides join the best bid (improve by a tick when the spread is >= 2
        ticks) only at <= fair_side - 2c (RI: >= 15c under fair), never crossing and never at or above our own ask on the
        side (I4); a resting bid keeps its queue place while it is at the touch or one tick behind and still >= e_min under
        fair, is re-pegged only >= 2 ticks behind (2 writes) and pulled when its edge is gone; clips 500 (pilot) / 1,000,
        <= 20% of the touch depth; 1.5k / 3k shares per market-side and 8k / 20k at cost in total (at a cap only the exit
        quotes); the party cap, the race room, the news guard (2c / 10 min -> 5 min), the poll-close glide; <= 8 writes/min;
        the pilot takes the top 60 market-sides by 24-h value printed >= 2c off fair."""
        c = self.controls
        if not c.on("tm") or c.paused or self.fills_blind:
            return {}
        h1m = set(self.h1_test_markets()) if self.h1_test_active(now) else set()   # C93b (Piero 16:15): the H1 sleeve = touch-maker quotes on H5's ten, inside 5,000
        if self.entry_budget(cash) <= 0 and not h1m:
            return {}
        if c.state("tm") == "pilot" and self.free_cash() >= self.buffer() and c.d.get("tm_autofull", True):   # control 'tm_autofull': False keeps the pilot
            c.set_layer("tm", "full", f"Piero 16:10 §3: free {self.free_cash():,.0f} >= BUFFER {self.buffer():,.0f}", by="engine")
            msg = f"TM -> FULL SIZE (clip {TM_CLIP:,}, {TM_INV_MARKET:,} per market-side, {TM_CAP_COST:,}): free {self.free_cash():,.0f} >= BUFFER {self.buffer():,.0f} (Piero 16:10 §3)"
            self.event("TM", msg); self.night.line(msg)
        h1_only = False
        if self.ring_window(now):
            self.tm_cov = dict(quoted=0, available=0, top=0, writes_left=0, writes_used=0, ring=True)
            if not h1m:
                return {}                                   # (1c) a ring window: no new TM entries (resting bids go; the exits in track_tm keep running)
            h1_only = True                                  # C93c (Piero 16:30): the ring stops are lifted inside the H1 sleeve only (its cash is reserved apart from the catcher)
        stop = self.tm_entry_stop(now)
        if stop != getattr(self, "tm_stop_logged", None):
            if self.tm_stop_logged is not None or stop:
                msg = ("TM entries STOP: 60 min before the ring (resting TM bids cancelled, cash back for the catcher; exits keep running)" if stop
                       else "TM entries back after the ring window")
                self.event("TM", msg); self.night.line(msg)
            self.tm_stop_logged = stop
        if stop or self.tm_buffer_gate(now) or self.trading_inv_gate(now):
            self.tm_cov = dict(quoted=0, available=0, top=0, writes_left=0, writes_used=0, ring=stop, buffer_pause=not stop)
            if not h1m:
                return {}                                   # C87 / C89: no TM entries (resting bids go; exits keep running)
            h1_only = True                                  # C93b / C93c: the 10k pause, the inventory stop and the ring stops lifted inside the H1 sleeve only
        sz = self.tm_sizes()
        glide = now >= time.mktime(time.strptime(TM_GLIDE_START, "%Y-%m-%d %H:%M:%S"))
        cap_cost = 0.0 if glide else self.tm_cap(now)       # Piero 22:15 (C82 item 2): base + max(0, free - BUFFER)
        e_min = self.tm_e_min()                             # (item 1): 2c above BUFFER, 4c below
        profile = self.tm_market_profile(now)               # (item 3): per-market clip / cap from the 24-h round-trip share
        self.tm_unfunded = []
        w_used = self.tm_writes_last_min(now)
        self.tm_wpeak = max(self.tm_wpeak, w_used)
        writes = TM_WRITES_PER_MIN - w_used
        batched = False
        existing = {}
        for o in self.open:
            rec = self.ledger.orders.get(o["id"])
            if rec and rec.get("layer") == "tm" and o["id"] not in self.ex.cancelling:
                existing[(o["ex"], o["side"])] = o
        cost_all = self.tm_cost()
        allow = max(0.0, min(cap_cost - cost_all, self.free_cash() - TM_PAUSE_FREE))   # C87b: held + resting <= the cap, and never the catcher's last 10k
        rest_used = 0.0                                     # resting TM bids kept or placed this loop, in rank order (notional)
        party = dash4.party_exposure(self)
        cands = self.tm_candidates()
        top = cands[: sz["sides"]] if sz["sides"] else cands
        rows = top + [r for r in cands[len(top):] if (r[1], r[2]) in existing]   # a resting side that still qualifies is kept while it rests (no churn)
        rows += [r for r in cands if r[1] in h1m and r not in rows]              # C93b: the H1 test markets quote whatever their rank
        if h1_only:
            rows = [r for r in rows if r[1] in h1m]
        h1_allow = max(0.0, float(H1_TEST["sleeve"]) - self.h1_cost()) if h1m else 0.0   # the sleeve less what the test holds / rests
        plan, quoted = {}, 0
        for score, ex, side, fs, best, sp in rows:
            key = (ex, side)
            cur = existing.get(key)
            if self.fm_poll_block(ex, now):
                writes -= 1 if cur is not None else 0       # a resting bid that gets pulled costs a cancel write
                continue
            f = self.fair.get(ex)
            h = [x for x in self.fair_hist.get(ex, []) if x[0] >= now - TM_NEWS_WINDOW_S]
            if not h or now - h[-1][0] >= 5:
                h.append((now, f))
                self.fair_hist[ex] = h
            move = max(v for _, v in h) - min(v for _, v in h)
            if move >= TM_NEWS_C - 1e-9:
                if self.tm_block.get(ex, 0) < now:
                    self.event("TM", f"TM {self.name(ex)}: Polymarket moved {move * 100:.1f}c in 10 min: TM orders pulled, back 5 min after it settles")
                self.tm_block[ex] = now + TM_REQUOTE_S
            if self.tm_block.get(ex, 0) > now or self.breaker.get(("dump", self.pol(ex, side)), 0) > now:
                writes -= 1 if cur is not None else 0
                continue
            held = self.book.get(ex, {}).get("pos", 0)
            if held <= -MIN_QTY if side == "yes" else held >= MIN_QTY:
                writes -= 1 if cur is not None else 0
                continue                                    # never bid against our own other side
            p = round(best + TICK, 3) if sp >= TM_IMPROVE_SPREAD else best
            if cur is not None and abs(cur["price"] - best) < 1e-9:
                p = best                                    # we are the best bid: never improve on ourselves
            ask = self.own_ask(ex, side)
            p0, own_block = p, ask is not None and p >= ask - 1e-9
            if own_block:
                p = round(ask - TICK, 3)                    # I4 / C93c item 2: a tick under our own ask on this side, never at or through it
            if p > fs - e_min + 1e-9 or p < best - 1e-9 or self.ri_edge_short(ex, p, fs) or not (MB_MIN_PX <= p <= MB_MAX_PX):
                if ex in h1m and own_block:
                    self.h1_skip_line(ex, side, now, f"H1 SKIP {self.name(ex)}: a sleeve bid {side.upper()} at {p0:.3f} would sit at / through our own ask at {ask:.3f}; "
                                                      f"a tick below ({p:.3f}) is {'behind the touch' if p < best - 1e-9 else 'without edge'}: not quoted")
                writes -= 1 if cur is not None else 0       # the pull is a cancel write
                continue                                    # no edge at the touch (a resting bid whose edge is gone is pulled)
            if ex in h1m and own_block:
                self.h1_skip_line(ex, side, now, f"H1 sleeve bid {self.name(ex)} {side.upper()} sits a tick under our own ask at {ask:.3f}: {p:.3f} (not {p0:.3f})")
            sub = f"tm:{ex}:{side}"
            clip, inv_cap = (profile.get(ex) or (sz["clip"], sz["inv"], None))[:2]
            if ex in h1m:
                clip = int(H1_TEST["clip"])                 # C93b: ~600 a quote inside the sleeve
            if cur is not None and ex not in h1m and rest_used + cur["qty"] * cur["price"] > allow + 1e-6:
                writes -= 1                                 # C87b: over the allowance: this resting bid is dropped (a cancel write)
                continue
            if cur is not None:
                at_touch = cur["price"] >= best - (TM_REPEG_TICKS - 1) * TICK - 1e-9 and cur["price"] <= p + 1e-9
                behind = int(round((best - cur["price"]) / TICK))
                gtc_keep = self.h1_on(ex) and cur["price"] <= p + 1e-9 and fs - cur["price"] >= e_min - 1e-9 and self.h1_keep("tm", ex, side, behind, now)   # H1: keep the place up to 30 min at >= 2 ticks behind
                if (at_touch and fs - cur["price"] >= e_min - 1e-9) or gtc_keep or writes < 2:   # queue priority kept; or no write budget for a re-peg
                    plan.setdefault(ex, []).append(dict(side=side, px=cur["price"], qty=int(cur["qty"]), sub=sub, ttl=H1_TTL_S if self.h1_on(ex) else TM_TTL_S))
                    rest_used += cur["qty"] * cur["price"]
                else:
                    plan.setdefault(ex, []).append(dict(side=side, px=p, qty=int(cur["qty"]), sub=sub, ttl=H1_TTL_S if self.h1_on(ex) else TM_TTL_S))
                    rest_used += cur["qty"] * p
                    writes -= 2
                quoted += 1
                continue
            if self.tm_inventory(ex, side) >= inv_cap - 1e-9 or (cost_all >= cap_cost - 1e-6 and ex not in h1m):
                if fs - p >= ROTATION_FUND_EDGE - 1e-9 and cost_all >= cap_cost - 1e-6:
                    self.tm_unfunded.append((fs - p, clip * p, p))   # (item 4): a >= 8c entry the rotation may fund
                continue                                    # at a cap: only the exit side quotes
            if party.get(self.pol(ex, side), 0.0) >= DUMP_PARTY_CAP:
                continue
            q = int(min(clip, inv_cap - self.tm_inventory(ex, side)))   # never past the per-market cap (C82: 1,750 held on a 1.5k cap seen in the pilot)
            room = self.race_room(ex, side, fs)
            q = min(q, int(room / p)) if room > 0 else 0
            if ex in h1m:
                q = min(q, int(h1_allow / p))               # C93b: inside the H1 test the sleeve is the only cash limit
            else:
                q = min(q, int((cap_cost - cost_all) / p))
                q = min(q, int(max(0.0, allow - rest_used) / p))   # C87b: resting + held within the cap and free - 10k (Piero 21:37: the catcher's last 10k)
            depth = self.tm_touch_depth(ex, side, now)
            if depth is not None:
                q = min(q, int(TM_DEPTH_SHARE * depth))
            if q < TM_MIN_QTY:
                continue
            if not batched:
                if writes < 1:
                    continue                                # the write budget is spent: new sides wait
                writes -= 1
                batched = True                              # placements of one loop batch into one write
            plan.setdefault(ex, []).append(dict(side=side, px=p, qty=q, sub=sub, ttl=H1_TTL_S if self.h1_on(ex) else TM_TTL_S))   # H1 arm A: GTC (no 20-min expiry)
            if ex in h1m:
                h1_allow -= q * p
            cost_all += q * p
            rest_used += q * p
            quoted += 1
        self.tm_cov = dict(quoted=quoted, available=len(cands), top=len(top), writes_left=writes, writes_used=w_used, resting=round(rest_used), allowance=round(allow))
        return plan

    def track_tm(self, now):
        """TM lots -> exits at fill + 1 tick (fill + 1c after 10 min unfilled), never below entry + 0.5c; a lot >= 8c under fair is
        held while cash is idle (day 11 §1.2); a sold-out tranche is a round trip."""
        tr, self.tm_view = self.ledger.tranches, {}
        live = []
        for ex, books in list(self.ledger.subbooks.items()):
            for sub, q in list(books.items()):
                if not sub.startswith("tm:") or abs(q) < MIN_QTY:
                    continue
                side = sub.split(":")[2]
                if (q > 0) != (side == "yes"):
                    continue                                # wrong side (an exit oversold): the audit's business
                t = tr.get(sub)
                if t is None or t.get("state") != "open":
                    t = tr[sub] = dict(ex=ex, side=side, kind="tm", t=stamp(), ts=now, state="open")
                since = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(t.get("ts", now) - 300))
                avg = self.ledger.tranche_avg(sub, "tm", ex=ex, side=side, since=since) or t.get("avg")
                if avg is None:
                    continue
                t["avg"] = round(avg, 5)
                live.append((sub, t, ex, q, avg, self.side_fair(ex, side)))
        held = self.hold_when_idle(live, now, merge=True) if RULES_V2 else set()
        for sub, t, ex, q, avg, fs in live:
            if sub in held or self.release_resting(ex, sub):   # C90: a release ask already covers the lot -> no second exit
                if not t.get("held"):
                    t["held"] = now
                    self.event("TM", f"TM {self.name(ex)}: {abs(q):,.0f} {t['side'].upper()} at {avg:.3f} is {(fs - avg) * 100:.1f}c under fair and cash is idle: held, not offered")
                continue
            if t.get("held"):
                t["held_s"] = float(t.get("held_s") or 0.0) + max(0.0, now - float(t.pop("held")))
            age = now - t.get("ts", now) - float(t.get("held_s") or 0.0)
            pxs = self.px.get(ex)
            ask_s = bots.side_book(pxs[0], pxs[1], t["side"])[1] if pxs and pxs[0] is not None and pxs[1] is not None else None
            floor_ = max(avg, float(t.get("last_px") or 0.0), max((l[1] for l in self.lots(ex, t["side"], sub) if l[2] == "engine" and l[0] > 0), default=0.0)) + RULE0_MARGIN
            if ask_s is not None:                           # turnover_plan §2.4: the exit joins the touch ask early (queue), never below any held lot's entry + 0.5c (Piero 15:35 item 2)
                px = max(ask_s, floor_)
            else:
                px = max(avg + (TM_EXIT_FIRST if age < TM_EXIT_STEP_S else TM_EXIT_LATE), floor_)
            px = bots.tick_up(round(max(px, floor_), 6))
            for o in self.open:                             # C84c: a resting exit within a tick of the target keeps its queue place (no re-peg write)
                rec = self.ledger.orders.get(o["id"])
                if rec and rec.get("layer") == "tmexit" and rec.get("sub") == sub and o["id"] not in self.ex.cancelling:
                    cur = round(1 - o["price"], 3)
                    if abs(cur - px) <= TICK + 1e-9 and cur >= floor_ - 1e-9:
                        px = cur
                    break
            self.tm_view.setdefault(ex, []).append(dict(sub=sub, side=t["side"], px=min(px, 0.99), avg=avg, age_min=round(age / 60, 1)))
        for sub, t in tr.items():
            if t.get("kind") == "tm" and t.get("state") in ("open", "closed") and not t.get("closed_ts") \
                    and abs(self.ledger.subbooks.get(t["ex"], {}).get(sub, 0)) < MIN_QTY:
                t.update(state="closed", closed_ts=now, hold_min=round((now - t.get("ts", now)) / 60, 1))
                st = self.controls.d.setdefault("tm_stats", dict(fills=0, buys=0, sells=0, realized=0.0, edge=0.0, round_trips=0, since=stamp()))
                st["round_trips"] = int(st.get("round_trips") or 0) + 1
                self.event("TM", f"TM {self.name(t['ex'])}: {sub} sold after {t['hold_min']:.0f} min (round trip done)")

    def tm_fill_events(self, fills):
        c = self.controls
        st = c.d.setdefault("tm_stats", dict(fills=0, buys=0, sells=0, realized=0.0, edge=0.0, round_trips=0, since=stamp()))
        changed = False
        for f in fills:
            if f["layer"] not in ("tm", "tmexit"):
                continue
            changed = True
            fv = self.side_fair(f["ex"], f["side"])
            edge = 0.0 if fv is None else (fv - f["price"]) * f["qty"]
            st["fills"] += 1
            st["buys" if f["layer"] == "tm" else "sells"] = int(st.get("buys" if f["layer"] == "tm" else "sells") or 0) + 1
            st["realized"] += f.get("realized") or 0.0
            if f["layer"] == "tm":
                st["edge"] += edge
            self.tm_fills.append(dict(t=time.time(), ex=f["ex"], side=f["side"], px=f["price"], qty=f["qty"], layer=f["layer"]))
            if f["layer"] == "tm":
                tr_ = self.ledger.tranches.get(f"tm:{f['ex']}:{f['side']}")
                if tr_ is not None and tr_.get("state") == "open":
                    tr_["last_px"] = max(float(tr_.get("last_px") or 0.0), f["price"])   # the newest / highest lot entry: the exit floor
            msg = (f"TM {'bought' if f['layer'] == 'tm' else 'sold (closing buy)'} {f['qty']:,.0f} {f['side'].upper()} on {self.name(f['ex'])} at {f['price']:.3f}"
                   + ("" if fv is None else f" (fair {fv:.3f}, edge {edge:+,.0f})") + ("" if f["layer"] == "tm" else f", realized {(f.get('realized') or 0):+,.0f}"))
            self.event("TM", msg)
            self.night.line(msg)
        if changed:
            with c.lock:
                c.d["tm_stats"] = st
                c.save()

    def tm_markout(self, horizon_s):
        now = time.time(); vals = []
        for f in self.tm_fills:
            if f["layer"] != "tm" or now - f["t"] < horizon_s:
                continue
            fh = self.fair_at(f["ex"], f["t"] + horizon_s)
            if fh is None:
                continue
            fs = fh if f["side"] == "yes" else 1 - fh
            vals.append(fs - f["px"])
        return dict(n=len(vals), avg=(sum(vals) / len(vals)) if vals else 0.0)

    def tm_gate(self, now):
        """Day 11 §2 gates: realized >= 0, 30-min mark-out >= 0 over >= 50 fills, round-trip share (reported), writes within
        budget, no I2 trims."""
        st = self.controls.d.get("tm_stats") or {}
        mo = self.tm_markout(1800)
        trims = self.ex.trims.get("tm", 0) + self.ex.trims.get("tmexit", 0)
        buys = int(st.get("buys") or 0)
        ok = (float(st.get("realized") or 0) >= 0 and mo["n"] >= TM_GATE_MIN_FILLS and mo["avg"] >= 0 and trims == 0 and self.tm_wpeak <= TM_WRITES_PER_MIN)
        why = (f"fills {st.get('fills', 0)} ({buys} buys, {st.get('sells', 0)} sells), realized {float(st.get('realized') or 0):+,.0f}, 30-min mark-out {mo['avg'] * 100:+.2f}c over {mo['n']} "
               f"(needs >= {TM_GATE_MIN_FILLS}), round trips {st.get('round_trips', 0)} ({(st.get('round_trips', 0) / buys) if buys else 0:.0%} of buys), I2 trims {trims}, peak writes/min {self.tm_wpeak} of {TM_WRITES_PER_MIN}")
        return dict(ok=ok, why=why)

    def tm_view_stats(self):
        st = self.controls.d.get("tm_stats") or {}
        inv = {}
        for ex, books in self.ledger.subbooks.items():
            for sub, q in books.items():
                if sub.startswith("tm:") and abs(q) >= 1:
                    inv[sub] = q
        return dict(fills=st.get("fills", 0), buys=st.get("buys", 0), sells=st.get("sells", 0), realized=round(float(st.get("realized") or 0), 2), edge=round(float(st.get("edge") or 0), 2),
                    round_trips=st.get("round_trips", 0), inventory=inv, cost=round(self.tm_cost(), 2), mo30=self.tm_markout(1800), cov=self.tm_cov,
                    writes_last_min=self.tm_writes_last_min(time.time()), on=self.controls.on("tm"), state=self.controls.state("tm"))

    ROTATION_SKIP = ("manual", "pairset:", "recycle:", "sleeve:", "wall:", "swing:", "fm:", "tm:", "sm:")   # Piero 22:15: never manual, wall, sleeve or pair-set lots; swing / fm / recycle / tm / sm have their own exits (the TM's and SM's already sit at the touch ask)

    ROTATION_SKIP = ("manual", "pairset:", "sleeve:", "wall:", "swing:")   # C89: Piero's position orders and pair sets only; every trading lot can be sold
    OWN_EXIT_SUBS = ("dump:", "tm:", "sm:", "fm:", "touch:", "scalp:", "longshot:", "rot:", "fmm:")   # subs whose layer rests its own exit while the lot is not held

    def trading_cost(self):
        """C89: the TRADING book at cost — every engine lot that is not core (RI, walls, manual, pair sets apart)."""
        return sum(cost for ex, side, sub, q, avg, fs, cost in self.engine_lots() if (str(ex), sub) not in self.core_set)

    def trading_inv_gate(self, now):
        """C89 (Piero 16:55 item 3): no new trading buys (TM, catcher, SM) while trading inventory at cost > 30k. True = stopped; logged on change."""
        tc = self.trading_cost()
        stop = tc > TRADING_INV_CAP
        if stop != getattr(self, "trading_inv_stopped", None):
            if self.trading_inv_stopped is not None or stop:
                msg = (f"TRADING INVENTORY {tc:,.0f} at cost > {TRADING_INV_CAP:,}: no new TM / catcher / SM buys until it is under" if stop
                       else f"TRADING INVENTORY {tc:,.0f} at cost <= {TRADING_INV_CAP:,}: new buys allowed again")
                self.event("CASH", msg); self.night.line(msg)
            self.trading_inv_stopped = stop
        return stop

    def rotation_plan(self, cash, now):
        """C93 (Piero 15:30 Oct 12). Item 1, HOLD THE FAVOURITES: no seller touches a lot with >= 6% return left to settlement (the
        C89 cash-target seller and the H3 swap seller are gone; the layers' own round-trip exits at >= entry + 0.5c and core's rule
        stay). Item 2, RELEASE THE NO-EDGE LOTS: every non-core lot (RI-GOV-D and the RI reservoir excluded) with < 6% return left at
        its sale price, or on a side with fair < 0.85 and < 3c of edge left, gets ONE GTC ask at max(lot average + 0.5c, touch ask).
        Core lots that left core (swap / 1c / gain lock) sell at the touch as before. Item 3: an ask keeps its place (the engine
        defers any re-peg 10 min); POSTED is logged here, FILLED from SIG's fills only. The 03:15 H1 shortfall sale (item 4) adds
        asks at the best bid from the lowest-return trading lots, capped by the edge it gives up."""
        if self.fills_blind:
            return {}
        forced = set(((self.controls.d.get("core") or {}).get("swap_out") or {}).keys())
        shortfall = self.h1_shortfall_plan(now)              # {ex: shares} at the bid, once, on the test day at 03:15
        rot_left = {str(k): int(v) for k, v in (((self.controls.d.get("rotation") or {}).get("sell_left")) or {}).items() if int(v) >= MIN_QTY}   # C94: the coin-flip rotation's sales (Piero 20:30 Oct 13)
        fund_left = self.fund_sync()                         # C97 item 1: the fund sale (its cash is not the rotation's)
        conv = self.conv_select(now) if self.wd_after("conv_from", now) else set()   # C103: the convergence sales sized to the reserve ramp
        for ex_ in fund_left:
            rot_left.pop(ex_, None)
        plan, listing = {}, []
        for ex, b in self.book.items():
            q = b["pos"]
            if not q or ex not in self.owner or self.owner[ex].kind != "market" or self.ex.is_frozen(ex) or ex in self.manual_ex or (str(ex) in dash5.RI_EX and str(ex) not in rot_left) or (ex in self.reservoirs() and str(ex) not in rot_left and str(ex) not in fund_left):
                continue                                    # Piero's manual market (RI-GOV-D), RI Senate R (his reservoir) and any reservoir market stay out (C94: unless he named it for the rotation; its reservoir subs still stay)
            res_subs = self.reservoir_subs(ex) if ex in self.reservoirs() else set()
            allow_all = (str(ex) in rot_left and str(ex) in (ROTATION.get("sell_all_subs") or ())) or (str(ex) in fund_left and str(ex) in (FUND.get("sell_all_subs") or ()))   # C95 / C97: the whole holding (swing sub included)
            side = "yes" if q > 0 else "no"
            if SELL_HOLD.get(str(ex)) == side:
                continue                                    # C100 item 1: held out of every sale rule (R-side, the party cap)
            bid, ask = bots.side_book(*self.px.get(ex, (None, None, None))[:2], side)[:2]
            fs = self.side_fair(ex, side)
            if ask is None or fs is None:
                continue
            split, why, px_sale, anc_wait = {}, None, None, False
            subs_iter = list(self.ledger.subbooks.get(ex, {}).items())
            if str(ex) in rot_left or str(ex) in fund_left:   # C95b: a funding sale lists its lots lowest floor first, so the one-price-per-loop rule never starves the touch-priced lots
                def _avg(kv):
                    t_ = self.ledger.tranches.get(kv[0]) or {}
                    return float(t_.get("avg") or ((self.book.get(ex) or {}).get("row") or {}).get("avgCost") or 0)
                subs_iter.sort(key=_avg)
            fresh_inv = now - self.inv_new_t.get(str(ex), 0.0) < 600   # C95c: maybe Piero's own trade, its fill record still coming
            ev_fl = min(0.99, bots.tick_up(round(fs / (1 + SELL_EV_MAX), 6))) if SELL_EV_MAX < 1 else 0.0   # C101 item C: no sale above EV/$ 5% (price >= fair / 1.05)
            anc = self.sale_anchor.get(str(ex))            # C100 item 3: every sale rule keeps its place (was fund / carry only)
            if anc is not None and anc.get("side") != side:
                anc = None; self.sale_anchor.pop(str(ex), None)
            if anc is not None:                              # Piero 01:05 item 4: re-peg only after the others' best ask sat below ours >= 5 min
                oa = self.fmm_others_ask(str(ex), side)
                if oa is not None and oa < anc["px"] - 1e-9:
                    anc.setdefault("below", now)
                    if now - anc["below"] >= SALE_REPEG_S:
                        old_px = anc["px"]
                        anc.update(px=max(round(oa, 3), ev_fl), t=now); anc.pop("below", None)   # C101: never under fair / 1.05
                        self.sale_stats[str(ex)]["repegs"] += 1
                        msg = f"SALE REPEG {self.name(ex)} {side.upper()}: the best ask sat at {oa:.3f} < our {old_px:.3f} for {SALE_REPEG_S // 60} min: down to max(lot avg + 0.5c, {oa:.3f})"
                        self.event("RELEASE", msg); self.night.line(msg)
                else:
                    anc.pop("below", None)
            bidsell = (NIGHT_SELL.get("on") and NIGHT_SELL["since"] <= time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(now)) < NIGHT_SELL["until"]
                       and self.fmm_mode(now) == "live" and self.fmm_free() < float(NIGHT_SELL["fmm_free_target"]) and self.night_sold() < float(NIGHT_SELL["cap"]))
            E_ = self.election
            need = self.el_recycle_need() if (E_ is not None and self.el_mode_cached == "live" and E_.strict_for(str(ex), side)) else 0.0   # C102 item 8
            cbs = (not bidsell and self.carry_bidsell_on(now) and str(ex) not in (CARRY_BIDSELL.get("exclude") or ())
                   and (self.carry_ev_held(ex, side) if self.carry_ev_held(ex, side) is not None else 1.0) <= CARRY_BIDSELL["ev_max"] + 1e-9)   # C100 item 4
            for sub, v in subs_iter:
                if sub == "inventory" and fresh_inv:
                    continue
                if v * q <= 0 or sub.startswith(("manual", "pairset:", "fmm:", "fmmlock:")) or (not allow_all and (sub.startswith(("sleeve:", "swing:")) or sub in res_subs)) or ((str(ex), sub) in self.core_set and str(ex) not in rot_left and str(ex) not in fund_left and need <= 0 and (str(ex), sub) not in conv):
                    continue                                # never a core lot (unless Piero named the market for the rotation, C94), his manual / pair / sleeve lots, or the RI reservoir
                key = f"{ex}|{sub}"
                sig_avg = float(((self.book.get(ex) or {}).get("row") or {}).get("avgCost") or 0)
                whole = abs(v) >= 0.95 * abs(q) - 1e-9
                lots = [l for l in self.lots(ex, side, sub) if l[0] > 0]
                tot = sum(l[0] for l in lots)
                if tot >= 0.95 * abs(v) - 1e-9:
                    avg = sum(l[0] * l[1] for l in lots) / tot   # C93d: the ledger's own FIFO lots (what rule 0 enforces) are the lot average
                elif sig_avg and whole:
                    avg = sig_avg                           # C93b (Piero 16:15 item 4): no lot record and the lot is the whole SIG position: SIG's average (the tranche record had mixed both sides' entries)
                else:
                    avg = (self.ledger.tranches.get(sub) or {}).get("avg") or sig_avg
                if not avg:
                    continue
                if whole and sig_avg and abs(float(avg) - sig_avg) > 0.02 and key not in self.lotavg_logged:
                    self.lotavg_logged.add(key)
                    self.event("RELEASE", f"LOT AVG CHECK {self.name(ex)} {side.upper()} {abs(v):,.0f} ({sub}): the ledger's lots average {float(avg):.3f}, SIG's position average {sig_avg:.3f} "
                                          f"(SIG averages every buy and lets a sale reduce the quantity at that average; the lots are what rule 0 holds): the ask floors at {float(avg) + RULE0_MARGIN:.3f}")
                floor_ = bots.tick_up(round(float(avg) + RULE0_MARGIN, 6))
                if sub == "inventory" and sig_avg:
                    floor_ = max(floor_, bots.tick_up(round(sig_avg + RULE0_MARGIN, 6)))   # C99e: 'inventory' has no lots of its own (its tranche key is shared): never under SIG's position average + 0.5c (Delaware 02:2x)
                if need > 0 and bid is not None and bid >= max(float(ELECTION["recycle_bid"]), floor_) - 1e-9:
                    px, reason = bid, "recycle at the bid"   # C102 item 8 (C7): wave recycling of a strict-called winner (core included)
                elif (str(ex), sub) in conv:
                    px, reason = self.sale_price(ex, side, max(ask, floor_), floor_, bid, "convergence", anc, False)   # C103: GTC at max(lot avg + 0.5c, touch), >= fair / 1.05
                elif sub.startswith("carry:"):
                    if str(ex) not in self.carry_sell and not cbs:
                        continue                            # C98: a carry lot is held to settlement unless the carry rule sells it
                    if str(ex) not in self.carry_sell:        # C100 item 4: EV/$ held <= 8% while FMM free < 15k: at the bid or not at all
                        sig_fl = max(floor_, bots.tick_up(round(sig_avg + RULE0_MARGIN, 6))) if sig_avg else floor_
                        if bid is None or bid < sig_fl - 1e-9:
                            continue
                        px, reason = bid, "carry sale at the bid"
                    else:
                        px, reason = max(ask, floor_), "carry sale"
                        px, reason = self.sale_price(ex, side, px, floor_, bid, reason, anc, bidsell or cbs)
                        if px is None:
                            anc_wait = anc is not None
                            continue
                elif str(ex) in fund_left:
                    if sub.startswith("rot:"):
                        continue                            # C97 item 1: rotation lots are not the fund's
                    px, reason = max(ask, floor_), "fund sale"   # C97 item 1 (Piero 15:55 Oct 17): every lot that clears rule 0, core included
                    px, reason = self.sale_price(ex, side, px, floor_, bid, reason, anc, bidsell)
                    if px is None:
                        anc_wait = anc is not None
                        continue
                elif str(ex) in rot_left and not sub.startswith("rot:"):
                    px, reason = self.sale_price(ex, side, max(ask, floor_), floor_, bid, "rotation sale", anc, False)   # C94 item 1; C100: keeps its place
                    if px is None:
                        anc_wait = anc is not None
                        continue
                elif key in forced:
                    px, reason = self.sale_price(ex, side, max(ask, floor_), floor_, bid, "core lock", anc, False)   # C100 item 3: VA-01's core-lock ask keeps its place
                    if px is None:
                        anc_wait = anc is not None
                        continue
                elif ex in shortfall and bid is not None and bid >= floor_ - 1e-9:
                    px, reason = bid, "h1 shortfall"      # item 4: at the best bid, >= lot average + 0.5c
                else:
                    px = self.sale_price(ex, side, max(ask, floor_), floor_, bid, "no-edge", anc, False)[0]   # C101: the return left at the price the ask keeps
                    ret = (fs - px) / px if px > 0 else 1.0
                    if sub.startswith("rot:") and ret >= ROTATION["release_exempt_ret"] - 1e-9:
                        continue                            # C94 item 3: a rotation lot has its own exit; the no-edge release takes it only under 2% left
                    if ret < NOEDGE["ret"] - 1e-9 or (fs < NOEDGE["low_fair"] - 1e-9 and fs - px < NOEDGE["low_edge_c"] - 1e-9):
                        reason = "no-edge"                   # (C100 item 3: px is the kept price)
                    else:
                        continue                            # item 1: >= 6% return left -> HOLD
                if reason == "convergence" and self.wd_after("conv_no_ev_floor", now):
                    pass                                    # C110 (Piero 11:40 Oct 22 item 3): from Oct 29 the ramp overrides the 5% floor (lowest EV/$ first)
                elif reason != "h1 shortfall" and ev_fl:    # C101 item C (Piero 15:15 Oct 21): no fire sale — EV/$ held <= 5% at the sale price
                    if reason.endswith("at the bid"):
                        if px < ev_fl - 1e-9:
                            continue                        # the bid would sell above 5% EV/$: no
                    else:
                        px = max(px, ev_fl)                  # the ask rests at >= fair / 1.05
                if sub.startswith(self.OWN_EXIT_SUBS) and self.own_exit_resting(ex, sub):
                    continue                                # its own layer's exit already rests at the same price: never a second one
                take = self.rule0_allowed(ex, side, sub, px, int(abs(v)))
                if reason == "h1 shortfall":
                    take = min(take, int(shortfall[ex]))
                if reason == "rotation sale":
                    take = min(take, int(rot_left[str(ex)]))
                if reason.startswith("fund sale"):
                    take = min(take, int(fund_left[str(ex)]))
                if reason == "recycle at the bid":
                    take = min(take, int(need / max(px, 0.01)) + 1)
                elif reason.endswith("at the bid"):
                    left_ = (float(NIGHT_SELL["cap"]) - self.night_sold()) if bidsell else (float(CARRY_BIDSELL["day_cap"]) - self.carry_bid_sold_today(now))
                    take = min(take, int(max(0.0, left_) / max(px, 0.01)))
                if take >= MIN_QTY:
                    if px_sale is not None and abs(px - px_sale) > 1e-9:
                        continue                            # one price per market per loop (the other lot waits for the next)
                    px_sale, why = px, reason
                    split[sub] = take
                    if reason == "h1 shortfall":
                        shortfall[ex] -= take               # C93e: the market's shortfall is shared by its sub-books (Oct 13 03:15: two dump lots each took the whole 2,505 -> 4,891 sold)
                    if reason == "rotation sale":
                        rot_left[str(ex)] -= take           # C94: the market's target is shared by its sub-books
                    if reason.startswith("fund sale"):
                        fund_left[str(ex)] -= take          # C97: the market's fund target is shared by its sub-books
                    if reason.endswith("at the bid") and not bidsell and reason != "recycle at the bid":
                        self.carry_bid_sent(now, ex, px)     # C100 item 4: the day's 7.5k counts these orders' fills
                    if reason == "recycle at the bid":
                        need -= take * px
            if not split:
                if anc_wait and not any(str(o["ex"]) == str(ex) and (self.ledger.orders.get(o["id"]) or {}).get("layer") == "release" for o in self.open):
                    self.sale_anchor.pop(str(ex), None)      # C100: no lot clears the kept price and nothing rests: the next post starts a new anchor
                continue
            if why != "h1 shortfall" and not why.endswith("at the bid") and str(ex) not in self.sale_anchor:
                self.sale_anchor[str(ex)] = dict(px=px_sale, side=side, t=now)   # posted once: this price is kept (item 4; C100: every sale rule)
            n = sum(split.values())
            left = fs - px_sale
            plan[ex] = dict(side=side, px=px_sale, cost=round(left, 4), subs=split, why=why, tier=0, ev=round(max(left, 0.0) * n, 2))
            self.release_why[ex] = (now, why)
            key = (ex, round(px_sale, 3))                    # C100 item 3: logged when the price changes, not when a fill shrinks it
            listing.append(f"{self.name(ex)} {side.upper()} {n:,} @ {px_sale:.3f} ({(left / px_sale):.1%} left)")
            if self.rotation_logged.get(ex) == key:
                continue
            self.rotation_logged[ex] = key
            msg = (f"POSTED {why.upper()} ask {self.name(ex)} {side.upper()} {n:,} at {px_sale:.3f} ({'touch' if abs(px_sale - ask) < 1e-9 else 'lot average + 0.5c' if why != 'h1 shortfall' else 'the bid'}): "
                   f"{left / px_sale:.1%} return left to settlement ({left * 100:.1f}c), would free {n * px_sale:,.0f}; lots " + ", ".join(f"{k.split(':')[0]} {v:,}" for k, v in split.items()))
            self.rotation_log.append((now, msg))
            self.event("RELEASE", msg)
            self.night.line(msg)
        live_rel = {str(o["ex"]) for o in self.open if (self.ledger.orders.get(o["id"]) or {}).get("layer") == "release"}
        for ex_ in [e for e in self.sale_anchor if e not in {str(k) for k in plan} and e not in live_rel]:
            self.sale_anchor.pop(ex_, None)                  # C100: sold out (or no longer for sale) and nothing rests: the old kept price goes
        if listing and not getattr(self, "noedge_listed", False):
            self.noedge_listed = True
            self.night.line(f"NO-EDGE ASKS (Piero 15:30 item 2), {len(listing)} markets, {sum(p['px'] * sum(p['subs'].values()) for p in plan.values()):,.0f} at those prices: " + "; ".join(listing))
        return plan

    def tm_migrations(self):
        """Once (C83): the touch-maker pilot on (top 60 sides, clip 500, 1.5k per market-side, 8k at cost) and the day-5 scalper off
        (the TM replaces it; its open lots keep their exits)."""
        c = self.controls
        d = c.d.setdefault("day11", {})
        if d.get("tm_pilot") or os.environ.get("SIG_TM_PILOT", "1") != "1":
            return
        with c.lock:
            c.d["layers"].setdefault("tm", dict(state="pilot", caps={}, killed=None))["state"] = "pilot"
            c.d["layers"].setdefault("scalp", dict(state="off", caps={}, killed=None))["state"] = "off"
            c.d.setdefault("tm_stats", dict(fills=0, buys=0, sells=0, realized=0.0, edge=0.0, round_trips=0, since=stamp()))
            d["tm_pilot"] = stamp()
            c.save()
        msg = (f"DAY 11 §2 (C83): touch maker pilot ON — the top {TM_SIDES_PILOT} market-sides by 24-h value printed >= 2c off fair, the cheap side only (bid <= fair - {TM_E_MIN * 100:.0f}c, "
               f"improve by a tick when the spread is >= {TM_IMPROVE_SPREAD} ticks), clip {TM_CLIP_PILOT}, <= {TM_DEPTH_SHARE:.0%} of the touch depth, {TM_INV_MARKET_PILOT:,} shares per market-side, "
               f"{TM_CAP_COST_PILOT:,} at cost; queue priority kept while at the touch or a tick behind, re-peg >= 2 ticks behind; {TM_TTL_S // 60}-min expiry; exits at fill + 0.5c then + 1c after 10 min, "
               f"never below entry + 0.5c, held while cash is idle if >= 8c under fair; pulled on a 2c / 10 min Polymarket move; no TM from a poll close - 10 min; <= {TM_WRITES_PER_MIN} writes/min; "
               f"weight 1.0 after L0. The day-5 scalper is OFF (replaced; its open lots keep their exits). Gates 06:00 / 15:00: realized >= 0, 30-min mark-out >= 0 over >= {TM_GATE_MIN_FILLS} fills, no I2 trims, writes within budget")
        self.event("TM", msg)
        self.night.line(msg)

    def c87_migrations(self):
        """Once (C87, Piero 19:56 Oct 8): the touch maker at full size (clip 1k, 3k per market-side, every side >= 2c, base cap
        30k at cost, entries only while free >= BUFFER); the spread maker on (cap 10k, regime-gated); the fair maker and the
        scalper v1 deleted (their switches off for good; the scalper's leftover shares go to 'held'; the FM's two lots are
        offered at max(entry + 0.5c, touch))."""
        from a_config import WRITES_PER_MIN
        c = self.controls
        d = c.d.setdefault("c87", {})
        if d.get("done") or os.environ.get("SIG_C87", "1") != "1":
            return
        moved = []
        for ex, books in list(self.ledger.subbooks.items()):
            for sub, q in list(books.items()):
                if sub.startswith("scalp:") and abs(q) >= 1:
                    self.ledger.move_sub(ex, sub, "inventory")
                    moved.append(f"{self.name(ex)} {abs(q):,.0f} {'YES' if q > 0 else 'NO'}")
        with c.lock:
            c.d["layers"].setdefault("tm", dict(state="full", caps={}, killed=None))["state"] = "full"
            sm = c.d["layers"].setdefault("sm", dict(state="pilot", caps={}, killed=None))
            if sm.get("state") == "off":
                sm["state"] = "pilot"
            for l in RETIRED_LAYERS:
                c.d["layers"].setdefault(l, dict(state="off", caps={}, killed=None))["state"] = "off"
            d.update(done=stamp(), scalp_moved=moved)
            c.save()
        msg = (f"C87 (Piero 19:56): touch maker FULL SIZE — clip {TM_CLIP:,}, {TM_INV_MARKET:,} per market-side, every side >= {TM_E_MIN * 100:.0f}c cheap vs fair (no top-60), "
               f"base cap {TM_CAP_COST:,} at cost (+ free above BUFFER), <= {TM_WRITES_PER_MIN} writes/min; entries pause while free < BUFFER (resume at BUFFER + {TM_BUFFER_RESUME:,}) and stop "
               f"60 min before each ring ({', '.join(a + '-' + b for a, b in TM_ENTRY_STOPS)}); exits always run. Spread maker ON: cap {SM_CAP_COST_PILOT:,}, <= {SM_WRITES_PER_MIN} writes/min, "
               f"new quotes only in BOUNCE markets (live regime every minute), exits >= entry + 0.5c elsewhere; a market < {SM_LOSS_MARKET_DAY:+.0f} on the SIG day is excluded for the day, "
               f"rolling 24 h < {SM_LOSS_24H:+.0f} -> exits only; nightly {SM_TUNE_AT} re-tune applied only if the replay beats the current settings. Fair maker and scalper v1 DELETED "
               f"(switches off for good, code in releases/archive/); the FM's lots offered at max(entry + 0.5c, touch); the scalper's leftover to held: {', '.join(moved) or 'none'}. "
               f"Write budget: TM {TM_WRITES_PER_MIN}, SM {SM_WRITES_PER_MIN}, fast path {FAST_WRITES_RESERVE} reserved, total <= {WRITES_PER_MIN}/min. Catcher unchanged.")
        self.event("CONTROL", msg)
        self.night.line(msg)

    # ------------------------------------------------------------------------------------------------ turnover_plan §1: the spread maker (SM)
    def sm_pilot(self):
        return self.controls.state("sm") != "full"

    def sm_sizes(self):
        p = self.sm_pilot()
        return dict(clip=SM_CLIP_PILOT if p else SM_CLIP, cap_sh=SM_CAP_SH_PILOT if p else SM_CAP_SH, cap=SM_CAP_COST_PILOT if p else SM_CAP_COST, n=SM_MARKETS_PILOT if p else SM_MARKETS)

    def sm_universe_calc(self, now, hours=24.0):
        """The bounce ranking from our own tape (data/intel/trades): a bounce = consecutive price runs p -> p +/- 1 tick -> p, matched
        shares = the two legs; score = bounces x matched. Never RI, an FM target, a manual market or a fair outside 5-95%."""
        cut = now - hours * 3600
        rows = collections.defaultdict(list)
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
                        rows[str(r["ex"])].append((r["ts"], round(float(r["p"]), 3), float(r.get("q") or 0)))
        fm_ex = {str(t["ex"]) for t in self.fm_targets()}
        stats = {}
        for ex, pr in rows.items():
            if ex in fm_ex or ex in self.manual_ex or (self.meta.get(ex) or {}).get("race") in RI_RACES:
                continue
            f = self.fair.get(ex)
            if f is None or not (SM_FAIR_LO <= f <= SM_FAIR_HI):
                continue
            pr.sort()
            runs = []
            for t, p, q in pr:
                if runs and abs(runs[-1][0] - p) < 1e-9:
                    runs[-1][1] += q
                else:
                    runs.append([p, q])
            n, matched = 0, 0.0
            for i in range(len(runs) - 2):
                if abs(abs(runs[i + 1][0] - runs[i][0]) - TICK) < 1e-9 and abs(runs[i + 2][0] - runs[i][0]) < 1e-9:
                    n += 1
                    matched += runs[i + 1][1] + runs[i + 2][1]
            if n:
                stats[ex] = dict(bounces=n, matched=int(matched), score=n * matched)
        order = sorted(stats, key=lambda e: -stats[e]["score"])[:SM_MARKETS]
        return dict(day=time.strftime("%Y-%m-%d", time.localtime(now)), t=stamp(), markets=order, stats={e: stats[e] for e in order})

    def sm_universe(self, now):
        """The SM's markets: a control override (sm_markets) wins; else the daily bounce ranking (cached in the controls)."""
        c = self.controls
        n = self.sm_sizes()["n"]
        ov = c.d.get("sm_markets")
        if ov:
            return [str(x) for x in ov][:n]
        u = c.d.get("sm_universe") or {}
        if u.get("day") != time.strftime("%Y-%m-%d", time.localtime(now)) and self.fair:
            try:
                u = self.sm_universe_calc(now)
            except Exception as err:
                self.event("ERR", f"SM universe: {err!r}")
                return [str(x) for x in u.get("markets", [])][:n]
            with c.lock:
                c.d["sm_universe"] = u
                c.save()
            top = u["markets"][:n]
            self.event("SM", f"SM universe refreshed ({u['day']}): {len(top)} markets by 24-h bounces x matched shares — "
                             + ", ".join(f"{self.name(e)} {u['stats'][e]['bounces']}/{u['stats'][e]['matched']:,}" for e in top[:8]) + (" …" if len(top) > 8 else ""))
        return [str(x) for x in u.get("markets", [])][:n]

    def sm_inventory(self, ex):
        return float(self.ledger.subbooks.get(ex, {}).get(f"sm:{ex}", 0.0))

    def sm_avg(self, ex):
        return (self.ledger.tranches.get(f"sm:{ex}") or {}).get("avg")

    def sm_cost(self):
        """SM inventory at cost: |shares| x the held side's average entry (the tranche avg is in that side's price)."""
        out = 0.0
        for ex, books in self.ledger.subbooks.items():
            q = books.get(f"sm:{ex}", 0.0)
            if abs(q) >= 1:
                avg = self.sm_avg(ex) or self.sig_mark(ex, "yes" if q > 0 else "no") or 0.5
                out += abs(q) * float(avg)
        return out

    def sm_reserve(self):
        if not self.controls.on("sm") or self.ring_window(time.time()):
            return 0.0                                      # (1c) in a ring window nothing is reserved for new SM entries
        return max(0.0, min(float(self.sm_sizes()["cap"]) - self.sm_cost(), self.free_cash() - TM_PAUSE_FREE))   # C88b: never the catcher's last 10k (Piero 01:55 / 21:37)

    def sm_writes_last_min(self, now):
        secs, cancels = set(), 0
        cut = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(now - 60))
        for o in self.ledger.orders.values():
            if o.get("layer") not in ("sm", "smexit"):
                continue
            if (o.get("ts") or 0) >= now - 60:
                secs.add(int(o["ts"]))
            if o.get("status") == "cancelled" and (o.get("closed") or "") >= cut:
                cancels += 1
        return len(secs) + cancels

    def sm_touch_size(self, ex, now):
        """(shares at the best bid, shares at the best ask) — an order-book read cached 5 min, only with spare reads, else None."""
        fb = self.feed_book(ex)                             # C86 live: the feed's book, no REST read
        if fb is not None:
            return (fb[0][0][1] if fb[0] else 0.0, fb[1][0][1] if fb[1] else 0.0)
        c = self.sm_depth.get(ex)
        if not c or now - c[0] > SM_DEPTH_CACHE_S:
            if not api.spare_reads() or now - getattr(self, "boot_t", 0) < STARTUP_HOLD_S:
                return None
            try:
                bids, asks = api.orderbook(ex, depth=1)
            except Exception as err:
                self.event("ERR", f"SM depth read {self.name(ex)}: {err!r}")
                return None
            c = self.sm_depth[ex] = (now, bids, asks)
        _, bids, asks = c
        return (bids[0][1] if bids else 0.0, asks[0][1] if asks else 0.0)

    # ------------------------------------------------------------------------------------------------ C87: SM regime gate, loss stops, re-tune
    def note_print(self, ex, pid, ts, p, q, fair=None):
        """One SIG print into the 30-min ring of `ex` (deduplicated by trade id across the tape and the live feed)."""
        if p is None or ts is None:
            return
        ex = str(ex)
        ids = self.print_ids.setdefault(ex, collections.deque(maxlen=4000))
        if pid is not None:
            if pid in ids:
                return
            ids.append(pid)
        ring = self.prints.setdefault(ex, collections.deque(maxlen=4000))
        ring.append((float(ts), round(float(p), 3), float(q or 0), fair))
        if self.fmm_paper["orders"] or self.fmm_paper["asks"]:
            try:
                self.fmm_paper_print(ex, float(ts), float(p), float(q or 0))
            except Exception:
                pass
        if self.gap_paper["asks"]:
            try:
                self.gapmm_paper_print(ex, float(ts), float(p), float(q or 0))
            except Exception:
                pass

    def seed_prints(self, now, exs):
        """Once: the ring of each market from today's (and yesterday's) tape file, the last 30 min."""
        cut = now - SM_REGIME_WIN_S
        want = set(exs)
        for day in sorted({cut, now}):
            path = os.path.join(DATA, "intel", "trades", time.strftime("%Y-%m-%d", time.localtime(day)) + ".jsonl")
            if not os.path.exists(path):
                continue
            with open(path) as fh:
                fh.seek(max(0, os.path.getsize(path) - 8_000_000))   # the tail is plenty for 30 min
                for line in fh:
                    try:
                        r = json.loads(line)
                    except ValueError:
                        continue
                    if str(r.get("ex")) in want and (r.get("ts") or 0) >= cut:
                        self.note_print(r["ex"], r.get("id"), r["ts"], r.get("p"), r.get("q"), r.get("fair"))

    @staticmethod
    def classify_regime(rows, cfg=SM_REGIME):
        """mm_playbook v0 (research/stats.regimes) on one window of prints [(ts, p, q, fair)] sorted by time: SWEEP if any print is
        >= 10 ticks from the window's first; NEWS if the fair moved >= 2c; QUIET if < 5 prints; TREND if the net move >= 3 ticks
        and < 40% of runs revert; BOUNCE if >= 20 prints and >= 50% of runs revert; else MIXED. -> (regime, stats)."""
        n = len(rows)
        if n == 0:
            return "QUIET", dict(n=0, net=0.0, bshare=0.0, fmove=0.0)
        p = [r[1] for r in rows]
        runs = []
        for x in p:
            if not runs or abs(runs[-1] - x) >= 1e-9:
                runs.append(x)
        rev = sum(1 for i in range(len(runs) - 2) if abs(runs[i + 2] - runs[i]) < 1e-9 and abs(abs(runs[i + 1] - runs[i]) - TICK) < 1e-9)
        bshare = rev / max(1, len(runs) - 2)
        net = abs(p[-1] - p[0]) / TICK
        sweep = any(abs(x - p[0]) >= cfg["sweep_ticks"] * TICK - 1e-9 for x in p)
        fs = [r[3] for r in rows if r[3] is not None]
        fmove = (max(fs) - min(fs)) if fs else 0.0
        if sweep:
            reg = "SWEEP"
        elif fmove >= cfg["news_c"] - 1e-9:
            reg = "NEWS"
        elif n < cfg["quiet_prints"]:
            reg = "QUIET"
        elif net >= cfg["trend_ticks"] and bshare < cfg["trend_bounce_max"]:
            reg = "TREND"
        elif n >= cfg["bounce_prints"] and bshare >= cfg["bounce_share"]:
            reg = "BOUNCE"
        else:
            reg = "MIXED"
        return reg, dict(n=n, net=round(net, 1), bshare=round(bshare, 3), fmove=round(fmove, 4))

    def sm_regime_update(self, now, exs):
        """Every minute: the regime of each SM market from its trailing 30 min of prints (tape + live feed); changes logged."""
        if not self.prints and exs:
            try:
                self.seed_prints(now, exs)
            except Exception as err:
                self.event("ERR", f"SM regime seed: {err!r}")
        feed = getattr(self, "mkt_feed", None)
        if feed is not None:
            for ex in exs:
                for t in list((getattr(feed, "trades", {}) or {}).get(ex) or []):
                    try:
                        self.note_print(ex, t[4], t[0], t[2], t[3], self.fair_at(ex, t[0]))
                    except Exception:
                        pass
        cut = now - SM_REGIME_WIN_S
        changes = []
        for ex in exs:
            rows = sorted(r for r in self.prints.get(ex, ()) if r[0] >= cut)
            reg, st = self.classify_regime(rows)
            prev = (self.sm_regime.get(ex) or {}).get("regime")
            self.sm_regime[ex] = dict(regime=reg, t=now, **st)
            if prev is not None and (prev in SM_QUOTE_REGIMES) != (reg in SM_QUOTE_REGIMES):
                changes.append(f"{self.name(ex)} {prev} -> {reg}")
        if changes:
            self.event("SM", "SM regime: " + "; ".join(changes[:12]) + (" …" if len(changes) > 12 else "") + f" (new quotes only in {'/'.join(SM_QUOTE_REGIMES)})")

    def sm_pnl(self, ex=None, since=0.0):
        """SM P&L since `since`: realized on SM fills + the open SM inventory at the SIG mark vs its average (conservative: the open
        part counts in full)."""
        real = sum(f.get("realized") or 0.0 for f in self.fills_since(since) if f["layer"] in ("sm", "smexit") and (ex is None or f["ex"] == ex))
        unreal = 0.0
        for e, books in self.ledger.subbooks.items():
            if ex is not None and e != ex:
                continue
            q = books.get(f"sm:{e}", 0.0)
            if abs(q) < 1:
                continue
            side = "yes" if q > 0 else "no"
            avg, m = self.sm_avg(e), self.sig_mark(e, side)
            if avg and m is not None:
                unreal += abs(q) * (float(m) - float(avg))
        return real + unreal

    def sm_loss_gate(self, now, exs):
        """Piero 19:56 b: a market under -50 on the SIG day is excluded for the day; the SM's rolling 24 h under -150 -> exits only.
        -> (excluded set, exits_only bool); every change is logged with its reason."""
        c = self.controls
        day = time.strftime("%Y-%m-%d", time.gmtime(now))
        t0 = now - (now % 86400)                            # 00:00 UTC: the SIG day
        ex_d = c.d.get("sm_excluded") or {}
        if ex_d.get("day") != day:
            ex_d = dict(day=day, markets={})
        for ex in exs:
            if ex in ex_d["markets"]:
                continue
            v = self.sm_pnl(ex, t0)
            if v < SM_LOSS_MARKET_DAY:
                ex_d["markets"][ex] = round(v, 2)
                msg = f"SM {self.name(ex)} EXCLUDED for the SIG day: {v:+,.0f} today (< {SM_LOSS_MARKET_DAY:+.0f}); exits only there (>= entry + 0.5c)"
                self.event("SM", msg); self.night.line(msg)
        if ex_d != c.d.get("sm_excluded"):
            with c.lock:
                c.d["sm_excluded"] = ex_d
                c.save()
        tot = self.sm_pnl(None, now - 86400)
        only = tot < SM_LOSS_24H
        if only != self.sm_exits_only:
            if self.sm_exits_only is not None or only:
                msg = (f"SM EXITS ONLY: rolling 24 h {tot:+,.0f} < {SM_LOSS_24H:+.0f} (no new quotes; inventory exits at >= entry + 0.5c)" if only
                       else f"SM entries back: rolling 24 h {tot:+,.0f} >= {SM_LOSS_24H:+.0f}")
                self.event("SM", msg); self.night.line(msg)
            self.sm_exits_only = only
        return set(ex_d["markets"]), only

    def sm_params(self, ex, sz):
        """(clip, re-peg ticks, skew ticks) for ex: the nightly re-tune's per-market setting (bounded), else the defaults."""
        t = ((self.controls.d.get("sm_tune") or {}).get("markets") or {}).get(ex) or {}
        clip = int(min(max(int(t["clip"]), SM_TUNE_BOUNDS["clip"][0]), SM_TUNE_BOUNDS["clip"][1])) if "clip" in t else int(sz["clip"])
        repeg = int(min(max(int(t.get("repeg", SM_REPEG_TICKS)), SM_TUNE_BOUNDS["repeg"][0]), SM_TUNE_BOUNDS["repeg"][1]))
        skew = int(min(max(int(t.get("skew", 1)), SM_TUNE_BOUNDS["skew"][0]), SM_TUNE_BOUNDS["skew"][1]))
        return clip, repeg, skew

    def sm_tune_apply(self, now):
        """The nightly re-tune (tools/sm_retune.py) writes data/intel/sm_tune_proposal.json: per market the setting that beat the
        current one in the replay by >= SM_TUNE_MIN_GAIN a day on >= SM_TUNE_MIN_FILLS fills. Applied once per new file, bounded,
        logged; anything malformed is ignored (the current settings stay)."""
        path = os.path.join(DATA, "intel", "sm_tune_proposal.json")
        try:
            m = os.path.getmtime(path)
        except OSError:
            return
        if m == self.sm_tune_mtime:
            return
        self.sm_tune_mtime = m
        try:
            with open(path) as fh:
                prop = json.load(fh)
        except (OSError, ValueError) as err:
            self.event("ERR", f"SM re-tune proposal unreadable: {err!r}")
            return
        c = self.controls
        if (c.d.get("sm_tune") or {}).get("applied_id") == prop.get("id"):
            return
        cur = dict((c.d.get("sm_tune") or {}).get("markets") or {})
        changed = []
        for ex, r in (prop.get("markets") or {}).items():
            new = r.get("new") or {}
            if not new or float(r.get("gain_day") or 0) < SM_TUNE_MIN_GAIN or int(r.get("fills") or 0) < SM_TUNE_MIN_FILLS:
                continue
            ok = all(k in SM_TUNE_BOUNDS and SM_TUNE_BOUNDS[k][0] <= int(v) <= SM_TUNE_BOUNDS[k][1] for k, v in new.items())
            if ok:
                cur[str(ex)] = {k: int(v) for k, v in new.items()}
                changed.append(f"{self.name(str(ex))} {new} (+{float(r['gain_day']):.1f}/day on {int(r['fills'])} replayed fills)")
        with c.lock:
            c.d["sm_tune"] = dict(markets=cur, applied_id=prop.get("id"), t=stamp())
            c.save()
        msg = f"SM nightly re-tune {prop.get('id')}: " + ("; ".join(changed) if changed else "no setting beat the current one by enough; settings unchanged")
        self.event("SM", msg); self.night.line(msg)

    def sm_plan(self, now, cash):
        """{ex: [quote-spec]} (turnover_plan §1): on the bounce universe join the best bid and the best ask (the ask = a buy of NO
        at 1 - ask), never crossing; keep each order while it is at the touch or one tick behind, re-peg only >= 2 ticks away;
        on a fill the side is re-placed (the plan keeps the price; the fast path does it within a second); PM fair sets the skew:
        the cheap side may run to +/- cap_sh, the rich side holds at most one clip; once long the ask stays at the touch and the
        bid steps one tick lower (mirror when short); the reducing side is an exit under rule 0; clip = min(500 / 1k, 30% of
        the touch size); pulled on a 2c / 10 min PM move or a SIG print >= 5c off the touch; poll-close glide; <= 10 writes/min;
        <= 10k / 20k at cost."""
        c = self.controls
        if not c.on("sm") or c.paused or self.fills_blind:
            return {}
        if self.entry_budget(cash) <= 0:
            return {}
        sz = self.sm_sizes()
        glide = now >= time.mktime(time.strptime(SM_GLIDE_START, "%Y-%m-%d %H:%M:%S"))
        cap_sh = 0 if glide else sz["cap_sh"]
        universe = self.sm_universe(now)
        inv_exs = [ex for ex, books in self.ledger.subbooks.items() if abs(books.get(f"sm:{ex}", 0.0)) >= MIN_QTY]
        exs = list(dict.fromkeys(list(universe) + inv_exs))    # C87: inventory outside today's universe still exits
        stale = [ex for ex in exs if now - float((self.sm_regime.get(ex) or {}).get("t") or 0) >= SM_REGIME_EVERY_S]
        if stale:
            self.sm_regime_update(now, stale)               # every minute per market (a market new to the list at once)
        if now - self.sm_regime_t >= SM_REGIME_EVERY_S or getattr(self, "sm_gate_exs", None) != exs:
            self.sm_regime_t, self.sm_gate_exs = now, list(exs)
            self.sm_tune_apply(now)
            self.sm_gate_cache = self.sm_loss_gate(now, exs)
        excluded, exits_only = getattr(self, "sm_gate_cache", (set(), False))
        inv_stop = self.trading_inv_gate(now)
        w_used = self.sm_writes_last_min(now)
        self.sm_wpeak = max(self.sm_wpeak, w_used)
        writes = SM_WRITES_PER_MIN - w_used
        batched = False
        existing = {}
        for o in self.open:
            rec = self.ledger.orders.get(o["id"])
            if rec and rec.get("layer") in ("sm", "smexit") and o["id"] not in self.ex.cancelling:
                existing[(o["ex"], o["side"])] = o
        cost_all = self.sm_cost()
        plan, quoted, gated = {}, 0, collections.Counter()
        for ex in exs:
            reg = (self.sm_regime.get(ex) or {}).get("regime")
            entries_ok = ex in universe and reg in SM_QUOTE_REGIMES and ex not in excluded and not exits_only and not inv_stop   # C87 / C89 gates (C93b, Piero 16:15: the H1 sleeve is touch-maker quotes only; the SM stays exits-only)
            gated["entries" if entries_ok else ("excluded" if ex in excluded else "exits_only" if exits_only else "not_universe" if ex not in universe else f"regime_{reg}")] += 1
            clip, repeg, skew = self.sm_params(ex, sz)
            if ex not in self.owner or self.ex.is_frozen(ex) or ex in self.manual_ex or self.fm_poll_block(ex, now):
                writes -= sum(1 for sd in ("yes", "no") if (ex, sd) in existing)
                continue
            f, px = self.fair.get(ex), self.px.get(ex)
            if f is None or px is None or px[0] is None or px[1] is None or px[1] <= px[0]:
                continue
            bid, ask, last = px[0], px[1], px[2]
            h = [x for x in self.fair_hist.get(ex, []) if x[0] >= now - SM_NEWS_WINDOW_S]
            if not h or now - h[-1][0] >= 5:
                h.append((now, f))
                self.fair_hist[ex] = h
            move = max(v for _, v in h) - min(v for _, v in h)
            if move >= SM_NEWS_C - 1e-9 and self.sm_block.get(ex, 0) < now:
                self.event("SM", f"SM {self.name(ex)}: Polymarket moved {move * 100:.1f}c in 10 min: quotes pulled, back 5 min after it settles")
                self.sm_block[ex] = now + SM_REQUOTE_S
            elif move >= SM_NEWS_C - 1e-9:
                self.sm_block[ex] = max(self.sm_block[ex], now + SM_REQUOTE_S)
            mid = (bid + ask) / 2
            if last is not None and abs(last - mid) >= SM_SWEEP_C - 1e-9 and self.sm_block.get(ex, 0) < now:
                self.event("SM", f"SM {self.name(ex)}: a print at {last:.3f} is {abs(last - mid) * 100:.1f}c off the touch (a sweep): quotes pulled 10 min")
                self.sm_block[ex] = now + SM_SWEEP_HOLD_S
            if self.sm_block.get(ex, 0) > now:
                writes -= sum(1 for sd in ("yes", "no") if (ex, sd) in existing)
                continue
            inv, avg = self.sm_inventory(ex), self.sm_avg(ex)
            long_, short_ = inv >= MIN_QTY, inv <= -MIN_QTY
            bid_px, ask_px = bid, ask
            if long_:
                bid_px = round(bid - skew * TICK, 3)        # once long: the ask stays at the touch, the bid steps `skew` ticks lower (re-tuned nightly)
            if short_:
                ask_px = round(ask + skew * TICK, 3)        # (tick_up / tick_down only round to the grid)
            yes_cheap, no_cheap = bid <= f + 1e-9, ask >= f - 1e-9
            size = self.sm_touch_size(ex, now)
            q_bid = int(min(clip, SM_DEPTH_SHARE * size[0])) if size else int(clip)
            q_ask = int(min(clip, SM_DEPTH_SHARE * size[1])) if size else int(clip)
            quotes = []
            sub = f"sm:{ex}"
            ring = self.ring_window(now)                     # (1c) in a ring window only the reducing sides quote
            # the bid (buy YES): an exit while short (sells the NO at 1 - bid >= entry + 0.5c), else an entry
            if short_ and avg and self.lot_policy(ex, "no", sub, avg, self.side_fair(ex, "no")) == "exit" and not self.release_resting(ex, sub):
                hi = max(float(avg), max((l[1] for l in self.lots(ex, "no", sub) if l[2] == "engine" and l[0] > 0), default=0.0))
                p = min(bid_px, round(1 - (hi + RULE0_MARGIN), 3))   # rule 0 on every NO lot
                if MB_MIN_PX <= p < ask_px:
                    quotes.append(dict(reduce=True, held="no", sell_px=round(1 - p, 3), qty=int(min(clip, -inv)), sub=sub, ttl=SM_TTL_S, side="yes", px=p))
            elif not short_:
                room_sh = cap_sh - max(0.0, inv)
                allowed = room_sh >= SM_MIN_QTY and (yes_cheap or inv < clip - 1e-9) and cost_all + q_bid * bid_px <= sz["cap"] + 1e-6 and not glide and not ring and entries_ok
                if allowed and q_bid >= SM_MIN_QTY and MB_MIN_PX <= bid_px < ask_px:
                    quotes.append(dict(side="yes", px=bid_px, qty=int(min(q_bid, room_sh)), sub=sub, ttl=H1_TTL_S if self.h1_on(ex, "sm") else SM_TTL_S))
            # the ask (sell YES = buy NO at 1 - ask): an exit while long (>= entry + 0.5c), else an entry
            if long_ and avg and self.lot_policy(ex, "yes", sub, avg, self.side_fair(ex, "yes")) == "exit" and not self.release_resting(ex, sub):
                hi = max(float(avg), max((l[1] for l in self.lots(ex, "yes", sub) if l[2] == "engine" and l[0] > 0), default=0.0))
                p = max(ask_px, round(hi + RULE0_MARGIN, 3))   # never below any held lot's entry + 0.5c
                if p <= MB_MAX_PX and p > bid_px:
                    quotes.append(dict(reduce=True, held="yes", sell_px=p, qty=int(min(clip, inv)), sub=sub, ttl=SM_TTL_S, side="no", px=round(1 - p, 3)))
            elif not long_:
                room_sh = cap_sh - max(0.0, -inv)
                allowed = room_sh >= SM_MIN_QTY and (no_cheap or -inv < clip - 1e-9) and cost_all + q_ask * (1 - ask_px) <= sz["cap"] + 1e-6 and not glide and not ring and entries_ok
                if allowed and q_ask >= SM_MIN_QTY and ask_px <= MB_MAX_PX and ask_px > bid_px:
                    quotes.append(dict(side="no", px=round(1 - ask_px, 3), qty=int(min(q_ask, room_sh)), sub=sub, ttl=H1_TTL_S if self.h1_on(ex, "sm") else SM_TTL_S))
            # keep the queue: a resting quote within one tick of its target keeps its price (no write); >= 2 ticks away it is re-pegged (2 writes)
            out = []
            for q in quotes:
                cur = existing.get((ex, q["side"]))
                if cur is not None:
                    crossing = (cur["price"] >= ask - 1e-9) if q["side"] == "yes" else (round(1 - cur["price"], 3) <= bid + 1e-9)
                    behind = int(round((q["px"] - cur["price"]) / TICK))
                    gtc_keep = self.h1_on(ex, "sm") and not q.get("reduce") and cur["price"] <= q["px"] + 1e-9 and self.h1_keep("sm", ex, q["side"], behind, now)   # H1 arm A
                    if ((cur["price"] <= q["px"] + 1e-9 and cur["price"] >= q["px"] - (repeg - 1) * TICK - 1e-9) or gtc_keep or writes < 2) and not crossing:
                        q = dict(q, px=cur["price"], qty=int(cur["qty"]))   # at the touch or one tick behind: keep the queue place (no write)
                        if q.get("reduce"):
                            q["sell_px"] = round(1 - cur["price"], 3)        # the resting order is the complement buy; the sale price is 1 - it
                    else:
                        writes -= 2
                else:
                    if not batched:
                        if writes < 1:
                            continue
                        writes -= 1
                        batched = True
                    cost_all += q["qty"] * (q["px"] if not q.get("reduce") else 0.0)
                out.append(q)
            for sd in ("yes", "no"):                        # a resting quote the plan no longer wants costs a cancel write
                if (ex, sd) in existing and not any(q["side"] == sd for q in out):
                    writes -= 1
            if out:
                plan[ex] = out
                quoted += len(out)
        self.sm_cov = dict(quoted=quoted, markets=len(plan), writes_left=writes, writes_used=w_used, gate=dict(gated), exits_only=exits_only, excluded=sorted(excluded))
        return plan

    def sm_fill_events(self, fills):
        c = self.controls
        st = c.d.setdefault("sm_stats", dict(fills=0, entries=0, exits=0, filled_sh=0.0, rt_sh=0.0, realized=0.0, edge=0.0, round_trips=0, since=stamp()))
        changed = False
        for f in fills:
            if f["layer"] not in ("sm", "smexit"):
                continue
            changed = True
            ex = f["ex"]; sub = f"sm:{ex}"
            fv = self.side_fair(ex, f["side"])
            edge = 0.0 if fv is None else (fv - f["price"]) * f["qty"]
            st["fills"] += 1
            if f["layer"] == "sm":
                st["entries"] += 1; st["filled_sh"] += f["qty"]; st["edge"] += edge
            else:
                st["exits"] += 1; st["rt_sh"] += f["qty"]
            st["realized"] += f.get("realized") or 0.0
            self.sm_fills.append(dict(t=time.time(), ex=ex, side=f["side"], px=f["price"], qty=f["qty"], layer=f["layer"]))
            q = self.sm_inventory(ex)
            tr = self.ledger.tranches.get(sub)
            if abs(q) >= 1:
                if tr is None or tr.get("state") != "open" or tr.get("side") != ("yes" if q > 0 else "no"):
                    tr = self.ledger.tranches[sub] = dict(ex=ex, side="yes" if q > 0 else "no", kind="sm", t=stamp(), ts=time.time(), state="open", avg=f["price"])
                tr["avg"] = round(self.ledger.tranche_avg(sub, "sm", ex=ex, side=tr["side"], since=time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(tr["ts"] - 300))) or f["price"], 5)
            elif tr is not None and tr.get("state") == "open":
                tr.update(state="closed", closed_ts=time.time())
                st["round_trips"] += 1
            msg = (f"SM {'filled' if f['layer'] == 'sm' else 'closed'} {f['qty']:,.0f} {f['side'].upper()} on {self.name(ex)} at {f['price']:.3f}"
                   + ("" if fv is None else f" (fair {fv:.3f}, edge {edge:+,.0f})") + f", realized {(f.get('realized') or 0):+,.0f}; inventory {q:+,.0f} YES")
            self.event("SM", msg)
        if changed:
            with c.lock:
                c.d["sm_stats"] = st
                c.save()

    def sm_markout30(self):
        """Average 30-min mark-out vs the SIG mid of the SM's entry fills (the ledger's mo30), where known."""
        vals = [f["mo30"] for f in self.ledger.fills if f.get("layer") == "sm" and f.get("mo30") is not None and not f.get("before_engine")]
        return dict(n=len(vals), avg=(sum(vals) / len(vals)) if vals else 0.0)

    def sm_gate(self, now):
        st = self.controls.d.get("sm_stats") or {}
        filled, rt = float(st.get("filled_sh") or 0), float(st.get("rt_sh") or 0)
        share = rt / filled if filled else 0.0
        real = float(st.get("realized") or 0)
        per_rt = real / rt if rt else 0.0
        mo = self.sm_markout30()
        trims = self.ex.trims.get("sm", 0) + self.ex.trims.get("smexit", 0)
        ok = share >= SM_GATE_RT and real > 0 and per_rt >= SM_GATE_PER_RT and mo["avg"] >= SM_GATE_MO30 and trims == 0 and self.sm_wpeak <= SM_WRITES_PER_MIN
        why = (f"filled {filled:,.0f} shares, round-trip share {share:.0%} (>= {SM_GATE_RT:.0%}), realized {real:+,.0f} = {per_rt * 100:+.2f}c per round-trip share (>= {SM_GATE_PER_RT * 100:.1f}c), "
               f"30-min mark-out vs the SIG mid {mo['avg'] * 100:+.2f}c over {mo['n']} (>= {SM_GATE_MO30 * 100:+.1f}c), I2 trims {trims}, peak writes/min {self.sm_wpeak} of {SM_WRITES_PER_MIN}")
        return dict(ok=ok, why=why)

    def sm_view_stats(self):
        st = self.controls.d.get("sm_stats") or {}
        inv = {ex: books.get(f"sm:{ex}") for ex, books in self.ledger.subbooks.items() if abs(books.get(f"sm:{ex}", 0.0)) >= 1}
        return dict(stats=st, inventory=inv, cost=round(self.sm_cost(), 2), cov=self.sm_cov, mo30=self.sm_markout30(), on=self.controls.on("sm"), state=self.controls.state("sm"),
                    universe=self.sm_universe(time.time()) if self.controls.on("sm") else [])

    def fast_sm(self, ex, rec, t_ev, now):
        """On an SM fill: re-place that side at once (one batch write) while inventory allows; the other quote already rests."""
        if not self.controls.on("sm") or self.sm_block.get(ex, 0) > now:
            return 0
        plan = self.sm_plan(now, min(self.cash_hist) if self.cash_hist else 0.0).get(ex) or []
        side_filled = rec["side"]
        wants = [q for q in plan if q["side"] == side_filled]
        if not wants:
            return 0
        items = []
        for q in wants:
            px = q["px"]
            items.append((self.owner[ex].name, q["sub"], "smexit" if q.get("reduce") else "sm", (ex, q["side"], "buy", px, int(q["qty"]))))
        self.ex.place(items, ttl_s=SM_TTL_S)
        n = self.ex.step_placements()
        self.fast_orders[(ex, "sm")] = now
        self.sample("fast_fill_to_sent", time.time() - t_ev)
        if getattr(self, "fast_match_t", None):
            self.sample("fast_match_to_sent", time.time() - self.fast_match_t)   # SIG's match -> sent (docs/latency.md: the event clock can start before the fill)
        self.fast_stats["sm_refills"] += 1
        self.event("FAST", f"FAST SM refill {self.name(ex)} {side_filled.upper()}: {len(items)} quote(s), {time.time() - t_ev:.2f} s after the fill event")
        return n

    def band_credit_migration(self):
        """Once (C84b, Piero 06:35 Oct 8): the band credit starts at 0 at the deploy; the band's own sale proceeds fund its buys."""
        c = self.controls
        sw = c.d.get("swing") or {}
        band = sw.get("band")
        if not band or "credit" in band:
            return
        with c.lock:
            band.update(credit=0.0, sold_q=0.0, sold_val=0.0, bought_q=0.0, bought_val=0.0, cycles=0)
            c.d["swing"] = sw
            c.save()
        msg = (f"BAND credit starts at 0 (C84b): RI Senate R NO sold by the band adds its proceeds, buys spend it at <= the cycle's sell price - {band.get('buy_under_sell', 0.01) * 100:.0f}c; "
               f"buys beyond the credit only while free >= BUFFER; the 2k guard stops every entry; sells unchanged (rule 0)")
        self.event("SWING", msg)
        self.night.line(msg)

    def sm_migrations(self):
        """Once (C84): the spread-maker pilot on (20 markets, +/- 1k, 10k, <= 10 writes/min) and the RI band replacing the fixed swing legs."""
        c = self.controls
        d = c.d.setdefault("day11", {})
        if d.get("sm_pilot") or os.environ.get("SIG_SM_PILOT", "1") != "1":
            return
        with c.lock:
            c.d["layers"].setdefault("sm", dict(state="pilot", caps={}, killed=None))["state"] = "pilot"
            c.d.setdefault("sm_stats", dict(fills=0, entries=0, exits=0, filled_sh=0.0, rt_sh=0.0, realized=0.0, edge=0.0, round_trips=0, since=stamp()))
            sw = c.d.get("swing")
            if sw:
                sw["band"] = dict(SWING_BAND)
                c.d["swing"] = sw
            d["sm_pilot"] = stamp()
            c.save()
        msg = (f"turnover_plan §1 (C84): spread maker pilot ON — the top {SM_MARKETS_PILOT} markets by 24-h bounces x matched shares (refreshed daily; never RI, FM targets or fair outside 5-95%), "
               f"both sides at the touch, queue kept within a tick, re-peg >= 2 ticks, refilled on fill (fast path), PM fair sets the skew (cheap side to +/-{SM_CAP_SH_PILOT:,} shares, rich side <= 1 clip; long -> bid a tick lower), "
               f"clip min({SM_CLIP_PILOT}, 30% of the touch), rule 0 on the reducing side, pulled on 2c/10 min PM moves or a print >= 5c off the touch, poll-close glide, <= {SM_WRITES_PER_MIN} writes/min, <= {SM_CAP_COST_PILOT:,} at cost. "
               f"§2.1: the RI Senate R swing now trades its NO inside [{SWING_BAND['lo']:,}, {SWING_BAND['hi']:,}] shares at the touch (ask >= entry + {SWING_BAND['sell_over_entry'] * 100:.0f}c, bid >= {SWING_BAND['buy_min_edge'] * 100:.0f}c under fair, clips {SWING_BAND['clip']:,}), the fixed 0.865/0.845 legs retired. "
               f"Gates 06:00 / 15:00: round-trip share >= {SM_GATE_RT:.0%}, realized > 0 and >= {SM_GATE_PER_RT * 100:.1f}c per round-trip share, 30-min mark-out >= {SM_GATE_MO30 * 100:+.1f}c, writes in budget, no I2 trims")
        self.event("SM", msg)
        self.night.line(msg)

    def mm_migrations(self):
        c = self.controls
        d = c.d.setdefault("mm_plan", {})
        if d.get("c81") or os.environ.get("SIG_FM_PILOT", "1") != "1":   # the test package sets SIG_FM_PILOT=0: the pilot never switches itself on in a rig
            return
        with c.lock:
            c.d["layers"].setdefault("fm", dict(state="full", caps={}, killed=None))["state"] = "full"
            c.d.setdefault("fm_stats", dict(fills=0, realized=0.0, edge=0.0, since=stamp()))
            d["c81"] = stamp()
            c.save()
        msg = (f"MM PLAN (C81): fair maker ON — {len(FM_TARGETS)} markets ({', '.join(t['name'] for t in FM_TARGETS)}), quotes at fair ± e (3c), clip {FM_CLIP}, "
               f"cap {FM_CAP_SH:,} shares per market and {FM_CAP_COST:,} at cost in total, skew 0.5c per 1k, never crossing, the reducing side exits under rule 0, "
               f"pulled on a 2c / 10 min Polymarket move (re-quote 5 min after), {FM_TTL_S}-s expiry, <= {FM_WRITES_PER_MIN} writes/min, weight 1.0 after L0 before the scalper; "
               f"auto-off at realized < {FM_OFF_REALIZED:+,.0f} or a 30-min mark-out < -0.5c over >= {FM_OFF_MIN_FILLS} fills; cap 0 from Nov 2 17:00 PT and no quotes from a state's poll close - 10 min")
        self.event("FM", msg)
        self.night.line(msg)

    def day11_migrations(self):
        """Once (C81c): rules v2 — RI unfrozen (the cap is recorded on the first priced loop), BUFFER recomputed at the deploy, the rules logged."""
        c = self.controls
        d11 = c.d.setdefault("day11", {})
        self.ri_held_cap = None if d11.get("ri_held_cap") is None else float(d11["ri_held_cap"])
        if d11.get("rules_v2") or not RULES_V2:
            return
        with c.lock:
            c.d["ri_freeze"] = False
            c.d.pop("buffer", None)
            d11["rules_v2"] = stamp()
            c.save()
        msg = (f"DAY 11 §1 (C81c) rules v2: no forced sales at any cash level (the 15k rebuild line is gone; rule 0 and the ladder are the only exits); "
               f"while free >= BUFFER a catch / touch lot >= {HOLD_IDLE_GAP * 100:.0f}c under fair is held, under BUFFER the ladder offers such lots lowest-edge first and k falls "
               f"(k table {{>= B+5k: 3, >= B: 3, >= B/2: 2, else {BUDGET_K_MIN:g}}}); RI unfrozen for turnover (held capped at the deploy level, +{RI_TURNOVER:,} in flight, levels >= {RI_MIN_EDGE * 100:.0f}c under fair); "
               f"L3 / L4 levels <= {L3_BUDGET_SHARE:.0%} of the weighted entry budget")
        self.event("CASH", msg)
        self.night.line(msg)

    def day10_migrations(self):
        """Once: the RI swing config (hold part = the wall lots beyond the 15k slice) and the swing layer on."""
        c = self.controls
        d10 = c.d.setdefault("day10", {})
        if d10.get("swing"):
            return
        cfg = dict(SWING)
        ex = str(cfg["ex"])
        books = self.ledger.subbooks.get(ex, {})
        wall_total = sum(abs(books.get(s, 0.0)) for s in cfg["wall_subs"])
        cfg["hold_qty"] = max(0.0, wall_total - float(cfg["slice"]))
        cfg["stats"] = dict(sold=0.0, bought=0.0, realized=0.0, cycles=0, legs=[])
        with c.lock:
            c.d["swing"] = cfg
            c.d["layers"].setdefault("swing", dict(state="full", caps={}, killed=None))["state"] = "full"
            d10["swing"] = stamp()
            c.save()
        msg = (f"DAY 10 §3 (C79): RI swing on — slice {cfg['slice']:,} of the {wall_total:,.0f} RI Senate R wall NO (the other {cfg['hold_qty']:,.0f} stay held): "
               f"sell leg NO ask {cfg['sell_px']:.3f} while held, buy-back NO bid {cfg['buy_px']:.3f} for what was sold (only >= {cfg['min_edge'] * 100:.0f}c under fair, never more than sold); "
               f"pulled while fair YES > {cfg['max_fair_yes']} or after a {cfg['news_c'] * 100:.0f}c rise in 10 min; rule 0 on every sale. RI frozen (D1): race room 0, no new RI walls")
        self.event("SWING", msg)
        self.night.line(msg)

    def day9_migrations(self):
        """Once: log the day-9 rules (the k budget, expected-edge ranking, hysteresis, re-price make-before-break)."""
        c = self.controls
        d9 = c.d.setdefault("day9", {})
        if d9.get("ri_senate_base") is not None and float(d9["ri_senate_base"]) < 1_000:   # C74: C73 recorded 4 at startup (no prices yet)
            with c.lock:
                d9.pop("ri_senate_base", None)
                c.save()
        self.ri_senate_base = None if d9.get("ri_senate_base") is None else float(d9["ri_senate_base"])   # else recorded by record_ri_base() once prices are in
        if not d9.get("sleeve"):                             # day 9 (Piero 18:50): the sleeve layer starts on
            with c.lock:
                c.d["layers"].setdefault("sleeve", dict(state="full", caps={}, killed=None))["state"] = "full"
                d9["sleeve"] = stamp()
                c.save()
            self.event("SLEEVE", f"DAY 9: sleeve layer on (settlement holds): {', '.join(f'{t['name']} <= {t['limit']:.3f} up to {t['cap']:,}' for t in SLEEVE_TARGETS)}; "
                                 f"{SLEEVE_CAP:,} at cost in total, only cash above {SLEEVE_MIN_FREE:,} free, clips <= {SLEEVE_CLIP:,} every {SLEEVE_GAP_S // 60} min, fair >= {SLEEVE_MIN_FAIR} and edge >= {SLEEVE_MIN_EDGE * 100:.0f}c")
        if d9.get("c73"):
            return
        with c.lock:
            d9["c73"] = stamp()
            c.save()
        msg = (f"DAY 9 (C73): sell-back ladder = fill + {DUMP_LADDER_FIRST * 100:g}c, one tick lower every {DUMP_LADDER_STEP_S // 60} min, floor entry + 0.5c (rule 0); "
               f"k_max by free cash {BUDGET_K_TABLE}; RI Senate +{RI_SENATE_ADD:,} beyond the holdings recorded at the first priced loop, on levels >= 15c under fair")
        self.event("DUMP", msg)
        self.night.line(msg)
        if d9.get("c71"):
            return
        with c.lock:
            d9["c71"] = stamp()
            c.save()
        r = DUMP_REACH
        msg = (f"DAY 9 (C71): entry budget = k x free (k_max {BUDGET_K_MAX:g} from the replay, -0.5 / +0.5 live); levels ranked by (fair - price) x reach / weight "
               f"(reach L0 {r['L0']:g}, L1 {r['L1']:g}, L2 {r['L2']:g}, L3 {r['L3']:g}, L4 {r['L4']:g}), L0 first within its 8k cap, then L1-L4, scalper, fat-finger; "
               f"resting levels keep their slot while they fit (no churn); re-prices and refreshes are make-before-break; a level being cancelled is not 'resting' "
               f"for the reconcile; gaps > {DUMP_GAP_LOG_S} s on funded sides are logged with their cause")
        self.event("DUMP", msg)
        self.night.line(msg)

    def day8_migrations(self):
        """Once (flag in overrides): the cash target is clamped to the new 35k cap and the day-8 budget weights are logged."""
        c = self.controls
        d8 = c.d.setdefault("day8", {})
        if d8.get("weights_v1"):
            return
        ct = c.d.get("cash_target") or {}
        T = float(ct.get("T") or CASH_T_START)
        with c.lock:
            if T > CASH_T_MAX:
                ct["T"] = float(CASH_T_MAX)
                c.d["cash_target"] = ct
            d8["weights_v1"] = stamp()
            c.save()
        w = ENTRY_WEIGHTS
        msg = (f"DAY 8: entry budget = {ENTRY_CASH_MULT:g} x free cash above the floor, weighted: L0 {w['L0']:g}, L1 {w['L1']:g}, L2 {w['L2']:g}, "
               f"L3-L4 {w['L3']:g}, scalper {w['scalp']:g}, walls {w['wall']:g}, fat-finger {w['fatfinger']:g} (calibration: dumps reaching ref -1 / -2 / -3.5 / -5c "
               f"in the Oct 4 20:00 - Oct 5 18:15 replay 40 / 25 / 10 / 1% per dump; our own fills since C64: L0 15% of placed shares, L1 0.1%, L2-L3 0, "
               f"no weight raised); T {T:,.0f}" + (f" -> {CASH_T_MAX:,}" if T > CASH_T_MAX else "") + f" (cap {CASH_T_MAX:,}, -10k after {CASH_T_IDLE_H} idle h); "
               f"release for T: tier 1 only; the -1c rebuild exception only under {REBUILD_EXC_FREE:,} free")
        self.event("CASH", msg)
        self.night.line(msg)

    @staticmethod
    def release_tier_of(f):
        """The tier of a release fill from its own numbers: the gap between the held side's fair and the sale price."""
        if f.get("fair") is None:
            return "unknown"
        gap = f["price"] - f["fair"]                     # held fair - sale = (1 - fair_c) - (1 - price_c)
        return "tier0" if gap <= RELEASE_TIER0 + 1e-9 else "tier1" if gap <= RELEASE_TIER1 + 1e-9 else "tier2" if gap <= RELEASE_TIER2 + 1e-9 else "legacy"

    def released_today_view(self):
        """Cash the release layer freed this SIG day (since 17:00 PT, day 6 §5.3), its average cost vs fair, and the branch
        of each sale (profit / funding)."""
        start = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(dash4.sig_day_start(time.time())))
        sig, w, by = 0.0, 0.0, collections.Counter()
        rel = [f for f in self.ledger.fills if f["layer"] == "release" and f["t"] >= start]
        for f in rel:
            if f["action"] != "buy":
                continue
            sale = 1 - f["price"]                           # a complement buy at p sells the held side at 1 - p
            held_fair = None if f.get("fair") is None else 1 - f["fair"]
            sig += sale * f["qty"]
            if held_fair is not None and sale > 0:
                w += (held_fair - sale) / sale * sale * f["qty"]
            by[self.release_tier_of(f)] += round(sale * f["qty"])    # tier0 / tier1 / tier2 from the gap at the fill (restart-proof)
        return dict(sig=round(sig, 2), avg_cost=round(w / sig, 4) if sig else None, fills=len(rel),
                    realized=round(sum(f.get("realized") or 0 for f in rel), 2), vs_fair=round(-w, 2),
                    demand=getattr(self, "release_demand", None), by_branch=dict(by), since="17:00 PT")

    def cover_why(self, ex, side, why):
        if self.coverage is not None and f"{ex}|{side}" in self.coverage["reasons"]:
            self.coverage["reasons"][f"{ex}|{side}"] = why  # the specific reason replaces "being placed"

    def reserve_ok(self, ex, w):
        """Day 2 §3.2: only these may spend the opportunity reserve (down to the floor): fat-finger levels, sniper takes on
        a new market (< 30 min old), and takes >= 8c through fair."""
        if w.get("layer") in ("fatfinger", "dump", "longshot", "wall", "sleeve", "swing", "fm", "tm", "sm"):   # a manual wall is bounded by its own cash cut (>= 2k free); the sleeve by its 20k-free rule
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

    def log_leaders_day(self, now):
        """Day 6 §6: the daily board (SIG day) every 5 min, 1 read: top 25 + our daily rank -> leaders_day.json / .csv, and
        the jump detector: an account up >= 1k since the last read is matched to the tape's big events in that window
        (+-5 min), with whether we were resting there and why not -> data/leaders_jumps.json and docs/leaders/<SIG day>.md."""
        if api.DRY_RUN or now - getattr(self, "leaders_day_t", 0) < LEADERS_DAY_EVERY_S or not self.controls.on("leaders") or not api.spare_reads():
            return
        prev_t, prev = getattr(self, "leaders_day_t", 0), getattr(self, "leaders_day_prev", {})
        self.leaders_day_t = now
        try:
            dd = api.leaderboard(25, period="1d")
        except Exception as err:
            self.event("ERR", f"daily leaderboard: {str(err)[:80]}")
            return
        top = [dict(rank=r.get("rank"), name=r.get("username") or r.get("profileId"), pnl=r.get("pnl"), trades=r.get("tradesCount"))
               for r in dd.get("leaderboard", [])[:25]]
        write_atomic(os.path.join(DATA, "leaders_day.json"), json.dumps(dict(t=stamp(), my_rank=dd.get("myRank"), total=dd.get("total"), top=top)))
        path, new = os.path.join(DATA, "leaders_day.csv"), not os.path.exists(os.path.join(DATA, "leaders_day.csv"))
        with open(path, "a", newline="") as fh:
            w = csv.writer(fh)
            if new:
                w.writerow(["time", "rank", "name", "pnl", "trades"])
            for r in top:
                w.writerow([stamp(), r["rank"], r["name"], r["pnl"], r["trades"]])
        cur = {r["name"]: r for r in top if r.get("pnl") is not None}
        self.leaders_day_prev = cur
        if not prev or not prev_t:
            return
        jumps = []
        for name, r in cur.items():
            p = prev.get(name)
            if p is None or r["pnl"] - p["pnl"] < JUMP_MIN:
                continue
            gain, dn = r["pnl"] - p["pnl"], (r.get("trades") or 0) - (p.get("trades") or 0)
            evs = self.events_between(prev_t - 300, now + 300)
            likely = []
            for e in evs[:4]:
                ours = [f for f in self.fills_since(prev_t - 300) if f["ex"] == e.get("ex") and f["action"] == "buy" and f["layer"] in ("dump", "touch", "fatfinger", "scalp")]
                likely.append(dict(market=e.get("market"), who=e.get("wrong"), px=f"{e.get('p_lo')}-{e.get('p_hi')}", shares=e.get("shares"), value=round(e.get("value") or 0),
                                   caught=round(sum(f["qty"] for f in ours)), why_not=None if ours else (e.get("missed_why") or "no levels there")[:120]))
            jumps.append(dict(t=stamp(), name=name, rank=r["rank"], gain=round(gain), trades=dn, since=time.strftime("%H:%M", time.localtime(prev_t)),
                              likely=likely))
        if jumps:
            path = os.path.join(DATA, "leaders_jumps.json")
            old = read_json(path, []) or []
            write_atomic(path, json.dumps((old + jumps)[-200:]))
            os.makedirs(os.path.join(os.path.dirname(DATA), "docs", "leaders"), exist_ok=True)
            md = os.path.join(os.path.dirname(DATA), "docs", "leaders", f"{dash4.sig_day_label(dash4.sig_day_start(now)).replace(' ', '-')}.md")
            new = not os.path.exists(md)
            with open(md, "a") as fh:
                if new:
                    fh.write(f"# Leader jumps, SIG day {dash4.sig_day_label(dash4.sig_day_start(now))} (day 6 §6: inferred from the public boards and our tape, no /dmm)\n\n"
                             "| time | account | daily rank | gain | trades | likely trade (tape event +-5 min) | we caught | why not |\n|---|---|---:|---:|---:|---|---:|---|\n")
                for j in jumps:
                    if j["likely"]:
                        for lk in j["likely"]:
                            fh.write(f"| {j['t'][11:16]} | {j['name']} | {j['rank']} | {j['gain']:+,} | {j['trades']} | {lk['market']} {lk['who']} {lk['shares']:,} sh at {lk['px']} (value {lk['value']:,}) | {lk['caught']:,} | {lk['why_not'] or ''} |\n")
                    else:
                        fh.write(f"| {j['t'][11:16]} | {j['name']} | {j['rank']} | {j['gain']:+,} | {j['trades']} | no big tape event in the window (marks moving on a held book?) | | |\n")
            for j in jumps:
                self.event("LEADERS", f"jump: {j['name']} {j['gain']:+,} since {j['since']} on {j['trades']} trades; "
                                      + ("; ".join(f"{lk['market']} {lk['who']} {lk['shares']:,} sh, we caught {lk['caught']:,}" for lk in j["likely"]) or "no tape event"))

    def events_between(self, t0, t1):
        """Big tape events (data/intel/events.jsonl) with t in [t0, t1], largest value first."""
        path = os.path.join(DATA, "intel", "events.jsonl")
        out = []
        if not os.path.exists(path):
            return out
        with open(path) as fh:
            lines = fh.readlines()[-400:]
        for line in lines:
            try:
                e = json.loads(line)
            except ValueError:
                continue
            if t0 <= (e.get("t") or 0) <= t1:
                out.append(e)
        return sorted(out, key=lambda e: -(e.get("value") or 0))

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
            if bk.get("state", "held") != "held" or bk["race"] not in by_race or self.ex.admit() < 1 or bk.get("h4"):
                continue                                    # H4 locks are held to settlement
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
        fmm_live = self.fmm_mode(now) == "live" and self.controls.on("fmm")   # fmm_spec §2.4: NO+NO locks at <= 0.990 replace H4 (<= 0.96, its free-cash gate)
        for pb in self.pairs:
            if not all(self.controls.allows(ex, "pairs") for ex in pb.legs):
                continue
            for e_, side in pb.screen(self.px, self.fair, PAIR_EDGE, PAIR_THIRD_PARTY):
                if now - pb.last_shot >= PAIR_COOLDOWN_S and not any(self.ex.is_frozen(ex) for ex in pb.legs):
                    cands.append((e_, side, pb))
        h4_budget = self.h4_budget(now) if self.h4_active() and not fmm_live else 0.0
        fmm_room = self.fmm_lock_room() if fmm_live else 0.0  # Piero 21:15 item 1: cash FMM's ladders don't need, <= 30% of engine free cash in new locks
        for e_, side, pb in sorted(cands, key=lambda c: -c[0]):
            if shots >= PAIR_MAX_SHOTS or self.ex.admit() < 2 or not api.spare_reads(20):
                break
            fmm_set = False                                 # alpha_spec §4: FMM's NO sets go through fmm_nsets (every book of a race)
            h4_set = not fmm_live and self.h4_active() and side == "no" and self.h4_race_ok(pb) and e_ >= H4["edge"] - 1e-9 and h4_budget > 0
            if fmm_live and side == "no":
                continue
            pb.last_shot = now
            books, key = {}, "bids" if side == "no" else "asks"
            try:                                                                # a bad read skips this pair, not the loop
                feed_bks = {ex: self.feed_book(ex) for ex in pb.legs}           # C88b: the feed's books first (zero reads); REST only for a leg the feed lacks
                missing = [ex for ex in pb.legs if feed_bks[ex] is None]
                if missing:
                    with ThreadPoolExecutor(max_workers=len(missing)) as pool:  # every missing leg's book at once
                        for ex, bk in zip(missing, pool.map(api.orderbook, missing)):
                            feed_bks[ex] = bk
                fetched = [feed_bks[ex] for ex in pb.legs]
            except Exception as err:
                self.event("ERR", f"{pb.race} order book: {str(err)[:120]}")
                continue
            for ex, (bids, asks) in zip(pb.legs, fetched):
                lv = bids if side == "no" else asks
                books[ex] = [(p, q - own.get((ex, key, round(p, 3)), 0)) for p, q in lv if q - own.get((ex, key, round(p, 3)), 0) > 0]
            if fmm_set:
                lock_q = int(min(FMM["lock_cap_sh"] - self.fmm_race_locked(pb.race, pb.legs), fmm_room / FMM["lock_cost"]))
                orders, qty, margin = pb.size(side, books, self.fair, 1 - FMM["lock_cost"], PAIR_THIRD_PARTY, max(0, lock_q))
            elif h4_set:
                race_used = sum(bk["qty"] * (bk["legs"] - bk["margin"]) for bk in self.ledger.baskets if bk.get("h4") and bk["race"] == pb.race)
                room = min(h4_budget, H4["per_race"] - race_used)
                orders, qty, margin = pb.size(side, books, self.fair, H4["edge"], PAIR_THIRD_PARTY, int(max(0, room) / 0.96))
            else:
                orders, qty, margin = pb.size(side, books, self.fair, PAIR_EDGE, PAIR_THIRD_PARTY, PAIR_MAX_QTY)
            if not orders:
                continue
            fairs = [self.side_fair(o[0], side) for o in orders]
            if fmm_set:
                pass                                        # a D/R NO+NO set pays 1.00 whoever wins: the walked cost <= 0.990 is the whole test
            elif None in fairs:
                if margin < 0.03 - 1e-9:                    # A12.1: no fair on a leg -> require >= 3c locked
                    continue
            elif sum(f - o[3] for f, o in zip(fairs, orders)) < -1e-9:
                self.event("PAIR", f"{pb.race}: {side.upper()} set skipped: locks {margin * 100:.1f}c but EV vs fair "
                                   f"{sum(f - o[3] for f, o in zip(fairs, orders)) * 100:+.1f}c (A12.1)")
                continue
            cost = sum(o[3] * o[4] for o in orders)
            if fmm_set:
                if cost > fmm_room + 1e-6 or qty * margin < PAIR_MIN_LOCK or sum(o[3] for o in orders) > FMM["lock_cost"] + 1e-9:
                    continue
                fmm_room -= cost
                self.fmm_unread.append((time.time(), cost))  # the fill guard sees the lock's cash at once
            elif (cost > budget and not h4_set) or (h4_set and cost > h4_budget) or qty * margin < PAIR_MIN_LOCK:
                continue
            if h4_set:
                h4_budget -= cost
                self.h4_used += cost
            set_id = f"{pb.race}:{int(now)}:{uuid.uuid4().hex[:6]}"                 # unique even for two sets a second

            def recorded(results, pb=pb, side=side, qty=qty, margin=margin, n=len(orders), set_id=set_id, ts=now, h4_set=h4_set, fmm_set=fmm_set):
                ids = [str(d.get("orderId")) for _, _, ok, d in results if ok and d.get("orderId") is not None]
                if len(ids) == n or api.DRY_RUN:            # the set exists once SIG acked every leg
                    with self.ledger.lock:
                        self.ledger.baskets.append(dict(id=set_id, t=stamp(), ts=ts, race=pb.race, side=side, qty=qty,
                                                        margin=round(margin, 4), legs=n, orders=ids, repaired=False, state="held",
                                                        h4=bool(h4_set or fmm_set), fmm=bool(fmm_set)))
            if self.ex.place([(pb.name, f"pairset:{set_id}", "set", o) for o in orders], ttl_s=PAIR_TTL_S, atomic=True,
                             on_done=recorded):
                budget -= cost
                shots += 1
                self.event("PAIR", f"{pb.race}: queued buy {side.upper()} on all {len(orders)} legs x{qty:,} at "
                                   f"{' + '.join(f'{o[3]:.3f}' for o in orders)} -> locks {margin * 100:.1f}c/set "
                                   f"(~{qty * margin:,.0f} SIG once every leg fills)")
                if fmm_set:
                    msg = (f"FMM LOCK {pb.race}: NO + NO x{qty:,} at {' + '.join(f'{o[3]:.3f}' for o in orders)} = {sum(o[3] for o in orders):.3f} "
                           f"(walked, atomic) -> +{qty * margin:,.0f} at settlement, held; cost {cost:,.0f} (inside cash, outside the inventory cap)")
                    self.event("FMM", msg); self.night.line(msg)
                    self.fmm_guard(now, "lock")
        if fmm_live:
            shots += self.fmm_nsets(now, own)
        return shots, start - budget

    # ------------------------------------------------------------------ C98: the CARRY book (alpha_spec §2, Piero 16:35 Oct 17)
    def carry_lots(self):
        """{sub: (ex, side, shares, avg)} of the carry book ('carry:<ex>:<side>')."""
        out = {}
        for ex, books in list(self.ledger.subbooks.items()):
            for sub, q in list(books.items()):
                if sub.startswith("carry:") and abs(q) >= 1:
                    side = "yes" if q > 0 else "no"
                    avg = float((self.ledger.tranches.get(sub) or {}).get("avg") or 0) or float(((self.book.get(ex) or {}).get("row") or {}).get("avgCost") or 0) or float(self.sig_mark(ex, side) or 0.5)
                    out[sub] = (str(ex), side, abs(q), avg)
        return out

    def carry_bid_notional(self):
        return sum(o["qty"] * o["price"] for o in self.open if o["action"] == "buy" and (self.ledger.orders.get(o["id"]) or {}).get("layer") == "carry")

    def carry_cap(self, race):
        return float((CARRY.get("race_caps") or {}).get(race, CARRY["race_cap"]))

    def carry_ev_held(self, ex, side):
        """EV/$ held at the exit bid: side fair / side bid - 1 (None without a bid or a fair)."""
        bid = bots.side_book(*self.px.get(ex, (None, None, None))[:2], side)[0]
        fs = self.side_fair(ex, side)
        return None if not bid or fs is None else fs / bid - 1

    def carry_candidates(self, now):
        """Favourite sides (fair >= 0.75) with >= 150k shares a day where a bid at the touch, <= fair - 5c, earns EV/$ >= 11%."""
        vol = self.rot_vol48(now)
        hand = {str(o["ex"]) for o in self.open if not self.ex.owned(o["id"])}
        out = []
        for ex, b in self.owner.items():
            ex = str(ex)
            if b.kind != "market" or ex in self.manual_ex or ex in hand or self.ex.is_frozen(ex):
                continue
            f = self.fair.get(ex)
            if f is None or vol.get(ex, 0.0) / 2 < CARRY["min_daily"]:
                continue
            side = "yes" if f >= CARRY["fav_min"] else "no" if f <= 1 - CARRY["fav_min"] else None
            if side is None:
                continue
            pos_ = float((self.book.get(ex) or {}).get("pos") or 0)
            if (side == "yes" and pos_ <= -1) or (side == "no" and pos_ >= 1):
                continue                                     # C95f: never net against a held other side
            bid, ask, fs = self.rot_touch(ex, side)
            if bid is None or fs is None:
                continue
            px = bots.tick_down(min(bid, fs - CARRY["below_fair"]))
            if CARRY_FIRST.get(str(ex), (None,))[0] == side:
                px = bots.tick_down(min(px, float(CARRY_FIRST[str(ex)][1])))   # C106: RI Senate R NO at <= 0.845
            if px < 0.01:
                continue
            ev = fs / px - 1
            if ev < CARRY["buy_ev"] - 1e-9:
                continue
            out.append(dict(ex=ex, side=side, px=px, fs=fs, ev=ev, race=self.rot_race(ex)))
        return sorted(out, key=lambda c: (CARRY_FIRST.get(str(c["ex"]), (None,))[0] != c["side"], -c["ev"]))   # C106: RI Senate R NO first

    def carry_refresh(self, now, cands):
        """Every 30 min: the carry lots to sell — EV/$ held <= 6.5% (at max(lot avg + 0.5c, best ask): rule 0 holds), and, when cash is
        short, the swap rule: a lot >= 4 points of EV/$ under a waiting buy."""
        if now - self.carry_t < CARRY["refresh_s"] and self.carry_t:
            return
        self.carry_t = now
        sell = {}
        lots = []
        for sub, (ex, side, q, avg) in self.carry_lots().items():
            ev = self.carry_ev_held(ex, side)
            if ev is None:
                continue
            lots.append((ev, ex, side, q, avg))
            if ev <= CARRY["sell_ev"] + 1e-9:
                sell[ex] = f"EV/$ held {ev * 100:.1f}% <= {CARRY['sell_ev'] * 100:.1f}%"
        room = self.carry_room()
        if cands and room < CARRY["min_clip"] * cands[0]["px"]:
            best = cands[0]["ev"]
            for ev, ex, side, q, avg in sorted(lots):
                if ex not in sell and best >= ev + CARRY["swap_pts"] - 1e-9:
                    sell[ex] = f"swap: {self.name(cands[0]['ex'])} {cands[0]['side'].upper()} {best * 100:.1f}% waits, this lot {ev * 100:.1f}%"
                    break
        new = {ex: why for ex, why in sell.items() if ex not in self.carry_sell}
        self.carry_sell = sell
        for ex, why in new.items():
            msg = f"CARRY SELL {self.name(ex)}: {why}; GTC ask at max(lot avg + 0.5c, best ask)"
            self.event("CARRY", msg); self.night.line(msg)

    def carry_room(self, signed=False):
        """Cash for new carry bids: the other layers' free cash (after FMM's first 10k) less every resting engine entry bid but FMM's
        (signed: below 0 when the resting bids are more than that cash)."""
        committed = sum(o["qty"] * o["price"] for o in self.open if o["action"] == "buy" and (self.ledger.orders.get(o["id"]) or {}).get("layer") not in self.REDUCING
                        and (self.ledger.orders.get(o["id"]) or {}).get("layer") not in self.FMM_LAYERS and self.ex.owned(o["id"]))
        return self.free_cash() - committed if signed else max(0.0, self.free_cash() - committed)

    def carry_plan(self, now):
        """{ex: [want-spec]}: one resting bid per candidate book at the touch (<= fair - 5c), sized to the race cap (12k at cost; RI Senate
        30k) and the cash left; a resting bid keeps its place and size."""
        if not CARRY.get("on") or self.controls.state("carry") == "off" or self.controls.paused or self.fills_blind:
            return {}
        cands = self.carry_candidates(now)
        self.carry_refresh(now, cands)
        if self.wd_after("carry_bids_stop", now):
            return {}                                        # C103 (C12): carry stops bidding Oct 30; its lots join the convergence sales
        race_cost = collections.Counter()
        for sub, (ex, side, q, avg) in self.carry_lots().items():
            race_cost[self.rot_race(ex)] += q * avg
        resting = {}
        for o in self.open:
            rec = self.ledger.orders.get(o["id"]) or {}
            if o["action"] == "buy" and rec.get("layer") == "carry":
                resting[(str(o["ex"]), o["side"])] = (round(o["price"], 3), o["qty"])
                race_cost[self.rot_race(o["ex"])] += o["qty"] * o["price"]
        room = self.carry_room(signed=True)
        pull = set()                                         # C99f (Piero 01:05 item 3): FMM's first 10k comes before carry bids — resting carry bids the cash
        for c in sorted(cands, key=lambda c: c["ev"]):       # no longer covers (after FMM's priority) are pulled, lowest EV/$ first (RI Senate R NO filled 08:00, FMM free -871)
            have = resting.get((c["ex"], c["side"]))
            if have and room < -1e-6:
                pull.add((c["ex"], c["side"])); room += have[0] * have[1]
        party = dict(net=self.party_view()["net"]) if ROTATION.get("party_cap") else None
        plan = {}
        for c in cands:
            ex, side, px = c["ex"], c["side"], c["px"]
            own = self.own_ask(ex, side)
            if own is not None and px >= own - 1e-9:
                continue                                     # no self-trade
            have = resting.get((ex, side))
            if have and abs(have[0] - px) < 1e-9:
                q = int(have[1])                            # keeps its place and size
                if not self.carry_party_ok(ex, side, q * px, party):
                    continue
                if (ex, side) in pull:
                    continue
            else:
                cap_left = self.carry_cap(c["race"]) - race_cost[c["race"]] + (have[0] * have[1] if have else 0.0)
                q = int(min(min(cap_left, room) / px, float(CARRY.get("max_bid_sh") or 1e12)))
                if q < CARRY["min_clip"] or not self.carry_party_ok(ex, side, q * px, party):
                    continue
                room -= q * px; race_cost[c["race"]] += q * px
            plan.setdefault(ex, []).append(dict(side=side, px=px, qty=q, sub=f"carry:{ex}:{side}", ttl=CARRY["ttl"]))
        self.carry_view = dict(cands=len(cands), bids=sum(len(v) for v in plan.values()), sell=dict(self.carry_sell))
        return plan

    def carry_fill_events(self, fills):
        """Carry fills: the lot's average (tranche), FILLED lines; each fill once."""
        for f in fills:
            if f.get("layer") != "carry":
                continue
            k = self.fill_key(f)
            if k in self.carry_seen:
                continue
            self.carry_seen.add(k)
            ex = str(f["ex"]); sub = (self.ledger.orders.get(str(f.get("order") or "")) or {}).get("sub") or f"carry:{ex}:{f['side']}"
            tr = self.ledger.tranches.get(sub)
            if tr is None or tr.get("state") != "open":
                tr = self.ledger.tranches[sub] = dict(ex=ex, side=f["side"], kind="carry", t=stamp(), ts=time.time(), state="open", avg=f["price"], qty=0.0)
            qa = float(tr.get("qty") or 0.0)
            tr["avg"] = round((float(tr["avg"]) * qa + f["price"] * f["qty"]) / (qa + f["qty"]), 5) if qa + f["qty"] > 0 else f["price"]
            tr["qty"] = qa + f["qty"]
            fs = self.side_fair(ex, f["side"])
            msg = f"CARRY FILL {self.name(ex)} {f['side'].upper()} {f['qty']:,.0f} at {f['price']:.3f}" + ("" if fs is None else f" (fair {fs:.3f}, EV/$ {fs / f['price'] - 1:.1%})") + "; held to settlement"
            self.event("CARRY", msg); self.night.line(msg)
        if len(self.carry_seen) > 5000:
            self.carry_seen = set(list(self.carry_seen)[-2000:])

    def sale_price(self, ex, side, px, floor_, bid, reason, anc, bidsell):
        """Piero 01:05 Oct 18: tonight a fund / carry lot sells at the bid when the bid >= lot avg + 0.5c (item 3); otherwise its ask keeps the
        anchored price (item 4) — a lot whose floor is above the anchor waits (None)."""
        sig_avg = float(((self.book.get(ex) or {}).get("row") or {}).get("avgCost") or 0)
        if bidsell and bid is not None and bid >= max(floor_, bots.tick_up(round(sig_avg + RULE0_MARGIN, 6)) if sig_avg else floor_) - 1e-9:
            return bid, reason + " at the bid"                  # C99d: at the bid only when it also clears SIG's position average + 0.5c (Delaware 01:55: -38 realized)
        if anc is not None:
            return max(anc["px"], floor_), reason              # C100b: a kept price under this lot's floor rests at the floor (it was None: the ask was pulled and
        return px, reason                                     # re-posted every 5 min after the anchor re-pegged under it, Alaska / PA-10 17:44-18:07 Oct 18)

    def night_sold(self):
        """Cash sold tonight by the release layer (fund and carry sales and the rest) since NIGHT_SELL['since']."""
        key = len(self.ledger.fills)
        if getattr(self, "_night_key", None) == key:
            return self._night_sold
        tot = sum(f["qty"] * (1 - f["price"]) for f in self.ledger.fills[-2000:]
                  if f.get("layer") == "release" and f.get("action") == "buy" and str(f.get("t", "")) >= NIGHT_SELL["since"])
        self._night_key, self._night_sold = key, tot
        return tot

    def sale_churn_line(self):
        """C100 item 3: SIG cancels / posts per market of every sell rule (release, rotation exits) and of the FMM exits since the last line
        (from the order records, not the log), the anchored re-pegs, and the carry bid-sales today."""
        t0, t1 = self.churn_t, stamp()
        self.churn_t = t1
        canc, post = collections.Counter(), collections.Counter()
        for rec in list(self.ledger.orders.values()):
            lay = rec.get("layer")
            if lay not in SELL_KEEP_LAYERS and lay != "fmmexit":
                continue
            k = ("FMM exits" if lay == "fmmexit" else str(rec.get("ex")))
            if t0 <= str(rec.get("placed") or "") < t1:
                post[k] += 1
            if rec.get("status") == "cancelled" and t0 <= str(rec.get("closed") or "") < t1:
                canc[k] += 1
        keys = sorted(set(canc) | set(post), key=lambda k_: (-canc[k_], -post[k_]))
        rows = [f"{k_ if k_ == 'FMM exits' else self.name(k_)} cancels {canc[k_]} posts {post[k_]}" + ("" if k_ == "FMM exits" else f" re-pegs {self.sale_stats[k_]['repegs']}")
                for k_ in keys[:8]]
        line = f"SALES churn (every sell rule; SIG cancels / posts since {t0[11:16]}): " + (", ".join(rows) or "none")
        line += (f"; resting anchors {len(self.sale_anchor)}; carry bid-sales today {self.carry_bid_sold_today(time.time()):,.0f} of {CARRY_BIDSELL['day_cap']:,.0f}"
                 f" (on while FMM free < {CARRY_BIDSELL['fmm_free_target']:,.0f}: FMM free {self.fmm_free():,.0f})")
        self.sale_stats.clear()
        return line

    def sell_undercut_update(self, now):
        """C100 item 3: for every resting sell order of the keep-place layers, since when the others' best ask has sat below ours (none:
        it hasn't); repeg_grace lets a re-peg down through only after SALE_REPEG_S of it, never a re-peg up."""
        live = set()
        for o in self.open:
            rec = self.ledger.orders.get(o["id"]) or {}
            if rec.get("layer") not in SELL_KEEP_LAYERS or o["action"] != "buy" or not self.ex.owned(o["id"]):
                continue
            live.add(o["id"])
            held = "no" if o["side"] == "yes" else "yes"
            oa = self.fmm_others_ask(str(o["ex"]), held)
            if oa is not None and oa < round(1 - o["price"], 3) - 1e-9:
                self.sell_undercut.setdefault(o["id"], now)
            else:
                self.sell_undercut.pop(o["id"], None)
        for oid in [k for k in self.sell_undercut if k not in live]:
            self.sell_undercut.pop(oid, None)

    def carry_bidsell_on(self, now):
        """C100 item 4: carry lots with EV/$ held <= 8% sell at the bid while FMM is live with free cash < 15k, up to 7.5k a day."""
        t = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(now))
        return bool(CARRY_BIDSELL.get("on") and t >= CARRY_BIDSELL["since"] and self.fmm_mode(now) == "live"
                    and self.fmm_free() < float(CARRY_BIDSELL["fmm_free_target"]) and self.carry_bid_sold_today(now) < float(CARRY_BIDSELL["day_cap"]))

    def carry_bid_sent(self, now, ex, px):
        """The (market, price) of a carry bid-sale order, kept for the day (its fills count toward the 7.5k)."""
        day = time.strftime("%Y-%m-%d", time.localtime(now))
        c = self.controls
        d = c.d.get("cbs") or {}
        if d.get("day") != day:
            d = dict(day=day, px=[])
        k = [str(ex), round(1 - float(px), 3)]
        if k not in d["px"]:
            with c.lock:
                d["px"] = d["px"] + [k]
                c.d["cbs"] = d
                c.save()

    def carry_bid_sold_today(self, now):
        """Cash of today's release fills at a carry bid-sale's price on a carry sub-book (C100 item 4's daily 7.5k)."""
        day = time.strftime("%Y-%m-%d", time.localtime(now))
        d = self.controls.d.get("cbs") or {}
        if d.get("day") != day or not d.get("px"):
            return 0.0
        sent = {(a, round(float(b), 3)) for a, b in d["px"]}
        key = (len(self.ledger.fills), len(sent), day)
        if getattr(self, "_cbs_key", None) == key:
            return self._cbs_sold
        tot = 0.0
        for f in self.ledger.fills[-3000:]:
            if f.get("layer") != "release" or not str(f.get("t", "")).startswith(day) or (str(f.get("ex")), round(float(f["price"]), 3)) not in sent:
                continue
            if str((self.ledger.orders.get(str(f.get("order") or "")) or {}).get("sub") or "").startswith("carry:"):
                tot += f["qty"] * (1 - f["price"])
        self._cbs_key, self._cbs_sold = key, tot
        return tot

    # ------------------------------------------------------------------ C102: election night (docs/night_reconcile.md §4 items 1-5, 9)
    def el_reserve(self, now):
        """C103: the election reserve (usable cash no entry layer may spend) on the day's step of the ramp."""
        if not ELECTION_RESERVE.get("on"):
            return 0.0
        t = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(now))
        if t >= ELECTION_RESERVE["until"]:
            return 0.0
        amt = 0.0
        for at, v in ELECTION_RESERVE["steps"]:
            if t >= at:
                amt = float(v)
        return amt

    def wd_after(self, key, now):
        return bool(WIND_DOWN.get("on") and time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(now)) >= WIND_DOWN[key])

    def wd_fmm_cap(self, now):
        cap = float("inf")
        if WIND_DOWN.get("on"):
            t = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(now))
            for at, v in WIND_DOWN["fmm_caps"]:
                if t >= at:
                    cap = float(v)
        return cap

    def conv_select(self, now):
        """C103 (cash_calendar §2 Oct 30 / §3 item 2): the lots that get a convergence ask — while usable cash plus the sale asks resting is
        under the day's election reserve, non-core lots first, lowest return left first, until their value covers the gap. Out: Piero's
        manual book, locks / pair sets, FMM, rotation (own exits), the night layers, RI Senate R NO (his keep), the held R-side lots."""
        cash = min(self.cash_raw_hist) if getattr(self, "cash_raw_hist", None) else 0.0
        usable = cash - CASH_RESERVE - self.manual_reserve_left()
        asks = sum(o["qty"] * (1 - o["price"]) for o in self.open if (self.ledger.orders.get(o["id"]) or {}).get("layer") in ("release", "rotexit"))
        gap = self.el_reserve(now) - usable - asks
        keep = getattr(self, "conv_keep", set())
        resting = {(str(o["ex"]), (self.ledger.orders.get(o["id"]) or {}).get("sub")) for o in self.open if (self.ledger.orders.get(o["id"]) or {}).get("layer") == "release"}
        keep = {k for k in keep if k in resting}             # an ask already out keeps its place
        if gap <= 0:
            self.conv_keep = keep
            return keep
        cands = []
        for ex, b in self.book.items():
            ex = str(ex); q = float(b.get("pos") or 0)
            if not q or ex in self.manual_ex or ex in SELL_HOLD or ex in dash5.RI_EX or ex in self.reservoirs():
                continue
            side = "yes" if q > 0 else "no"
            bid, ask = bots.side_book(*self.px.get(ex, (None, None, None))[:2], side)[:2]
            fs = self.side_fair(ex, side)
            if ask is None or fs is None:
                continue
            for sub, v in (self.ledger.subbooks.get(ex) or {}).items():
                if v * q <= 0 or abs(v) < MIN_QTY or str(sub).startswith(("manual", "pairset:", "fmm:", "fmmlock:", "rot:", "callsnipe:", "drift:", "sleeve:", "swing:")) or (ex, sub) in keep:
                    continue
                avg = float((self.ledger.tranches.get(sub) or {}).get("avg") or ((b.get("row") or {}).get("avgCost")) or 0)
                px = max(ask, bots.tick_up(round(avg + RULE0_MARGIN, 6)), bots.tick_up(round(fs / (1 + SELL_EV_MAX), 6)) if (SELL_EV_MAX < 1 and not self.wd_after("conv_no_ev_floor", now)) else 0.0)
                cands.append(((ex, sub) in self.core_set, fs / px - 1, ex, sub, abs(v) * px))
        out = set(keep)
        for core, ret, ex, sub, val in sorted(cands):
            if gap <= 0:
                break
            out.add((ex, sub)); gap -= val
        if out - keep:
            msg = (f"CONVERGENCE (cash calendar): usable {usable:,.0f} + asks {asks:,.0f} under the {self.el_reserve(now):,.0f} reserve: asks on " +
                   ", ".join(f"{self.name(e)} {s_.split(':')[0]}" for e, s_ in sorted(out - keep))[:600])
            self.event("RELEASE", msg); self.night.line(msg)
        self.conv_keep = out
        return out

    def election_cash_line(self, now):
        """cash_calendar §3 item 4: the ELECTION CASH line — usable cash vs the ramp and the 60k (cash >= 67k) target by Nov 3 15:00."""
        cash = min(self.cash_raw_hist) if getattr(self, "cash_raw_hist", None) else 0.0
        usable = cash - CASH_RESERVE - self.manual_reserve_left()
        er = self.el_reserve(now)
        nxt = next(((at, v) for at, v in ELECTION_RESERVE["steps"] if time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(now)) < at), None)
        asks = sum(o["qty"] * (1 - o["price"]) for o in self.open if (self.ledger.orders.get(o["id"]) or {}).get("layer") in ("release", "rotexit"))
        return (f"ELECTION CASH: cash {cash:,.0f}, usable {usable:,.0f} (cash - 5k - Piero's reserve left), election reserve now {er:,.0f}"
                + (f", next {nxt[1]:,.0f} from {nxt[0][5:16]}" if nxt else "") + f"; sale asks resting would bring {asks:,.0f}; target 60,000 usable by Nov 3 15:00 "
                f"(short {max(0.0, 60_000 - usable - asks):,.0f} at today's asks)")

    def el_mode(self, now=None):
        """off / shadow / live: the `election_mode` control, else shadow from ELECTION['shadow_from'] (zero orders). Live only by control."""
        m = (self.controls.d.get("election") or {}).get("mode")
        if m in ("off", "shadow", "live"):
            return m
        t = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(now or time.time()))
        return "shadow" if ELECTION.get("on") and t >= ELECTION["shadow_from"] else "off"

    def el_build(self):
        """The Election from data/election/poll_close.json and the engine's races: the first and last poll close per race (PT)."""
        rows = (json.load(open(os.path.join(DATA, "election", "poll_close.json"))).get("markets") or [])
        day = ELECTION["election_day"]

        def ep(hm):
            hm = str(hm); plus = hm.endswith("+1")
            return time.mktime(time.strptime(f"{day} {hm.replace('+1', '')}", "%Y-%m-%d %H:%M")) + (86400 if plus else 0)
        meta, first, last = {}, {}, {}
        for r in rows:
            ex = str(r.get("ex"))
            if ex not in self.owner or not r.get("first_close_pt") or not r.get("last_close_pt"):
                continue
            race = self.rot_race(ex)
            state = race if "control" in str(race).lower() else r.get("state")   # sim: the control markets are not Alaska's (its 20k state cap)
            meta[ex] = dict(race=race, party=r.get("party"), title=r.get("title"), state=state)
            f_, l_ = ep(r["first_close_pt"]), ep(r["last_close_pt"])
            first[race] = min(first.get(race, f_), f_); last[race] = max(last.get(race, l_), l_)
        self.election = el.Election(meta, last, time.time, self.el_place, self.el_cancel, self.el_book, self.el_log, manual=tuple(str(x) for x in self.manual_ex),
                                    cash_floor=el.CASH_FLOOR, first_close=first, levels=self.el_levels, place_dry=self.el_place_dry,
                                    precall_dry=bool(ELECTION.get("precall_dry")))
        msg = (f"ELECTION armed ({self.el_mode()}): {len(meta)} books / {len(last)} races from poll_close.json; first close "
               f"{time.strftime('%m-%d %H:%M', time.localtime(min(first.values())))} PT, last {time.strftime('%m-%d %H:%M', time.localtime(max(last.values())))} PT; "
               f"call = PM side bid >= {el.CALL_HOLD} for {el.CALL_HOLD_S:.0f} s or >= {el.CALL_STRICT} once after the state's last close; sniper <= min({el.PRICE_CAP}, "
               f"PM bid - {el.PRICE_UNDER_PM * 100:.0f}c), {el.RACE_SHARES:,} shares a race, {el.STATE_CAP:,.0f} a state, {el.TTL_S} s; no post-call bids") if meta else "ELECTION: no books mapped"
        self.event("NIGHT", msg); self.night.line(msg)

    def el_book(self, ex):
        px = self.px.get(str(ex)) or self.px.get(ex)
        return None if not px else (px[0], px[1], 0)

    def el_levels(self, ex, side):
        fb = self.feed_book(str(ex))
        if fb is None:
            return None
        return list(fb[1]) if side == "yes" else [(round(1 - p, 3), q) for p, q in fb[0]]

    def el_log(self, kind, text):
        self.el_logq.append((kind, text))                    # the feed thread logs here; the loop writes (no file I/O off the loop)

    def el_place_dry(self, items, layer, ttl, why):
        self.el_shadowq.append(dict(t=stamp(), kind="order", why=why, layer=layer, ttl=ttl, items=[list(i) for i in items], mode="shadow",
                                    injected=bool(getattr(self, "el_inj", None))))
        return "dry"

    def el_place(self, items, layer, ttl):
        """Shadow: the would-be order to shadow.jsonl. Live: through the executor (the fast_l0 pattern), never while new risk is paused."""
        mode = self.el_mode_cached
        if mode != "live":
            self.el_shadowq.append(dict(t=stamp(), ts=round(time.time(), 3), kind="order", why="call", layer=layer, ttl=ttl, items=[list(i) for i in items], mode=mode,
                                        injected=bool(getattr(self, "el_inj", None))))
            self.el_hour["would_orders"] += 1
            return "shadow"
        if self.controls.paused:
            return None
        out = [(self.owner[ex].name, f"{layer}:{ex}:{side}", layer, (ex, side, action, round(px, 3), int(q))) for ex, side, action, px, q in items if ex in self.owner]
        box = {}
        key = self.ex.place(out, ttl_s=ttl, on_done=lambda part: self.election.confirm(box.get("k"), part)) if out else None   # sim: count what SIG took
        box["k"] = key
        if key:
            self.ex.step_placements()
            self.el_hour["orders"] += 1
            for ex, side, action, px, q in items:
                self.el_sent[str(ex)] = time.time()
            self.el_shadowq.append(dict(t=stamp(), kind="sent", layer=layer, key=key, items=[list(i) for i in items], mode="live"))
        return key

    def el_cancel(self, o, reason=""):
        if self.el_mode_cached != "live":
            self.el_shadowq.append(dict(t=stamp(), kind="cancel", id=o.get("id"), ex=o.get("ex"), reason=reason, mode=self.el_mode_cached))
            return True
        oo = next((x for x in self.open if x["id"] == o.get("id")), None)
        return bool(oo and self.ex.cancel(oo, reason=reason))

    def el_quote(self, ex, b, a, now):
        """PolyFeed thread: one PM quote -> the detector, on the loop's immutable snapshot; a call wakes the loop (the snipe goes from there)."""
        E = self.election
        if E is None or self.el_mode_cached == "off":
            return
        snap = self.el_snap
        if E.on_pm(str(ex), (b + a) / 2, now, snap["cash"], snap["pos"], pm_bid=b, pm_ask=a, defer=True) == "queued":
            lane = getattr(self, "el_lane_ev", None)
            if lane is not None:
                lane.set()                                  # C104 item 7: the call lane sends the snipe now (C106: shadow too)
            self.rt_event("call", {str(ex)}, now)

    def el_inject_start(self, now, inj):
        """C104 (item 10): start a synthetic path — the race treated as closed (shadow), its quotes fed through the detector hook."""
        E = self.election
        ex = str(inj["ex"]); race = E.race(ex)
        if race is None:
            self.el_log("INJECT", f"{self.name(ex)}: not an armed book; nothing injected")
            return
        self.el_inj = dict(ex=ex, race=race, t0=now, path=sorted(inj["path"]), i=0, orig=(E.close.get(race), E.first.get(race)))
        if inj.get("close_now", True):
            E.close[race] = E.first[race] = now - 60
        self.el_log("INJECT", f"{race}: {len(inj['path'])} synthetic PM quotes into {self.name(ex)} over {inj['path'][-1][0]:.0f} s (shadow; polls treated as closed)")

    def el_inject_step(self, now):
        inj = getattr(self, "el_inj", None)
        if not inj:
            return
        E = self.election
        P = inj["path"]
        while inj["i"] < len(P) and inj["t0"] + P[inj["i"]][0] <= now:
            _, b, a = P[inj["i"]]
            inj["i"] += 1
            self.el_quote(inj["ex"], b, a, time.time())       # C108: the quote's own time (the loop's start would inflate the lane's measured latency)
            if E.queue and getattr(self, "el_lane_ev", None) is None:
                E.run_queue(now, self.el_snap["cash"], self.el_snap["pos"])   # no lane thread (tests): from the loop
        if inj["i"] >= len(P) and now - (inj["t0"] + P[-1][0]) >= el.TTL_S:
            race = inj["race"]
            E.close[race], E.first[race] = inj["orig"]
            for e in [e for e in list(E.calls) if E.race(e) == race]:
                E.calls.pop(e, None)
            E.race_stop.discard(race); E.pulled.discard(race)
            for k in [k for k in E.run0 if E.race(k[0]) == race]:
                E.run0.pop(k, None)
            self.el_inj = None
            self.el_log("INJECT", f"{race}: synthetic path done; the race's real poll close restored and its synthetic call cleared")

    def el_lane_once(self):
        """C104 (night item 7, latency.md §3): one pass of the call lane — the queued snipes go out from here, off the loop, through the
        executor (its locks; the last 5 writes are the lane's while the election is live). -> orders placed."""
        E = self.election
        if E is None or self.el_mode_cached == "off" or self.controls.paused or not E.queue:
            return 0                                         # C106: in shadow too (it only writes would-be orders there): the lane's latency is measured
        snap = self.el_snap
        return E.run_queue(time.time(), snap["cash"], snap["pos"])

    def el_lane_loop(self):
        while True:
            self.el_lane_ev.wait(timeout=5.0)
            self.el_lane_ev.clear()
            try:
                self.el_lane_once()
            except Exception as err:
                self.el_log("LANE", f"call lane error: {err!r}")

    def el_layer_of(self, o):
        return (self.ledger.orders.get(o["id"]) or {}).get("layer") if self.ex.owned(o["id"]) else None

    def el_step(self, now):
        """Each loop: the snapshot the feed thread reads (replaced, never mutated), the poll-close pulls, the queued snipes, the settlement
        morning, then the logs and files (calls.jsonl, shadow.jsonl, NIGHT lines)."""
        mode = self.el_mode(now)
        self.el_mode_cached = mode
        self.ex.lane_reserve = 5 if mode == "live" else 0   # C104: 5 writes a minute stay the call lane's while live (latency.md §2)
        if mode == "off":
            return
        if self.election is None:
            if getattr(self, "el_build_failed", False):
                return
            try:
                self.el_build()
            except Exception as err:                        # no poll-close map: say so once, the rest of the engine runs on
                self.el_build_failed = True
                self.event("ERR", f"election: not armed: {err!r}")
                return
        E = self.election
        cash = min(self.cash_raw_hist) if getattr(self, "cash_raw_hist", None) else 0.0
        pos = {str(ex): float(b.get("pos") or 0) for ex, b in self.book.items()}
        self.el_snap = dict(cash=cash, pos=pos)
        oo = [dict(id=o["id"], ex=str(o["ex"]), side=o["side"], action=o["action"], price=o["price"], qty=o["qty"]) for o in self.open]
        st = self.controls.d.get("election") or {}
        if st.get("inject") and mode == "shadow":
            self.el_inject_start(now, st["inject"])
            with self.controls.lock:
                self.controls.d["election"].pop("inject", None)
                self.controls.save()
        self.el_inject_step(now)
        if st.get("stop_req"):
            n = E.kill(now, oo, pos, self.el_layer_of)
            with self.controls.lock:
                self.controls.d["election"].pop("stop_req", None)
                self.controls.save()
            self.controls.set_paused(True, f"election_stop (Piero): {n} orders pulled, exits kept")
        if st.get("rearm_req"):
            E.rearm((st["rearm_req"] or {}).get("race"))
            with self.controls.lock:
                self.controls.d["election"].pop("rearm_req", None)
                self.controls.save()
            self.el_log("REARM", f"election_rearm: {(st['rearm_req'] or {}).get('race') or 'every race and the kill switch'} (new risk stays as it is)")
        E.on_tick(now, cash, pos, oo, self.el_layer_of)
        try:
            if mode == "live" or now - getattr(self, "drift_t", 0.0) >= 60:   # C103: shadow runs the drift scan once a minute (C102 shadow: +1 s a loop)
                self.drift_t = now
                self.drift_plan(now)                        # C102 item 6: the night drift (shadow: as if every race had closed)
            self.callexit_plan(now)                         # C102 item 4: RULE0-CALLED exits of lots a strict call says lose
        except Exception as err:
            self.event("ERR", f"drift plan: {err!r}")
        if not self.controls.paused and E.queue:
            E.run_queue(now, cash, pos)
        if WIND_DOWN.get("on") and not getattr(self, "el_checklist_done", False) and time.strftime("%Y-%m-%d %H:%M", time.localtime(now)) >= f"{ELECTION['election_day']} {WIND_DOWN['checklist_at'][11:]}":
            self.el_checklist_done = True                   # C102 item 5 (v1 §6): the pre-night checklist, once at 14:45 PT
            self.night.line(self.el_checklist(now, cash))
        day1 = time.mktime(time.strptime(ELECTION["election_day"], "%Y-%m-%d")) + 86400
        E.settlement(now, cash, pos, {str(k): v for k, v in self.fair.items()}, day1 + 6 * 3600, day1 + 8.5 * 3600)
        self.el_flush(mode)

    def el_flush(self, mode):
        E = self.election
        d = os.path.join(DATA, "election")
        os.makedirs(d, exist_ok=True)
        tag = f"ELECTION ({mode})"
        while self.el_logq:
            kind, text = self.el_logq.popleft()
            self.el_hour[kind] += 1
            self.event("NIGHT", f"{kind} {text}")
            if kind != "PRECALL":
                self.night.line(f"{tag} {kind} {text}")
        rows = []
        with E.lock:
            rows, E.records = E.records, []
        if rows:
            with open(os.path.join(d, "calls.jsonl"), "a") as fh:
                for r in rows:
                    fh.write(json.dumps(dict(r, ts=r.get("t"), t=time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(r.get("t") or 0)), mode=mode,
                                             injected=bool(getattr(self, "el_inj", None)) and r.get("race") == (self.el_inj or {}).get("race")), default=str) + "\n")
        if self.el_shadowq:
            with open(os.path.join(d, "shadow.jsonl"), "a") as fh:
                while self.el_shadowq:
                    fh.write(json.dumps(self.el_shadowq.popleft(), default=str) + "\n")

    def el_recycle_need(self):
        """C7: the cash the next wave still needs — usable (cash - 5k - 2k) under ELECTION['recycle_target']."""
        return max(0.0, float(ELECTION["recycle_target"]) - (self.el_snap["cash"] - el.CASH_FLOOR))

    def el_fill_events(self, fills):
        """night_reconcile §4 item 9: each fill of an election layer (call snipe, drift, call exit, settle) -> one NIGHT line and a row of
        data/election/orders.jsonl: the PM signal, the call, the order sent and the fill, on one clock."""
        E = self.election
        rows = []
        for f in fills:
            if f.get("layer") not in ("callsnipe", "drift", "driftexit", "callexit", "settle"):
                continue
            k = self.fill_key(f)
            if k in self.el_fill_seen:
                continue
            self.el_fill_seen.add(k)
            ex = str(f["ex"]); c = (E.calls.get(ex) if E else None) or {}
            if E is not None and f["layer"] in ("callsnipe", "settle"):
                E.on_fill(ex, float(f["qty"]), float(f["price"]))   # sim: a filled snipe's cash hold goes (no double count)
            try:
                t_fill = time.mktime(time.strptime(str(f["t"]), "%Y-%m-%d %H:%M:%S"))
            except ValueError:
                t_fill = time.time()
            sig, call, sent = c.get("signal_t"), c.get("t"), self.el_sent.get(ex)
            r = dict(t=f["t"], ex=ex, race=E.race(ex) if E else None, layer=f["layer"], side=f["side"], qty=f["qty"], price=f["price"],
                     signal_t=sig, call_t=call, sent_t=sent, fill_t=t_fill, signal_to_sent=None if not (sig and sent) else round(sent - sig, 2),
                     sent_to_fill=None if not sent else round(t_fill - sent, 2))
            rows.append(r)
            msg = (f"NIGHT {f['layer']} {self.name(ex)} {f['side'].upper()} {f['qty']:,.0f} at {f['price']:.3f}: signal -> sent "
                   f"{'n/a' if r['signal_to_sent'] is None else str(r['signal_to_sent']) + ' s'}, sent -> fill {'n/a' if r['sent_to_fill'] is None else str(r['sent_to_fill']) + ' s'}")
            self.event("NIGHT", msg); self.night.line(msg)
        if rows:
            d = os.path.join(DATA, "election"); os.makedirs(d, exist_ok=True)
            with open(os.path.join(d, "orders.jsonl"), "a") as fh:
                for r in rows:
                    fh.write(json.dumps(r, default=str) + "\n")
        if len(self.el_fill_seen) > 5000:
            self.el_fill_seen = set(list(self.el_fill_seen)[-2000:])

    def callexit_plan(self, now):
        """night_reconcile §4 item 4 (C17; Piero's alpha_spec §5.4 YES): after a strict call (the 0.98 rule, after the last close) against a
        lot we hold, every engine sub-book of it (stuck coin flips included; Piero's manual book, pair sets and locks never) rests a sell at
        the held side's best bid, rule 0 exempted (RULE0-CALLED). Shadow: logged once per book."""
        self.callexit_now = {}
        E, mode = self.election, self.el_mode_cached
        if E is None or mode == "off":
            return
        for ex, b in self.book.items():
            ex = str(ex); q = float(b.get("pos") or 0)
            if abs(q) < MIN_QTY or ex in self.manual_ex:
                continue
            held = "yes" if q > 0 else "no"
            if not E.strict_against(ex, held):
                continue
            bid = bots.side_book(*self.px.get(ex, (None, None, None))[:2], held)[0]
            if bid is None:
                continue
            rows = [dict(side=held, px=round(bid, 3), sub=sub) for sub, v in (self.ledger.subbooks.get(ex) or {}).items()
                    if v * q > 0 and abs(v) >= MIN_QTY and not str(sub).startswith(("manual", "pairset:", "fmmlock:"))]
            if not rows:
                continue
            if mode == "live":
                self.callexit_now[ex] = rows
            if ex not in self.callexit_logged:
                self.callexit_logged.add(ex)
                self.el_log("CALLEXIT", f"{E.race(ex)}: {held.upper()} called against (strict): {len(rows)} lot(s) to sell at the best bid {bid:.3f}"
                                        + ("" if mode == "live" else " (shadow: nothing sent)"))

    def drift_lots(self):
        """{sub: (ex, side, shares, lot avg)} of the night drift's lots (sub 'drift:<ex>:<side>')."""
        out = {}
        for ex, books in self.ledger.subbooks.items():
            for sub, q in books.items():
                if sub.startswith("drift:") and abs(q) >= MIN_QTY:
                    side = "yes" if q > 0 else "no"
                    lots = [l for l in self.lots(ex, side, sub) if l[0] > 0]
                    tot = sum(l[0] for l in lots)
                    avg = sum(l[0] * l[1] for l in lots) / tot if tot else float(((self.book.get(ex) or {}).get("row") or {}).get("avgCost") or 0)
                    out[sub] = (str(ex), side, abs(q), avg)
        return out

    def drift_baseline(self, ex, now, shadow):
        """C110: (baseline gap, PM at the anchor). Live: the book's 6-h median gap (print - PM fair) and PM at its race's first poll close,
        frozen the first time they are read after the close (persisted). Shadow: a rolling anchor 60 min back, the current 6-h median."""
        if shadow:
            return self.gap6(now).get(str(ex)), self.fair_at(str(ex), now - float(ELECTION["drift_anchor_s"]))
        st = self.controls.d.setdefault("drift_base", {})
        b = st.get(str(ex))
        if b is None:
            E = self.election
            first = E.first.get(E.race(str(ex))) if E else None
            g = self.gap6(now).get(str(ex))
            pm0 = self.fair_at(str(ex), first) if first else None
            if g is None:
                return None, None
            b = st[str(ex)] = dict(base=round(float(g), 4), pm0=round(float(pm0 if pm0 is not None else self.fair.get(str(ex)) or 0), 4), t=stamp())
            with self.controls.lock:
                self.controls.save()
        return b["base"], b["pm0"]

    def drift_plan(self, now):
        """night_reconcile §4 item 6, C110 (Piero 11:40 Oct 22): the pre-call drift trades the CHANGE, not SIG's permanent 6-12c gap level.
        From a race's first poll close until it is called: when PM has moved >= 3c since the close and the SIG mid - PM gap sits >= 3c beyond
        the book's 6-h median gap before the close (in the direction of the move: SIG lagging), take the lagging side walking the book to the
        baseline-implied price - 3c (120 s); its lot exits when the gap is back within 1c of the baseline, never under lot avg + 0.5c. Caps: the
        rotation's race / total caps x 2, the 10k party cap. Shadow: a rolling 60-min anchor instead of the close; would-be takes to
        shadow.jsonl (one per book per 30 min); nothing is sent."""
        self.drift_now, self.drift_view = {}, {}
        E, mode = self.election, self.el_mode_cached
        if E is None or mode == "off":
            return
        shadow = mode == "shadow"
        lots = self.drift_lots()
        for sub, (ex, side, q, avg) in lots.items():
            pm = self.fair.get(ex)
            base, _ = self.drift_baseline(ex, now, shadow)
            if pm is None or base is None or not avg:
                continue
            tgt = (pm + base - float(ELECTION["drift_exit_gap"])) if side == "yes" else (1 - (pm + base + float(ELECTION["drift_exit_gap"])))
            px = max(bots.tick_up(round(avg + RULE0_MARGIN, 6)), bots.tick_up(round(tgt, 6)))
            self.drift_view.setdefault(ex, []).append(dict(sub=sub, side=side, px=min(px, 0.99), avg=avg))
        if (shadow and not ELECTION.get("drift_shadow")) or (not shadow and (self.controls.paused or E.stopped)):
            return
        mult, gap_c, move_c = float(ELECTION["drift_cap_mult"]), float(ELECTION["drift_gap"]), float(ELECTION["drift_move"])
        rc = collections.Counter()
        for _, (ex, side, q, avg) in list(self.rot_lots().items()) + list(lots.items()):
            rc[self.rot_race(ex)] += q * avg
        total = sum(rc.values())
        party = dict(net=self.party_view()["net"]) if ROTATION.get("party_cap") else None
        usable = self.el_snap["cash"] - el.CASH_FLOOR
        for ex in list(E.meta):
            ok = E.drift_ok(ex, now) if not shadow else (not E.stopped and E.called(ex) is None and E.race(ex) not in E.race_stop and ex not in E.manual)
            if not ok or (shadow and now - self.drift_logged.get(ex, -1e18) < float(ELECTION["drift_every_s"])):
                continue
            pm = self.fair.get(ex); px_ = self.px.get(ex) or (None, None, None)
            if pm is None or px_[0] is None or px_[1] is None:
                continue
            base, pm0 = self.drift_baseline(ex, now, shadow)
            if base is None or pm0 is None:
                continue
            move = pm - pm0
            gap = (px_[0] + px_[1]) / 2 - pm
            if move >= move_c - 1e-9 and gap <= base - gap_c + 1e-9:
                side, lim = "yes", pm + base - gap_c                      # PM up, SIG lagging below its usual gap: buy YES up to that price
            elif move <= -move_c + 1e-9 and gap >= base + gap_c - 1e-9:
                side, lim = "no", 1 - (pm + base + gap_c)                 # PM down, SIG lagging above: buy NO
            else:
                continue
            pos_ = float((self.book.get(ex) or {}).get("pos") or 0)
            if (side == "yes" and pos_ <= -1) or (side == "no" and pos_ >= 1):
                continue                                    # C107: never a take that nets our own other-side lot
            race = self.rot_race(ex)
            lv = self.el_levels(ex, side) or []
            take = [(p_, q_) for p_, q_ in lv if p_ <= lim + 1e-9 and q_ > 0]
            if not take:
                continue
            limit, avail = max(p_ for p_, _ in take), sum(q_ for _, q_ in take)
            room = min(self.rot_cap(race) * mult - rc[race], float(ROTATION.get("total_cap") or 30_000) * mult - total, usable)
            qty = int(min(avail, float(ELECTION["drift_clip"]), max(0.0, room) / limit))
            if qty < MIN_QTY or not self.carry_party_ok(ex, side, qty * limit, party):
                continue
            if shadow:
                self.drift_logged[ex] = now
                self.el_hour["drift_would"] += 1
                self.el_shadowq.append(dict(t=stamp(), ts=round(time.time(), 3), kind="order", why="drift", layer="drift", ttl=int(ELECTION["drift_ttl_s"]), mode="shadow",
                                            items=[[ex, side, "buy", round(limit, 3), qty]], pm=round(pm, 4), pm0=round(pm0, 4), move=round(move, 4),
                                            gap=round(gap, 4), base=round(base, 4)))
            else:
                self.drift_now.setdefault(ex, []).append(dict(side=side, px=round(limit, 3), qty=qty, sub=f"drift:{ex}:{side}", ttl=int(ELECTION["drift_ttl_s"])))
            rc[race] += qty * limit; total += qty * limit; usable -= qty * limit

    def el_checklist(self, now, cash):
        E = self.election
        fmm_b = sum(1 for o in self.open if (self.ledger.orders.get(o["id"]) or {}).get("layer") in ("fmm", "fmmtake") and o["action"] == "buy")
        car_b = sum(1 for o in self.open if (self.ledger.orders.get(o["id"]) or {}).get("layer") == "carry")
        pm_age = now - max([t for _, _, t in (self.pm_feed.quotes.values() if self.pm_feed else [])] + [0.0])
        first = min(E.first.values()) if E and E.first else None
        usable = cash - CASH_RESERVE - self.manual_reserve_left()
        return (f"ELECTION CHECKLIST {time.strftime('%H:%M', time.localtime(now))}: mode {self.el_mode_cached}{' (live needs election_mode=live)' if self.el_mode_cached != 'live' else ''}; "
                f"kill {'ON' if E and E.stopped else 'off'}; books armed {len(E.meta) if E else 0} / races {len(E.close) if E else 0}; first pulls "
                f"{time.strftime('%H:%M', time.localtime(first - el.PULL_BEFORE_S)) if first else 'n/a'} PT; usable cash {usable:,.0f} (target 60,000 {'OK' if usable >= 60_000 else 'SHORT'}); "
                f"FMM bids resting {fmm_b}, carry bids {car_b} (both should be 0); PM feed last quote {pm_age:.0f} s ago; writes left {api.writes_left()}; "
                f"new risk {'PAUSED' if self.controls.paused else 'on'}; poll_close.json {'ok' if E else 'MISSING'}")

    def el_hour_line(self):
        h, E = self.el_hour, self.election
        line = (f"ELECTION ({self.el_mode_cached}) last hour: PRECALL {h['PRECALL']}, calls {h['CALL']}, no-calls {h['NOCALL']}, reversals {h['REVERSAL']}, "
                f"would-be orders {h['would_orders']} + drift {h['drift_would']}, orders sent {h['orders']}, pulls {h['POLLS']}; books armed {len((E.meta if E else {}))}, calls held "
                f"{sum(1 for c in (E.calls.values() if E else []) if not c.get('reversed'))}, races stopped {len(E.race_stop) if E else 0}, kill {'ON' if E and E.stopped else 'off'}"
                + (lambda L: f"; call lane (quote -> order handed over) n {len(L)} p50 {L[len(L) // 2] * 1000:.0f} ms p95 {L[min(len(L) - 1, int(0.95 * len(L)))] * 1000:.0f} ms" if L else "")(sorted(E.lane_lat if E else [])))
        self.el_hour.clear()
        return line

    AZ06_ORDERS = ("46303780", "46306401", "46313591", "46315471")

    def az06_audit(self):
        """C100 item 2, once: SIG's own record (action / side / quantity / price) of Piero's four AZ-06 orders of Oct 17 15:54-15:57."""
        c = self.controls
        if (c.d.get("c100_az06") or {}).get("done") or not api.spare_reads(20):
            return
        out = []
        for oid in self.AZ06_ORDERS:
            try:
                r = api.order(oid) or {}
            except Exception as err:
                self.event("ERR", f"AZ-06 order {oid} read: {err!r}")
                return
            out.append(f"{oid} {str(r.get('action') or '?').lower()} {str(r.get('side') or '?').upper()} {r.get('quantity')} at {r.get('price')} "
                       f"(filled {r.get('filledQuantity', r.get('filled', '?'))}, {r.get('status') or ('open' if r.get('open') else 'closed')})")
        msg = "AUDIT AZ-06 House race R (Piero's Oct 17 15:54-15:57 orders, SIG's records): " + "; ".join(out)
        self.event("AUDIT", msg); self.night.line(msg)
        with c.lock:
            c.d["c100_az06"] = dict(done=stamp(), orders=out)
            c.save()

    def manual_line(self):
        """Piero 01:05 item 7: his manual tickets, next to the party line — SIG's position vs the manual sub-book."""
        rows = []
        for ex, books in self.ledger.subbooks.items():
            m = float(books.get("manual", 0.0) or 0.0)
            if abs(m) < 1:
                continue
            pos = float((self.book.get(str(ex)) or {}).get("pos") or 0)
            side = "yes" if pos >= 0 else "no"
            fs = self.side_fair(str(ex), side)
            avg = float(((self.book.get(str(ex)) or {}).get("row") or {}).get("avgCost") or 0)
            tag = "R ticket" if self.party_dir(str(ex), side) == "R" else "D ticket" if self.party_dir(str(ex), side) == "D" else "ticket"
            eng_ = sum(float(v) for k, v in books.items() if k != "manual")   # C106: the engine's own sub-books here (Oregon Oct 21: an FMM lot of 63)
            note = ("" if abs(abs(pos) - abs(m)) < 1 else f" (manual {m:+,.0f} + engine sub-books {eng_:+,.0f} = SIG {pos:+,.0f})" if abs(m + eng_ - pos) < 1
                    else f" (manual sub-book {m:+,.0f} + engine {eng_:+,.0f}: off SIG's {pos:+,.0f})")
            rows.append(f"{self.name(ex)} {side.upper()} SIG {abs(pos):,.0f} @ {avg:.3f} = {abs(pos) * avg:,.0f} at cost, fair {fs if fs is None else round(fs, 3)} [{tag}]{note}")
        return "MANUAL (Piero's tickets): " + ("; ".join(rows) if rows else "none")

    def carry_party_ok(self, ex, side, notional, party):
        """alpha_spec §5.5: a carry buy on a close-race side (0.20-0.80) may not raise |net party exposure| above the cap."""
        if party is None:
            return True
        lo, hi = ROTATION.get("party_band") or (0.20, 0.80)
        fs = self.side_fair(ex, side)
        if fs is None or not (lo <= fs <= hi):
            return True                                      # a safe favourite is not close-race exposure
        p_ = self.party_dir(ex, side)
        delta = notional if p_ == "D" else -notional if p_ == "R" else 0.0
        cap = float(ROTATION.get("party_cap") or 0)
        if abs(party["net"] + delta) > cap + 1e-6 and abs(party["net"] + delta) > abs(party["net"]):
            return False
        party["net"] += delta
        return True

    def carry_status(self):
        lots = self.carry_lots()
        cost = sum(q * avg for ex, side, q, avg in lots.values())
        ev = sum(q * ((self.side_fair(ex, side) or avg) - avg) for ex, side, q, avg in lots.values())
        bids = [o for o in self.open if o["action"] == "buy" and (self.ledger.orders.get(o["id"]) or {}).get("layer") == "carry"]
        return (f"CARRY {len(lots)} lots, cost {cost:,.0f}, EV to settlement {ev:+,.0f} = {ev / cost * 100 if cost else 0:.1f}% per $ (target >= 11% by Tue); "
                f"bids resting {len(bids)} / {sum(o['qty'] * o['price'] for o in bids):,.0f}; selling {len(self.carry_sell)} ({', '.join(self.name(ex) for ex in list(self.carry_sell)[:6])})")

    def c98_migration(self, now):
        """Once (C98, alpha_spec §2): the favourites in the holdings bucket move to the carry book (held to settlement): not rotation, FMM, lock,
        pair-set or manual lots, not the fund sale's markets, not RI Senate R NO, not the catcher's lots since the allocation. A resting exit of
        a moved sub-book is re-pointed at the carry sub-book (its fill books there) and the planner then cancels it."""
        c = self.controls
        if (c.d.get("c98") or {}).get("done") or not CARRY.get("on") or not self.book:
            return
        fund_ex = {str(k) for k, v in (((c.d.get("fund") or {}).get("sell_left")) or {}).items() if int(v) >= MIN_QTY}
        moved, total = [], 0.0
        for ex, books in list(self.ledger.subbooks.items()):
            ex = str(ex)
            if ex in fund_ex or ex in dash5.RI_EX or ex in self.manual_ex or ex not in self.owner or self.owner[ex].kind != "market":
                continue
            if abs(float(books.get("manual", 0.0))) >= 1:
                continue                                     # Piero trades this market by hand (AZ-06, Oct 17 15:54): the engine leaves it alone
            pos_ = float((self.book.get(ex) or {}).get("pos") or 0)
            for sub, q in list(books.items()):
                if abs(q) < 1 or sub == "manual" or sub.startswith(("rot:", "fmm:", "fmmlock:", "pairset:", "carry:")) or q * pos_ <= 0:
                    continue                                 # only lots on the side SIG holds (an offset sub-book is not a holding)
                side = "yes" if q > 0 else "no"
                fs = self.side_fair(ex, side)
                if fs is None or fs < CARRY["fav_min"] - 1e-9:
                    continue
                t = self.ledger.tranches.get(sub) or {}
                if sub.startswith("dump:") and str(t.get("t") or "") >= ALLOC["since"]:
                    continue                                 # the catcher's slice
                avg = float(t.get("avg") or 0) or float(((self.book.get(ex) or {}).get("row") or {}).get("avgCost") or 0) or float(self.sig_mark(ex, side) or 0)
                dst = f"carry:{ex}:{side}"
                for oid, rec in self.ledger.orders.items():
                    if rec.get("ex") == ex and rec.get("sub") == sub and rec.get("status") in ("open", "live", "acked", "queued"):
                        rec["sub"] = dst
                qm = abs(self.ledger.move_sub(ex, sub, dst))
                tr = self.ledger.tranches.get(dst)
                if tr is None:
                    tr = self.ledger.tranches[dst] = dict(ex=ex, side=side, kind="carry", t=stamp(), ts=now, state="open", avg=avg, qty=0.0)
                qa = float(tr.get("qty") or 0.0)
                tr["avg"] = round((float(tr["avg"]) * qa + avg * qm) / (qa + qm), 5) if qa + qm > 0 else avg
                tr["qty"] = qa + qm
                if t:
                    t["state"] = "closed"
                moved.append(f"{self.name(ex)} {side.upper()} {qm:,.0f} ({sub.split(':')[0]})"); total += qm * avg
        with c.lock:
            c.d["c98"] = dict(done=stamp(), moved=len(moved), cost=round(total, 2))
            c.d["layers"].setdefault("carry", dict(state="off", since=now, caps={}, killed=None))["state"] = "full"
            c.save()
        self.ledger.save()
        msg = (f"C98 (alpha_spec §2, Piero 16:35 Oct 17): CARRY book — {len(moved)} favourite lots ({total:,.0f} at cost) held to settlement, no +0.5c exits: " + "; ".join(moved[:40]) +
               f". Every {CARRY['refresh_s'] // 60} min: sell EV/$ held <= {CARRY['sell_ev'] * 100:.1f}% at max(lot avg + 0.5c, best ask); buy resting bids on favourites (fair >= {CARRY['fav_min']:.2f}, "
               f">= {CARRY['min_daily']:,} shares/day) at the touch, <= fair - {CARRY['below_fair'] * 100:.0f}c, EV/$ >= {CARRY['buy_ev'] * 100:.0f}%; caps {CARRY['race_cap']:,.0f} a race, RI Senate "
               f"{CARRY['race_caps'].get('Rhode Island Senate', 0):,.0f}; after FMM's first 10k; swap a lot >= {CARRY['swap_pts'] * 100:.0f} points under a waiting buy when cash is short. Invariants unchanged.")
        self.event("CONTROL", f"C98: carry book — {len(moved)} lots, {total:,.0f} at cost (the full text is in the night log)"); self.night.line(msg)

    def nset_size(self, pb, books, ratio, max_q):
        """alpha_spec §4: an all-NO set over every book of a race pays >= n - 1 (n if an unlisted candidate wins). Walk each leg's YES bids
        (a NO bought at 1 - the YES bid) while the set's cost n - Σ worst bids stays <= ratio x (n - 1). -> (orders, qty, cost per set)."""
        n = len(pb.legs)
        cums = {}
        for ex in pb.legs:
            c, cums[ex] = 0.0, []
            for p_, q_ in books.get(ex) or []:
                c += q_
                cums[ex].append((c, p_))
        worst = lambda ex, q: next((p_ for c, p_ in cums[ex] if c >= q - 1e-9), None)
        best_q, wpx = 0, None
        for q in sorted({int(c) for ex in pb.legs for c, _ in cums[ex]} | {int(max_q)}):
            q = min(q, int(max_q))
            if q < 1:
                continue
            w = {ex: worst(ex, q) for ex in pb.legs}
            if None in w.values():
                break
            if n - sum(w.values()) > ratio * (n - 1) + 1e-9:
                break
            best_q, wpx = q, w
            if q >= max_q:
                break
        if best_q < MIN_QTY:
            return [], 0, 0.0
        orders = [(ex, "no", "buy", round(1 - wpx[ex], 3), int(best_q)) for ex in pb.legs]
        return orders, int(best_q), round(n - sum(wpx.values()), 4)

    def fmm_nsets(self, now, own):
        """alpha_spec §4 (Piero 16:35 Oct 17): every race's all-NO set (2 or more books) taken at a walked cost <= 0.985 x payout, as ONE atomic
        multi-leg order, only from idle cash (inside the 10k lock limit, after FMM's and carry's bids), never sold."""
        room = self.fmm_lock_room()
        if room <= 0:
            return 0
        ratio, shots = float(FMM["lock_cost"]), 0
        cands = []
        for pb in self.pairs:
            n = len(pb.legs)
            if n < 2 or len(set(pb.parties)) != n or now - getattr(pb, "last_nset", -1e9) < PAIR_COOLDOWN_S or pb.race in (FMM.get("nset_skip") or ()):
                continue
            if not all(self.controls.allows(ex, "pairs") for ex in pb.legs) or any(self.ex.is_frozen(ex) for ex in pb.legs):
                continue
            bids = [self.px.get(ex, (None, None, None))[0] for ex in pb.legs]
            if None in bids:
                continue
            cost = n - sum(bids)
            if cost > ratio * (n - 1) + 1e-9:
                continue
            cands.append((((n - 1) - cost) / cost, pb))
        for e_, pb in sorted(cands, key=lambda c: -c[0]):
            n = len(pb.legs)
            if room <= 0 or shots >= PAIR_MAX_SHOTS or self.ex.admit() < n or not api.spare_reads(20):
                break
            pb.last_nset = now
            try:
                feed_bks = {ex: self.feed_book(ex) for ex in pb.legs}
                for ex in [ex for ex in pb.legs if feed_bks[ex] is None]:
                    feed_bks[ex] = api.orderbook(ex)
            except Exception as err:
                self.event("ERR", f"{pb.race} n-set book: {str(err)[:120]}")
                continue
            books = {ex: [(p_, q_ - own.get((ex, "bids", round(p_, 3)), 0)) for p_, q_ in feed_bks[ex][0] if q_ - own.get((ex, "bids", round(p_, 3)), 0) > 0] for ex in pb.legs}
            max_q = int(min(FMM["lock_cap_sh"] - self.fmm_race_locked(pb.race, pb.legs), room / (ratio * (n - 1))))
            orders, qty, cost_set = self.nset_size(pb, books, ratio, max_q)
            if not orders:
                continue
            margin = (n - 1) - cost_set
            cost = qty * cost_set
            if cost > room + 1e-6 or qty * margin < PAIR_MIN_LOCK:
                continue
            room -= cost
            self.fmm_unread.append((time.time(), cost))      # the fill guard sees the lock's cash at once
            set_id = f"{pb.race}:{int(now)}:{uuid.uuid4().hex[:6]}"

            def recorded(results, pb=pb, qty=qty, margin=margin, n=n, set_id=set_id, ts=now, cost_set=cost_set):
                ids = [str(d.get("orderId")) for _, _, ok, d in results if ok and d.get("orderId") is not None]
                if len(ids) == n or api.DRY_RUN:            # the set exists once SIG acked every leg
                    with self.ledger.lock:
                        self.ledger.baskets.append(dict(id=set_id, t=stamp(), ts=ts, race=pb.race, side="no", qty=qty, margin=round(margin, 4),
                                                        cost=round(cost_set, 4), legs=n, orders=ids, repaired=False, state="held", h4=True, fmm=True))
            if self.ex.place([(pb.name, f"pairset:{set_id}", "set", o) for o in orders], ttl_s=PAIR_TTL_S, atomic=True, on_done=recorded):
                shots += 1
                msg = (f"FMM LOCK {pb.race} ({n} books): NO x {n} x{qty:,} at {' + '.join(f'{o[3]:.3f}' for o in orders)} = {cost_set:.3f} for a payout of {n - 1} "
                       f"(walked, <= {ratio:.3f} x payout, atomic) -> +{qty * margin:,.0f} at settlement, held; cost {cost:,.0f} (idle cash)")
                self.event("FMM", msg); self.night.line(msg)
                self.fmm_guard(now, "lock")
        return shots

    # ------------------------------------------------------------------------------------------------ C93 item 4: the H1 decisive test (Oct 13 04:00-15:00 PT)
    def h1_test_day(self, now):
        return time.strftime("%Y-%m-%d", time.localtime(now)) == H1_TEST["day"]

    def h1_skip_line(self, ex, side, now, msg):
        """C93c item 2: a self-trade skip / step-down inside the sleeve, logged once an hour per market-side."""
        d = getattr(self, "h1_skip_logged", None)
        if d is None:
            d = self.h1_skip_logged = {}
        if now - d.get((ex, side), 0) >= 3600:
            d[(ex, side)] = now
            self.event("H1", msg); self.night.line(msg)

    def h1_test_markets(self):
        return [str(x) for x in H1_SM_UNIVERSE[:int(H1_TEST["n_markets"])]]

    def h1_test_active(self, now):
        """True inside the window on the test day while the -150-at-fair stop has not fired."""
        hm = time.strftime("%H:%M", time.localtime(now))
        return self.h1_test_day(now) and H1_TEST["start"] <= hm < H1_TEST["end"] and not self.h1_test_stopped and not self.controls.d.get("h1_test_off")

    def h1_sleeve_reserve(self, now):
        """Item 4: 5,000 reserved ahead of the catcher from 03:45 to 15:00 on the test day, less what the test already holds / rests."""
        hm = time.strftime("%H:%M", time.localtime(now))
        if not self.h1_test_day(now) or not (H1_TEST["reserve_from"] <= hm < H1_TEST["end"]) or self.h1_test_stopped:
            return 0.0
        return max(0.0, float(H1_TEST["sleeve"]) - self.h1_cost())

    def h1_cost(self):
        """What the H1 test uses (C93b: touch-maker quotes only): TM inventory at cost in its markets + the notional of its resting TM entry quotes."""
        out = 0.0
        for ex in self.h1_test_markets():
            for side in ("yes", "no"):
                inv = self.tm_inventory(ex, side)
                if inv >= 1:
                    avg = (self.ledger.tranches.get(f"tm:{ex}:{side}") or {}).get("avg") or self.sig_mark(ex, side) or 0.5
                    out += inv * float(avg)
            for o in self.open:
                rec = self.ledger.orders.get(o["id"]) or {}
                if o["ex"] == ex and rec.get("layer") == "tm" and o["action"] == "buy" and not jx.reduces(o["side"], "buy", self.book.get(ex, {}).get("pos", 0)):
                    out += o["qty"] * o["price"]
        return out

    def h1_test_fills(self, now):
        """The test's own fills (SM entries and exits in its markets since the window opened on the test day)."""
        lt = time.localtime(now)
        t0 = time.mktime((lt.tm_year, lt.tm_mon, lt.tm_mday, int(H1_TEST["start"][:2]), int(H1_TEST["start"][3:]), 0, 0, 0, -1))
        mk = set(self.h1_test_markets())
        return [f for f in self.fills_since(t0) if f["ex"] in mk and f.get("layer") in ("tm", "tmexit")]   # C93b: the TM's fills

    def h1_test_pnl_fair(self, now):
        return sum(self.fill_pnl_fair(f) for f in self.h1_test_fills(now))

    def h1_test_check(self, now):
        """Every controller pass on the test day: the -150 stop at fair on the test's own fills; the window's open / close lines."""
        if not self.h1_test_day(now):
            return
        hm = time.strftime("%H:%M", time.localtime(now))
        st = self.controls.d.setdefault("h1_test", {})
        if H1_TEST["start"] <= hm < H1_TEST["end"] and not self.h1_test_stopped:
            pf = self.h1_test_pnl_fair(now)
            if pf < H1_TEST["stop_fair"] - 1e-9:
                self.h1_test_stopped = True
                with self.controls.lock:
                    st["stopped"] = stamp(); st["pnl_fair_at_stop"] = round(pf, 2); self.controls.save()
                msg = f"H1 TEST STOPPED: {pf:+,.0f} at fair on its own fills < {H1_TEST['stop_fair']:+,.0f}; back to control (the sleeve is released)"
                self.event("CONTROL", msg); self.night.line(msg); self.alerts.append(f"{time.strftime('%H:%M')} {msg}")
            if not st.get("opened"):
                with self.controls.lock:
                    st["opened"] = stamp(); self.controls.save()
                msg = (f"H1 TEST OPEN ({H1_TEST['start']}-{H1_TEST['end']} PT): GTC touch-maker quotes (the side below fair) on {', '.join(self.name(e) for e in self.h1_test_markets())} inside the {H1_TEST['sleeve']:,.0f} sleeve "
                       f"(clip {H1_TEST['clip']}, the TM 10k pause, the inventory stop and the ring stops lifted there (C93c); the spread maker stays exits-only; no sleeve bid at or through our own ask), exits per the TM rule GTC; stop {H1_TEST['stop_fair']:+,.0f} at fair; free cash {self.free_cash():,.0f}")
                self.event("CONTROL", msg); self.night.line(msg)
        elif hm >= H1_TEST["end"] and st.get("opened") and not st.get("closed"):
            with self.controls.lock:
                st["closed"] = stamp(); self.controls.save()
            fills = self.h1_test_fills(now)
            msg = f"H1 TEST CLOSED: {len(fills)} fills, {self.h1_test_pnl_fair(now):+,.0f} at fair, cost in use {self.h1_cost():,.0f}; the report follows (docs/h1_report_oct13.md)"
            self.event("CONTROL", msg); self.night.line(msg)

    def h1_shortfall_plan(self, now):
        """Item 4: at 03:15 on the test day, once: if free cash is under the 5,000 sleeve, {ex: shares} to sell at the best bid from the
        lowest-return trading lots (>= lot average + 0.5c), until the shortfall is covered or the edge given up reaches ~400."""
        if self.h1_shortfall_done or not self.h1_test_day(now):
            return {}
        hm = time.strftime("%H:%M", time.localtime(now))
        if not (H1_TEST["shortfall_at"] <= hm < H1_TEST["reserve_from"]):
            return {}
        self.h1_shortfall_done = True
        short = float(H1_TEST["sleeve"]) - self.free_cash()
        if short <= 0:
            self.night.line(f"H1 shortfall check 03:15: free {self.free_cash():,.0f} >= the {H1_TEST['sleeve']:,.0f} sleeve: nothing to sell")
            return {}
        cands = []
        for ex, side, sub, q, avg, fs, cost in self.engine_lots():
            if (ex, sub) in self.core_set or fs is None:
                continue
            bid = bots.side_book(*self.px.get(ex, (None, None, None))[:2], side)[0]
            if bid is None or bid < bots.tick_up(round(avg + RULE0_MARGIN, 6)) - 1e-9:
                continue
            cands.append(((fs - bid) / bid, ex, side, sub, q, bid, fs))
        out, raised, given = {}, 0.0, 0.0
        for ret, ex, side, sub, q, bid, fs in sorted(cands):
            if raised >= short or given >= H1_TEST["shortfall_max_cost"]:
                break
            take = int(min(q, (short - raised) / bid, max(0.0, (H1_TEST["shortfall_max_cost"] - given) / max(fs - bid, 1e-6))))
            if take < MIN_QTY:
                continue
            out[ex] = out.get(ex, 0) + take
            raised += take * bid; given += take * (fs - bid)
        self.night.line(f"H1 shortfall sale 03:15: free {self.free_cash():,.0f}, short {short:,.0f} of the sleeve -> {len(out)} markets at the best bid, ~{raised:,.0f} raised, ~{given:,.0f} of settlement value given up (cap {H1_TEST['shortfall_max_cost']:,.0f})")
        return out

    def repeg_grace(self, b, cancels, places, now):
        """C93 item 3: a re-peg (a cancel paired with a new want on the same layer / sub / side) waits until the order has sat off its
        target for > 10 min; a pull (no replacement) goes at once. -> (cancels, places) to act on now."""
        nsub = lambda o: str(o.get("sub") or "").rsplit(":", 1)[0] if o.get("layer") == "fmm" else o.get("sub")   # FMM bids: one sub-book per price
        key = lambda o: (o.get("layer"), nsub(o), o.get("side"), o.get("action"))
        want_keys = collections.Counter(key(w) for w in places)
        keep_c, deferred = [], collections.Counter()
        for o in cancels:
            k = key(o)
            if o.get("layer") in SELL_KEEP_LAYERS and o.get("id") and want_keys.get(k):   # C100 item 3 (Piero 17:15 Oct 18): post once and keep place
                w = next((w for w in places if key(w) == k), None)
                old_ask, new_ask = 1 - float(o["price"]), 1 - float(w["px"])
                if abs(new_ask - old_ask) >= 1e-9:
                    t_u = self.sell_undercut.get(o["id"])
                    fs_ = self.side_fair(str(o["ex"]), "no" if o["side"] == "yes" else "yes")
                    if (o.get("layer") == "release" and new_ask > old_ask and SELL_EV_MAX < 1 and fs_ is not None
                            and old_ask < bots.tick_up(round(fs_ / (1 + SELL_EV_MAX), 6)) - 1e-9):
                        keep_c.append(o)                    # C101 item C: an ask under fair / 1.05 (a sale above 5% EV/$) moves up to it at once
                        continue
                    if new_ask > old_ask or t_u is None or now - t_u < SALE_REPEG_S:
                        want_keys[k] -= 1
                        deferred[k] += 1
                        continue                            # never up; down only after the others' best ask sat below ours >= 5 min
                    keep_c.append(o)
                    continue
            if o.get("layer") == "fmmexit":                 # fmm_spec v2 §2.3: an FMM exit follows the best ask at once
                keep_c.append(o)
                continue
            if o.get("id") and want_keys.get(k):
                t0 = self.drift_since.setdefault(o["id"], now)
                if now - t0 < REPEG_AWAY_S:
                    want_keys[k] -= 1
                    deferred[k] += 1
                    continue                                # the order keeps its place a little longer
            keep_c.append(o)
        keep_p = []
        for w in places:
            k = key(w)
            if deferred.get(k):
                deferred[k] -= 1
                continue                                    # the replacement waits with it
            keep_p.append(w)
        return keep_c, keep_p

    def h4_active(self):
        """H4 runs unless paused (Piero 17:35: paused until H1's verdict)."""
        return bool(H4.get("on")) and not (self.controls.d.get("edge") or {}).get("h4_paused")

    def h4_race_ok(self, pb):
        """H4: a two-candidate race (D and R only, no independent / third candidate listed)."""
        if len(pb.legs) != 2:
            return False
        parties = {str((self.meta.get(ex) or {}).get("party") or "") for ex in pb.legs}
        if parties != {"D", "R"}:
            return False
        race = (self.meta.get(pb.legs[0]) or {}).get("race")
        return not any(m.get("race") == race and str(m.get("party") or "") not in ("D", "R") for m in self.meta.values())

    def h4_budget(self, now):
        """H4: cash for pair locks = min(10k cap - used, free - the catcher's 10k reserve), and 0 while an entry with >= 6% edge waits."""
        used = sum(bk["qty"] * (bk["legs"] - bk["margin"]) for bk in self.ledger.baskets if bk.get("h4") and bk.get("state", "held") == "held")
        self.h4_used = used
        waiting = max([e for e, _, _ in self.dump_cut_rows] + [e for e, _, _ in self.tm_unfunded] + [0.0])
        if waiting >= H4["waiting_edge"] - 1e-9 or self.wd_after("locks_stop", now):
            return 0.0                                       # C103: no new two-book locks from Oct 29 (their cash comes back Nov 4)
        return max(0.0, min(H4["cap"] - used, self.free_cash() - H4["idle_free"]))

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
        try:
            self.day_line(time.time(), cash)
        except Exception as err:
            self.event("ERR", f"day line: {err!r}")
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

    def day_line(self, now, cash):
        """Day 6 §7: one RUN_LOG line per hour 07:00 -> 15:00 PT: big-event value on the tape, our catches + touch fills
        (cost, edge vs fair), capture %, round trips, realized, minutes cash-constrained, free cash at the start of the
        hour, ratchet level, writes/min, 429s."""
        hh = time.localtime(now).tm_hour
        start_cash = getattr(self, "hour_start_cash", None)
        self.hour_start_cash = cash - CASH_RESERVE
        if not (7 <= hh <= 15):
            return
        t0 = now - 3600
        evs = self.events_between(t0, now)
        ev_value = sum(e.get("value") or 0 for e in evs)
        buys = [f for f in self.fills_since(t0) if f["layer"] in ("dump", "touch") and f["action"] == "buy"]
        cost = sum(f["qty"] * f["price"] for f in buys)
        edge = sum(f["qty"] * ((f.get("fair") if f.get("fair") is not None else f["price"]) - f["price"]) for f in buys)
        trips = sum(1 for t in self.ledger.tranches.values() if t.get("kind") in ("dump", "touch") and t.get("state") == "closed"
                    and (t.get("closed_ts") or 0) >= t0)
        realized = sum(f.get("realized") or 0 for f in self.fills_since(t0) if f["layer"] in ("dumpexit", "touchexit"))
        h = api.health(3600)
        wr = h.get("writes_last_min")
        r = (self.controls.d.get("ratchet") or {}).get("dump") or {}
        rel = [f for f in self.fills_since(t0) if f["layer"] == "release" and f["action"] == "buy"]
        tiers = collections.Counter()
        ev_up = 0.0
        for f in rel:
            tiers[self.release_tier_of(f)] += round((1 - f["price"]) * f["qty"])
            if f.get("fair") is not None:
                ev_up += max(0.0, (1 - f["fair"]) - (1 - f["price"])) * f["qty"]
        T = float((self.controls.d.get("cash_target") or {}).get("T") or CASH_T_START)
        line = (f"DAY {hh:02d}:00 · tape big-event value {ev_value:,.0f} · ours: {len(buys)} catches ({sum(1 for f in buys if f['layer'] == 'touch')} touch), "
                f"cost {cost:,.0f}, edge vs fair {edge:+,.0f} · capture {100 * edge / ev_value if ev_value else 0:.1f}% · round trips {trips}, realized {realized:+,.0f} · "
                f"cash-constrained {self.constrained_minutes(t0)} min · free cash at the hour start {'n/a' if start_cash is None else f'{start_cash:,.0f}'} · T {T:,.0f} · "
                f"release sold {dict(tiers) or 'none'} (EV given up {ev_up:,.0f}) · rule 0 held back {self.rule0_blocked_value(t0):,} · "
                f"ratchet dump L{r.get('level', 0)} · sides uncovered > 30 s: {sum(1 for t_, *_ in self.uncovered_n if t_ >= t0)} · "
                f"party D {dash4.party_exposure(self)['D']:,.0f} / R {dash4.party_exposure(self)['R']:,.0f} · writes/min now {wr}, 429s last hour {h.get('throttled', 0)}")
        self.night.line(line)

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
        if not stopped and self.last_state is not None and time.time() - getattr(self, "dash_t", 0.0) < DASH_EVERY_S:
            return                                          # Piero 21:15 item 3: built once a minute (docs/latency.md: ~4.5 s of the ~6 s fill -> next order)
        self.dash_t = time.time()
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
        for attempt in range(3):                            # a dict the executor thread grows mid-iteration raises RuntimeError: rebuild (C82)
            try:
                self.last_state = self.dash_state(stopped)  # read by the dashboard thread; never mutated after this
                break
            except RuntimeError as e:
                if attempt == 2 or "changed size" not in str(e):
                    self.event("ERR", f"dashboard state: {e!r}")
                    break
            except Exception as e:                          # the dashboard must never stop trading
                self.event("ERR", f"dashboard state: {e!r}")
                break
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
                      if self.strategy_of(f["bot"], f["layer"]) not in ("position", "old engine") and f["layer"] not in D5_EXCLUDE_LAYERS)
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
              "touch": ("Touch (big sellers)", "Joins the SIG best bid on >= 93% favourites priced 6c+ under fair; sells at entry + 1.5c or holds to settlement"),
              "wall": ("Wall (manual pilot)", "One big resting order placed by Piero through a control; buy-back at his price, held after 6 h, pulled on a 2c Polymarket move"),
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
        if f["layer"] in ("dumpexit", "touchexit", "scalpexit"):   # day 6 §5.3: both numbers, the first is what the leaderboard sees
            sub = (self.ledger.orders.get(f["order"]) or {}).get("sub")
            avg = (self.ledger.tranches.get(sub) or {}).get("avg")
            vs = "" if avg is None else f"{(px - avg) * 100:+.1f}c vs entry · "
            under = "" if fair is None else f" · {(fair - px) * 100:.1f}c under fair"
            return f"{label} sold back {f['qty']:,.0f} {side.upper()} on {self.name(f['ex'])} at {px:.3f}: {vs}realized {f.get('realized') or 0:+,.0f}{under}"
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
        try:
            v5 = dash5.build(self, api, v4)
        except Exception as err:
            self.event("ERR", f"dashboard v5 payload: {err!r}")
            v5 = {}
        return dict(**v5, sigday=v4.get("sigday"), status=v4.get("status"), daily4=v4.get("daily"), feed=v4.get("feed"), strategies4=v4.get("strategies"),
                    capital4=v4.get("capital"), holdings4=v4.get("holdings"), leaders4=v4.get("leaders"), admin4=v4.get("admin"),
                    money=money, daily=daily, series=self.series_view(), capital=capital, strategies=strategies,
                    coverage=cov, dump_coverage=self.dump_cov,
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
        try:
            book = dash5.market_book(self, ex)
        except Exception:
            book = {}
        return dict(orders=[{k: o.get(k) for k in ("placed", "bot", "layer", "side", "action", "price", "qty", "filled", "status")} for o in orders],
                    fills=[{k: f.get(k) for k in ("t", "bot", "layer", "side", "action", "price", "qty", "edge", "mo30")} for f in fills], book=book)

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
                c.d["auto_pause"] = dict(history={})         # C95d: Piero's resume clears the 3x-in-2-h lock
                c.save()
            self.eng.cash_short = None
            self.eng.trims_seen, self.eng.trim_t = sum(self.eng.ex.trims.values()), None   # C95d: only a trim AFTER the click can pause again
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
            if layer in RETIRED_LAYERS and state != "off":
                raise ValueError(f"{layer} was deleted in C87 (Piero 19:56 Oct 8): it stays off")
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
        if action == "travel":                             # day 6 §4.2: ./travel.sh [off]
            return self.eng.travel(bool(args.get("on", True)))
        if action == "wall":                               # Piero's manual wall: {ex, side, price, qty, exit_px, ttl_h=24, hold_h=6, news_c=0.02}
            ex, side = str(args.get("ex")), str(args.get("side", "")).lower()
            if ex not in self.eng.owner or side not in ("yes", "no"):
                raise ValueError("wall needs a known market (ex) and side yes/no (the side we BUY)")
            px, qty, exit_px = float(args["price"]), int(args["qty"]), float(args["exit_px"])
            if not (0.01 <= px <= 0.99) or not (0.01 <= exit_px <= 0.99) or qty < MIN_QTY:
                raise ValueError("wall price / exit_px must be 0.01-0.99 and qty >= 10")
            if c.d.get("ri_freeze") and (self.eng.meta.get(ex) or {}).get("race") in RI_RACES:
                raise ValueError("RI is frozen (day 10 D1): no new RI walls")
            sell_px = (1 - exit_px) if side == "no" else exit_px          # what the held side would fetch on the buy-back
            holding = args.get("exit_min_free") is not None or sell_px > px + WALL_RECYCLE_EXIT_C + 1e-9 or float(args.get("hold_h", 6)) >= float(args.get("ttl_h", 24))
            if holding:                                                   # day 10 §4.1: a hold-to-settlement wall needs free >= 30k after it, netted
                free_now = (min(self.eng.cash_hist) if self.eng.cash_hist else 0.0) - CASH_RESERVE
                others = sum((x["qty"] - (x.get("filled") or 0)) * x["px"] for x in self.eng.walls() if x.get("state") == "active" and not (x.get("once") and (x.get("filled") or 0) >= 1))
                if free_now - others - qty * px < WALL_HOLD_MIN_FREE:
                    raise ValueError(f"a hold-to-settlement wall needs free cash >= {WALL_HOLD_MIN_FREE:,} after a full fill (free {free_now:,.0f}, other walls {others:,.0f}, this {qty * px:,.0f}): refused (day 10 §4.1)")
            wid = f"w{int(time.time())}"
            w = dict(id=wid, ex=ex, side=side, px=round(px, 3), qty=qty, exit_px=round(exit_px, 3), ttl_s=int(float(args.get("ttl_h", 24)) * 3600),
                     hold_s=int(float(args.get("hold_h", 6)) * 3600), news_c=float(args.get("news_c", WALL_NEWS_C)), t=time.time(), state="active",
                     filled=0.0, fill_t=None, by=str(args.get("by", "Piero")),
                     min_free=float(args.get("min_free", WALL_MIN_FREE)), min_edge=(None if args.get("min_edge") is None else float(args["min_edge"])),   # C76: per-wall cut and edge floor
                     exit_min_free=(None if args.get("exit_min_free") is None else float(args["exit_min_free"])),   # C77: buy-back rests only while free < this (else held)
                     once=bool(args.get("once", False)))                                                            # C77: one fill only: never re-rested after a fill
            with c.lock:
                c.d.setdefault("walls", []).append(w)
                if c.state("wall") == "off":
                    c.d["layers"].setdefault("wall", dict(state="off", caps={}, killed=None))["state"] = "full"
                c.save()
            msg = (f"WALL {wid} placed by {w['by']}: {self.eng.name(ex)} buy {qty:,} {side.upper()} at {px:.3f} (= sell {'YES' if side == 'no' else 'NO'} at {1 - px:.3f}), "
                   f"TTL {w['ttl_s'] / 3600:.0f} h; buy-back YES bid {exit_px:.3f} for what fills, held after {w['hold_s'] / 3600:.0f} h; pulled if Polymarket moves {w['news_c'] * 100:.0f}c against it in 10 min; "
                   f"cut so a full fill leaves >= {w['min_free']:,.0f} free" + ("" if w["min_edge"] is None else f"; rests only while the edge vs fair >= {w['min_edge'] * 100:.0f}c")
                   + ("" if w["exit_min_free"] is None else f"; fills held to settlement, the buy-back rests only while free < {w['exit_min_free']:,.0f}") + ("; one fill only" if w["once"] else ""))
            self.eng.event("WALL", msg)
            self.eng.night.line(msg)
            return msg
        if action == "rollback_request":                   # dashboard v5 Admin: queue a release rollback; the Mac's mirror pull runs push_release.sh for it
            rel = str(args.get("release") or "")
            if not re.match(r"^C[0-9A-Za-z]+-\d{4}$", rel):
                raise ValueError("release must look like C86c-1741")
            with c.lock:
                c.d["rollback_request"] = dict(release=rel, state="pending", t=stamp(), id=str(int(time.time())))
                c.save()
            self.eng.event("CONTROL", f"ROLLBACK requested from the dashboard: {rel} (the Mac pushes it on its next mirror pull)")
            return f"rollback to {rel} queued: the Mac pushes it on its next mirror pull"
        if action == "sell_now":                           # Piero 17:15 Oct 8: an immediate sale, fill-or-cancel: {ex, side, px (held-side), qty, clip=5000, ttl_s=10}
            ex, side = str(args.get("ex")), str(args.get("side", "no")).lower()
            px, qty = float(args["px"]), int(args["qty"])
            if ex not in self.eng.owner or side not in ("yes", "no") or not (0.01 <= px <= 0.99) or qty < MIN_QTY:
                raise ValueError("sell_now needs a known market, side yes/no, px 0.01-0.99 (the held side's sale price) and qty >= 10")
            with c.lock:
                c.d["sell_now"] = dict(ex=ex, side=side, px=px, qty=qty, clip=int(args.get("clip", 5_000)), ttl_s=float(args.get("ttl_s", 10)),
                                       subs=list(args.get("subs") or []), state="pending", t=stamp())
                c.save()
            return f"sell_now {self.eng.name(ex)}: {qty:,} {side.upper()} at {px:.3f}, fill-or-cancel, queued for the next loop"
        if action == "wall_cancel":
            wid = str(args.get("id"))
            with c.lock:
                for w in c.d.get("walls") or []:
                    if w["id"] == wid and w.get("state") == "active":
                        w["state"] = "cancelled"
                c.save()
            self.eng.event("WALL", f"WALL {wid} cancelled by control")
            return f"wall {wid} cancelled"
        if action == "dedup_fills":                        # Oct 5: undo synthetic fills that duplicated SIG's fill (dry_run lists them)
            dry = bool(args.get("dry_run", True))
            fixed = self.eng.ledger.dedup_synthetic(dry_run=dry, window_s=int(args.get("window_s") or 0))
            if (fixed["removed"] or fixed["netted"]) and not dry:
                self.eng.ledger.save()
                self.eng.event("AUDIT", f"DEDUP by control: {len(fixed['removed'])} synthetic fills removed, {len(fixed['netted'])} phantom pairs netted")
            return (f"{'would remove' if dry else 'removed'} {len(fixed['removed'])} duplicated synthetic fills; {'would net' if dry else 'netted'} "
                    + "; ".join(f"{ex} {sub} {old:+,.0f} -> {new:+,.0f}" for ex, sub, old, new in fixed["netted"][:20]))
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
        if action == "election_mode":                      # C102 (night item 5): off / shadow / live
            m = str(args.get("mode") or "")
            if m not in ("off", "shadow", "live"):
                raise ValueError("mode: off | shadow | live")
            with c.lock:
                c.d.setdefault("election", {})["mode"] = m
                c.save()
            msg = f"ELECTION mode -> {m} by control (Piero)" + ("" if m != "live" else ": the call sniper sends orders from now")
            self.eng.event("NIGHT", msg); self.eng.night.line(msg)
            return msg
        if action == "election_inject":                    # C104 (night item 10, the Oct 25 rehearsal): a synthetic PM path into one book, shadow only
            st = c.d.get("election") or {}
            if self.eng.el_mode_cached == "live" or st.get("mode") == "live":
                raise ValueError("election_inject runs in shadow only")
            ex = str(args.get("ex") or "")
            path = [[float(a), float(b), float(c_)] for a, b, c_ in (args.get("path") or [])]
            if ex not in self.eng.owner or not path or len(path) > 2000:
                raise ValueError("election_inject needs a known ex and a path [[seconds from now, PM YES bid, PM YES ask], ...] (<= 2000 points)")
            with c.lock:
                c.d.setdefault("election", {})["inject"] = dict(ex=ex, path=path, close_now=bool(args.get("close_now", True)), t=stamp())
                c.save()
            return f"election_inject queued: {len(path)} quotes into {self.eng.name(ex)} over {path[-1][0]:.0f} s (shadow; polls treated as closed: {bool(args.get('close_now', True))})"
        if action == "election_stop":                      # the kill switch: done by the loop (entries, call snipes, drift pulled; exits kept)
            with c.lock:
                c.d.setdefault("election", {})["stop_req"] = stamp()
                c.save()
            return "election_stop queued for the next loop: entries, call snipes and drift pulled, exits and foreign orders kept, new risk paused"
        if action == "election_rearm":
            race = str(args.get("race") or "") or None
            with c.lock:
                c.d.setdefault("election", {})["rearm_req"] = dict(t=stamp(), race=race)
                c.save()
            return f"election_rearm queued for the next loop: {race or 'every race and the kill switch'}"
        if action == "fmm_restart":                        # Piero 01:05 Oct 18 item 2: FMM back on after a stop (cap / K as given)
            st = c.d.setdefault("fmm", {})
            if st.get("decision") != "live":
                raise ValueError("FMM is not live (no gate decision yet): nothing to restart")
            cap = float(args.get("inv_cap") or st.get("inv_cap") or FMM["inv_cap_live"])
            k = float(args.get("K") or st.get("K") or FMM["k_live"])
            if not (0 < cap <= 60_000 and 1 <= k <= 5):
                raise ValueError("inv_cap must be in (0, 60k] and K in [1, 5]")
            with c.lock:
                was = st.pop("stopped", None)
                st["inv_cap"], st["K"] = cap, k
                st.setdefault("restarts", []).append(dict(t=stamp(), was=was, inv_cap=cap, K=k, by="dashboard"))
                st["trims0"] = sum(v for k_, v in self.eng.ex.trims.items() if k_ in ("fmm", "fmmtake", "fmmexit", "fmmhedge"))
                c.d["layers"].setdefault("fmm", dict(state="off", since=time.time(), caps={}, killed=None))["state"] = "full"
                c.save()
            msg = f"FMM RESTARTED by control (Piero): inventory cap {cap:,.0f}, K {k:g}; was stopped: {(was or {}).get('why', 'not stopped')}"
            self.eng.event("FMM", msg); self.eng.night.line(msg)
            return msg
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
        tok = None                                          # C95d: reuse the token across restarts, so an open page reconnects by itself
        try:
            m_ = re.search(r"[?&]t=([A-Za-z0-9_\-]{16,})", open(DASHBOARD_URL_FILE).read())
            tok = m_.group(1) if m_ else None
        except OSError:
            pass
        app = dashboard.create_app(lambda: eng.last_state, EngineControls(eng), market_detail=eng.market_detail, token=tok)
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
                try:
                    eng.fast_step(time.time())              # mm_plan §2: act on our fills / PM moves within ~1 s, before the full loop
                except Exception as e:
                    eng.event("ERR", f"fast path: {e!r}")
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
