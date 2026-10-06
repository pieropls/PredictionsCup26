"""A. CONFIG: every setting of the pipeline.

Files, in the order data flows through them:
  a_config.py      settings (this file)
  b_sig.py         SIG Super Market API: reads, batch + atomic multi-leg orders, write budget
  c_polymarket.py  Polymarket fair prices for every SIG market
  d_ledger.py      record of every order, fill and arb basket (data/ledger.json)
  e_bots.py        the bots: PositionBot (manual positions), MarketBot (one per market), PairBot (one per race)
  f_engine.py      THE LOOP. Run: python3 -u f_engine.py --live      (dry run without --live)
  g_dashboard.py   writes data/dashboard.html every loop
  h_status.py      terminal summary: python3 h_status.py
  data/            everything the engine writes;  tests/  offline tests: python3 -m unittest discover tests
"""
import os

# A1. connection --------------------------------------------------------------------------------------------------
BASE_URL = "https://sig.thesuper.market/api/v1"
def _api_key():
    """SIG_API_KEY from the environment (the VM), else ~/.config/sig/api_key (chmod 600). Never stored in the code."""
    if os.environ.get("SIG_API_KEY"):
        return os.environ["SIG_API_KEY"].strip()
    path = os.path.expanduser("~/.config/sig/api_key")
    return open(path).read().strip() if os.path.exists(path) else ""


API_KEY = _api_key()                                        # My Profile -> API Keys (read + trade)
TOURNAMENT = "midterm-elections"
DRY_RUN = True                  # f_engine.py --live switches this off
ENGINE_HOST = os.environ.get("SIG_ENGINE_HOST", "MacBookPino")   # the ONE machine allowed to run --live, compared
                                # with `scutil --get LocalHostName` (or data/host_id), never the Wi-Fi-derived hostname

# A2. engine loop and SIG limits (100 reads + 30 writes / min per account; SIG processes orders slowly under load) ----
LOOP_S = 15
FILL_POLL_S = 10                # day 2 §6: SIG sends no account_batch -> REST fills at least every 10 s
WRITES_PER_MIN = 25
BATCH_SIZE = 20                 # orders per batch write
MAX_PLACE_PER_LOOP = 60         # new orders per loop
SETTLE_S = 90                   # after a write with an unknown outcome: nothing new on those markets for this long
PM_MAX_AGE_S = 90               # Polymarket data older than this: no new market-bot orders
TICK, MIN_QTY, QTY_TOL = 0.005, 10, 0.25
READ_TIMEOUT_S = 45             # SIG's authenticated reads take 18-34 s under load (Oct 1 night); 15 s was too short
INITIAL_BALANCE = 100_000

# A3. account risk (checked before every order: filled + resting + this loop's new orders) -------------------------
RISK_MAX_EXPOSURE = 90_000      # SIG of exposure across market + pair bots (at Polymarket fair, at cost without one)
CASH_RESERVE = 5_000            # night plan §4.2 hard floor: free cash after every resting engine entry buy
MARKET_MAX_POS = 3_000          # SIG per market, per side

# A4. position bots: manual positions, worked out with resting limit exits -------------------------------------------
# exits: (price in the HELD side's terms, fraction of the position), filled top-down so a sweep takes the cheapest
# first; exits below the Polymarket fair are held back.  adds: (price, SIG notional) far bids that add on spikes.
POSITION_BOTS = {
    "AK-GOV-D": dict(market=368, exits=[(0.30, .15), (0.40, .20), (0.50, .20), (0.60, .20), (0.775, .25)], adds=[(0.10, 300)]),
    "RI-GOV-D": dict(market=384, exits=[(0.97, .50), (0.98, .50)], adds=[]),
}
POSITION_MAX_ADD = 1.5
MANUAL_MARKETS = []             # SIG market ids no bot may touch

# A5. market bots: one per market, every price checked against Polymarket AND the SIG book --------------------------
MB_PM_MAX_SPREAD = 0.04         # no Polymarket reference when its own spread is wider
MB_EXIT_EDGE = 0.01             # inventory offered back at max(PM fair + this, SIG best price - 1 tick); never > held
MB_TOUCH_PM_EDGE = 0.015        # touch: join/improve the SIG best bid, only below SIG mid AND below PM fair - this
MB_TOUCH_NOTIONAL = 100
MB_TAKE_EDGE = 0.04             # take: SIG has gapped this far through PM fair -> buy the gap (marketable limit)
MB_TAKE_NOTIONAL = 150
MB_LADDER = [(0.10, 150), (0.20, 250)]   # sweep catchers: resting at PM fair -/+ distance (SIG notional)
MB_FOCUS = 40                   # markets that get touch + ladder (most room in the SIG book); every market gets exit/take
MB_REFILL_S = 300               # after a fill, that side waits this long before adding again
MB_QUOTE_TTL_S = 900            # market-bot orders expire on SIG after this long (backstop if the engine stalls)
MB_MIN_PX, MB_MAX_PX = 0.01, 0.99

# Polymarket lists these races only by candidate: SIG (race, party) -> PM market ids summed
PM_EXTRA = {
    ("Alaska Governor", "D"): ["635016", "635014"],                              # Kreiss-Tomkins, T. Begich
    ("Alaska Governor", "R"): ["635012", "635004", "635011", "635003", "635006"],  # Wilson, Bronson, Taylor, Bishop, Dahlstrom
    ("Alaska Senate", "D"): ["634976"],                                          # Peltola
    ("Alaska Senate", "R"): ["634974"],                                          # Sullivan
    ("Nebraska Senate", "I"): ["634893"],                                        # Osborn
}

# A5b. strategy layers v2 (night plan §4-5; plan_review A2-A6, A9) -----------------------------------------------------
ROC_MIN, ROC_REF = 0.04, 0.08   # D4: entries need edge/price >= 4%; size scales with roc / 8%, clipped 0.5x..2x
ROC_MIN_TIGHT = 0.06            # day 2 §3.4: while free cash is below its target; also MM on a held side with fair >= 0.95
ROC_HOLD_MIN = 0.10             # afternoon §2.3: hold-to-settlement entries (classic MM v2) need ROC >= 10%
SETTLE_DATE = "2026-11-04"      # release order: expected return per day to settlement (all markets settle after Nov 3)
RECYCLE_BID_EDGE = 0.005        # recycler: sell into the SIG bid while it is >= fair_side + this
RECYCLE_REST_EDGE = 0.01        # ... else rest an exit at max(fair_side + this, best ask - 1 tick)
RECYCLE_TAKE_TTL_S, RECYCLE_REST_TTL_S = 300, 1800
MM_EDGE, MM_U_MULT, MM_NOTIONAL, MM_TTL_S = 0.02, 1.5, 100, 1800     # e(p) = max(2c, 1.5u) * s(p); base SIG per quote
LADDER_LEVELS = [(0.10, 150), (0.20, 250)]   # (base offset x s(p), base SIG notional)
LADDER_TTL_S = 4 * 3600
LADDER_CAPACITY_SHARE = 0.4     # ladder placements use at most this share of SIG's measured processing capacity
TAKE_EDGE, TAKE_U_MULT, TAKE_NOTIONAL, TAKE_TTL_S = 0.04, 1.5, 150, 300   # take gaps >= max(4c*s(p), 1.5u)
FAIR_HALF_LIFE_S, FAIR_JUMP, FAIR_CONFIRM_S = 20.0, 0.015, 5.0       # A6: EWMA mid; a >= 1.5c move must persist 5 s
PM_MIN_LIQUIDITY = 2_000        # A6: no fair when Polymarket liquidity is below this
REF_WINDOW_S, REF_MIN_SAMPLES, REF_MAX_SPREAD = 1800, 6, 0.10       # ladder anchor: 30-min median SIG mid
BREAKER_WINDOW_S, BREAKER_MARKETS, BREAKER_SHARE, BREAKER_PAUSE_S = 600, 5, 0.8, 1800   # §4.5 correlated-fill breaker
BREAKER_FAIR_MOVE = 0.01        # ... and only if Polymarket fair moved >= 1c against those fills (news), not on MM flow
MM_PILOT_MARKETS, LADDER_PILOT_MARKETS, PILOT_S = 10, 40, 2 * 3600
REQUOTE_CANCELS_PER_MIN = 20    # A9: MM/ladder requote cancels per minute (one per write); picked-off quotes always go
# turnover MM (afternoon D-C): quote at SIG's own level, Polymarket as a guardrail, inventory kept moving
TMM_MID_HALF_LIFE_S = 30        # SIG mid EWMA (the microprice needs top-of-book sizes, which /exchanges/prices doesn't give)
TMM_BUY_EDGE = 0.01             # never buy above fair_side - 1c
TMM_KAPPA_MAX = 0.04            # never sell below fair_side - min(4c, |SIG 24 h median - fair| + 1c)
TMM_SIZE = {"A": 1_500, "B": 300}   # SIG per quote
TMM_CAP_SIDE = 5_000            # inventory at cost per market per side
TMM_SKEW_TICKS = 3              # skew grows linearly to this at 50% of TMM_CAP_SIDE
TMM_OFFER_MID_S, TMM_RELEASE_S = 2 * 3600, 6 * 3600   # older inventory: offered at SIG mid; then to the release layer
TMM_TTL_S = 900
# scalper (day 5 §2, "Mode A"): the turnover MM redesigned; it runs only on cash the dump catcher isn't using
SCALP_MARKETS, SCALP_PILOT_MARKETS = 20, 10   # the highest-traded markets over 24 h (our own tape)
SCALP_BUY_EDGE = 0.02           # join the best bid (improve it by a tick when the spread is >= 2 ticks) only at <= fair_side - 2c
SCALP_IMPROVE_SPREAD = 2        # ticks
SCALP_CLIP_MIN, SCALP_CLIP_MAX = 300, 500
SCALP_MAX_BUYS = 2              # resting buys per market
SCALP_SELL_MIN = 0.005          # ask at best ask - 1 tick, never below entry + 0.5c
SCALP_STEP1_S, SCALP_STEP1_GAP = 1800, 0.04   # after 30 min: entry + 0 if that is still >= fair_side - 4c
SCALP_HAND_S = 2 * 3600         # after 2 h: the dump catcher's tranche rules (held to settlement if not sold)
SCALP_INV_MARKET = 3_000        # inventory at cost per market
SCALP_SLICE_MAX = 20_000        # the slice never grows past this (ratchet, idle-cash rule)
SCALP_YIELD_RESERVE = 15_000    # no new buys while the dump catcher's funded reserve < 15k
SCALP_WRITES_PER_MIN = 6        # placements per minute for the layer (entries wait; the plan freezes at the cap)
SCALP_TTL_S = 900
SCALP_PILOT_S = 2 * 3600
SCALP_GATE_TRADES, SCALP_GATE_PACE = 60, 3.0   # gate: realized > 0, mark-out 30m >= 0, >= 60 trades, >= 3x/day turnover of the slice
# no idle cash (day 5 §3): capital use = (tranches + scalper inventory + takes) / free cash above the floor
IDLE_USE_LOW, IDLE_USE_HIGH, IDLE_HOLD_S = 0.40, 0.70, 2 * 3600
IDLE_SLICE_STEP, IDLE_ROC_MIN = 5_000, 0.08    # step 1: scalper slice +5k (max 20k); step 2: takes at ROC >= 8% for 2 h

# A6. pair bots: one per race. NO on every party pays >= legs-1; YES on every party pays 1 unless an outsider wins ---
PAIR_ENABLED = True
PAIR_EDGE = 0.02                # locked per set at the margin (A12: >= 2c)
PAIR_THIRD_PARTY = 0.01         # YES sets price the outsider at max(this, 1 - sum of Polymarket fairs)
PAIR_MAX_QTY = 3_000            # shares per leg per shot
PAIR_MAX_SHOTS = 3              # sets sent per loop (each costs ~30 s of SIG time: book reads + an atomic write)
PAIR_MIN_LOCK = 3.0             # skip sets locking less than this many SIG (qty x margin): not worth the loop time
PAIR_TTL_S = 120                # unfilled leg remainders expire; must exceed SIG's processing delay (§9.3/9.5: up to ~40 s)
PAIR_COOLDOWN_S = 30            # per race

# A6b. night plan (docs/night_plan.md): capital buckets per layer, (pilot, full, max) in SIG. The dashboard can set a
# layer's cap anywhere in 0..max; without an override the cap follows the layer's state (pilot / full) ----------------
LAYER_CAPS = {
    "mm": {"inventory": (10_000, 25_000, 40_000)},             # inventory at cost (afternoon D-C: turnover MM, 25k bucket)
    "ladder": {"filled": (3_000, 15_000, 25_000), "resting": (10_000, 60_000, 80_000)},
    "pairs": {"capital": (3_000, 10_000, 15_000)},
    "sniper": {"per_market": (1_000, 3_000, 3_000), "per_day": (10_000, 10_000, 10_000)},
    "fatfinger": {"resting": (8_000, 300_000, 400_000), "filled": (30_000, 30_000, 40_000)},  # resting = policy (no cash lock);
                                                                                               # afternoon D-B: 40k -> 300k
    "dump": {"resting": (400_000, 800_000, 900_000)},          # resting cost (policy; fills bounded by the per-side reserve fit)
    "longshot": {"resting": (20_000, 20_000, 25_000)},        # day 3 §2: total resting cost (worst case 200 per side)
    "scalp": {"inventory": (10_000, 10_000, 20_000)},         # day 5 §2: the slice at cost (3k per market); ratchet / idle rule up to 20k
    "touch": {"budget": (7_500, 15_000, 30_000)},             # day 6 §2: the layer budget at cost (pilot half); ratchet up to 30k
    "wall": {"notional": (50_000, 50_000, 100_000)},          # manual walls (control): bounded by the cash guard, not by this
}
# fat-finger catcher (morning_plan Part 1, day 2 §4): cheap deep bids on every side worth a lot
FAT_LEVELS = [0.12]             # Oct 3 (Piero): only the 0.12 level on each side (0.08 / 0.05: 0 fills since day 1)
FAT_SIZES = {"A": {0.12: 5_000}, "B": {0.12: 3_000}}   # shares (tier B had no 0.12 level: its old top size, 3,000)
FAT_BAND_EDGES = [0.05, 0.08, 0.12]  # queue bands stay (0.08, 0.12] etc. so the 0.12 level means 0.12 (one tick over a rival there)
FAT_TIER_A_ONLY = True          # Oct 3 (Piero): fat-finger levels on tier-A sides only (open orders < 600: one read page fewer)
FAT_NEW_LEVEL, FAT_NEW_SIZES = 0.05, {"A": 20_000, "B": 6_000}   # brand-new unmapped markets: 0.05 at half these, both sides
FAT_MIN_FAIR, FAT_UNMAPPED_REF, FAT_FAIR_CAP = 0.35, 0.60, 0.25   # eligible side; unmapped: SIG ref >= 0.60 (half size)
FAT_TTL_S, FAT_QUEUE_S, FAT_REARM_S = 24 * 3600, 2 * 3600, 600
FAT_NEWS_DROP, FAT_NEWS_WINDOW_S, FAT_NEWS_HOLD_S = 0.15, 600, 1800
FAT_MOVE = 0.10                 # re-place only when fair moves > 10c (or crosses FAT_MIN_FAIR)
FAT_CAPACITY_SHARE = 0.30       # initial placement uses <= 30% of measured SIG capacity
FAT_REFRESH_MAX_H = 60          # pilot gate: placements per hour after the initial placement (≈ 0: TTL, re-arm, queue steps)
INTEL_READS, INTEL_REVISIT_S = 2, 1800    # day 2 §5 tape collector: markets a loop (spare reads only); revisit every 30 min
INTEL_DAILY_HOUR = 7                      # docs/daily + docs/leaders + tier A refresh at 07:00 PT
HEADLINE_RACES = ["U.S. Senate", "U.S. House", "Rhode Island Senate", "Oklahoma Senate", "Massachusetts Governor",
                  "New Hampshire Senate", "Minnesota Senate"]   # always fat-finger tier A (morning_plan 1.4)
FAT_BOOK_READS = 3              # queue reads (order books) per loop
# dump catcher v3 (day 4 §2, "shallow and fast"): rest just under SIG's price on every favourite side, sell back within minutes
DUMP_MIN_FAIR = 0.80                      # sides with Polymarket fair_side >= this (not manual, no opposite holding, PM fresh)
DUMP_V3_OFFSETS = [0.01, 0.02, 0.035]     # L1/L2/L3 = SIG 30-min median of the side - these
DUMP_V3_WEIGHTS = [0.50, 0.30, 0.20]      # day 5 §1.3 (was 40/35/25): L1 fills most often
DUMP_FAIR_GAP = 0.03                      # every level <= fair_side - 3c: we only buy at least 3c under fair
DUMP_SLIDE = 0.02                         # 30-min median < 6-h median - 2c: a slide in progress -> freeze the ref, never lower
DUMP_SIDE_DIV, DUMP_SIDE_MIN, DUMP_SIDE_MAX = 4, 2_000, 10_000  # day 5 §1.2: per side = funded reserve / 4, clipped; arena x 1.5
DUMP_ARENA_MAX, DUMP_SIDE_HARD_MAX = 15_000, 15_000   # arena sides <= 15k; the ratchet never takes a side above 15k or reserve / 2
DUMP_PARTY_CAP = 40_000                   # day 5 §1.5: held dump inventory at cost per party side (D favourites / R favourites)
RATCHET_STEP, RATCHET_EVERY_S = 1.25, 3600       # day 6 §1.1: +25% per passing 1-h gate (was 2 h); one notch back + 1 h hold on a failure
RATCHET_MIN_FILLS_DAY, RATCHET_MIN_FILLS_NIGHT = 10, 5   # fewer fills in the hour: hold the level (not a fail)
DAY_WINDOW = (6, 15)            # PT hours of the day window (06:00-15:00): 10x the dumps of the night
DUMP_CONC_CAPS = [(0.95, 20_000), (0.90, 15_000), (0.0, 12_000)]   # day 7 §2.2: cap per RACE by fair_side (position + resting at cost, all engine layers)
RI_RACES, RI_ENGINE_CAP = ("Rhode Island Senate", "Rhode Island Governor"), 10_000   # day 7 §2.3: engine tranches get their own 10k on RI beyond the old position
DUMP_L0_MIN_FAIR, DUMP_L0_GAP, DUMP_L0_CLIP = 0.90, 0.05, 2_000   # day 7 §2.1: L0 joins the SIG best bid when it sits >= 5c under a >= 90% fair
DUMP_L0_REFILLS, DUMP_L0_WINDOW_S = 3, 600            # sweep follow: at most 3 refills per side per 10 min while the tranche is open
UNCOVERED_LOG_S = 30            # §1.3: an eligible side without its level for > 30 s is logged
# Oct 5 22:50 (Piero): hard cash guards after the floor breach
ENTRY_CASH_MULT, ENTRY_NEAR_TOUCH, ENTRY_NEAR_WEIGHT, ENTRY_MIN_FREE = 2.0, 0.01, 3.0, 2_000   # resting entries <= 2 x free cash; near-touch bids count 3x; < 2k free: none
DUMP_NEAR_TTL_S, DUMP_NEAR_GAP = 1200, 0.02      # L0 and levels within 2c of the touch: 20-min TTL (make-before-break); deeper 60 min
DUMP_L0_BUDGET = 8_000          # L0 holds <= 8k at cost across all sides; refills only under it; L1-L4 get the cash first
REBUILD_FREE, REBUILD_GAP1, REBUILD_GAP2 = 15_000, 0.07, 0.10   # rebuild free cash to 15k: engine lots above entry + 0.5c at the bid, lowest gap first
REBUILD_MARGIN, REBUILD_MARGIN_GAP = -0.01, 0.05 # Piero 23:00: lots <= 5c under fair may go down to entry - 1c for the rebuild
# wall layer (Piero's manual pilot, Oct 5 23:40): one big resting order placed by control, with its buy-back, hold and news guard
WALL_TTL_S, WALL_HOLD_S, WALL_NEWS_C, WALL_NEWS_WINDOW_S, WALL_MIN_FREE = 24 * 3600, 6 * 3600, 0.02, 600, 2_000
DUMP_V3_OFFSETS_DAY, DUMP_V3_WEIGHTS_DAY = [0.01, 0.02, 0.035, 0.05], [0.45, 0.25, 0.15, 0.15]   # §1.3: L4 in the day window (replay: 5c)
DUMP_RECYCLE_FAST_S = 1800      # §1.5: cash-constrained -> a tranche older than 30 min sells back at fill + 0.5c
CONSTRAINED_FREE, CONSTRAINED_FILLS_30M = 10_000, 3_000   # §3.3: constrained = free cash < 10k AND dump fills >= 3k cost in 30 min
RELEASE_FUND_PER_H, RELEASE_MIN_AGE_S = 10_000, 24 * 3600  # §3.3 funding branch: <= 10k per hour, never a holding bought < 24 h ago
# capital policy v2 (day 6 §9): rule 0 + the cash ratchet + release tiers
RULE0_MARGIN = 0.005            # an engine lot sells only at >= its entry + 0.5c, in any layer; day-1 lots (Oct 1 PT) may go below entry
DAY1_CUTOFF = "2026-10-02 00:00:00"   # fills before this (PT) or before the engine are day-1 lots
CASH_T_START, CASH_T_STEP, CASH_T_MIN, CASH_T_MAX = 25_000, 10_000, 25_000, 55_000
CASH_T_CONSTRAINED_MIN, CASH_T_PEAK_UP, CASH_T_PEAK_DOWN, CASH_T_IDLE_H = 15, 0.70, 0.40, 3
RELEASE_TIER0, RELEASE_TIER1, RELEASE_TIER2 = 0.02, 0.05, 0.07   # gap to fair: tier 0 always (rule 0), tier 1 for T, tier 2 for T if trading >= 10%/day
RELEASE_TIER2_RETURN = 0.10     # the trading layers' realized return per day on their average capital over 24 h
SCALP_DAY_OFF = None            # day 6 §10.3 (01:20 addendum): the scalper runs all day; it yields to the catcher and keeps its 2-h hand-off
DUMP_REFRESH_S, DUMP_REFRESH_JITTER_S = 45 * 60, 900   # §4.1: a 60-min level is re-placed at 30-60 min, staggered +-15 min (a deploy re-places them all at once)
# touch layer (day 6 §2): be the bid the big sellers hit, on favourites SIG prices far under fair
TOUCH_MIN_FAIR, TOUCH_GAP = 0.93, 0.06   # fair_side >= 0.93 and the SIG best bid <= fair_side - 6c
TOUCH_CLIP, TOUCH_RACE_CAP = 2_000, 8_000   # shares per bid; per race at cost including what we hold on the race
TOUCH_TTL_S, TOUCH_SELL_MIN = 1800, 0.015   # 30-min dead-man TTL; exit at max(entry + 1.5c, best ask - 1 tick)
TOUCH_NEWS_FAIR, TOUCH_NEWS_SIG, TOUCH_NEWS_HOLD_S = 0.02, 0.03, 1800   # fair -2c in 10 min, or SIG -3c while PM moves against us: 30 min off
TOUCH_YIELD_RESERVE, TOUCH_PILOT_S, TOUCH_GATE_FILLS, TOUCH_RATCHET_MAX = 15_000, 4 * 3600, 10, 30_000
DUMP_ARENA_MULT, DUMP_ARENA_N = 1.5, 10   # arena = the markets with the most big events in the last 48 h (intel events + dumps)
DUMP_CONC_CAP = 12_000                    # position (at cost) + resting dump bids on a side <= this
DUMP_SELL_MIN, DUMP_SELL_FAIR_GAP = 0.015, 0.005   # sell-back at max(pre-dump ref, fill + 1.5c), <= fair_side - 0.5c
DUMP_STEP1_S, DUMP_STEP2_S = 1200, 3600   # day 5 §1.4: after 20 min -> max(fill + 1c, ref - 0.5c); after 60 min -> fill + 0.5c
DUMP_STEP_MIN, DUMP_STEP2_MIN = 0.01, 0.005
DUMP_HOLD_S = 6 * 3600                    # unfilled after 6 h: the tranche goes to inventory (held to settlement)
DUMP_TTL_S, DUMP_QUEUE_S, DUMP_BOOK_READS = 3600, 1800, 2   # day 6 §4.1: dead-man TTL 60 min (was 6 h)
DUMP_NEWS_DROP = 0.03                     # fair_side -3c in 10 min -> that side's levels pulled 30 min
DUMP_BREAKER_SIDES, DUMP_BREAKER_FAIR = 4, 0.01   # >= 4 sides of one party filled in 10 min AND PM >= 1c against -> 30 min
DUMP_WRITE_SHARE, DUMP_WRITES_IDLE = 0.30, 15   # placement <= 30% of SIG write capacity, only while writes_left >= 15
DUMP_PILOT_S = 4 * 3600                   # pilot: 4 h at half size on every side, then the gate
DUMP_MIN_SIDE_COST = 300
RELEASE_FALLBACK_QTY = 500                # Oct 3 (b): no spare read for the depth -> sell <= this many shares at the bid
# longshot catcher (day 3 §2): resting bids at 0.5-1c on sides Polymarket prices <= 5%, to catch sweeps through them
LONG_MAX_FAIR = 0.05                      # sides with Polymarket fair_side <= this
LONG_LEVELS = [(0.005, 20_000), (0.010, 10_000)]   # (price, shares): 100 SIG each
LONG_TINY_FAIR, LONG_TINY_QTY = 0.005, 10_000      # fair_side < 0.005: only the 0.005 level, 10,000 shares
LONG_SIDE_MAX_COST = 200
LONG_TTL_S, LONG_EXIT_TTL_S = 24 * 3600, 24 * 3600
LONG_EXIT_MULT = 3.0                      # exits: half at 3x entry, half at max(3x entry, SIG 6-h median); never below 3x
LONG_STALE_S = 72 * 3600                  # unsold after this: kept, flagged on the dashboard
LONG_NEWS_RISE, LONG_NEWS_WINDOW_S, LONG_NEWS_HOLD_S = 0.03, 600, 1800   # fair_side +3c in 10 min -> pull that side 30 min
LONG_BREAKER_SIDES, LONG_BREAKER_PAUSE_S = 5, 1800  # >= 5 sides filled in 10 min -> no new levels for 30 min
LONG_PILOT_S = 24 * 3600
KAPPA_FROM_OCT30 = 0.03         # D3: capital discount from Oct 30 (election-night cash)
LOSS_STOP = 2_000               # D5: bot-owned P&L at fair, rolling 24 h
OPPORTUNITY_RESERVE = 35_000    # day 3 §3 (25k afternoon, 10k day 2): on top of the floor; dump/fat-finger/longshot fills, snipes, >= 8c takes
AK_RELEASE = 0.50               # D1 + D1b (day 2 §1): share of the original AK NO position released to the recycler
RELEASE_EVERY_S, RELEASE_MARKETS = 30, 3   # release layer (day 2 §3.3): plan every 30 s, <= 3 markets (= order-book reads)
RELEASE_PILOT_TARGET = 10_000   # pilot: free cash above the floor
RELEASE_MAX_COST = 0.05         # day 4 §3 (release on demand): a holding is released only if it sells >= its average entry
                                # cost (a realized profit, price >= fair_side - 5%), or if the dump catcher is cash-constrained
                                # now and the holding returns less per day to settlement than the dump catcher did over 24 h
RELEASE_QUIET_HOURS = (7, 8)    # no release 07:00-08:00 PT (Piero's daily review sees the book untouched)
LIQUIDATE = {"AK-GOV-D": dict(slices=3, over_fair=0.01)}   # day 4 §3 (Piero): the day-1 AK NO sold at max(fair + 1c, SIG bid)
# day 4 §4 reads
PNL_READ_S = 60                 # /portfolio/pnl every 60 s (was every loop)
ORDERS_READ_S = 30              # open orders: after our own writes or fills, else a full read every 30 s
PRICE_ROTATE = 100              # prices: markets with our orders/holdings every loop; the rest rotate, one read (100) a loop
LEADERS_EVERY_S = 900           # leaders logger every 15 min (was 10)
LEADERS_DAY_EVERY_S, JUMP_MIN = 300, 1_000   # day 6 §6: the daily board every 5 min; a gain >= 1k between reads is a jump
TAPE_MIN_PER_MIN = 6            # tape collector: guaranteed 6 reads/min
BACKOFF_429_S, BACKOFF_LOOP_S = 120, 20.0   # after a 429: 20 s loops for 2 min

# A7. files ---------------------------------------------------------------------------------------------------------
DATA = os.environ.get("SIG_DATA") or os.path.join(os.path.dirname(os.path.abspath(__file__)), "data")   # releases share it
PM_MAP_FILE = os.path.join(DATA, "pm_map.json")
POSITION_REF_FILE = os.path.join(DATA, "position_ref.json")
LEDGER_FILE = os.path.join(DATA, "ledger.json")
PNL_FILE = os.path.join(DATA, "pnl.csv")
PNL_EVERY_S = 120
STATE_FILE = os.path.join(DATA, "state.json")
DASHBOARD_FILE = os.path.join(DATA, "dashboard.html")
PID_FILE = os.path.join(DATA, "engine.pid")
LOCK_FILE = os.path.join(DATA, "engine.lock")
OVERRIDES_FILE = os.path.join(DATA, "overrides.json")     # layer flags and controls (survive restarts)
DASHBOARD_URL_FILE = os.path.join(DATA, "dashboard_url.txt")   # the live dashboard's URL with its access token
STATE_EVERY_S = 60              # data/state.json and the static data/dashboard.html: rewritten at most this often
HOST_ID_FILE = os.path.join(DATA, "host_id")
NIGHT_LOG_FILE = os.path.join(os.path.dirname(DATA), "docs", "RUN_LOG.md")   # day 2: the run log (NIGHT_LOG.md = the night)
