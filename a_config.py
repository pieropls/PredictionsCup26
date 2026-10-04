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
    "dump": {"resting": (120_000, 450_000, 500_000)},          # resting cost (policy; fills bounded by the per-side reserve fit);
                                                               # Oct 3: full = every eligible favourite side (~125 sides, ~440k)
    "longshot": {"resting": (20_000, 20_000, 25_000)},        # day 3 §2: total resting cost (worst case 200 per side)
}
# fat-finger catcher (morning_plan Part 1, day 2 §4): cheap deep bids on every side worth a lot
FAT_LEVELS = [0.12]             # Oct 3 (Piero): only the 0.12 level on each side (0.08 / 0.05: 0 fills since day 1)
FAT_SIZES = {"A": {0.12: 5_000}, "B": {0.12: 3_000}}   # shares (tier B had no 0.12 level: its old top size, 3,000)
FAT_BAND_EDGES = [0.05, 0.08, 0.12]  # queue bands stay (0.08, 0.12] etc. so the 0.12 level means 0.12 (one tick over a rival there)
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
# dump catcher (afternoon D-A): big players dumping size 7-20c under fair on favourites; buy it, sell the rebound
DUMP_MIN_FAIR, DUMP_MIN_REF = 0.90, 0.80   # sides with Polymarket fair_side >= 0.90 and SIG 30-min ref_side >= 0.80
DUMP_OFFSETS = [0.07, 0.12, 0.20]          # levels at fair_side - these, each also <= ref_side - DUMP_REF_GAP
DUMP_REF_GAP = 0.04
DUMP_COST = {"A": 8_000, "B": 2_000}       # SIG cost of a full fill of all three levels, per side (split evenly)
DUMP_MIN_SIDE_COST = 300                   # a side whose reserve room is below this gets no levels
DUMP_TTL_S, DUMP_QUEUE_S, DUMP_BOOK_READS = 6 * 3600, 1800, 2
DUMP_REBOUND_MIN = 0.05                    # rebound sell at max(pre-dump ref_side - 1 tick, fill + 5c): A2 exception
DUMP_HOLD_S = 24 * 3600                    # an unsold rebound tranche goes to inventory (normal A2 rules) after this
DUMP_NEWS_DROP = 0.05                      # Polymarket fair_side drop > 5c in 10 min -> that side's levels pulled 30 min
DUMP_BREAKER_MARKETS, DUMP_BREAKER_FAIR = 3, 0.01   # >= 3 markets filled in 10 min, same political side, PM >= 1c against
DUMP_PILOT_SIDES, DUMP_PILOT_S = 15, 4 * 3600
DUMP_REARM_S = 600
# day 3 §1 (night §2): the dump catcher on the arena sides only (where the sweeps happen), v2 reference, concentration cap
DUMP_ARENA = [("Rhode Island Senate D", "yes"), ("Rhode Island Senate R", "no"), ("Oklahoma Senate D", "no"),
              ("Massachusetts Governor R", "no"), ("New Hampshire Senate D", "yes"), ("New Hampshire Senate R", "no"),
              ("Minnesota Senate D", "yes"), ("U.S. House D", "yes"), ("MN-05 House race D", "yes")]
DUMP_ARENA_MIN_FAIR, DUMP_ARENA_COST = 0.85, 12_000  # arena sides eligible down to fair_side 0.85; cost per side (day 3 §3: 8k -> 12k)
DUMP_CONC_CAP = 12_000                    # no new levels where position on the side (at cost) + resting dump bids >= this
DUMP_OTHER_COST = 3_000                   # Oct 3 (dump full): non-arena favourite sides, 3k over 2 levels
DUMP_OTHER_OFFSETS = [0.04, 0.07]         # ... at ref - these, each <= fair_side - DUMP_OTHER_FAIR_GAP
DUMP_OTHER_FAIR_GAP = 0.05
RELEASE_FALLBACK_QTY = 500                # Oct 3 (b): no spare read for the depth -> sell <= this many shares at the bid
DUMP_REF_LONG_S = 6 * 3600                # reference = max(SIG 30-min median, SIG 6-h median)
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
RELEASE_TARGET = 40_000         # day 3 §3: free cash above the floor, from Oct 3 to Nov 3 (was 25k by Oct 3, 40k by Oct 10)

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
