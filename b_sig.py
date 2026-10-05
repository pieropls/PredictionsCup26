"""B. SIG: Super Market REST client (/api/v1).
Book prices are YES-normalized; order prices are in the order's own side. Positions net per exchange.
Rate limit per account: 100 reads + 30 writes per minute; writes are budgeted here (writes_left)."""
import threading
import time
import uuid
from concurrent.futures import ThreadPoolExecutor
from collections import deque
from datetime import datetime, timedelta, timezone

import requests

from a_config import BASE_URL, API_KEY, TOURNAMENT, DRY_RUN, WRITES_PER_MIN, READ_TIMEOUT_S


class RateLimited(Exception):
    """429 from SIG: wait retry_after seconds before the next write."""
    def __init__(self, retry_after):
        super().__init__(f"rate limited, retry after {retry_after}s")
        self.retry_after = retry_after


class Unknown(Exception):
    """A write whose outcome we don't know (timed out twice): it may or may not have happened."""

S = requests.Session()
S.headers.update({"Authorization": f"Bearer {API_KEY}"})
T = {}                  # tournament {id, slug}, set by init()
_writes = deque()       # timestamps of writes in the last minute
_wlock = threading.RLock()  # the executor's cancel and placement lanes write from two threads
_calls = deque(maxlen=2000)  # (time, "read"|"write", seconds, status) of every request: the dashboard's health panel
LAST_429 = [0.0]        # day 4 §4: the main loop backs off to 20 s loops for 2 min after one
_throttled = deque(maxlen=1000)  # times of HTTP 429 answers (the fat-finger pilot gate: write usage under the limit)


def _req(method, path, timeout=None, **kw):
    timeout = timeout or (READ_TIMEOUT_S if method == "GET" else 45)
    if method != "GET":
        with _wlock:
            if writes_left() <= 0:
                raise RuntimeError("write budget exhausted")
            _writes.append(time.time())
    kind = "read" if method == "GET" else "write"
    for attempt in range(3 if method == "GET" else 1):          # reads retry; writes are reconciled next loop instead
        t0 = time.time()
        try:
            r = S.request(method, BASE_URL + path, timeout=timeout, **kw)
        except (requests.Timeout, requests.ConnectionError):
            _calls.append((t0, kind, time.time() - t0, 0))
            if method != "GET" or attempt == 2:
                raise
            time.sleep(1 + attempt)
            continue
        _calls.append((t0, kind, time.time() - t0, r.status_code))
        if r.status_code < 500 or method != "GET":
            break
        time.sleep(1 + attempt)
    if r.status_code == 429:
        _throttled.append(time.time())
        LAST_429[0] = time.time()
        if method != "GET":                                    # writes: let the executor pause its lanes instead of blocking
            raise RateLimited(float(r.headers.get("Retry-After", 60)))
        time.sleep(float(r.headers.get("Retry-After", 60)))
    if not r.ok:
        raise RuntimeError(f"{method} {path} -> {r.status_code} {r.text[:300]}")
    return r.json() if r.content else {}


def _pages(path, params, timeout=None):
    out, cursor = [], None
    while True:
        d = _req("GET", path, timeout=timeout, params={**params, **({"cursor": cursor} if cursor else {})})
        out += d["data"]
        if not d["pagination"]["hasMore"]:
            return out
        cursor = d["pagination"]["nextCursor"]


def health(window_s=600):
    """SIG request stats for the dashboard: latency (median, p90), 5xx/timeout rate, reads and writes in the last minute."""
    now, out = time.time(), {}
    calls = [c for c in list(_calls) if c[0] >= now - window_s]
    for kind in ("read", "write"):
        xs = sorted(c[2] for c in calls if c[1] == kind)
        bad = sum(1 for c in calls if c[1] == kind and (c[3] >= 500 or c[3] == 0))
        out[kind] = dict(n=len(xs), p50=round(xs[len(xs) // 2], 2) if xs else None, p90=round(xs[int(len(xs) * 0.9)], 2) if xs else None,
                         bad_rate=round(bad / len(xs), 3) if xs else 0.0)
    out["reads_last_min"] = sum(1 for c in calls if c[1] == "read" and c[0] >= now - 60)
    out["writes_last_min"] = WRITES_PER_MIN - writes_left()
    out["read_limit"], out["write_limit"] = 100, 30
    out["throttled"] = sum(1 for t in list(_throttled) if t >= now - window_s)
    out["last_ok"] = max((c[0] + c[2] for c in calls if 0 < c[3] < 500), default=None)
    return out


def reads_last_min():
    now = time.time()
    return sum(1 for x in list(_calls) if x[1] == "read" and x[0] >= now - 60)


def spare_reads(keep=30):
    """Day 2 §6: optional reads (queue and depth reads, pair books, the market scan, leaders) go only while >= keep of
    SIG's 100 reads a minute are left, so the loop's own reads (orders, positions, fills, prices: fills and exits) never
    hit a 429."""
    return reads_last_min() <= 100 - keep


def order(oid):
    """One order record (day 4 §1: evidence before any cash-short pause): {id, exchangeId, side, action, quantity, open, ...}."""
    return _req("GET", f"/orders/{oid}")


def recent_writes_ok(n=20):
    """Share of the last n writes that SIG answered with < 500 (day 4 §1.3: an automatic STOP AND PULL lifts at >= 80%)."""
    ws = [c for c in list(_calls) if c[1] == "write"][-n:]
    return 1.0 if not ws else sum(1 for c in ws if 0 < c[3] < 500) / len(ws)


def writes_left():
    with _wlock:
        while _writes and _writes[0] < time.time() - 60:
            _writes.popleft()
        return WRITES_PER_MIN - len(_writes)


# --- reads ----------------------------------------------------------------------------------------------------------

def init():
    d = _req("GET", f"/tournaments/{TOURNAMENT}")
    T.update(id=d["id"], slug=d["slug"])
    return d


def tournament_markets():
    """[{id, title, status, exchanges: [{id, option, ...}]}] for every market in the tournament."""
    return _pages(f"/tournaments/{T['slug']}/markets", {"limit": 100}, timeout=60)     # slow endpoint (40 s+ seen)


def positions_raw():
    """Open position rows: exchangeId, quantity, avgCost, currentPrice, unrealizedPnl, lots[{side, quantity, entryPrice}]."""
    return [p for p in _req("GET", f"/tournaments/{T['slug']}/portfolio/positions")["positions"] if not p["settled"]]


def net(p):
    """Net YES shares of a position row (long NO is negative)."""
    return sum(l["quantity"] * (1 if l["side"].lower() == "yes" else -1) for l in p["lots"])


def positions():
    out = {}
    for p in positions_raw():
        out[str(p["exchangeId"])] = out.get(str(p["exchangeId"]), 0) + net(p)
    return out


def open_orders():
    """[{id, ex, side, action, price, qty}]: qty is what's left, price is in the order's own side terms."""
    return [{"id": str(o["id"]), "ex": str(o["exchangeId"]), "side": o["side"], "action": o["action"],
             "price": round(float(o["priceLimit"]), 3), "qty": float(o["quantity"])}
            for o in _pages("/orders", {"status": "open", "tournamentId": T["id"], "limit": 200}) if o["priceLimit"] is not None]


def prices(exs):
    """{ex: (best_bid, best_ask, last)} YES-normalized; 100 exchanges per read."""
    exs, out = sorted(set(map(str, exs))), {}
    chunks = [exs[i:i + 100] for i in range(0, len(exs), 100)]
    get = lambda c: _req("GET", "/exchanges/prices", params={"ids": ",".join(c), "tournamentId": T["id"]})["data"]
    with ThreadPoolExecutor(max_workers=4) as pool:                 # the chunks are independent reads: fetch together
        for data in pool.map(get, chunks):
            for x in data:
                out[str(x["exchangeId"])] = (x["bestBid"], x["bestAsk"], x["latestPrice"])
    return out


def orderbook(ex, depth=10):
    """-> (bids, asks): [(yes_price, qty)] best first."""
    d = _req("GET", f"/exchanges/{ex}/orderbook", params={"tournamentId": T["id"], "depth": depth})
    lv = lambda side: [(float(l["price"]), float(l["quantity"])) for l in d[side]]
    return lv("bids"), lv("asks")


FILL_COVERAGE = {}      # evening §1.2: the fills projection's coverage on the latest read {t, seq} (the stall detector)


def fills(known=(), max_pages=5):
    """Fills newest first, paging back until one whose key (tradeId:orderId) is already known (or max_pages):
    [{id, orderId, exchangeId, price (order side terms), quantity (negative = NO), side, filledAt (UTC)}]."""
    out, cursor = [], None
    for _ in range(max_pages):
        d = _req("GET", f"/tournaments/{T['slug']}/portfolio/fills", params={"limit": 100, **({"cursor": cursor} if cursor else {})})
        if not cursor:
            FILL_COVERAGE.update(t=time.time(), seq=(d.get("coverage") or {}).get("projectedThroughSequence"),
                                 newest=(d["data"][0].get("filledAt") if d.get("data") else None))
        out += d["data"]
        if any(f"{f['id']}:{f.get('orderId')}" in known for f in d["data"]) or not d["pagination"]["hasMore"]:
            return out
        cursor = d["pagination"]["nextCursor"]
    return out


def leaderboard(limit=20, offset=0):
    """Tournament leaderboard (sort by P&L): {leaderboard: [{rank, username, pnl, tradesCount, volume, roi}], total, myRank}."""
    return _req("GET", f"/tournaments/{T['slug']}/leaderboard", params={"limit": limit, "offset": offset, "sort": "pnl", "period": "all"})


def recent_markets(limit=20):
    """Newest tournament markets first (1 read): [{id, title, status, createdAt, exchanges: [{id, ...}]}]."""
    return _req("GET", "/markets", params={"tournamentId": T["id"], "sort": "recent", "limit": limit, "status": "open"})["data"]


def pnl():
    """{totalAccountValue, totalHoldingsValue, unrealizedPnl, roi, ...}"""
    return _req("GET", f"/tournaments/{T['slug']}/portfolio/pnl", params={"period": "all"})


# --- writes (no-ops in DRY_RUN) ---------------------------------------------------------------------------------------

def _send(path, field, orders, ttl_s, key=None):
    """One idempotent write of several orders -> [(order, ok, data)]. A timeout or 5xx is retried once with the same
    idempotency key (SIG replays the stored result instead of placing twice); if that fails too, raises Unknown."""
    if not orders:
        return []
    if DRY_RUN:
        return [(o, True, {"orderId": None, "open": True}) for o in orders]
    exp = {"expirationDate": (datetime.now(timezone.utc) + timedelta(seconds=ttl_s)).isoformat()} if ttl_s else {}
    body = {"idempotencyKey": key or str(uuid.uuid4()),
            field: [{"exchangeId": ex, "side": s, "action": a, "price": p, "quantity": int(q), "tournamentId": T["id"], **exp}
                  for ex, s, a, p, q in orders]}
    for attempt in (1, 2):
        try:
            res = _req("POST", path, timeout=75, json=body)
            break
        except (requests.Timeout, requests.ConnectionError) as e:
            err = e
        except RateLimited:
            if attempt == 2:                                       # the first attempt may have landed: the caller must
                raise Unknown(f"{len(orders)} orders: timed out, then rate limited")     # never re-send it as new
            raise
        except RuntimeError as e:
            if "REQUEST_IN_FLIGHT" in str(e):                      # the first attempt is still being processed by SIG
                raise Unknown(f"{len(orders)} orders still in flight")
            if attempt == 2 and "write budget exhausted" in str(e):
                raise Unknown(f"{len(orders)} orders: timed out, no budget left to retry")
            if " -> 5" not in str(e):
                raise
            err = e
    else:
        raise Unknown(f"{len(orders)} orders: {err!r}")
    out = [(o, False, {"error": "no result returned"}) for o in orders]
    for r in res.get("results", []):
        out[r["index"]] = (orders[r["index"]], bool(r.get("ok", True)), r.get("data") or {})   # multi-leg: all-or-nothing, no "ok"
    return out


def place_batch(orders, ttl_s=None, key=None):
    """Up to 50 independent orders (ex, side, action, price, qty) in one write; each can succeed or fail on its own.
    data on success: orderId, open, remainingQuantity, quantityTraded, fillPrice; on failure: the error."""
    orders = list(orders)
    assert len(orders) <= 50
    return _send("/orders/batch", "orders", orders, ttl_s, key)


def place_multileg(legs, ttl_s=None, key=None):
    """Up to 10 legs placed ATOMICALLY in one transaction: all legs are placed or none (a leg can still rest unfilled)."""
    legs = list(legs)
    assert 1 <= len(legs) <= 10
    return _send("/orders/multi-leg", "legs", legs, ttl_s, key)


def cancel(oid):
    if not DRY_RUN:
        _req("DELETE", f"/orders/{oid}")


def cancel_all(ex):
    """Every open order on ONE exchange. A tournament-wide cancel-all would also kill position exits and manual orders,
    so it is impossible here: calling without an exchange raises."""
    if not ex:
        raise ValueError("tournament-wide cancel-all is forbidden: pass an exchange id")
    if not DRY_RUN:
        _req("POST", "/orders/cancel-all", json={"tournamentId": T["id"], "exchangeId": ex})
