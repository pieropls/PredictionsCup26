"""Build the two fixtures the election-night rehearsal (tests/sim_election.py) reads, so it never needs the scratchpad or the server:

  tests/fixtures/night24_paths.json.gz   the 68 usable 2024 Polymarket races (CO-08 excluded: opened Nov 9) as 1-min paths of the
                                         pre-close FAVOURITE's token and the other token, from the state's last poll close - 12 h to
                                         + 54 h, cleaned with the night_tapes §4.3 rule; the removed "emptied book" minutes are kept as
                                         a flag (the sim turns them into one-sided PM books)
  tests/fixtures/night26_levels.json     the 2026 snapshot per SIG book: PM fair (the engine's), SIG bid / ask, from data/state.json,
                                         plus a copy of data/election/poll_close.json (fallback when data/ is absent)

Usage: python3 -I research/night/night_sim_build.py <raw pm_hist dir> <night out dir> [repo root]
(pm_hist dir = the folder with races24.json and hist24/; out dir = the one with races24_metrics_v2.json). Stdlib only; reads only."""
import gzip
import json
import os
import sys
import time

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)
from pm_common import ED24, close_ts  # noqa: E402
from pm24_analyze2_lib import load_clean  # noqa: E402

PRE_H, POST_H, STEP = 12, 54, 60


def raw_series(raw, tok):
    h = json.load(open(os.path.join(raw, "hist24", f"{tok[:20]}_f1.json"))).get("history", [])
    return [(x["t"], x["p"]) for x in h]


def grid(series, art, t0, n):
    """Forward-fill 1-min prints onto t0 + i*60; art: set of print times that were removed as emptied-book artifacts."""
    out, flags, j, last, last_art = [], [], 0, None, False
    for i in range(n):
        t = t0 + i * STEP
        while j < len(series) and series[j][0] <= t:
            last, last_art = series[j][1], series[j][0] in art
            j += 1
        if last is None:
            last = series[0][1] if series else 0.5
        out.append(int(round(last * 10000)))
        flags.append(1 if last_art else 0)
    return out, flags


def main(raw, out_dir, root):
    races = json.load(open(os.path.join(raw, "races24.json")))
    ms = {(r["kind"], r["state"], r["district"]): r for r in json.load(open(os.path.join(out_dir, "races24_metrics_v2.json")))}
    paths = []
    for r in races:
        if r["district"] == "CO-08":
            continue
        cf, cl = close_ts(ED24, r["state"], r["district"], r["kind"] == "Control")
        m = ms[(r["kind"], r["state"], r["district"])]
        w_raw = raw_series(raw, r["win"]["tok"])
        w_cl = load_clean(raw, r["win"]["tok"])
        w_art = {t for t, _ in w_raw} - {t for t, _ in w_cl}
        if r["lose"].get("complement"):
            l_cl, l_art = [(t, round(1 - p, 4)) for t, p in w_cl], set(w_art)
        else:
            l_raw = raw_series(raw, r["lose"]["tok"])
            l_cl = load_clean(raw, r["lose"]["tok"])
            l_art = {t for t, _ in l_raw} - {t for t, _ in l_cl}
        # the cleaned series plus the artifact prints at their own times (flagged): the sim decides what an emptied book looks like
        w_full = sorted(w_cl + [(t, 0.5) for t in w_art])
        l_full = sorted(l_cl + [(t, 0.5) for t in l_art])
        t0, n = int(cl - PRE_H * 3600), (PRE_H + POST_H) * 3600 // STEP
        wp, wf = grid(w_full, w_art, t0, n)
        lp, lf = grid(l_full, l_art, t0, n)
        p_pre = m["p_pre"] if m["p_pre"] is not None else m["p_close"]
        fav_won = p_pre >= 0.5
        key = f"{r['kind']} {r['district'] or r['state']}"
        paths.append(dict(key=key, kind=r["kind"], state=r["state"], district=r["district"], vol=round(r["win"].get("vol") or 0),
                          liquid=(r["win"].get("vol") or 0) >= 250_000, last_close=int(cl), first_close=int(cf), t0=t0, step=STEP,
                          p_pre_winner=p_pre, fav_won=fav_won, winner=r["win"]["label"], loser=r["lose"].get("label"),
                          fav=wp if fav_won else lp, fav_art=wf if fav_won else lf, oth=lp if fav_won else wp, oth_art=lf if fav_won else wf))
    os.makedirs(os.path.join(root, "tests", "fixtures"), exist_ok=True)
    p = os.path.join(root, "tests", "fixtures", "night24_paths.json.gz")
    with gzip.open(p, "wt") as fh:
        json.dump(dict(built=time.strftime("%Y-%m-%d %H:%M %Z"), source="Polymarket CLOB prices-history 1-min, Nov 5-13 2024 (docs/night_tapes.md §1)",
                       units="price x 10000; t0 + i*step seconds (2024 epoch); fav = the pre-close favourite's token", races=paths), fh,
                  separators=(",", ":"))
    print("wrote", p, os.path.getsize(p), "bytes,", len(paths), "races")

    st = json.load(open(os.path.join(root, "data", "state.json")))
    pc = json.load(open(os.path.join(root, "data", "election", "poll_close.json")))
    books = {}
    for b in st.get("bots", []):
        books[str(b["ex"])] = dict(fair=b.get("fair"), sig_bid=b.get("bid"), sig_ask=b.get("ask"), title=b.get("market"),
                                   manual=b.get("kind") == "position")
    q = os.path.join(root, "tests", "fixtures", "night26_levels.json")
    with open(q, "w") as fh:
        json.dump(dict(snapshot=st.get("time"), note="PM fair and SIG bid/ask per book from data/state.json (read-only copy)",
                       books=books, poll_close=pc), fh, indent=0)
    print("wrote", q, os.path.getsize(q), "bytes,", len(books), "books")


if __name__ == "__main__":
    main(sys.argv[1], sys.argv[2], sys.argv[3] if len(sys.argv) > 3 else os.path.dirname(os.path.dirname(HERE)))
