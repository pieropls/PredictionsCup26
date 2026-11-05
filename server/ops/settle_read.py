"""SETTLEMENT WATCH (Piero 10:25 Nov 4), the SIG half: read-only GETs through the engine's b_sig, one JSON object on stdout; nothing
is written anywhere. Run by ops.py settle (sig-ops@settle, user sig, the unit's EnvironmentFile: the key is passed by systemd, never read
here). 5 reads: the tournament (our cash = myBalance), the market list (status / settledWith / settledOn, 3 pages), our position rows
(settled ones included). With `final`: 3 more, the leaderboard's top 70 and our own row."""
import json
import sys

sys.dont_write_bytecode = True
sys.path.insert(0, "/opt/sig-engine/engine")
import b_sig as api  # noqa: E402


def main():
    tour = api.init()
    out = dict(tour={k: tour.get(k) for k in ("status", "endDate", "myBalance", "memberCount", "marketCount")})
    out["markets"] = [{k: r.get(k) for k in ("id", "title", "status", "settledWith", "settledOn", "settlementDate", "exchanges")}
                      for r in api.tournament_markets()]
    d = api._req("GET", f"/tournaments/{api.T['slug']}/portfolio/positions")
    out["positions"], out["summary"] = d.get("positions") or [], d.get("summary")
    if "final" in sys.argv[1:]:
        lb = api.leaderboard(50)
        rows = list(lb.get("leaderboard") or []) + list(api.leaderboard(20, offset=50).get("leaderboard") or [])
        me = api.leaderboard(1, offset=int(lb["myRank"]) - 1).get("leaderboard") if lb.get("myRank") else []
        out["leaders"] = dict(rows=rows, my_rank=lb.get("myRank"), me=(me or [None])[0], total=lb.get("total"))
    print(json.dumps(out))


if __name__ == "__main__":
    main()
