"""Test package guard (day 6 §5.2): every test process gets its own data directory before anything imports a_config,
so a test or a debug script can never write into the live data/ (the 19:52 Oct 4 incident: three scripts that imported
f_engine before tests.test_engine appended fake rows to pnl.csv, leaders.csv and the journal)."""
import os
import tempfile

if not os.environ.get("SIG_DATA"):
    os.environ["SIG_DATA"] = tempfile.mkdtemp(prefix="sig_tests_data_")
_LIVE = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "data")
assert os.path.realpath(os.environ["SIG_DATA"]) != os.path.realpath(_LIVE), "tests must never run on the live data/"
