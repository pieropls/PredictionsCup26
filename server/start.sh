#!/bin/bash
# The unit's ExecStart: refuse to start while the Mac's engine looks alive (its state.json changed in the last 5 min).
set -euo pipefail
cd /opt/sig-engine
MAC="${MAC_STATE_JSON:-/opt/sig-engine/mac_state/state.json}"
if [ -f "$MAC" ]; then
  # GNU stat (the server) or BSD stat (a dry run on the Mac); if the age cannot be read the lock FAILS CLOSED
  mtime=$(stat -c %Y "$MAC" 2>/dev/null || stat -f %m "$MAC" 2>/dev/null || true)
  [ -n "$mtime" ] || { echo "refusing to start: cannot read the age of $MAC (two engines may never run at once)"; exit 75; }
  age=$(( $(date +%s) - mtime ))
  if [ "$age" -lt 300 ]; then
    echo "refusing to start: the Mac's state.json changed ${age}s ago (two engines may never run at once)"; exit 75
  fi
fi
[ -n "${SIG_API_KEY:-}" ] || { echo "SIG_API_KEY missing from the env file"; exit 78; }
mkdir -p run
exec .venv/bin/python3 -u engine/f_engine.py --live
