#!/bin/bash
# Rollback to the Mac (~5 min): stop the server unit, pull its data/ back, re-point ENGINE_HOST, redeploy the current release here.
set -e
cd "$(dirname "$0")/.."
SERVER="${SIG_SERVER:-user@your-server}"; REL="${1:?usage: rollback.sh releases/<name>}"
ssh "$SERVER" "systemctl disable --now sig-engine; sleep 3; systemctl is-active sig-engine || true"
launchctl unload ~/Library/LaunchAgents/com.sig.mirror.plist 2>/dev/null || true
rsync -a --exclude '*.lock' --exclude '*.pid' "$SERVER:/opt/sig-engine/data/" data/ && rsync -a "$SERVER:/opt/sig-engine/docs/RUN_LOG.md" docs/RUN_LOG.md
sed -i '' 's/^ENGINE_HOST = os.environ.get("SIG_ENGINE_HOST", ".*")/ENGINE_HOST = os.environ.get("SIG_ENGINE_HOST", "my-laptop")/' a_config.py
./deploy.sh "$REL"
echo "- $(date +%H:%M) ROLLBACK: engine back on the Mac ($REL); the server unit is disabled (Claude Code)" >> docs/RUN_LOG.md
