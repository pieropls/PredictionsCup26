#!/bin/bash
# Day 10 D3 (server step 6): push a tested release to the server and restart the unit there. The Mac stays the only place
# code is edited; nothing in data/ and no key ever travels. Usage: deploy/server/push_release.sh releases/<name>
# Refuses while the Mac engine is running (one engine at a time) unless SIG_CUTOVER_OK=1 is set (the cutover itself).
# C125 (Piero 11:40 Oct 30): /opt/sig-engine never changes owner while the engine runs. Oct 29 00:25 the Mac rsync (-a, as root) gave
# the folder the Mac's uid 501 for the seconds before the chown -R; the running engine (user sig) could not see data/overrides.json,
# fell back to the defaults and saved them. Now the Mac rsyncs into a root-only stage; on the server the engine is stopped, the stage
# is copied in by the server's rsync with --chown=sig:sig (data/ excluded), the folder modes are set, and the engine started.
# data/overrides.json is backed up before the push and after the stop; if the stop wrote fewer keys or other layer states than the
# pre-push copy, that copy is put back before the start.
set -e
cd "$(dirname "$0")/../.."
REL="$1"; SERVER="${SIG_SERVER:-user@your-server}"
[ -d "$REL" ] || { echo "no release $REL"; exit 1; }
[ -f "$REL/.tested" ] || { echo "REFUSED: $REL has no .tested marker"; exit 1; }
if [ "${SIG_CUTOVER_OK:-0}" != "1" ] && [ -f data/engine.pid ] && kill -0 "$(cat data/engine.pid)" 2>/dev/null; then
  echo "REFUSED: the Mac engine is running (pid $(cat data/engine.pid)); two engines never run at once. Cut over first (SIG_CUTOVER_OK=1 only during the cutover)."; exit 1
fi
NAME="$(basename "$REL")"
STAGE=/opt/sig-stage/tree                       # /opt/sig-stage is root:root 700: the Mac's uid on the staged files is never visible
BK=/opt/sig-engine/data/backup
PERMS='--no-perms'   # Piero 13:15 Oct 22: rsync -a copied the release folder's 755 onto /opt/sig-engine after each push; --no-perms leaves
                     # the folders' modes alone (the Mac's openrsync 2.6.9 accepts --chmod but does not apply it: tested 10:50 Oct 23)
ssh "$SERVER" "set -e; install -d -o root -g root -m 700 /opt/sig-stage; install -d -o sig -g sig -m 750 $BK; \
  cp -p /opt/sig-engine/data/overrides.json $BK/overrides-\$(date +%F_%H%M%S)-pre-$NAME.json && echo 'overrides.json backed up (pre-push)'"
rsync -a $PERMS --exclude data --exclude '__pycache__' --exclude '.tested' "$REL/" "$SERVER:$STAGE/"
rsync -a $PERMS --exclude data --exclude releases --exclude '.venv' --exclude '__pycache__' --exclude '*.pid' --exclude '*.lock' --exclude '.git' --exclude run --exclude '.DS_Store' --exclude 'docs/screenshots' --exclude 'RUN_LOG.md' --exclude '*_spec.md' tools deploy docs "$SERVER:$STAGE/"
ssh "$SERVER" "set -e; PRE=\$(ls -t $BK/overrides-*-pre-$NAME.json | head -1); systemctl stop sig-engine; \
  cp -p /opt/sig-engine/data/overrides.json $BK/overrides-\$(date +%F_%H%M%S)-stopped-$NAME.json; \
  python3 -c 'import json,sys; a,b=(json.load(open(p)) for p in sys.argv[1:3]); L=lambda d: {k: v.get(\"state\") for k, v in (d.get(\"layers\") or {}).items()}; sys.exit(0 if set(a) <= set(b) and L(a) == L(b) else 1)' \"\$PRE\" /opt/sig-engine/data/overrides.json \
    || { cp -p \"\$PRE\" /opt/sig-engine/data/overrides.json; echo \"OVERRIDES RESTORED: the stop wrote fewer keys or other layer states; \$PRE put back\"; }; \
  rsync -a --no-perms --chown=sig:sig --exclude /data $STAGE/ /opt/sig-engine/; \
  chown -R sig:sig /opt/sig-engine && chmod 750 /opt/sig-engine && chown root:sig /opt/research && chmod 750 /opt/research; \
  echo \"\$(date '+%F %T') $NAME\" >> /opt/sig-engine/data/releases.log; \
  systemctl start sig-engine && sleep 3 && systemctl is-active sig-engine && journalctl -u sig-engine -n 3 --no-pager | tail -3" \
  || { echo "PUSH FAILED on the server after staging: the engine may be stopped (systemctl status sig-engine; data/backup/ has the overrides copies)"; exit 1; }
echo "pushed $REL to $SERVER and restarted sig-engine $(date '+%F %T')"
deploy/server/perms_check.sh || echo "PERMS FAIL: tell Piero (the line is in RUN_LOG)"   # Piero 13:15 Oct 22 item 2
