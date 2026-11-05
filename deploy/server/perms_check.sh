#!/bin/bash
# After every deploy (push_release.sh): assert /opt/sig-engine is 750 sig:sig, /opt/research 750 root:sig and the push's stage
# /opt/sig-stage 700 root:root. On any failure a PERMS FAIL line goes to RUN_LOG and this script exits 1. Usage: deploy/server/perms_check.sh
SERVER="${SIG_SERVER:-user@your-server}"
ssh -o BatchMode=yes "$SERVER" 'bash -s' <<'REMOTE'
p1=$(stat -c "%a %U:%G" /opt/sig-engine); p2=$(stat -c "%a %U:%G" /opt/research); p3=$(stat -c "%a %U:%G" /opt/sig-stage 2>/dev/null || echo absent)
msg="/opt/sig-engine $p1 (want 750 sig:sig), /opt/research $p2 (want 750 root:sig), /opt/sig-stage $p3 (want 700 root:root)"
if [ "$p1" = "750 sig:sig" ] && [ "$p2" = "750 root:sig" ] && [ "$p3" = "700 root:root" ]; then
  echo "PERMS OK: $msg"
else
  echo "PERMS FAIL: $msg"
  printf -- "- [%s] PERMS FAIL after deploy: %s\n" "$(date +%H:%M)" "$msg" >> /opt/sig-engine/docs/RUN_LOG.md && chown sig:sig /opt/sig-engine/docs/RUN_LOG.md
  exit 1
fi
REMOTE
