#!/bin/bash
# Install / refresh the server-side ops timers (run as root on engine-host; Piero 00:20 Oct 26 item 1). Idempotent. Touches only
# sig-ops* units; never the sig-engine service.
set -euo pipefail
cd /opt/sig-engine/server/ops
install -m 644 systemd/sig-ops@.service systemd/sig-ops-*.timer /etc/systemd/system/
mkdir -p /opt/sig-engine/data/ops && chown sig:sig /opt/sig-engine/data/ops && chmod 750 /opt/sig-engine/data/ops
systemctl daemon-reload
for t in systemd/sig-ops-*.timer; do systemctl enable "$(basename "$t")" >/dev/null 2>&1; systemctl restart "$(basename "$t")"; done   # restart: a re-dated timer only reschedules then
systemctl list-timers 'sig-ops-*' --all --no-pager
