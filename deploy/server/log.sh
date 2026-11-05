#!/bin/bash
# Day 10 §9.3 / mm_plan §6: append one RUN_LOG line ON THE SERVER, stamped with the server's own clock (never a typed time).
# Usage: deploy/server/log.sh "text"   (a leading "- " or "### " is kept; the stamp goes right after it)
SERVER="${SIG_SERVER:-user@your-server}"
printf '%s\n' "$1" | ssh -o BatchMode=yes "$SERVER" 'TS=$(date "+%H:%M"); while IFS= read -r L; do case "$L" in "- "*) printf -- "- [%s] %s\n" "$TS" "${L#- }";; "### "*) printf "### [%s] %s\n" "$TS" "${L#\#\#\# }";; *) printf "%s\n" "$L";; esac; done >> /opt/sig-engine/docs/RUN_LOG.md && chown sig:sig /opt/sig-engine/docs/RUN_LOG.md'
