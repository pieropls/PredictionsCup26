#!/bin/bash
# The dashboard stays on the server's 127.0.0.1; open it on the laptop through this tunnel, then http://127.0.0.1:8765/?t=<token>
# (the token is in the mirrored data/dashboard_url.txt).
exec ssh -N -o ServerAliveInterval=30 -o ExitOnForwardFailure=yes -L 8765:127.0.0.1:8765 "${1:?usage: tunnel.sh user@host}"
