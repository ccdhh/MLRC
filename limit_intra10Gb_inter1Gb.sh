#!/bin/bash
# Intra-rack 10 Gb/s, inter-rack 1 Gb/s (wondershaper, Kbps).
#
# Suggested usage (measure merge-phase bandwidth only):
#   - During data placement: do not run this script (keeps placement fast).
#   - Before "start merge now? (Y/N)": in another terminal run
#       sh limit_all_intra10Gb_inter1Gb.sh
#   - Then enter Y locally to start merge.
#   - After merge: sh unlimit_all_proxy.sh
#
# If NIC-to-rack mapping is reversed, swap the bandwidth values for enp6s0f0 / enp6s0f1.

INTRA_RACK_Kbps=10485760   # 10 Gb/s intra-rack
INTER_RACK_Kbps=1048576    # 1 Gb/s inter-rack

set -u

WS_BIN="$(command -v wondershaper || true)"
if [ -z "$WS_BIN" ]; then
    for p in /usr/sbin/wondershaper /sbin/wondershaper /usr/bin/wondershaper; do
        if [ -x "$p" ]; then
            WS_BIN="$p"
            break
        fi
    done
fi
if [ -z "$WS_BIN" ]; then
    echo "Error: wondershaper not found. Please install it or add it to PATH." >&2
    exit 127
fi

applied=0

# enp6s0f0: intra-rack
if ip link show enp6s0f0 >/dev/null 2>&1 && ip link show enp6s0f0 | grep -q 'state UP'; then
    "$WS_BIN" -a enp6s0f0 -d "$INTRA_RACK_Kbps" -u "$INTRA_RACK_Kbps" || exit 1
    echo "enp6s0f0: $INTRA_RACK_Kbps Kbps (10Gb/s intra-rack)"
    applied=1
fi

# enp6s0f1: inter-rack
if ip link show enp6s0f1 >/dev/null 2>&1 && ip link show enp6s0f1 | grep -q 'state UP'; then
    "$WS_BIN" -a enp6s0f1 -d "$INTER_RACK_Kbps" -u "$INTER_RACK_Kbps" || exit 1
    echo "enp6s0f1: $INTER_RACK_Kbps Kbps (1Gb/s inter-rack)"
    applied=1
fi

if [ "$applied" -eq 0 ]; then
    echo "Warning: No active interface matched (enp6s0f0/enp6s0f1)." >&2
    exit 2
fi
