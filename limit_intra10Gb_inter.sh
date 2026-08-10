#!/bin/bash
# Fixed intra-rack 10 Gb/s; configurable inter-rack bandwidth
# (wondershaper, Kbps; same scale as limit_1Gb.sh: 1 Gb/s = 1048576 Kbps).
#
# Usage: sh limit_intra10Gb_inter.sh <inter-rack_Gb/s>
#   Supported: 0.5 | 1 | 2 | 5 | 10
#
# Run before merge; skip during placement. After merge: sh unlimit_all_proxy.sh
#
# If NIC-to-rack mapping is reversed, swap enp6s0f0 / enp6s0f1 roles below.

INTER_GB="${1:-}"

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

case "$INTER_GB" in
  0.5) INTER_RACK_Kbps=524288 ;;      # 0.5 Gb/s
  1)   INTER_RACK_Kbps=1048576 ;;     # 1 Gb/s (10:1 vs intra-rack)
  2)   INTER_RACK_Kbps=2097152 ;;     # 2 Gb/s
  5)   INTER_RACK_Kbps=5242880 ;;     # 5 Gb/s
  10)  INTER_RACK_Kbps=10485760 ;;    # 10 Gb/s (same as intra-rack)
  *)
    echo "Usage: $0 <0.5|1|2|5|10>   (inter-rack Gb/s; intra-rack fixed at 10 Gb/s)" >&2
    exit 1
    ;;
esac

INTRA_RACK_Kbps=10485760   # 10 Gb/s intra-rack (fixed)

# enp6s0f0: intra-rack
if ip link show enp6s0f0 >/dev/null 2>&1 && ip link show enp6s0f0 | grep -q 'state UP'; then
    "$WS_BIN" -a enp6s0f0 -d "$INTRA_RACK_Kbps" -u "$INTRA_RACK_Kbps" || exit 1
    echo "enp6s0f0: $INTRA_RACK_Kbps Kbps (10 Gb/s intra-rack)"
    applied=1
fi

# enp6s0f1: inter-rack
if ip link show enp6s0f1 >/dev/null 2>&1 && ip link show enp6s0f1 | grep -q 'state UP'; then
    "$WS_BIN" -a enp6s0f1 -d "$INTER_RACK_Kbps" -u "$INTER_RACK_Kbps" || exit 1
    echo "enp6s0f1: $INTER_RACK_Kbps Kbps (inter-rack ${INTER_GB} Gb/s)"
    applied=1
fi

if [ "$applied" -eq 0 ]; then
  echo "Warning: No active interface matched (enp6s0f0/enp6s0f1)." >&2
  exit 2
fi
