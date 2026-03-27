#!/bin/bash
# 解除 enp6s0f0、enp6s0f1 上的 wondershaper 限速（与 limit_intra10Gb_inter1Gb.sh 成对使用）

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

if ip link show enp6s0f0 &> /dev/null; then
    "$WS_BIN" clear enp6s0f0 || exit 1
    echo "cleared: enp6s0f0"
fi
if ip link show enp6s0f1 &> /dev/null; then
    "$WS_BIN" clear enp6s0f1 || exit 1
    echo "cleared: enp6s0f1"
fi
