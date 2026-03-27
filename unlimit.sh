#!/bin/bash

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

# 清除 enp6s0f0（只要接口存在就清）
if ip link show enp6s0f0 &> /dev/null; then
    "$WS_BIN" clear enp6s0f0 || exit 1
    applied=1
fi

# 清除 enp6s0f1（只要接口存在就清）
if ip link show enp6s0f1 &> /dev/null; then
    "$WS_BIN" clear enp6s0f1 || exit 1
    applied=1
fi

if [ "$applied" -eq 0 ]; then
    echo "Error: No active interface found!" >&2
    exit 1
fi

exit 0