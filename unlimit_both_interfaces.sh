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

failed=0
cleared_any=0

clear_iface() {
    iface="$1"
    root_deleted=0
    ingress_deleted=0

    if ! ip link show "$iface" >/dev/null 2>&1; then
        return 0
    fi

    # Wondershaper clear is unreliable on some nodes; clear qdisc directly.
    tc qdisc del dev "$iface" root >/dev/null 2>&1
    if [ $? -eq 0 ]; then
        root_deleted=1
    fi

    tc qdisc del dev "$iface" ingress >/dev/null 2>&1
    if [ $? -eq 0 ]; then
        ingress_deleted=1
    fi

    qdisc_state="$(tc qdisc show dev "$iface" 2>/dev/null)"
    case "$qdisc_state" in
        *"htb "*|*"ingress "*) 
            echo "failed to clear $iface: $qdisc_state" >&2
            failed=1
            return 0
            ;;
    esac

    if [ "$root_deleted" -eq 1 ] || [ "$ingress_deleted" -eq 1 ]; then
        echo "cleared: $iface"
        cleared_any=1
        return 0
    fi

    echo "already clear: $iface"
    cleared_any=1
    return 0
}

clear_iface enp6s0f0
clear_iface enp6s0f1

if [ "$cleared_any" -eq 0 ]; then
    echo "Warning: No matched interface found (enp6s0f0/enp6s0f1)." >&2
fi

exit "$failed"
