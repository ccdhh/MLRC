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

# Avoid duplicate work.
SEEN_IFACES=""
maybe_clear_iface() {
    iface="$1"
    [ -z "$iface" ] && return 0
    case " $SEEN_IFACES " in
        *" $iface "*) return 0 ;;
    esac
    SEEN_IFACES="$SEEN_IFACES $iface"
    clear_iface "$iface"
}

# 1) Try legacy fixed names first.
maybe_clear_iface enp6s0f0
maybe_clear_iface enp6s0f1

# 2) Clear interfaces that carry 10.10.1.x addresses.
for iface in $(ip -4 -o addr show scope global 2>/dev/null | awk '$4 ~ /^10\\.10\\.1\\./ {print $2}'); do
    maybe_clear_iface "$iface"
done

# 3) Fallback: clear any interface currently showing htb/ingress qdisc.
for iface in $(tc qdisc show 2>/dev/null | awk '/^(qdisc htb|qdisc ingress)/ {for(i=1;i<=NF;i++) if($i=="dev"){print $(i+1); break}}' | sort -u); do
    maybe_clear_iface "$iface"
done

if [ "$cleared_any" -eq 0 ]; then
    echo "Warning: No matched interface found for clear (legacy names / 10.10.1.x / htb+ingress)." >&2
fi

exit "$failed"
