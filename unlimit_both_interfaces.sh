#!/bin/bash
# 解除 enp6s0f0、enp6s0f1 上的 wondershaper 限速（与 limit_intra10Gb_inter1Gb.sh 成对使用）

if ip link show enp6s0f0 &> /dev/null && ip link show enp6s0f0 | grep -q 'state UP'; then
    wondershaper -c -a enp6s0f0
    echo "cleared: enp6s0f0"
fi
if ip link show enp6s0f1 &> /dev/null && ip link show enp6s0f1 | grep -q 'state UP'; then
    wondershaper -c -a enp6s0f1
    echo "cleared: enp6s0f1"
fi
