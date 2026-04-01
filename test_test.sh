#!/bin/bash
# 机架内固定 10Gb/s，机架间带宽可配（wondershaper，单位 Kbps；与现有 limit_1Gb.sh 一致：1Gb/s = 1048576 Kbps）
#
# 用法: sh limit_intra10Gb_inter.sh <机架间_Gb/s>
#   支持: 0.5 | 1 | 2 | 5 | 10
#
# 合并前再执行；放置阶段勿执行。合并后: sh unlimit_all_proxy.sh
#
# 若网卡与机架对应相反，请交换脚本里 enp6s0f0 / enp6s0f1 的用途。

INTER_GB="${1:-}"

set -u

# shellcheck source=/dev/null
. "$(cd "$(dirname "$0")" && pwd)/shape_prep.sh"
shape_prep_env
shape_prep_load_modules
shape_prep_tc_resolve || { echo "Error: tc (iproute2) not found." >&2; exit 127; }

WS_BIN="$(command -v wondershaper || true)"
if [ -z "$WS_BIN" ]; then
  for p in /usr/sbin/wondershaper /sbin/wondershaper /usr/local/sbin/wondershaper /usr/bin/wondershaper; do
    if [ -x "$p" ]; then
      WS_BIN="$p"
      break
    fi
  done
fi
if [ -z "$WS_BIN" ]; then
  echo "Error: wondershaper if not found。Debian/Ubuntu can: sudo apt-get install -y wondershaper" >&2
  echo "  if use install_wondershaper.sh to install from source code; if only pdsh to send to all nodes，can not install on node0." >&2
  exit 127
fi

applied=0

case "$INTER_GB" in
  0.5) INTER_RACK_Kbps=524288 ;;      # 0.5 Gb/s
  1)   INTER_RACK_Kbps=1048576 ;;     # 1 Gb/s (与机架内 10:1)
  2)   INTER_RACK_Kbps=2097152 ;;     # 2 Gb/s
  5)   INTER_RACK_Kbps=5242880 ;;     # 5 Gb/s
  10)  INTER_RACK_Kbps=10485760 ;;    # 10 Gb/s（与机架内同速）
  *)
    echo "Usage: $0 <0.5|1|2|5|10>   (inter-rack Gb/s; intra-rack fixed at 10 Gb/s)" >&2
    exit 1
    ;;
esac

INTRA_RACK_Kbps=10485760   # 10 Gb/s 机架内（固定）

# magnific0 / Debian 包装均使用 -a -d -u；勿再用位置参数
# enp6s0f0: 机架内
if ip link show enp6s0f0 &> /dev/null && ip link show enp6s0f0 | grep -q 'state UP'; then
    shape_prep_clear_iface_qdisc enp6s0f0
    "$WS_BIN" -a enp6s0f0 -d "$INTRA_RACK_Kbps" -u "$INTRA_RACK_Kbps" || exit 1
    echo "enp6s0f0: $INTRA_RACK_Kbps Kbps (intra-rack 10 Gb/s)"
    applied=1
fi

# enp6s0f1: 机架间
if ip link show enp6s0f1 &> /dev/null && ip link show enp6s0f1 | grep -q 'state UP'; then
    shape_prep_clear_iface_qdisc enp6s0f1
    "$WS_BIN" -a enp6s0f1 -d "$INTER_RACK_Kbps" -u "$INTER_RACK_Kbps" || exit 1
    echo "enp6s0f1: $INTER_RACK_Kbps Kbps (inter-rack ${INTER_GB} Gb/s)"
    applied=1
fi

if [ "$applied" -eq 0 ]; then
  echo "Warning: No active interface matched (enp6s0f0/enp6s0f1)." >&2
  exit 2
fi