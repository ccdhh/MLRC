#!/usr/bin/env bash
# From a host that has L3 reachability to cluster nodes, verify TCP ports for UniLRC.
#
# Usage:
#   ./check_cluster_connectivity.sh
#   ./check_cluster_connectivity.sh /path/to/clusterInformation.xml /path/to/parameterConfiguration.xml
#
# Requires: bash with /dev/tcp, timeout(1). Run from a machine that can route to 10.10.1.x
# (e.g. a client in the same subnet, or use SSH port-forward — not from an unrelated admin host).

set -uo pipefail

ROOT="$(cd "$(dirname "$0")" && pwd)"
CLUSTER_XML="${1:-$ROOT/project/config/clusterInformation.xml}"
SYS_XML="${2:-$ROOT/project/config/parameterConfiguration.xml}"

if [[ "$CLUSTER_XML" == *"/path/to/"* ]] || [[ "$SYS_XML" == *"/path/to/"* ]]; then
  echo "ERROR: Replace the documentation placeholder with real paths, or run with no arguments:"
  echo "  $0"
  exit 1
fi

if [[ ! -f "$CLUSTER_XML" ]]; then
  echo "Missing cluster XML: $CLUSTER_XML"
  exit 1
fi
if [[ ! -f "$SYS_XML" ]]; then
  echo "Missing sys config: $SYS_XML"
  exit 1
fi

echo "=== Source host (where this script runs) ==="
echo "  hostname: $(hostname)"
echo "  addresses: $(hostname -I 2>/dev/null | tr -s ' ' | sed 's/ $//')"
echo "  (If this machine has no route to the cluster subnet, every remote probe below will FAIL.)"
echo ""

probe() {
  local ip="$1" port="$2" label="$3"
  if timeout 3 bash -c "echo >/dev/tcp/$ip/$port" 2>/dev/null; then
    echo "  OK   $ip:$port ($label)"
    return 0
  else
    echo "  FAIL $ip:$port ($label)"
    return 1
  fi
}

ok_count=0
fail_count=0
count_probe() {
  if probe "$1" "$2" "$3"; then
    ok_count=$((ok_count + 1))
  else
    fail_count=$((fail_count + 1))
  fi
}

COORD_IP=$(grep -m1 '<CoordinatorIP>' "$SYS_XML" | sed -n 's/.*<CoordinatorIP>\([^<]*\)<.*/\1/p')
COORD_PORT=$(grep -m1 '<CoordinatorPort>' "$SYS_XML" | sed -n 's/.*<CoordinatorPort>\([^<]*\)<.*/\1/p')

echo "=== Coordinator ($SYS_XML) ==="
if [[ -n "$COORD_IP" && -n "$COORD_PORT" ]]; then
  count_probe "$COORD_IP" "$COORD_PORT" "coordinator gRPC (as in XML)"
  # If we are on the coordinator box, gRPC listens on 0.0.0.0 — loopback should work if process is up
  if echo " $(hostname -I 2>/dev/null) " | grep -q " ${COORD_IP} "; then
    echo "  (detected local address $COORD_IP — localhost check:)"
    count_probe "127.0.0.1" "$COORD_PORT" "coordinator gRPC (127.0.0.1 only if run ON coordinator)"
  fi
else
  echo "  (could not parse CoordinatorIP/CoordinatorPort)"
fi

echo ""
echo "=== Proxies (gRPC + data port = proxy_port+1 per PROXY_PORT_SHIFT) ==="
while read -r pp; do
  [[ -z "$pp" ]] && continue
  pip="${pp%%:*}"
  pport="${pp##*:}"
  pdata=$((pport + 1))
  echo "-- $pip gRPC $pport, data $pdata"
  count_probe "$pip" "$pport" "proxy gRPC"
  count_probe "$pip" "$pdata" "proxy data"
done < <(grep -oE 'proxy="[^"]+"' "$CLUSTER_XML" | sed 's/proxy="//;s/"$//' | sort -u)

echo ""
echo "=== Datanodes (gRPC on uri port, bulk on port+50) ==="
while read -r uri; do
  [[ -z "$uri" ]] && continue
  dip="${uri%%:*}"
  dport="${uri##*:}"
  ddata=$((dport + 50))
  echo "-- $dip logical $dport -> data $ddata"
  count_probe "$dip" "$dport" "datanode gRPC"
  count_probe "$dip" "$ddata" "datanode bulk TCP"
done < <(grep 'datanode uri=' "$CLUSTER_XML" | sed -n 's/.*uri="\([^"]*\)".*/\1/p' | sort -u)

echo ""
echo "=== Summary ==="
echo "  OK: $ok_count   FAIL: $fail_count"
if [[ "$fail_count" -gt 0 ]]; then
  echo ""
  echo "FAIL 常见原因:"
  echo "  1) 进程未启动: 在对应机器上启动 run_coordinator / run_proxy_only / run_datanode"
  echo "  2) 防火墙: 放行 coordinator(如55555)、proxy(50405-50406)、datanode(17600,17650)"
  echo "  3) 执行脚本的机器与 10.10.1.x 不通: 换到集群内客户端或 coordinator 上执行本脚本，或先 ping 任一节点的 XML IP"
  echo "  4) 若 coordinator 上 127.0.0.1 OK 但 10.10.1.2 FAIL: 检查本机是否真有该地址、是否绑在正确网卡"
fi
echo ""
echo "Tip: datanode 建议监听 0.0.0.0 (见 run_datanode.sh)；XML 中填写其它节点访问本机时使用的 IP。"
