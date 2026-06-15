#!/bin/bash
# Start coordinator + one repair proxy per datanode (see clusterInformation.xml).
# Run from repo root. Requires: project/cmake/build/* binaries and generated clusterInformation.xml.
#
# Process stdout/stderr go to logs/ (not the terminal). Set GLRC_FOREGROUND=1 to keep old behavior.
set -euo pipefail

ROOT="$(cd "$(dirname "$0")" && pwd)"
cd "$ROOT"

BIN="$ROOT/project/cmake/build"
HOST="${GLRC_HOST:-127.0.0.1}"
LOG_DIR="$ROOT/logs"
FOREGROUND="${GLRC_FOREGROUND:-0}"

if [[ ! -x "$BIN/run_coordinator" ]]; then
  echo "Missing $BIN/run_coordinator — run: cd project && bash compile.sh"
  exit 1
fi

mkdir -p logs storage
rm -rf storage/*

python3 "$ROOT/small_tools/generate_local_cluster.py" --host "$HOST"

echo "Stopping old processes..."
pkill -9 -f 'run_coordinator|run_proxy|run_datanode' 2>/dev/null || true
sleep 1

launch() {
  local logfile="$1"
  shift
  if [[ "$FOREGROUND" == "1" ]]; then
    "$@" &
  else
    "$@" >>"$logfile" 2>&1 &
  fi
}

echo "Starting coordinator (log: logs/coordinator.log)..."
launch "$LOG_DIR/coordinator.log" "$BIN/run_coordinator"
sleep 2

proxy_count=0
echo "Starting repair proxies (one per datanode)..."
while read -r proxy; do
  safe="${proxy//:/_}"
  launch "$LOG_DIR/proxy_${safe}.log" env DDRT_QUIET_CONFIG=1 "$BIN/run_proxy" "$proxy"
  proxy_count=$((proxy_count + 1))
done < <(python3 "$ROOT/small_tools/generate_local_cluster.py" --host "$HOST" --list-proxies)
echo "  started $proxy_count proxies -> logs/proxy_*.log"

sleep 3

dn_count=0
echo "Starting datanodes..."
while read -r dn; do
  port="${dn##*:}"
  safe="${dn//:/_}"
  launch "$LOG_DIR/datanode_${safe}.log" env DDRT_QUIET_CONFIG=1 "$BIN/run_datanode" "0.0.0.0:$port"
  dn_count=$((dn_count + 1))
done < <(python3 "$ROOT/small_tools/generate_local_cluster.py" --host "$HOST" --list-datanodes)
echo "  started $dn_count datanodes -> logs/datanode_*.log"

sleep 3
live_proxy="$(pgrep -c run_proxy || echo 0)"
live_dn="$(pgrep -c run_datanode || echo 0)"
echo "Ready: coordinator + ${live_proxy} proxies + ${live_dn} datanodes"
echo "Logs: $LOG_DIR/  (tail -f logs/coordinator.log for repair traces)"
echo "Kill with: bash kill_all.sh"
