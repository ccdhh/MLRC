#!/usr/bin/env bash

# Start the coordinator on its dedicated host and one datanode/proxy pair on
# every storage host.  Storage hosts reuse the same two service ports because
# each pair has a distinct real IP address.

set -euo pipefail
source "$(cd "$(dirname "$0")" && pwd)/common.sh"
load_hosts

BIN="$REMOTE_ROOT/project/cmake/build"

echo "Stopping stale processes..."
"$(cd "$(dirname "$0")" && pwd)/stop.sh" --keep-storage

echo "Starting coordinator on $COORDINATOR_HOST..."
remote "$COORDINATOR_HOST" \
  "cd $REMOTE_ROOT && mkdir -p logs storage && DDRT_ONE_PROXY_PER_HOST=1 nohup $BIN/run_coordinator > logs/coordinator.log 2>&1 &"
sleep 2

for host in "${STORAGE_HOSTS[@]}"; do
  echo "Starting datanode + proxy on $host..."
  remote "$host" \
    "cd $REMOTE_ROOT && mkdir -p logs storage && DDRT_ONE_PROXY_PER_HOST=1 DDRT_QUIET_CONFIG=1 nohup $BIN/run_datanode ${host}:${DATANODE_PORT} > logs/datanode.log 2>&1 & DDRT_ONE_PROXY_PER_HOST=1 DDRT_QUIET_CONFIG=1 nohup $BIN/run_proxy ${host}:${PROXY_PORT} > logs/proxy.log 2>&1 &"
done

echo "Started coordinator and ${#STORAGE_HOSTS[@]} storage pairs."
echo "Check with: cloudlab/status.sh"
