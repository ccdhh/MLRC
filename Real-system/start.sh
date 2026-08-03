#!/usr/bin/env bash

# Start the coordinator on its dedicated host and one datanode/proxy pair on
# every storage host.  Storage hosts reuse the same two service ports because
# each pair has a distinct real IP address.

set -euo pipefail
source "$(cd "$(dirname "$0")" && pwd)/common.sh"
load_hosts

BIN="$REMOTE_ROOT/project/cmake/build"
if [[ -z "$STORAGE_ROOT" || "$STORAGE_ROOT" == "/" ]]; then
  echo "unsafe DDRT_STORAGE_ROOT: '$STORAGE_ROOT'" >&2
  exit 2
fi

echo "Stopping stale processes..."
"$(cd "$(dirname "$0")" && pwd)/stop.sh" --keep-storage

# Start storage first. Coordinator registers gRPC stubs at startup; if proxies
# are still down (old order), hop-ready RPCs can hit Connection refused — and
# OptimalLRC local chains touch every G* hop, so one late proxy fails repair.
for host in "${STORAGE_HOSTS[@]}"; do
  echo "Starting datanode + proxy on $host..."
  remote "$host" \
    "cd $REMOTE_ROOT && rm -rf '$STORAGE_ROOT' && mkdir -p logs '$STORAGE_ROOT' && ( DDRT_STORAGE_ROOT='$STORAGE_ROOT' DDRT_ONE_PROXY_PER_HOST=1 DDRT_QUIET_CONFIG=1 $BIN/run_datanode ${host}:${DATANODE_PORT} >logs/datanode.log 2>&1 </dev/null & DDRT_ONE_PROXY_PER_HOST=1 DDRT_QUIET_CONFIG=1 $BIN/run_proxy ${host}:${PROXY_PORT} >logs/proxy.log 2>&1 </dev/null & ) && echo started"
done

wait_tcp() {
  local host="$1" port="$2" tries="${3:-60}"
  local i
  for ((i = 1; i <= tries; i++)); do
    if timeout 1 bash -c "echo >/dev/tcp/${host}/${port}" 2>/dev/null; then
      return 0
    fi
    sleep 1
  done
  return 1
}

echo "Waiting for ${#STORAGE_HOSTS[@]} proxies on :${PROXY_PORT}..."
not_ready=()
for host in "${STORAGE_HOSTS[@]}"; do
  if wait_tcp "$host" "$PROXY_PORT" 90; then
    echo "  proxy ready: $host:$PROXY_PORT"
  else
    echo "  proxy NOT ready: $host:$PROXY_PORT" >&2
    not_ready+=("$host")
  fi
done
if ((${#not_ready[@]} > 0)); then
  echo "Proxies still down after wait: ${not_ready[*]}" >&2
  exit 1
fi

# Background inside a subshell so ssh can exit immediately.  Plain
# "nohup ... &" leaves the remote shell (and ssh) waiting on the job,
# including when the coordinator host is node0 via 10.10.1.1.
echo "Starting coordinator on $COORDINATOR_HOST..."
remote "$COORDINATOR_HOST" \
  "cd $REMOTE_ROOT && mkdir -p logs storage && ( DDRT_ONE_PROXY_PER_HOST=1 $BIN/run_coordinator >logs/coordinator.log 2>&1 </dev/null & ) && echo started"

echo "Waiting for coordinator :${COORDINATOR_PORT}..."
if ! wait_tcp "$COORDINATOR_HOST" "$COORDINATOR_PORT" 60; then
  echo "Coordinator not listening on $COORDINATOR_HOST:$COORDINATOR_PORT" >&2
  exit 1
fi

echo "Started coordinator and ${#STORAGE_HOSTS[@]} storage pairs."
echo "Check with: Real-system/status.sh"
