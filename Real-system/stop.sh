#!/usr/bin/env bash

# Stop all MLRC roles.  Storage is preserved by default; pass
# --clear-storage only when a new experiment must start with empty disks.

set -euo pipefail
source "$(cd "$(dirname "$0")" && pwd)/common.sh"
load_hosts

CLEAR_STORAGE=0
if [[ "${1:-}" == "--clear-storage" ]]; then
  CLEAR_STORAGE=1
elif [[ "${1:-}" != "" && "${1:-}" != "--keep-storage" ]]; then
  echo "usage: $0 [--clear-storage|--keep-storage]" >&2
  exit 2
fi

# Use pkill -x (exact process name) so the remote command line itself is not
# matched.  A broad pkill -f 'run_coordinator|...' on the coordinator host
# (node0 / 10.10.1.1) would kill stop.sh / start.sh via the ssh client argv.
for host in "${ALL_HOSTS[@]}"; do
  command="pkill -x run_coordinator >/dev/null 2>&1 || true"
  command+="; pkill -x run_proxy >/dev/null 2>&1 || true"
  command+="; pkill -x run_datanode >/dev/null 2>&1 || true"
  command+="; pkill -x main_client >/dev/null 2>&1 || true"
  if ((CLEAR_STORAGE)) && [[ "$host" != "$COORDINATOR_HOST" && "$host" != "$CLIENT_HOST" ]]; then
    command+="; rm -rf $REMOTE_ROOT/storage/* '$STORAGE_ROOT'"
  fi
  remote "$host" "$command"
done

echo "Cluster stopped."
