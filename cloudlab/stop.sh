#!/usr/bin/env bash

# Stop all DdlRT roles.  Storage is preserved by default; pass
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

for host in "${ALL_HOSTS[@]}"; do
  command="pkill -f 'run_coordinator|run_proxy|run_datanode|main_client' >/dev/null 2>&1 || true"
  if ((CLEAR_STORAGE)) && [[ "$host" != "$COORDINATOR_HOST" && "$host" != "$CLIENT_HOST" ]]; then
    command+="; rm -rf $REMOTE_ROOT/storage/*"
  fi
  remote "$host" "$command"
done

echo "Cluster stopped."
