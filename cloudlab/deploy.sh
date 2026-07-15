#!/usr/bin/env bash

# Synchronize the prepared repository from node0 to every CloudLab host.
# Build once on node0 before running this script; binaries and generated
# topology files are copied verbatim so every process uses the same revision.

set -euo pipefail
source "$(cd "$(dirname "$0")" && pwd)/common.sh"
load_hosts

[[ -x "$ROOT/project/cmake/build/run_coordinator" ]] || {
  echo "missing build artifacts; run 'bash compile.sh' on node0 first" >&2
  exit 1
}

for host in "${ALL_HOSTS[@]}"; do
  if [[ "$host" == "$COORDINATOR_HOST" ]]; then
    continue
  fi
  echo "Deploying to $host..."
  rsync -az --delete \
    --exclude='.git/' --exclude='storage/' --exclude='logs/' \
    "$ROOT/" "${SSH_USER}@${host}:${REMOTE_ROOT}/"
done

echo "Deployment complete."
