#!/usr/bin/env bash

# Run the gLRC client from its dedicated CloudLab node.  Extra arguments are
# forwarded to main_client, e.g. a coordinator address override.

set -euo pipefail
source "$(cd "$(dirname "$0")" && pwd)/common.sh"
load_hosts

args=("$@")
quoted_args=""
for arg in "${args[@]}"; do
  quoted_args+=" $(printf '%q' "$arg")"
done

remote "$CLIENT_HOST" \
  "cd $REMOTE_ROOT && COORDINATOR_ADDR=${COORDINATOR_HOST}:${COORDINATOR_PORT} $REMOTE_ROOT/project/cmake/build/main_client${quoted_args}"
