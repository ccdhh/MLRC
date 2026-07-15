#!/usr/bin/env bash

# Run the gLRC client from its dedicated CloudLab node.
#
# Usage:
#   ./cloudlab/run_client.sh                  # interactive: prompt for f and trials
#   ./cloudlab/run_client.sh -f 3 -n 10       # 3 failed blocks, 10 random trials
#   ./cloudlab/run_client.sh --fail-count 2 --trials 5
#   GLRC_FAIL_COUNT=2 GLRC_TRIALS=5 ./cloudlab/run_client.sh
#
# Extra args are forwarded to main_client (see main_client --help).

set -euo pipefail
source "$(cd "$(dirname "$0")" && pwd)/common.sh"
load_hosts

interactive=1
for arg in "$@"; do
  case "$arg" in
    -f|--fail-count|-n|--trials|-h|--help) interactive=0 ;;
  esac
done
if [[ -n "${GLRC_FAIL_COUNT:-}" || -n "${GLRC_TRIALS:-}" ]]; then
  interactive=0
fi

quoted_args=""
for arg in "$@"; do
  quoted_args+=" $(printf '%q' "$arg")"
done

env_exports="COORDINATOR_ADDR=${COORDINATOR_HOST}:${COORDINATOR_PORT}"
[[ -n "${GLRC_FAIL_COUNT:-}" ]] && env_exports+=" GLRC_FAIL_COUNT=$(printf '%q' "$GLRC_FAIL_COUNT")"
[[ -n "${GLRC_TRIALS:-}" ]] && env_exports+=" GLRC_TRIALS=$(printf '%q' "$GLRC_TRIALS")"
[[ -n "${GLRC_STRIPE_NUM:-}" ]] && env_exports+=" GLRC_STRIPE_NUM=$(printf '%q' "$GLRC_STRIPE_NUM")"
[[ -n "${GLRC_FAILED_BLOCKS:-}" ]] && env_exports+=" GLRC_FAILED_BLOCKS=$(printf '%q' "$GLRC_FAILED_BLOCKS")"

ssh_args=(-o BatchMode=yes -o ConnectTimeout=10 -o StrictHostKeyChecking=accept-new)
if [[ -f "$SSH_KEY" ]]; then
  ssh_args+=(-i "$SSH_KEY" -o IdentitiesOnly=yes)
fi
# Allocate a TTY so main_client can prompt for f / trials.
if ((interactive)); then
  ssh_args+=(-t)
fi

ssh "${ssh_args[@]}" "${SSH_USER}@${CLIENT_HOST}" \
  "cd $REMOTE_ROOT && ${env_exports} $REMOTE_ROOT/project/cmake/build/main_client${quoted_args}"
