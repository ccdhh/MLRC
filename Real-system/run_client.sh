#!/usr/bin/env bash

# Run the gLRC client from its dedicated CloudLab node.
#
# Usage:
#   ./cloudlab/run_client.sh                  # interactive: prompt for f and trials
#   ./cloudlab/run_client.sh -f 3 -n 10       # 3 failed blocks, 10 random trials
#   ./cloudlab/run_client.sh --fail-count 2 --trials 5
#   ./cloudlab/run_client.sh --failed-blocks D0,D7,G1 -n 10
#   ./cloudlab/run_client.sh -f 3 -n 10 --seed 20260721
#   ./cloudlab/run_client.sh --max-failure -n 10
#   GLRC_FAIL_COUNT=2 GLRC_TRIALS=5 ./cloudlab/run_client.sh
#
# Extra args are forwarded to main_client (see main_client --help).
# Every run is also saved under logs/client_runs. Override the directory with
# DDRT_CLIENT_LOG_DIR, or disable logging with DDRT_CLIENT_LOG=0.

set -euo pipefail
source "$(cd "$(dirname "$0")" && pwd)/common.sh"
load_hosts

interactive=1
for arg in "$@"; do
  case "$arg" in
    -f|--fail-count|-n|--trials|--failed-blocks|--seed|--failure-mode|--max-failure|-h|--help) interactive=0 ;;
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
[[ -n "${GLRC_RANDOM_SEED:-}" ]] && env_exports+=" GLRC_RANDOM_SEED=$(printf '%q' "$GLRC_RANDOM_SEED")"
[[ -n "${GLRC_FAILURE_MODE:-}" ]] && env_exports+=" GLRC_FAILURE_MODE=$(printf '%q' "$GLRC_FAILURE_MODE")"

ssh_args=(-o BatchMode=yes -o ConnectTimeout=10 -o StrictHostKeyChecking=accept-new)
if [[ -f "$SSH_KEY" ]]; then
  ssh_args+=(-i "$SSH_KEY" -o IdentitiesOnly=yes)
fi
# Allocate a TTY so main_client can prompt for f / trials.
if ((interactive)); then
  ssh_args+=(-t)
fi

client_command="cd $REMOTE_ROOT && ${env_exports} $REMOTE_ROOT/project/cmake/build/main_client${quoted_args}"
if [[ "${DDRT_CLIENT_LOG:-1}" == "0" ]]; then
  ssh "${ssh_args[@]}" "${SSH_USER}@${CLIENT_HOST}" "$client_command"
else
  log_dir="${DDRT_CLIENT_LOG_DIR:-$ROOT/logs/client_runs}"
  mkdir -p "$log_dir"
  timestamp="$(date '+%Y%m%d_%H%M%S')"
  log_file="$log_dir/glrc_${timestamp}_$$.log"
  {
    echo "========== DdlRT client run =========="
    echo "started_at: $(date --iso-8601=seconds)"
    echo "client_host: $CLIENT_HOST"
    echo "arguments:${quoted_args:- (interactive)}"
    echo "log_file: $log_file"
    echo "======================================"
    status=0
    ssh "${ssh_args[@]}" "${SSH_USER}@${CLIENT_HOST}" "$client_command" || status=$?
    echo
    echo "finished_at: $(date --iso-8601=seconds)"
    echo "exit_code: $status"
    exit "$status"
  } 2>&1 | tee "$log_file"
  echo "Saved client run log: $log_file"
fi
