#!/usr/bin/env bash

# Run the gLRC client from its dedicated real-system node.
#
# Usage:
#   ./Real-system/run_client.sh                  # interactive: prompt for f and trials
#   ./Real-system/run_client.sh -f 3 -n 10       # 3 failed blocks, 10 random trials
#   ./Real-system/run_client.sh --fail-count 2 --trials 5
#   ./Real-system/run_client.sh --failed-blocks D0,D7,G1 -n 10
#   ./Real-system/run_client.sh -f 3 -n 10 --seed 20260721
#   ./Real-system/run_client.sh --max-failure -n 10
#   GLRC_STRIPE_NUM=34 ./Real-system/run_client.sh --failed-nodes 2,15 --node-repair -n 5
#   GLRC_STRIPE_NUM=9 ./Real-system/run_client.sh --failed-nodes 1,2 --node-repair --reuse-stripes -n 5
#   GLRC_FAIL_COUNT=2 GLRC_TRIALS=5 ./Real-system/run_client.sh
#
# Extra args are forwarded to main_client (see main_client --help).
# Every SSH session is teed under logs/client_runs. Override with
# DDRT_CLIENT_LOG_DIR, or disable with DDRT_CLIENT_LOG=0.
# Node-repair also writes full/summary logs on the client host; this script
# pulls latest_glrc_node_repair*.{log,csv} into logs/client_runs/from_client/.
# Disable in-process logging with GLRC_SAVE_LOG=0.

set -euo pipefail
source "$(cd "$(dirname "$0")" && pwd)/common.sh"
load_hosts

interactive=1
for arg in "$@"; do
  case "$arg" in
    -f|--fail-count|-n|--trials|--failed-blocks|--seed|--failure-mode|--max-failure|--node-repair|--failed-nodes|--failed-node-ips|--reuse-stripes|-h|--help) interactive=0 ;;
  esac
done
if [[ -n "${GLRC_FAIL_COUNT:-}" || -n "${GLRC_TRIALS:-}" || -n "${GLRC_FAILED_NODES:-}" || -n "${GLRC_FAILED_NODE_IPS:-}" || -n "${GLRC_REUSE_STRIPES:-}" ]]; then
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
[[ -n "${GLRC_FAILED_NODES:-}" ]] && env_exports+=" GLRC_FAILED_NODES=$(printf '%q' "$GLRC_FAILED_NODES")"
[[ -n "${GLRC_FAILED_NODE_IPS:-}" ]] && env_exports+=" GLRC_FAILED_NODE_IPS=$(printf '%q' "$GLRC_FAILED_NODE_IPS")"
[[ -n "${GLRC_REUSE_STRIPES:-}" ]] && env_exports+=" GLRC_REUSE_STRIPES=$(printf '%q' "$GLRC_REUSE_STRIPES")"

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
  set +e
  {
    echo "========== MLRC client run =========="
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
  status=${PIPESTATUS[0]}
  set -e
  echo "Saved client run log: $log_file"

  # Pull structured node-repair logs written by main_client on the client host.
  remote_log_dir="${REMOTE_ROOT}/logs/client_runs"
  fetch_dir="$log_dir/from_client"
  mkdir -p "$fetch_dir"
  scp_args=()
  for a in "${ssh_args[@]}"; do
    [[ "$a" == "-t" ]] && continue
    scp_args+=("$a")
  done
  for name in latest_glrc_node_repair.log latest_glrc_node_repair_summary.csv; do
    if scp "${scp_args[@]}" "${SSH_USER}@${CLIENT_HOST}:${remote_log_dir}/${name}" \
        "$fetch_dir/${name}" >/dev/null 2>&1; then
      echo "Fetched client result: $fetch_dir/${name}"
    fi
  done
  exit "$status"
fi
