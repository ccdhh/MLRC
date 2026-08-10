#!/bin/bash
# gLRC repair test helper (interactive f + trial count, full metrics).
#
# One-shot interactive test (recommended; prompts for f and trial count):
#   cd /users/chendh/DdlRT && ./test_glrc_ilp.sh test
#
# Other subcommands:
#   start   start the cluster
#   stop    stop processes and clear storage
#   client  run client only (non-TTY: use GLRC_FAIL_COUNT / GLRC_TRIALS)
#
# Switch C0-C3: edit project/config/parameterConfiguration.xml then restart
set -euo pipefail

ROOT="$(cd "$(dirname "$0")" && pwd)"
cd "$ROOT"

BIN="$ROOT/project/cmake/build"
CLIENT="$BIN/main_client"

cmd="${1:-}"

cluster_running() {
  pgrep -x run_coordinator >/dev/null 2>&1 \
    && [[ "$(pgrep -c run_proxy 2>/dev/null || echo 0)" -ge 1 ]] \
    && [[ "$(pgrep -c run_datanode 2>/dev/null || echo 0)" -ge 1 ]]
}

wait_ready() {
  local tries=60
  for ((i=0; i<tries; i++)); do
    if cluster_running; then
      sleep 2
      echo "Cluster ready: coordinator=1 proxy=$(pgrep -c run_proxy || echo 0) datanode=$(pgrep -c run_datanode || echo 0)"
      return 0
    fi
    sleep 2
  done
  echo "Timed out waiting for cluster processes" >&2
  return 1
}

ensure_cluster() {
  if cluster_running; then
    echo "Cluster already running (skip start)."
    return 0
  fi
  ulimit -n 65535 2>/dev/null || true
  bash "$ROOT/start_local_glrc_cluster.sh"
  wait_ready
}

run_client_interactive() {
  if [[ ! -x "$CLIENT" ]]; then
    echo "Missing $CLIENT — run: bash compile.sh" >&2
    exit 1
  fi
  export GLRC_STRIPE_NUM="${GLRC_STRIPE_NUM:-1}"
  export COORDINATOR_ADDR="${COORDINATOR_ADDR:-127.0.0.1:55555}"
  # Interactive mode: do not preset f/trials; main_client reads from the terminal
  unset GLRC_FAIL_COUNT GLRC_TRIALS GLRC_FAILED_BLOCKS

  echo ""
  echo "======== gLRC interactive repair test ========"
  echo "Config: project/config/parameterConfiguration.xml"
  echo "Metrics per trial: selected_equations, equation_select_time,"
  echo "  disk_read_time, network_transfer_time, decode_time, disk_write_time, total_time"
  echo "============================================="
  echo ""

  if [[ -t 0 ]]; then
    "$CLIENT" "$COORDINATOR_ADDR"
  else
    # When launched from a script, still attach to the TTY for cin >> f / trials
    "$CLIENT" "$COORDINATOR_ADDR" </dev/tty
  fi
}

case "$cmd" in
  start)
    ulimit -n 65535 2>/dev/null || true
    bash "$ROOT/start_local_glrc_cluster.sh"
    wait_ready
    ;;
  stop)
    bash "$ROOT/kill_all.sh"
    ;;
  client)
    export GLRC_STRIPE_NUM="${GLRC_STRIPE_NUM:-1}"
    export GLRC_FAIL_COUNT="${GLRC_FAIL_COUNT:-1}"
    export GLRC_TRIALS="${GLRC_TRIALS:-1}"
    export COORDINATOR_ADDR="${COORDINATOR_ADDR:-127.0.0.1:55555}"
    "$CLIENT" "$COORDINATOR_ADDR"
    ;;
  test)
    ensure_cluster
    run_client_interactive
    ;;
  *)
    cat <<EOF
Usage: $0 {test|start|stop|client}

  test    one-shot test: start cluster if needed -> prompt f/trials -> print metrics
  start   start the cluster only
  stop    stop the cluster
  client  non-interactive client (env GLRC_FAIL_COUNT / GLRC_TRIALS)

Examples:
  cd $ROOT && ./test_glrc_ilp.sh test
EOF
    exit 1
    ;;
esac
