#!/bin/bash
# gLRC repair test helper (interactive f + trial count, full metrics).
#
# 一键交互测试（推荐，会提示输入 f 和实验次数）:
#   cd /users/chendh/DdlRT && ./test_glrc_ilp.sh test
#
# 其他子命令:
#   start   启动集群
#   stop    停止并清 storage
#   client  仅跑 client（非 TTY 时用 GLRC_FAIL_COUNT / GLRC_TRIALS 环境变量）
#
# 切换 C0–C3: 改 project/config/parameterConfiguration.xml 后 restart
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
  # 交互模式：不预设 f / trials，由 main_client 从终端读取
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
    # 从脚本调用时仍尝试绑定终端以支持 cin >> f / trials
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

  test    一键测试：自动启集群（若未运行）→ 交互输入 f 与实验次数 → 输出完整指标
  start   仅启动集群
  stop    停止集群
  client  非交互 client（环境变量 GLRC_FAIL_COUNT / GLRC_TRIALS）

示例:
  cd $ROOT && ./test_glrc_ilp.sh test
EOF
    exit 1
    ;;
esac
