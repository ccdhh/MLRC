#!/bin/bash
[ -n "${BASH_VERSION:-}" ] || exec bash "$0" "$@"
set -e
set -o pipefail

ROOT_DIR="/users/qiliang/UniLRC"
HOSTS_FILE="$ROOT_DIR/datanode_hosts"
USER="root"
FRESH_MODE=0
if [ "$#" -gt 0 ]; then
  for arg in "$@"; do
    case "$arg" in
      --fresh)
        FRESH_MODE=1
        ;;
      *)
        echo "Unknown argument: $arg"
        echo "Usage: bash start_datanode.sh [--fresh]"
        exit 1
        ;;
    esac
  done
fi

REMOTE_COMMAND_ATTACH="cd $ROOT_DIR && node=\$(hostname) && set -- \$(hostname -I 2>/dev/null) && ip=\${1:-unknown} && ts=\$(date +%H:%M:%S) && echo __DATANODE_STARTING__ node=\$node ip=\$ip time=\$ts && exec bash run_datanode.sh"
# IMPORTANT: keep remote command quoting simple; pdsh executes via sh -c.
# Use bash -lc with a single-quoted script and avoid nested quotes/parentheses.
REMOTE_COMMAND_DETACH="cd $ROOT_DIR && bash -lc 'node=\$(hostname); set -- \$(hostname -I 2>/dev/null); ip=\${1:-unknown}; ts=\$(date +%H:%M:%S); echo __DATANODE_STARTING__ node=\$node ip=\$ip time=\$ts; nohup bash run_datanode.sh >/tmp/unilrc-datanode-start.log 2>&1 < /dev/null & echo __DATANODE_START_SENT__; if command -v ss >/dev/null 2>&1; then for i in 1 2 3 4 5; do ss -lnt 2>/dev/null | grep -q :17600 && ss -lnt 2>/dev/null | grep -q :17650 && { echo __DATANODE_LISTENING__ node=\$node ip=\$ip time=\$(date +%H:%M:%S); exit 0; }; sleep 1; done; echo __DATANODE_LISTENING_FAIL__ node=\$node ip=\$ip time=\$(date +%H:%M:%S); else echo __DATANODE_LISTENING_SKIP__ node=\$node ip=\$ip time=\$(date +%H:%M:%S) no_ss; fi'"
LOCK_FILE="/tmp/start_datanode.lock"

# Bottleneck is usually concurrent SSH from one host, not run_datanode itself:
# sshd MaxStartups drops connections, ConnectTimeout fires, or client runs out of fds.
# Defaults are conservative; raise DATANODE_PARALLEL only after tuning sshd / network.
#   DATANODE_PARALLEL=24 bash start_datanode.sh
#   DATANODE_RETRY=5      bash start_datanode.sh
: "${DATANODE_PARALLEL:=12}"
: "${DATANODE_RETRY:=3}"
: "${DATANODE_PDSH_TIMEOUT:=8}"
: "${DATANODE_FORCE:=0}"
: "${DATANODE_PRECHECK:=1}"
: "${DATANODE_PRECHECK_TIMEOUT:=6}"
: "${DATANODE_ATTACH:=0}"
: "${DATANODE_REQUIRE_LISTENING:=1}"

SSH_OPTS="-o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null -o LogLevel=ERROR -o BatchMode=yes -o PreferredAuthentications=publickey -o NumberOfPasswordPrompts=0 -o ConnectionAttempts=1 -o ConnectTimeout=10"

if [ ! -f "$HOSTS_FILE" ]; then
  echo "Error: missing $HOSTS_FILE"
  exit 1
fi

if [ "$FRESH_MODE" = "1" ]; then
  echo "[fresh] sweeping old processes before start..."
  bash "$ROOT_DIR/kill_all_nodes.sh" || true
  bash "$ROOT_DIR/stop_start_datanode.sh"
fi

echo "Starting datanodes on $(wc -l < "$HOSTS_FILE") hosts (parallel=$DATANODE_PARALLEL, retry=$DATANODE_RETRY, attach=$DATANODE_ATTACH)..."

# Optional force mode: clean previous stuck launcher/pdsh first.
if [ "$DATANODE_FORCE" = "1" ]; then
  echo "[force] terminating old start_datanode/pdsh jobs..."
  pgrep -f "start_datanode.sh" | awk -v self="$$" '$1 != self {print $1}' | xargs -r kill -9 2>/dev/null || true
  pgrep -f "pdsh -R ssh .*run_datanode.sh" | xargs -r kill -9 2>/dev/null || true
  rm -f "$LOCK_FILE"
fi

# Prevent accidental concurrent runs, which can look like a hang.
if command -v flock >/dev/null 2>&1; then
  exec 9>"$LOCK_FILE"
  if ! flock -n 9; then
    echo "Another start_datanode.sh is already running (lock: $LOCK_FILE)."
    echo "Wait for it to finish or terminate old pdsh/start_datanode first."
    echo "Tip:"
    echo "  DATANODE_FORCE=1 bash start_datanode.sh"
    echo "or:"
    echo "  bash stop_start_datanode.sh && bash start_datanode.sh"
    exit 1
  fi
fi

remaining_file="$(mktemp)"
trap 'rm -f "$remaining_file"' EXIT
sort -u "$HOSTS_FILE" > "$remaining_file"

if [ "$DATANODE_PRECHECK" = "1" ]; then
  echo "Prechecking SSH key auth/reachability (parallel=$DATANODE_PARALLEL, timeout=${DATANODE_PRECHECK_TIMEOUT}s)..."
  precheck_ok="$(mktemp)"
  precheck_fail="$(mktemp)"
  pids=()

  while read -r host; do
    [ -n "$host" ] || continue
    (
      # ssh reads stdin by default; in this while-read loop that can consume
      # remaining host lines and make precheck stop early. Force no-stdin.
      if command -v timeout >/dev/null 2>&1; then
        timeout "${DATANODE_PRECHECK_TIMEOUT}s" ssh -n $SSH_OPTS -l "$USER" "$host" "true" </dev/null >/dev/null 2>&1
      else
        ssh -n $SSH_OPTS -l "$USER" "$host" "true" </dev/null >/dev/null 2>&1
      fi
      if [ "$?" -eq 0 ]; then
        echo "$host" >> "$precheck_ok"
      else
        echo "$host" >> "$precheck_fail"
      fi
    ) &
    pids+=("$!")
    if [ "${#pids[@]}" -ge "$DATANODE_PARALLEL" ]; then
      wait -n
    fi
  done < "$remaining_file"
  wait

  sort -u "$precheck_ok" > "$remaining_file"
  ok_count="$(wc -l < "$remaining_file")"
  fail_count="$(wc -l < "$precheck_fail")"
  echo "SSH precheck result: ok=$ok_count, fail=$fail_count"
  if [ "$fail_count" -gt 0 ]; then
    echo "[WARN] These hosts failed SSH precheck (likely key/auth/network)."
    cat "$precheck_fail"
  fi
  rm -f "$precheck_ok" "$precheck_fail"

  if [ "$ok_count" -eq 0 ]; then
    echo "[ERROR] No hosts passed SSH precheck; aborting."
    exit 1
  fi
fi

if [ "$DATANODE_ATTACH" = "1" ]; then
  total="$(wc -l < "$remaining_file")"
  echo "[WARN] attach mode keeps SSH sessions open; with -f=$DATANODE_PARALLEL it will only hold up to $DATANODE_PARALLEL hosts."
  echo "[WARN] Use DATANODE_ATTACH=0 for full-cluster startup. Attach mode is for live-debug on a small host subset."
  echo "[Attach mode] starting on $total hosts and streaming remote run_datanode output."
  echo "[Attach mode] script will not return by itself; press Ctrl+C to stop streaming."
  set +e
  PDSH_SSH_ARGS_APPEND="$SSH_OPTS" sudo pdsh -R ssh -t "$DATANODE_PDSH_TIMEOUT" -w ^"$remaining_file" -l "$USER" -f "$DATANODE_PARALLEL" "$REMOTE_COMMAND_ATTACH"
  rc=$?
  set -e
  if [ "$rc" -ne 0 ]; then
    echo "[ERROR] attach-mode pdsh exited with status $rc."
    exit "$rc"
  fi
  exit 0
fi

attempt=1
while true; do
  total="$(wc -l < "$remaining_file")"
  if [ "$total" -eq 0 ]; then
    echo "All datanode hosts completed."
    exit 0
  fi

  echo "[Attempt $attempt] starting on $total hosts..."
  attempt_log="$(mktemp)"
  (
    while true; do
      sleep 5
      # This is local heartbeat while pdsh fans out; not datanode "running" status.
      echo "[Attempt $attempt] pdsh still in progress (waiting for all $total SSH sessions)... $(date '+%H:%M:%S')"
    done
  ) &
  hb_pid=$!

  set +e
  PDSH_SSH_ARGS_APPEND="$SSH_OPTS" sudo pdsh -R ssh -t "$DATANODE_PDSH_TIMEOUT" -w ^"$remaining_file" -l "$USER" -f "$DATANODE_PARALLEL" "$REMOTE_COMMAND_DETACH" 2>&1 | tee "$attempt_log"
  rc=${PIPESTATUS[0]}
  set -e
  kill "$hb_pid" 2>/dev/null || true
  wait "$hb_pid" 2>/dev/null || true

  # Collect hosts that failed with ssh transport errors.
  next_remaining="$(mktemp)"
  ssh_fail_file="$(mktemp)"
  listening_ok_file="$(mktemp)"
  listening_fail_file="$(mktemp)"
  expected_hosts_file="$(mktemp)"
  listening_missing_file="$(mktemp)"
  combined_fail_file="$(mktemp)"
  awk '
    /pdsh@/ && /ssh exited with exit code/ {
      host=$2;
      sub(/:$/, "", host);
      print host;
    }
  ' "$attempt_log" | sort -u > "$ssh_fail_file"

  # Parse explicit listening outcomes emitted by remote start checks.
  awk '
    /__DATANODE_LISTENING__/ {
      host=$1;
      sub(/:$/, "", host);
      print host;
    }
  ' "$attempt_log" | sort -u > "$listening_ok_file"

  awk '
    /__DATANODE_LISTENING_FAIL__/ || /__DATANODE_LISTENING_SKIP__/ {
      host=$1;
      sub(/:$/, "", host);
      print host;
    }
  ' "$attempt_log" | sort -u > "$listening_fail_file"

  sort -u "$remaining_file" > "$expected_hosts_file"
  if [ "$DATANODE_REQUIRE_LISTENING" = "1" ]; then
    # Any host without a positive LISTENING marker is retried.
    comm -23 "$expected_hosts_file" "$listening_ok_file" > "$listening_missing_file"
  else
    : > "$listening_missing_file"
  fi

  cat "$ssh_fail_file" "$listening_fail_file" "$listening_missing_file" | sort -u > "$combined_fail_file"
  mv "$combined_fail_file" "$next_remaining"
  rm -f "$attempt_log" "$ssh_fail_file" "$listening_ok_file" "$listening_fail_file" "$expected_hosts_file" "$listening_missing_file"

  # If pdsh died (OOM SIGKILL=137, etc.) the log may have no per-host lines — do NOT treat as success.
  if [ "$rc" -ne 0 ]; then
    echo "[ERROR] pdsh exited with status $rc (137 often means OOM killer). This is NOT a successful full run."
    if [ ! -s "$next_remaining" ]; then
      echo "[ERROR] No per-host failures parsed; assuming all $total hosts need retry."
      sort -u "$remaining_file" > "$next_remaining"
    fi
  fi

  ok_count=$(( total - $(wc -l < "$next_remaining") ))
  retry_count="$(wc -l < "$next_remaining")"
  echo "[Attempt $attempt] result: ok=$ok_count retry=$retry_count"

  if [ ! -s "$next_remaining" ]; then
    echo "Command executed successfully on all datanode nodes."
    rm -f "$next_remaining"
    exit 0
  fi

  if [ "$attempt" -ge "$DATANODE_RETRY" ]; then
    echo "Failed hosts after $attempt attempts:"
    cat "$next_remaining"
    rm -f "$next_remaining"
    exit 1
  fi

  mv "$next_remaining" "$remaining_file"
  attempt=$((attempt + 1))
  echo "Retrying failed hosts only in 2s..."
  sleep 2
done
