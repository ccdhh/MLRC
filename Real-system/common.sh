#!/usr/bin/env bash

set -euo pipefail

REAL_SYSTEM_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT="$(cd "$REAL_SYSTEM_DIR/.." && pwd)"
HOSTS_FILE="${DDRT_HOSTS_FILE:-$HOME/optimallrc/conf/Real-system_hosts}"
SSH_USER="${DDRT_SSH_USER:-$USER}"
SSH_KEY="${DDRT_SSH_KEY:-$HOME/.ssh/id_rsa}"
REMOTE_ROOT="${DDRT_REMOTE_ROOT:-~/MLRC}"
DATANODE_PORT="${DDRT_DATANODE_PORT:-17600}"
PROXY_PORT="${DDRT_PROXY_PORT:-50405}"
COORDINATOR_PORT="${DDRT_COORDINATOR_PORT:-55555}"
STORAGE_ROOT="${DDRT_STORAGE_ROOT:-/tmp/ddlrt_storage}"

load_hosts() {
  [[ -f "$HOSTS_FILE" ]] || {
    echo "hosts file not found: $HOSTS_FILE" >&2
    exit 1
  }
  mapfile -t ALL_HOSTS < <(awk '!/^[[:space:]]*($|#)/ {print $1}' "$HOSTS_FILE")
  ((${#ALL_HOSTS[@]} >= 3)) || {
    echo "hosts file must contain coordinator, client, and storage hosts" >&2
    exit 1
  }
  COORDINATOR_HOST="${ALL_HOSTS[0]}"
  CLIENT_HOST="${ALL_HOSTS[1]}"
  STORAGE_HOSTS=("${ALL_HOSTS[@]:2}")
}

remote() {
  local host="$1"
  shift
  # Do not let a remotely backgrounded service retain this script's stdin.
  # This is required for start.sh to return after launching a daemon over SSH.
  local ssh_args=(-n -o BatchMode=yes -o ConnectTimeout=10 -o StrictHostKeyChecking=accept-new)
  if [[ -f "$SSH_KEY" ]]; then
    ssh_args+=(-i "$SSH_KEY" -o IdentitiesOnly=yes)
  fi
  ssh "${ssh_args[@]}" "${SSH_USER}@${host}" "$@"
}

usage_hosts() {
  local storage_count=0
  if [[ -v STORAGE_HOSTS ]]; then
    storage_count="${#STORAGE_HOSTS[@]}"
  fi
  cat <<EOF
Hosts: $HOSTS_FILE
  coordinator: ${COORDINATOR_HOST:-not loaded}
  client:      ${CLIENT_HOST:-not loaded}
  storage:     ${storage_count} hosts
EOF
}
