#!/usr/bin/env bash

set -euo pipefail
source "$(cd "$(dirname "$0")" && pwd)/common.sh"
load_hosts

for host in "${ALL_HOSTS[@]}"; do
  printf '%-20s ' "$host"
  remote "$host" "pgrep -af 'run_coordinator|run_proxy|run_datanode|main_client' || true" \
    | tr '\n' ';'
  echo
done
