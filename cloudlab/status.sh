#!/usr/bin/env bash

set -euo pipefail
source "$(cd "$(dirname "$0")" && pwd)/common.sh"
load_hosts

for host in "${ALL_HOSTS[@]}"; do
  printf '%-20s ' "$host"
  # Exact-name pgrep so the remote ssh/pgrep argv is not listed as a hit.
  remote "$host" \
    "pgrep -ax run_coordinator || true; pgrep -ax run_proxy || true; pgrep -ax run_datanode || true; pgrep -ax main_client || true" \
    | tr '\n' ';'
  echo
done
