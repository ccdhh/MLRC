#!/bin/bash
# Stop local gLRC processes (coordinator / proxy / datanode / client) and clear ./storage.

# Best-effort kill by process name first.
pkill -9 -f run_datanode 2>/dev/null || true
pkill -9 -f run_proxy 2>/dev/null || true
pkill -9 -f run_coordinator 2>/dev/null || true
pkill -9 -f main_client 2>/dev/null || true

# Then kill by listening ports (handles renamed binaries / wrapper commands).
for p in 17600 17650 50405 50406 55555; do
  if command -v fuser >/dev/null 2>&1; then
    fuser -k -n tcp "$p" 2>/dev/null || true
  elif command -v lsof >/dev/null 2>&1; then
    lsof -ti tcp:"$p" | xargs -r kill -9 2>/dev/null || true
  fi
done

rm -rf ./storage/*