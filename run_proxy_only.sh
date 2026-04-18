#!/bin/bash
set -e

pkill -9 run_proxy || true

LOCAL_IP=$(
  ip -4 -o addr show scope global 2>/dev/null | awk '
    {
      split($4, ip_and_mask, "/");
      if (ip_and_mask[1] ~ /^10\.10\.1\./) {
        print ip_and_mask[1];
        exit;
      }
    }
  '
)

# Fallback for environments where `ip` is unavailable.
if [ -z "$LOCAL_IP" ]; then
  LOCAL_IP=$(hostname -I 2>/dev/null | tr " " "\n" | awk '/^10\.10\.1\./ { print; exit }')
fi

if [ -z "$LOCAL_IP" ]; then
  echo "No 10.10.1.x address found for proxy node"
  exit 1
fi

echo "Proxy bind IP: $LOCAL_IP"
./project/cmake/build/run_proxy "${LOCAL_IP}:50405" &
