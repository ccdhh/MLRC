#!/bin/bash
set -e

pkill -9 run_datanode || true

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
  echo "WARN: No 10.10.1.x found; server still binds 0.0.0.0. Put the correct routable IP in clusterInformation.xml."
else
  echo "Datanode reachable IP (sanity check vs cluster XML): $LOCAL_IP"
fi

# Listen on all interfaces so peers can reach this host using the IP in clusterInformation.xml
# even if this machine has multiple addresses or `ip` order differs from XML.
echo "Datanode gRPC/TCP bind: 0.0.0.0:17600 (+ bulk port +50)"
./project/cmake/build/run_datanode "0.0.0.0:17600" &
