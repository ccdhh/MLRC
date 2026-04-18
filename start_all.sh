#!/bin/bash
set -e

# One-click startup order for physical cluster:
# 1) datanodes (17600/17650)
# 2) proxies   (50405/50406)
# 3) coordinator (55555 by config)

ROOT_DIR="/users/qiliang/UniLRC"

echo "[start_all] starting datanodes..."
bash "$ROOT_DIR/start_datanode.sh"

echo "[start_all] wait 3s for datanodes to listen..."
sleep 3

echo "[start_all] starting proxies..."
bash "$ROOT_DIR/start_proxy.sh"

echo "[start_all] wait 2s for proxies to listen..."
sleep 2

echo "[start_all] starting coordinator..."
bash "$ROOT_DIR/start_coordinator.sh"

echo "[start_all] done."
echo "[start_all] tip: run ./check_cluster_connectivity.sh from 10.10.1.2 to verify ports."
