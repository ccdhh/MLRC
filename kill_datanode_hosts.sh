#!/bin/bash
[ -n "${BASH_VERSION:-}" ] || exec bash "$0" "$@"
set -e

ROOT_DIR="/users/qiliang/UniLRC"
USER="root"
SSH_OPTS="-o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null"
PARALLEL=80
HOSTS_FILE="$ROOT_DIR/datanode_hosts"

if [ ! -f "$HOSTS_FILE" ]; then
  echo "Error: missing $HOSTS_FILE"
  exit 1
fi

echo "Carpet-killing datanodes on $(wc -l < "$HOSTS_FILE") hosts..."
set +e
PDSH_SSH_ARGS_APPEND="$SSH_OPTS" sudo pdsh -R ssh -w ^"$HOSTS_FILE" -l "$USER" -f "$PARALLEL" \
  "pkill -9 -f run_datanode 2>/dev/null || true; \
   for p in 17600 17650; do \
     if command -v fuser >/dev/null 2>&1; then fuser -k -n tcp \$p 2>/dev/null || true; \
     elif command -v lsof >/dev/null 2>&1; then lsof -ti tcp:\$p | xargs -r kill -9 2>/dev/null || true; fi; \
   done"
rc=$?
set -e

if [ "$rc" -ne 0 ]; then
  echo "Finished with partial failures (some datanode hosts unreachable)."
  exit 1
fi

echo "Done."
