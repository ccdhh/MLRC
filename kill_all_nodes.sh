#!/bin/bash
[ -n "${BASH_VERSION:-}" ] || exec bash "$0" "$@"
set -e

ROOT_DIR="/users/qiliang/UniLRC"
USER="root"
REMOTE_COMMAND="cd $ROOT_DIR && bash kill_all.sh"
SSH_OPTS="-o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null"
PARALLEL=80

# Kill on both infra nodes and all datanode hosts.
for hf in hosts datanode_hosts; do
  if [ ! -f "$ROOT_DIR/$hf" ]; then
    echo "Error: missing host file $ROOT_DIR/$hf"
    exit 1
  fi
done

TMP_HOSTS="$(mktemp)"
trap 'rm -f "$TMP_HOSTS"' EXIT
awk 'NF > 0 {print $1}' "$ROOT_DIR/hosts" "$ROOT_DIR/datanode_hosts" | sort -u > "$TMP_HOSTS"

echo "Pass 1/3: kill_all.sh on $(wc -l < "$TMP_HOSTS") nodes (hosts + datanode_hosts)..."
set +e
PDSH_SSH_ARGS_APPEND="$SSH_OPTS" sudo pdsh -R ssh -w ^"$TMP_HOSTS" -l "$USER" -f "$PARALLEL" "$REMOTE_COMMAND"
rc1=$?

echo "Pass 2/3: datanode_hosts carpet kill (process + ports)..."
PDSH_SSH_ARGS_APPEND="$SSH_OPTS" sudo pdsh -R ssh -w ^"$ROOT_DIR/datanode_hosts" -l "$USER" -f "$PARALLEL" \
  "pkill -9 -f run_datanode 2>/dev/null || true; \
   for p in 17600 17650; do \
     if command -v fuser >/dev/null 2>&1; then fuser -k -n tcp \$p 2>/dev/null || true; \
     elif command -v lsof >/dev/null 2>&1; then lsof -ti tcp:\$p | xargs -r kill -9 2>/dev/null || true; fi; \
   done"
rc2=$?

echo "Pass 3/3: local kill_all.sh..."
cd "$ROOT_DIR"
bash kill_all.sh
set -e

if [ "$rc1" -ne 0 ] || [ "$rc2" -ne 0 ]; then
  echo "Finished with partial failures (some nodes unreachable or command failed)."
  echo "Re-run to sweep stragglers, or check SSH/connectivity for unreachable nodes."
  exit 1
fi

echo "Done. All remote kill passes returned success."