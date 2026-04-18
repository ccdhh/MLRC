#!/bin/bash
[ -n "${BASH_VERSION:-}" ] || exec bash "$0" "$@"
set -e

SOURCE_DIR="/users/qiliang/UniLRC"
REMOTE_DIR="/users/qiliang/UniLRC"
SSH_OPTS="-o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null"

# How many rsync+ssh sessions run at once (default tuned for many racks / many hosts).
# Lower if the control node disk or uplink becomes saturated; raise if idle (e.g. 48, 64).
: "${UPDATE_ALL_PARALLEL:=32}"

# Rsync mode: on fast LAN, compression (-z) often hurts; whole-file avoids delta algorithm overhead.
# Override if needed, e.g.  UPDATE_ALL_RSYNC_FLAGS="-az"  for slow links.
: "${UPDATE_ALL_RSYNC_FLAGS:=-a --whole-file --no-compress}"

# Extra ssh tuning (optional): multiplex one connection per target for less TCP/SSH handshake cost.
# Example: export UPDATE_ALL_SSH_EXTRA="-o ControlMaster=auto -o ControlPath=~/.ssh/cm-%r@%h:%p -o ControlPersist=300"
: "${UPDATE_ALL_SSH_EXTRA:=}"

SSH_FULL_OPTS="$SSH_OPTS $UPDATE_ALL_SSH_EXTRA"

# Sync to all main nodes + all datanode hosts.
HOST_FILES=("hosts" "datanode_hosts")

for hf in "${HOST_FILES[@]}"; do
    if [ ! -f "$SOURCE_DIR/$hf" ]; then
        echo "Error: $hf not found at $SOURCE_DIR/$hf"
        exit 1
    fi
done

# Build a unique host list from both files.
mapfile -t HOST_LIST < <(
    awk 'NF > 0 {print $1}' "$SOURCE_DIR/hosts" "$SOURCE_DIR/datanode_hosts" \
    | sort -u
)

if [ "${#HOST_LIST[@]}" -eq 0 ]; then
    echo "Error: no hosts to sync."
    exit 1
fi

echo "Syncing UniLRC to ${#HOST_LIST[@]} hosts (parallel=${UPDATE_ALL_PARALLEL})..."
echo "rsync flags: ${UPDATE_ALL_RSYNC_FLAGS}"
any_failed=0
failtmp="$(mktemp)"
trap 'rm -f "$failtmp"' EXIT

sync_one() {
    local ip="$1"
    echo "[$ip] starting..."
    if sudo rsync ${UPDATE_ALL_RSYNC_FLAGS} \
      --exclude='project/cmake/build/CMakeFiles' \
      --exclude='project/cmake/build/run_client' \
      --exclude='project/cmake/build/main_test' \
      --exclude='project/cmake/build/main_client' \
      --exclude='storage/*' \
      -e "ssh $SSH_FULL_OPTS" \
      "$SOURCE_DIR/" "$ip:$REMOTE_DIR/"; then
        echo "[$ip] OK"
    else
        echo "[$ip] FAILED" >&2
        echo "$ip" >>"$failtmp"
    fi
}

export -f sync_one
export SOURCE_DIR REMOTE_DIR UPDATE_ALL_RSYNC_FLAGS SSH_FULL_OPTS failtmp

# Parallel xargs: one rsync per host, up to UPDATE_ALL_PARALLEL at a time.
printf '%s\n' "${HOST_LIST[@]}" | xargs -P "${UPDATE_ALL_PARALLEL}" -n1 bash -c 'sync_one "$1"' _ || true

if [ -s "$failtmp" ]; then
    echo "Sync finished with failures:"
    sort -u "$failtmp"
    any_failed=1
fi

if [ $any_failed -ne 0 ]; then
    echo "Please re-run after fixing SSH/rsync on failed hosts, or lower UPDATE_ALL_PARALLEL if overload."
    exit 1
fi

echo "All done!"
echo "Skip auto-generating run scripts/config to avoid overwriting clusterInformation.xml."
echo "If you explicitly need legacy generated scripts, run: bash generate_run_proxy.sh"
