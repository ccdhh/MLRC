#!/bin/bash
[ -n "${BASH_VERSION:-}" ] || exec bash "$0" "$@"
set -e

echo "Stopping lingering datanode launcher jobs..."
# Avoid killing this script itself.
pgrep -f "start_datanode.sh" | awk -v self="$$" '$1 != self {print $1}' | xargs -r kill -9 2>/dev/null || true
pgrep -f "pdsh -R ssh .*run_datanode.sh" | xargs -r kill -9 2>/dev/null || true
rm -f /tmp/start_datanode.lock
echo "Done. You can now run: bash start_datanode.sh"
