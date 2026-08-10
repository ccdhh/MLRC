#!/bin/bash
# Apply on all proxy nodes: intra-rack 10 Gb/s, inter-rack 1 Gb/s.
# Prefer running this just before merge; avoid during placement to keep writes fast.

HOSTS_FILE="proxy_hosts"
USER="root"
REMOTE_COMMAND="cd /users/qiliang/UniLRC && sh limit_intra10Gb_inter1Gb.sh"
PARALLEL=5

echo "Running bandwidth limit on all nodes (intra 10Gb, inter 1Gb)..."
sudo pdsh -R ssh -w ^$HOSTS_FILE -l $USER -f $PARALLEL "$REMOTE_COMMAND"

if [ $? -eq 0 ]; then
	echo "Command executed successfully on all nodes."
else
	echo "Failed to execute command on some nodes."
fi
