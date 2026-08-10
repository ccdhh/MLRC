#!/bin/bash
# Clear bandwidth limits on all proxy nodes (pair with limit_all_intra10Gb_inter1Gb.sh)

HOSTS_FILE="proxy_hosts"
USER="root"
REMOTE_COMMAND="cd /users/qiliang/UniLRC && sh unlimit_both_interfaces.sh"
PARALLEL=5

echo "Clearing bandwidth limits on all proxy nodes..."
sudo pdsh -R ssh -w ^$HOSTS_FILE -l $USER -f $PARALLEL "$REMOTE_COMMAND"

if [ $? -eq 0 ]; then
	echo "Done."
else
	echo "Some nodes may have failed."
fi
