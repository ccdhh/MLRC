#!/bin/bash

HOSTS_FILE="proxy_hosts"

USER="root"

REMOTE_COMMAND="cd /users/qiliang/UniLRC && bash run_proxy_only.sh"
SSH_OPTS="-o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null"

PARALLEL=50

echo "Running command on all proxy nodes..."
PDSH_SSH_ARGS_APPEND="$SSH_OPTS" sudo pdsh -R ssh -w ^$HOSTS_FILE -l $USER -f $PARALLEL "$REMOTE_COMMAND"

if [ $? -eq 0 ]; then
  echo "Command executed successfully on all proxy nodes."
else
  echo "Failed to execute command on some proxy nodes."
fi
