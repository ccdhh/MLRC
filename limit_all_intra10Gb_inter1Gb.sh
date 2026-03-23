#!/bin/bash
# 在所有 proxy 节点上设置：机架内 10Gb/s，机架间 1Gb/s
# 建议在「合并前」再执行；放置阶段勿执行，避免拖慢写入。

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
