#!/bin/bash
# 在所有 proxy 节点上：机架内 10Gb/s + 指定机架间带宽（Gb/s）
# 用法: sh limit_all_intra10Gb_inter.sh <0.5|1|2|5|10>
#
# 五次实验示例（每次合并前执行一条，合并后 unlimit_all_proxy.sh）:
#   sh limit_all_intra10Gb_inter.sh 0.5
#   sh limit_all_intra10Gb_inter.sh 1
#   sh limit_all_intra10Gb_inter.sh 2
#   sh limit_all_intra10Gb_inter.sh 5
#   sh limit_all_intra10Gb_inter.sh 10

INTER_GB="${1:-}"
if [ -z "$INTER_GB" ]; then
  echo "Usage: $0 <0.5|1|2|5|10>" >&2
  exit 1
fi

HOSTS_FILE="proxy_hosts"
USER="root"
REMOTE_COMMAND="cd /users/qiliang/UniLRC && sh test_test.sh $INTER_GB"
PARALLEL=5

echo "Applying intra 10 Gb/s + inter ${INTER_GB} Gb/s on all proxy nodes..."
sudo pdsh -R ssh -w ^$HOSTS_FILE -l $USER -f $PARALLEL "$REMOTE_COMMAND"

if [ $? -eq 0 ]; then
	echo "Done."
else
	echo "Some nodes may have failed."
fi