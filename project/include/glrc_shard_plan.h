#ifndef GLRC_SHARD_PLAN_H
#define GLRC_SHARD_PLAN_H

#include <string>
#include <vector>

namespace ECProject
{

struct GlrcShardSlice
{
  int shard_id = 0;
  int byte_off = 0;
  int byte_len = 0;
};

/** One failed-block partition: owns a contiguous range of global shard indices [0, shard_count). */
/** Phase2: each failed block owns shard_count stripes; repair compute is block-centric (per-block BW). */
struct GlrcPartitionShardPlan
{
  int partition_id = 0;
  int failed_block_id = 0;
  /** Deployment index (which proxy/datanode pool); repair ingress cap is per proxy process. */
  int cluster_id = 0;
  std::string proxy_ip;
  int proxy_port = 0;
  int shard_begin = 0;
  int shard_count = 0;
  int byte_off = 0;
  int byte_len = 0;
  std::vector<GlrcShardSlice> shards;
};

struct GlrcPhase2ShardPlan
{
  int shard_count = 16;
  int stripe_byte_len = 0;
  int block_size = 0;
  std::vector<GlrcPartitionShardPlan> partitions;

  /** Assign active_shard_count stripes across f partitions; stripe_byte_len = block_size / global_shard_count.
   *  active_shard_count may be < f: first (active_shard_count % f) partitions get one extra shard; others may get 0. */
  static bool build(int active_shard_count, int block_size, const std::vector<int> &failed_block_ids,
                    const std::vector<int> &failed_cluster_ids,
                    const std::vector<std::string> &failed_proxy_ips,
                    const std::vector<int> &failed_proxy_ports, int global_shard_count,
                    GlrcPhase2ShardPlan &out, std::string &error_message);
};

} // namespace ECProject

#endif