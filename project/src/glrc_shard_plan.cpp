#include "glrc_shard_plan.h"

namespace ECProject
{

bool GlrcPhase2ShardPlan::build(int active_shard_count, int block_size,
                                const std::vector<int> &failed_block_ids,
                                const std::vector<int> &failed_cluster_ids,
                                const std::vector<std::string> &failed_proxy_ips,
                                const std::vector<int> &failed_proxy_ports, int global_shard_count,
                                GlrcPhase2ShardPlan &out, std::string &error_message)
{
  out = GlrcPhase2ShardPlan();
  error_message.clear();
  const int f = (int)failed_block_ids.size();
  if (f < 1)
  {
    error_message = "empty failure set";
    return false;
  }
  const int global_S = global_shard_count > 0 ? global_shard_count : active_shard_count;
  if (active_shard_count < 1 || global_S < 1 || block_size < 1)
  {
    error_message = "invalid shard_count or block_size";
    return false;
  }
  if (block_size % global_S != 0)
  {
    error_message = "BlockSize must be divisible by global shard count";
    return false;
  }
  if (f > active_shard_count)
  {
    error_message = "failed block count f exceeds active shard count";
    return false;
  }
  if ((int)failed_cluster_ids.size() != f || (int)failed_proxy_ips.size() != f ||
      (int)failed_proxy_ports.size() != f)
  {
    error_message = "partition metadata size mismatch";
    return false;
  }

  const int base = active_shard_count / f;
  const int rem = active_shard_count % f;
  out.shard_count = active_shard_count;
  out.block_size = block_size;
  out.stripe_byte_len = block_size / global_S;
  out.partitions.resize(f);

  int shard_cursor = 0;
  for (int i = 0; i < f; i++)
  {
    const int cnt = base + (i < rem ? 1 : 0);
    GlrcPartitionShardPlan &p = out.partitions[i];
    p.partition_id = i;
    p.failed_block_id = failed_block_ids[i];
    p.cluster_id = failed_cluster_ids[i];
    p.proxy_ip = failed_proxy_ips[i];
    p.proxy_port = failed_proxy_ports[i];
    p.shard_begin = shard_cursor;
    p.shard_count = cnt;
    p.byte_off = shard_cursor * out.stripe_byte_len;
    p.byte_len = cnt * out.stripe_byte_len;
    p.shards.clear();
    for (int s = 0; s < cnt; s++)
    {
      GlrcShardSlice slice;
      slice.shard_id = shard_cursor + s;
      slice.byte_off = slice.shard_id * out.stripe_byte_len;
      slice.byte_len = out.stripe_byte_len;
      p.shards.push_back(slice);
    }
    shard_cursor += cnt;
  }
  return true;
}

} // namespace ECProject
