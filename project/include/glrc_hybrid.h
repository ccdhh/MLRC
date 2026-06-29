#ifndef GLRC_HYBRID_H
#define GLRC_HYBRID_H

#include "glrc_repair_ilp.h"
#include <string>
#include <unordered_set>
#include <vector>

namespace ECProject
{

struct GlrcHybridChooseResult
{
  bool success = false;
  /** Split point: [0,p) Phase2+ILP, [p,S) Pipeline+local-first. p=0 => pipeline-only on full stripe. */
  int p = 0;
  double t_phase2_est_sec = 0.0;
  double t_pipeline_est_sec = 0.0;
  double p_continuous = 0.0;
  int max_partition_failed_block_id = -1;
  std::string error_message;
};

/**
 * Choose hybrid split point p in [0, S-1].
 * GlrcHybridP config: "auto" or integer in [0, S-1].
 *
 * p=0: all shards via local-first pipeline when each placement group has at most one failure
 *       (e.g. D0+D5+G1 or L0+L1+L2), or GlrcHybridP=0.
 * p in (0,S-1): hybrid split; T_hot_block(p) ≈ T_hub(p) for auto mode when p>0.
 */
GlrcHybridChooseResult glrc_hybrid_choose_p(int k, int r, int z, int f, int global_shard_count, int block_size,
                                            double link_mbps, const GlrcIlpRepairPlan *plan_ilp, int hub_block_id,
                                            int pipeline_local_direct_count,
                                            const std::unordered_set<int> &local_direct_failed_block_ids,
                                            const std::vector<int> &failed_block_ids,
                                            const std::string &hybrid_p_config);

} // namespace ECProject

#endif
