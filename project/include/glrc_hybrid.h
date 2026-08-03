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
 * Auto p=0 shortcut (full-stripe local-first pipeline).
 * gLRC: true when each placement group has at most one failure (each group has a local parity).
 * AzureLRC: same, but never when any global parity G* failed — Azure's global bucket has no
 *           local parity, so a G failure needs the T_hot/T_hub cost model (may choose p>0).
 * OptimalLRC: always false. Optimal local equations include every G*, so concurrent local
 *             chains share those hops and the hotspot stays ≈f (not 1); forcing p=0 undercounts.
 */
bool glrc_hybrid_auto_forces_p0(int k, int r, int z, const std::vector<int> &failed_block_ids,
                                GlrcCodecMode codec = GlrcCodecMode::GLRC);

/**
 * Choose hybrid split point p in [0, S-1].
 * GlrcHybridP config: "auto" or integer in [0, S-1].
 *
 * Auto mode: solve continuous p* from T_hot(p) ≈ T_hub(p), where
 *   T_hot(p) ≈ a2*p + (σ/B)*(S-p) on the hottest failed node; every Pipeline
 *              tail enters it through either local-direct receive or hub writeback,
 *   T_hub(p)  = (σ/B)*p [hub helper read] + f_hub*(σ/B)*(S-p) [hub chains],
 * then discretize by comparing floor(p*) and ceil(p*) and picking the one with smaller
 * max(T_phase2_est, T_pipeline_est).
 * Global stripe [0,p) runs Phase2+ILP (split across failed blocks; p<f allowed — some blocks get 0 shards).
 * Global stripe [p,S) runs Pipeline+local-first on every failed block's repair proxy.
 *
 * p=0: GlrcHybridP=0, or auto + glrc_hybrid_auto_forces_p0(...).
 * Azure + failed G*: auto never forces p=0; p* modeling ignores local-direct so a lone
 * global hub chain cannot collapse to the f_hub=1 / p*=0 fixed point.
 * Optimal: auto never forces p=0; p* modeling always ignores local-direct (shared G* hops
 * keep f_hub≈f even when every selected equation is classified local-direct).
 */
GlrcHybridChooseResult glrc_hybrid_choose_p(int k, int r, int z, int f, int global_shard_count, int block_size,
                                            double link_mbps, const GlrcIlpRepairPlan *plan_ilp, int hub_block_id,
                                            int pipeline_local_direct_count,
                                            const std::unordered_set<int> &local_direct_failed_block_ids,
                                            const std::vector<int> &failed_block_ids,
                                            const std::string &hybrid_p_config,
                                            GlrcCodecMode codec = GlrcCodecMode::GLRC);

} // namespace ECProject

#endif
