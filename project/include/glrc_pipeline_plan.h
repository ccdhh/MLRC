#ifndef GLRC_PIPELINE_PLAN_H
#define GLRC_PIPELINE_PLAN_H

#include "glrc_repair_ilp.h"
#include <string>
#include <vector>

namespace ECProject
{
  enum class GlrcPipelineRole
  {
    HUB = 0,
    CHAIN_HEAD = 1,
    HOP_SERVER = 2,
    LOCAL_DIRECT = 3,
    /** Release listen-port tracking after a recovery; no data transfer. */
    TEARDOWN = 4,
    /** Bind pipeline listen ports only; coordinator waits before starting data plane. */
    READY = 5
  };

  struct GlrcPipelineHopInfo
  {
    int block_id = -1;
    int node_id = -1;
    std::string datanode_ip;
    int datanode_port = 0;
    std::string proxy_ip;
    int proxy_port = 0;
    std::string block_key;
    unsigned char coef = 0;
  };

  struct GlrcPipelineChainPlan
  {
    int chain_id = -1;
    int equation_index = -1;
    int eq_slot = -1;
    std::string equation_name;
    bool local_direct = false;
    int local_direct_failed_block_id = -1;
    std::string local_direct_failed_block_key;
    std::string local_direct_replaced_ip;
    int local_direct_replaced_port = 0;
    /** Ordered hops (head -> tail). Tail may be hub when hub participates in the equation. */
    std::vector<GlrcPipelineHopInfo> hops;
    bool hub_is_chain_tail = false;
    bool hub_in_equation = false;
  };

  struct GlrcPipelinePlan
  {
    int hub_block_id = -1;
    std::string hub_proxy_ip;
    int hub_proxy_port = 0;
    std::string hub_datanode_ip;
    int hub_datanode_port = 0;
    std::string hub_block_key;
    unsigned char hub_coef = 0;
    int exchange_epoch = 0;
    std::vector<GlrcPipelineChainPlan> hub_chains;
    std::vector<GlrcPipelineChainPlan> local_direct_chains;
  };

  struct GlrcPipelineNodeLookup
  {
    int node_id = -1;
    std::string datanode_ip;
    int datanode_port = 0;
    std::string proxy_ip;
    int proxy_port = 0;
    std::string block_key;
  };

  /**
   * Build per-equation pipeline chains for hub-path and local-direct repairs.
   * node_lookup[block_id] must be populated for every block referenced in hops.
   */
  bool glrc_build_pipeline_plan(int k, int r, int z, const std::vector<int> &failed_block_ids,
                                const GlrcIlpRepairPlan &repair_plan,
                                const std::vector<GlrcEquation> &all_equations, int hub_block_id,
                                const std::vector<GlrcPipelineNodeLookup> &node_lookup, int exchange_epoch,
                                GlrcPipelinePlan &out_plan, std::string &error_message);

} // namespace ECProject

#endif
