#include "glrc_pipeline_plan.h"
#include "glrc_pipeline_codec.h"
#include "unilrc_encoder.h"
#include <algorithm>
#include <unordered_set>

namespace ECProject
{
namespace
{
  bool fill_hop(int block_id, unsigned char coef, const std::vector<GlrcPipelineNodeLookup> &node_lookup,
                GlrcPipelineHopInfo &hop, std::string &error_message)
  {
    if (block_id < 0 || block_id >= (int)node_lookup.size())
    {
      error_message = "pipeline node lookup missing block " + std::to_string(block_id);
      return false;
    }
    const GlrcPipelineNodeLookup &node = node_lookup[block_id];
    if (node.proxy_ip.empty() || node.datanode_ip.empty())
    {
      error_message = "pipeline node lookup incomplete for block " + std::to_string(block_id);
      return false;
    }
    hop.block_id = block_id;
    hop.node_id = node.node_id;
    hop.datanode_ip = node.datanode_ip;
    hop.datanode_port = node.datanode_port;
    hop.proxy_ip = node.proxy_ip;
    hop.proxy_port = node.proxy_port;
    hop.block_key = node.block_key;
    hop.coef = coef;
    return true;
  }

  void sort_chain_survivors(std::vector<int> &survivors, int hub_block_id)
  {
    std::sort(survivors.begin(), survivors.end(), std::greater<int>());
    auto hub_it = std::find(survivors.begin(), survivors.end(), hub_block_id);
    if (hub_it != survivors.end())
    {
      const int hub = *hub_it;
      survivors.erase(hub_it);
      survivors.push_back(hub);
    }
  }
} // namespace

bool glrc_build_pipeline_plan(int k, int r, int z, const std::vector<int> &failed_block_ids,
                              const GlrcIlpRepairPlan &repair_plan,
                              const std::vector<GlrcEquation> &all_equations, int hub_block_id,
                              const std::vector<GlrcPipelineNodeLookup> &node_lookup, int exchange_epoch,
                              GlrcPipelinePlan &out_plan, std::string &error_message,
                              GlrcCodecMode codec)
{
  out_plan = GlrcPipelinePlan{};
  out_plan.hub_block_id = hub_block_id;
  out_plan.exchange_epoch = exchange_epoch;

  if (hub_block_id < 0 || hub_block_id >= (int)node_lookup.size())
  {
    error_message = "invalid hub block id";
    return false;
  }
  const GlrcPipelineNodeLookup &hub_node = node_lookup[hub_block_id];
  out_plan.hub_proxy_ip = hub_node.proxy_ip;
  out_plan.hub_proxy_port = hub_node.proxy_port;
  out_plan.hub_datanode_ip = hub_node.datanode_ip;
  out_plan.hub_datanode_port = hub_node.datanode_port;
  out_plan.hub_block_key = hub_node.block_key;

  const int n = k + r + z;
  std::unordered_set<int> failed_set(failed_block_ids.begin(), failed_block_ids.end());
  std::unordered_set<int> local_direct_set;
  for (int eq_idx : repair_plan.selected_equation_indices)
  {
    if (eq_idx < 0 || eq_idx >= (int)all_equations.size() || all_equations[eq_idx].type != GlrcEquationType::LOCAL)
      continue;
    int failed_in_eq = 0;
    for (int b : all_equations[eq_idx].involved_blocks)
    {
      if (failed_set.count(b))
        failed_in_eq++;
    }
    if (failed_in_eq == 1)
      local_direct_set.insert(eq_idx);
  }

  int chain_id = 0;
  int eq_slot = 0;
  for (int eq_idx : repair_plan.selected_equation_indices)
  {
    if (eq_idx < 0 || eq_idx >= (int)all_equations.size())
    {
      error_message = "invalid equation index in repair plan";
      return false;
    }

    const GlrcEquation &eq = all_equations[eq_idx];
    std::vector<unsigned char> coef_row;
    glrc_pipeline_build_coef_row(k, r, z, eq_idx, coef_row, codec);

    std::vector<int> survivors;
    for (int b : eq.involved_blocks)
    {
      if (!failed_set.count(b))
        survivors.push_back(b);
    }
    sort_chain_survivors(survivors, hub_block_id);

    const bool local_direct = local_direct_set.count(eq_idx) > 0;
    GlrcPipelineChainPlan chain;
    chain.chain_id = chain_id++;
    chain.equation_index = eq_idx;
    chain.eq_slot = eq_slot++;
    chain.equation_name = eq.name;
    chain.local_direct = local_direct;
    chain.hub_in_equation = coef_row[hub_block_id] != 0;

    if (local_direct)
    {
      // Figure-style single-chain pipeline: survivors N1→N2→…→Nk, sink R = failed
      // block's repair proxy.  Final XOR of survivors is the recovered block.
      for (int b : eq.involved_blocks)
      {
        if (failed_set.count(b))
        {
          chain.local_direct_failed_block_id = b;
          if (b >= 0 && b < (int)node_lookup.size())
          {
            chain.local_direct_failed_block_key = node_lookup[b].block_key;
            chain.local_direct_replaced_ip = node_lookup[b].datanode_ip;
            chain.local_direct_replaced_port = node_lookup[b].datanode_port;
            chain.local_direct_sink_proxy_ip = node_lookup[b].proxy_ip;
            chain.local_direct_sink_proxy_port = node_lookup[b].proxy_port;
          }
          break;
        }
      }
      if (chain.local_direct_sink_proxy_ip.empty() || survivors.empty())
      {
        error_message = "local pipeline missing sink proxy or survivors";
        return false;
      }
      // Stable chain order (high→low block id). Do not force global hub to the
      // end — the sink R is outside the survivor hop list.
      std::sort(survivors.begin(), survivors.end(), std::greater<int>());
      for (int b : survivors)
      {
        GlrcPipelineHopInfo hop;
        if (!fill_hop(b, coef_row[b], node_lookup, hop, error_message))
          return false;
        chain.hops.push_back(hop);
      }
      chain.hub_is_chain_tail = false;
      out_plan.local_direct_chains.push_back(chain);
      continue;
    }

    chain.hub_is_chain_tail = chain.hub_in_equation;
    for (int b : survivors)
    {
      if (chain.hub_is_chain_tail && b == hub_block_id)
        continue;
      GlrcPipelineHopInfo hop;
      if (!fill_hop(b, coef_row[b], node_lookup, hop, error_message))
        return false;
      chain.hops.push_back(hop);
    }
    if (chain.hub_is_chain_tail)
    {
      GlrcPipelineHopInfo hub_hop;
      if (!fill_hop(hub_block_id, coef_row[hub_block_id], node_lookup, hub_hop, error_message))
        return false;
      chain.hops.push_back(hub_hop);
    }
    out_plan.hub_chains.push_back(chain);
  }

  out_plan.hub_coef = 0;
  (void)n;
  return true;
}

} // namespace ECProject
