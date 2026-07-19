#include "glrc_hybrid.h"
#include "glrc_repair_ilp.h"
#include "glrc_shard_plan.h"
#include "link_bandwidth.h"
#include <algorithm>
#include <cmath>
#include <cctype>

namespace ECProject
{
namespace
{

/** -2 = auto; -1 = parse error; >=0 fixed p including 0 */
int parse_fixed_p(const std::string &cfg, int global_shard_count, std::string &error)
{
  if (cfg.empty())
  {
    error = "empty GlrcHybridP";
    return -1;
  }
  if (cfg == "auto" || cfg == "AUTO")
    return -2;
  try
  {
    size_t idx = 0;
    const int p = std::stoi(cfg, &idx);
    if (idx != cfg.size())
    {
      error = "invalid GlrcHybridP (trailing chars): " + cfg;
      return -1;
    }
    if (p < 0 || p >= global_shard_count)
    {
      error = "GlrcHybridP out of range [0, S-1]: p=" + std::to_string(p) + " S=" + std::to_string(global_shard_count);
      return -1;
    }
    return p;
  }
  catch (...)
  {
    error = "invalid GlrcHybridP integer: " + cfg;
    return -1;
  }
}

int max_partition_shard_count(int p, int f)
{
  if (f < 1 || p < 1)
    return 0;
  const int base = p / f;
  const int rem = p % f;
  return base + (rem > 0 ? 1 : 0);
}

int max_partition_failed_block_id(int p, int f, const std::vector<int> &failed_block_ids)
{
  if (failed_block_ids.empty() || p < 1)
    return -1;
  if (f <= 1)
    return failed_block_ids[0];
  const int base = p / f;
  const int rem = p % f;
  if (rem <= 0)
    return failed_block_ids[0];
  return failed_block_ids[std::min(rem - 1, (int)failed_block_ids.size() - 1)];
}

bool hub_in_helper_set(int hub_block_id, const GlrcIlpRepairPlan &plan_ilp)
{
  for (int hid : plan_ilp.helper_block_ids)
    if (hid == hub_block_id)
      return true;
  return false;
}

void estimate_full_pipeline_time(int f, int global_shard_count, int stripe_byte_len, int block_size,
                                 double link_mbps, int pipeline_local_direct_count, int hub_block_id,
                                 const GlrcIlpRepairPlan *plan_ilp, double &t_pipeline_out)
{
  t_pipeline_out = 0.0;
  if (link_mbps <= 0.0 || global_shard_count < 1 || stripe_byte_len < 1)
    return;

  const int m = pipeline_local_direct_count;
  const int f_hub = std::max(0, f - m);
  const int lower = global_shard_count;

  if (m == f)
  {
    // All local-direct: each failed block repairs full stripe locally; bottleneck ≈ one full block transfer.
    t_pipeline_out = node_block_transfer_seconds(static_cast<size_t>(block_size), link_mbps);
    return;
  }

  if (f_hub > 0 && lower > 0)
  {
    const size_t chain_bytes =
        static_cast<size_t>(f_hub) * static_cast<size_t>(lower) * static_cast<size_t>(stripe_byte_len);
    t_pipeline_out += node_block_transfer_seconds(chain_bytes, link_mbps);
  }
  if (plan_ilp != nullptr && hub_in_helper_set(hub_block_id, *plan_ilp))
  {
    (void)hub_block_id;
    // p=0: no upper Phase2 hub-helper read; hub only serves pipeline half.
  }
  if (m > 0)
  {
    const size_t local_bytes = static_cast<size_t>(lower) * static_cast<size_t>(stripe_byte_len);
    t_pipeline_out = std::max(t_pipeline_out, node_block_transfer_seconds(local_bytes, link_mbps));
  }
}

void estimate_hybrid_times(int p, int f, int global_shard_count, int stripe_byte_len, int block_size,
                           double link_mbps, const GlrcIlpRepairPlan &plan_ilp, int hub_block_id,
                           int pipeline_local_direct_count,
                           const std::unordered_set<int> &local_direct_failed, int hot_block_id,
                           double &t_phase2_out, double &t_pipeline_out)
{
  t_phase2_out = 0.0;
  t_pipeline_out = 0.0;
  if (f < 1 || stripe_byte_len < 1 || global_shard_count < 1)
    return;

  if (p <= 0)
  {
    estimate_full_pipeline_time(f, global_shard_count, stripe_byte_len, block_size, link_mbps,
                                pipeline_local_direct_count, hub_block_id, &plan_ilp, t_pipeline_out);
    return;
  }
  if (p >= global_shard_count)
    return;

  const int p_max = max_partition_shard_count(p, f);
  const int h1 = std::max(1, plan_ilp.helper_block_count);
  const int lower = global_shard_count - p;
  const int f_hub = std::max(0, f - pipeline_local_direct_count);

  if (link_mbps > 0.0)
  {
    t_phase2_out = glrc_phase2_est_block_network_sec(h1, f, p_max, stripe_byte_len, link_mbps);
    const bool hot_receives_pipeline =
        hot_block_id >= 0 && local_direct_failed.count(hot_block_id) == 0 && lower > 0;
    if (hot_receives_pipeline)
    {
      // The hub writes the Pipeline tail directly to this failed datanode,
      // which still consumes the same physical node ingress as the Phase2
      // helper and exchange traffic for the hot failure partition.
      const size_t write_bytes = static_cast<size_t>(lower) * static_cast<size_t>(stripe_byte_len);
      t_phase2_out += node_block_transfer_seconds(write_bytes, link_mbps);
    }

    double t_hub = 0.0;
    if (hub_in_helper_set(hub_block_id, plan_ilp))
    {
      const size_t hub_helper_bytes = static_cast<size_t>(p) * static_cast<size_t>(stripe_byte_len);
      t_hub += node_block_transfer_seconds(hub_helper_bytes, link_mbps);
    }
    if (f_hub > 0 && lower > 0)
    {
      const size_t chain_bytes =
          static_cast<size_t>(f_hub) * static_cast<size_t>(lower) * static_cast<size_t>(stripe_byte_len);
      t_hub += node_block_transfer_seconds(chain_bytes, link_mbps);
    }
    t_pipeline_out = t_hub;
  }
}

double solve_continuous_p(int f, int global_shard_count, int stripe_byte_len, double link_mbps,
                          const GlrcIlpRepairPlan &plan_ilp, int hub_block_id,
                          const std::unordered_set<int> &local_direct_failed, int hot_block_id,
                          int pipeline_local_direct_count)
{
  if (link_mbps <= 0.0 || f < 1 || global_shard_count < 2 || stripe_byte_len < 1)
    return static_cast<double>(global_shard_count) / 2.0;

  const int f_hub = std::max(0, f - pipeline_local_direct_count);
  const double sigma = static_cast<double>(stripe_byte_len);
  const double inv_b = sigma / link_mbps;

  const int h1 = std::max(1, plan_ilp.helper_block_count);
  const double a2 = static_cast<double>(h1 + f - 1) * inv_b / static_cast<double>(f);
  const bool hot_receives_pipeline =
      hot_block_id >= 0 && local_direct_failed.count(hot_block_id) == 0;
  const double hot_pipe_slope = hot_receives_pipeline ? inv_b : 0.0;

  const double hub_helper_slope = hub_in_helper_set(hub_block_id, plan_ilp) ? inv_b : 0.0;
  const double hub_chain_slope = f_hub > 0 ? static_cast<double>(f_hub) * inv_b : 0.0;

  // T_hot(p) ≈ a2*p + hot_pipe_slope*(S-p): the Pipeline tail writes into
  // the hot failed datanode and shares that physical node's ingress with
  // Phase2 helper and exchange traffic.
  // T_hub(p) = hub_helper_slope*p + hub_chain_slope*(S-p).
  // Expand to slope*p + intercept for p* = (hub_intercept - hot_intercept) / (hot_slope - hub_slope).
  const double hot_slope = a2 - hot_pipe_slope;
  const double hub_slope = hub_helper_slope - hub_chain_slope;
  const double hot_intercept = hot_pipe_slope * static_cast<double>(global_shard_count);
  const double hub_intercept = hub_chain_slope * static_cast<double>(global_shard_count);

  const double denom = hot_slope - hub_slope;
  if (std::fabs(denom) < 1e-12)
    return static_cast<double>(global_shard_count) / 2.0;

  const double p_star = (hub_intercept - hot_intercept) / denom;
  return std::max(0.0, std::min(static_cast<double>(global_shard_count - 1), p_star));
}

/** Among floor(p*) and ceil(p*), pick p minimizing max(T_phase2_est, T_pipeline_est). */
int discretize_p(double p_continuous, int global_shard_count, int f, int block_size, int stripe_byte_len,
                 double link_mbps, const GlrcIlpRepairPlan &plan_ilp, int hub_block_id,
                 int pipeline_local_direct_count, const std::unordered_set<int> &local_direct_failed,
                 const std::vector<int> &failed_block_ids, double &t_phase2_out, double &t_pipeline_out)
{
  const int hi = global_shard_count - 1;
  int p_floor = static_cast<int>(std::floor(p_continuous));
  int p_ceil = static_cast<int>(std::ceil(p_continuous));
  p_floor = std::max(1, std::min(hi, p_floor));
  p_ceil = std::max(1, std::min(hi, p_ceil));

  std::vector<int> candidates = {p_floor, p_ceil};
  std::sort(candidates.begin(), candidates.end());
  candidates.erase(std::unique(candidates.begin(), candidates.end()), candidates.end());

  double best_score = 1e300;
  int best_p = p_floor;
  double best_t2 = 0.0, best_tp = 0.0;
  for (int p : candidates)
  {
    const int hot = max_partition_failed_block_id(p, f, failed_block_ids);
    double t2 = 0.0, tp = 0.0;
    estimate_hybrid_times(p, f, global_shard_count, stripe_byte_len, block_size, link_mbps, plan_ilp, hub_block_id,
                          pipeline_local_direct_count, local_direct_failed, hot, t2, tp);
    const double s = std::max(t2, tp);
    if (s < best_score || (s == best_score && std::abs(p - p_continuous) < std::abs(best_p - p_continuous)))
    {
      best_score = s;
      best_p = p;
      best_t2 = t2;
      best_tp = tp;
    }
  }
  t_phase2_out = best_t2;
  t_pipeline_out = best_tp;
  return best_p;
}

} // namespace

GlrcHybridChooseResult glrc_hybrid_choose_p(int k, int r, int z, int f, int global_shard_count, int block_size,
                                            double link_mbps, const GlrcIlpRepairPlan *plan_ilp, int hub_block_id,
                                            int pipeline_local_direct_count,
                                            const std::unordered_set<int> &local_direct_failed_block_ids,
                                            const std::vector<int> &failed_block_ids,
                                            const std::string &hybrid_p_config)
{
  GlrcHybridChooseResult out;
  if (f < 1 || global_shard_count < 1 || block_size < 1 || block_size % global_shard_count != 0)
  {
    out.error_message = "invalid hybrid choose_p dimensions";
    return out;
  }

  const int stripe_byte_len = block_size / global_shard_count;
  std::string parse_err;
  const int fixed_p = parse_fixed_p(hybrid_p_config, global_shard_count, parse_err);

  if (fixed_p >= 0)
  {
    out.p = fixed_p;
    out.p_continuous = static_cast<double>(fixed_p);
    if (fixed_p == 0)
    {
      estimate_full_pipeline_time(f, global_shard_count, stripe_byte_len, block_size, link_mbps,
                                  pipeline_local_direct_count, hub_block_id, plan_ilp, out.t_pipeline_est_sec);
      out.success = true;
      return out;
    }
    if (plan_ilp == nullptr || !plan_ilp->success)
    {
      out.error_message = "fixed p>0 requires successful ILP plan";
      return out;
    }
    out.max_partition_failed_block_id = max_partition_failed_block_id(out.p, f, failed_block_ids);
    estimate_hybrid_times(out.p, f, global_shard_count, stripe_byte_len, block_size, link_mbps, *plan_ilp, hub_block_id,
                          pipeline_local_direct_count, local_direct_failed_block_ids, out.max_partition_failed_block_id,
                          out.t_phase2_est_sec, out.t_pipeline_est_sec);
    out.success = true;
    return out;
  }

  if (fixed_p != -2)
  {
    out.error_message = parse_err.empty() ? "invalid GlrcHybridP" : parse_err;
    return out;
  }

  // Auto: at most one failure per placement group => p=0 (full-stripe local-first pipeline).
  if (glrc_failures_at_most_one_per_group(k, r, z, failed_block_ids))
  {
    out.p = 0;
    out.p_continuous = 0.0;
    estimate_full_pipeline_time(f, global_shard_count, stripe_byte_len, block_size, link_mbps,
                                pipeline_local_direct_count, hub_block_id, plan_ilp, out.t_pipeline_est_sec);
    out.success = true;
    return out;
  }

  if (plan_ilp == nullptr || !plan_ilp->success)
  {
    out.error_message = "auto choose_p requires successful ILP plan when p may exceed 0";
    return out;
  }

  const int hot_guess = max_partition_failed_block_id(global_shard_count / 2, f, failed_block_ids);
  out.p_continuous =
      solve_continuous_p(f, global_shard_count, stripe_byte_len, link_mbps, *plan_ilp, hub_block_id,
                         local_direct_failed_block_ids, hot_guess, pipeline_local_direct_count);
  out.p = discretize_p(out.p_continuous, global_shard_count, f, block_size, stripe_byte_len, link_mbps, *plan_ilp,
                       hub_block_id, pipeline_local_direct_count, local_direct_failed_block_ids, failed_block_ids,
                       out.t_phase2_est_sec, out.t_pipeline_est_sec);
  out.max_partition_failed_block_id = max_partition_failed_block_id(out.p, f, failed_block_ids);
  out.success = true;
  return out;
}

} // namespace ECProject
