#include "glrc_repair_ilp.h"
#include "link_bandwidth.h"
#include "unilrc_encoder.h"

#include <algorithm>
#include <chrono>
#include <climits>
#include <cmath>
#include <iostream>
#include <unordered_map>
#include <unordered_set>

namespace ECProject
{
  void glrc_build_placement_groups(int k, int r, int z, std::vector<std::vector<int>> &groups)
  {
    groups.clear();
    groups.resize(z);
    int payload_block_id = 0;
    for (int g = 0; g < z; g++)
    {
      const int payload_count = glrc_payload_blocks_in_group(g, k, r, z);
      for (int j = 0; j < payload_count; j++)
        groups[g].push_back(payload_block_id++);
      groups[g].push_back(k + r + g);
    }
  }

  static void build_equation_from_group(int k, int r, int z, int group_id,
                                        const std::vector<std::vector<int>> &groups, GlrcEquation &eq)
  {
    eq.type = GlrcEquationType::LOCAL;
    eq.index = group_id;
    eq.name = "L" + std::to_string(group_id);
    eq.involved_blocks = groups[group_id];
  }

  static void build_global_equation(int k, int global_id, GlrcEquation &eq)
  {
    eq.type = GlrcEquationType::GLOBAL;
    eq.index = global_id;
    eq.name = "G" + std::to_string(global_id);
    eq.involved_blocks.clear();
    for (int i = 0; i < k; i++)
      eq.involved_blocks.push_back(i);
    eq.involved_blocks.push_back(k + global_id);
  }

  void glrc_build_recovery_equations(int k, int r, int z, const std::vector<int> &failed_block_ids,
                                     std::vector<GlrcEquation> &all_equations,
                                     std::vector<int> &candidate_equation_indices)
  {
    all_equations.clear();
    candidate_equation_indices.clear();
    std::vector<std::vector<int>> groups;
    glrc_build_placement_groups(k, r, z, groups);

    for (int i = 0; i < z; i++)
    {
      GlrcEquation eq;
      build_equation_from_group(k, r, z, i, groups, eq);
      all_equations.push_back(eq);
    }
    for (int j = 0; j < r; j++)
    {
      GlrcEquation eq;
      build_global_equation(k, j, eq);
      all_equations.push_back(eq);
    }

    std::unordered_set<int> failed_set(failed_block_ids.begin(), failed_block_ids.end());
    for (int idx = 0; idx < (int)all_equations.size(); idx++)
    {
      for (int b : all_equations[idx].involved_blocks)
      {
        if (failed_set.count(b))
        {
          candidate_equation_indices.push_back(idx);
          break;
        }
      }
    }
  }

  static std::vector<int> helper_blocks_for_equation(const GlrcEquation &eq,
                                                     const std::unordered_set<int> &failed_set)
  {
    std::vector<int> helpers;
    for (int b : eq.involved_blocks)
      if (!failed_set.count(b))
        helpers.push_back(b);
    return helpers;
  }

  static int union_helper_count(const std::vector<int> &selected_eq_indices,
                                const std::vector<GlrcEquation> &all_equations,
                                const std::unordered_set<int> &failed_set,
                                std::vector<int> &out_helpers)
  {
    std::unordered_set<int> helper_set;
    for (int idx : selected_eq_indices)
    {
      std::vector<int> h = helper_blocks_for_equation(all_equations[idx], failed_set);
      for (int b : h)
        helper_set.insert(b);
    }
    out_helpers.assign(helper_set.begin(), helper_set.end());
    std::sort(out_helpers.begin(), out_helpers.end());
    return (int)out_helpers.size();
  }

  static bool combination_covers(const std::vector<int> &combo, const std::vector<GlrcEquation> &all_equations,
                                 const std::unordered_set<int> &failed_set)
  {
    std::unordered_set<int> covered;
    for (int idx : combo)
    {
      for (int b : all_equations[idx].involved_blocks)
        if (failed_set.count(b))
          covered.insert(b);
    }
    return covered.size() == failed_set.size();
  }

  static void enumerate_combinations(int n, int f, int start, std::vector<int> &current,
                                     std::vector<std::vector<int>> &out)
  {
    if ((int)current.size() == f)
    {
      out.push_back(current);
      return;
    }
    for (int i = start; i <= n - (f - (int)current.size()); i++)
    {
      current.push_back(i);
      enumerate_combinations(n, f, i + 1, current, out);
      current.pop_back();
    }
  }

  bool glrc_solve_repair_ilp(int k, int r, int z, const std::vector<int> &failed_block_ids,
                             GlrcIlpRepairPlan &plan)
  {
    plan = GlrcIlpRepairPlan();
    plan.failed_block_ids = failed_block_ids;
    if (failed_block_ids.empty())
    {
      plan.error_message = "empty failure set";
      return false;
    }

    auto t0 = std::chrono::high_resolution_clock::now();
    std::vector<GlrcEquation> all_equations;
    std::vector<int> candidate_indices;
    glrc_build_recovery_equations(k, r, z, failed_block_ids, all_equations, candidate_indices);

    int f = (int)failed_block_ids.size();
    if ((int)candidate_indices.size() < f)
    {
      plan.error_message = "not enough candidate equations";
      return false;
    }

    std::unordered_set<int> failed_set(failed_block_ids.begin(), failed_block_ids.end());
    std::vector<std::vector<int>> combos;
    std::vector<int> current;
    enumerate_combinations((int)candidate_indices.size(), f, 0, current, combos);

    int best_helpers = INT_MAX;
    std::vector<int> best_combo;
    std::vector<int> best_helper_ids;

    for (const auto &positions : combos)
    {
      std::vector<int> selected;
      for (int pos : positions)
        selected.push_back(candidate_indices[pos]);
      if (!combination_covers(selected, all_equations, failed_set))
        continue;
      if (!glrc_ilp_decode_matrix_invertible(k, r, z, failed_block_ids, selected))
        continue;
      std::vector<int> helpers;
      int cnt = union_helper_count(selected, all_equations, failed_set, helpers);
      if (cnt < best_helpers)
      {
        best_helpers = cnt;
        best_combo = selected;
        best_helper_ids = helpers;
      }
    }

    auto t1 = std::chrono::high_resolution_clock::now();
    plan.ilp_solve_time_sec =
        std::chrono::duration_cast<std::chrono::duration<double>>(t1 - t0).count();

    if (best_combo.empty())
    {
      plan.error_message = "no decodable equation combination (cover + invertible matrix)";
      return false;
    }

    plan.success = true;
    plan.selected_equation_indices = best_combo;
    plan.helper_block_ids = best_helper_ids;
    plan.helper_block_count = best_helpers;
    for (int idx : best_combo)
      plan.selected_equations.push_back(all_equations[idx].name);
    return true;
  }

  static int local_then_global_primary_equation_index(int failed_id, int k, int r, int z)
  {
    if (failed_id < k + r)
      return glrc_payload_group_id(failed_id, k, r, z);
    return failed_id - k - r;
  }

  static bool fill_plan_from_selection(const std::vector<int> &selected,
                                       const std::vector<GlrcEquation> &all_equations,
                                       const std::unordered_set<int> &failed_set, int k, int r, int z,
                                       const std::vector<int> &failed_block_ids, GlrcIlpRepairPlan &plan)
  {
    if (!combination_covers(selected, all_equations, failed_set))
      return false;
    if (!glrc_ilp_decode_matrix_invertible(k, r, z, failed_block_ids, selected))
      return false;
    std::vector<int> helpers;
    const int cnt = union_helper_count(selected, all_equations, failed_set, helpers);
    plan.success = true;
    plan.selected_equation_indices = selected;
    plan.helper_block_ids = helpers;
    plan.helper_block_count = cnt;
    plan.selected_equations.clear();
    for (int idx : selected)
      plan.selected_equations.push_back(all_equations[idx].name);
    return true;
  }

  static int equation_failed_cover_count(const GlrcEquation &eq, const std::unordered_set<int> &failed_set)
  {
    int cnt = 0;
    for (int b : eq.involved_blocks)
      if (failed_set.count(b))
        cnt++;
    return cnt;
  }

  static std::vector<int> local_equations_by_failed_cover(int z, const std::vector<GlrcEquation> &all_equations,
                                                          const std::unordered_set<int> &failed_set)
  {
    std::vector<int> locals;
    for (int lg = 0; lg < z; lg++)
      locals.push_back(lg);
    std::sort(locals.begin(), locals.end(), [&](int a, int b) {
      const int ca = equation_failed_cover_count(all_equations[a], failed_set);
      const int cb = equation_failed_cover_count(all_equations[b], failed_set);
      if (ca != cb)
        return ca > cb;
      return a < b;
    });
    return locals;
  }

  static void build_local_then_global_base_selection(int f, int k, int r, int z,
                                                     const std::vector<GlrcEquation> &all_equations,
                                                     const std::unordered_set<int> &failed_set,
                                                     std::vector<int> &selected)
  {
    selected.clear();
    for (int j = 0; j < r && (int)selected.size() < f; j++)
    {
      if (failed_set.count(k + j))
        continue;
      selected.push_back(z + j);
    }
    const std::vector<int> local_order = local_equations_by_failed_cover(z, all_equations, failed_set);
    for (int lg : local_order)
    {
      if ((int)selected.size() >= f)
        break;
      if (failed_set.count(k + r + lg))
        continue;
      selected.push_back(lg);
    }
    for (int j = 0; j < r && (int)selected.size() < f; j++)
    {
      const int eq = z + j;
      if (std::find(selected.begin(), selected.end(), eq) == selected.end())
        selected.push_back(eq);
    }
    for (int lg : local_order)
    {
      if ((int)selected.size() >= f)
        break;
      if (std::find(selected.begin(), selected.end(), lg) == selected.end())
        selected.push_back(lg);
    }
    if ((int)selected.size() > f)
      selected.resize(f);
  }

  static void build_global_prefix_selection(int f, int k, int r, int z, std::vector<int> &selected)
  {
    selected.clear();
    for (int j = 0; j < r && (int)selected.size() < f; j++)
      selected.push_back(z + j);
    for (int lg = 0; lg < z && (int)selected.size() < f; lg++)
      selected.push_back(lg);
    if ((int)selected.size() > f)
      selected.resize(f);
  }

  static bool try_local_variants(const std::vector<int> &base, int f, int k, int r, int z,
                                 const std::unordered_set<int> &failed_set,
                                 const std::vector<int> &failed_block_ids,
                                 const std::vector<GlrcEquation> &all_equations, GlrcIlpRepairPlan &plan)
  {
    const int anchor_local =
        local_then_global_primary_equation_index(failed_block_ids[0], k, r, z);
    std::vector<int> try_locals;
    try_locals.push_back(anchor_local);
    for (int lg = 0; lg < z; lg++)
    {
      if (lg != anchor_local)
        try_locals.push_back(lg);
    }

    if (fill_plan_from_selection(base, all_equations, failed_set, k, r, z, failed_block_ids, plan))
      return true;

    for (int lg : try_locals)
    {
      std::vector<int> variant = base;
      if (std::find(variant.begin(), variant.end(), lg) != variant.end())
        continue;
      bool replaced = false;
      for (int i = (int)variant.size() - 1; i >= 0; i--)
      {
        if (variant[i] < z)
        {
          variant[i] = lg;
          replaced = true;
          break;
        }
      }
      if (!replaced && !variant.empty())
        variant.back() = lg;
      if (fill_plan_from_selection(variant, all_equations, failed_set, k, r, z, failed_block_ids, plan))
        return true;
    }
    return false;
  }

  static bool try_global_first_fallback(int f, int k, int r, int z,
                                        const std::unordered_set<int> &failed_set,
                                        const std::vector<int> &failed_block_ids,
                                        const std::vector<GlrcEquation> &all_equations,
                                        const std::vector<int> &candidate_indices, GlrcIlpRepairPlan &plan)
  {
    std::vector<int> prefix;
    build_global_prefix_selection(f, k, r, z, prefix);
    if ((int)prefix.size() == f && try_local_variants(prefix, f, k, r, z, failed_set, failed_block_ids,
                                                      all_equations, plan))
      return true;

    std::vector<std::vector<int>> combos;
    std::vector<int> current;
    enumerate_combinations((int)candidate_indices.size(), f, 0, current, combos);
    std::sort(combos.begin(), combos.end(), [&](const std::vector<int> &a, const std::vector<int> &b) {
      auto score = [&](const std::vector<int> &positions) {
        int globals = 0;
        int sum = 0;
        for (int pos : positions)
        {
          const int eq = candidate_indices[pos];
          sum += eq;
          if (eq >= z)
            globals++;
        }
        return std::make_pair(-globals, sum);
      };
      return score(a) < score(b);
    });

    for (const auto &positions : combos)
    {
      std::vector<int> selected;
      for (int pos : positions)
        selected.push_back(candidate_indices[pos]);
      if (fill_plan_from_selection(selected, all_equations, failed_set, k, r, z, failed_block_ids, plan))
        return true;
    }
    return false;
  }

  bool glrc_solve_repair_local_then_global(int k, int r, int z, const std::vector<int> &failed_block_ids,
                                           GlrcIlpRepairPlan &plan)
  {
    plan = GlrcIlpRepairPlan();
    plan.failed_block_ids = failed_block_ids;
    if (failed_block_ids.empty())
    {
      plan.error_message = "empty failure set";
      return false;
    }

    auto t0 = std::chrono::high_resolution_clock::now();
    std::vector<GlrcEquation> all_equations;
    std::vector<int> candidate_indices;
    glrc_build_recovery_equations(k, r, z, failed_block_ids, all_equations, candidate_indices);

    const int f = (int)failed_block_ids.size();
    std::unordered_set<int> failed_set(failed_block_ids.begin(), failed_block_ids.end());
    std::vector<int> selected;

    if (f == 1)
    {
      selected.push_back(local_then_global_primary_equation_index(failed_block_ids[0], k, r, z));
      if (!fill_plan_from_selection(selected, all_equations, failed_set, k, r, z, failed_block_ids, plan))
        plan.error_message = "single-failure local equation not decodable";
    }
    else
    {
      build_local_then_global_base_selection(f, k, r, z, all_equations, failed_set, selected);
      if ((int)selected.size() < f)
        plan.error_message = "not enough equations for failure count";
      else if (!try_local_variants(selected, f, k, r, z, failed_set, failed_block_ids, all_equations, plan) &&
               !try_global_first_fallback(f, k, r, z, failed_set, failed_block_ids, all_equations,
                                          candidate_indices, plan))
        plan.error_message = "local-then-global could not find decodable equation set";
    }

    auto t1 = std::chrono::high_resolution_clock::now();
    plan.ilp_solve_time_sec =
        std::chrono::duration_cast<std::chrono::duration<double>>(t1 - t0).count();
    return plan.success;
  }

  bool glrc_solve_repair_local_first(int k, int r, int z, const std::vector<int> &failed_block_ids,
                                     GlrcIlpRepairPlan &plan)
  {
    plan = GlrcIlpRepairPlan();
    plan.failed_block_ids = failed_block_ids;
    if (failed_block_ids.empty())
    {
      plan.error_message = "empty failure set";
      return false;
    }

    auto t0 = std::chrono::high_resolution_clock::now();
    std::vector<GlrcEquation> all_equations;
    std::vector<int> candidate_indices;
    glrc_build_recovery_equations(k, r, z, failed_block_ids, all_equations, candidate_indices);

    const int f = (int)failed_block_ids.size();
    if ((int)candidate_indices.size() < f)
    {
      plan.error_message = "not enough candidate equations";
      return false;
    }

    std::unordered_set<int> failed_set(failed_block_ids.begin(), failed_block_ids.end());
    std::vector<std::vector<int>> combos;
    std::vector<int> current;
    enumerate_combinations((int)candidate_indices.size(), f, 0, current, combos);

    struct Candidate
    {
      std::vector<int> selected;
      std::vector<int> helpers;
      int local_count = 0;
      int local_failed_cover = 0;
      int helper_count = INT_MAX;
    };

    bool found = false;
    Candidate best;
    for (const auto &positions : combos)
    {
      Candidate cand;
      for (int pos : positions)
        cand.selected.push_back(candidate_indices[pos]);
      if (!combination_covers(cand.selected, all_equations, failed_set))
        continue;
      if (!glrc_ilp_decode_matrix_invertible(k, r, z, failed_block_ids, cand.selected))
        continue;

      for (int eq_idx : cand.selected)
      {
        if (eq_idx < z)
        {
          cand.local_count++;
          cand.local_failed_cover += equation_failed_cover_count(all_equations[eq_idx], failed_set);
        }
      }
      cand.helper_count = union_helper_count(cand.selected, all_equations, failed_set, cand.helpers);

      const bool better =
          !found ||
          cand.local_count > best.local_count ||
          (cand.local_count == best.local_count && cand.local_failed_cover > best.local_failed_cover) ||
          (cand.local_count == best.local_count && cand.local_failed_cover == best.local_failed_cover &&
           cand.helper_count < best.helper_count) ||
          (cand.local_count == best.local_count && cand.local_failed_cover == best.local_failed_cover &&
           cand.helper_count == best.helper_count && cand.selected < best.selected);
      if (better)
      {
        best = std::move(cand);
        found = true;
      }
    }

    auto t1 = std::chrono::high_resolution_clock::now();
    plan.ilp_solve_time_sec =
        std::chrono::duration_cast<std::chrono::duration<double>>(t1 - t0).count();

    if (!found)
    {
      plan.error_message = "local-first could not find decodable equation set";
      return false;
    }

    plan.success = true;
    plan.selected_equation_indices = best.selected;
    plan.helper_block_ids = best.helpers;
    plan.helper_block_count = best.helper_count;
    for (int idx : best.selected)
      plan.selected_equations.push_back(all_equations[idx].name);
    return true;
  }

  std::string glrc_normalize_equation_policy(const std::string &policy)
  {
    if (policy == "local-then-global" || policy == "ceph-style" || policy == "ceph")
      return "local-then-global";
    if (policy == "local-first")
      return "local-first";
    if (policy == "ilp-min-helper" || policy == "ilp" || policy.empty())
      return "ilp-min-helper";
    return policy;
  }

  bool glrc_solve_repair_plan(int k, int r, int z, const std::vector<int> &failed_block_ids,
                              const std::string &equation_policy, GlrcIlpRepairPlan &plan)
  {
    const std::string normalized = glrc_normalize_equation_policy(equation_policy);
    if (normalized == "local-then-global")
      return glrc_solve_repair_local_then_global(k, r, z, failed_block_ids, plan);
    if (normalized == "local-first")
      return glrc_solve_repair_local_first(k, r, z, failed_block_ids, plan);
    if (normalized == "ilp-min-helper")
      return glrc_solve_repair_ilp(k, r, z, failed_block_ids, plan);
    plan = GlrcIlpRepairPlan();
    plan.failed_block_ids = failed_block_ids;
    plan.error_message = "unknown GlrcEquationPolicy: " + equation_policy;
    return false;
  }

double glrc_phase2_est_block_read_sec(int helper_count, int block_shard_count, int stripe_byte_len,
                                    double link_mbps)
{
  if (helper_count < 1 || block_shard_count < 1 || stripe_byte_len < 1)
    return 0.0;
  const size_t bytes = static_cast<size_t>(helper_count) * static_cast<size_t>(block_shard_count) *
                       static_cast<size_t>(stripe_byte_len);
  return node_block_transfer_seconds(bytes, link_mbps);
}

double glrc_phase2_est_block_exchange_sec(int failed_count, int block_shard_count, int stripe_byte_len,
                                          double link_mbps)
{
  if (failed_count < 2 || block_shard_count < 1 || stripe_byte_len < 1)
    return 0.0;
  const size_t bytes = static_cast<size_t>(failed_count - 1) * static_cast<size_t>(block_shard_count) *
                       static_cast<size_t>(stripe_byte_len);
  return node_block_transfer_seconds(bytes, link_mbps);
}

double glrc_phase2_est_block_network_sec(int helper_count, int failed_count, int block_shard_count,
                                         int stripe_byte_len, double link_mbps)
{
  const double ex_sec = glrc_phase2_est_block_exchange_sec(failed_count, block_shard_count, stripe_byte_len, link_mbps);
  // Duplex: (f-1) peer sends and (f-1) recvs overlap; uplink/downlink each at B -> one (f-1)*s*stripe/B slot.
  return glrc_phase2_est_block_read_sec(helper_count, block_shard_count, stripe_byte_len, link_mbps) + ex_sec;
}

double glrc_phase2_est_system_network_sec(int helper_count, int failed_count,
                                          const std::vector<int> &partition_shard_counts, int stripe_byte_len,
                                          double link_mbps)
{
  double est_max = 0.0;
  for (int shards : partition_shard_counts)
  {
    const double t = glrc_phase2_est_block_network_sec(helper_count, failed_count, shards, stripe_byte_len, link_mbps);
    est_max = std::max(est_max, t);
  }
  return est_max;
}

bool glrc_failures_at_most_one_per_group(int k, int r, int z, const std::vector<int> &failed_block_ids)
{
  if (failed_block_ids.empty())
    return false;
  std::vector<std::vector<int>> groups;
  glrc_build_placement_groups(k, r, z, groups);
  std::vector<int> per_group(groups.size(), 0);
  for (int fid : failed_block_ids)
  {
    bool found = false;
    for (size_t gi = 0; gi < groups.size(); gi++)
    {
      for (int b : groups[gi])
      {
        if (b == fid)
        {
          per_group[gi]++;
          if (per_group[gi] > 1)
            return false;
          found = true;
          break;
        }
      }
      if (found)
        break;
    }
    if (!found)
      return false;
  }
  return true;
}

std::string glrc_block_label(int block_id, int k, int r, int z)
{
  const int n = k + r + z;
  if (block_id < 0 || block_id >= n)
    return "?";
  if (block_id < k)
    return "D" + std::to_string(block_id);
  if (block_id < k + r)
    return "G" + std::to_string(block_id - k);
  return "L" + std::to_string(block_id - k - r);
}

std::string glrc_format_block_list(const std::vector<int> &block_ids, int k, int r, int z)
{
  std::string out;
  for (size_t i = 0; i < block_ids.size(); i++)
  {
    if (i > 0)
      out += ' ';
    out += glrc_block_label(block_ids[i], k, r, z);
  }
  return out;
}

} // namespace ECProject
