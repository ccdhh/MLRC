#ifndef GLRC_REPAIR_ILP_H
#define GLRC_REPAIR_ILP_H

#include <string>
#include <vector>

namespace ECProject
{
  /** Hybrid / ILP codec family (encoding matrix + equation semantics). */
  enum class GlrcCodecMode
  {
    GLRC = 0,
    AZURE = 1,
    OPTIMAL = 2
  };

  GlrcCodecMode glrc_codec_mode(const std::string &code_type);
  bool glrc_code_supports_hybrid(const std::string &code_type);
  bool glrc_code_is_azure(const std::string &code_type);
  bool glrc_code_is_optimal(const std::string &code_type);

  enum class GlrcEquationType
  {
    LOCAL,
    GLOBAL
  };

  struct GlrcEquation
  {
    GlrcEquationType type;
    int index; // local group id or global parity id
    std::string name; // e.g. "L0", "G2"
    std::vector<int> involved_blocks;
  };

  struct GlrcIlpRepairPlan
  {
    bool success = false;
    std::vector<int> failed_block_ids;
    std::vector<std::string> selected_equations;
    std::vector<int> selected_equation_indices;
    std::vector<int> helper_block_ids;
    int helper_block_count = 0;
    double ilp_solve_time_sec = 0.0;
    std::string error_message;
  };

  /** Build placement groups: group i lists all block ids (D/G/L) in that placement group. */
  void glrc_build_placement_groups(int k, int r, int z, std::vector<std::vector<int>> &groups);

  /**
   * Azure-LRC local placement groups only (z groups): each is k/z data + Li.
   * Global parities G* are not in these groups; they trail the RR ring separately.
   */
  void azure_build_placement_groups(int k, int r, int z, std::vector<std::vector<int>> &groups);

  /** Build all recovery equations E and candidate set E_F for failed blocks F. */
  void glrc_build_recovery_equations(int k, int r, int z, const std::vector<int> &failed_block_ids,
                                     std::vector<GlrcEquation> &all_equations,
                                     std::vector<int> &candidate_equation_indices);

  /** Azure-LRC equations: local XOR groups + RS global equations (same index layout as gLRC). */
  void azure_build_recovery_equations(int k, int r, int z, const std::vector<int> &failed_block_ids,
                                      std::vector<GlrcEquation> &all_equations,
                                      std::vector<int> &candidate_equation_indices);

  /**
   * Optimal-LRC algebraic local groups (same layout as Azure flat): k/z data + Li.
   * Global G* trail separately under flat RR.
   */
  void optimal_build_placement_groups(int k, int r, int z, std::vector<std::vector<int>> &groups);

  /**
   * Optimal-LRC equations: local (weighted data + Li + all G*) + Cauchy global (all D + Gj).
   */
  void optimal_build_recovery_equations(int k, int r, int z, const std::vector<int> &failed_block_ids,
                                        std::vector<GlrcEquation> &all_equations,
                                        std::vector<int> &candidate_equation_indices);

  /**
   * 0-1 ILP via exhaustive search (|E_F| is small: at most z+r).
   * Minimize helper block union; select exactly f equations covering all failures
   * with an invertible decode matrix (same ff system as decode_glrc_ilp).
   */
  bool glrc_solve_repair_ilp(int k, int r, int z, const std::vector<int> &failed_block_ids,
                             GlrcIlpRepairPlan &plan, GlrcCodecMode codec = GlrcCodecMode::GLRC);

  /**
   * Ceph-style repair equation selection:
   * f=1 -> local L_g for failed data (or related local for parity failure);
   * f>1 -> prefer global equations G0,G1,... (skip failed G blocks), fill with local equations
   *        ordered by how many failures they cover when globals are insufficient.
   */
  bool glrc_solve_repair_local_then_global(int k, int r, int z, const std::vector<int> &failed_block_ids,
                                           GlrcIlpRepairPlan &plan, GlrcCodecMode codec = GlrcCodecMode::GLRC);

  /**
   * Local-first repair equation selection:
   * try combinations with as many local equations as possible first; add global equations only
   * when local equations cannot cover/invert the failed-block system.
   */
  bool glrc_solve_repair_local_first(int k, int r, int z, const std::vector<int> &failed_block_ids,
                                     GlrcIlpRepairPlan &plan, GlrcCodecMode codec = GlrcCodecMode::GLRC);

  /** Normalize config value to "local-then-global", "local-first", or "ilp-min-helper". */
  std::string glrc_normalize_equation_policy(const std::string &policy);

  /** Dispatch by GlrcEquationPolicy (phase1/phase2 share the same equation planner). */
  bool glrc_solve_repair_plan(int k, int r, int z, const std::vector<int> &failed_block_ids,
                              const std::string &equation_policy, GlrcIlpRepairPlan &plan,
                              GlrcCodecMode codec = GlrcCodecMode::GLRC);

  /** Human-readable block id: D0..D(k-1), G0..G(r-1), L0..L(z-1). */
  std::string glrc_block_label(int block_id, int k, int r, int z);

  /** Space-separated labels for a list of block ids. */
  std::string glrc_format_block_list(const std::vector<int> &block_ids, int k, int r, int z);

  /** Phase2 helper ingress for one failed block: H * shard_count * stripe / BW. */
  double glrc_phase2_est_block_read_sec(int helper_count, int block_shard_count, int stripe_byte_len,
                                        double link_mbps);

  /** Phase2 exchange time: (f-1)*shard*stripe/B; parallel peers, duplex (send/recv not summed). */
  double glrc_phase2_est_block_exchange_sec(int failed_count, int block_shard_count, int stripe_byte_len,
                                            double link_mbps);

  /** Per-block network: helper read + exchange (sequential phases on repair site). */
  double glrc_phase2_est_block_network_sec(int helper_count, int failed_count, int block_shard_count,
                                           int stripe_byte_len, double link_mbps);

  /** System wall-clock network lower bound: max over failed blocks. */
  double glrc_phase2_est_system_network_sec(int helper_count, int failed_count,
                                            const std::vector<int> &partition_shard_counts,
                                            int stripe_byte_len, double link_mbps);

  /** True when every placement group contains at most one failed block (local-first pipeline suffices). */
  bool glrc_failures_at_most_one_per_group(int k, int r, int z, const std::vector<int> &failed_block_ids,
                                           GlrcCodecMode codec = GlrcCodecMode::GLRC);

} // namespace ECProject

#endif