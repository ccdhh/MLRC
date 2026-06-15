#ifndef GLRC_PIPELINE_CODEC_H
#define GLRC_PIPELINE_CODEC_H

#include "glrc_repair_ilp.h"
#include <string>
#include <vector>

namespace ECProject
{
  /** Per-byte equation accumulation: local=L_xor (coef must be 1), global=GF Cauchy row. */
  enum class GlrcPipelineEqCodec
  {
    LOCAL_XOR = 0,
    GLOBAL_CAUCHY = 1
  };

  /** Build per-block coefficients for one recovery equation (same system as decode_glrc_ilp). */
  void glrc_pipeline_build_coef_row(int k, int r, int z, int eq_index, std::vector<unsigned char> &coef_row_out);

  GlrcPipelineEqCodec glrc_pipeline_equation_codec(int k, int r, int z, int eq_index);

  /**
   * Validate coefficient row matches equation semantics:
   *   local  -> involved survivors use coef 1
   *   global -> D0..D(k-1) Cauchy coeffs + G_g coef 1
   */
  bool glrc_pipeline_verify_coef_row(int k, int r, int z, int eq_index,
                                     const std::vector<unsigned char> &coef_row,
                                     std::string &error_message);

  /** RHS byte update: dst ^= coef*src (XOR form used by gLRC equations). */
  void glrc_pipeline_accumulate_byte(unsigned char &dst, unsigned char src, unsigned char coef,
                                     GlrcPipelineEqCodec codec);

  /** RHS stripe update over len bytes. */
  void glrc_pipeline_accumulate_range(unsigned char *dst, const unsigned char *src, unsigned char coef, int len,
                                      GlrcPipelineEqCodec codec);

  /** Apply weighted local stripe into accumulator (chain head first hop). */
  void glrc_pipeline_init_partial_range(unsigned char *acc, const unsigned char *src, unsigned char coef, int len,
                                        GlrcPipelineEqCodec codec);

} // namespace ECProject

#endif
