#include "glrc_pipeline_codec.h"
#include "unilrc_encoder.h"
#include <sstream>

namespace ECProject
{
namespace
{
  void build_coef_row_impl(int k, int r, int z, int n, int eq_index, const unsigned char *encode_matrix,
                           const std::vector<std::vector<int>> &groups, std::vector<unsigned char> &coef_row)
  {
    coef_row.assign(n, 0);
    if (eq_index < z)
    {
      for (int b : groups[eq_index])
        coef_row[b] = 1;
    }
    else
    {
      const int g = eq_index - z;
      for (int i = 0; i < k; i++)
        coef_row[i] = encode_matrix[(k + g) * k + i];
      coef_row[k + g] = 1;
    }
  }
} // namespace

void glrc_pipeline_build_coef_row(int k, int r, int z, int eq_index, std::vector<unsigned char> &coef_row_out)
{
  const int n = k + r + z;
  std::vector<std::vector<int>> groups;
  glrc_build_placement_groups(k, r, z, groups);
  unsigned char *encode_matrix = new unsigned char[(k + r + z) * k];
  gen_glrc_matrix(encode_matrix, k, r, z);
  build_coef_row_impl(k, r, z, n, eq_index, encode_matrix, groups, coef_row_out);
  delete[] encode_matrix;
}

GlrcPipelineEqCodec glrc_pipeline_equation_codec(int k, int r, int z, int eq_index)
{
  if (eq_index >= 0 && eq_index < z)
    return GlrcPipelineEqCodec::LOCAL_XOR;
  if (eq_index >= z && eq_index < z + r)
    return GlrcPipelineEqCodec::GLOBAL_CAUCHY;
  (void)k;
  (void)r;
  return GlrcPipelineEqCodec::LOCAL_XOR;
}

bool glrc_pipeline_verify_coef_row(int k, int r, int z, int eq_index,
                                   const std::vector<unsigned char> &coef_row, std::string &error_message)
{
  const int n = k + r + z;
  if ((int)coef_row.size() != n)
  {
    error_message = "coef_row size mismatch";
    return false;
  }

  std::vector<unsigned char> expect;
  glrc_pipeline_build_coef_row(k, r, z, eq_index, expect);

  if (eq_index < z)
  {
    std::vector<std::vector<int>> groups;
    glrc_build_placement_groups(k, r, z, groups);
    for (int b : groups[eq_index])
    {
      if (coef_row[b] != 1)
      {
        error_message = "local equation L" + std::to_string(eq_index) + " block " + std::to_string(b) +
                        " coef=" + std::to_string(coef_row[b]) + " expected XOR coef 1";
        return false;
      }
    }
    for (int b = 0; b < n; b++)
    {
      if (coef_row[b] != expect[b])
      {
        error_message = "local coef row mismatch at block " + std::to_string(b);
        return false;
      }
    }
    return true;
  }

  const int g = eq_index - z;
  if (coef_row[k + g] != 1)
  {
    error_message = "global equation G" + std::to_string(g) + " parity coef must be 1";
    return false;
  }
  for (int b = 0; b < n; b++)
  {
    if (coef_row[b] != expect[b])
    {
      error_message = "global Cauchy coef mismatch at block " + std::to_string(b);
      return false;
    }
  }
  return true;
}

void glrc_pipeline_accumulate_byte(unsigned char &dst, unsigned char src, unsigned char coef,
                                   GlrcPipelineEqCodec codec)
{
  if (coef == 0)
    return;
  if (codec == GlrcPipelineEqCodec::LOCAL_XOR)
  {
    if (coef != 1)
      dst ^= gf_mul(coef, src);
    else
      dst ^= src;
    return;
  }
  dst ^= gf_mul(coef, src);
}

void glrc_pipeline_accumulate_range(unsigned char *dst, const unsigned char *src, unsigned char coef, int len,
                                    GlrcPipelineEqCodec codec)
{
  if (coef == 0 || len <= 0)
    return;
  if (codec == GlrcPipelineEqCodec::LOCAL_XOR && coef == 1)
  {
    for (int i = 0; i < len; i++)
      dst[i] ^= src[i];
    return;
  }
  for (int i = 0; i < len; i++)
    glrc_pipeline_accumulate_byte(dst[i], src[i], coef, codec);
}

void glrc_pipeline_init_partial_range(unsigned char *acc, const unsigned char *src, unsigned char coef, int len,
                                      GlrcPipelineEqCodec codec)
{
  if (len <= 0)
    return;
  if (codec == GlrcPipelineEqCodec::LOCAL_XOR)
  {
    if (coef != 1)
    {
      for (int i = 0; i < len; i++)
        acc[i] = gf_mul(coef, src[i]);
    }
    else
    {
      for (int i = 0; i < len; i++)
        acc[i] = src[i];
    }
    return;
  }
  for (int i = 0; i < len; i++)
    acc[i] = gf_mul(coef, src[i]);
}

} // namespace ECProject