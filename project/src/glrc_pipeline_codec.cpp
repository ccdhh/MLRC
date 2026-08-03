#include "glrc_pipeline_codec.h"
#include "unilrc_encoder.h"
#include <algorithm>
#include <cstring>
#include <sstream>

namespace ECProject
{
namespace
{
  void select_groups_and_matrix(int k, int r, int z, GlrcCodecMode codec,
                                std::vector<std::vector<int>> &groups, unsigned char *&encode_matrix)
  {
    if (codec == GlrcCodecMode::AZURE)
      azure_build_placement_groups(k, r, z, groups);
    else if (codec == GlrcCodecMode::OPTIMAL)
      optimal_build_placement_groups(k, r, z, groups);
    else
      glrc_build_placement_groups(k, r, z, groups);
    encode_matrix = new unsigned char[(k + r + z) * k];
    if (codec == GlrcCodecMode::AZURE)
      gen_azure_lrc_matrix(encode_matrix, k, r, z);
    else if (codec == GlrcCodecMode::OPTIMAL)
      gen_optimal_lrc_matrix(encode_matrix, k, r, z);
    else
      gen_glrc_matrix(encode_matrix, k, r, z);
  }

  void build_coef_row_impl(int k, int r, int z, int n, int eq_index, const unsigned char *encode_matrix,
                           const std::vector<std::vector<int>> &groups, std::vector<unsigned char> &coef_row,
                           GlrcCodecMode codec)
  {
    coef_row.assign(n, 0);
    if (eq_index < z)
    {
      if (codec == GlrcCodecMode::AZURE)
      {
        for (int b : groups[eq_index])
          coef_row[b] = 1;
      }
      else if (codec == GlrcCodecMode::OPTIMAL)
      {
        for (int b : groups[eq_index])
          coef_row[b] = (b < k) ? glrc_local_block_coefficient(b, k, r) : 1;
        for (int g = 0; g < r; g++)
          coef_row[k + g] = 1;
      }
      else
      {
        for (int b : groups[eq_index])
          coef_row[b] = glrc_local_block_coefficient(b, k, r);
      }
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

void glrc_pipeline_build_coef_row(int k, int r, int z, int eq_index, std::vector<unsigned char> &coef_row_out,
                                  GlrcCodecMode codec)
{
  const int n = k + r + z;
  std::vector<std::vector<int>> groups;
  unsigned char *encode_matrix = nullptr;
  select_groups_and_matrix(k, r, z, codec, groups, encode_matrix);
  build_coef_row_impl(k, r, z, n, eq_index, encode_matrix, groups, coef_row_out, codec);
  delete[] encode_matrix;
}

GlrcPipelineEqCodec glrc_pipeline_equation_codec(int k, int r, int z, int eq_index)
{
  if (eq_index >= 0 && eq_index < z)
    return GlrcPipelineEqCodec::LOCAL_LINEAR;
  if (eq_index >= z && eq_index < z + r)
    return GlrcPipelineEqCodec::GLOBAL_CAUCHY;
  (void)k;
  (void)r;
  return GlrcPipelineEqCodec::LOCAL_LINEAR;
}

bool glrc_pipeline_verify_coef_row(int k, int r, int z, int eq_index,
                                   const std::vector<unsigned char> &coef_row, std::string &error_message,
                                   GlrcCodecMode codec)
{
  const int n = k + r + z;
  if ((int)coef_row.size() != n)
  {
    error_message = "coef_row size mismatch";
    return false;
  }

  std::vector<unsigned char> expect;
  glrc_pipeline_build_coef_row(k, r, z, eq_index, expect, codec);
  if (coef_row != expect)
  {
    error_message = "coef_row mismatch for equation " + std::to_string(eq_index);
    return false;
  }
  return true;
}

void glrc_pipeline_accumulate_byte(unsigned char &dst, unsigned char src, unsigned char coef,
                                   GlrcPipelineEqCodec codec)
{
  if (coef == 0)
    return;
  if (codec == GlrcPipelineEqCodec::LOCAL_LINEAR)
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
  if (codec == GlrcPipelineEqCodec::LOCAL_LINEAR && coef == 1)
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
  if (codec == GlrcPipelineEqCodec::LOCAL_LINEAR)
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
