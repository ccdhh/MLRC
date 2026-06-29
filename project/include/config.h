#ifndef CONFIG_H
#define CONFIG_H

#include "devcommon.h"
#include <vector>

namespace ECProject
{
  const int DATANODE_PORT_SHIFT = 50;
  const int PROXY_PORT_SHIFT = 1;
  /** Base TCP port for Phase-2 stripe exchange (see phase2_exchange_port). */
  const int PROXY_PHASE2_EXCHANGE_BASE = 52000;
  /** Port band per recovery epoch (must exceed max f * partition stride). */
  const int PROXY_PHASE2_EPOCH_STRIDE = 256;
  /** Must match generate_local_cluster.py BASE_PROXY / PROXY_STRIDE. */
  const int PROXY_GRPC_BASE = 50405;
  const int PROXY_GRPC_STRIDE = 2;
  /** Inclusive max proxy index (45 local proxies: 0..44). Must match generate_local_cluster.py. */
  const int PROXY_GRPC_MAX_INDEX = 44;
  const int PROXY_PHASE2_PER_PROXY_BAND = 256;
  /** Base TCP port for gLRC pipeline shard exchange (see pipeline_*_listen_port). */
  const int PROXY_PIPELINE_EXCHANGE_BASE = 53000;
  const int PROXY_PIPELINE_PER_PROXY_BAND = 256;

  class Config
  {
  private:
    static Config *instance;
    Config(const std::string &configPath);

  public:
    static Config *getInstance(const std::string &configPath);
    void loadConfig(const std::string &configPath);
    void printConfigs() const;
    void validateConfig() const;
    int get_N();
    void get_num_arry();

    int AlignedSize = 4096;
    int UnitSize = 8 * 1024;
    unsigned int BlockSize = 1024 * 1024;
    /**
     * Link cap (MB/s). 0 = unlimited.
     * - Proxy: shared aggregate ingress when pulling helper blocks in parallel.
     * - Datanode: per-node ingress on recovery write-back (single flow per block).
     */
    double NodeBlockBandwidthMBps = 0.0;
    int alpha = 2;
    int z = 2;
    // TODO: need to modify configs to support directly setting k,r,z
    int n = alpha * z * z + z;
    int k = alpha * z * z - alpha * z;
    int r = alpha * z;
    int DatanodeNumPerCluster = 0;
    int ClusterNum = 0;
    std::string CoordinatorIP = "0.0.0.0";
    int CoordinatorPort = 55555;
    std::string AppendMode = "UNILRC_MODE";
    std::string CodeType = "UniLRC";
    /** gLRC repair: "phase1", "phase2", "pipeline", or "hybrid". */
    std::string GlrcRepairMode = "phase1";
    /** Hybrid split p: "auto" or integer in [0, GlrcShardCount-1]. 0 = pipeline-only. */
    std::string GlrcHybridP = "auto";
    std::string GlrcEquationPolicy = "local-then-global";
    int GlrcShardCount = 16;
    /** Pipeline in-flight shard window (W). 0 = W=GlrcShardCount (full pipeline); 1 = serial W=1. */
    int GlrcPipelineWindow = 0;
    /** Phase2: write recovered blocks back to datanodes (disable for repeated random-failure trials on one stripe). */
    bool GlrcPhase2WriteBack = true;
    int N = 0;
    std::vector<int> num_arry;
  };
}

#endif