#include "config.h"
#include "link_bandwidth.h"
#include "tinyxml2.h"
#include <algorithm>
#include <cassert>
#include <cctype>
#include <cmath>
#include <cstdlib>

namespace ECProject
{
  namespace
  {
    std::string trim_config_text(const char *text)
    {
      if (text == nullptr)
        return {};
      std::string value(text);
      const auto first = std::find_if_not(value.begin(), value.end(),
                                          [](unsigned char c) { return std::isspace(c) != 0; });
      const auto last = std::find_if_not(value.rbegin(), value.rend(),
                                         [](unsigned char c) { return std::isspace(c) != 0; }).base();
      return first < last ? std::string(first, last) : std::string();
    }
  }

  Config *Config::instance = nullptr;

  Config::Config(const std::string &configPath)
  {
    loadConfig(configPath);
    printConfigs();
    validateConfig();
  }

  void Config::validateConfig() const
  {
    assert(BlockSize % UnitSize == 0 && "Error: BlockSize must be divisible by UnitSize");
    assert((AppendMode == "REP_MODE" || AppendMode == "UNILRC_MODE" || AppendMode == "CACHED_MODE" || AppendMode == "EQUIOX_MODE") && "Error: AppendMode must be REP_MODE, UNILRC_MODE, or CACHED_MODE");
    assert((CodeType == "UniLRC" || CodeType == "AzureLRC" || CodeType == "OptimalLRC" || CodeType == "UniformLRC" || CodeType == "gLRC" || CodeType == "RS") && "Error: CodeType must be UniLRC, AzureLRC, OptimalLRC, UniformLRC, gLRC, or RS");
    if (CodeType == "gLRC" || CodeType == "AzureLRC" || CodeType == "OptimalLRC")
    {
      assert((GlrcRepairMode == "phase1" || GlrcRepairMode == "phase2" ||
              GlrcRepairMode == "pipeline" || GlrcRepairMode == "hybrid") &&
             "Error: GlrcRepairMode must be phase1, phase2, pipeline, or hybrid");
    }
    assert(DatanodeNumPerCluster > 0 && "Error: DatanodeNumPerCluster must be greater than 0");
    assert(ClusterNum > 0 && "Error: ClusterNum must be greater than 0");
    if (CodeType == "UniLRC")
    {
      assert(DatanodeNumPerCluster > n / z && "Error: DatanodeNumPerCluster must be greater than n / z");
      assert(ClusterNum > z && "Error: ClusterNum must be greater than z");
    }
    if (CodeType == "AzureLRC")
    {
      // Flat (rack-free) Azure-LRC: same hybrid data-plane as gLRC; cluster is
      // only a proxy partitioning, not a co-location constraint.
      assert(z > 0 && "Error: z must be positive for AzureLRC");
      assert(k % z == 0 && "Error: k must be divisible by z for AzureLRC");
      const int max_blocks_per_local_group = k / z + 1;
      assert(DatanodeNumPerCluster >= max_blocks_per_local_group &&
             "Error: DatanodeNumPerCluster must fit an AzureLRC local group");
      assert(ClusterNum * DatanodeNumPerCluster >= n &&
             "Error: ClusterNum * DatanodeNumPerCluster must be >= n for AzureLRC");
    }
    if (CodeType == "OptimalLRC")
    {
      // Flat (rack-free) Optimal-LRC: hybrid data-plane like Azure/gLRC; cluster is
      // only proxy partitioning, not rack co-location.
      assert(z > 0 && "Error: z must be positive for OptimalLRC");
      assert(k % z == 0 && "Error: k must be divisible by z for OptimalLRC");
      const int max_blocks_per_local_group = k / z + 1;
      assert(DatanodeNumPerCluster >= max_blocks_per_local_group &&
             "Error: DatanodeNumPerCluster must fit an OptimalLRC local group");
      assert(ClusterNum * DatanodeNumPerCluster >= n &&
             "Error: ClusterNum * DatanodeNumPerCluster must be >= n for OptimalLRC");
    }
    if (CodeType == "UniformLRC")
    {
      assert(DatanodeNumPerCluster > r && "Error: DatanodeNumPerCluster must be greater than r");
      assert(ClusterNum > ((((k + r) / z + 1) / (r + 1) + (bool)(((k + r) / z + 1) % (r + 1))) * ((k + r) % z)) + (((k + r) / z) / (r + 1) + (bool)(((k + r) / z) % (r + 1))) * (z - ((k + r) % z)) && "Error: ClusterNum must be greater than ((((k + r) / z + 1) / (r + 1) + (bool)(((k + r) / z + 1) % (r + 1))) * ((k + r) % z)) + (((k + r) / z) / (r + 1) + (bool)(((k + r) / z) % (r + 1))) * (z - ((k + r) % z))");
    }
    if (CodeType == "gLRC")
    {
      assert(z > 0 && "Error: z must be positive for gLRC");
      int max_payload_per_group = (k + r + z - 1) / z;
      assert(max_payload_per_group >= r &&
             "Error: the final balanced gLRC group must hold all r global parities");
      int max_blocks_per_group = max_payload_per_group + 1;
      assert(DatanodeNumPerCluster >= max_blocks_per_group &&
             "Error: DatanodeNumPerCluster must fit the largest gLRC placement group");
      assert(ClusterNum >= z && "Error: ClusterNum must be at least z for gLRC");
    }
    if (CodeType == "RS")
    {
      int n = k + r;

      assert(DatanodeNumPerCluster  >= 1 && "Error: DatanodeNumPerCluster must be >= 1 for RS code");

      assert(ClusterNum * DatanodeNumPerCluster >= n && "Error: ClusterNum * DatanodeNumPerCluster must be >= (k + r) for RS code");
    }
  }

  Config *Config::getInstance(const std::string &configPath)
  {
    if (instance == nullptr)
    {
      instance = new Config(configPath);
    }
    return instance;
  }

  void Config::loadConfig(const std::string &configPath)
  {
    tinyxml2::XMLDocument doc;
    if (doc.LoadFile(configPath.c_str()) != tinyxml2::XML_SUCCESS)
    {
      std::cerr << "Failed to load config file: " << configPath << std::endl;
      return;
    }

    tinyxml2::XMLElement *root = doc.RootElement();
    if (root == nullptr)
    {
      std::cerr << "Invalid config file format" << std::endl;
      return;
    }

    if (auto elem = root->FirstChildElement("AlignedSize"))
      AlignedSize = std::stoi(elem->GetText());
    if (auto elem = root->FirstChildElement("UnitSize"))
      UnitSize = std::stoi(elem->GetText());
    if (auto elem = root->FirstChildElement("BlockSize"))
      BlockSize = std::stoi(elem->GetText());
    if (auto elem = root->FirstChildElement("NodeBlockBandwidthMBps"))
      NodeBlockBandwidthMBps = std::stod(elem->GetText());
    if (auto elem = root->FirstChildElement("z"))
      z = std::stoi(elem->GetText());
    if (auto elem = root->FirstChildElement("CodeType"))
      CodeType = trim_config_text(elem->GetText());
    if (auto elem = root->FirstChildElement("GlrcRepairMode"))
      GlrcRepairMode = trim_config_text(elem->GetText());
    if (auto elem = root->FirstChildElement("GlrcEquationPolicy"))
      GlrcEquationPolicy = trim_config_text(elem->GetText());
    if (auto elem = root->FirstChildElement("GlrcShardCount"))
      GlrcShardCount = std::stoi(elem->GetText());
    if (auto elem = root->FirstChildElement("GlrcPipelineWindow"))
      GlrcPipelineWindow = std::stoi(elem->GetText());
    if (auto elem = root->FirstChildElement("GlrcHybridP"))
      GlrcHybridP = trim_config_text(elem->GetText());
    if (auto elem = root->FirstChildElement("GlrcPhase2WriteBack"))
    {
      std::string v = trim_config_text(elem->GetText());
      GlrcPhase2WriteBack = !(v == "0" || v == "false" || v == "FALSE");
    }
    if (CodeType == "UniLRC")
    {
      if (auto elem = root->FirstChildElement("alpha"))
        alpha = std::stoi(elem->GetText());
      k = alpha * z * z - alpha * z;
      r = alpha * z;
    }
    else if (CodeType == "RS")
    {
      this->z = 0;
      if (auto elem = root->FirstChildElement("k"))
        k = std::stoi(elem->GetText());
      if (auto elem = root->FirstChildElement("r"))
        r = std::stoi(elem->GetText());
    }
    else
    {
      if (auto elem = root->FirstChildElement("k"))
        k = std::stoi(elem->GetText());
      if (auto elem = root->FirstChildElement("r"))
        r = std::stoi(elem->GetText());
    }
    n = k + r + z;

    if (auto elem = root->FirstChildElement("DatanodeNumPerCluster"))
      DatanodeNumPerCluster = std::stoi(elem->GetText());
    if (auto elem = root->FirstChildElement("ClusterNum"))
      ClusterNum = std::stoi(elem->GetText());
    if (auto elem = root->FirstChildElement("CoordinatorIP"))
      CoordinatorIP = trim_config_text(elem->GetText());
    if (auto elem = root->FirstChildElement("CoordinatorPort"))
      CoordinatorPort = std::stoi(elem->GetText());
    if (auto elem = root->FirstChildElement("ClientIP"))
      ClientIP = trim_config_text(elem->GetText());
    if (auto elem = root->FirstChildElement("ClientPort"))
      ClientPort = std::stoi(elem->GetText());
    if (auto elem = root->FirstChildElement("AppendMode"))
      AppendMode = trim_config_text(elem->GetText());
    N = get_N(); // N
    get_num_arry();
  }

  void Config::printConfigs() const
  {
    const char *quiet = std::getenv("DDRT_QUIET_CONFIG");
    if (quiet && quiet[0] != '\0' && quiet[0] != '0')
      return;

    std::cout << "Configuration Parameters:" << std::endl;
    std::cout << "  AlignedSize: " << AlignedSize << " bytes" << std::endl;
    std::cout << "  UnitSize: " << UnitSize << " bytes" << std::endl;
    std::cout << "  BlockSize: " << BlockSize << " bytes" << std::endl;
    if (NodeBlockBandwidthMBps > 0.0)
    {
      std::cout << "  NodeBlockBandwidthMBps: " << NodeBlockBandwidthMBps << " MB/s" << std::endl;
      std::cout << "    node NIC model: hop egress + hub/sink aggregate ingress wall-clock floor"
                << " (fan-in TCP drains unpaced; same-host DN↔proxy unlimited)" << std::endl;
      std::cout << "  SingleBlockTransferTime: "
                << node_block_transfer_seconds(BlockSize, NodeBlockBandwidthMBps) << " s/block at full link"
                << std::endl;
    }
    std::cout << "  alpha: " << (int)alpha << std::endl;
    std::cout << "  z: " << (int)z << std::endl;
    std::cout << "  n: " << n << std::endl;
    std::cout << "  k: " << k << std::endl;
    std::cout << "  r: " << r << std::endl;
    std::cout << "  (n, k, r, z): (" << n << ", " << k << ", " << r << ", " << (int)z << ")" << std::endl;
    std::cout << "  DatanodeNumPerCluster: " << DatanodeNumPerCluster << " nodes/cluster" << std::endl;
    std::cout << "  ClusterNum: " << (int)ClusterNum << " clusters" << std::endl;
    std::cout << "  CoordinatorIP: " << CoordinatorIP << std::endl;
    std::cout << "  CoordinatorPort: " << CoordinatorPort << std::endl;
    std::cout << "  ClientIP: " << ClientIP << std::endl;
    std::cout << "  ClientPort: " << ClientPort << std::endl;
    std::cout << "  AppendMode: " << AppendMode << std::endl;
    std::cout << "  CodeType: " << CodeType << std::endl;
    if (CodeType == "gLRC" || CodeType == "AzureLRC" || CodeType == "OptimalLRC")
    {
      std::cout << "  GlrcRepairMode: " << GlrcRepairMode << std::endl;
      std::cout << "  GlrcEquationPolicy: " << GlrcEquationPolicy << std::endl;
      std::cout << "  GlrcShardCount: " << GlrcShardCount << std::endl;
      std::cout << "  GlrcPipelineWindow: " << GlrcPipelineWindow
                << (GlrcPipelineWindow == 0 ? " (full pipeline, W=GlrcShardCount)" : "") << std::endl;
      std::cout << "  GlrcHybridP: " << GlrcHybridP << std::endl;
      std::cout << "  GlrcPhase2WriteBack: " << (GlrcPhase2WriteBack ? "true" : "false") << std::endl;
    }
  }
  int Config::get_N()
  {
    // Find the maximum N such that ceil(((2^N * k) + r) / r) fits in ClusterNum.
    // If ClusterNum is too small, return 0 to avoid infinite loop.
    if (ClusterNum <= 0 || r <= 0) return 0;
    for (int N = 0; N < 32; N++)
    {
      int temp_num_1 = static_cast<int>(std::ceil(((std::pow(2.0, N) * k) + r) / static_cast<double>(r)));
      int temp_num_2 = static_cast<int>(std::ceil(((std::pow(2.0, N + 1) * k) + r) / static_cast<double>(r)));
      if (temp_num_1 <= ClusterNum && temp_num_2 > ClusterNum)
      {
        return N;
      }
    }
    return 0;
  }
  void Config::get_num_arry()
  {
    this->num_arry.clear();
    for (int i = 1; i <= N; i++)
    {
      int temp_num = (static_cast<int>(std::ceil((std::pow(2.0, i - 1) * k + r) / static_cast<double>(r))) * 2) -
                     static_cast<int>(std::ceil((std::pow(2.0, i) * k + r) / static_cast<double>(r)));
      int L = static_cast<int>(temp_num);
      this->num_arry.push_back(L);
    }
  }
}
