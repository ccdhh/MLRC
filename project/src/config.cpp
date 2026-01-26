#include "config.h"
#include "tinyxml2.h"
#include <cassert>

namespace ECProject
{
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
    assert((AppendMode == "REP_MODE" || AppendMode == "UNILRC_MODE" || AppendMode == "CACHED_MODE"||AppendMode=="EQUIOX_MODE") && "Error: AppendMode must be REP_MODE, UNILRC_MODE, or CACHED_MODE");
    assert((CodeType == "UniLRC" || CodeType == "AzureLRC" || CodeType == "OptimalLRC" || CodeType == "UniformLRC"||CodeType=="RS") && "Error: CodeType must be UniLRC, AzureLRC, OptimalLRC, or UniformLRC");
    assert(DatanodeNumPerCluster > 0 && "Error: DatanodeNumPerCluster must be greater than 0");
    assert(ClusterNum > 0 && "Error: ClusterNum must be greater than 0");
    if (CodeType == "UniLRC")
    {
      assert(DatanodeNumPerCluster > n / z && "Error: DatanodeNumPerCluster must be greater than n / z");
      assert(ClusterNum > z && "Error: ClusterNum must be greater than z");
    }
    if (CodeType == "AzureLRC")
    {
      assert(DatanodeNumPerCluster > k / z + 1 && "Error: DatanodeNumPerCluster must be greater than k / z + 1");
      assert(ClusterNum > z + 1 && "Error: ClusterNum must be greater than z + 1");
    }
    if (CodeType == "OptimalLRC")
    {
      assert(DatanodeNumPerCluster > r + 1 && "Error: DatanodeNumPerCluster must be greater than r + 1");
      assert(ClusterNum > std::ceil(1.0 * k / z / (r + 1)) * z + 1 && "Error: ClusterNum must be greater than std::ceil(1.0 * k / z / (r + 1)) * z + 1");
    }
    if (CodeType == "UniformLRC")
    {
      assert(DatanodeNumPerCluster > r && "Error: DatanodeNumPerCluster must be greater than r");
      assert(ClusterNum > ((((k + r) / z + 1) / (r + 1) + (bool)(((k + r) / z + 1) % (r + 1))) * ((k + r) % z)) + (((k + r) / z) / (r + 1) + (bool)(((k + r) / z) % (r + 1))) * (z - ((k + r) % z)) && "Error: ClusterNum must be greater than ((((k + r) / z + 1) / (r + 1) + (bool)(((k + r) / z + 1) % (r + 1))) * ((k + r) % z)) + (((k + r) / z) / (r + 1) + (bool)(((k + r) / z) % (r + 1))) * (z - ((k + r) % z))");
    }
    if (CodeType == "RS")
    {
       int n = k + r;

    assert(DatanodeNumPerCluster >= 1 &&"Error: DatanodeNumPerCluster must be >= 1 for RS code");

    assert(ClusterNum * DatanodeNumPerCluster >= n &&"Error: ClusterNum * DatanodeNumPerCluster must be >= (k + r) for RS code");
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
    if (auto elem = root->FirstChildElement("z"))
      z = std::stoi(elem->GetText());
    if (auto elem = root->FirstChildElement("CodeType"))
      CodeType = std::string(elem->GetText());
    if (CodeType == "UniLRC")
    {
      if (auto elem = root->FirstChildElement("alpha"))
        alpha = std::stoi(elem->GetText());
      k = alpha * z * z - alpha * z;
      r = alpha * z;
    }
    else if(CodeType=="RS")
    {
      this->z=0;
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
      CoordinatorIP = std::string(elem->GetText());
    if (auto elem = root->FirstChildElement("CoordinatorPort"))
      CoordinatorPort = std::stoi(elem->GetText());
    if (auto elem = root->FirstChildElement("AppendMode"))
      AppendMode = std::string(elem->GetText());
  }

  void Config::printConfigs() const
  {
    std::cout << "Configuration Parameters:" << std::endl;
    std::cout << "  AlignedSize: " << AlignedSize << " bytes" << std::endl;
    std::cout << "  UnitSize: " << UnitSize << " bytes" << std::endl;
    std::cout << "  BlockSize: " << BlockSize << " bytes" << std::endl;
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
    std::cout << "  AppendMode: " << AppendMode << std::endl;
    std::cout << "  CodeType: " << CodeType << std::endl;
  }
}
