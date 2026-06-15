#include "coordinator.h"
#include "runtime_paths.h"

int main(int argc, char **argv)
{
  const std::string config_path =
      resolve_path_relative_to_executable(argc > 0 ? argv[0] : nullptr,
                                          "../../config/clusterInformation.xml");
  const std::string sys_config_path =
      resolve_path_relative_to_executable(argc > 0 ? argv[0] : nullptr,
                                          "../../config/parameterConfiguration.xml");
  std::cout << "Cluster config path: " << config_path << std::endl;
  std::cout << "Sys config path: " << sys_config_path << std::endl;

  ECProject::Config *config = ECProject::Config::getInstance(sys_config_path);
  ECProject::Coordinator coordinator(config->CoordinatorIP + ":" + std::to_string(config->CoordinatorPort), config_path, sys_config_path);
  coordinator.Run();
  return 0;
}