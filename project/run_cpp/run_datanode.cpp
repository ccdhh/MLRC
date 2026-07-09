#include "datanode.h"
#include "runtime_paths.h"

int main(int argc, char **argv)
{
    // pid_t pid = fork();
    // if (pid > 0)
    // {
    //     exit(0);
    // }
    // setsid();
    if (true)
    {
        umask(0);
        close(STDIN_FILENO);
        // close(STDOUT_FILENO);
        // close(STDERR_FILENO);
    }

    std::string ip_and_port(argv[1]);
    // std::string ip = ip_and_port.substr(0, ip_and_port.find(":"));
    // int port = std::stoi(ip_and_port.substr(ip_and_port.find(":") + 1, ip_and_port.size()));
    const std::string sys_config_path =
        resolve_path_relative_to_executable(argc > 0 ? argv[0] : nullptr,
                                            "../../config/parameterConfiguration.xml");
    const std::string cluster_info_path =
        resolve_path_relative_to_executable(argc > 0 ? argv[0] : nullptr,
                                            "../../config/clusterInformation.xml");
    ECProject::DataNode datanode(ip_and_port, sys_config_path, cluster_info_path);
    datanode.Run();
    return 0;
}
