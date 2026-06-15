#include "proxy.h"
#include "runtime_paths.h"
#include <csignal>

int main(int argc, char **argv)
{
    // std::string coordinator_ip = "0.0.0.0";
    // if (argc == 3)
    // {
    //     coordinator_ip = std::string(argv[2]);
    // }
    // pid_t pid = fork();
    // if (pid > 0)
    // {
    //     exit(0);
    // }
    // setsid();

    std::string ip_and_port(argv[1]);
    std::signal(SIGPIPE, SIG_IGN);
    if (true)
    {
        umask(0);
        close(STDIN_FILENO);
        // close(STDOUT_FILENO);
        // close(STDERR_FILENO);
    }

    const std::string config_path =
        resolve_path_relative_to_executable(argc > 0 ? argv[0] : nullptr,
                                            "../../config/clusterInformation.xml");
    const std::string sys_config_path =
        resolve_path_relative_to_executable(argc > 0 ? argv[0] : nullptr,
                                            "../../config/parameterConfiguration.xml");
    std::cout << "Cluster config path: " << config_path << std::endl;
    std::cout << "Sys config path: " << sys_config_path << std::endl;

    ECProject::Config *config = ECProject::Config::getInstance(sys_config_path);
    ECProject::Proxy proxy(ip_and_port, config_path, config->CoordinatorIP + ":" + std::to_string(config->CoordinatorPort), sys_config_path);
    proxy.Run();
    return 0;
}