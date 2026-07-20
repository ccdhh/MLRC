#include "client.h"
#include "toolbox.h"
#include <fstream>
#include <sys/time.h>
#include <unistd.h>
#include <sys/stat.h>
#include <cstdlib>
#include "config.h"
#include <iomanip>
#include <iostream>
#include <chrono>
#include <algorithm>
#include <random>
#include <numeric>
#include <sstream>
#include <thread>
#include "unilrc_encoder.h"
#include "glrc_repair_ilp.h"

static int env_int_or(const char *name, int default_value)
{
    const char *v = std::getenv(name);
    if (!v || v[0] == '\0')
        return default_value;
    return std::max(1, std::atoi(v));
}

static bool parse_failed_blocks_env(const char *raw, int n, std::vector<int> &out)
{
    if (!raw || raw[0] == '\0')
        return false;
    out.clear();
    std::stringstream ss(raw);
    std::string token;
    while (std::getline(ss, token, ','))
    {
        if (token.empty())
            continue;
        int id = std::stoi(token);
        if (id < 0 || id >= n)
            return false;
        out.push_back(id);
    }
    std::sort(out.begin(), out.end());
    out.erase(std::unique(out.begin(), out.end()), out.end());
    return !out.empty();
}

#include "runtime_paths.h"

static void print_glrc_client_usage(const char *argv0)
{
    std::cerr
        << "Usage: " << (argv0 ? argv0 : "main_client")
        << " [options] [coordinator_ip:port]\n"
        << "  -f, --fail-count N   number of failed blocks per trial (1..n-1)\n"
        << "  -n, --trials N       number of random trials (>=1)\n"
        << "  --no-warmup          skip pre-trial pipeline warmup recovery\n"
        << "  -h, --help           show this help\n"
        << "\n"
        << "If -f/-n are omitted: prompt interactively when stdin is a TTY;\n"
        << "otherwise use env GLRC_FAIL_COUNT / GLRC_TRIALS (default 1/1).\n"
        << "Coordinator may also be set via COORDINATOR_ADDR.\n";
}

/** Returns false on parse error / --help. Sets *show_help if help was requested. */
static bool parse_glrc_cli_args(int argc, char **argv, int &fail_count, int &trial_count,
                                bool &have_f, bool &have_n, std::string &coordinator_override,
                                bool &show_help, bool &skip_warmup)
{
    have_f = false;
    have_n = false;
    show_help = false;
    skip_warmup = false;
    fail_count = 0;
    trial_count = 0;
    coordinator_override.clear();

    for (int i = 1; i < argc; ++i)
    {
        const std::string arg = argv[i];
        auto need_value = [&](const char *opt) -> const char * {
            if (i + 1 >= argc)
            {
                std::cerr << "missing value for " << opt << std::endl;
                return nullptr;
            }
            return argv[++i];
        };

        if (arg == "-h" || arg == "--help")
        {
            show_help = true;
            return false;
        }
        if (arg == "-f" || arg == "--fail-count")
        {
            const char *v = need_value(arg.c_str());
            if (!v)
                return false;
            fail_count = std::atoi(v);
            have_f = true;
            continue;
        }
        if (arg == "-n" || arg == "--trials")
        {
            const char *v = need_value(arg.c_str());
            if (!v)
                return false;
            trial_count = std::atoi(v);
            have_n = true;
            continue;
        }
        if (arg == "--no-warmup")
        {
            skip_warmup = true;
            continue;
        }
        if (!arg.empty() && arg[0] == '-')
        {
            std::cerr << "unknown option: " << arg << std::endl;
            return false;
        }
        if (!coordinator_override.empty())
        {
            std::cerr << "unexpected argument: " << arg << std::endl;
            return false;
        }
        coordinator_override = arg;
    }
    return true;
}

static bool read_glrc_test_params(int n, int &fail_count, int &trial_count,
                                  bool have_f, bool have_n)
{
    const char *env_f = std::getenv("GLRC_FAIL_COUNT");
    const char *env_t = std::getenv("GLRC_TRIALS");
    const bool non_interactive = !isatty(STDIN_FILENO);

    if (!have_f)
    {
        if (non_interactive)
            fail_count = (env_f && env_f[0]) ? std::max(1, std::atoi(env_f)) : 1;
        else
        {
            std::cout << "请输入失败块数量 f (1.." << (n - 1) << "): " << std::flush;
            if (!(std::cin >> fail_count))
            {
                std::cerr << "无效的 f，请输入 [1, " << (n - 1) << "] 之间的整数。" << std::endl;
                return false;
            }
        }
    }
    if (fail_count < 1 || fail_count >= n)
    {
        std::cerr << "无效的 f=" << fail_count << "，请输入 [1, " << (n - 1) << "] 之间的整数。"
                  << std::endl;
        return false;
    }

    if (!have_n)
    {
        if (non_interactive)
            trial_count = (env_t && env_t[0]) ? std::max(1, std::atoi(env_t)) : 1;
        else
        {
            std::cout << "请输入随机实验次数: " << std::flush;
            if (!(std::cin >> trial_count))
            {
                std::cerr << "无效的实验次数，请输入 >= 1 的整数。" << std::endl;
                return false;
            }
        }
    }
    if (trial_count < 1)
    {
        std::cerr << "无效的实验次数=" << trial_count << "，请输入 >= 1 的整数。" << std::endl;
        return false;
    }
    return true;
}

static void print_glrc_trial_metrics(const ECProject::GlrcMultiRecoveryMetrics &m, int k, int r, int z)
{
    std::cout << std::fixed << std::setprecision(6);
    std::cout << "  repair_mode:       " << m.repair_mode << std::endl;
    std::cout << "  equation_policy:   " << m.equation_policy << std::endl;
    std::cout << "  selected_equations:  ";
    for (size_t i = 0; i < m.selected_equations.size(); i++)
        std::cout << (i ? " " : "") << m.selected_equations[i];
    if (m.selected_equations.empty())
        std::cout << "(none)";
    std::cout << std::endl;
    std::cout << "  helper_blocks (" << m.helper_block_count << "):     ";
    for (size_t i = 0; i < m.helper_block_ids.size(); i++)
        std::cout << (i ? " " : "") << ECProject::glrc_block_label(m.helper_block_ids[i], k, r, z);
    if (m.helper_block_ids.empty())
        std::cout << "(none)";
    std::cout << std::endl;
    std::cout << "  --- timing (seconds) ---" << std::endl;
    std::cout << "  equation_select_time:  " << m.ilp_time
              << "  (ILP / local-then-global planner)" << std::endl;
    if (m.data_plane_time > 0.0)
    {
      std::cout << "  setup_time:              " << m.setup_time << "  (plan + listener readiness)" << std::endl;
      std::cout << "  data_plane_time:         " << m.data_plane_time
                << "  (stream + shard decode + writeback wall-clock)" << std::endl;
      std::cout << "  teardown_time:           " << m.teardown_time << std::endl;
      std::cout << "  client_wall_time:        " << m.client_wall_time << "  (entire coordinator RPC)" << std::endl;
    }
    else
      std::cout << "  network_transfer_time:   " << m.network_time << std::endl;
    std::cout << "  disk_read_time:          " << m.disk_read_time << "  (legacy per-proxy metric)" << std::endl;
    std::cout << "  decode_time:             " << m.decode_time << std::endl;
    std::cout << "  disk_write_time:         " << m.disk_write_time << "  (overlapping work total)" << std::endl;
    std::cout << "  repair_time:             " << m.total_time;
    if (m.repair_mode == "pipeline")
      std::cout << "  (data-plane wall-clock)";
    std::cout << std::endl;
    if (m.repair_mode == "hybrid")
    {
        std::cout << "  hybrid_p:                " << m.hybrid_p;
        if (m.hybrid_p_continuous > 0.0 || m.hybrid_p == 0)
            std::cout << "  (p*=" << m.hybrid_p_continuous << ")";
        std::cout << std::endl;
        const double phase2_other =
            std::max(0.0, m.hybrid_phase2_wall_time - m.hybrid_phase2_network_hot_time);
        const double pipeline_other =
            std::max(0.0, m.hybrid_pipeline_wall_time - m.hybrid_hub_egress_hot_time);
        const double theoretical_network_critical =
            std::max(m.hybrid_failed_node_hot_time, m.hybrid_hub_egress_hot_time);
        const double hotspot_residual =
            m.data_plane_time - theoretical_network_critical;
        std::cout << "  --- hybrid critical-path breakdown ---" << std::endl;
        std::cout << "  p_select_time:           " << m.hybrid_p_select_time << std::endl;
        std::cout << "  phase2_wall:             " << m.hybrid_phase2_wall_time << std::endl;
        std::cout << "    phase2_network_hot:    " << m.hybrid_phase2_network_hot_time << std::endl;
        std::cout << "    phase2_other_tail:     " << phase2_other << std::endl;
        std::cout << "  pipeline_wall:           " << m.hybrid_pipeline_wall_time << std::endl;
        std::cout << "    max_chain_node_egress_hot: " << m.hybrid_hub_egress_hot_time << std::endl;
        std::cout << "    pipeline_other_tail:   " << pipeline_other << std::endl;
        std::cout << "  pipeline_tail_ingress:   " << m.hybrid_pipeline_tail_ingress_time << std::endl;
        std::cout << "  failed_node_hot:         " << m.hybrid_failed_node_hot_time << std::endl;
        std::cout << "  atomic_publish:          " << m.hybrid_commit_time
                  << "  (per-block atomic rename after both ranges arrive)" << std::endl;
        std::cout << "  theoretical_network_critical: " << theoretical_network_critical
                  << "  (max(failed_node_hot, max_chain_node_egress_hot))" << std::endl;
        std::cout << "  hotspot_residual:        " << hotspot_residual
                  << "  (actual data_plane - theoretical network critical)" << std::endl;
    }
}

static int run_glrc_repair_test(ECProject::Client &client, const ECProject::Config *config,
                                int fail_count, int trial_count, bool have_f, bool have_n,
                                bool skip_warmup)
{
    const int k = config->k;
    const int r = config->r;
    const int z = config->z;
    const int n = k + r + z;
    const int stripe_num = env_int_or("GLRC_STRIPE_NUM", 1);

    std::cout << "[gLRC] config (n,k,r,z)=(" << n << "," << k << "," << r << "," << z << ")"
              << " mode=" << config->GlrcRepairMode
              << " policy=" << config->GlrcEquationPolicy << std::endl;

    if (!read_glrc_test_params(n, fail_count, trial_count, have_f, have_n))
        return 1;

    std::cout << "[gLRC] Writing " << stripe_num << " stripe(s)..." << std::endl;
    for (int i = 0; i < stripe_num; i++)
    {
        if (!client.set())
        {
            std::cerr << "[gLRC] set() failed at stripe index " << i << std::endl;
            return 1;
        }
    }

    std::vector<int> fixed_failed;
    const bool use_fixed = parse_failed_blocks_env(std::getenv("GLRC_FAILED_BLOCKS"), n, fixed_failed);
    if (use_fixed && (int)fixed_failed.size() != fail_count)
    {
        std::cerr << "[gLRC] GLRC_FAILED_BLOCKS count must match GLRC_FAIL_COUNT=" << fail_count << std::endl;
        return 1;
    }

    const bool warmup_enabled = [&]() {
        if (skip_warmup ||
            (config->GlrcRepairMode != "pipeline" && config->GlrcRepairMode != "phase2"))
            return false;
        if (const char *w = std::getenv("GLRC_WARMUP"))
            return w[0] != '0';
        return true;
    }();
    if (warmup_enabled)
    {
        std::vector<int> warmup_failed;
        if (config->GlrcRepairMode == "phase2")
        {
            // Exercise the same number of partition proxies, helper streams,
            // peer exchanges, and decode workers as the measured trials.
            if (use_fixed)
                warmup_failed = fixed_failed;
            else
            {
                warmup_failed.resize(static_cast<size_t>(fail_count));
                std::iota(warmup_failed.begin(), warmup_failed.end(), 0);
            }
        }
        else
        {
            warmup_failed = {use_fixed && !fixed_failed.empty() ? fixed_failed.front() : 0};
        }
        std::cout << "\n[gLRC] Warmup recovery (excluded from trial stats) failed_blocks: "
                  << ECProject::glrc_format_block_list(warmup_failed, k, r, z) << std::endl;
        ECProject::GlrcMultiRecoveryMetrics warmup_metrics;
        if (!client.multi_block_recovery_breakdown(0, warmup_failed, warmup_metrics))
        {
            std::cerr << "  Warmup FAILED: " << warmup_metrics.message << std::endl;
            return 1;
        }
        std::cout << "  warmup data_plane_time: " << warmup_metrics.network_time << " s" << std::endl;
    }

    std::cout << "\n[gLRC] Running " << trial_count << " random trial(s) with f=" << fail_count
              << " on stripe 0..." << std::endl;

    std::mt19937 rng(std::random_device{}());
    std::vector<int> all_blocks(n);
    std::iota(all_blocks.begin(), all_blocks.end(), 0);

    int success_count = 0;
    double sum_total = 0.0, sum_ilp = 0.0, sum_read = 0.0, sum_net = 0.0;
    double sum_decode = 0.0, sum_write = 0.0;
    double sum_helpers = 0.0;
    double sum_hybrid_p = 0.0, sum_hybrid_p_continuous = 0.0;
    int min_hybrid_p = -1, max_hybrid_p = -1;
    int hybrid_success_count = 0;
    std::vector<int> hybrid_p_samples;

    for (int t = 0; t < trial_count; t++)
    {
        std::vector<int> failed = use_fixed ? fixed_failed : all_blocks;
        if (!use_fixed)
        {
            std::shuffle(failed.begin(), failed.end(), rng);
            failed.resize(static_cast<size_t>(fail_count));
            std::sort(failed.begin(), failed.end());
        }

        ECProject::GlrcMultiRecoveryMetrics m;
        std::cout << "\n--- trial " << (t + 1) << "/" << trial_count
                  << " failed_blocks: " << ECProject::glrc_format_block_list(failed, k, r, z)
                  << std::endl;

        if (!client.multi_block_recovery_breakdown(0, failed, m))
        {
            std::cerr << "  FAILED: " << m.message << std::endl;
            continue;
        }

        success_count++;
        sum_total += m.total_time;
        sum_ilp += m.ilp_time;
        sum_read += m.disk_read_time;
        sum_net += m.network_time;
        sum_decode += m.decode_time;
        sum_write += m.disk_write_time;
        sum_helpers += m.helper_block_count;
        if (m.repair_mode == "hybrid")
        {
            hybrid_success_count++;
            sum_hybrid_p += m.hybrid_p;
            sum_hybrid_p_continuous += m.hybrid_p_continuous;
            hybrid_p_samples.push_back(m.hybrid_p);
            if (min_hybrid_p < 0 || m.hybrid_p < min_hybrid_p)
                min_hybrid_p = m.hybrid_p;
            if (max_hybrid_p < 0 || m.hybrid_p > max_hybrid_p)
                max_hybrid_p = m.hybrid_p;
        }

        print_glrc_trial_metrics(m, k, r, z);

        if (t + 1 < trial_count)
            std::this_thread::sleep_for(std::chrono::milliseconds(500));
    }

    std::cout << "\n========== gLRC repair batch summary ==========" << std::endl;
    std::cout << "  mode=" << config->GlrcRepairMode
              << " policy=" << config->GlrcEquationPolicy << std::endl;
    std::cout << "  f=" << fail_count << " trials=" << trial_count
              << " success=" << success_count << std::endl;
    if (success_count > 0)
    {
        const double cnt = static_cast<double>(success_count);
        std::cout << std::fixed << std::setprecision(6);
        std::cout << "  avg_equation_select_time:  " << (sum_ilp / cnt) << " s" << std::endl;
        std::cout << "  avg_disk_read_time:        " << (sum_read / cnt) << " s" << std::endl;
        std::cout << "  avg_network_transfer_time: " << (sum_net / cnt)
                  << " s  (data-plane wall-clock for pipeline)" << std::endl;
        std::cout << "  avg_decode_time:           " << (sum_decode / cnt) << " s" << std::endl;
        std::cout << "  avg_disk_write_time:       " << (sum_write / cnt) << " s" << std::endl;
        std::cout << "  avg_repair_time:           " << (sum_total / cnt) << " s";
        if (config->GlrcRepairMode == "pipeline")
          std::cout << "  (data-plane wall-clock)";
        std::cout << std::endl;
        std::cout << "  avg_helper_blocks:         " << (sum_helpers / cnt) << std::endl;
        if (config->GlrcRepairMode == "hybrid" && hybrid_success_count > 0)
        {
            const double hp_cnt = static_cast<double>(hybrid_success_count);
            std::cout << "  --- hybrid p stats (n=" << hybrid_success_count << ") ---" << std::endl;
            std::cout << "  avg_hybrid_p:              " << (sum_hybrid_p / hp_cnt) << std::endl;
            std::cout << "  avg_hybrid_p_continuous:   " << (sum_hybrid_p_continuous / hp_cnt) << std::endl;
            std::cout << "  min_hybrid_p:              " << min_hybrid_p << std::endl;
            std::cout << "  max_hybrid_p:              " << max_hybrid_p << std::endl;
            std::cout << "  hybrid_p_per_trial:        ";
            for (size_t i = 0; i < hybrid_p_samples.size(); i++)
                std::cout << (i ? ", " : "") << hybrid_p_samples[i];
            std::cout << std::endl;
        }
    }
    std::cout << "===============================================" << std::endl;

    return (success_count == trial_count) ? 0 : 1;
}

int main(int argc, char **argv)
{
    int cli_fail = 0, cli_trials = 0;
    bool have_f = false, have_n = false, show_help = false, skip_warmup = false;
    std::string coordinator_override;
    if (!parse_glrc_cli_args(argc, argv, cli_fail, cli_trials, have_f, have_n,
                             coordinator_override, show_help, skip_warmup))
    {
        print_glrc_client_usage(argc > 0 ? argv[0] : nullptr);
        return show_help ? 0 : 2;
    }

    const std::string sys_config_path =
        resolve_path_relative_to_executable(argc > 0 ? argv[0] : nullptr,
                                            "../../config/parameterConfiguration.xml");
    std::cout << "Config path: " << sys_config_path << std::endl;

    const ECProject::Config *config = ECProject::Config::getInstance(sys_config_path);

    std::string coordinator_addr = config->CoordinatorIP + ":" + std::to_string(config->CoordinatorPort);
    if (!coordinator_override.empty()) {
        coordinator_addr = coordinator_override;
        std::cout << "Using coordinator address (from argv): " << coordinator_addr << std::endl;
    } else {
        const char *env_addr = std::getenv("COORDINATOR_ADDR");
        if (env_addr && env_addr[0] != '\0') {
            coordinator_addr = env_addr;
            std::cout << "Using coordinator address (from COORDINATOR_ADDR): " << coordinator_addr << std::endl;
        }
    }

    std::string client_ip = config->ClientIP;
    int client_port = config->ClientPort;
    ECProject::Client client(client_ip, client_port, coordinator_addr, sys_config_path);
    std::cout << client.sayHelloToCoordinatorByGrpc("Client ID: " + client_ip + ":" + std::to_string(client_port)) << std::endl;

    if (config->CodeType == "gLRC")
        return run_glrc_repair_test(client, config, cli_fail, cli_trials, have_f, have_n, skip_warmup);

    std::vector<int> parameters = client.get_parameters();
    int k = parameters[0];
    int r = parameters[1];
    int z = parameters[2];
    std::string code_type;
    if(parameters[4] == 0){
        code_type = "AzureLRC";
    }
    else if(parameters[4] == 1){
        code_type = "OptimalLRC";
    }
    else if(parameters[4] == 2){
        code_type = "UniformLRC";
    }
    else if(parameters[4] == 3){
        code_type = "UniLRC";
    }
    else if(parameters[4] == 4){
        code_type = "RS";
    }
    else if(parameters[4] == 5){
        code_type = "gLRC";
    }
    else{
        std::cout << "Code type error" << std::endl;
        return -1;
    }
    double block_size = static_cast<double> (parameters[3]) / 1024 / 1024; //MB
    int n = k + r + z;


    
    int stripe_num = 1000; // 条带数量
    size_t total_write_size = static_cast<size_t>(stripe_num * block_size * n); // MB，用于计算 throughput
    std::cout << "Starting set stripe operation" << std::endl;
    std::chrono::high_resolution_clock::time_point set_start = std::chrono::high_resolution_clock::now();
    for(int i = 0; i < stripe_num; i++){
        client.set();
    }
    std::chrono::high_resolution_clock::time_point set_end = std::chrono::high_resolution_clock::now();
    std::cout << "Set stripe operation finished" << std::endl;
    std::cout << "Conducting experiments, please wait..." << std::endl;
    std::chrono::duration<double> set_time = std::chrono::duration_cast<std::chrono::duration<double>>(set_end - set_start);
    std::cout << "write throughput: " << (static_cast<double> (total_write_size) / set_time.count() / 1024) << "MB/s" << std::endl;

    std::cout << "\n[Merge bandwidth] if you want to limit the bandwidth during merge, please execute the following commands:\n"
            << "  before merge please execute: sh limit_all_intra10Gb_inter1Gb.sh\n"
            << "  after merge please execute: sh unlimit_all_proxy.sh\n\n";
     int merge_round=1;

    while (true)
    {
        if(merge_round>2)
        {
            std::cout<<"merge completed"<<std::endl;
            break;
        }
        std::cout << "start[ "<<merge_round<<" time]merge now? (Y/N)" << std::endl;
        char choose;
        std::cin >> choose;
        if (choose == 'Y' || choose == 'y')
        {      
            client.start_merge(merge_round);
            ++merge_round;
        }
        else if (choose == 'N' || choose == 'n')
        {
            break;
        }
        else
        {
            std::cout << "Invalid input, please enter Y or N." << std::endl;
        }
    }
    
    
    
    // std::string output_file_name = "test_" + code_type + "_" + std::to_string(k) + "_" + std::to_string(r) + "_" + std::to_string(z) + ".txt";
    // std::ofstream output_file(output_file_name);
    // if (!output_file.is_open())
    // {
    //     std::cerr << "Error opening file: " << output_file_name << std::endl;
    //     return 1;
    // }
    // freopen(output_file_name.c_str(), "w", stdout);
    // std::mt19937 rng(std::random_device{}());

    // std::uniform_int_distribution<int> dist_500(0, k*stripe_num - 500);
    // std::uniform_real_distribution<double> dist_double(0.0, 1.0);
    
    // std::string trace_file_path = std::string(buff) + cwf.substr(1, cwf.rfind('/') - 1) + "/../../../trace/ibm_test_trace.csv";
    // std::fstream trace_file(trace_file_path);
    // std::string trace_line;
    // while(std::getline(trace_file, trace_line)){
    //     std::string operation;
    //     int operation_size;
    //     std::istringstream iss(trace_line);
    //     std::getline(iss, operation, ',');
    //     iss >> operation_size;
    //     std::chrono::high_resolution_clock::time_point t1 = std::chrono::high_resolution_clock::now();
    //     if(operation == "GET"){
    //         int start_block_id = dist_500(rng);
    //         client.get_blocks(start_block_id, start_block_id + operation_size - 1);
    //     }
    //     else if(operation == "PUT"){
    //         client.sub_set(operation_size);
    //     }
        // else if(operation=="MERGE")
        // {
        //      // 1) 选择要读的“部分块”（数据块+校验块）
        //     auto ids = pick_some_data_and_parity_blocks(...);

        //      // 2) 读取这些块的数据
        //     auto blocks = client.get_blocks_by_ids(ids);   // 需要你在 Client 新增这个接口

        //      // 3) 合并算法
        //     auto merged = merge_algorithm(blocks);

        //      // 4) 写回（覆盖写 or 写到新块再切元数据）
        //     client.put_blocks_by_ids(target_ids, merged);  // 需要你在 Client 新增写接口
        // }
    //     else{
    //         std::cerr << "Unknown operation: " << operation << std::endl;
    //         return -1;
    //     }
    //     std::chrono::high_resolution_clock::time_point t2 = std::chrono::high_resolution_clock::now();
    //     std::chrono::duration<double> time_span = std::chrono::duration_cast<std::chrono::duration<double>>(t2 - t1);
    //     std::cout << operation << " operation time: " << time_span.count() << " seconds" << std::endl;
    // // }

    /*std::string trace_file_path = std::string(buff) + cwf.substr(1, cwf.rfind('/') - 1) + "/../../../trace/ycsb_final.txt";
    std::fstream trace_file(trace_file_path);
    std::string trace_line;
    while(std::getline(trace_file, trace_line)){
        std::string operation;
        std::istringstream iss(trace_line);
        std::getline(iss, operation, ' ');
        std::chrono::high_resolution_clock::time_point t1 = std::chrono::high_resolution_clock::now();
        if(operation == "R"){
            int block_id;
            iss >> block_id;
            client.get_blocks(block_id, block_id);
        }
        else if(operation == "U"){
            client.sub_set(1);
        }
        else{
            std::cerr << "Unknown operation: " << operation << std::endl;
            return -1;
        }
        std::chrono::high_resolution_clock::time_point t2 = std::chrono::high_resolution_clock::now();
        std::chrono::duration<double> time_span = std::chrono::duration_cast<std::chrono::duration<double>>(t2 - t1);
        std::cout << operation << " operation time: " << time_span.count() << " seconds" << std::endl;
    }*/

    
    //for read test
    // std::cout << "Normal read test start" << std::endl;
    // std::vector<std::chrono::duration<double>> read_time_spans;
    // for(int i = 0; i < 5; i++){
    //     size_t data_size;
    //     int id = i;
    //     std::string key = std::to_string(id);
    //     std::chrono::high_resolution_clock::time_point t1 = std::chrono::high_resolution_clock::now();
    //     std::shared_ptr<char[]> data = client.get(key, data_size);
    //     if(!data){
    //         std::cout << "Get operation failed" << std::endl;
    //         continue;
    //     }
    //     std::chrono::high_resolution_clock::time_point t2 = std::chrono::high_resolution_clock::now();
    //     std::chrono::duration<double> time_span = std::chrono::duration_cast<std::chrono::duration<double>>(t2 - t1);
    //     read_time_spans.push_back(time_span);
    //     //std::cout << "get time: " << time_span.count() << std::endl;
    // }
    // std::chrono::duration<double> read_total_time_span = std::accumulate(read_time_spans.begin(), read_time_spans.end(), std::chrono::duration<double>(0));
    // std::cout << "Total time: " << read_total_time_span.count() << std::endl;
    // std::cout << "Average time: " << read_total_time_span.count() / read_time_spans.size() << std::endl;
    // std::cout << "Throughput: " << read_time_spans.size() / read_total_time_span.count() << std::endl;
    // std::cout << "Speed" << static_cast<size_t>(block_size) * k / (read_total_time_span.count() / read_time_spans.size()) << "MB/s" << std::endl;
    // std::chrono::duration<double> read_max_time_span = *std::max_element(read_time_spans.begin(), read_time_spans.end());
    // std::chrono::duration<double> read_min_time_span = *std::min_element(read_time_spans.begin(), read_time_spans.end());
    // std::cout << "Max speed: " << static_cast<size_t>(block_size) * k / read_min_time_span.count() << "MB/s" << std::endl;
    // std::cout << "Min speed: " << static_cast<size_t>(block_size) * k / read_max_time_span.count() << "MB/s" << std::endl;
    // std::cout << "Normal read test end" << std::endl;
    // std::cout << std::endl;
    
    // //for degraded read test
    // std::vector<std::chrono::duration<double>> degraded_read_time_spans;
    // std::cout << "Degraded read test start" << std::endl;
    // for(int i = 0; i < k; i++){
    //     size_t data_size;
    //     int id = i;
    //     std::string key = std::to_string(id);
    //     std::chrono::high_resolution_clock::time_point t1 = std::chrono::high_resolution_clock::now();
    //     std::shared_ptr<char[]> data = client.get_degraded_read_block(0, i);
    //     if(!data){
    //         std::cout << "Degraded read operation failed" << std::endl;
    //         continue;
    //     }
    //     std::chrono::high_resolution_clock::time_point t2 = std::chrono::high_resolution_clock::now();
    //     std::chrono::duration<double> time_span = std::chrono::duration_cast<std::chrono::duration<double>>(t2 - t1);
    //     degraded_read_time_spans.push_back(time_span);
    //     //std::cout << "get time: " << time_span.count() << std::endl;
    // }
    // std::chrono::duration<double> degraded_read_total_time_span = std::accumulate(degraded_read_time_spans.begin(), degraded_read_time_spans.end(), std::chrono::duration<double>(0));
    // std::cout << "Average time: " << degraded_read_total_time_span.count() / degraded_read_time_spans.size() << std::endl;
    // std::chrono::duration<double> degraded_read_max_time_span = *std::max_element(degraded_read_time_spans.begin(), degraded_read_time_spans.end());
    // std::chrono::duration<double> degraded_read_min_time_span = *std::min_element(degraded_read_time_spans.begin(), degraded_read_time_spans.end());
    // std::cout << "Max time: "<< degraded_read_max_time_span.count() << std::endl;
    // std::cout << "Min time: "<< degraded_read_min_time_span.count() << std::endl;
    // std::cout << "Throughput: " << degraded_read_time_spans.size() / degraded_read_total_time_span.count() << std::endl;
    // std::cout << "Speed" << static_cast<size_t>(block_size)  / (degraded_read_total_time_span.count() / degraded_read_time_spans.size()) << "MB/s" << std::endl;
    // std::cout << "Max speed: " << static_cast<size_t>(block_size)  / degraded_read_min_time_span.count() << "MB/s" << std::endl;
    // std::cout << "Min speed: " << static_cast<size_t>(block_size)  / degraded_read_max_time_span.count() << "MB/s" << std::endl;
    // std::cout << "Degraded read test end" << std::endl;
    // std::cout << std::endl;
    
    //for single block recovery
    /*
    std::cout << "Single block recovery test start" << std::endl;
    std::vector<std::chrono::duration<double>> block_recovery_time_spans;
    for(int i = 0; i < n; i++){
        std::chrono::high_resolution_clock::time_point t1 = std::chrono::high_resolution_clock::now();
        client.recovery(0, i);
        std::chrono::high_resolution_clock::time_point t2 = std::chrono::high_resolution_clock::now();
        std::chrono::duration<double> time_span = std::chrono::duration_cast<std::chrono::duration<double>>(t2 - t1);
        block_recovery_time_spans.push_back(time_span);
        //std::cout << "single block repair time: " << time_span.count() << std::endl;
    }
    std::chrono::duration<double> block_recovery_total_time_span = std::accumulate(block_recovery_time_spans.begin(), block_recovery_time_spans.end(), std::chrono::duration<double>(0));
    std::chrono::duration<double> block_recovery_max_time_span = *std::max_element(block_recovery_time_spans.begin(), block_recovery_time_spans.end());
    std::chrono::duration<double> block_recovery_min_time_span = *std::min_element(block_recovery_time_spans.begin(), block_recovery_time_spans.end());
    //std::cout << "Total time: " << total_time_span.count() << std::endl;
    std::cout << "Average time: " << block_recovery_total_time_span.count() / block_recovery_time_spans.size() << std::endl;
    std::cout << "Max time: "<< block_recovery_max_time_span.count() << std::endl;
    std::cout << "Min time: "<< block_recovery_min_time_span.count() << std::endl;
    std::cout << "Single block recovery test end" << std::endl;
    std::cout << std::endl;
    */
    /*
    //for full node repair
    std::cout << "Full node repair test start" << std::endl;
    int node_num = 5;
    std::vector<int> node_ids;
    while(node_ids.size() < node_num){
        int random_id = rand() % (19 * 30);
        if(std::find(node_ids.begin(), node_ids.end(), random_id) == node_ids.end()){
            node_ids.push_back(random_id);
        }
    }

    std::vector<double> full_node_recovery_speeds;
    for(int i = 0; i < node_num; i++){
        std::chrono::high_resolution_clock::time_point t1 = std::chrono::high_resolution_clock::now();
        int block_num = client.recovery_full_node(node_ids[i]);
        std::chrono::high_resolution_clock::time_point t2 = std::chrono::high_resolution_clock::now();
        std::chrono::duration<double> time_span = std::chrono::duration_cast<std::chrono::duration<double>>(t2 - t1);
        //std::cout << "full node repair time: " << time_span.count() << std::endl;
        //std::cout << "block num: " << block_num << std::endl;
        double total_size = block_num * block_size; //MB
        //std::cout << "Speed: " << total_size / time_span.count() << "MB/s" << std::endl;
        full_node_recovery_speeds.push_back(total_size / time_span.count());
    }
    std::cout << "Average speed: " << std::accumulate(full_node_recovery_speeds.begin(), full_node_recovery_speeds.end(), 0.0) / full_node_recovery_speeds.size() << "MB/s" << std::endl;
    std::cout << "Max speed: " << *std::max_element(full_node_recovery_speeds.begin(), full_node_recovery_speeds.end()) << "MB/s" << std::endl;
    std::cout << "Min speed: " << *std::min_element(full_node_recovery_speeds.begin(), full_node_recovery_speeds.end()) << "MB/s" << std::endl;
    std::cout << "Full node repair test end" << std::endl;
    std::cout << std::endl;
    //for decode test
    std::cout << "Decode test start" << std::endl;
    std::vector<double> decode_time_spans;
    for(int i = 0; i < n; i++){
        double decode_time_span;
        client.decode_test(0, i, client_ip, client_port, decode_time_span);
        decode_time_spans.push_back(decode_time_span);
    }
    std::cout << "Average decode time: " << std::endl;
    std::cout << std::accumulate(decode_time_spans.begin(), decode_time_spans.end(), 0.0) / decode_time_spans.size() << std::endl;
    std::cout << "Decode test end" << std::endl;
    std::cout << std::endl;*/
    return 0;
}
