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
#include <cctype>
#include <random>
#include <numeric>
#include <sstream>
#include <cstdint>
#include <limits>
#include <stdexcept>
#include <thread>
#include <unordered_set>
#include <ctime>
#include <cerrno>
#include "unilrc_encoder.h"
#include "glrc_repair_ilp.h"
#include "runtime_paths.h"

namespace {

class TeeStreambuf : public std::streambuf
{
public:
  TeeStreambuf(std::streambuf *primary, std::streambuf *secondary)
      : primary_(primary), secondary_(secondary) {}

protected:
  int overflow(int ch) override
  {
    if (ch == traits_type::eof())
      return traits_type::not_eof(ch);
    const auto c = static_cast<char>(ch);
    const int r1 = primary_ ? primary_->sputc(c) : traits_type::eof();
    const int r2 = secondary_ ? secondary_->sputc(c) : c;
    return (r1 == traits_type::eof() || r2 == traits_type::eof()) ? traits_type::eof() : ch;
  }

  std::streamsize xsputn(const char *s, std::streamsize n) override
  {
    const std::streamsize n1 = primary_ ? primary_->sputn(s, n) : n;
    const std::streamsize n2 = secondary_ ? secondary_->sputn(s, n) : n;
    return std::min(n1, n2);
  }

  int sync() override
  {
    const int r1 = primary_ ? primary_->pubsync() : 0;
    const int r2 = secondary_ ? secondary_->pubsync() : 0;
    return (r1 == 0 && r2 == 0) ? 0 : -1;
  }

private:
  std::streambuf *primary_;
  std::streambuf *secondary_;
};

struct ExperimentLogger
{
  std::ofstream full_log;
  std::ofstream summary_csv;
  std::string full_log_path;
  std::string summary_csv_path;
  std::streambuf *old_cout = nullptr;
  std::streambuf *old_cerr = nullptr;
  TeeStreambuf *tee_out = nullptr;
  TeeStreambuf *tee_err = nullptr;

  bool start(const std::string &prefix)
  {
    if (const char *disabled = std::getenv("GLRC_SAVE_LOG"))
    {
      if (disabled[0] == '0')
        return false;
    }

    std::string log_dir;
    if (const char *env_dir = std::getenv("DDRT_CLIENT_LOG_DIR"))
      log_dir = env_dir;
    else
      log_dir = resolve_path_relative_to_executable(nullptr, "../../logs/client_runs");

    if (mkdir(log_dir.c_str(), 0755) != 0 && errno != EEXIST)
    {
      std::cerr << "[gLRC] warning: cannot create log dir " << log_dir << std::endl;
      return false;
    }

    const auto now = std::chrono::system_clock::now();
    const std::time_t tt = std::chrono::system_clock::to_time_t(now);
    std::tm tm{};
    localtime_r(&tt, &tm);
    char stamp[32];
    std::strftime(stamp, sizeof(stamp), "%Y%m%d_%H%M%S", &tm);

    full_log_path = log_dir + "/" + prefix + "_" + stamp + ".log";
    summary_csv_path = log_dir + "/" + prefix + "_" + stamp + "_summary.csv";
    const std::string latest_log = log_dir + "/latest_" + prefix + ".log";
    const std::string latest_csv = log_dir + "/latest_" + prefix + "_summary.csv";

    full_log.open(full_log_path, std::ios::out | std::ios::trunc);
    summary_csv.open(summary_csv_path, std::ios::out | std::ios::trunc);
    if (!full_log.is_open())
    {
      std::cerr << "[gLRC] warning: cannot open log file " << full_log_path << std::endl;
      return false;
    }

    old_cout = std::cout.rdbuf();
    old_cerr = std::cerr.rdbuf();
    tee_out = new TeeStreambuf(old_cout, full_log.rdbuf());
    tee_err = new TeeStreambuf(old_cerr, full_log.rdbuf());
    std::cout.rdbuf(tee_out);
    std::cerr.rdbuf(tee_err);

    std::cout << "[gLRC] saving full log: " << full_log_path << std::endl;
    if (summary_csv.is_open())
      std::cout << "[gLRC] saving summary csv: " << summary_csv_path << std::endl;
    latest_log_path_ = latest_log;
    latest_csv_path_ = latest_csv;
    return true;
  }

  void stop()
  {
    std::cout.flush();
    std::cerr.flush();
    if (old_cout)
      std::cout.rdbuf(old_cout);
    if (old_cerr)
      std::cerr.rdbuf(old_cerr);
    old_cout = nullptr;
    old_cerr = nullptr;
    delete tee_out;
    delete tee_err;
    tee_out = nullptr;
    tee_err = nullptr;
    if (full_log.is_open())
    {
      full_log.flush();
      full_log.close();
      // Stable names for run_client.sh to fetch after the run.
      if (!latest_log_path_.empty())
      {
        std::ifstream src(full_log_path, std::ios::binary);
        std::ofstream dst(latest_log_path_, std::ios::binary | std::ios::trunc);
        dst << src.rdbuf();
      }
      std::cout << "[gLRC] full log saved: " << full_log_path << std::endl;
    }
    if (summary_csv.is_open())
    {
      summary_csv.flush();
      summary_csv.close();
      if (!latest_csv_path_.empty())
      {
        std::ifstream src(summary_csv_path, std::ios::binary);
        std::ofstream dst(latest_csv_path_, std::ios::binary | std::ios::trunc);
        dst << src.rdbuf();
      }
      std::cout << "[gLRC] summary csv saved: " << summary_csv_path << std::endl;
    }
  }

  ~ExperimentLogger() { stop(); }

private:
  std::string latest_log_path_;
  std::string latest_csv_path_;
};

} // namespace

static int env_int_or(const char *name, int default_value)
{
    const char *v = std::getenv(name);
    if (!v || v[0] == '\0')
        return default_value;
    return std::max(1, std::atoi(v));
}

static bool parse_failed_blocks(const std::string &raw, int k, int r, int z,
                                std::vector<int> &out, std::string &error)
{
    out.clear();
    std::stringstream ss(raw);
    std::string token;
    while (std::getline(ss, token, ','))
    {
        token.erase(token.begin(), std::find_if(token.begin(), token.end(),
                                               [](unsigned char c) { return !std::isspace(c); }));
        token.erase(std::find_if(token.rbegin(), token.rend(),
                                [](unsigned char c) { return !std::isspace(c); }).base(),
                    token.end());
        if (token.empty())
            continue;
        int id = -1;
        try
        {
            size_t consumed = 0;
            if (std::isalpha(static_cast<unsigned char>(token[0])))
            {
                const char kind = static_cast<char>(std::toupper(static_cast<unsigned char>(token[0])));
                const int index = std::stoi(token.substr(1), &consumed);
                if (consumed != token.size() - 1 || index < 0)
                    throw std::invalid_argument("bad block label");
                if (kind == 'D' && index < k)
                    id = index;
                else if (kind == 'G' && index < r)
                    id = k + index;
                else if (kind == 'L' && index < z)
                    id = k + r + index;
            }
            else
            {
                id = std::stoi(token, &consumed);
                if (consumed != token.size())
                    throw std::invalid_argument("bad block id");
            }
        }
        catch (const std::exception &)
        {
            error = "invalid failed block: " + token;
            return false;
        }
        if (id < 0 || id >= k + r + z)
        {
            error = "failed block out of range: " + token;
            return false;
        }
        out.push_back(id);
    }
    std::sort(out.begin(), out.end());
    const size_t original_size = out.size();
    out.erase(std::unique(out.begin(), out.end()), out.end());
    if (out.empty())
    {
        error = "failed block list is empty";
        return false;
    }
    if (out.size() != original_size)
    {
        error = "failed block list contains duplicates";
        return false;
    }
    return true;
}

static void print_glrc_client_usage(const char *argv0)
{
    std::cerr
        << "Usage: " << (argv0 ? argv0 : "main_client")
        << " [options] [coordinator_ip:port]\n"
        << "  -f, --fail-count N   number of failed blocks per trial (1..n-1)\n"
        << "  -n, --trials N       number of random trials (>=1)\n"
        << "  --failed-blocks LIST fixed failures, e.g. D0,D7,G1 or 0,7,25\n"
        << "  --seed N             deterministic random seed (uint32)\n"
        << "  --failure-mode MODE  random (default) | max\n"
        << "  --max-failure        shorthand for --failure-mode max\n"
        << "  --no-warmup          skip pre-trial pipeline warmup recovery\n"
        << "  --node-repair        multi-stripe repair for whole failed nodes\n"
        << "  --failed-nodes LIST  node ids, e.g. 2,15 (implies --node-repair)\n"
        << "  --failed-node-ips LIST  node IPs, e.g. 172.16.3.66,172.16.3.65\n"
        << "  --reuse-stripes      node-repair: skip write; repair last GLRC_STRIPE_NUM stripes\n"
        << "  -n also repeats node-repair trials with 500ms isolation gap\n"
        << "  -h, --help           show this help\n"
        << "\n"
        << "If -f/-n are omitted: prompt interactively when stdin is a TTY;\n"
        << "otherwise use env GLRC_FAIL_COUNT / GLRC_TRIALS (default 1/1).\n"
        << "Stripe count: GLRC_STRIPE_NUM (default 1).\n"
        << "Equivalent env vars: GLRC_FAILED_BLOCKS, GLRC_RANDOM_SEED,\n"
        << "GLRC_FAILED_NODES, GLRC_FAILED_NODE_IPS, GLRC_REUSE_STRIPES=1.\n"
        << "Coordinator may also be set via COORDINATOR_ADDR.\n";
}

static bool parse_int_list(const std::string &raw, std::vector<int> &out, std::string &error)
{
    out.clear();
    std::stringstream ss(raw);
    std::string token;
    while (std::getline(ss, token, ','))
    {
        token.erase(token.begin(), std::find_if(token.begin(), token.end(),
                                               [](unsigned char c) { return !std::isspace(c); }));
        token.erase(std::find_if(token.rbegin(), token.rend(),
                                [](unsigned char c) { return !std::isspace(c); }).base(),
                    token.end());
        if (token.empty())
            continue;
        try
        {
            size_t consumed = 0;
            const int value = std::stoi(token, &consumed);
            if (consumed != token.size() || value < 0)
                throw std::invalid_argument("bad int");
            out.push_back(value);
        }
        catch (const std::exception &)
        {
            error = "invalid integer list token: " + token;
            return false;
        }
    }
    if (out.empty())
    {
        error = "integer list is empty";
        return false;
    }
    return true;
}

static bool parse_string_list(const std::string &raw, std::vector<std::string> &out, std::string &error)
{
    out.clear();
    std::stringstream ss(raw);
    std::string token;
    while (std::getline(ss, token, ','))
    {
        token.erase(token.begin(), std::find_if(token.begin(), token.end(),
                                               [](unsigned char c) { return !std::isspace(c); }));
        token.erase(std::find_if(token.rbegin(), token.rend(),
                                [](unsigned char c) { return !std::isspace(c); }).base(),
                    token.end());
        if (token.empty())
            continue;
        out.push_back(token);
    }
    if (out.empty())
    {
        error = "string list is empty";
        return false;
    }
    return true;
}

/** Returns false on parse error / --help. Sets *show_help if help was requested. */
static bool parse_glrc_cli_args(int argc, char **argv, int &fail_count, int &trial_count,
                                bool &have_f, bool &have_n, std::string &coordinator_override,
                                bool &show_help, bool &skip_warmup, std::string &failure_mode,
                                std::string &fixed_failed_raw, std::string &seed_raw,
                                bool &node_repair, std::string &failed_nodes_raw,
                                std::string &failed_node_ips_raw, bool &reuse_stripes)
{
    have_f = false;
    have_n = false;
    show_help = false;
    skip_warmup = false;
    node_repair = false;
    reuse_stripes = false;
    fail_count = 0;
    trial_count = 0;
    coordinator_override.clear();
    failure_mode.clear();
    fixed_failed_raw.clear();
    seed_raw.clear();
    failed_nodes_raw.clear();
    failed_node_ips_raw.clear();

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
        if (arg == "--failed-blocks")
        {
            const char *v = need_value(arg.c_str());
            if (!v)
                return false;
            fixed_failed_raw = v;
            continue;
        }
        if (arg == "--seed")
        {
            const char *v = need_value(arg.c_str());
            if (!v)
                return false;
            seed_raw = v;
            continue;
        }
        if (arg == "--max-failure")
        {
            failure_mode = "max";
            continue;
        }
        if (arg == "--failure-mode")
        {
            const char *v = need_value(arg.c_str());
            if (!v)
                return false;
            failure_mode = v;
            continue;
        }
        if (arg == "--node-repair")
        {
            node_repair = true;
            continue;
        }
        if (arg == "--reuse-stripes")
        {
            reuse_stripes = true;
            node_repair = true;
            continue;
        }
        if (arg == "--failed-nodes")
        {
            const char *v = need_value(arg.c_str());
            if (!v)
                return false;
            failed_nodes_raw = v;
            node_repair = true;
            continue;
        }
        if (arg == "--failed-node-ips")
        {
            const char *v = need_value(arg.c_str());
            if (!v)
                return false;
            failed_node_ips_raw = v;
            node_repair = true;
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
            std::cout << "Enter failure count f (1.." << (n - 1) << "): " << std::flush;
            if (!(std::cin >> fail_count))
            {
                std::cerr << "Invalid f; enter an integer in [1, " << (n - 1) << "]." << std::endl;
                return false;
            }
        }
    }
    if (fail_count < 1 || fail_count >= n)
    {
        std::cerr << "Invalid f=" << fail_count << "; enter an integer in [1, " << (n - 1) << "]."
                  << std::endl;
        return false;
    }

    if (!have_n)
    {
        if (non_interactive)
            trial_count = (env_t && env_t[0]) ? std::max(1, std::atoi(env_t)) : 1;
        else
        {
            std::cout << "Enter number of random trials: " << std::flush;
            if (!(std::cin >> trial_count))
            {
                std::cerr << "Invalid trial count; enter an integer >= 1." << std::endl;
                return false;
            }
        }
    }
    if (trial_count < 1)
    {
        std::cerr << "Invalid trial count=" << trial_count << "; enter an integer >= 1." << std::endl;
        return false;
    }
    return true;
}

static std::string normalize_failure_mode(std::string mode)
{
    std::transform(mode.begin(), mode.end(), mode.begin(),
                   [](unsigned char c) { return static_cast<char>(std::tolower(c)); });
    if (mode.empty())
        return "random";
    if (mode == "maximum" || mode == "max-tolerance")
        return "max";
    return mode;
}

static bool generate_max_failure_pattern(int k, int r, int z, std::mt19937 &rng,
                                         std::vector<int> &failed, std::string &error)
{
    const int n = k + r + z;
    std::vector<std::vector<int>> groups;
    ECProject::glrc_build_placement_groups(k, r, z, groups);
    if (static_cast<int>(groups.size()) != z)
    {
        error = "placement group count does not match z";
        return false;
    }

    constexpr int kMaxAttempts = 1000;
    for (int attempt = 0; attempt < kMaxAttempts; ++attempt)
    {
        failed.clear();
        std::vector<char> selected(static_cast<size_t>(n), 0);

        // First lose one data block from every local placement group.
        bool valid_groups = true;
        for (const auto &group : groups)
        {
            std::vector<int> data_candidates;
            for (int block_id : group)
                if (block_id >= 0 && block_id < k)
                    data_candidates.push_back(block_id);
            if (data_candidates.empty())
            {
                valid_groups = false;
                break;
            }
            std::uniform_int_distribution<size_t> pick(0, data_candidates.size() - 1);
            const int block_id = data_candidates[pick(rng)];
            failed.push_back(block_id);
            selected[static_cast<size_t>(block_id)] = 1;
        }
        if (!valid_groups)
        {
            error = "a placement group contains no data block";
            return false;
        }

        // Then lose r additional random blocks from everything that remains.
        std::vector<int> remaining;
        for (int block_id = 0; block_id < n; ++block_id)
            if (!selected[static_cast<size_t>(block_id)])
                remaining.push_back(block_id);
        if (static_cast<int>(remaining.size()) < r)
        {
            error = "not enough remaining blocks for global-parity failures";
            return false;
        }
        std::shuffle(remaining.begin(), remaining.end(), rng);
        failed.insert(failed.end(), remaining.begin(), remaining.begin() + r);
        std::sort(failed.begin(), failed.end());

        // Maximum-count erasure patterns are not all necessarily decodable.
        // Keep the requested distribution but resample until all z+r equations
        // form a valid full-rank recovery system.
        ECProject::GlrcIlpRepairPlan plan;
        if (ECProject::glrc_solve_repair_plan(k, r, z, failed, "ilp-min-helper", plan) &&
            static_cast<int>(plan.selected_equations.size()) == z + r)
            return true;
    }

    error = "could not sample a decodable maximum-tolerance pattern in 1000 attempts";
    return false;
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
    std::cout << "  network_transfer_time:   " << m.network_time;
    if (m.repair_mode == "phase2")
      std::cout << "  (max helper-stream span across partitions)";
    else if (m.repair_mode == "pipeline")
      std::cout << "  (data-plane wall-clock)";
    std::cout << std::endl;
    if (m.repair_mode == "phase2")
      std::cout << "  phase2_stream_wall_time: " << m.phase2_stream_wall_time
                << "  (helper + peer exchange + write join, max partition)" << std::endl;
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

static int run_glrc_node_repair_test(ECProject::Client &client, const ECProject::Config *config,
                                     const std::string &failed_nodes_raw,
                                     const std::string &failed_node_ips_raw,
                                     int trial_count, bool have_n, bool reuse_stripes)
{
    const int k = config->k;
    const int r = config->r;
    const int z = config->z;
    const int n = k + r + z;
    const int stripe_num = env_int_or("GLRC_STRIPE_NUM", 1);
    const size_t block_bytes = static_cast<size_t>(config->BlockSize);

    if (!reuse_stripes)
    {
        if (const char *env = std::getenv("GLRC_REUSE_STRIPES"))
        {
            if (env[0] == '1' || env[0] == 'y' || env[0] == 'Y' || env[0] == 't' || env[0] == 'T')
                reuse_stripes = true;
        }
    }

    if (!have_n)
    {
        const char *env_t = std::getenv("GLRC_TRIALS");
        trial_count = (env_t && env_t[0]) ? std::max(1, std::atoi(env_t)) : 1;
    }
    if (trial_count < 1)
    {
        std::cerr << "[gLRC] invalid trial count: " << trial_count << std::endl;
        return 1;
    }

    std::string nodes_raw = failed_nodes_raw;
    std::string ips_raw = failed_node_ips_raw;
    if (nodes_raw.empty())
    {
        if (const char *env = std::getenv("GLRC_FAILED_NODES"))
            nodes_raw = env;
    }
    if (ips_raw.empty())
    {
        if (const char *env = std::getenv("GLRC_FAILED_NODE_IPS"))
            ips_raw = env;
    }

    std::vector<int> node_ids;
    std::vector<std::string> node_ips;
    std::string parse_error;
    if (!nodes_raw.empty() && !parse_int_list(nodes_raw, node_ids, parse_error))
    {
        std::cerr << "[gLRC] invalid --failed-nodes: " << parse_error << std::endl;
        return 1;
    }
    if (!ips_raw.empty() && !parse_string_list(ips_raw, node_ips, parse_error))
    {
        std::cerr << "[gLRC] invalid --failed-node-ips: " << parse_error << std::endl;
        return 1;
    }
    if (node_ids.empty() && node_ips.empty())
    {
        std::cerr << "[gLRC] --node-repair requires --failed-nodes and/or --failed-node-ips"
                  << std::endl;
        return 1;
    }

    ExperimentLogger logger;
    logger.start("glrc_node_repair");
    if (logger.summary_csv.is_open())
    {
        logger.summary_csv
            << "trial,success,stripes_repaired,stripes_total,failed_blocks,"
            << "batch_wall_time_s,avg_stripe_repair_time_s,avg_stripe_data_plane_s,"
            << "sum_stripe_repair_time_s,recovered_mib,throughput_mib_s,"
            << "repair_mode,equation_policy,failed_nodes\n";
    }

    std::cout << "[gLRC] node-repair mode (n,k,r,z)=(" << n << "," << k << "," << r << "," << z
              << ") mode=" << config->GlrcRepairMode
              << " policy=" << config->GlrcEquationPolicy
              << " stripes=" << stripe_num
              << " trials=" << trial_count
              << (reuse_stripes ? " reuse_stripes=1" : " reuse_stripes=0") << std::endl;

    std::unordered_set<int> target_stripe_set;
    if (reuse_stripes)
    {
        std::vector<int> existing;
        std::string list_error;
        if (!client.list_stripe_ids(existing, list_error))
        {
            std::cerr << "[gLRC] list_stripe_ids failed: " << list_error << std::endl;
            return 1;
        }
        if (existing.empty())
        {
            std::cerr << "[gLRC] --reuse-stripes requires existing stripes on coordinator"
                      << std::endl;
            return 1;
        }
        std::sort(existing.begin(), existing.end());
        const int take = std::min(stripe_num, static_cast<int>(existing.size()));
        for (int i = static_cast<int>(existing.size()) - take; i < static_cast<int>(existing.size());
             ++i)
            target_stripe_set.insert(existing[static_cast<size_t>(i)]);
        std::vector<int> selected(target_stripe_set.begin(), target_stripe_set.end());
        std::sort(selected.begin(), selected.end());
        std::cout << "[gLRC] reuse-stripes: coordinator has " << existing.size()
                  << " stripe(s); repairing last " << take << " by id:";
        for (int sid : selected)
            std::cout << " " << sid;
        std::cout << std::endl;
        if (take < stripe_num)
        {
            std::cout << "[gLRC] warning: requested GLRC_STRIPE_NUM=" << stripe_num
                      << " but only " << existing.size() << " stripe(s) exist" << std::endl;
        }
    }
    else
    {
        // Coordinator keeps stripes across client runs. Only repair stripes created
        // in this experiment, not leftovers from a previous node-repair / single-stripe run.
        std::vector<int> stripes_before;
        std::string list_error;
        if (!client.list_stripe_ids(stripes_before, list_error))
        {
            std::cerr << "[gLRC] list_stripe_ids failed before write: " << list_error << std::endl;
            return 1;
        }
        if (!stripes_before.empty())
        {
            std::cout << "[gLRC] warning: coordinator already has " << stripes_before.size()
                      << " stripe(s); this run will only repair newly written stripes"
                      << std::endl;
        }
        std::unordered_set<int> before_set(stripes_before.begin(), stripes_before.end());

        std::cout << "[gLRC] Writing " << stripe_num << " stripe(s)..." << std::endl;
        for (int i = 0; i < stripe_num; i++)
        {
            if (!client.set())
            {
                std::cerr << "[gLRC] set() failed at stripe index " << i << std::endl;
                return 1;
            }
        }

        std::vector<int> stripes_after;
        if (!client.list_stripe_ids(stripes_after, list_error))
        {
            std::cerr << "[gLRC] list_stripe_ids failed after write: " << list_error << std::endl;
            return 1;
        }
        for (int sid : stripes_after)
        {
            if (!before_set.count(sid))
                target_stripe_set.insert(sid);
        }
    }

    // Freeze the failure pattern once, matching single-stripe fixed-block trials:
    // each trial re-repairs the same logical blocks (writeback stays on original nodes).
    std::vector<int> resolved_node_ids;
    std::vector<std::string> resolved_node_ips;
    std::map<int, std::vector<int>> stripe_to_failed;
    std::string query_error;
    if (!client.get_blocks_on_nodes(node_ids, node_ips, resolved_node_ids, resolved_node_ips,
                                    stripe_to_failed, query_error))
    {
        std::cerr << "[gLRC] get_blocks_on_nodes failed: " << query_error << std::endl;
        return 1;
    }

    for (auto it = stripe_to_failed.begin(); it != stripe_to_failed.end();)
    {
        if (!target_stripe_set.count(it->first))
            it = stripe_to_failed.erase(it);
        else
            ++it;
    }

    std::cout << "[gLRC] Failed nodes:";
    for (size_t i = 0; i < resolved_node_ids.size(); ++i)
    {
        std::cout << " " << resolved_node_ids[i];
        if (i < resolved_node_ips.size())
            std::cout << "(" << resolved_node_ips[i] << ")";
    }
    std::cout << std::endl;

    if (stripe_to_failed.empty())
    {
        std::cerr << "[gLRC] no target-stripe blocks found on the selected nodes" << std::endl;
        return 1;
    }

    std::vector<int> stripe_ids;
    stripe_ids.reserve(stripe_to_failed.size());
    for (const auto &kv : stripe_to_failed)
        stripe_ids.push_back(kv.first);
    std::sort(stripe_ids.begin(), stripe_ids.end());
    if (static_cast<int>(stripe_ids.size()) != static_cast<int>(target_stripe_set.size()))
    {
        std::cout << "[gLRC] warning: expected " << target_stripe_set.size()
                  << " target stripe(s) on failed nodes, found " << stripe_ids.size()
                  << std::endl;
    }

    int total_failed_blocks = 0;
    for (int stripe_id : stripe_ids)
        total_failed_blocks += static_cast<int>(stripe_to_failed[stripe_id].size());

    std::cout << "[gLRC] Will repair " << stripe_ids.size() << " stripe(s), "
              << total_failed_blocks << " failed block(s) in stripe order, "
              << trial_count << " trial(s)" << std::endl;
    for (int stripe_id : stripe_ids)
    {
        std::cout << "  stripe " << stripe_id << ": "
                  << ECProject::glrc_format_block_list(stripe_to_failed[stripe_id], k, r, z)
                  << std::endl;
    }

    const double recovered_mib =
        static_cast<double>(total_failed_blocks) * static_cast<double>(block_bytes) / (1024.0 * 1024.0);

    int full_success_trials = 0;
    double sum_batch_wall = 0.0;
    double sum_avg_stripe_repair = 0.0;
    double sum_throughput = 0.0;

    for (int t = 0; t < trial_count; ++t)
    {
        std::cout << "\n########## node-repair trial " << (t + 1) << "/" << trial_count
                  << " ##########" << std::endl;

        int success_count = 0;
        double sum_repair = 0.0;
        double sum_data_plane = 0.0;
        const auto batch_start = std::chrono::steady_clock::now();

        for (size_t i = 0; i < stripe_ids.size(); ++i)
        {
            const int stripe_id = stripe_ids[i];
            const std::vector<int> &failed = stripe_to_failed[stripe_id];
            ECProject::GlrcMultiRecoveryMetrics m;
            std::cout << "\n--- trial " << (t + 1) << "/" << trial_count
                      << " stripe " << (i + 1) << "/" << stripe_ids.size()
                      << " id=" << stripe_id
                      << " failed_blocks: " << ECProject::glrc_format_block_list(failed, k, r, z)
                      << std::endl;
            if (!client.multi_block_recovery_breakdown(stripe_id, failed, m))
            {
                std::cerr << "  FAILED: " << m.message << std::endl;
                continue;
            }
            success_count++;
            sum_repair += m.total_time;
            sum_data_plane += (m.data_plane_time > 0.0 ? m.data_plane_time : m.network_time);
            print_glrc_trial_metrics(m, k, r, z);
        }

        const double batch_wall =
            std::chrono::duration<double>(std::chrono::steady_clock::now() - batch_start).count();
        const bool trial_ok = (success_count == static_cast<int>(stripe_ids.size()));
        if (trial_ok)
            full_success_trials++;

        std::cout << "\n========== gLRC node-repair trial " << (t + 1) << "/" << trial_count
                  << " summary ==========" << std::endl;
        std::cout << "  mode=" << config->GlrcRepairMode
                  << " policy=" << config->GlrcEquationPolicy << std::endl;
        std::cout << "  stripes_written=" << stripe_num
                  << " stripes_repaired=" << success_count << "/" << stripe_ids.size()
                  << " failed_blocks=" << total_failed_blocks << std::endl;
        std::cout << std::fixed << std::setprecision(6);
        std::cout << "  batch_wall_time:          " << batch_wall << " s" << std::endl;
        if (success_count > 0)
        {
            const double cnt = static_cast<double>(success_count);
            std::cout << "  avg_stripe_repair_time:   " << (sum_repair / cnt) << " s" << std::endl;
            std::cout << "  avg_stripe_data_plane:    " << (sum_data_plane / cnt) << " s" << std::endl;
            std::cout << "  sum_stripe_repair_time:   " << sum_repair << " s" << std::endl;
            if (trial_ok)
            {
                sum_batch_wall += batch_wall;
                sum_avg_stripe_repair += (sum_repair / cnt);
                if (batch_wall > 0.0)
                    sum_throughput += (recovered_mib / batch_wall);
            }
        }
        std::cout << "  recovered_data:           " << recovered_mib << " MiB" << std::endl;
        const double throughput =
            (batch_wall > 0.0) ? (recovered_mib / batch_wall) : 0.0;
        if (batch_wall > 0.0)
            std::cout << "  throughput:               " << throughput << " MiB/s" << std::endl;
        std::cout << "================================================" << std::endl;

        if (logger.summary_csv.is_open())
        {
            const double avg_repair =
                (success_count > 0) ? (sum_repair / static_cast<double>(success_count)) : 0.0;
            const double avg_dp =
                (success_count > 0) ? (sum_data_plane / static_cast<double>(success_count)) : 0.0;
            std::ostringstream nodes_ss;
            for (size_t ni = 0; ni < resolved_node_ids.size(); ++ni)
            {
                if (ni)
                    nodes_ss << ";";
                nodes_ss << resolved_node_ids[ni];
                if (ni < resolved_node_ips.size())
                    nodes_ss << "(" << resolved_node_ips[ni] << ")";
            }
            logger.summary_csv << std::fixed << std::setprecision(6)
                               << (t + 1) << "," << (trial_ok ? 1 : 0) << ","
                               << success_count << "," << stripe_ids.size() << ","
                               << total_failed_blocks << ","
                               << batch_wall << "," << avg_repair << "," << avg_dp << ","
                               << sum_repair << "," << recovered_mib << "," << throughput << ","
                               << config->GlrcRepairMode << "," << config->GlrcEquationPolicy << ","
                               << nodes_ss.str() << "\n";
            logger.summary_csv.flush();
        }

        // Same inter-trial gap as single-stripe experiments: let limiter / sockets settle.
        if (t + 1 < trial_count)
            std::this_thread::sleep_for(std::chrono::milliseconds(500));
    }

    if (trial_count > 1)
    {
        std::cout << "\n========== gLRC node-repair batch summary ==========" << std::endl;
        std::cout << "  trials=" << trial_count
                  << " full_success=" << full_success_trials << std::endl;
        if (full_success_trials > 0)
        {
            const double cnt = static_cast<double>(full_success_trials);
            std::cout << std::fixed << std::setprecision(6);
            std::cout << "  avg_batch_wall_time:       " << (sum_batch_wall / cnt) << " s"
                      << std::endl;
            std::cout << "  avg_avg_stripe_repair:     " << (sum_avg_stripe_repair / cnt) << " s"
                      << std::endl;
            std::cout << "  avg_throughput:            " << (sum_throughput / cnt) << " MiB/s"
                      << std::endl;
        }
        std::cout << "====================================================" << std::endl;
    }

    logger.stop();
    return (full_success_trials == trial_count) ? 0 : 1;
}

static int run_glrc_repair_test(ECProject::Client &client, const ECProject::Config *config,
                                int fail_count, int trial_count, bool have_f, bool have_n,
                                bool skip_warmup, const std::string &requested_failure_mode,
                                const std::string &cli_fixed_failed_raw,
                                const std::string &cli_seed_raw)
{
    const int k = config->k;
    const int r = config->r;
    const int z = config->z;
    const int n = k + r + z;
    const int stripe_num = env_int_or("GLRC_STRIPE_NUM", 1);
    const std::string failure_mode = normalize_failure_mode(requested_failure_mode);
    if (failure_mode != "random" && failure_mode != "max")
    {
        std::cerr << "Invalid failure mode: " << failure_mode << " (expected random or max)" << std::endl;
        return 1;
    }
    if (failure_mode == "max")
    {
        fail_count = z + r;
        have_f = true;
    }

    const char *env_fixed = std::getenv("GLRC_FAILED_BLOCKS");
    const std::string fixed_failed_raw =
        !cli_fixed_failed_raw.empty() ? cli_fixed_failed_raw
                                     : ((env_fixed && env_fixed[0]) ? env_fixed : "");
    std::vector<int> fixed_failed;
    const bool use_fixed = !fixed_failed_raw.empty();
    if (use_fixed)
    {
        std::string parse_error;
        if (!parse_failed_blocks(fixed_failed_raw, k, r, z, fixed_failed, parse_error))
        {
            std::cerr << "[gLRC] invalid fixed failure list: " << parse_error << std::endl;
            return 1;
        }
        if (failure_mode == "max")
        {
            std::cerr << "[gLRC] --failed-blocks cannot be combined with failure_mode=max" << std::endl;
            return 1;
        }
        if (have_f && fail_count != static_cast<int>(fixed_failed.size()))
        {
            std::cerr << "[gLRC] fixed failure count " << fixed_failed.size()
                      << " does not match --fail-count " << fail_count << std::endl;
            return 1;
        }
        fail_count = static_cast<int>(fixed_failed.size());
        have_f = true;
    }

    const char *env_seed = std::getenv("GLRC_RANDOM_SEED");
    const std::string seed_raw =
        !cli_seed_raw.empty() ? cli_seed_raw : ((env_seed && env_seed[0]) ? env_seed : "");
    uint32_t random_seed = 0;
    if (seed_raw.empty())
    {
        std::random_device rd;
        random_seed = static_cast<uint32_t>(rd());
    }
    else
    {
        try
        {
            size_t consumed = 0;
            const unsigned long long parsed = std::stoull(seed_raw, &consumed, 10);
            if (consumed != seed_raw.size() ||
                parsed > static_cast<unsigned long long>(std::numeric_limits<uint32_t>::max()))
                throw std::out_of_range("seed");
            random_seed = static_cast<uint32_t>(parsed);
        }
        catch (const std::exception &)
        {
            std::cerr << "[gLRC] invalid random seed: " << seed_raw
                      << " (expected 0.." << std::numeric_limits<uint32_t>::max() << ")" << std::endl;
            return 1;
        }
    }

    std::cout << "[gLRC] config (n,k,r,z)=(" << n << "," << k << "," << r << "," << z << ")"
              << " mode=" << config->GlrcRepairMode
              << " policy=" << config->GlrcEquationPolicy
              << " failure_mode=" << failure_mode
              << " random_seed=" << random_seed;
    if (use_fixed)
        std::cout << " fixed_failed=" << ECProject::glrc_format_block_list(fixed_failed, k, r, z);
    std::cout << std::endl;

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

    std::mt19937 rng(random_seed);
    std::mt19937 warmup_rng(random_seed ^ 0x9e3779b9U);
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
            else if (failure_mode == "max")
            {
                std::string pattern_error;
                if (!generate_max_failure_pattern(k, r, z, warmup_rng, warmup_failed, pattern_error))
                {
                    std::cerr << "  Warmup pattern generation FAILED: " << pattern_error << std::endl;
                    return 1;
                }
            }
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

    std::cout << "\n[gLRC] Running " << trial_count << " " << failure_mode
              << " trial(s) with f=" << fail_count
              << " on stripe 0..." << std::endl;

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
        std::vector<int> failed;
        if (use_fixed)
            failed = fixed_failed;
        else if (failure_mode == "max")
        {
            std::string pattern_error;
            if (!generate_max_failure_pattern(k, r, z, rng, failed, pattern_error))
            {
                std::cerr << "  FAILED to generate maximum-tolerance pattern: "
                          << pattern_error << std::endl;
                continue;
            }
        }
        else
        {
            failed = all_blocks;
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
              << " policy=" << config->GlrcEquationPolicy
              << " failure_mode=" << failure_mode << std::endl;
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
    bool node_repair = false, reuse_stripes = false;
    std::string coordinator_override, failure_mode, fixed_failed_raw, seed_raw;
    std::string failed_nodes_raw, failed_node_ips_raw;
    if (!parse_glrc_cli_args(argc, argv, cli_fail, cli_trials, have_f, have_n,
                             coordinator_override, show_help, skip_warmup, failure_mode,
                             fixed_failed_raw, seed_raw, node_repair, failed_nodes_raw,
                             failed_node_ips_raw, reuse_stripes))
    {
        print_glrc_client_usage(argc > 0 ? argv[0] : nullptr);
        return show_help ? 0 : 2;
    }
    if (failure_mode.empty())
    {
        if (const char *env_mode = std::getenv("GLRC_FAILURE_MODE"))
            failure_mode = env_mode;
    }
    if (!node_repair)
    {
        if (const char *env_nodes = std::getenv("GLRC_FAILED_NODES"))
        {
            if (env_nodes[0] != '\0')
            {
                failed_nodes_raw = env_nodes;
                node_repair = true;
            }
        }
        if (const char *env_ips = std::getenv("GLRC_FAILED_NODE_IPS"))
        {
            if (env_ips[0] != '\0')
            {
                failed_node_ips_raw = env_ips;
                node_repair = true;
            }
        }
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
    {
        if (node_repair)
            return run_glrc_node_repair_test(client, config, failed_nodes_raw, failed_node_ips_raw,
                                            cli_trials, have_n, reuse_stripes);
        return run_glrc_repair_test(client, config, cli_fail, cli_trials, have_f, have_n,
                                    skip_warmup, failure_mode, fixed_failed_raw, seed_raw);
    }

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


    
    int stripe_num = 1000; // number of stripes
    size_t total_write_size = static_cast<size_t>(stripe_num * block_size * n); // bytes; used for throughput
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
        //      // 1) Choose partial blocks to read (data + parity)
        //     auto ids = pick_some_data_and_parity_blocks(...);

        //      // 2) Read those blocks
        //     auto blocks = client.get_blocks_by_ids(ids);   // requires a new Client API

        //      // 3) Merge algorithm
        //     auto merged = merge_algorithm(blocks);

        //      // 4) Write back (overwrite or write new blocks and switch metadata)
        //     client.put_blocks_by_ids(target_ids, merged);  // requires a new Client write API
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
