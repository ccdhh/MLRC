#ifndef COORDINATOR_H
#define COORDINATOR_H
#include "coordinator.grpc.pb.h"
#include "proxy.grpc.pb.h"
#include <grpc++/create_channel.h>
#include <grpcpp/ext/proto_server_reflection_plugin.h>
#include <grpcpp/grpcpp.h>
#include <grpcpp/health_check_service_interface.h>
#include <meta_definition.h>
#include <mutex>
#include <thread>
#include <condition_variable>
#include <config.h>
#include <toolbox.h>
#include "unilrc_encoder.h"
#include "glrc_repair_ilp.h"
#include "glrc_pipeline_plan.h"
// #define IF_DEBUG true
#define IF_DEBUG false
namespace ECProject
{
  class CoordinatorImpl final
      : public coordinator_proto::coordinatorService::Service
  {
  public:
    CoordinatorImpl()
    {
      m_cur_cluster_id = 0;
      m_cur_stripe_id = 0;
    }
    ~CoordinatorImpl() {};
    grpc::Status setParameter(
        grpc::ServerContext *context,
        const coordinator_proto::Parameter *parameter,
        coordinator_proto::RepIfSetParaSuccess *setParameterReply) override;
    grpc::Status sayHelloToCoordinator(
        grpc::ServerContext *context,
        const coordinator_proto::RequestToCoordinator *helloRequestToCoordinator,
        coordinator_proto::ReplyFromCoordinator *helloReplyFromCoordinator) override;
    grpc::Status checkalive(
        grpc::ServerContext *context,
        const coordinator_proto::RequestToCoordinator *helloRequestToCoordinator,
        coordinator_proto::ReplyFromCoordinator *helloReplyFromCoordinator) override;
    // set
    grpc::Status uploadOriginKeyValue(
        grpc::ServerContext *context,
        const coordinator_proto::RequestProxyIPPort *keyValueSize,
        coordinator_proto::ReplyProxyIPPort *proxyIPPort) override;
    grpc::Status reportCommitAbort(
        grpc::ServerContext *context,
        const coordinator_proto::CommitAbortKey *commit_abortkey,
        coordinator_proto::ReplyFromCoordinator *helloReplyFromCoordinator) override;
    grpc::Status checkCommitAbort(
        grpc::ServerContext *context,
        const coordinator_proto::AskIfSuccess *key_opp,
        coordinator_proto::RepIfSuccess *reply) override;
    grpc::Status uploadSetValue(
        grpc::ServerContext *context,
        const coordinator_proto::RequestProxyIPPort *keyValueSize,
        coordinator_proto::ReplyProxyIPsPorts *proxyIPPort) override;
    grpc::Status uploadSubsetValue(
        grpc::ServerContext *context,
        const coordinator_proto::RequestProxyIPPort *keyValueSize,
        coordinator_proto::ReplyProxyIPsPorts *proxyIPPort) override;
    // append
    grpc::Status uploadAppendValue(
        grpc::ServerContext *context,
        const coordinator_proto::RequestProxyIPPort *keyValueSize,
        coordinator_proto::ReplyProxyIPsPorts *proxyIPPort) override;
    // get
    grpc::Status getValue(
        grpc::ServerContext *context,
        const coordinator_proto::KeyAndClientIP *keyClient,
        coordinator_proto::RepIfGetSuccess *getReplyClient) override;
    // get stripe
    grpc::Status getStripe(
        grpc::ServerContext *context,
        const coordinator_proto::KeyAndClientIP *keyClient,
        coordinator_proto::ReplyProxyIPsPorts *proxyIPPort) override;
    // get blocks
    grpc::Status getBlocks(
        grpc::ServerContext *context,
        const coordinator_proto::BlockIDsAndClientIP *blockIDsClient,
        coordinator_proto::ReplyProxyIPsPorts *proxyIPPort) override;
    grpc::Status getDegradedReadBlocks(
        grpc::ServerContext *context,
        const coordinator_proto::BlockIDsAndClientIP *blockIDsClient,
        coordinator_proto::ReplyProxyIPsPorts *proxyIPPort) override;
    // degraded read
    grpc::Status getDegradedReadBlock(
      grpc::ServerContext *context,
      const coordinator_proto::KeyAndClientIP *keyClient,
      coordinator_proto::DegradedReadReply *degradedReadReply) override;
    grpc::Status getDegradedReadBlockBreakdown(
      grpc::ServerContext *context,
      const coordinator_proto::KeyAndClientIP *keyClient,
      coordinator_proto::DegradedReadReply *degradedReadReply) override;
    // recovery
    grpc::Status getRecovery(
        grpc::ServerContext *context,
        const coordinator_proto::KeyAndClientIP *keyClient,
        coordinator_proto::RecoveryReply *replyClient) override;
    grpc::Status getRecoveryBreakdown(
        grpc::ServerContext *context,
        const coordinator_proto::KeyAndClientIP *keyClient,
        coordinator_proto::RecoveryReply *replyClient) override;
    grpc::Status decodeTest(
        grpc::ServerContext *context,
        const coordinator_proto::KeyAndClientIP *keyClient,
        coordinator_proto::DegradedReadReply *degradedReadReply) override;
    grpc::Status fullNodeRecovery(
      grpc::ServerContext *context,
      const coordinator_proto::NodeIdFromClient *request,
      coordinator_proto::RepBlockNum* response) override;
    grpc::Status multiBlockRecovery(
      grpc::ServerContext *context,
      const coordinator_proto::StripeIdAndBlockIDsFromClient *request,
      coordinator_proto::RecoveryReply *replyClient) override;
    // delete
    grpc::Status delByKey(
        grpc::ServerContext *context,
        const coordinator_proto::KeyFromClient *del_key,
        coordinator_proto::RepIfDeling *delReplyClient) override;
    grpc::Status delByStripe(
        grpc::ServerContext *context,
        const coordinator_proto::StripeIdFromClient *stripeid,
        coordinator_proto::RepIfDeling *delReplyClient) override;
    // other
    grpc::Status listStripes(
        grpc::ServerContext *context,
        const coordinator_proto::RequestToCoordinator *req,
        coordinator_proto::RepStripeIds *listReplyClient) override;

    bool init_clusterinfo(std::string m_clusterinfo_path);
    bool init_proxyinfo();
    /** Repair proxy (gRPC) for blocks on this datanode  one proxy per datanode when configured. */
    std::string repair_proxy_key_for_node(int node_id) const;
    void update_stripe_info_in_node(bool add_or_sub, int t_node_id, int stripe_id);
    int randomly_select_a_cluster(int stripe_id);
    int randomly_select_a_node(int cluster_id, int stripe_id);
    int generate_placement(int stripe_id, int block_size);
    void blocks_in_cluster(std::map<char, std::vector<ECProject::Block *>> &block_info, int cluster_id, int stripe_id);
    void find_max_group(int &max_group_id, int &max_group_num, int cluster_id, int stripe_id);
    int count_block_num(char type, int cluster_id, int stripe_id, int group_id);
    bool find_block(char type, int cluster_id, int stripe_id);

    void initialize_unilrc_and_azurelrc_stripe_placement(Stripe *stripe);
    void initialize_optimal_lrc_stripe_placement(Stripe *stripe);
    void initialize_glrc_stripe_placement(Stripe *stripe);
    void initialize_uniform_lrc_stripe_placement(Stripe *stripe);
    void add_to_map(std::map<int, std::vector<int>> &map, int key, int value);
    std::vector<proxy_proto::AppendStripeDataPlacement> generate_add_plans(Stripe *stripe);
    std::vector<proxy_proto::AppendStripeDataPlacement> generate_sub_add_plans(Stripe *stripe, size_t subset_size);
    std::vector<proxy_proto::AppendStripeDataPlacement> generateAppendPlan(Stripe *stripe, int curr_logical_offset, int append_size);
    void update_stripe_info_in_node(int t_node_id, int stripe_id, int index);
    int getClusterAppendSize(Stripe *stripe, const std::map<int, std::pair<int, int>> &block_to_slice_sizes, int curr_group_id, int parity_slice_size);
    void notify_proxies_ready(const proxy_proto::AppendStripeDataPlacement &plan);
    std::vector<int> get_recovery_group_ids(std::string code_type, int k, int r, int z, int failed_block_id);
    void init_recovery_group_lookup_table();
    void print_stripe_data_placement(Stripe &stripe);
    int get_cluster_id_by_group_id(Stripe &stripe, int group_id);
    void getStripeFromProxy(std::string client_ip, int client_port, std::string proxy_ip, int proxy_port, int stripe_id, int group_id, std::vector<int> block_ids);
    bool recovery_one_block(int stripe_id, int failed_block_id);
    bool recovery_glrc_ilp_breakdown(int stripe_id, const std::vector<int> &failed_block_ids,
                                       coordinator_proto::RecoveryReply *recovery_reply);
    bool recovery_glrc_ilp_phase2_breakdown(int stripe_id, const std::vector<int> &failed_block_ids,
                                              coordinator_proto::RecoveryReply *recovery_reply);
    bool recovery_glrc_ilp_phase2_breakdown_ex(int stripe_id, const std::vector<int> &failed_block_ids,
                                                 coordinator_proto::RecoveryReply *recovery_reply,
                                                 const GlrcIlpRepairPlan *preset_plan, int shard_count_override,
                                                 bool acquire_mutex, double *out_orchestration_wait_sec);
    bool recovery_glrc_ilp_pipeline_breakdown(int stripe_id, const std::vector<int> &failed_block_ids,
                                              coordinator_proto::RecoveryReply *recovery_reply);
    bool recovery_glrc_ilp_pipeline_breakdown_ex(int stripe_id, const std::vector<int> &failed_block_ids,
                                                   coordinator_proto::RecoveryReply *recovery_reply,
                                                   const GlrcIlpRepairPlan *preset_plan,
                                                   const GlrcPipelinePlan *preset_pipeline, int hub_block_id_preset,
                                                   int global_shard_begin, int local_shard_count_override,
                                                   bool acquire_mutex, double *out_orchestration_wait_sec);
    bool recovery_glrc_ilp_hybrid_breakdown(int stripe_id, const std::vector<int> &failed_block_ids,
                                            coordinator_proto::RecoveryReply *recovery_reply);
    bool recovery_one_block_breakdown(int stripe_id, int failed_block_id, 
      std::vector<double> &disk_io_start_time, std::vector<double> &disk_io_end_time, std::vector<double> &decode_start_time, std::vector<double> &decode_end_time,
      std::vector<double> &network_start_time, std::vector<double> &network_end_time, double &cross_rack_network_time, double &cross_rack_xor_time,
      std::vector<double> &grpc_notify_time, std::vector<double> &grpc_start_time, std::vector<double> &data_node_grpc_notify_time, std::vector<double> &data_node_grpc_start_time,
      double &dest_data_node_network_time, double &dest_data_node_disk_io_time);
    bool degraded_read_one_block_breakdown(int stripe_id, int failed_block_id, std::string client_ip, int client_port, 
      std::vector<double> &disk_io_start_time, std::vector<double> &disk_io_end_time, std::vector<double> &decode_start_time, std::vector<double> &decode_end_time,
      std::vector<double> &network_start_time, std::vector<double> &network_end_time, double &cross_rack_network_time, double &cross_rack_xor_time,
      std::vector<double> &grpc_notify_time, std::vector<double> &grpc_start_time, std::vector<double> &data_node_grpc_notify_time, std::vector<double> &data_node_grpc_start_time);
    bool degraded_read_one_block(int stripe_id, int failed_block_id, std::string client_ip, int client_port);
    bool degraded_read_one_block_for_workload(int stripe_id, int failed_block_id, std::string client_ip, int client_port, int block_id_to_send);
    ECProject::Config *m_sys_config;
    ECProject::ToolBox *m_toolbox;
    int m_cur_cluster_id = 0;
    int m_cur_stripe_id = 0;
    std::unordered_map<std::string, ObjectInfo> m_object_commit_table;
    std::unordered_map<std::string, ObjectInfo> m_object_updating_table;
    std::map<int, Cluster> m_cluster_table;
    std::map<int, Node> m_node_table;
    std::map<int, Stripe> m_stripe_table;
    std::map<std::string, StripeOffset> m_cur_offset_table;
    std::map<int, std::vector<int>> m_recovery_group_lookup_table;
    
    std::vector<int> get_data_block_num_per_group(int k, int r, int z, std::string code_type);

  private:
    std::mutex m_mutex;
    std::condition_variable cv;
    std::map<std::string, std::unique_ptr<proxy_proto::proxyService::Stub>>
        m_proxy_ptrs;
    ECSchema m_encode_parameters;
    std::vector<int> m_stripe_deleting_table;
    int m_num_of_Clusters;
    // merge groups, for DIS and OPT, the stripes from the same group object to the selected placement scheme
    std::vector<std::vector<int>> m_merge_groups;
    std::vector<int> m_free_clusters;
    int m_agg_start_cid = 0;
  };

  class Coordinator
  {
  public:
    Coordinator(
        std::string m_coordinator_ip_port,
        std::string m_clusterinfo_path)
        : m_coordinator_ip_port{m_coordinator_ip_port},
          m_clusterinfo_path{m_clusterinfo_path}
    {
      m_coordinatorImpl.init_clusterinfo(m_clusterinfo_path);
      m_coordinatorImpl.init_proxyinfo();
    };
    Coordinator(
        std::string m_coordinator_ip_port,
        std::string m_clusterinfo_path, std::string sys_config_path)
        : m_coordinator_ip_port{m_coordinator_ip_port},
          m_clusterinfo_path{m_clusterinfo_path}
    {
      m_coordinatorImpl.init_clusterinfo(m_clusterinfo_path);
      m_coordinatorImpl.init_proxyinfo();
      m_coordinatorImpl.m_sys_config = ECProject::Config::getInstance(sys_config_path);
      m_coordinatorImpl.m_toolbox = ECProject::ToolBox::getInstance();

      // initializing
      m_coordinatorImpl.m_cur_cluster_id = 0;
      m_coordinatorImpl.m_cur_stripe_id = 0;
      m_coordinatorImpl.m_object_commit_table.clear();
      m_coordinatorImpl.m_object_updating_table.clear();
      for (auto it = m_coordinatorImpl.m_cluster_table.begin(); it != m_coordinatorImpl.m_cluster_table.end(); it++)
      {
        Cluster &t_cluster = it->second;
        t_cluster.blocks.clear();
        t_cluster.stripes.clear();
      }
      for (auto it = m_coordinatorImpl.m_node_table.begin(); it != m_coordinatorImpl.m_node_table.end(); it++)
      {
        Node &t_node = it->second;
        t_node.stripes.clear();
      }
      m_coordinatorImpl.m_stripe_table.clear();
      m_coordinatorImpl.m_cur_offset_table.clear();
    };
    // Coordinator
    void Run()
    {
      grpc::EnableDefaultHealthCheckService(true);
      grpc::reflection::InitProtoReflectionServerBuilderPlugin();
      grpc::ServerBuilder builder;
      std::string server_address(m_coordinator_ip_port);
      builder.AddListeningPort(server_address, grpc::InsecureServerCredentials());
      builder.RegisterService(&m_coordinatorImpl);
      std::unique_ptr<grpc::Server> server(builder.BuildAndStart());
      std::cout << "Server listening on " << server_address << std::endl;
      server->Wait();
    }


  private:
    std::string m_coordinator_ip_port;
    std::string m_clusterinfo_path;
    ECProject::CoordinatorImpl m_coordinatorImpl;
  };
} // namespace ECProject

#endif
