#ifndef PROXY_H
#define PROXY_H
#include "coordinator.grpc.pb.h"
#include "proxy.grpc.pb.h"
#include "datanode.grpc.pb.h"
#include "devcommon.h"
#include "meta_definition.h"
#include "lrc.h"
#include <asio.hpp>
#include <grpc++/health_check_service_interface.h>
#include <grpcpp/ext/proto_server_reflection_plugin.h>
#include <grpcpp/grpcpp.h>
#include <thread>
#include <semaphore.h>
#include <memory>
#include <unordered_map>
#include <config.h>
#include <link_bandwidth.h>
#include <toolbox.h>
#include <queue>
// #define IF_DEBUG true
#define IF_DEBUG false
namespace ECProject
{
  /** Last error from glrcIlpPhase2Recovery on this thread (for gRPC status). */
  std::string glrc_phase2_take_last_error();
  /** Last error from glrcIlpPipelineRecovery on this thread (for gRPC status). */
  std::string glrc_pipeline_take_last_error();

  class ProxyImpl final
      : public proxy_proto::proxyService::Service,
        public std::enable_shared_from_this<ECProject::ProxyImpl>
  {

  public:
    ProxyImpl(std::string proxy_ip_port, std::string config_path, std::string coordinator_address) : config_path(config_path), proxy_ip_port(proxy_ip_port), acceptor(io_context, asio::ip::tcp::endpoint(asio::ip::address::from_string(proxy_ip_port.substr(0, proxy_ip_port.find(':')).c_str()), ECProject::PROXY_PORT_SHIFT + std::stoi(proxy_ip_port.substr(proxy_ip_port.find(':') + 1, proxy_ip_port.size())))), m_coordinator_address(coordinator_address)
    {
      init_coordinator();
      init_datanodes(config_path);
      m_ip = proxy_ip_port.substr(0, proxy_ip_port.find(':'));
      m_port = std::stoi(proxy_ip_port.substr(proxy_ip_port.find(':') + 1, proxy_ip_port.size()));
      std::cout << "Cluster id:" << m_self_cluster_id << std::endl;
    }
    ~ProxyImpl() {};
    grpc::Status checkalive(
        grpc::ServerContext *context,
        const proxy_proto::CheckaliveCMD *request,
        proxy_proto::RequestResult *response) override;
    // encode and set
    grpc::Status encodeAndSetObject(
        grpc::ServerContext *context,
        const proxy_proto::ObjectAndPlacement *object_and_placement,
        proxy_proto::SetReply *response) override;
    // append
    grpc::Status scheduleAppend2Datanode(
        grpc::ServerContext *context,
        const proxy_proto::AppendStripeDataPlacement *append_stripe_data_placement,
        proxy_proto::SetReply *response) override;
    // decode and get
    grpc::Status decodeAndGetObject(
        grpc::ServerContext *context,
        const proxy_proto::ObjectAndPlacement *object_and_placement,
        proxy_proto::GetReply *response) override;
    // degraded read
    grpc::Status degradedRead(
        grpc::ServerContext *context,
        const proxy_proto::DegradedReadRequest *degraded_read_request,
        proxy_proto::DegradedReadReply *response) override;
    grpc::Status degradedRead2Client(
      grpc::ServerContext *context,
      const proxy_proto::RecoveryRequest *recovery_request,
      proxy_proto::DegradedReadReply *response) override;
    grpc::Status degradedReadBreakdown(
      grpc::ServerContext *context,
      const proxy_proto::DegradedReadRequest *degraded_read_request,
      proxy_proto::DegradedReadReply *response) override;
    grpc::Status degradedRead2ClientBreakdown(
      grpc::ServerContext *context,
      const proxy_proto::RecoveryRequest *recovery_request,
      proxy_proto::DegradedReadReply *response) override;
    grpc::Status degradedReadWithBlockStripeID(
        grpc::ServerContext *context,
        const proxy_proto::DegradedReadRequest *degraded_read_request,
        proxy_proto::GetReply *response) override;
    grpc::Status partialDecoding(
        grpc::ServerContext *context,
        const proxy_proto::PartialDecodingRequest *partial_decoding_request,
        proxy_proto::DegradedReadReply *response) override;
    // recovery
    grpc::Status recovery(
        grpc::ServerContext *context,
        const proxy_proto::RecoveryRequest *recovery_request,
        proxy_proto::RecoveryReply *response) override;
    grpc::Status recoveryBreakdown(
        grpc::ServerContext *context,
        const proxy_proto::RecoveryRequest *recovery_request,
        proxy_proto::RecoveryReply *response) override;

    grpc::Status multipleRecovery(
        grpc::ServerContext *context,
        const proxy_proto::MultipleRecoveryRequest *multiple_recovery_request,
        proxy_proto::GetReply *response) override;
    // delete
    grpc::Status deleteBlock(
        grpc::ServerContext *context,
        const proxy_proto::NodeAndBlock *node_and_block,
        proxy_proto::DelReply *response) override;
    // get stripe
    grpc::Status getBlocks(
        grpc::ServerContext *context,
        const proxy_proto::StripeAndBlockIDs *request,
        proxy_proto::GetReply *response) override;

    bool SetToDatanode(const char *key, size_t key_length, const char *value, size_t value_length, const char *ip, int port, int offset);
    bool GetFromDatanode(const char *key, size_t key_length, char *value, size_t value_length, const char *ip, int port, int offset);
    bool DelInDatanode(std::string key, std::string node_ip_port);

    ECProject::Config *m_sys_config;
    ECProject::ToolBox *m_toolbox;
    std::queue<std::shared_ptr<char[]>> m_pre_allocated_buffer_queue;
    bool AppendToDatanode(const char *block_key, int block_id, size_t append_size, const char *append_buf, int append_offset, const char *ip, int port, bool is_serialized);
    bool MergeParityOnDatanode(const char *block_key, int block_id, const char *ip, int port, const std::string &append_mode);
    void printAppendStripeDataPlacement(const proxy_proto::AppendStripeDataPlacement *append_stripe_data_placement);
    std::vector<unsigned char *> convertToUnsignedCharArray(std::vector<char*> &input);
    bool GetFromDatanode(const std::string &key, char *value, const size_t value_length, const char *ip, const int port);
    bool GetFromDatanode(const std::string &key, char *value, const size_t value_length, const char *ip, const int port, 
      double *disk_io_start_time, double *disk_io_end_time, double *network_start_time, double *network_end_time, double *grpc_notify_time, double *grpc_start_time);
    bool RecoveryToDatanode(const char *block_key, int block_id, const char *buf, const char *ip, int port);
    bool RecoveryToDatanodeBreakdown(const char *block_key, int block_id, const char *buf, const char *ip, int port, double *network_time, double *disk_io_time);
    bool RecoveryToDatanodeStripeBreakdown(const char *block_key, int block_id, const char *buf, const char *ip, int port,
                                           int recovery_offset, int recovery_size, double *network_time,
                                           double *disk_io_time);
    void get_from_node(const std::string &block_key, char *block_value, const size_t block_size, const char *datanode_ip, const int datanode_port, bool *status, int index);
    void get_from_node_breakdown(const std::string &block_key, char *block_value, const size_t block_size, const char *datanode_ip, const int datanode_port, bool *status, int index, 
      double *disk_io_start_time, double *disk_io_end_time, double *network_start_time, double *network_end_time, double *grpc_notify_time, double *grpc_start_time);
    void initNodeBandwidth();
    bool GetFromDatanodeStripeRangeBreakdown(const std::string &key, char *value, size_t full_block_size,
                                             int read_offset, int read_length, const char *ip, const int port,
                                             double *disk_io_start_time, double *disk_io_end_time,
                                             double *network_start_time, double *network_end_time,
                                             double *grpc_notify_time, double *grpc_start_time,
                                             SharedBandwidthLimiter *block_bandwidth = nullptr);
    bool GetFromDatanodeStripeRangeCompactBreakdown(const std::string &key, char *value, size_t full_block_size,
                                                    int read_offset, int read_length, const char *ip, const int port,
                                                    double *disk_io_start_time, double *disk_io_end_time,
                                                    double *network_start_time, double *network_end_time,
                                                    double *grpc_notify_time, double *grpc_start_time,
                                                    SharedBandwidthLimiter *block_bandwidth = nullptr);
    void get_from_node_stripe_range_breakdown(const std::string &block_key, char *block_value, size_t full_block_size,
                                              int read_offset, int read_length, const char *datanode_ip,
                                              const int datanode_port, bool *status, int index,
                                              double *disk_io_start_time, double *disk_io_end_time,
                                              double *network_start_time, double *network_end_time,
                                              double *grpc_notify_time, double *grpc_start_time,
                                              SharedBandwidthLimiter *block_bandwidth);
    struct Phase2BlockDuplexBw
    {
      SharedBandwidthLimiter *ingress = nullptr;
      SharedBandwidthLimiter *egress = nullptr;
    };
    Phase2BlockDuplexBw phase2BlockDuplexBandwidth(int repair_block_id, int exchange_epoch);
    bool glrcIlpPhase2Recovery(const proxy_proto::RecoveryRequest *recovery_request,
                               proxy_proto::RecoveryReply *response);
    bool glrcIlpPipelineRecovery(const proxy_proto::RecoveryRequest *recovery_request,
                                   proxy_proto::RecoveryReply *response);
    bool glrcIlpPipelineHubRecovery(const proxy_proto::RecoveryRequest *recovery_request,
                                    proxy_proto::RecoveryReply *response);
    bool glrcIlpPipelineChainHeadRecovery(const proxy_proto::RecoveryRequest *recovery_request,
                                          proxy_proto::RecoveryReply *response);
    bool glrcIlpPipelineHopServerRecovery(const proxy_proto::RecoveryRequest *recovery_request,
                                          proxy_proto::RecoveryReply *response);
    bool glrcIlpPipelineLocalDirectRecovery(const proxy_proto::RecoveryRequest *recovery_request,
                                            proxy_proto::RecoveryReply *response);
    bool glrcIlpPipelineTeardownRecovery(const proxy_proto::RecoveryRequest *recovery_request,
                                         proxy_proto::RecoveryReply *response);

  private:
    std::mutex m_glrc_phase2_mutex;
    int m_phase2_block_bw_epoch = -1;
    std::unordered_map<int, std::shared_ptr<SharedBandwidthLimiter>> m_phase2_block_ingress_bw;
    std::unordered_map<int, std::shared_ptr<SharedBandwidthLimiter>> m_phase2_block_egress_bw;
    std::shared_ptr<SharedBandwidthLimiter> m_ingress_bandwidth;
    std::shared_ptr<SharedBandwidthLimiter> m_egress_bandwidth;
    std::mutex m_mutex;
    std::condition_variable cv;
    bool init_coordinator();
    bool init_datanodes(std::string datanodeinfo_path);
    std::unique_ptr<coordinator_proto::coordinatorService::Stub> m_coordinator_ptr;
    std::map<std::string, std::unique_ptr<datanode_proto::datanodeService::Stub>> m_datanode_ptrs;
    std::string config_path;
    std::string proxy_ip_port;
    std::string m_ip;
    int m_port;
    int m_self_cluster_id;
    asio::io_context io_context;
    asio::ip::tcp::acceptor acceptor;
    sem_t sem;
    std::string m_coordinator_address;
  };

  class Proxy
  {
  public:
    Proxy(std::string proxy_ip_port, std::string config_path, std::string coordinator_address) : proxy_ip_port(proxy_ip_port), m_proxyImpl_ptr(proxy_ip_port, config_path, coordinator_address)
    {
    }
    Proxy(std::string proxy_ip_port, std::string config_path, std::string coordinator_address, std::string sys_config_path) : proxy_ip_port(proxy_ip_port), m_proxyImpl_ptr(proxy_ip_port, config_path, coordinator_address)
    {
      m_proxyImpl_ptr.m_sys_config = ECProject::Config::getInstance(sys_config_path);
      m_proxyImpl_ptr.m_toolbox = ECProject::ToolBox::getInstance();
      m_proxyImpl_ptr.initNodeBandwidth();
    }
    void Run()
    {
      grpc::EnableDefaultHealthCheckService(true);
      grpc::reflection::InitProtoReflectionServerBuilderPlugin();
      grpc::ServerBuilder builder;
      std::cout << "proxy_ip_port:" << proxy_ip_port << std::endl;
      builder.AddListeningPort(proxy_ip_port, grpc::InsecureServerCredentials());
      builder.RegisterService(&m_proxyImpl_ptr);
      // Pipeline repair may run several concurrent recoveryBreakdown RPCs on one proxy (multi-chain hops).
      builder.SetSyncServerOption(grpc::ServerBuilder::NUM_CQS, 8);
      builder.SetSyncServerOption(grpc::ServerBuilder::MIN_POLLERS, 4);
      builder.SetSyncServerOption(grpc::ServerBuilder::MAX_POLLERS, 32);
      std::unique_ptr<grpc::Server> server(builder.BuildAndStart());
      server->Wait();
    }

  private:
    std::string proxy_ip_port;
    ECProject::ProxyImpl m_proxyImpl_ptr;
  };
} // namespace ECProject
#endif
