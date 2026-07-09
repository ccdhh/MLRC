#ifndef DATANODE_H
#define DATANODE_H

#include "datanode.grpc.pb.h"
#include <grpc++/health_check_service_interface.h>
#include <grpcpp/ext/proto_server_reflection_plugin.h>
#include <grpcpp/grpcpp.h>
#include <asio.hpp>
#include <string>
#include <vector>
#include "meta_definition.h"
#include "config.h"
#include "link_bandwidth.h"
#include <memory>
// #define IF_DEBUG true
#define IF_DEBUG false
namespace ECProject
{
    class DatanodeImpl final
        : public datanode_proto::datanodeService::Service
    {
    public:
        DatanodeImpl(std::string datanode_ip_port) : datanode_ip_port(datanode_ip_port), acceptor(io_context, asio::ip::tcp::endpoint(asio::ip::address::from_string(datanode_ip_port.substr(0, datanode_ip_port.find(':')).c_str()), ECProject::DATANODE_PORT_SHIFT + std::stoi(datanode_ip_port.substr(datanode_ip_port.find(':') + 1, datanode_ip_port.size()))))
        {
            m_ip = datanode_ip_port.substr(0, datanode_ip_port.find(':'));
            m_port = std::stoi(datanode_ip_port.substr(datanode_ip_port.find(':') + 1, datanode_ip_port.size()));
            m_download_port = m_port + ECProject::DATANODE_PORT_SHIFT;
        }
        ~DatanodeImpl() {};
        grpc::Status checkalive(
            grpc::ServerContext *context,
            const datanode_proto::CheckaliveCMD *request,
            datanode_proto::RequestResult *response) override;
        // set
        grpc::Status handleSet(
            grpc::ServerContext *context,
            const datanode_proto::SetInfo *set_info,
            datanode_proto::RequestResult *response) override;
        // append
        grpc::Status handleAppend(
            grpc::ServerContext *context,
            const datanode_proto::AppendInfo *append_info,
            datanode_proto::RequestResult *response) override;
        // merge parity
        grpc::Status handleMergeParity(
            grpc::ServerContext *context,
            const datanode_proto::MergeParityInfo *merge_parity_info,
            datanode_proto::RequestResult *response) override;
        // merge parity with rep
        /*grpc::Status handleMergeParityWithRep(
            grpc::ServerContext *context,
            const datanode_proto::MergeParityInfo *merge_parity_info,
            datanode_proto::RequestResult *response) override;*/
        // recovery
        grpc::Status handleRecovery(
            grpc::ServerContext *context,
            const datanode_proto::MergeParityInfo *recovery_info,
            datanode_proto::RequestResult *response) override;
        grpc::Status handleRecoveryBreakdown(
            grpc::ServerContext *context,
            const datanode_proto::MergeParityInfo *recovery_info,
            datanode_proto::RequestResult *response) override;
        // get
        grpc::Status handleGet(
            grpc::ServerContext *context,
            const datanode_proto::GetInfo *get_info,
            datanode_proto::RequestResult *response) override;
        grpc::Status handleGetBreakdown(
            grpc::ServerContext *context,
            const datanode_proto::GetInfo *get_info,
            datanode_proto::RequestResult *response) override;
        // stripe-level parity merge (local)
        grpc::Status handleStripeMergeParity(
            grpc::ServerContext *context,
            const datanode_proto::StripeMergeParityInfo *info,
            datanode_proto::RequestResult *response) override;
        grpc::Status readBlockBytes(
            grpc::ServerContext *context,
            const datanode_proto::ReadBlockBytesRequest *request,
            datanode_proto::ReadBlockBytesReply *response) override;
        // delete
        grpc::Status handleDelete(
            grpc::ServerContext *context,
            const datanode_proto::DelInfo *del_info,
            datanode_proto::RequestResult *response) override;

        void serialize(const std::string &filename, const ParitySlice &slice);
        std::vector<ParitySlice> deserialize(const std::string &filename);
        void deserialize(const std::string &filename, char *buf);
        bool createDirectories(const std::string &path);
        ECProject::Config *m_sys_config;
        void initNodeBandwidth();
        void initRepairProxyPairing(const std::string &cluster_info_path);
        bool isLocalRepairProxy(const std::string &proxy_ip, int proxy_port) const;
        /** nullptr = unlimited to paired local repair proxy; else shared node egress limiter. */
        SharedBandwidthLimiter *egressBandwidthForRepairProxy(const std::string &proxy_ip, int proxy_port) const;
        /** nullptr = unlimited from paired local repair proxy; else shared node ingress limiter. */
        SharedBandwidthLimiter *ingressBandwidthForRepairProxy(const std::string &proxy_ip, int proxy_port) const;

    private:
        std::string m_local_repair_proxy_ip;
        int m_local_repair_proxy_port = 0;
        std::shared_ptr<SharedBandwidthLimiter> m_ingress_bandwidth;
        std::shared_ptr<SharedBandwidthLimiter> m_egress_bandwidth;
        std::string datanode_ip_port;
        std::string m_ip;
        int m_port;
        int m_block_size;
        int m_download_port;
        asio::io_context io_context;
        asio::ip::tcp::acceptor acceptor;
    };

    class DataNode
    {
    public:
        DataNode(std::string datanode_ip_port) : datanode_ip_port(datanode_ip_port), m_datanodeImpl_ptr(datanode_ip_port) {}
        DataNode(std::string datanode_ip_port, std::string sys_config_path, std::string cluster_info_path)
            : datanode_ip_port(datanode_ip_port), m_datanodeImpl_ptr(datanode_ip_port)
        {
            m_datanodeImpl_ptr.m_sys_config = ECProject::Config::getInstance(sys_config_path);
            m_datanodeImpl_ptr.initNodeBandwidth();
            m_datanodeImpl_ptr.initRepairProxyPairing(cluster_info_path);
        }

        void Run()
        {
            grpc::EnableDefaultHealthCheckService(true);
            grpc::reflection::InitProtoReflectionServerBuilderPlugin();
            grpc::ServerBuilder builder;
            std::cout << "datanode_ip_port:" << datanode_ip_port << std::endl;
            builder.AddListeningPort(datanode_ip_port, grpc::InsecureServerCredentials());
            builder.RegisterService(&m_datanodeImpl_ptr);
            std::unique_ptr<grpc::Server> server(builder.BuildAndStart());
            server->Wait();
        }

    private:
        std::string datanode_ip_port;
        ECProject::DatanodeImpl m_datanodeImpl_ptr;
    };
}

#endif