#include "datanode.h"
#include "link_bandwidth.h"
#include "toolbox.h"
#include "unilrc_encoder.h"
#include "tinyxml2.h"
#include <chrono>
#include <cstdio>
#include <cstring>
#include <fstream>
#include <memory>
#include <thread>
#include <unistd.h>
#include <sys/stat.h>
namespace ECProject
{
    // Publish a full block only after the bytes are on disk.  Truncate-in-place
    // while a concurrent GET streams the same key causes short reads / EOF mid
    // block (seen as chain_head "shard send count mismatch" right after SET).
    static bool write_block_file_atomic(const std::string &writepath, const char *data, size_t len)
    {
        const std::string tmp = writepath + ".tmp." + std::to_string(static_cast<long long>(getpid())) + "." +
                                std::to_string(static_cast<long long>(std::chrono::steady_clock::now().time_since_epoch().count()));
        {
            std::ofstream ofs(tmp, std::ios::binary | std::ios::out | std::ios::trunc);
            if (!ofs)
                return false;
            ofs.write(data, static_cast<std::streamsize>(len));
            if (!ofs)
            {
                std::remove(tmp.c_str());
                return false;
            }
            ofs.flush();
            if (!ofs)
            {
                std::remove(tmp.c_str());
                return false;
            }
        }
        if (std::rename(tmp.c_str(), writepath.c_str()) != 0)
        {
            std::remove(tmp.c_str());
            return false;
        }
        return true;
    }

    void DatanodeImpl::initNodeBandwidth()
    {
        std::lock_guard<std::mutex> lock(m_repair_link_bw_mu);
        m_node_ingress_bw.reset();
        m_node_egress_bw.reset();
    }

    SharedBandwidthLimiter *DatanodeImpl::nodeIngressBandwidth() const
    {
        if (m_sys_config == nullptr || m_sys_config->NodeBlockBandwidthMBps <= 0.0)
            return nullptr;
        std::lock_guard<std::mutex> lock(m_repair_link_bw_mu);
        if (!m_node_ingress_bw)
            m_node_ingress_bw = std::make_shared<SharedBandwidthLimiter>(m_sys_config->NodeBlockBandwidthMBps);
        return m_node_ingress_bw.get();
    }

    SharedBandwidthLimiter *DatanodeImpl::nodeEgressBandwidth() const
    {
        if (m_sys_config == nullptr || m_sys_config->NodeBlockBandwidthMBps <= 0.0)
            return nullptr;
        std::lock_guard<std::mutex> lock(m_repair_link_bw_mu);
        if (!m_node_egress_bw)
            m_node_egress_bw = std::make_shared<SharedBandwidthLimiter>(m_sys_config->NodeBlockBandwidthMBps);
        return m_node_egress_bw.get();
    }

    void DatanodeImpl::initRepairProxyPairing(const std::string &cluster_info_path)
    {
        m_local_repair_proxy_ip.clear();
        m_local_repair_proxy_port = 0;
        tinyxml2::XMLDocument xml;
        if (xml.LoadFile(cluster_info_path.c_str()) != tinyxml2::XML_SUCCESS)
            return;
        tinyxml2::XMLElement *root = xml.RootElement();
        if (root == nullptr)
            return;
        for (tinyxml2::XMLElement *cluster = root->FirstChildElement(); cluster != nullptr;
             cluster = cluster->NextSiblingElement())
        {
            const char *cluster_proxy = cluster->Attribute("proxy");
            if (cluster_proxy == nullptr)
                continue;
            tinyxml2::XMLElement *nodes = cluster->FirstChildElement();
            if (nodes == nullptr)
                continue;
            for (tinyxml2::XMLElement *node = nodes->FirstChildElement(); node != nullptr;
                 node = node->NextSiblingElement())
            {
                const char *node_uri = node->Attribute("uri");
                if (node_uri == nullptr)
                    continue;
                const std::string uri(node_uri);
                const size_t colon = uri.rfind(':');
                if (colon == std::string::npos)
                    continue;
                const int node_port = std::stoi(uri.substr(colon + 1));
                if (node_port != m_port)
                    continue;
                std::string repair_proxy = cluster_proxy;
                if (const char *dn_proxy = node->Attribute("proxy"))
                    repair_proxy = std::string(dn_proxy);
                const size_t proxy_colon = repair_proxy.rfind(':');
                if (proxy_colon == std::string::npos)
                    return;
                m_local_repair_proxy_ip = repair_proxy.substr(0, proxy_colon);
                m_local_repair_proxy_port = std::stoi(repair_proxy.substr(proxy_colon + 1));
                return;
            }
        }
    }

    bool DatanodeImpl::isLocalRepairProxy(const std::string &proxy_ip, int proxy_port) const
    {
        if (proxy_ip.empty() || proxy_port <= 0)
            return false;
        // Co-located proxy on this datanode host: not NIC traffic.
        if (!m_ip.empty() && proxy_ip == m_ip)
            return true;
        if (m_local_repair_proxy_port <= 0)
            return false;
        return m_local_repair_proxy_ip == proxy_ip && m_local_repair_proxy_port == proxy_port;
    }

    SharedBandwidthLimiter *DatanodeImpl::egressBandwidthForRepairProxy(const std::string &proxy_ip,
                                                                        int proxy_port) const
    {
        if (isLocalRepairProxy(proxy_ip, proxy_port))
            return nullptr;
        (void)proxy_ip;
        (void)proxy_port;
        return nodeEgressBandwidth();
    }

    SharedBandwidthLimiter *DatanodeImpl::ingressBandwidthForRepairProxy(const std::string &proxy_ip,
                                                                         int proxy_port) const
    {
        if (isLocalRepairProxy(proxy_ip, proxy_port))
            return nullptr;
        (void)proxy_ip;
        (void)proxy_port;
        return nodeIngressBandwidth();
    }

    grpc::Status DatanodeImpl::checkalive(
        grpc::ServerContext *context,
        const datanode_proto::CheckaliveCMD *request,
        datanode_proto::RequestResult *response)
    {
        // std::cout << "[Datanode] checkalive " << request->name() << std::endl;
        response->set_message(true);
        return grpc::Status::OK;
    }

    void DatanodeImpl::serialize(const std::string &filename, const ParitySlice &slice)
    {
        std::ofstream outFile(filename, std::ios::out | std::ios::binary | std::ios::app);
        if (outFile.is_open())
        {
            // Serialize a single struct
            outFile.write(reinterpret_cast<const char *>(&slice.offset), sizeof(slice.offset));
            outFile.write(reinterpret_cast<const char *>(&slice.size), sizeof(slice.size));
            outFile.write(slice.slice_ptr, slice.size);
            outFile.flush();
            outFile.close();
        }
        else
        {
            std::cerr << "Unable to open file for writing." << std::endl;
        }
    }

    std::vector<ParitySlice> DatanodeImpl::deserialize(const std::string &filename)
    {
        std::vector<ParitySlice> slices;
        std::ifstream inFile(filename, std::ios::in | std::ios::binary);
        if (inFile.is_open())
        {
            // Read until end of file
            while (inFile.peek() != EOF)
            {
                ParitySlice slice;

                // Read basic data types
                inFile.read(reinterpret_cast<char *>(&slice.offset), sizeof(slice.offset));
                inFile.read(reinterpret_cast<char *>(&slice.size), sizeof(slice.size));

                // for output, append a \0 at the end
                // slice.slice_ptr = new char[slice.size + 1];
                // inFile.read(slice.slice_ptr, slice.size);
                // slice.slice_ptr[slice.size] = '\0';

                // for no output
                slice.slice_ptr = new char[slice.size];
                inFile.read(slice.slice_ptr, slice.size);

                slices.push_back(std::move(slice));
            }
            inFile.close();
        }
        else
        {
            std::cerr << "Unable to open file for reading." << std::endl;
        }
        return slices;
    }

    void DatanodeImpl::deserialize(const std::string &filename, char *buf)
    {
        std::ifstream inFile(filename, std::ios::in | std::ios::binary);
        if (inFile.is_open())
        {
            int accumulated_offset = 0;
            // read until file end
            while (inFile.peek() != EOF)
            {
                int dummy_offset, size;

                // read basic data types
                inFile.read(reinterpret_cast<char *>(&dummy_offset), sizeof(dummy_offset));
                inFile.read(reinterpret_cast<char *>(&size), sizeof(size));

                // read data to buf
                inFile.read(buf + accumulated_offset, size);
                accumulated_offset += size;
            }
            inFile.close();
        }
        else
        {
            std::cerr << "Unable to open file for reading." << std::endl;
        }
    }

    // create directories for the given path
    bool DatanodeImpl::createDirectories(const std::string &path)
    {
        size_t pos = 0;
        std::string dir;
        while ((pos = path.find('/', pos)) != std::string::npos)
        {
            dir = path.substr(0, pos++);
            if (dir.empty())
                continue;
            if (access(dir.c_str(), 0) == -1)
            {
                if (mkdir(dir.c_str(), S_IRWXU) == -1)
                {
                    return false;
                }
            }
        }

        // create the last directory if it does not exist
        if (!path.empty() && access(path.c_str(), 0) == -1)
        {
            return mkdir(path.c_str(), S_IRWXU) != -1;
        }
        return true;
    }

    grpc::Status DatanodeImpl::handleAppend(
        grpc::ServerContext *context,
        const datanode_proto::AppendInfo *append_info,
        datanode_proto::RequestResult *response)
    {
        std::string block_key = append_info->block_key();
        int block_id = append_info->block_id();
        int append_size = append_info->append_size();
        int append_offset = append_info->append_offset();
        bool is_serialized = append_info->is_serialized();

        // append_offset must be the physical offset of the block
        auto dataBlockHandler = [this](std::string block_key, int append_size, int append_offset) mutable
        {
            try
            {
                std::vector<char> buf(append_size);
                // only send data
                asio::error_code ec;
                asio::ip::tcp::socket socket(io_context);
                acceptor.accept(socket);
                asio::read(socket, asio::buffer(buf.data(), append_size), ec);

                asio::error_code ignore_ec;
                socket.shutdown(asio::ip::tcp::socket::shutdown_both, ignore_ec);
                socket.close(ignore_ec);

                std::string targetdir = "./storage/" + std::to_string(m_port) + "/";
                std::string writepath = targetdir + block_key;

                // std::cout << "[Datanode" << m_port << "][Append101] writepath: " << writepath << " append_offset: " << append_offset << " append_size: " << append_size << std::endl;

                if (access(targetdir.c_str(), 0) == -1)
                {
                    createDirectories(targetdir);
                }

                if (append_offset == 0)
                {
                    // Create or truncate when starting a block from offset 0 (repair may rewrite).
                    std::ofstream create_file(writepath, std::ios::binary | std::ios::out | std::ios::trunc);
                    create_file.close();
                }

                // Open file in append mode
                // write the data to the disk using pagecache
                std::ofstream append_file(writepath, std::ios::binary | std::ios::out | std::ios::app);
                // Append data from buffer to end of file
                append_file.write(buf.data(), append_size);
                if (IF_DEBUG)
                {
                    std::cout << "[Datanode" << m_port << "][Append120] successfully append data block " << block_key << " with " << append_size << " bytes" << std::endl;
                }
                append_file.flush();
                append_file.close();
            }
            catch (const std::exception &e)
            {
                std::cerr << e.what() << '\n';
            }
        };

        // append_offset must be the physical offset of the block
        auto ParityBlockHandler = [this](std::string block_key, int append_size, int append_offset, bool is_serialized) mutable
        {
            try
            {
                char *buf = new char[append_size];
                // only send data
                asio::error_code ec;
                asio::ip::tcp::socket socket(io_context);
                acceptor.accept(socket);
                asio::read(socket, asio::buffer(buf, append_size), ec);

                asio::error_code ignore_ec;
                socket.shutdown(asio::ip::tcp::socket::shutdown_both, ignore_ec);
                socket.close(ignore_ec);

                std::string targetdir = "./storage/" + std::to_string(m_port) + "/";
                std::string writepath = targetdir + block_key;

                // std::cout << "[Datanode" << m_port << "][Append101] writepath: " << writepath << " append_offset: " << append_offset << " append_size: " << append_size << std::endl;

                if (access(targetdir.c_str(), 0) == -1)
                {
                    createDirectories(targetdir);
                }

                if (append_offset == 0 && access(writepath.c_str(), 0) == -1)
                {
                    // std::cout << "create parity block file with path: " << writepath << std::endl;
                    // Create new file if append_offset is 0 and file does not exist
                    std::ofstream create_file(writepath, std::ios::binary | std::ios::out | std::ios::trunc);
                    create_file.close();
                }

                // serialize and append to file
                if (is_serialized)
                {
                    serialize(writepath, ParitySlice(append_offset, append_size, buf));
                }
                else
                {
                    std::ofstream append_file(writepath, std::ios::binary | std::ios::out | std::ios::app);
                    append_file.write(buf, append_size);
                    append_file.flush();
                    append_file.close();
                }

                if (IF_DEBUG)
                {
                    std::cout << "[Datanode" << m_port << "][Append167] successfully append parity block " << block_key << " with " << append_size << " bytes" << std::endl;
                }
            }
            catch (const std::exception &e)
            {
                std::cerr << e.what() << '\n';
            }
        };

        try
        {
            if (IF_DEBUG)
            {
                // std::cout << "[Datanode" << m_port << "][Append109] block_key: " << block_key << ", block_id: " << block_id << ", append_size: " << append_size << ", append_offset: " << append_offset << " is_serialized: " << is_serialized << std::endl;
            }
            if (block_id < m_sys_config->k)
            {
                std::thread my_thread(dataBlockHandler, block_key, append_size, append_offset);
                my_thread.detach();
            }
            else
            {
                std::thread my_thread(ParityBlockHandler, block_key, append_size, append_offset, is_serialized);
                my_thread.detach();
            }
            response->set_message(true);
        }
        catch (const std::exception &e)
        {
            std::cerr << e.what() << '\n';
        }

        return grpc::Status::OK;
    }

    grpc::Status DatanodeImpl::handleRecovery(
        grpc::ServerContext *context,
        const datanode_proto::MergeParityInfo *recovery_info,
        datanode_proto::RequestResult *response)
    {
        std::string block_key = recovery_info->block_key();
        int block_id = recovery_info->block_id();
        SharedBandwidthLimiter *ingress_bw =
            ingressBandwidthForRepairProxy(recovery_info->proxy_ip(), recovery_info->proxy_port());

        auto handler = [this, ingress_bw](std::string block_key, int block_id) mutable
        {
            try
            {
                std::vector<char> buf(m_sys_config->BlockSize);
                // only send data
                asio::error_code ec;
                asio::ip::tcp::socket socket(io_context);
                acceptor.accept(socket);
                tcp_read_with_shared_bandwidth(socket, buf.data(), m_sys_config->BlockSize, ingress_bw, ec);

                asio::error_code ignore_ec;
                socket.shutdown(asio::ip::tcp::socket::shutdown_both, ignore_ec);
                socket.close(ignore_ec);

                std::string targetdir = "./storage/" + std::to_string(m_port) + "/";
                std::string writepath = targetdir + block_key;
                if(access(targetdir.c_str(), 0) == -1)
                {
                    mkdir(targetdir.c_str(), S_IRWXU);
                }

                std::ofstream ofs(writepath, std::ios::binary | std::ios::out | std::ios::trunc);
                if (!ofs.is_open())
                {
                    std::cerr << "[Recovery] Failed to open file: " << writepath << std::endl;
                    exit(-1);
                }
                ofs.write(buf.data(), m_sys_config->BlockSize);
                ofs.flush();
                ofs.close();

                if (IF_DEBUG)
                {
                    std::cout << "[Datanode" << m_port << "][Recovery] successfully recovery block " << block_key << " with " << m_sys_config->BlockSize << " bytes" << std::endl;
                }
            }
            catch (const std::exception &e)
            {
                std::cerr << e.what() << '\n';
            }
        };

        try
        {
            std::thread my_thread(handler, block_key, block_id);
            my_thread.join();
            response->set_message(true);
        }
        catch (const std::exception &e)
        {
            std::cerr << e.what() << '\n';
        }

        return grpc::Status::OK;
    }

    grpc::Status DatanodeImpl::handleRecoveryBreakdown(
        grpc::ServerContext *context,
        const datanode_proto::MergeParityInfo *recovery_info,
        datanode_proto::RequestResult *response)
    {
        std::chrono::high_resolution_clock::time_point grpc_start_time = std::chrono::high_resolution_clock::now();
        response->set_grpc_start_time(std::chrono::duration_cast<std::chrono::duration<double>>(grpc_start_time.time_since_epoch()).count());

        std::string block_key = recovery_info->block_key();
        int block_id = recovery_info->block_id();
        const int recovery_offset = recovery_info->recovery_offset();
        const int recovery_size =
            recovery_info->recovery_size() > 0 ? recovery_info->recovery_size() : m_sys_config->BlockSize;
        const bool stripe_mode = recovery_info->recovery_size() > 0;
        // A full-block (recovery_size==0) stream is paced by the pipeline hub's
        // shared egress NIC.  Charging the target ingress again makes one
        // physical transfer take ~2B/w because the receiver cannot observe the
        // sender's pre-write sleep.  Stripe-mode legacy callers retain target
        // ingress accounting because they may fan in from independent senders.
        SharedBandwidthLimiter *ingress_bw =
            recovery_info->recovery_size() == 0
                ? nullptr
                : ingressBandwidthForRepairProxy(recovery_info->proxy_ip(), recovery_info->proxy_port());

        auto handler = [this, ingress_bw, &response](std::string block_key, int block_id, int recovery_offset,
                                                     int recovery_size, bool stripe_mode) mutable {
            try
            {
                asio::error_code ec;
                asio::ip::tcp::socket socket(io_context);
                acceptor.accept(socket);

                std::string targetdir = "./storage/" + std::to_string(m_port) + "/";
                std::string writepath = targetdir + block_key;
                if (access(targetdir.c_str(), 0) == -1)
                {
                    mkdir(targetdir.c_str(), S_IRWXU);
                }

                std::chrono::high_resolution_clock::time_point disk_begin{};
                std::chrono::high_resolution_clock::time_point disk_end{};
                double disk_active_sec = 0.0;
                size_t wrote = 0;

                // Stream TCP payload directly into the block file.  The old path
                // materialized the entire recovery payload in memory before writing,
                // so proxy-side writeback could not overlap network receive with disk I/O.
                auto stream_to_file = [&](auto &ofs) -> bool {
                    constexpr size_t kStreamChunkBytes = 1024 * 1024;
                    std::vector<char> buffer(std::min(kStreamChunkBytes, static_cast<size_t>(recovery_size)));
                    size_t remaining = static_cast<size_t>(recovery_size);
                    bool started = false;
                    while (remaining > 0 && !ec)
                    {
                        const size_t requested = std::min(buffer.size(), remaining);
                        // Credit real NIC/disk wait time against the link budget (claim + I/O +
                        // remainder) so a 1 Gbps host is not paced at ~B/2.
                        tcp_read_with_shared_bandwidth(socket, buffer.data(), requested, ingress_bw, ec,
                                                       SharedBandwidthPace::DrainThenAccount);
                        if (ec)
                            break;
                        if (!started)
                        {
                            disk_begin = std::chrono::high_resolution_clock::now();
                            started = true;
                        }
                        const auto write_begin = std::chrono::high_resolution_clock::now();
                        ofs.write(buffer.data(), static_cast<std::streamsize>(requested));
                        disk_active_sec +=
                            std::chrono::duration<double>(
                                std::chrono::high_resolution_clock::now() - write_begin)
                                .count();
                        if (!ofs.good())
                        {
                            ec = asio::error::fault;
                            break;
                        }
                        wrote += requested;
                        remaining -= requested;
                    }
                    const auto flush_begin = std::chrono::high_resolution_clock::now();
                    ofs.flush();
                    disk_active_sec +=
                        std::chrono::duration<double>(
                            std::chrono::high_resolution_clock::now() - flush_begin)
                            .count();
                    if (disk_begin.time_since_epoch().count() != 0)
                    {
                        disk_end =
                            disk_begin +
                            std::chrono::duration_cast<std::chrono::high_resolution_clock::duration>(
                                std::chrono::duration<double>(disk_active_sec));
                    }
                    return !ec && wrote == static_cast<size_t>(recovery_size);
                };

                bool ok = false;
                if (stripe_mode)
                {
                    if (recovery_offset == 0)
                    {
                        std::ofstream ofs(writepath, std::ios::binary | std::ios::out | std::ios::trunc);
                        if (!ofs.is_open())
                        {
                            std::cerr << "[Recovery] Failed to open file: " << writepath << std::endl;
                            exit(-1);
                        }
                        ok = stream_to_file(ofs);
                        ofs.close();
                    }
                    else
                    {
                        std::fstream ofs(writepath, std::ios::binary | std::ios::in | std::ios::out);
                        if (!ofs.is_open())
                        {
                            std::cerr << "[Recovery] Failed to open file for stripe write: " << writepath << std::endl;
                            exit(-1);
                        }
                        ofs.seekp(recovery_offset, std::ios::beg);
                        ok = stream_to_file(ofs);
                        ofs.close();
                    }
                }
                else
                {
                    const std::string tmp =
                        writepath + ".recovery.tmp." +
                        std::to_string(static_cast<long long>(getpid())) + "." +
                        std::to_string(static_cast<long long>(
                            std::chrono::steady_clock::now().time_since_epoch().count()));
                    std::ofstream ofs(tmp, std::ios::binary | std::ios::out | std::ios::trunc);
                    if (!ofs.is_open())
                    {
                        std::cerr << "[Recovery] Failed to open temp file: " << tmp << std::endl;
                    }
                    else
                    {
                        ok = stream_to_file(ofs);
                        ofs.close();
                        if (ok)
                        {
                            if (std::rename(tmp.c_str(), writepath.c_str()) != 0)
                            {
                                std::cerr << "[Recovery] Failed to publish temp file: " << tmp
                                          << " -> " << writepath << std::endl;
                                ok = false;
                            }
                        }
                        if (!ok)
                            std::remove(tmp.c_str());
                    }
                }

                asio::error_code ignore_ec;
                socket.shutdown(asio::ip::tcp::socket::shutdown_both, ignore_ec);
                socket.close(ignore_ec);

                if (!ok)
                {
                    std::cerr << "[Datanode" << m_port << "][Recovery] stream write failed block=" << block_key
                              << " wrote=" << wrote << "/" << recovery_size
                              << (ec ? (" err=" + ec.message()) : std::string()) << std::endl;
                }
                else if (disk_begin.time_since_epoch().count() != 0)
                {
                    response->set_disk_io_start_time(
                        std::chrono::duration_cast<std::chrono::duration<double>>(disk_begin.time_since_epoch()).count());
                    response->set_disk_io_end_time(
                        std::chrono::duration_cast<std::chrono::duration<double>>(disk_end.time_since_epoch()).count());
                }
                response->set_message(ok);

                if (IF_DEBUG)
                {
                    std::cout << "[Datanode" << m_port << "][Recovery] block " << block_key << " wrote " << wrote
                              << " bytes @ offset " << recovery_offset << std::endl;
                }
                (void)block_id;
            }
            catch (const std::exception &e)
            {
                std::cerr << e.what() << '\n';
            }
        };

        try
        {
            std::thread my_thread(handler, block_key, block_id, recovery_offset, recovery_size, stripe_mode);
            my_thread.join();
        }
        catch (const std::exception &e)
        {
            std::cerr << e.what() << '\n';
        }

        return grpc::Status::OK;
    }


    grpc::Status DatanodeImpl::handleMergeParity(
        grpc::ServerContext *context,
        const datanode_proto::MergeParityInfo *merge_parity_info,
        datanode_proto::RequestResult *response)
    {
        std::string block_key = merge_parity_info->block_key();
        int block_id = merge_parity_info->block_id();
        auto handler = [this](std::string block_key, int block_id) mutable
        {
            try
            {
                std::string targetdir = "./storage/" + std::to_string(m_port) + "/";
                std::string readpath = targetdir + block_key;

                // std::cout << "[Datanode" << m_port << "][Merge Parity Slices] readpath: " << readpath << std::endl;

                if (access(readpath.c_str(), 0) == -1)
                {
                    std::cerr << "[Datanode" << m_port << "][Merge Parity Slices] file does not exist!" << readpath << std::endl;
                    exit(-1);
                }
                std::vector<ParitySlice> slices = deserialize(readpath);
                std::string writepath = targetdir + block_key;
                std::ofstream ofs(writepath, std::ios::binary | std::ios::out | std::ios::trunc);
                std::unique_ptr<char[]> mergedBuf(new char[m_sys_config->BlockSize]);
                memset(mergedBuf.get(), 0, m_sys_config->BlockSize);
                for (const auto &slice : slices)
                {
                    for (int i = 0; i < slice.size; i++)
                    {
                        assert(slice.offset + i < m_sys_config->BlockSize && "Parity slice.offset + i >= m_sys_config->BlockSize!");
                        mergedBuf[slice.offset + i] ^= slice.slice_ptr[i];
                    }
                }
                ofs.write(mergedBuf.get(), m_sys_config->BlockSize);
                ofs.flush();
                ofs.close();
            }
            catch (const std::exception &e)
            {
                std::cerr << e.what() << '\n';
            }
        };

        try
        {
            std::thread my_thread(handler, block_key, block_id);
            my_thread.detach();
            response->set_message(true);
        }
        catch (const std::exception &e)
        {
            std::cerr << e.what() << '\n';
        }

        return grpc::Status::OK;
    }

    /*grpc::Status DatanodeImpl::handleMergeParityWithRep(
        grpc::ServerContext *context,
        const datanode_proto::MergeParityInfo *merge_parity_info,
        datanode_proto::RequestResult *response)
    {
        std::string block_key = merge_parity_info->block_key();
        int block_id = merge_parity_info->block_id();
        auto handler = [this](std::string block_key, int block_id) mutable
        {
            try
            {
                std::string targetdir = "./storage/" + std::to_string(m_port) + "/";
                std::string readpath = targetdir + block_key;
                if (access(readpath.c_str(), 0) == -1)
                {
                    std::cout << "[Datanode" << m_port << "][Merge Parity Slices] file does not exist!" << readpath << std::endl;
                    exit(-1);
                }

                std::string writepath = targetdir + block_key;
                std::ofstream ofs(writepath, std::ios::binary | std::ios::out | std::ios::trunc);
                std::unique_ptr<char[]> dataBuf(new char[m_sys_config->BlockSize * m_sys_config->k]);
                memset(dataBuf.get(), 0, m_sys_config->BlockSize * m_sys_config->k);
                std::unique_ptr<char[]> mergedBuf(new char[m_sys_config->BlockSize]);
                memset(mergedBuf.get(), 0, m_sys_config->BlockSize);

                deserialize(readpath, dataBuf.get());
                ECProject::encode_unilrc_w_rep_mode(m_sys_config->k, m_sys_config->r, m_sys_config->z, reinterpret_cast<unsigned char *>(dataBuf.get()), reinterpret_cast<unsigned char *>(mergedBuf.get()), m_sys_config->BlockSize, m_sys_config->UnitSize, block_id);

                ofs.write(mergedBuf.get(), m_sys_config->BlockSize);
                ofs.flush();
                ofs.close();
            }
            catch (const std::exception &e)
            {
                std::cerr << e.what() << '\n';
            }
        };

        try
        {
            std::thread my_thread(handler, block_key, block_id);
            my_thread.detach();
            response->set_message(true);
        }
        catch (const std::exception &e)
        {
            std::cerr << e.what() << '\n';
        }

        return grpc::Status::OK;
    }*/

    grpc::Status DatanodeImpl::handleSet(
        grpc::ServerContext *context,
        const datanode_proto::SetInfo *set_info,
        datanode_proto::RequestResult *response)
    {
        std::string block_key = set_info->block_key();
        int block_size = set_info->block_size();
        std::string proxy_ip = set_info->proxy_ip();
        int proxy_port = set_info->proxy_port();
        bool ispull = set_info->ispull();
        auto handler1 = [this](std::string block_key, int block_size) mutable
        {
            try
            {
                // char *buf = new char[block_size];
                std::vector<char> buf(block_size);
                // only send data
                asio::error_code ec;
                asio::ip::tcp::socket socket(io_context);
                acceptor.accept(socket);
                asio::read(socket, asio::buffer(buf.data(), block_size), ec);

                std::string targetdir = "./storage/" + std::to_string(m_port) + "/";
                std::string writepath = targetdir + block_key;
                if (access(targetdir.c_str(), 0) == -1)
                {
                    mkdir(targetdir.c_str(), S_IRWXU);
                }

                // Durable publish before closing TCP so SET completion (peer EOF)
                // implies the block is visible to a following GET/recovery.
                if (!write_block_file_atomic(writepath, buf.data(), static_cast<size_t>(block_size)))
                {
                    std::cerr << "[Datanode" << m_port << "][Write] atomic write failed for " << block_key << std::endl;
                }
                else if (IF_DEBUG)
                {
                    std::cout << "[Datanode" << m_port << "][Write] successfully write " << block_key << " with "
                              << block_size << "bytes" << std::endl;
                }

                asio::error_code ignore_ec;
                socket.shutdown(asio::ip::tcp::socket::shutdown_both, ignore_ec);
                socket.close(ignore_ec);
            }
            catch (const std::exception &e)
            {
                std::cerr << e.what() << '\n';
            }
        };
        auto handler2 = [this, proxy_ip, proxy_port](std::string block_key, int block_size) mutable
        {
            try
            {
                std::vector<char> buf(block_size);

                asio::ip::tcp::socket socket(io_context);
                asio::ip::tcp::resolver resolver(io_context);
                asio::error_code con_error;
                asio::connect(socket, resolver.resolve({std::string(proxy_ip), std::to_string(proxy_port)}), con_error);
                asio::error_code ec;
                if (!con_error && IF_DEBUG)
                {
                    std::cout << "[Datanode" << m_port << "] Connect to " << proxy_ip << ":" << proxy_port << " success!" << std::endl;
                }

                asio::read(socket, asio::buffer(buf.data(), block_size), ec);

                std::string targetdir = "./storage/" + std::to_string(m_port) + "/";
                std::string writepath = targetdir + block_key;
                if (access(targetdir.c_str(), 0) == -1)
                {
                    mkdir(targetdir.c_str(), S_IRWXU);
                }

                if (!write_block_file_atomic(writepath, buf.data(), static_cast<size_t>(block_size)))
                {
                    std::cerr << "[Datanode" << m_port << "][Write] atomic write failed for " << block_key << std::endl;
                }
                else if (IF_DEBUG)
                {
                    std::cout << "[Datanode" << m_port << "][Write] successfully write " << block_key << " with "
                              << block_size << "bytes" << std::endl;
                }

                asio::error_code ignore_ec;
                socket.shutdown(asio::ip::tcp::socket::shutdown_both, ignore_ec);
                socket.close(ignore_ec);
            }
            catch (const std::exception &e)
            {
                std::cerr << e.what() << '\n';
            }
        };
        try
        {
            if (IF_DEBUG)
            {
                std::cout << "[Datanode" << m_port << "][SET] ready to handle set!" << std::endl;
            }
            if (ispull)
            {
                std::thread my_thread(handler2, block_key, block_size);
                my_thread.join();
            }
            else
            {
                std::thread my_thread(handler1, block_key, block_size);
                my_thread.detach();
            }
            response->set_message(true);
        }
        catch (std::exception &e)
        {
            std::cout << "exception" << std::endl;
            std::cout << e.what() << std::endl;
        }
        return grpc::Status::OK;
    }

    grpc::Status DatanodeImpl::handleGetBreakdown(
        grpc::ServerContext *context,
        const datanode_proto::GetInfo *get_info,
        datanode_proto::RequestResult *response)
    {
        std::chrono::high_resolution_clock::time_point grpc_start = std::chrono::high_resolution_clock::now();
        response->set_grpc_start_time(std::chrono::duration_cast<std::chrono::duration<double>>(grpc_start.time_since_epoch()).count());

        std::string block_key = get_info->block_key();
        int block_size = get_info->block_size();
        int read_offset = get_info->read_offset();
        int read_length = get_info->read_length();
        if (read_length <= 0)
        {
            read_offset = 0;
            read_length = block_size;
        }
        SharedBandwidthLimiter *egress_bw =
            egressBandwidthForRepairProxy(get_info->proxy_ip(), get_info->proxy_port());
        std::string targetdir = "./storage/" + std::to_string(m_port) + "/";
        std::string readpath = targetdir + block_key;

        auto data_acceptor = std::make_shared<asio::ip::tcp::acceptor>(io_context);
        try
        {
            asio::ip::tcp::endpoint data_ep(asio::ip::address::from_string(m_ip), 0);
            data_acceptor->open(data_ep.protocol());
            data_acceptor->set_option(asio::ip::tcp::acceptor::reuse_address(true));
            data_acceptor->bind(data_ep);
            data_acceptor->listen(asio::socket_base::max_listen_connections);
        }
        catch (const std::exception &e)
        {
            std::cout << "[Datanode" << m_port << "][GET] data acceptor bind failed: " << e.what() << std::endl;
            response->set_message(false);
            return grpc::Status(grpc::StatusCode::INTERNAL, "data acceptor bind failed");
        }
        response->set_data_port(data_acceptor->local_endpoint().port());

        const bool stripe_pipeline_read = read_length > 0 && read_length < block_size;
        // Return the data port before disk I/O.  Pipeline clients can now
        // connect to every helper concurrently; each handler streams file
        // chunks directly into its socket instead of first materializing the
        // entire block in memory on the synchronous gRPC path.
        auto handler = [this, readpath, read_offset, read_length, stripe_pipeline_read, egress_bw, data_acceptor]() mutable
        {
            int data_port = data_acceptor->local_endpoint().port();
            asio::error_code error;
            asio::ip::tcp::socket socket(io_context);
            asio::error_code accept_ec;
            data_acceptor->accept(socket, accept_ec);
            if (accept_ec)
            {
                std::cerr << "[Datanode" << m_port << "][GET] accept on data_port " << data_port
                          << " failed: " << accept_ec.message() << std::endl;
            }
            else
            {
                size_t wrote = 0;
                std::ifstream ifs(readpath, std::ios::binary);
                if (!ifs)
                {
                    std::cerr << "[Datanode" << m_port << "][GET] file does not exist " << readpath << std::endl;
                    error = asio::error::not_found;
                }
                else
                {
                    ifs.seekg(read_offset, std::ios::beg);
                    constexpr size_t kStreamChunkBytes = 1024 * 1024;
                    std::vector<char> buffer(std::min(kStreamChunkBytes, static_cast<size_t>(read_length)));
                    size_t remaining = static_cast<size_t>(read_length);
                    while (remaining > 0 && !error)
                    {
                        const size_t requested = std::min(buffer.size(), remaining);
                        ifs.read(buffer.data(), static_cast<std::streamsize>(requested));
                        const size_t read_bytes = static_cast<size_t>(ifs.gcount());
                        if (read_bytes != requested)
                        {
                            error = asio::error::eof;
                            break;
                        }
                        if (stripe_pipeline_read || egress_bw == nullptr)
                            asio::write(socket, asio::buffer(buffer.data(), read_bytes), error);
                        else
                            tcp_write_with_shared_bandwidth(socket, buffer.data(), read_bytes, egress_bw, error);
                        if (!error)
                        {
                            wrote += read_bytes;
                            remaining -= read_bytes;
                        }
                    }
                }
                if (IF_DEBUG)
                {
                    std::cout << "[Datanode" << m_port << "][GET] data_port " << data_port << " wrote " << wrote << "/"
                              << read_length << (error ? (" err=" + error.message()) : std::string()) << std::endl;
                }
            }
            asio::error_code ignore_ec;
            socket.shutdown(asio::ip::tcp::socket::shutdown_both, ignore_ec);
            socket.close(ignore_ec);
            data_acceptor->close(ignore_ec);
        };
        try
        {
            if (IF_DEBUG)
            {
                std::cout << "[Datanode" << m_port << "][GET] ready to handle get!" << std::endl;
            }
            std::thread my_thread(handler);
            my_thread.detach();
            response->set_message(true);
        }
        catch (std::exception &e)
        {
            std::cout << "exception" << std::endl;
            std::cout << e.what() << std::endl;
        }
        return grpc::Status::OK;
    }

    grpc::Status DatanodeImpl::handleGet(
        grpc::ServerContext *context,
        const datanode_proto::GetInfo *get_info,
        datanode_proto::RequestResult *response)
    {
        std::string block_key = get_info->block_key();
        int block_size = get_info->block_size();
        std::string proxy_ip = get_info->proxy_ip();
        int proxy_port = get_info->proxy_port();
        SharedBandwidthLimiter *egress_bw = egressBandwidthForRepairProxy(proxy_ip, proxy_port);
        std::string targetdir = "./storage/" + std::to_string(m_port) + "/";
        std::string readpath = targetdir + block_key;
        char *buf = new char[block_size];
        if (access(readpath.c_str(), 0) == -1)
        {
            std::cout << "[Datanode" << m_port << "][Read] file does not exist!" << readpath << std::endl;
        }
        else
        {
            if (IF_DEBUG)
            {
                std::cout << "[Datanode" << m_port << "][GET] read from the disk and write to socket with port " << m_port + ECProject::DATANODE_PORT_SHIFT << std::endl;
            }
            std::ifstream ifs(readpath);
            ifs.read(buf, block_size);
            ifs.close();
        }
        auto handler = [this, block_size, egress_bw](char *buf) mutable
        {
            asio::error_code error;
            asio::ip::tcp::socket socket(io_context);
            acceptor.accept(socket);
            tcp_write_with_shared_bandwidth(socket, buf, block_size, egress_bw, error);
            asio::error_code ignore_ec;
            socket.shutdown(asio::ip::tcp::socket::shutdown_both, ignore_ec);
            socket.close(ignore_ec);
            if (IF_DEBUG)
            {
                std::cout << "[Datanode" << m_port << "][GET] write to socket!" << std::endl;
            }
            delete buf;
        };
        try
        {
            if (IF_DEBUG)
            {
                std::cout << "[Datanode" << m_port << "][GET] ready to handle get!" << std::endl;
            }
            std::thread my_thread(handler, buf);
            my_thread.detach();
            response->set_message(true);
        }
        catch (std::exception &e)
        {
            std::cout << "exception" << std::endl;
            std::cout << e.what() << std::endl;
        }
        return grpc::Status::OK;
    }

    grpc::Status DatanodeImpl::readBlockBytes(
        grpc::ServerContext *context,
        const datanode_proto::ReadBlockBytesRequest *request,
        datanode_proto::ReadBlockBytesReply *response)
    {
        (void)context;
        std::string block_key = request->block_key();
        int block_size = request->block_size();
        std::string targetdir = "./storage/" + std::to_string(m_port) + "/";
        std::string readpath = targetdir + block_key;

        if (block_size <= 0 || access(readpath.c_str(), 0) == -1) {
            response->set_ok(false);
            return grpc::Status::OK;
        }

        struct stat st {};
        if (stat(readpath.c_str(), &st) != 0 || st.st_size == 0) {
            response->set_ok(false);
            return grpc::Status::OK;
        }

        std::string data;
        data.resize(static_cast<size_t>(block_size), '\0');

        if (static_cast<size_t>(st.st_size) == static_cast<size_t>(block_size)) {
            std::ifstream ifs(readpath, std::ios::binary);
            if (!ifs.is_open()) {
                response->set_ok(false);
                return grpc::Status::OK;
            }
            ifs.read(data.data(), block_size);
            if (ifs.gcount() != static_cast<std::streamsize>(block_size)) {
                response->set_ok(false);
                return grpc::Status::OK;
            }
        } else {
            /* Serialized parity layout (append with is_serialized): rebuild flat block */
            deserialize(readpath, data.data());
        }

        response->set_ok(true);
        response->set_data(std::move(data));
        return grpc::Status::OK;
    }

    grpc::Status DatanodeImpl::handleStripeMergeParity(
        grpc::ServerContext *context,
        const datanode_proto::StripeMergeParityInfo *info,
        datanode_proto::RequestResult *response)
    {
        std::string parity_key_a = info->parity_key_a();
        std::string parity_key_b = info->parity_key_b();
        std::string new_parity_key = info->new_parity_key();
        int block_size = info->block_size();
        unsigned char coeff = static_cast<unsigned char>(info->gf_coeff());
        std::string parity_b_ip = info->parity_b_datanode_ip();
        int parity_b_port = info->parity_b_datanode_port();

        std::string targetdir = "./storage/" + std::to_string(m_port) + "/";
        std::string path_a = targetdir + parity_key_a;
        std::string path_b = targetdir + parity_key_b;
        std::string path_new = targetdir + new_parity_key;

        if (access(path_a.c_str(), 0) == -1) {
            std::cerr << "[Datanode" << m_port << "][StripeMergeParity] parity A not found: " << path_a << std::endl;
            response->set_message(false);
            return grpc::Status::OK;
        }
        std::unique_ptr<char[]> buf_a(new char[block_size]);
        std::unique_ptr<char[]> buf_b(new char[block_size]);
        std::unique_ptr<char[]> buf_new(new char[block_size]);

        struct stat sta {};
        if (stat(path_a.c_str(), &sta) != 0) {
            std::cerr << "[Datanode" << m_port << "][StripeMergeParity] stat parity A failed: " << path_a << std::endl;
            response->set_message(false);
            return grpc::Status::OK;
        }
        if (static_cast<size_t>(sta.st_size) == static_cast<size_t>(block_size)) {
            std::ifstream ifs_a(path_a, std::ios::binary);
            ifs_a.read(buf_a.get(), block_size);
            if (ifs_a.gcount() != static_cast<std::streamsize>(block_size)) {
                std::cerr << "[Datanode" << m_port << "][StripeMergeParity] parity A short read: " << path_a << std::endl;
                response->set_message(false);
                return grpc::Status::OK;
            }
            ifs_a.close();
        } else {
            memset(buf_a.get(), 0, static_cast<size_t>(block_size));
            deserialize(path_a, buf_a.get());
        }

        bool loaded_b = false;
        if (access(path_b.c_str(), 0) != -1) {
            struct stat stb {};
            if (stat(path_b.c_str(), &stb) == 0 &&
                static_cast<size_t>(stb.st_size) == static_cast<size_t>(block_size)) {
                std::ifstream ifs_b(path_b, std::ios::binary);
                ifs_b.read(buf_b.get(), block_size);
                if (ifs_b.gcount() == static_cast<std::streamsize>(block_size))
                    loaded_b = true;
                ifs_b.close();
            } else if (access(path_b.c_str(), 0) != -1) {
                memset(buf_b.get(), 0, static_cast<size_t>(block_size));
                deserialize(path_b, buf_b.get());
                loaded_b = true;
            }
        } else if (!parity_b_ip.empty() && parity_b_port > 0) {
            const std::string peer = parity_b_ip + ":" + std::to_string(parity_b_port);
            grpc::Status read_st = grpc::Status::OK;
            for (int attempt = 0; attempt < 5 && !loaded_b; ++attempt) {
                auto channel = grpc::CreateChannel(peer, grpc::InsecureChannelCredentials());
                (void)channel->GetState(true);
                auto stub = datanode_proto::datanodeService::NewStub(channel);
                grpc::ClientContext read_ctx;
                read_ctx.set_deadline(std::chrono::system_clock::now() + std::chrono::seconds(60));
                datanode_proto::ReadBlockBytesRequest read_req;
                datanode_proto::ReadBlockBytesReply read_rep;
                read_req.set_block_key(parity_key_b);
                read_req.set_block_size(block_size);
                read_st = stub->readBlockBytes(&read_ctx, read_req, &read_rep);
                if (read_st.ok() && read_rep.ok() &&
                    read_rep.data().size() == static_cast<size_t>(block_size)) {
                    memcpy(buf_b.get(), read_rep.data().data(), static_cast<size_t>(block_size));
                    loaded_b = true;
                    break;
                }
                std::this_thread::sleep_for(std::chrono::milliseconds(80 * (attempt + 1)));
            }
            if (!loaded_b) {
                std::cerr << "[Datanode" << m_port << "][StripeMergeParity] remote read parity B failed peer="
                          << peer << " grpc=" << read_st.error_code() << " "
                          << read_st.error_message() << std::endl;
            }
        }

        if (!loaded_b) {
            std::cerr << "[Datanode" << m_port
                      << "][StripeMergeParity] parity B not found locally or remotely: "
                      << parity_key_b << std::endl;
            response->set_message(false);
            return grpc::Status::OK;
        }

        // P'_j = P^A_j XOR gf_mul(coeff, P^B_j)
        for (int i = 0; i < block_size; i++) {
            unsigned char a_byte = static_cast<unsigned char>(buf_a[i]);
            unsigned char b_byte = static_cast<unsigned char>(buf_b[i]);
            buf_new[i] = static_cast<char>(a_byte ^ ECProject::gf_mul(coeff, b_byte));
        }

        if (access(targetdir.c_str(), 0) == -1) {
            createDirectories(targetdir);
        }

        std::ofstream ofs(path_new, std::ios::binary | std::ios::out | std::ios::trunc);
        ofs.write(buf_new.get(), block_size);
        ofs.flush();
        ofs.close();

        std::cout << "[Datanode" << m_port << "][StripeMergeParity] merged "
                  << parity_key_a << " + coeff*" << parity_key_b
                  << " -> " << new_parity_key << std::endl;

        response->set_message(true);
        response->set_exec_seconds(0.0);
        return grpc::Status::OK;
    }

    grpc::Status DatanodeImpl::handleDelete(
        grpc::ServerContext *context,
        const datanode_proto::DelInfo *del_info,
        datanode_proto::RequestResult *response)
    {
        std::string block_key = del_info->block_key();
        std::string file_path = "./storage/" + std::to_string(m_port) + "/" + block_key;
        if (IF_DEBUG)
        {
            std::cout << "[Datanode" << m_port << "] File path:" << file_path << std::endl;
        }
        if (remove(file_path.c_str()))
        {
            std::cout << "[DEL] delete error!" << std::endl;
        }
        response->set_message(true);
        return grpc::Status::OK;
    }
}