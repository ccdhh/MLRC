#include "proxy.h"
#include "config.h"
#include "link_bandwidth.h"
#include "unilrc_encoder.h"
#include <asio.hpp>
#include <algorithm>
#include <chrono>
#include <cstdint>
#include <cstdio>
#include <cstring>
#include <iostream>
#include <string>
#include <atomic>
#if defined(__linux__) || defined(__APPLE__)
#include <sys/socket.h>
#endif
#include <condition_variable>
#include <deque>
#include <map>
#include <mutex>
#include <thread>
#include <unordered_set>
#include <vector>

namespace ECProject
{
namespace
{
constexpr uint32_t kPhase2Magic = 0x50483232u; // "PH22" per-shard peer exchange frame
constexpr uint32_t kPhase2HelloMagic = 0x50483248u; // "PH2H" peer connect hello

struct Phase2ConnectHello
{
  uint32_t magic;
  uint32_t from_partition;
};

thread_local std::string g_glrc_phase2_last_error;

std::mutex g_phase2_bind_mutex;
std::unordered_set<int> g_phase2_active_listen_ports;

void phase2_trace(const char *msg)
{
  FILE *f = fopen("/users/chendh/DdlRT/logs/phase2_trace.log", "a");
  if (f)
  {
    fprintf(f, "%s\n", msg);
    fclose(f);
  }
}

void set_phase2_error(const std::string &msg)
{
  g_glrc_phase2_last_error = msg;
  phase2_trace(msg.c_str());
}

void set_exchange_socket_timeouts(asio::ip::tcp::socket &socket, int seconds)
{
  if (seconds <= 0)
    return;
#if defined(__linux__) || defined(__APPLE__)
  struct timeval tv;
  tv.tv_sec = seconds;
  tv.tv_usec = 0;
  const auto native = socket.native_handle();
  setsockopt(native, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof(tv));
  setsockopt(native, SOL_SOCKET, SO_SNDTIMEO, &tv, sizeof(tv));
#endif
}

struct Phase2PeerHeader
{
  uint32_t magic;
  uint32_t from_partition;
  uint32_t stripe_byte_len;
  uint32_t shard_begin;
  uint32_t shard_count;
};

bool lookup_peer_shards(const proxy_proto::RecoveryRequest *req, int peer_part, int &shard_begin, int &shard_count)
{
  for (int k = 0; k < req->phase2_peer_partition_ids_size(); k++)
  {
    if (req->phase2_peer_partition_ids(k) == peer_part)
    {
      shard_begin = req->phase2_peer_shard_begins(k);
      shard_count = req->phase2_peer_shard_counts(k);
      return true;
    }
  }
  return false;
}

int phase2_proxy_index(int proxy_grpc_port)
{
  const int idx = (proxy_grpc_port - PROXY_GRPC_BASE) / PROXY_GRPC_STRIDE;
  return idx < 0 ? 0 : idx;
}

int phase2_exchange_port(int proxy_grpc_port, int partition_id, int exchange_epoch)
{
  const int proxy_idx = phase2_proxy_index(proxy_grpc_port);
  const int epoch_slot = exchange_epoch % 128;
  return PROXY_PHASE2_EXCHANGE_BASE + proxy_idx * PROXY_PHASE2_PER_PROXY_BAND + epoch_slot * 16 +
         partition_id;
}

void close_phase2_socket(asio::ip::tcp::socket &socket)
{
  // Graceful close (FIN, not RST): peer must be able to drain buffered reply
  // bytes before EOF. An abortive close (SO_LINGER 0) would surface as
  // "Connection reset by peer" on the still-reading side.
  asio::error_code ec;
  socket.shutdown(asio::ip::tcp::socket::shutdown_both, ec);
  socket.close(ec);
}

bool write_all(asio::ip::tcp::socket &socket, const char *data, size_t len, asio::error_code &ec)
{
  ec.clear();
  size_t off = 0;
  while (off < len && !ec)
  {
    off += asio::write(socket, asio::buffer(data + off, len - off), ec);
  }
  return !ec && off == len;
}

bool read_all(asio::ip::tcp::socket &socket, char *data, size_t len, asio::error_code &ec)
{
  ec.clear();
  size_t off = 0;
  while (off < len && !ec)
  {
    off += asio::read(socket, asio::buffer(data + off, len - off), ec);
  }
  return !ec && off == len;
}

bool write_all_bw(asio::ip::tcp::socket &socket, const char *data, size_t len, SharedBandwidthLimiter *bw,
                  asio::error_code &ec)
{
  ec.clear();
  if (bw == nullptr || bw->bandwidth_mbps() <= 0.0)
    return write_all(socket, data, len, ec);
  tcp_write_with_shared_bandwidth(socket, data, len, bw, ec);
  return !ec;
}

bool read_all_bw(asio::ip::tcp::socket &socket, char *data, size_t len, SharedBandwidthLimiter *bw, asio::error_code &ec)
{
  ec.clear();
  if (bw == nullptr || bw->bandwidth_mbps() <= 0.0)
    return read_all(socket, data, len, ec);
  tcp_read_with_shared_bandwidth(socket, data, len, bw, ec);
  return !ec;
}

Phase2PeerHeader make_shard_header(int from_partition, int global_shard, int stripe_byte_len)
{
  Phase2PeerHeader hdr{};
  hdr.magic = kPhase2Magic;
  hdr.from_partition = static_cast<uint32_t>(from_partition);
  hdr.stripe_byte_len = static_cast<uint32_t>(stripe_byte_len);
  hdr.shard_begin = static_cast<uint32_t>(global_shard);
  hdr.shard_count = 1;
  return hdr;
}

bool send_shard_frame(asio::ip::tcp::socket &socket, const Phase2PeerHeader &hdr, const char *payload,
                      SharedBandwidthLimiter *bw, asio::error_code &ec)
{
  if (!write_all_bw(socket, reinterpret_cast<const char *>(&hdr), sizeof(hdr), bw, ec))
    return false;
  return write_all_bw(socket, payload, hdr.stripe_byte_len, bw, ec);
}

bool recv_shard_frame(asio::ip::tcp::socket &socket, Phase2PeerHeader &hdr, char *payload, size_t payload_cap,
                      SharedBandwidthLimiter *ingress_bw, asio::error_code &ec)
{
  if (!read_all_bw(socket, reinterpret_cast<char *>(&hdr), sizeof(hdr), ingress_bw, ec))
    return false;
  if (hdr.magic != kPhase2Magic || hdr.shard_count != 1 || hdr.stripe_byte_len == 0 ||
      hdr.stripe_byte_len > payload_cap)
  {
    ec = asio::error::invalid_argument;
    return false;
  }
  return read_all_bw(socket, payload, hdr.stripe_byte_len, ingress_bw, ec);
}

bool connect_phase2_exchange(asio::ip::tcp::socket &socket, const std::string &peer_ip, int listen_port,
                             asio::error_code &ec, int max_retries = 500)
{
  const auto endpoint = asio::ip::tcp::endpoint(asio::ip::address::from_string(peer_ip), listen_port);
  for (int attempt = 0; attempt < max_retries; attempt++)
  {
    ec.clear();
    if (socket.is_open())
    {
      socket.close(ec);
      ec.clear();
    }
    socket.connect(endpoint, ec);
    if (!ec)
      return true;
    if (ec != asio::error::connection_refused)
      break;
    std::this_thread::sleep_for(std::chrono::milliseconds(20));
  }
  return false;
}

bool bind_phase2_acceptor(asio::ip::tcp::acceptor &acceptor, const asio::ip::tcp::endpoint &listen_ep,
                          int listen_port, asio::error_code &ec, int max_retries = 100)
{
  for (int retry = 0; retry < max_retries; retry++)
  {
    ec.clear();
    acceptor.open(listen_ep.protocol(), ec);
    if (ec)
      return false;
    acceptor.set_option(asio::ip::tcp::acceptor::reuse_address(true), ec);
    if (ec)
    {
      acceptor.close(ec);
      return false;
    }
    {
      std::lock_guard<std::mutex> lock(g_phase2_bind_mutex);
      acceptor.bind(listen_ep, ec);
    }
    if (!ec)
    {
      acceptor.listen(asio::socket_base::max_listen_connections, ec);
      if (!ec)
      {
        std::lock_guard<std::mutex> lock(g_phase2_bind_mutex);
        g_phase2_active_listen_ports.insert(listen_port);
        return true;
      }
    }
    acceptor.close(ec);
    if (ec != asio::error::address_in_use)
      return false;
    std::this_thread::sleep_for(std::chrono::milliseconds(50));
  }
  return false;
}

void release_phase2_listen_port(int listen_port)
{
  if (listen_port <= 0)
    return;
  std::lock_guard<std::mutex> lock(g_phase2_bind_mutex);
  g_phase2_active_listen_ports.erase(listen_port);
}

class JoinThreadGuard
{
public:
  explicit JoinThreadGuard(std::thread &t) : thread_(t) {}
  ~JoinThreadGuard()
  {
    if (thread_.joinable())
      thread_.join();
  }
  JoinThreadGuard(const JoinThreadGuard &) = delete;
  JoinThreadGuard &operator=(const JoinThreadGuard &) = delete;

private:
  std::thread &thread_;
};
} // namespace

ProxyImpl::Phase2BlockDuplexBw ProxyImpl::phase2BlockDuplexBandwidth(int repair_block_id, int exchange_epoch)
{
  Phase2BlockDuplexBw out;
  if (m_sys_config == nullptr || m_sys_config->NodeBlockBandwidthMBps <= 0.0)
    return out;
  std::lock_guard<std::mutex> lock(m_glrc_phase2_mutex);
  if (m_phase2_block_bw_epoch != exchange_epoch)
  {
    m_phase2_block_ingress_bw.clear();
    m_phase2_block_egress_bw.clear();
    m_phase2_block_bw_epoch = exchange_epoch;
  }
  const double mbps = m_sys_config->NodeBlockBandwidthMBps;
  auto &in_slot = m_phase2_block_ingress_bw[repair_block_id];
  auto &out_slot = m_phase2_block_egress_bw[repair_block_id];
  if (!in_slot)
    in_slot = std::make_shared<SharedBandwidthLimiter>(mbps);
  if (!out_slot)
    out_slot = std::make_shared<SharedBandwidthLimiter>(mbps);
  out.ingress = in_slot.get();
  out.egress = out_slot.get();
  return out;
}

bool ProxyImpl::GetFromDatanodeStripeRangeBreakdown(const std::string &key, char *value, size_t full_block_size,
                                                    int read_offset, int read_length, const char *ip, const int port,
                                                    double *disk_io_start_time, double *disk_io_end_time,
                                                    double *network_start_time, double *network_end_time,
                                                    double *grpc_notify_time, double *grpc_start_time,
                                                    SharedBandwidthLimiter *block_bandwidth)
{
  try
  {
    grpc::ClientContext context;
    datanode_proto::GetInfo get_info;
    datanode_proto::RequestResult result;
    get_info.set_block_key(key);
    get_info.set_block_size(static_cast<int>(full_block_size));
    get_info.set_read_offset(read_offset);
    get_info.set_read_length(read_length);
    get_info.set_proxy_ip(m_ip);
    get_info.set_proxy_port(m_port);
    std::string node_ip_port = std::string(ip) + ":" + std::to_string(port);
    auto dn_it = m_datanode_ptrs.find(node_ip_port);
    if (dn_it == m_datanode_ptrs.end() || !dn_it->second)
    {
      std::cerr << "[Proxy" << m_self_cluster_id << "][gLRC ILP Phase2] datanode stub missing: " << node_ip_port
                << std::endl;
      return false;
    }
    context.set_deadline(std::chrono::system_clock::now() + std::chrono::seconds(120));
    auto grpc_notify = std::chrono::high_resolution_clock::now();
    {
      char tb[320];
      snprintf(tb, sizeof(tb), "helper grpc call key=%s @ %s off=%d len=%d proxy=%s:%d", key.c_str(),
               node_ip_port.c_str(), read_offset, read_length, m_ip.c_str(), m_port);
      phase2_trace(tb);
    }
    grpc::Status stat = dn_it->second->handleGetBreakdown(&context, get_info, &result);
    if (!stat.ok())
    {
      char tb[384];
      snprintf(tb, sizeof(tb), "helper grpc failed key=%s @ %s off=%d len=%d : %s", key.c_str(),
               node_ip_port.c_str(), read_offset, read_length, stat.error_message().c_str());
      phase2_trace(tb);
      std::cerr << "[Proxy" << m_self_cluster_id << "][gLRC ILP Phase2] " << tb << std::endl;
      return false;
    }

    if (disk_io_start_time)
      *disk_io_start_time = result.disk_io_start_time();
    if (disk_io_end_time)
      *disk_io_end_time = result.disk_io_end_time();
    if (grpc_notify_time)
      *grpc_notify_time =
          std::chrono::duration_cast<std::chrono::duration<double>>(grpc_notify.time_since_epoch()).count();
    if (grpc_start_time)
      *grpc_start_time = result.grpc_start_time();

    asio::io_context io_context;
    asio::ip::tcp::resolver resolver(io_context);
    asio::ip::tcp::socket socket(io_context);
    auto begin = std::chrono::high_resolution_clock::now();
    const int data_port = result.data_port();
    {
      char tb[256];
      snprintf(tb, sizeof(tb), "helper grpc ok key=%s @ %s:%d off=%d len=%d data_port=%d", key.c_str(), ip, port,
               read_offset, read_length, data_port);
      phase2_trace(tb);
    }
    asio::error_code connect_ec;
    asio::connect(socket, resolver.resolve({std::string(ip), std::to_string(data_port)}), connect_ec);
    if (connect_ec)
    {
      std::cerr << "[Proxy" << m_self_cluster_id << "][gLRC ILP Phase2] helper tcp connect failed key=" << key << " @ "
                << ip << ":" << data_port << " " << connect_ec.message() << std::endl;
      return false;
    }

    // Bound the data read so a stalled helper transfer surfaces as a logged error
    // instead of hanging the whole partition (and the coordinator) indefinitely.
    set_exchange_socket_timeouts(socket, 45);
    asio::error_code ec;
    // nullptr block_bandwidth = unlimited (pipeline co-located local stripe reads).
    tcp_read_with_shared_bandwidth(socket, value + read_offset, static_cast<size_t>(read_length), block_bandwidth,
                                   ec);
    if (ec)
    {
      char tb[256];
      snprintf(tb, sizeof(tb), "helper tcp read failed key=%s @ %s:%d len=%d : %s", key.c_str(), ip, data_port,
               read_length, ec.message().c_str());
      set_phase2_error(tb);
      std::cerr << "[Proxy" << m_self_cluster_id << "][gLRC ILP Phase2] " << tb << std::endl;
      return false;
    }
    {
      char tb[256];
      snprintf(tb, sizeof(tb), "helper read done key=%s @ %s:%d len=%d", key.c_str(), ip, data_port, read_length);
      phase2_trace(tb);
    }
    asio::error_code ignore_ec;
    socket.shutdown(asio::ip::tcp::socket::shutdown_both, ignore_ec);
    socket.close(ignore_ec);
    auto end = std::chrono::high_resolution_clock::now();
    if (network_start_time)
      *network_start_time = std::chrono::duration_cast<std::chrono::duration<double>>(begin.time_since_epoch()).count();
    if (network_end_time)
      *network_end_time = std::chrono::duration_cast<std::chrono::duration<double>>(end.time_since_epoch()).count();
    return true;
  }
  catch (const std::exception &e)
  {
    std::cerr << e.what() << '\n';
    return false;
  }
}

bool ProxyImpl::GetFromDatanodeStripeRangeCompactBreakdown(const std::string &key, char *value, size_t full_block_size,
                                                           int read_offset, int read_length, const char *ip,
                                                           const int port, double *disk_io_start_time,
                                                           double *disk_io_end_time, double *network_start_time,
                                                           double *network_end_time, double *grpc_notify_time,
                                                           double *grpc_start_time,
                                                           SharedBandwidthLimiter *block_bandwidth)
{
  try
  {
    grpc::ClientContext context;
    datanode_proto::GetInfo get_info;
    datanode_proto::RequestResult result;
    get_info.set_block_key(key);
    get_info.set_block_size(static_cast<int>(full_block_size));
    get_info.set_read_offset(read_offset);
    get_info.set_read_length(read_length);
    get_info.set_proxy_ip(m_ip);
    get_info.set_proxy_port(m_port);
    std::string node_ip_port = std::string(ip) + ":" + std::to_string(port);
    auto dn_it = m_datanode_ptrs.find(node_ip_port);
    if (dn_it == m_datanode_ptrs.end() || !dn_it->second)
    {
      std::cerr << "[Proxy" << m_self_cluster_id << "][gLRC ILP Phase2] datanode stub missing: " << node_ip_port
                << std::endl;
      return false;
    }
    context.set_deadline(std::chrono::system_clock::now() + std::chrono::seconds(120));
    auto grpc_notify = std::chrono::high_resolution_clock::now();
    grpc::Status stat = dn_it->second->handleGetBreakdown(&context, get_info, &result);
    if (!stat.ok())
    {
      char tb[384];
      snprintf(tb, sizeof(tb), "helper grpc failed key=%s @ %s off=%d len=%d : %s", key.c_str(),
               node_ip_port.c_str(), read_offset, read_length, stat.error_message().c_str());
      phase2_trace(tb);
      std::cerr << "[Proxy" << m_self_cluster_id << "][gLRC ILP Phase2] " << tb << std::endl;
      return false;
    }

    if (disk_io_start_time)
      *disk_io_start_time = result.disk_io_start_time();
    if (disk_io_end_time)
      *disk_io_end_time = result.disk_io_end_time();
    if (grpc_notify_time)
      *grpc_notify_time =
          std::chrono::duration_cast<std::chrono::duration<double>>(grpc_notify.time_since_epoch()).count();
    if (grpc_start_time)
      *grpc_start_time = result.grpc_start_time();

    asio::io_context io_context;
    asio::ip::tcp::resolver resolver(io_context);
    asio::ip::tcp::socket socket(io_context);
    auto begin = std::chrono::high_resolution_clock::now();
    const int data_port = result.data_port();
    asio::error_code connect_ec;
    asio::connect(socket, resolver.resolve({std::string(ip), std::to_string(data_port)}), connect_ec);
    if (connect_ec)
    {
      std::cerr << "[Proxy" << m_self_cluster_id << "][gLRC ILP Phase2] helper tcp connect failed key=" << key << " @ "
                << ip << ":" << data_port << " " << connect_ec.message() << std::endl;
      return false;
    }

    set_exchange_socket_timeouts(socket, 45);
    asio::error_code ec;
    tcp_read_with_shared_bandwidth(socket, value, static_cast<size_t>(read_length), block_bandwidth, ec);
    if (ec)
    {
      char tb[256];
      snprintf(tb, sizeof(tb), "helper tcp compact read failed key=%s @ %s:%d len=%d : %s", key.c_str(), ip,
               data_port, read_length, ec.message().c_str());
      set_phase2_error(tb);
      std::cerr << "[Proxy" << m_self_cluster_id << "][gLRC ILP Phase2] " << tb << std::endl;
      return false;
    }
    asio::error_code ignore_ec;
    socket.shutdown(asio::ip::tcp::socket::shutdown_both, ignore_ec);
    socket.close(ignore_ec);
    auto end = std::chrono::high_resolution_clock::now();
    if (network_start_time)
      *network_start_time = std::chrono::duration_cast<std::chrono::duration<double>>(begin.time_since_epoch()).count();
    if (network_end_time)
      *network_end_time = std::chrono::duration_cast<std::chrono::duration<double>>(end.time_since_epoch()).count();
    return true;
  }
  catch (const std::exception &e)
  {
    std::cerr << e.what() << '\n';
    return false;
  }
}

void ProxyImpl::get_from_node_stripe_range_breakdown(const std::string &block_key, char *block_value,
                                                     size_t full_block_size, int read_offset, int read_length,
                                                     const char *datanode_ip, const int datanode_port, bool *status,
                                                     int index, double *disk_io_start_time, double *disk_io_end_time,
                                                     double *network_start_time, double *network_end_time,
                                                     double *grpc_notify_time, double *grpc_start_time,
                                                     SharedBandwidthLimiter *block_bandwidth)
{
  status[index] = GetFromDatanodeStripeRangeBreakdown(block_key, block_value, full_block_size, read_offset, read_length,
                                                      datanode_ip, datanode_port, disk_io_start_time, disk_io_end_time,
                                                      network_start_time, network_end_time, grpc_notify_time,
                                                      grpc_start_time, block_bandwidth);
}

std::string glrc_phase2_take_last_error()
{
  return g_glrc_phase2_last_error;
}

bool ProxyImpl::glrcIlpPhase2Recovery(const proxy_proto::RecoveryRequest *recovery_request,
                                      proxy_proto::RecoveryReply *response)
{
  g_glrc_phase2_last_error.clear();
  const int exchange_epoch = recovery_request->phase2_exchange_epoch();
  const int partition_id = recovery_request->phase2_partition_id();
  {
    char buf[256];
    snprintf(buf, sizeof(buf), "enter proxy=%s:%d partition=%d", m_ip.c_str(), m_port, partition_id);
    phase2_trace(buf);
  }
  if (partition_id < 0 || partition_id >= recovery_request->failed_block_ids_size())
  {
    set_phase2_error("invalid partition_id");
    return false;
  }
  const int repair_block_id = recovery_request->failed_block_ids(partition_id);
  const Phase2BlockDuplexBw block_bw = phase2BlockDuplexBandwidth(repair_block_id, exchange_epoch);
  const int f = recovery_request->failed_block_ids_size();
  const int byte_off = recovery_request->phase2_byte_off();
  const int byte_len = recovery_request->phase2_byte_len();
  const int stripe_byte_len = recovery_request->phase2_stripe_byte_len();
  const int block_size = m_sys_config->BlockSize;
  const int helper_n = recovery_request->datanodeip_size();

  if (f < 1 || helper_n < 1 || byte_len < 1 || stripe_byte_len < 1)
  {
    char buf[256];
    snprintf(buf, sizeof(buf), "bad dims f=%d helpers=%d byte_len=%d stripe_len=%d", f, helper_n, byte_len,
             stripe_byte_len);
    set_phase2_error(buf);
    return false;
  }

  int accept_remaining = 0;
  for (int peer = 0; peer < recovery_request->phase2_peer_partition_ids_size(); peer++)
  {
    if (recovery_request->phase2_peer_partition_ids(peer) < partition_id)
      accept_remaining++;
  }

  asio::io_context accept_io;
  std::unique_ptr<asio::ip::tcp::acceptor> acceptor;
  std::mutex incoming_mu;
  std::condition_variable incoming_cv;
  std::map<int, std::unique_ptr<asio::ip::tcp::socket>> server_peer_socks;
  std::atomic<bool> accept_failed{false};
  std::atomic<bool> accept_stop{false};
  std::thread accept_thread;
  bool accept_started = false;
  int listen_port = 0;

  auto stop_acceptor = [&]() {
    accept_stop.store(true);
    if (acceptor)
    {
      asio::error_code close_ec;
      acceptor->close(close_ec);
    }
  };
  auto join_accept_thread = [&]() {
    if (!accept_started)
      return;
    stop_acceptor();
    incoming_cv.notify_all();
    if (accept_thread.joinable())
      accept_thread.join();
    accept_started = false;
    release_phase2_listen_port(listen_port);
    listen_port = 0;
  };
  auto start_exchange_acceptor = [&]() -> bool {
    if (accept_remaining <= 0 || accept_started)
      return true;
    accept_stop.store(false);
    accept_failed.store(false);
    listen_port = phase2_exchange_port(m_port, partition_id, exchange_epoch);
    try
    {
      acceptor = std::make_unique<asio::ip::tcp::acceptor>(accept_io);
      asio::ip::tcp::endpoint listen_ep(asio::ip::address::from_string(m_ip), listen_port);
      asio::error_code bind_ec;
      if (!bind_phase2_acceptor(*acceptor, listen_ep, listen_port, bind_ec))
      {
        char buf[384];
        snprintf(buf, sizeof(buf), "exchange bind failed port=%d: %s", listen_port, bind_ec.message().c_str());
        set_phase2_error(buf);
        std::cerr << "[Proxy" << m_self_cluster_id << "][gLRC ILP Phase2] partition " << partition_id << " " << buf
                  << std::endl;
        acceptor.reset();
        return false;
      }
      acceptor->non_blocking(true);
    }
    catch (const std::exception &e)
    {
      char buf[384];
      snprintf(buf, sizeof(buf), "exchange bind failed port=%d: %s", listen_port, e.what());
      set_phase2_error(buf);
      std::cerr << "[Proxy" << m_self_cluster_id << "][gLRC ILP Phase2] partition " << partition_id << " " << buf
                << std::endl;
      acceptor.reset();
      return false;
    }
    accept_thread = std::thread([&]() {
      int accepted = 0;
      while (!accept_stop.load() && accepted < accept_remaining)
      {
        auto sock = std::make_unique<asio::ip::tcp::socket>(accept_io);
        asio::error_code accept_ec;
        acceptor->accept(*sock, accept_ec);
        if (accept_ec)
        {
          if (accept_ec == asio::error::would_block || accept_ec == asio::error::try_again)
          {
            std::this_thread::sleep_for(std::chrono::milliseconds(5));
            continue;
          }
          if (!accept_stop.load())
            accept_failed.store(true);
          break;
        }
        set_exchange_socket_timeouts(*sock, 45);
        asio::error_code ec;
        Phase2ConnectHello hello{};
        if (!read_all(*sock, reinterpret_cast<char *>(&hello), sizeof(hello), ec) ||
            hello.magic != kPhase2HelloMagic)
        {
          if (!accept_stop.load())
            accept_failed.store(true);
          close_phase2_socket(*sock);
          break;
        }
        const int from_part = static_cast<int>(hello.from_partition);
        {
          std::lock_guard<std::mutex> lk(incoming_mu);
          server_peer_socks[from_part] = std::move(sock);
        }
        incoming_cv.notify_all();
        accepted++;
      }
    });
    accept_started = true;
    return true;
  };

  if (!start_exchange_acceptor())
    return false;

  const int shard_begin = recovery_request->phase2_shard_begin();
  const int local_shard_count = recovery_request->phase2_shard_count_local();

  int max_exchange_rounds = local_shard_count;
  for (int peer = 0; peer < recovery_request->phase2_peer_partition_ids_size(); peer++)
  {
    const int peer_part = recovery_request->phase2_peer_partition_ids(peer);
    if (peer_part == partition_id)
      continue;
    int peer_shard_count = 0;
    int peer_shard_begin = 0;
    if (lookup_peer_shards(recovery_request, peer_part, peer_shard_begin, peer_shard_count))
      max_exchange_rounds = std::max(max_exchange_rounds, peer_shard_count);
  }

  std::vector<int> failed_ids;
  std::vector<int> eq_indices;
  for (int i = 0; i < f; i++)
    failed_ids.push_back(recovery_request->failed_block_ids(i));
  for (int i = 0; i < recovery_request->selected_equation_indices_size(); i++)
    eq_indices.push_back(recovery_request->selected_equation_indices(i));
  if ((int)eq_indices.size() != f)
  {
    set_phase2_error("selected_equation_indices size mismatch");
    return false;
  }

  std::vector<int> block_idxs;
  for (int i = 0; i < helper_n; i++)
    block_idxs.push_back(recovery_request->blockids(i));

  std::vector<unsigned char> phase2_decode_inverse;
  std::vector<std::vector<int>> phase2_eq_helper_indices;
  std::vector<std::vector<unsigned char>> phase2_eq_helper_coefs;
  if (!glrc_ilp_prepare_helper_decode(m_sys_config->k, m_sys_config->r, m_sys_config->z, block_idxs, failed_ids,
                                      eq_indices, phase2_decode_inverse, phase2_eq_helper_indices,
                                      phase2_eq_helper_coefs))
  {
    set_phase2_error("phase2 prepare helper decode failed");
    return false;
  }

  std::vector<char *> get_bufs(helper_n);
  for (int i = 0; i < helper_n; i++)
  {
    get_bufs[i] = static_cast<char *>(std::aligned_alloc(32, block_size));
    if (get_bufs[i] == nullptr)
    {
      set_phase2_error("helper buffer alloc failed");
      for (int j = 0; j < i; j++)
        free(get_bufs[j]);
      join_accept_thread();
      return false;
    }
    std::memset(get_bufs[i], 0, block_size);
  }

  std::vector<unsigned char *> recovered_ptrs(f);
  for (int i = 0; i < f; i++)
  {
    recovered_ptrs[i] = new unsigned char[block_size];
    std::memset(recovered_ptrs[i], 0, block_size);
  }

  double min_disk_start = 0.0, max_disk_end = 0.0;
  double min_net_start = 0.0, max_net_end = 0.0;
  double min_decode_start = 0.0, max_decode_end = 0.0;
  double total_decode_time = 0.0;
  double min_grpc_notify = 0.0, max_grpc_start = 0.0;
  bool have_disk = false, have_net = false, have_decode = false;
  bool have_grpc = false;

  constexpr int kExchangeSocketTimeoutSec = 45;
  std::atomic<bool> exchange_failed{false};
  const auto ex_begin = std::chrono::high_resolution_clock::now();

  struct ClientPeerConn
  {
    bool connected = false;
    std::unique_ptr<asio::ip::tcp::socket> sock;
  };
  std::mutex client_peer_mu;
  std::map<int, ClientPeerConn> client_peer_socks;

  auto ensure_client_peer = [&](int peer_part, const std::string &peer_ip, int peer_port) -> asio::ip::tcp::socket * {
    {
      std::lock_guard<std::mutex> lk(client_peer_mu);
      ClientPeerConn &slot = client_peer_socks[peer_part];
      if (slot.connected && slot.sock)
        return slot.sock.get();
    }

    asio::error_code ec;
    const int target_port = phase2_exchange_port(peer_port, peer_part, exchange_epoch);
    auto connected_sock = std::make_unique<asio::ip::tcp::socket>(accept_io);
    if (!connect_phase2_exchange(*connected_sock, peer_ip, target_port, ec))
    {
      char buf[384];
      snprintf(buf, sizeof(buf), "connect to peer %d @ %s:%d failed: %s", peer_part, peer_ip.c_str(), target_port,
               ec.message().c_str());
      set_phase2_error(buf);
      phase2_trace(buf);
      exchange_failed.store(true);
      return nullptr;
    }
    set_exchange_socket_timeouts(*connected_sock, kExchangeSocketTimeoutSec);
    Phase2ConnectHello hello{};
    hello.magic = kPhase2HelloMagic;
    hello.from_partition = static_cast<uint32_t>(partition_id);
    if (!write_all(*connected_sock, reinterpret_cast<const char *>(&hello), sizeof(hello), ec))
    {
      set_phase2_error("hello write to peer " + std::to_string(peer_part) + " failed: " + ec.message());
      close_phase2_socket(*connected_sock);
      exchange_failed.store(true);
      return nullptr;
    }

    std::lock_guard<std::mutex> lk(client_peer_mu);
    ClientPeerConn &slot = client_peer_socks[peer_part];
    slot.sock = std::move(connected_sock);
    slot.connected = true;
    return slot.sock.get();
  };

  auto ensure_server_peer = [&](int peer_part) -> asio::ip::tcp::socket * {
    std::unique_lock<std::mutex> lk(incoming_mu);
    const auto wait_deadline = std::chrono::steady_clock::now() + std::chrono::seconds(90);
    while (server_peer_socks.find(peer_part) == server_peer_socks.end() && !accept_failed.load() &&
           !exchange_failed.load())
    {
      if (incoming_cv.wait_until(lk, wait_deadline) == std::cv_status::timeout)
        break;
    }
    if (server_peer_socks.find(peer_part) == server_peer_socks.end())
    {
      set_phase2_error("exchange wait peer " + std::to_string(peer_part) + " timed out");
      exchange_failed.store(true);
      return nullptr;
    }
    return server_peer_socks[peer_part].get();
  };

  auto setup_peer_connections = [&]() {
    std::vector<std::thread> setup_threads;
    for (int peer = 0; peer < recovery_request->phase2_peer_partition_ids_size(); peer++)
    {
      const int peer_part = recovery_request->phase2_peer_partition_ids(peer);
      if (peer_part == partition_id)
        continue;
      if (peer_part < 0 || peer_part >= f)
      {
        exchange_failed.store(true);
        continue;
      }
      setup_threads.emplace_back([&, peer_part, peer]() {
        if (exchange_failed.load())
          return;
        const int peer_port = recovery_request->phase2_peer_proxy_ports(peer);
        const std::string peer_ip = recovery_request->phase2_peer_proxy_ips(peer);
        if (partition_id < peer_part)
        {
          if (ensure_client_peer(peer_part, peer_ip, peer_port) == nullptr)
            return;
        }
        else
        {
          if (ensure_server_peer(peer_part) == nullptr)
            return;
        }
      });
    }
    for (auto &th : setup_threads)
    {
      if (th.joinable())
        th.join();
    }
  };

  setup_peer_connections();
  if (exchange_failed.load() || accept_failed.load())
  {
    for (char *p : get_bufs)
      free(p);
    for (unsigned char *p : recovered_ptrs)
      delete[] p;
    join_accept_thread();
    return false;
  }

  auto exchange_shard_with_peer = [&](int shard_k) {
    if (exchange_failed.load())
      return;
    std::vector<std::thread> peer_threads;
    for (int peer = 0; peer < recovery_request->phase2_peer_partition_ids_size(); peer++)
    {
      const int peer_part = recovery_request->phase2_peer_partition_ids(peer);
      if (peer_part == partition_id)
        continue;
      if (peer_part < 0 || peer_part >= f)
      {
        exchange_failed.store(true);
        continue;
      }

      peer_threads.emplace_back([&, peer_part, peer]() {
        if (exchange_failed.load())
          return;

        int peer_shard_begin = 0;
        int peer_shard_count = 0;
        if (!lookup_peer_shards(recovery_request, peer_part, peer_shard_begin, peer_shard_count))
        {
          exchange_failed.store(true);
          return;
        }

        const int peer_port = recovery_request->phase2_peer_proxy_ports(peer);
        const std::string peer_ip = recovery_request->phase2_peer_proxy_ips(peer);
        const int global_shard = shard_begin + shard_k;
        const int stripe_off = global_shard * stripe_byte_len;
        asio::error_code ec;

        if (partition_id < peer_part)
        {
          asio::ip::tcp::socket *sock = ensure_client_peer(peer_part, peer_ip, peer_port);
          if (sock == nullptr)
            return;
          if (shard_k < local_shard_count)
          {
            Phase2PeerHeader out_hdr = make_shard_header(partition_id, global_shard, stripe_byte_len);
            if (!send_shard_frame(*sock, out_hdr, reinterpret_cast<char *>(recovered_ptrs[peer_part] + stripe_off),
                                  block_bw.egress, ec))
            {
              set_phase2_error("client shard send to peer " + std::to_string(peer_part) + " failed: " + ec.message());
              exchange_failed.store(true);
              return;
            }
          }
          if (shard_k < peer_shard_count)
          {
            Phase2PeerHeader rhdr{};
            std::vector<char> recv_stripe(stripe_byte_len);
            if (!recv_shard_frame(*sock, rhdr, recv_stripe.data(), recv_stripe.size(), block_bw.ingress, ec))
            {
              set_phase2_error("client shard recv from peer " + std::to_string(peer_part) + " failed: " + ec.message());
              exchange_failed.store(true);
              return;
            }
            const int recv_global = static_cast<int>(rhdr.shard_begin);
            std::memcpy(recovered_ptrs[partition_id] + static_cast<size_t>(recv_global) * stripe_byte_len,
                        recv_stripe.data(), stripe_byte_len);
          }
        }
        else
        {
          asio::ip::tcp::socket *sock = ensure_server_peer(peer_part);
          if (sock == nullptr)
            return;
          if (shard_k < peer_shard_count)
          {
            Phase2PeerHeader rhdr{};
            std::vector<char> recv_stripe(stripe_byte_len);
            if (!recv_shard_frame(*sock, rhdr, recv_stripe.data(), recv_stripe.size(), block_bw.ingress, ec))
            {
              set_phase2_error("server shard recv from peer " + std::to_string(peer_part) + " failed: " + ec.message());
              exchange_failed.store(true);
              return;
            }
            const int recv_global = static_cast<int>(rhdr.shard_begin);
            std::memcpy(recovered_ptrs[partition_id] + static_cast<size_t>(recv_global) * stripe_byte_len,
                        recv_stripe.data(), stripe_byte_len);
          }
          if (shard_k < local_shard_count)
          {
            Phase2PeerHeader out_hdr = make_shard_header(partition_id, global_shard, stripe_byte_len);
            if (!send_shard_frame(*sock, out_hdr, reinterpret_cast<char *>(recovered_ptrs[peer_part] + stripe_off),
                                  block_bw.egress, ec))
            {
              set_phase2_error("server shard send to peer " + std::to_string(peer_part) + " failed: " + ec.message());
              exchange_failed.store(true);
              return;
            }
          }
        }
      });
    }
    for (auto &th : peer_threads)
    {
      if (th.joinable())
        th.join();
    }
  };

  double total_write_net = 0.0;
  double total_write_disk = 0.0;

  for (int shard_k = 0; shard_k < max_exchange_rounds && !exchange_failed.load(); shard_k++)
  {
    if (shard_k < local_shard_count)
    {
    const int stripe_off = byte_off + shard_k * stripe_byte_len;

    std::unique_ptr<bool[]> status(new bool[helper_n]);
    std::fill_n(status.get(), helper_n, false);
    std::vector<double> disk_io_start(helper_n, 0.0), disk_io_end(helper_n, 0.0);
    std::vector<double> net_start(helper_n, 0.0), net_end(helper_n, 0.0);
    std::vector<double> grpc_notify(helper_n, 0.0), grpc_start(helper_n, 0.0);

    std::vector<std::thread> get_threads;
    for (int i = 0; i < helper_n; i++)
    {
      get_threads.emplace_back(&ProxyImpl::get_from_node_stripe_range_breakdown, this, recovery_request->blockkeys(i),
                               get_bufs[i], static_cast<size_t>(block_size), stripe_off, stripe_byte_len,
                               recovery_request->datanodeip(i).c_str(), recovery_request->datanodeport(i), status.get(),
                               i, &disk_io_start[i], &disk_io_end[i], &net_start[i], &net_end[i], &grpc_notify[i],
                               &grpc_start[i], block_bw.ingress);
    }
    for (auto &th : get_threads)
      th.join();

    if (!std::all_of(status.get(), status.get() + helper_n, [](bool v) { return v; }))
    {
      for (int i = 0; i < helper_n; i++)
      {
        if (!status[i])
        {
          char buf[384];
          snprintf(buf, sizeof(buf), "helper read failed shard=%d idx=%d block_id=%d key=%s @ %s:%d", shard_k, i,
                   recovery_request->blockids(i), recovery_request->blockkeys(i).c_str(),
                   recovery_request->datanodeip(i).c_str(), recovery_request->datanodeport(i));
          set_phase2_error(buf);
          std::cerr << "[Proxy" << m_self_cluster_id << "][gLRC ILP Phase2] partition " << partition_id << " " << buf
                    << std::endl;
          break;
        }
      }
      exchange_failed.store(true);
      break;
    }

    for (int i = 0; i < helper_n; i++)
    {
      if (!have_disk)
      {
        min_disk_start = disk_io_start[i];
        max_disk_end = disk_io_end[i];
        have_disk = true;
      }
      else
      {
        min_disk_start = std::min(min_disk_start, disk_io_start[i]);
        max_disk_end = std::max(max_disk_end, disk_io_end[i]);
      }
      if (!have_net)
      {
        min_net_start = net_start[i];
        max_net_end = net_end[i];
        have_net = true;
      }
      else
      {
        min_net_start = std::min(min_net_start, net_start[i]);
        max_net_end = std::max(max_net_end, net_end[i]);
      }
      if (!have_grpc)
      {
        min_grpc_notify = grpc_notify[i];
        max_grpc_start = grpc_start[i];
        have_grpc = true;
      }
      else
      {
        min_grpc_notify = std::min(min_grpc_notify, grpc_notify[i]);
        max_grpc_start = std::max(max_grpc_start, grpc_start[i]);
      }
    }

    std::vector<unsigned char *> block_ptrs = convertToUnsignedCharArray(get_bufs);
    auto t3 = std::chrono::high_resolution_clock::now();
    std::vector<std::vector<unsigned char>> shard_recovered;
    const bool decode_ok =
        decode_glrc_ilp_helper_compact(block_ptrs.data(), phase2_decode_inverse, phase2_eq_helper_indices,
                                       phase2_eq_helper_coefs, f, stripe_off, stripe_byte_len, shard_recovered);
    auto t4 = std::chrono::high_resolution_clock::now();
    for (char *p : get_bufs)
      (void)p;
    if (!decode_ok)
    {
      char buf[256];
      snprintf(buf, sizeof(buf), "decode failed shard=%d off=%d len=%d repair_block=%d", shard_k, stripe_off,
               stripe_byte_len, repair_block_id);
      set_phase2_error(buf);
      std::cerr << "[Proxy" << m_self_cluster_id << "][gLRC ILP Phase2] partition " << partition_id << " " << buf
                << std::endl;
      exchange_failed.store(true);
      break;
    }

    const double decode_duration = std::chrono::duration<double>(t4 - t3).count();
    total_decode_time += decode_duration;
    if (!have_decode)
    {
      min_decode_start = 0.0;
      max_decode_end = total_decode_time;
      have_decode = true;
    }
    else
    {
      max_decode_end = total_decode_time;
    }

    for (int t = 0; t < f; t++)
      std::memcpy(recovered_ptrs[t] + stripe_off, shard_recovered[t].data(), stripe_byte_len);
    }

    exchange_shard_with_peer(shard_k);

    if (shard_k < local_shard_count && recovery_request->phase2_do_write_back())
    {
      const int stripe_off = byte_off + shard_k * stripe_byte_len;
      const int write_idx = partition_id;
      if (write_idx >= 0 && write_idx < f)
      {
        double wnet = 0.0, wdisk = 0.0;
        if (!RecoveryToDatanodeStripeBreakdown(recovery_request->failed_block_keys(write_idx).c_str(),
                                               failed_ids[write_idx],
                                               reinterpret_cast<char *>(recovered_ptrs[write_idx] + stripe_off),
                                               recovery_request->replaced_node_ips(write_idx).c_str(),
                                               recovery_request->replaced_node_ports(write_idx), stripe_off,
                                               stripe_byte_len, &wnet, &wdisk))
        {
          exchange_failed.store(true);
          break;
        }
        total_write_net += wnet;
        total_write_disk += wdisk;
      }
    }

    {
      char buf[256];
      snprintf(buf, sizeof(buf), "shard pipeline done proxy=%s:%d partition=%d shard=%d/%d", m_ip.c_str(), m_port,
               partition_id, shard_k + 1, local_shard_count);
      phase2_trace(buf);
    }
  }

  for (char *p : get_bufs)
    free(p);

  {
    std::lock_guard<std::mutex> lk(client_peer_mu);
    for (auto &kv : client_peer_socks)
    {
      if (kv.second.connected && kv.second.sock)
        close_phase2_socket(*kv.second.sock);
    }
  }
  {
    std::lock_guard<std::mutex> lk(incoming_mu);
    for (auto &kv : server_peer_socks)
    {
      if (kv.second)
        close_phase2_socket(*kv.second);
    }
  }
  join_accept_thread();

  if (exchange_failed.load() || accept_failed.load())
  {
    std::cerr << "[Proxy" << m_self_cluster_id << "][gLRC ILP Phase2] partition " << partition_id
              << " streaming peer exchange failed" << std::endl;
    if (!exchange_failed.load())
      set_phase2_error("phase2 accept failed");
    for (unsigned char *p : recovered_ptrs)
      delete[] p;
    return false;
  }

  const auto ex_end = std::chrono::high_resolution_clock::now();
  const double exchange_net_time = std::chrono::duration<double>(ex_end - ex_begin).count();

  if (have_disk)
  {
    response->set_disk_io_start_time(min_disk_start);
    response->set_disk_io_end_time(max_disk_end);
  }
  if (have_net)
  {
    response->set_network_start_time(min_net_start);
    response->set_network_end_time(max_net_end);
  }
  if (have_grpc)
  {
    response->set_data_node_grpc_notify_time(min_grpc_notify);
    response->set_data_node_grpc_start_time(max_grpc_start);
  }
  if (have_decode)
  {
    response->set_decode_start_time(min_decode_start);
    response->set_decode_end_time(max_decode_end);
  }
  response->set_cross_rack_time(exchange_net_time);
  if (recovery_request->phase2_do_write_back())
  {
    response->set_dest_data_node_network_time(total_write_net);
    response->set_dest_data_node_disk_io_time(total_write_disk);
  }

  for (unsigned char *p : recovered_ptrs)
    delete[] p;

  {
    char buf[256];
    snprintf(buf, sizeof(buf), "ok proxy=%s:%d partition=%d streaming_shards=%d", m_ip.c_str(), m_port, partition_id,
             local_shard_count);
    phase2_trace(buf);
  }
  std::cout << "[Proxy" << m_self_cluster_id << "][gLRC ILP Phase2] partition " << partition_id
            << " streaming shards=" << local_shard_count << " byte_len=" << byte_len << " ok" << std::endl;
  return true;
}

} // namespace ECProject
