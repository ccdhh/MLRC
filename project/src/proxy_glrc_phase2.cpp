#include "proxy.h"
#include "config.h"
#include "link_bandwidth.h"
#include "unilrc_encoder.h"
#include <asio.hpp>
#include <algorithm>
#include <chrono>
#include <cstdint>
#include <cstdlib>
#include <cstdio>
#include <cstring>
#include <iostream>
#include <string>
#include <atomic>
#if defined(__linux__) || defined(__APPLE__)
#include <sys/socket.h>
#include <unistd.h>
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

std::mutex g_phase2_error_mutex;
std::string g_glrc_phase2_last_error;

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
  {
    std::lock_guard<std::mutex> lock(g_phase2_error_mutex);
    // Preserve the first concrete worker failure; later peer EOF/timeouts are
    // usually consequences and should not hide the root cause.
    if (g_glrc_phase2_last_error.empty())
      g_glrc_phase2_last_error = msg;
  }
  phase2_trace(msg.c_str());
}

void clear_phase2_error()
{
  std::lock_guard<std::mutex> lock(g_phase2_error_mutex);
  g_glrc_phase2_last_error.clear();
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
  // Real deployments place exactly one proxy on each host, so every host can
  // safely reuse the same phase-2 listener band.  This avoids exhausting the
  // fixed single-machine port range when n grows past the local simulator.
  const char *one_proxy_per_host = std::getenv("DDRT_ONE_PROXY_PER_HOST");
  if (one_proxy_per_host != nullptr && one_proxy_per_host[0] != '\0' &&
      one_proxy_per_host[0] != '0')
    return 0;
  const int idx = (proxy_grpc_port - PROXY_GRPC_BASE) / PROXY_GRPC_STRIDE;
  if (idx < 0)
    return 0;
  if (idx > PROXY_GRPC_MAX_INDEX)
    return PROXY_GRPC_MAX_INDEX;
  return idx;
}

int phase2_exchange_port(int proxy_grpc_port, int partition_id, int exchange_epoch)
{
  const int proxy_idx = phase2_proxy_index(proxy_grpc_port);
  const int epoch_slot = exchange_epoch % PROXY_PHASE2_EPOCH_STRIDE;
  const int port = PROXY_PHASE2_EXCHANGE_BASE + proxy_idx * PROXY_PHASE2_PER_PROXY_BAND +
                   epoch_slot * 8 + partition_id;
  if (port >= PROXY_PIPELINE_EXCHANGE_BASE)
  {
    std::cerr << "[gLRC Phase2] exchange port overflow proxy_idx=" << proxy_idx << " port=" << port
              << " (must stay below pipeline base " << PROXY_PIPELINE_EXCHANGE_BASE << ")\n";
  }
  return port;
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
  tcp_read_with_shared_bandwidth(socket, data, len, bw, ec, SharedBandwidthPace::DrainThenAccount);
  return !ec;
}

Phase2PeerHeader make_shard_header(int from_partition, int global_shard, int stripe_byte_len, int shard_count = 1)
{
  Phase2PeerHeader hdr{};
  hdr.magic = kPhase2Magic;
  hdr.from_partition = static_cast<uint32_t>(from_partition);
  hdr.stripe_byte_len = static_cast<uint32_t>(stripe_byte_len);
  hdr.shard_begin = static_cast<uint32_t>(global_shard);
  hdr.shard_count = static_cast<uint32_t>(shard_count);
  return hdr;
}

bool send_shard_frame(asio::ip::tcp::socket &socket, const Phase2PeerHeader &hdr, const char *payload,
                      SharedBandwidthLimiter *bw, asio::error_code &ec)
{
  const size_t payload_len = static_cast<size_t>(hdr.stripe_byte_len) * static_cast<size_t>(hdr.shard_count);
  if (!write_all_bw(socket, reinterpret_cast<const char *>(&hdr), sizeof(hdr), bw, ec))
    return false;
  return write_all_bw(socket, payload, payload_len, bw, ec);
}

bool recv_shard_frame(asio::ip::tcp::socket &socket, Phase2PeerHeader &hdr, char *payload, size_t payload_cap,
                      SharedBandwidthLimiter *ingress_bw, asio::error_code &ec)
{
  if (!read_all_bw(socket, reinterpret_cast<char *>(&hdr), sizeof(hdr), ingress_bw, ec))
    return false;
  const size_t payload_len = static_cast<size_t>(hdr.stripe_byte_len) * static_cast<size_t>(hdr.shard_count);
  if (hdr.magic != kPhase2Magic || hdr.shard_count < 1 || hdr.stripe_byte_len == 0 ||
      payload_len > payload_cap)
  {
    ec = asio::error::invalid_argument;
    return false;
  }
  return read_all_bw(socket, payload, payload_len, ingress_bw, ec);
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
                          int listen_port, asio::error_code &ec, int max_retries = 600)
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
    asio::error_code close_ec;
    acceptor.close(close_ec);
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

struct Phase2ExchangeAcceptor
{
  std::shared_ptr<asio::io_context> accept_io;
  std::unique_ptr<asio::ip::tcp::acceptor> acceptor;
  std::mutex incoming_mu;
  std::condition_variable incoming_cv;
  std::map<int, std::unique_ptr<asio::ip::tcp::socket>> server_peer_socks;
  std::atomic<bool> accept_failed{false};
  std::atomic<bool> accept_stop{false};
  std::thread accept_thread;
  bool accept_started = false;
  int listen_port = 0;
  int accept_remaining = 0;

  void stop_acceptor()
  {
    accept_stop.store(true);
    if (acceptor)
    {
      asio::error_code close_ec;
      acceptor->close(close_ec);
    }
  }

  void join_acceptor()
  {
    if (!accept_started)
      return;
    stop_acceptor();
    incoming_cv.notify_all();
    if (accept_thread.joinable())
      accept_thread.join();
    accept_started = false;
    release_phase2_listen_port(listen_port);
    listen_port = 0;
  }
};

struct Phase2ReadySession
{
  int exchange_epoch = -1;
  std::map<int, std::unique_ptr<Phase2ExchangeAcceptor>> by_partition;
};

std::mutex g_phase2_ready_mutex;
Phase2ReadySession g_phase2_ready_session;

void clear_phase2_ready_session()
{
  std::lock_guard<std::mutex> lock(g_phase2_ready_mutex);
  for (auto &kv : g_phase2_ready_session.by_partition)
  {
    if (kv.second)
      kv.second->join_acceptor();
  }
  g_phase2_ready_session = Phase2ReadySession{};
}

bool phase2_start_exchange_acceptor(Phase2ExchangeAcceptor &acc, const std::string &ip, int proxy_grpc_port,
                                    int partition_id, int exchange_epoch, int accept_remaining)
{
  if (accept_remaining <= 0 || acc.accept_started)
    return true;
  acc.accept_remaining = accept_remaining;
  acc.accept_stop.store(false);
  acc.accept_failed.store(false);
  acc.accept_io = std::make_shared<asio::io_context>();
  acc.listen_port = phase2_exchange_port(proxy_grpc_port, partition_id, exchange_epoch);
  try
  {
    acc.acceptor = std::make_unique<asio::ip::tcp::acceptor>(*acc.accept_io);
    asio::ip::tcp::endpoint listen_ep(asio::ip::address::from_string(ip), acc.listen_port);
    asio::error_code bind_ec;
    if (!bind_phase2_acceptor(*acc.acceptor, listen_ep, acc.listen_port, bind_ec))
    {
      char buf[384];
      snprintf(buf, sizeof(buf), "exchange bind failed port=%d: %s", acc.listen_port, bind_ec.message().c_str());
      set_phase2_error(buf);
      std::cerr << "[Proxy][gLRC ILP Phase2] partition " << partition_id << " " << buf << std::endl;
      acc.acceptor.reset();
      return false;
    }
    acc.acceptor->non_blocking(true);
  }
  catch (const std::exception &e)
  {
    char buf[384];
    snprintf(buf, sizeof(buf), "exchange bind failed port=%d: %s", acc.listen_port, e.what());
    set_phase2_error(buf);
    std::cerr << "[Proxy][gLRC ILP Phase2] partition " << partition_id << " " << buf << std::endl;
    acc.acceptor.reset();
    return false;
  }

  Phase2ExchangeAcceptor *acc_ptr = &acc;
  acc.accept_thread = std::thread([acc_ptr]() {
    int accepted = 0;
    while (!acc_ptr->accept_stop.load() && accepted < acc_ptr->accept_remaining)
    {
      auto sock = std::make_unique<asio::ip::tcp::socket>(*acc_ptr->accept_io);
      asio::error_code accept_ec;
      acc_ptr->acceptor->accept(*sock, accept_ec);
      if (accept_ec)
      {
        if (accept_ec == asio::error::would_block || accept_ec == asio::error::try_again)
        {
          std::this_thread::sleep_for(std::chrono::milliseconds(5));
          continue;
        }
        if (!acc_ptr->accept_stop.load())
          acc_ptr->accept_failed.store(true);
        break;
      }
      set_exchange_socket_timeouts(*sock, 45);
      asio::error_code ec;
      Phase2ConnectHello hello{};
      if (!read_all(*sock, reinterpret_cast<char *>(&hello), sizeof(hello), ec) ||
          hello.magic != kPhase2HelloMagic)
      {
        if (!acc_ptr->accept_stop.load())
          acc_ptr->accept_failed.store(true);
        close_phase2_socket(*sock);
        break;
      }
      const int from_part = static_cast<int>(hello.from_partition);
      {
        std::lock_guard<std::mutex> lk(acc_ptr->incoming_mu);
        acc_ptr->server_peer_socks[from_part] = std::move(sock);
      }
      acc_ptr->incoming_cv.notify_all();
      accepted++;
    }
  });
  acc.accept_started = true;
  return true;
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
    SharedBandwidthLimiter *read_bw = block_bandwidth;
    if (isLocalDatanode(ip, port))
      read_bw = nullptr;
    // Remote DN→proxy: limited on datanode egress only (node-pair, one end).
    tcp_read_with_shared_bandwidth(socket, value + read_offset, static_cast<size_t>(read_length), read_bw, ec);
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
    if (read_offset < 0 || read_length < 0 || static_cast<size_t>(read_offset + read_length) > full_block_size)
      return false;
    asio::error_code ec;
    SharedBandwidthLimiter *read_bw = block_bandwidth;
    if (isLocalDatanode(ip, port))
      read_bw = nullptr;
    // Remote DN→proxy: limited on datanode egress only (node-pair, one end).
    tcp_read_with_shared_bandwidth(socket, value, static_cast<size_t>(read_length), read_bw, ec);
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

std::string glrc_phase2_take_last_error()
{
  std::lock_guard<std::mutex> lock(g_phase2_error_mutex);
  std::string out = g_glrc_phase2_last_error;
  g_glrc_phase2_last_error.clear();
  return out;
}

void glrc_phase2_clear_ready_session()
{
  clear_phase2_ready_session();
}

bool ProxyImpl::glrcIlpPhase2Recovery(const proxy_proto::RecoveryRequest *recovery_request,
                                      proxy_proto::RecoveryReply *response)
{
  clear_phase2_error();
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
  const int f = recovery_request->failed_block_ids_size();
  const int byte_off = recovery_request->phase2_byte_off();
  const int byte_len = recovery_request->phase2_byte_len();
  const int stripe_byte_len = recovery_request->phase2_stripe_byte_len();
  const int block_size = m_sys_config->BlockSize;
  const int helper_n = recovery_request->datanodeip_size();

  if (f < 1)
  {
    set_phase2_error("bad dims f");
    return false;
  }

  int accept_remaining = 0;
  for (int peer = 0; peer < recovery_request->phase2_peer_partition_ids_size(); peer++)
  {
    if (recovery_request->phase2_peer_partition_ids(peer) < partition_id)
      accept_remaining++;
  }

  Phase2ExchangeAcceptor exchange_acc;
  std::unique_ptr<Phase2ExchangeAcceptor> exchange_acc_ptr;
  auto join_accept_thread = [&]() {
    if (exchange_acc_ptr)
      exchange_acc_ptr->join_acceptor();
    else
      exchange_acc.join_acceptor();
  };
  auto active_acceptor = [&]() -> Phase2ExchangeAcceptor & {
    return exchange_acc_ptr ? *exchange_acc_ptr : exchange_acc;
  };

  if (recovery_request->phase2_ready_only())
  {
    if (accept_remaining > 0)
    {
      exchange_acc_ptr = std::make_unique<Phase2ExchangeAcceptor>();
      if (!phase2_start_exchange_acceptor(*exchange_acc_ptr, m_ip, m_port, partition_id, exchange_epoch,
                                          accept_remaining))
        return false;
      std::lock_guard<std::mutex> lock(g_phase2_ready_mutex);
      if (g_phase2_ready_session.exchange_epoch != exchange_epoch)
      {
        for (auto &kv : g_phase2_ready_session.by_partition)
        {
          if (kv.second)
            kv.second->join_acceptor();
        }
        g_phase2_ready_session = Phase2ReadySession{};
        g_phase2_ready_session.exchange_epoch = exchange_epoch;
      }
      auto &slot = g_phase2_ready_session.by_partition[partition_id];
      if (slot)
        slot->join_acceptor();
      slot = std::move(exchange_acc_ptr);
    }
    return true;
  }

  if (helper_n < 1 || byte_len < 0 || stripe_byte_len < 1)
  {
    char buf[256];
    snprintf(buf, sizeof(buf), "bad dims f=%d helpers=%d byte_len=%d stripe_len=%d", f, helper_n, byte_len,
             stripe_byte_len);
    set_phase2_error(buf);
    return false;
  }

  {
    std::lock_guard<std::mutex> lock(g_phase2_ready_mutex);
    if (g_phase2_ready_session.exchange_epoch == exchange_epoch)
    {
      auto it = g_phase2_ready_session.by_partition.find(partition_id);
      if (it != g_phase2_ready_session.by_partition.end())
      {
        exchange_acc_ptr = std::move(it->second);
        g_phase2_ready_session.by_partition.erase(it);
      }
    }
  }

  if (!active_acceptor().accept_started && accept_remaining > 0)
  {
    if (!exchange_acc_ptr)
      exchange_acc_ptr = std::make_unique<Phase2ExchangeAcceptor>();
    if (!phase2_start_exchange_acceptor(*exchange_acc_ptr, m_ip, m_port, partition_id, exchange_epoch,
                                        accept_remaining))
      return false;
  }

  const int shard_begin = recovery_request->phase2_shard_begin();
  const int local_shard_count = recovery_request->phase2_shard_count_local();
  const int total_shard_count = recovery_request->phase2_shard_count();
  const int64_t phase2_write_bytes =
      static_cast<int64_t>(total_shard_count) * static_cast<int64_t>(stripe_byte_len);
  const bool hybrid_partial_write =
      recovery_request->phase2_share_proxy_node_bandwidth() && phase2_write_bytes < block_size;
  if (shard_begin < 0 || local_shard_count < 0 || total_shard_count < 1 ||
      shard_begin + local_shard_count > total_shard_count ||
      byte_off != shard_begin * stripe_byte_len ||
      byte_len != local_shard_count * stripe_byte_len ||
      phase2_write_bytes <= 0 || phase2_write_bytes > block_size ||
      (!recovery_request->phase2_share_proxy_node_bandwidth() &&
       phase2_write_bytes != block_size))
  {
    set_phase2_error("invalid phase2 shard geometry");
    join_accept_thread();
    return false;
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
                                      phase2_eq_helper_coefs,
                                      glrc_codec_mode(m_sys_config->CodeType)))
  {
    set_phase2_error("phase2 prepare helper decode failed");
    return false;
  }
  std::vector<std::vector<unsigned char>> phase2_eq_helper_g_tbls(f);
  for (int ei = 0; ei < f; ei++)
  {
    const int term_count = static_cast<int>(phase2_eq_helper_coefs[ei].size());
    phase2_eq_helper_g_tbls[ei].resize(static_cast<size_t>(term_count) * 32);
    if (term_count > 0)
      ec_init_tables(term_count, 1, phase2_eq_helper_coefs[ei].data(), phase2_eq_helper_g_tbls[ei].data());
  }
  std::vector<unsigned char> phase2_inv_g_tbls(static_cast<size_t>(f) * static_cast<size_t>(f) * 32);
  ec_init_tables(f, f, phase2_decode_inverse.data(), phase2_inv_g_tbls.data());

  std::vector<char *> get_bufs(helper_n);
  for (int i = 0; i < helper_n; i++)
  {
    const size_t helper_bytes = byte_len > 0 ? static_cast<size_t>(byte_len) : 1u;
    const size_t helper_buf_size = (helper_bytes + 31u) & ~static_cast<size_t>(31u);
    get_bufs[i] = static_cast<char *>(std::aligned_alloc(32, helper_buf_size));
    if (get_bufs[i] == nullptr)
    {
      set_phase2_error("helper buffer alloc failed");
      for (int j = 0; j < i; j++)
        free(get_bufs[j]);
      join_accept_thread();
      return false;
    }
    std::memset(get_bufs[i], 0, helper_buf_size);
  }

  std::vector<unsigned char *> recovered_ptrs(f);
  for (int i = 0; i < f; i++)
  {
    recovered_ptrs[i] = new unsigned char[block_size];
    std::memset(recovered_ptrs[i], 0, block_size);
  }

  std::atomic<bool> exchange_failed{false};
  std::mutex decoded_mu;
  std::condition_variable decoded_cv;
  int decoded_shards = 0;
  bool decode_done = false;
  std::mutex helper_round_mu;
  std::condition_variable helper_round_cv;
  std::vector<int> helper_round_done(std::max(local_shard_count, 0), 0);
  int helper_round_to_start = -1;
  bool helper_round_stop = false;

  // This partition owns failed block `partition_id`. Open one persistent DN
  // stream up front, then publish each globally ordered shard as soon as local
  // decode or peer exchange makes it available. Standalone streams a complete
  // atomic block; hybrid streams only its contiguous [0,p) prefix.
  struct Phase2WriteStream
  {
    asio::io_context io;
    asio::ip::tcp::socket socket{io};
    grpc::ClientContext grpc_context;
    datanode_proto::MergeParityInfo recovery_info;
    datanode_proto::RequestResult recovery_result;
    grpc::Status grpc_status;
    std::thread grpc_thread;
    std::thread writer_thread;
    std::mutex mu;
    std::condition_variable cv;
    std::vector<char> ready;
    int next_shard = 0;
    bool producer_done = false;
    bool failed = false;
    double network_time = 0.0;
  };

  // Even a partition assigned zero local decode shards still receives the
  // other partitions' shards and owns one failed block, so it must write back.
  const bool phase2_write_enabled =
      total_shard_count > 0 && recovery_request->phase2_do_write_back();
  std::unique_ptr<Phase2WriteStream> write_stream;
  auto mark_write_shard_ready = [&](int global_shard) {
    if (!write_stream || global_shard < 0 || global_shard >= total_shard_count)
      return;
    {
      std::lock_guard<std::mutex> lock(write_stream->mu);
      write_stream->ready[static_cast<size_t>(global_shard)] = 1;
    }
    write_stream->cv.notify_one();
  };

  if (phase2_write_enabled)
  {
    write_stream = std::make_unique<Phase2WriteStream>();
    write_stream->ready.assign(static_cast<size_t>(total_shard_count), 0);
    const int write_idx = partition_id;
    const std::string rep_ip = recovery_request->replaced_node_ips(write_idx);
    const int rep_port = recovery_request->replaced_node_ports(write_idx);
    const std::string node_ip_port = rep_ip + ":" + std::to_string(rep_port);
    auto dn_it = m_datanode_ptrs.find(node_ip_port);
    if (dn_it == m_datanode_ptrs.end() || !dn_it->second)
    {
      set_phase2_error("phase2 write stream datanode stub missing: " + node_ip_port);
      exchange_failed.store(true);
    }
    else
    {
      write_stream->recovery_info.set_block_key(recovery_request->failed_block_keys(write_idx));
      write_stream->recovery_info.set_block_id(failed_ids[write_idx]);
      write_stream->recovery_info.set_recovery_offset(0);
      // Standalone covers all S shards and keeps the datanode's atomic
      // temp-file publish. Hybrid covers only [0,p): use one persistent
      // prefix stream, opened before decode/exchange (the datanode's offset-0
      // stripe path truncates when it opens), then leave the concurrently
      // written pipeline-owned [p,S) tail untouched.
      write_stream->recovery_info.set_recovery_size(
          hybrid_partial_write ? static_cast<int>(phase2_write_bytes) : 0);
      write_stream->recovery_info.set_recovery_defer_fsync(hybrid_partial_write);
      if (hybrid_partial_write)
      {
        write_stream->recovery_info.set_recovery_commit_token(recovery_request->hybrid_commit_token());
        write_stream->recovery_info.set_recovery_commit_parts(2);
      }
      write_stream->recovery_info.set_proxy_ip(m_ip);
      write_stream->recovery_info.set_proxy_port(m_port);
      write_stream->grpc_context.set_deadline(std::chrono::system_clock::now() + std::chrono::seconds(180));

      datanode_proto::datanodeService::Stub *dn_stub = dn_it->second.get();
      Phase2WriteStream *ws = write_stream.get();
      write_stream->grpc_thread = std::thread([dn_stub, ws]() {
        ws->grpc_status =
            dn_stub->handleRecoveryBreakdown(&ws->grpc_context, ws->recovery_info,
                                             &ws->recovery_result);
      });

      asio::ip::tcp::resolver resolver(write_stream->io);
      asio::error_code connect_ec;
      asio::connect(write_stream->socket,
                    resolver.resolve({rep_ip, std::to_string(rep_port + ECProject::DATANODE_PORT_SHIFT)}),
                    connect_ec);
      if (connect_ec)
      {
        set_phase2_error("phase2 write stream connect failed: " + node_ip_port + " " +
                         connect_ec.message());
        exchange_failed.store(true);
        asio::error_code ignore_ec;
        write_stream->socket.close(ignore_ec);
        write_stream->grpc_context.TryCancel();
      }
      else
      {
        set_exchange_socket_timeouts(write_stream->socket, 45);
        SharedBandwidthLimiter *write_bw =
            egressBandwidthForDatanodeWrite(rep_ip.c_str(), rep_port);
        write_stream->writer_thread = std::thread([&, ws, write_bw]() {
          while (!exchange_failed.load())
          {
            int shard = -1;
            {
              std::unique_lock<std::mutex> lock(ws->mu);
              ws->cv.wait_for(lock, std::chrono::milliseconds(50), [&]() {
                return exchange_failed.load() || ws->failed ||
                       (ws->next_shard < total_shard_count &&
                        ws->ready[static_cast<size_t>(ws->next_shard)] != 0) ||
                       ws->producer_done;
              });
              if (exchange_failed.load() || ws->failed)
                break;
              if (ws->next_shard < total_shard_count &&
                  ws->ready[static_cast<size_t>(ws->next_shard)] != 0)
              {
                shard = ws->next_shard++;
              }
              else if (ws->producer_done)
              {
                if (ws->next_shard != total_shard_count)
                {
                  ws->failed = true;
                  set_phase2_error("phase2 write stream missing shard " +
                                   std::to_string(ws->next_shard));
                  exchange_failed.store(true);
                  decoded_cv.notify_all();
                  helper_round_cv.notify_all();
                }
                break;
              }
            }
            if (shard < 0)
              continue;

            asio::error_code write_ec;
            const auto write_begin = std::chrono::steady_clock::now();
            tcp_write_with_shared_bandwidth(
                ws->socket,
                reinterpret_cast<const char *>(recovered_ptrs[partition_id] +
                                               static_cast<size_t>(shard) * stripe_byte_len),
                static_cast<size_t>(stripe_byte_len), write_bw, write_ec);
            ws->network_time +=
                std::chrono::duration<double>(std::chrono::steady_clock::now() - write_begin).count();
            if (write_ec)
            {
              ws->failed = true;
              set_phase2_error("phase2 write stream shard " + std::to_string(shard) +
                               " failed: " + write_ec.message());
              exchange_failed.store(true);
              decoded_cv.notify_all();
              helper_round_cv.notify_all();
              break;
            }
          }

          asio::error_code ignore_ec;
          ws->socket.shutdown(asio::ip::tcp::socket::shutdown_both, ignore_ec);
          ws->socket.close(ignore_ec);
        });
      }
    }
  }

  double min_disk_start = 0.0, max_disk_end = 0.0;
  double min_net_start = 0.0, max_net_end = 0.0;
  double min_decode_start = 0.0, max_decode_end = 0.0;
  double total_decode_time = 0.0;
  double min_grpc_notify = 0.0, max_grpc_start = 0.0;
  bool have_disk = false, have_net = false, have_decode = false;
  bool have_grpc = false;

  constexpr int kExchangeSocketTimeoutSec = 45;
  double total_exchange_time = 0.0;

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
    Phase2ExchangeAcceptor &acc = active_acceptor();
    if (!acc.accept_io)
      acc.accept_io = std::make_shared<asio::io_context>();
    auto connected_sock = std::make_unique<asio::ip::tcp::socket>(*acc.accept_io);
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
    std::unique_lock<std::mutex> lk(active_acceptor().incoming_mu);
    const auto wait_deadline = std::chrono::steady_clock::now() + std::chrono::seconds(90);
    while (active_acceptor().server_peer_socks.find(peer_part) == active_acceptor().server_peer_socks.end() &&
           !active_acceptor().accept_failed.load() && !exchange_failed.load())
    {
      if (active_acceptor().incoming_cv.wait_until(lk, wait_deadline) == std::cv_status::timeout)
        break;
    }
    if (active_acceptor().server_peer_socks.find(peer_part) == active_acceptor().server_peer_socks.end())
    {
      set_phase2_error("exchange wait peer " + std::to_string(peer_part) + " timed out");
      exchange_failed.store(true);
      return nullptr;
    }
    return active_acceptor().server_peer_socks[peer_part].get();
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
  if (exchange_failed.load() || active_acceptor().accept_failed.load())
  {
    if (write_stream)
    {
      {
        std::lock_guard<std::mutex> lock(write_stream->mu);
        write_stream->producer_done = true;
      }
      write_stream->cv.notify_all();
      if (write_stream->writer_thread.joinable())
        write_stream->writer_thread.join();
      else
      {
        asio::error_code ignore_ec;
        write_stream->socket.close(ignore_ec);
        write_stream->grpc_context.TryCancel();
      }
      if (write_stream->grpc_thread.joinable())
        write_stream->grpc_thread.join();
    }
    for (char *p : get_bufs)
      free(p);
    for (unsigned char *p : recovered_ptrs)
      delete[] p;
    join_accept_thread();
    return false;
  }

  auto wait_until_decoded = [&](int shard_k) -> bool {
    if (shard_k >= local_shard_count)
      return true;
    std::unique_lock<std::mutex> lk(decoded_mu);
    decoded_cv.wait(lk, [&]() { return exchange_failed.load() || decoded_shards > shard_k || decode_done; });
    return !exchange_failed.load() && decoded_shards > shard_k;
  };

  std::vector<std::thread> exchange_workers;
  std::vector<std::unique_ptr<asio::ip::tcp::socket>> exchange_read_socks;
  auto add_exchange_workers = [&](int peer, int peer_part) {
    if (exchange_failed.load())
      return;

    int peer_shard_begin = 0;
    int peer_shard_count = 0;
    if (!lookup_peer_shards(recovery_request, peer_part, peer_shard_begin, peer_shard_count))
    {
      exchange_failed.store(true);
      decoded_cv.notify_all();
      return;
    }

    const int peer_port = recovery_request->phase2_peer_proxy_ports(peer);
    const std::string peer_ip = recovery_request->phase2_peer_proxy_ips(peer);
    SharedBandwidthLimiter *peer_egress = egressBandwidthForPeer(peer_ip);
    SharedBandwidthLimiter *peer_ingress = ingressBandwidthForPeer(peer_ip);
    asio::ip::tcp::socket *sock =
        partition_id < peer_part ? ensure_client_peer(peer_part, peer_ip, peer_port) : ensure_server_peer(peer_part);
    if (sock == nullptr)
    {
      exchange_failed.store(true);
      decoded_cv.notify_all();
      return;
    }

    // Keep one Asio object per thread. Concurrent synchronous read/write on
    // the same TCP connection is full-duplex at the OS level, but sharing one
    // asio::socket object across threads is not guaranteed thread-safe.
    asio::error_code dup_ec;
    const int duplicated_fd = ::dup(sock->native_handle());
    if (duplicated_fd < 0)
    {
      set_phase2_error("failed to duplicate exchange socket for peer " + std::to_string(peer_part));
      exchange_failed.store(true);
      decoded_cv.notify_all();
      return;
    }
    auto read_sock = std::make_unique<asio::ip::tcp::socket>(sock->get_executor());
    read_sock->assign(sock->local_endpoint().protocol(), duplicated_fd, dup_ec);
    if (dup_ec)
    {
      ::close(duplicated_fd);
      set_phase2_error("failed to assign duplicated exchange socket for peer " + std::to_string(peer_part) +
                       ": " + dup_ec.message());
      exchange_failed.store(true);
      decoded_cv.notify_all();
      return;
    }
    asio::ip::tcp::socket *recv_sock = read_sock.get();
    exchange_read_socks.push_back(std::move(read_sock));

    exchange_workers.emplace_back([&, sock, peer_part, peer_egress]() {
      asio::error_code send_ec;
      for (int shard_k = 0; shard_k < local_shard_count && !exchange_failed.load(); shard_k++)
      {
        if (!wait_until_decoded(shard_k))
          return;
        const int global_shard = shard_begin + shard_k;
        const int stripe_off = global_shard * stripe_byte_len;
        const Phase2PeerHeader hdr = make_shard_header(partition_id, global_shard, stripe_byte_len);
        if (!send_shard_frame(*sock, hdr, reinterpret_cast<char *>(recovered_ptrs[peer_part] + stripe_off),
                              peer_egress, send_ec))
        {
          set_phase2_error("shard send to peer " + std::to_string(peer_part) + " failed: " +
                           send_ec.message());
          exchange_failed.store(true);
          decoded_cv.notify_all();
          helper_round_cv.notify_all();
          return;
        }
      }
    });

    exchange_workers.emplace_back(
        [&, recv_sock, peer_part, peer_shard_begin, peer_shard_count, peer_ingress]() {
          asio::error_code recv_ec;
          std::vector<char> recv_stripe(stripe_byte_len);
          for (int shard_k = 0; shard_k < peer_shard_count && !exchange_failed.load(); shard_k++)
          {
            Phase2PeerHeader hdr{};
            if (!recv_shard_frame(*recv_sock, hdr, recv_stripe.data(), recv_stripe.size(), peer_ingress, recv_ec))
            {
              set_phase2_error("shard recv from peer " + std::to_string(peer_part) + " failed: " +
                               recv_ec.message());
              exchange_failed.store(true);
              decoded_cv.notify_all();
              helper_round_cv.notify_all();
              return;
            }
            const int recv_global = static_cast<int>(hdr.shard_begin);
            const int expected_global = peer_shard_begin + shard_k;
            if (recv_global != expected_global || hdr.shard_count != 1)
            {
              set_phase2_error("shard recv header mismatch from peer " + std::to_string(peer_part));
              exchange_failed.store(true);
              decoded_cv.notify_all();
              helper_round_cv.notify_all();
              return;
            }
            std::memcpy(recovered_ptrs[partition_id] + static_cast<size_t>(recv_global) * stripe_byte_len,
                        recv_stripe.data(), stripe_byte_len);
            mark_write_shard_ready(recv_global);
          }
        });
  };

  auto start_exchange_workers = [&]() {
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
      add_exchange_workers(peer, peer_part);
    }
  };
  auto join_exchange_workers = [&]() {
    decoded_cv.notify_all();
    for (auto &th : exchange_workers)
    {
      if (th.joinable())
        th.join();
    }
    for (auto &sock : exchange_read_socks)
    {
      if (sock && sock->is_open())
      {
        asio::error_code ignore_ec;
        sock->close(ignore_ec);
      }
    }
  };

  double total_write_net = 0.0;
  double total_write_disk = 0.0;

  std::mutex helper_metric_mu;
  auto update_helper_breakdown_one = [&](double disk_io_start, double disk_io_end, double net_start, double net_end,
                                         double grpc_notify, double grpc_start) {
    std::lock_guard<std::mutex> lk(helper_metric_mu);
    if (!have_disk)
    {
      min_disk_start = disk_io_start;
      max_disk_end = disk_io_end;
      have_disk = true;
    }
    else
    {
      min_disk_start = std::min(min_disk_start, disk_io_start);
      max_disk_end = std::max(max_disk_end, disk_io_end);
    }
    if (!have_net)
    {
      min_net_start = net_start;
      max_net_end = net_end;
      have_net = true;
    }
    else
    {
      min_net_start = std::min(min_net_start, net_start);
      max_net_end = std::max(max_net_end, net_end);
    }
    if (!have_grpc)
    {
      min_grpc_notify = grpc_notify;
      max_grpc_start = grpc_start;
      have_grpc = true;
    }
    else
    {
      min_grpc_notify = std::min(min_grpc_notify, grpc_notify);
      max_grpc_start = std::max(max_grpc_start, grpc_start);
    }
  };

  auto start_helper_round = [&](int shard_k) {
    if (shard_k < 0 || shard_k >= local_shard_count)
      return;
    {
      std::lock_guard<std::mutex> lk(helper_round_mu);
      helper_round_to_start = shard_k;
    }
    helper_round_cv.notify_all();
  };

  auto wait_helper_round_done = [&](int shard_k) -> bool {
    if (shard_k < 0 || shard_k >= local_shard_count)
      return true;
    std::unique_lock<std::mutex> lk(helper_round_mu);
    helper_round_cv.wait(lk, [&]() {
      return exchange_failed.load() || helper_round_done[shard_k] >= helper_n;
    });
    return !exchange_failed.load() && helper_round_done[shard_k] >= helper_n;
  };

  std::vector<std::thread> helper_workers;
  std::mutex compact_helper_read_mu;
  std::condition_variable compact_helper_read_cv;
  int compact_helper_readers = 0;
  auto start_helper_workers = [&]() {
    helper_workers.reserve(helper_n);
    for (int i = 0; i < helper_n; i++)
    {
      helper_workers.emplace_back([&, i]() {
        grpc::ClientContext context;
        datanode_proto::GetInfo get_info;
        datanode_proto::RequestResult result;
        get_info.set_block_key(recovery_request->blockkeys(i));
        get_info.set_block_size(block_size);
        get_info.set_read_offset(byte_off);
        get_info.set_read_length(byte_len);
        get_info.set_proxy_ip(m_ip);
        get_info.set_proxy_port(m_port);
        const std::string node_ip_port =
            recovery_request->datanodeip(i) + ":" + std::to_string(recovery_request->datanodeport(i));
        auto dn_it = m_datanode_ptrs.find(node_ip_port);
        if (dn_it == m_datanode_ptrs.end() || !dn_it->second)
        {
          set_phase2_error("helper stream datanode stub missing: " + node_ip_port);
          exchange_failed.store(true);
          helper_round_cv.notify_all();
          decoded_cv.notify_all();
          return;
        }
        context.set_deadline(std::chrono::system_clock::now() + std::chrono::seconds(120));
        const auto grpc_notify_tp = std::chrono::high_resolution_clock::now();
        grpc::Status stat = dn_it->second->handleGetBreakdown(&context, get_info, &result);
        if (!stat.ok())
        {
          set_phase2_error("helper stream grpc failed key=" + recovery_request->blockkeys(i) + " @ " + node_ip_port +
                           ": " + stat.error_message());
          exchange_failed.store(true);
          helper_round_cv.notify_all();
          decoded_cv.notify_all();
          return;
        }

        asio::io_context io_context;
        asio::ip::tcp::resolver resolver(io_context);
        asio::ip::tcp::socket socket(io_context);
        asio::error_code connect_ec;
        asio::connect(socket,
                      resolver.resolve({recovery_request->datanodeip(i), std::to_string(result.data_port())}),
                      connect_ec);
        if (connect_ec)
        {
          set_phase2_error("helper stream tcp connect failed key=" + recovery_request->blockkeys(i) + " @ " +
                           recovery_request->datanodeip(i) + ":" + std::to_string(result.data_port()) + " " +
                           connect_ec.message());
          exchange_failed.store(true);
          helper_round_cv.notify_all();
          decoded_cv.notify_all();
          return;
        }
        set_exchange_socket_timeouts(socket, 45);
        const double grpc_notify =
            std::chrono::duration_cast<std::chrono::duration<double>>(grpc_notify_tp.time_since_epoch()).count();
        update_helper_breakdown_one(result.disk_io_start_time(), result.disk_io_end_time(), grpc_notify,
                                    grpc_notify, grpc_notify, result.grpc_start_time());

        SharedBandwidthLimiter *helper_read_bw =
            isLocalDatanode(recovery_request->datanodeip(i).c_str(),
                            recovery_request->datanodeport(i))
                ? nullptr
                : nodeIngressBandwidth();

        // A per-shard global barrier makes all helper senders stop and wake for
        // every shard. With 48 helpers this control-plane fan-in is substantial
        // (and grows linearly with p/f), although the model charges only bytes.
        // Drain each helper's contiguous assigned range once, then publish all
        // rounds together. The failed proxies still run in parallel and the
        // shared limiter preserves the one-NIC byte time without barrier tails.
        if (local_shard_count > 0)
        {
          // These streams all terminate on one physical ingress NIC. Let one
          // socket drain at a time instead of allowing 48 readers to contend
          // in the kernel and then serializing only their accounting. Since
          // the limiter already models one 125 MB/s NIC, this preserves the
          // intended aggregate throughput while removing scheduler/TCP tails.
          {
            std::unique_lock<std::mutex> lock(compact_helper_read_mu);
            compact_helper_read_cv.wait(lock, [&]() {
              return compact_helper_readers == 0;
            });
            if (exchange_failed.load())
            {
              lock.unlock();
              compact_helper_read_cv.notify_all();
              return;
            }
            compact_helper_readers = 1;
          }
          asio::error_code read_ec;
          const auto net_begin = std::chrono::high_resolution_clock::now();
          tcp_read_with_shared_bandwidth(
              socket, get_bufs[i], static_cast<size_t>(byte_len), helper_read_bw, read_ec,
              SharedBandwidthPace::DrainThenAccount);
          const auto net_end = std::chrono::high_resolution_clock::now();
          {
            std::lock_guard<std::mutex> lock(compact_helper_read_mu);
            compact_helper_readers = 0;
          }
          compact_helper_read_cv.notify_one();
          if (read_ec)
          {
            set_phase2_error("compact helper stream read failed key=" +
                             recovery_request->blockkeys(i) + " @ " + node_ip_port +
                             ": " + read_ec.message());
            exchange_failed.store(true);
            helper_round_cv.notify_all();
            decoded_cv.notify_all();
            return;
          }
          update_helper_breakdown_one(
              result.disk_io_start_time(), result.disk_io_end_time(),
              std::chrono::duration_cast<std::chrono::duration<double>>(
                  net_begin.time_since_epoch())
                  .count(),
              std::chrono::duration_cast<std::chrono::duration<double>>(
                  net_end.time_since_epoch())
                  .count(),
              grpc_notify, result.grpc_start_time());
          {
            std::lock_guard<std::mutex> lk(helper_round_mu);
            for (int shard_k = 0; shard_k < local_shard_count; ++shard_k)
              helper_round_done[shard_k]++;
          }
          helper_round_cv.notify_all();
          return;
        }

        int last_round = -1;
        while (!exchange_failed.load())
        {
          int shard_k = -1;
          {
            std::unique_lock<std::mutex> lk(helper_round_mu);
            helper_round_cv.wait(lk, [&]() {
              return helper_round_stop || exchange_failed.load() || helper_round_to_start > last_round;
            });
            if (helper_round_stop || exchange_failed.load())
              return;
            shard_k = helper_round_to_start;
          }
          if (shard_k < 0 || shard_k >= local_shard_count)
            return;

          const int stripe_off = byte_off + shard_k * stripe_byte_len;
          char *dst = get_bufs[i] + static_cast<size_t>(shard_k) * static_cast<size_t>(stripe_byte_len);
          (void)stripe_off;
          asio::error_code read_ec;
          const auto net_begin = std::chrono::high_resolution_clock::now();
          // All remote helper streams and peer streams terminate on this
          // repair node's one physical RX NIC.  Share the same 125 MB/s
          // node-ingress timeline across them; same-host DN traffic bypasses
          // the NIC model.
          // RX must drain before accounting. Waiting on the ingress limiter
          // before read() applies the 1-Gbps budget on top of the real TCP
          // transfer, stalls all helper streams behind the per-shard barrier,
          // and makes Phase2 much slower than its node-NIC model predicts.
          tcp_read_with_shared_bandwidth(socket, dst, static_cast<size_t>(stripe_byte_len), helper_read_bw, read_ec,
                                         SharedBandwidthPace::DrainThenAccount);
          const auto net_end = std::chrono::high_resolution_clock::now();
          if (read_ec)
          {
            char buf[384];
            snprintf(buf, sizeof(buf), "helper stream shard read failed idx=%d block_id=%d key=%s off=%d len=%d @ %s:%d: %s",
                     i, recovery_request->blockids(i), recovery_request->blockkeys(i).c_str(), stripe_off,
                     stripe_byte_len, recovery_request->datanodeip(i).c_str(), recovery_request->datanodeport(i),
                     read_ec.message().c_str());
            set_phase2_error(buf);
            std::cerr << "[Proxy" << m_self_cluster_id << "][gLRC ILP Phase2] partition " << partition_id << " "
                      << buf << std::endl;
            exchange_failed.store(true);
            helper_round_cv.notify_all();
            decoded_cv.notify_all();
            return;
          }

          update_helper_breakdown_one(result.disk_io_start_time(), result.disk_io_end_time(),
                                      std::chrono::duration_cast<std::chrono::duration<double>>(
                                          net_begin.time_since_epoch())
                                          .count(),
                                      std::chrono::duration_cast<std::chrono::duration<double>>(
                                          net_end.time_since_epoch())
                                          .count(),
                                      grpc_notify, result.grpc_start_time());
          {
            std::lock_guard<std::mutex> lk(helper_round_mu);
            helper_round_done[shard_k]++;
          }
          helper_round_cv.notify_all();
          last_round = shard_k;
        }
        asio::error_code ignore_ec;
        socket.shutdown(asio::ip::tcp::socket::shutdown_both, ignore_ec);
        socket.close(ignore_ec);
      });
    }
  };

  auto stop_helper_workers = [&]() {
    {
      std::lock_guard<std::mutex> lk(helper_round_mu);
      helper_round_stop = true;
    }
    helper_round_cv.notify_all();
    for (auto &th : helper_workers)
    {
      if (th.joinable())
        th.join();
    }
  };

  const auto ex_begin = std::chrono::high_resolution_clock::now();
  start_exchange_workers();
  if (local_shard_count > 0)
  {
    start_helper_workers();
    start_helper_round(0);
  }

  std::vector<unsigned char *> block_ptrs = convertToUnsignedCharArray(get_bufs);
  std::vector<std::vector<unsigned char>> decode_rhs(f, std::vector<unsigned char>(static_cast<size_t>(stripe_byte_len)));
  std::vector<std::vector<unsigned char>> shard_recovered(f,
                                                          std::vector<unsigned char>(static_cast<size_t>(stripe_byte_len)));

  std::atomic<bool> decode_worker_failed{false};
  std::atomic<bool> decode_worker_stop{false};
  std::mutex decode_submit_mu;
  std::condition_variable decode_submit_cv;
  std::atomic<int> decode_submit_shard{-1};
  std::atomic<int> decode_inflight_shard{-1};
  std::mutex decode_metric_mu;

  std::thread decode_worker;
  if (local_shard_count > 0)
  {
    decode_worker = std::thread([&]() {
      while (true)
      {
        int shard_k = -1;
        {
          std::unique_lock<std::mutex> lk(decode_submit_mu);
          decode_submit_cv.wait(lk, [&]() {
            return decode_worker_stop.load() || decode_submit_shard.load() >= 0;
          });
          if (decode_worker_stop.load() && decode_submit_shard.load() < 0)
            return;
          shard_k = decode_submit_shard.exchange(-1);
          if (shard_k < 0)
            continue;
          decode_inflight_shard.store(shard_k);
        }

        const int stripe_off = byte_off + shard_k * stripe_byte_len;
        const auto t3 = std::chrono::high_resolution_clock::now();
        const bool decode_ok =
            decode_glrc_ilp_helper_compact_prepared(block_ptrs.data(), phase2_eq_helper_indices, phase2_eq_helper_g_tbls,
                                                    phase2_inv_g_tbls, f, byte_off, stripe_off, stripe_byte_len,
                                                    decode_rhs, shard_recovered);
        const auto t4 = std::chrono::high_resolution_clock::now();
        if (!decode_ok)
        {
          char buf[256];
          snprintf(buf, sizeof(buf), "decode failed shard=%d off=%d len=%d repair_block=%d", shard_k, stripe_off,
                   stripe_byte_len, repair_block_id);
          set_phase2_error(buf);
          std::cerr << "[Proxy" << m_self_cluster_id << "][gLRC ILP Phase2] partition " << partition_id << " " << buf
                    << std::endl;
          decode_worker_failed.store(true);
          exchange_failed.store(true);
          decoded_cv.notify_all();
          decode_inflight_shard.store(-1);
          decode_submit_cv.notify_all();
          return;
        }

        const double decode_duration = std::chrono::duration<double>(t4 - t3).count();
        {
          std::lock_guard<std::mutex> lk(decode_metric_mu);
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
        }

        for (int t = 0; t < f; t++)
          std::memcpy(recovered_ptrs[t] + stripe_off, shard_recovered[t].data(), stripe_byte_len);
        mark_write_shard_ready(shard_begin + shard_k);
        {
          std::lock_guard<std::mutex> lk(decoded_mu);
          decoded_shards = shard_k + 1;
        }
        decoded_cv.notify_all();
        decode_inflight_shard.store(-1);
        decode_submit_cv.notify_all();

        char buf[256];
        snprintf(buf, sizeof(buf), "shard pipeline done proxy=%s:%d partition=%d shard=%d/%d", m_ip.c_str(), m_port,
                 partition_id, shard_k + 1, local_shard_count);
        phase2_trace(buf);
      }
    });
  }

  for (int shard_k = 0; shard_k < local_shard_count && !exchange_failed.load(); shard_k++)
  {
    if (!wait_helper_round_done(shard_k))
    {
      exchange_failed.store(true);
      break;
    }
    start_helper_round(shard_k + 1);

    {
      std::unique_lock<std::mutex> lk(decode_submit_mu);
      decode_submit_cv.wait(lk, [&]() {
        return exchange_failed.load() || decode_submit_shard.load() < 0;
      });
      if (exchange_failed.load())
        break;
      decode_submit_shard.store(shard_k);
    }
    decode_submit_cv.notify_one();
  }

  if (local_shard_count > 0)
  {
    {
      std::unique_lock<std::mutex> lk(decode_submit_mu);
      decode_submit_cv.wait(lk, [&]() {
        if (exchange_failed.load())
          return true;
        if (decode_submit_shard.load() >= 0 || decode_inflight_shard.load() >= 0)
          return false;
        std::lock_guard<std::mutex> dlk(decoded_mu);
        return decoded_shards >= local_shard_count;
      });
    }
    decode_worker_stop.store(true);
    decode_submit_cv.notify_all();
    if (decode_worker.joinable())
      decode_worker.join();
    if (decode_worker_failed.load())
      exchange_failed.store(true);
  }

  stop_helper_workers();
  {
    std::lock_guard<std::mutex> lk(decoded_mu);
    decode_done = true;
  }
  decoded_cv.notify_all();
  join_exchange_workers();

  if (write_stream)
  {
    {
      std::lock_guard<std::mutex> lock(write_stream->mu);
      write_stream->producer_done = true;
    }
    write_stream->cv.notify_all();
    if (write_stream->writer_thread.joinable())
      write_stream->writer_thread.join();
    else
    {
      asio::error_code ignore_ec;
      write_stream->socket.close(ignore_ec);
      write_stream->grpc_context.TryCancel();
    }
    if (write_stream->grpc_thread.joinable())
      write_stream->grpc_thread.join();

    if (!exchange_failed.load() &&
        (write_stream->failed || write_stream->next_shard != total_shard_count ||
         !write_stream->grpc_status.ok() || !write_stream->recovery_result.message()))
    {
      set_phase2_error(hybrid_partial_write
                           ? "phase2 hybrid prefix write stream did not complete"
                           : "phase2 full-block write stream did not complete");
      exchange_failed.store(true);
    }
    total_write_net += write_stream->network_time;
    total_write_disk += write_stream->recovery_result.disk_io_end_time() -
                        write_stream->recovery_result.disk_io_start_time();
    response->set_recovery_commit_time(write_stream->recovery_result.recovery_commit_time());
  }

  const auto ex_end = std::chrono::high_resolution_clock::now();
  total_exchange_time += std::chrono::duration<double>(ex_end - ex_begin).count();

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
    std::lock_guard<std::mutex> lk(active_acceptor().incoming_mu);
    for (auto &kv : active_acceptor().server_peer_socks)
    {
      if (kv.second)
        close_phase2_socket(*kv.second);
    }
  }
  join_accept_thread();

  if (exchange_failed.load() || active_acceptor().accept_failed.load())
  {
    std::cerr << "[Proxy" << m_self_cluster_id << "][gLRC ILP Phase2] partition " << partition_id
              << " streaming peer exchange failed" << std::endl;
    if (!exchange_failed.load())
      set_phase2_error("phase2 accept failed");
    for (unsigned char *p : recovered_ptrs)
      delete[] p;
    return false;
  }

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
  response->set_cross_rack_time(total_exchange_time);
  response->set_cross_rack_xor_time(total_decode_time);
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
