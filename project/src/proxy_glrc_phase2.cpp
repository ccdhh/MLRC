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
#include <vector>

namespace ECProject
{
namespace
{
constexpr uint32_t kPhase2Magic = 0x50483232u; // "PH22" slim single-block peer exchange

thread_local std::string g_glrc_phase2_last_error;

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
    SharedBandwidthLimiter *bw = block_bandwidth != nullptr ? block_bandwidth : m_ingress_bandwidth.get();
    tcp_read_with_shared_bandwidth(socket, value + read_offset, static_cast<size_t>(read_length), bw, ec);
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
  struct IncomingPeer
  {
    std::unique_ptr<asio::ip::tcp::socket> sock;
    Phase2PeerHeader hdr{};
    std::vector<char> payload;
  };
  std::map<int, IncomingPeer> incoming_by_part;
  std::deque<std::unique_ptr<asio::ip::tcp::socket>> pending_accept_socks;
  std::atomic<bool> accept_failed{false};
  std::atomic<bool> accept_stop{false};
  std::thread accept_thread;
  bool accept_started = false;

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
  };
  auto start_exchange_acceptor = [&]() -> bool {
    if (accept_remaining <= 0 || accept_started)
      return true;
    accept_stop.store(false);
    accept_failed.store(false);
    try
    {
      acceptor = std::make_unique<asio::ip::tcp::acceptor>(accept_io);
      asio::ip::tcp::endpoint listen_ep(asio::ip::address::from_string(m_ip),
                                        phase2_exchange_port(m_port, partition_id, exchange_epoch));
      acceptor->open(listen_ep.protocol());
      acceptor->set_option(asio::ip::tcp::acceptor::reuse_address(true));
      acceptor->bind(listen_ep);
      acceptor->listen(asio::socket_base::max_listen_connections);
      acceptor->non_blocking(true);
    }
    catch (const std::exception &e)
    {
      char buf[384];
      snprintf(buf, sizeof(buf), "exchange bind failed port=%d: %s",
               phase2_exchange_port(m_port, partition_id, exchange_epoch), e.what());
      set_phase2_error(buf);
      std::cerr << "[Proxy" << m_self_cluster_id << "][gLRC ILP Phase2] partition " << partition_id << " " << buf
                << std::endl;
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
        {
          std::lock_guard<std::mutex> lk(incoming_mu);
          pending_accept_socks.push_back(std::move(sock));
        }
        incoming_cv.notify_all();
        accepted++;
      }
    });
    accept_started = true;
    return true;
  };

  std::unique_ptr<bool[]> status(new bool[helper_n]);
  std::fill_n(status.get(), helper_n, false);
  std::vector<char *> get_bufs(helper_n);
  for (int i = 0; i < helper_n; i++)
  {
    get_bufs[i] = static_cast<char *>(std::aligned_alloc(32, block_size));
    std::memset(get_bufs[i], 0, block_size);
  }

  std::vector<double> disk_io_start(helper_n, 0.0), disk_io_end(helper_n, 0.0);
  std::vector<double> net_start(helper_n, 0.0), net_end(helper_n, 0.0);
  std::vector<double> grpc_notify(helper_n, 0.0), grpc_start(helper_n, 0.0);

  std::vector<std::thread> get_threads;
  for (int i = 0; i < helper_n; i++)
  {
    // Partial (sharded) helper read: only this partition's byte range [byte_off, byte_off+byte_len).
    // This is the bandwidth optimization of Phase2 sharding. Correct concurrent pairing of the gRPC
    // request and the proxy's data connection is guaranteed by the datanode returning a dedicated
    // per-request data_port (see handleGetBreakdown), so distinct byte ranges of the same block can
    // be read concurrently without mispairing.
    get_threads.emplace_back(&ProxyImpl::get_from_node_stripe_range_breakdown, this, recovery_request->blockkeys(i),
                             get_bufs[i], static_cast<size_t>(block_size),
                             recovery_request->phase2_byte_off(), recovery_request->phase2_byte_len(),
                             recovery_request->datanodeip(i).c_str(), recovery_request->datanodeport(i), status.get(),
                             i, &disk_io_start[i], &disk_io_end[i], &net_start[i], &net_end[i], &grpc_notify[i],
                             &grpc_start[i], block_bw.ingress);
  }
  for (auto &th : get_threads)
    th.join();

  {
    char buf[256];
    snprintf(buf, sizeof(buf), "helpers joined proxy=%s:%d partition=%d", m_ip.c_str(), m_port, partition_id);
    phase2_trace(buf);
  }

  if (!std::all_of(status.get(), status.get() + helper_n, [](bool v) { return v; }))
  {
    for (int i = 0; i < helper_n; i++)
    {
      if (!status[i])
      {
        char buf[384];
        snprintf(buf, sizeof(buf), "helper read failed idx=%d block_id=%d key=%s @ %s:%d", i,
                 recovery_request->blockids(i), recovery_request->blockkeys(i).c_str(),
                 recovery_request->datanodeip(i).c_str(), recovery_request->datanodeport(i));
        set_phase2_error(buf);
        std::cerr << "[Proxy" << m_self_cluster_id << "][gLRC ILP Phase2] partition " << partition_id << " "
                  << buf << std::endl;
        break;
      }
    }
    for (char *p : get_bufs)
      free(p);
    return false;
  }

  response->set_disk_io_start_time(*std::min_element(disk_io_start.begin(), disk_io_start.end()));
  response->set_disk_io_end_time(*std::max_element(disk_io_end.begin(), disk_io_end.end()));
  response->set_network_start_time(*std::min_element(net_start.begin(), net_start.end()));
  response->set_network_end_time(*std::max_element(net_end.begin(), net_end.end()));
  response->set_data_node_grpc_notify_time(*std::min_element(grpc_notify.begin(), grpc_notify.end()));
  response->set_data_node_grpc_start_time(*std::max_element(grpc_start.begin(), grpc_start.end()));

  std::vector<int> block_idxs;
  for (int i = 0; i < helper_n; i++)
    block_idxs.push_back(recovery_request->blockids(i));
  std::vector<unsigned char *> block_ptrs = convertToUnsignedCharArray(get_bufs);

  std::vector<int> failed_ids;
  std::vector<int> eq_indices;
  for (int i = 0; i < f; i++)
    failed_ids.push_back(recovery_request->failed_block_ids(i));
  for (int i = 0; i < recovery_request->selected_equation_indices_size(); i++)
    eq_indices.push_back(recovery_request->selected_equation_indices(i));

  if ((int)eq_indices.size() != f)
  {
    set_phase2_error("selected_equation_indices size mismatch");
    for (char *p : get_bufs)
      free(p);
    return false;
  }

  auto t3 = std::chrono::high_resolution_clock::now();
  std::vector<unsigned char *> recovered_ptrs;
  bool decode_ok = decode_glrc_ilp_range(m_sys_config->k, m_sys_config->r, m_sys_config->z, block_size, block_idxs,
                                         block_ptrs.data(), failed_ids, eq_indices, byte_off, byte_len, recovered_ptrs);
  auto t4 = std::chrono::high_resolution_clock::now();
  {
    char buf[256];
    snprintf(buf, sizeof(buf), "decode done proxy=%s:%d partition=%d ok=%d", m_ip.c_str(), m_port, partition_id,
             decode_ok ? 1 : 0);
    phase2_trace(buf);
  }
  response->set_decode_start_time(
      std::chrono::duration_cast<std::chrono::duration<double>>(t3.time_since_epoch()).count());
  response->set_decode_end_time(std::chrono::duration_cast<std::chrono::duration<double>>(t4.time_since_epoch()).count());

  for (char *p : get_bufs)
    free(p);

  if (!decode_ok)
  {
    std::cerr << "[Proxy" << m_self_cluster_id << "][gLRC ILP Phase2] partition " << partition_id
              << " decode_glrc_ilp_range failed (off=" << byte_off << " len=" << byte_len << ")" << std::endl;
    {
      char buf[256];
      snprintf(buf, sizeof(buf), "decode failed off=%d len=%d repair_block=%d", byte_off, byte_len, repair_block_id);
      set_phase2_error(buf);
    }
    for (unsigned char *p : recovered_ptrs)
      delete[] p;
    return false;
  }

  const int shard_begin = recovery_request->phase2_shard_begin();
  const int local_shard_count = recovery_request->phase2_shard_count_local();
  const size_t local_send_bytes =
      static_cast<size_t>(local_shard_count) * static_cast<size_t>(stripe_byte_len);

  auto make_out_header = [&]() {
    Phase2PeerHeader hdr{};
    hdr.magic = kPhase2Magic;
    hdr.from_partition = static_cast<uint32_t>(partition_id);
    hdr.stripe_byte_len = static_cast<uint32_t>(stripe_byte_len);
    hdr.shard_begin = static_cast<uint32_t>(shard_begin);
    hdr.shard_count = static_cast<uint32_t>(local_shard_count);
    return hdr;
  };

  if (!start_exchange_acceptor())
  {
    for (unsigned char *p : recovered_ptrs)
      delete[] p;
    return false;
  }

  {
    char buf[256];
    snprintf(buf, sizeof(buf), "exchange start proxy=%s:%d partition=%d peers=%d accept_remaining=%d", m_ip.c_str(),
             m_port, partition_id, recovery_request->phase2_peer_partition_ids_size(), accept_remaining);
    phase2_trace(buf);
  }

  // Per peer: send peer_part's block strip (local shards only); recv home block strip from peer.
  // f-1 peer sessions run in parallel.
  std::atomic<bool> exchange_failed{false};
  constexpr int kExchangeSocketTimeoutSec = 45;

  auto process_one_pending_socket = [&](std::unique_lock<std::mutex> &lk) -> bool {
    if (pending_accept_socks.empty())
      return false;
    auto sock = std::move(pending_accept_socks.front());
    pending_accept_socks.pop_front();
    lk.unlock();

    set_exchange_socket_timeouts(*sock, kExchangeSocketTimeoutSec);
    asio::error_code ec;
    Phase2PeerHeader rhdr{};
    bool ok = read_all(*sock, reinterpret_cast<char *>(&rhdr), sizeof(rhdr), ec) && rhdr.magic == kPhase2Magic;
    std::vector<char> payload;
    if (ok)
    {
      const size_t payload_sz =
          static_cast<size_t>(rhdr.shard_count) * static_cast<size_t>(rhdr.stripe_byte_len);
      payload.resize(payload_sz);
      ok = read_all(*sock, payload.data(), payload_sz, ec);
    }
    if (!ok)
    {
      set_phase2_error("server header/payload read failed: " + ec.message());
      std::cerr << "[Proxy" << m_self_cluster_id << "][gLRC ILP Phase2] partition " << partition_id
                << " server header/payload read failed: " << ec.message() << " magic=" << rhdr.magic << std::endl;
      exchange_failed.store(true);
      lk.lock();
      return false;
    }
    const int from_part = static_cast<int>(rhdr.from_partition);
    lk.lock();
    IncomingPeer peer;
    peer.sock = std::move(sock);
    peer.hdr = rhdr;
    peer.payload = std::move(payload);
    incoming_by_part[from_part] = std::move(peer);
    incoming_cv.notify_all();
    return true;
  };

  const auto ex_begin = std::chrono::high_resolution_clock::now();
  std::vector<std::thread> exchange_threads;

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

    exchange_threads.emplace_back([&, peer_part, peer]() {
      if (exchange_failed.load())
        return;

      int peer_shard_begin = 0;
      int peer_shard_count = 0;
      if (!lookup_peer_shards(recovery_request, peer_part, peer_shard_begin, peer_shard_count))
      {
        exchange_failed.store(true);
        return;
      }
      const size_t recv_bytes =
          static_cast<size_t>(peer_shard_count) * static_cast<size_t>(stripe_byte_len);

      const int peer_port = recovery_request->phase2_peer_proxy_ports(peer);
      const std::string peer_ip = recovery_request->phase2_peer_proxy_ips(peer);

      Phase2PeerHeader out_hdr = make_out_header();
      std::vector<char> send_strip(local_send_bytes);
      std::memcpy(send_strip.data(), recovered_ptrs[peer_part] + byte_off, local_send_bytes);

      asio::error_code ec;

      if (partition_id < peer_part)
      {
        asio::io_context io;
        asio::ip::tcp::socket sock(io);
        const auto connect_deadline = std::chrono::steady_clock::now() + std::chrono::seconds(20);
        bool connected = false;
        while (!connected && std::chrono::steady_clock::now() < connect_deadline && !exchange_failed.load())
        {
          ec.clear();
          asio::ip::tcp::socket try_sock(io);
          asio::connect(try_sock,
                        asio::ip::tcp::resolver(io).resolve(
                            peer_ip, std::to_string(phase2_exchange_port(peer_port, peer_part, exchange_epoch))),
                        ec);
          if (!ec)
          {
            sock = std::move(try_sock);
            connected = true;
            break;
          }
          std::this_thread::sleep_for(std::chrono::milliseconds(10));
        }
        if (!connected)
        {
          std::cout << "[Proxy" << m_self_cluster_id << "][gLRC ILP Phase2] partition " << partition_id
                    << " connect peer " << peer_part << " port "
                    << phase2_exchange_port(peer_port, peer_part, exchange_epoch) << " failed: " << ec.message()
                    << std::endl;
          set_phase2_error("connect to peer " + std::to_string(peer_part) + " failed");
          exchange_failed.store(true);
          return;
        }
        set_exchange_socket_timeouts(sock, kExchangeSocketTimeoutSec);
        if (!write_all_bw(sock, reinterpret_cast<char *>(&out_hdr), sizeof(out_hdr), block_bw.egress, ec) ||
            !write_all_bw(sock, send_strip.data(), send_strip.size(), block_bw.egress, ec))
        {
          set_phase2_error("client write to peer " + std::to_string(peer_part) + " failed: " + ec.message());
          std::cerr << "[Proxy" << m_self_cluster_id << "][gLRC ILP Phase2] partition " << partition_id
                    << " client write to peer " << peer_part << " failed: " << ec.message() << std::endl;
          exchange_failed.store(true);
          close_phase2_socket(sock);
          return;
        }
        Phase2PeerHeader rhdr{};
        std::vector<char> recv_strip(recv_bytes);
        if (!read_all_bw(sock, reinterpret_cast<char *>(&rhdr), sizeof(rhdr), block_bw.ingress, ec) ||
            rhdr.magic != kPhase2Magic ||
            !read_all_bw(sock, recv_strip.data(), recv_strip.size(), block_bw.ingress, ec))
        {
          set_phase2_error("client read from peer " + std::to_string(peer_part) + " failed: " + ec.message());
          std::cerr << "[Proxy" << m_self_cluster_id << "][gLRC ILP Phase2] partition " << partition_id
                    << " client read from peer " << peer_part << " failed: " << ec.message()
                    << " magic=" << rhdr.magic << std::endl;
          exchange_failed.store(true);
          close_phase2_socket(sock);
          return;
        }
        std::memcpy(recovered_ptrs[partition_id] + static_cast<size_t>(peer_shard_begin) * stripe_byte_len,
                    recv_strip.data(), recv_bytes);
        close_phase2_socket(sock);
        {
          char buf[256];
          snprintf(buf, sizeof(buf), "exchange client done partition=%d peer=%d recv=%zu", partition_id, peer_part,
                   recv_bytes);
          phase2_trace(buf);
        }
      }
      else
      {
        IncomingPeer incoming;
        {
          std::unique_lock<std::mutex> lk(incoming_mu);
          const auto wait_deadline = std::chrono::steady_clock::now() + std::chrono::seconds(45);
          while (incoming_by_part.find(peer_part) == incoming_by_part.end() && !accept_failed.load() &&
                 !exchange_failed.load())
          {
            while (incoming_by_part.find(peer_part) == incoming_by_part.end() && process_one_pending_socket(lk))
            {
            }
            if (incoming_by_part.find(peer_part) != incoming_by_part.end())
              break;
            if (incoming_cv.wait_until(lk, wait_deadline) == std::cv_status::timeout)
              break;
          }
          if (incoming_by_part.find(peer_part) == incoming_by_part.end())
          {
            set_phase2_error("exchange wait peer " + std::to_string(peer_part) + " timed out");
            exchange_failed.store(true);
            return;
          }
          incoming = std::move(incoming_by_part[peer_part]);
          incoming_by_part.erase(peer_part);
        }
        std::memcpy(recovered_ptrs[partition_id] + static_cast<size_t>(peer_shard_begin) * stripe_byte_len,
                    incoming.payload.data(), incoming.payload.size());
        set_exchange_socket_timeouts(*incoming.sock, kExchangeSocketTimeoutSec);
        if (!write_all_bw(*incoming.sock, reinterpret_cast<char *>(&out_hdr), sizeof(out_hdr), block_bw.egress, ec) ||
            !write_all_bw(*incoming.sock, send_strip.data(), send_strip.size(), block_bw.egress, ec))
        {
          set_phase2_error("server write to peer " + std::to_string(peer_part) + " failed: " + ec.message());
          std::cerr << "[Proxy" << m_self_cluster_id << "][gLRC ILP Phase2] partition " << partition_id
                    << " server write to peer " << peer_part << " failed: " << ec.message() << std::endl;
          exchange_failed.store(true);
          close_phase2_socket(*incoming.sock);
          return;
        }
        close_phase2_socket(*incoming.sock);
        {
          char buf[256];
          snprintf(buf, sizeof(buf), "exchange server done partition=%d peer=%d recv=%zu", partition_id, peer_part,
                   incoming.payload.size());
          phase2_trace(buf);
        }
      }
    });
  }

  for (auto &th : exchange_threads)
    th.join();
  if (accept_failed.load())
    exchange_failed.store(true);

  if (exchange_failed.load())
  {
    std::cerr << "[Proxy" << m_self_cluster_id << "][gLRC ILP Phase2] partition " << partition_id
              << " slim peer exchange failed" << std::endl;
    set_phase2_error("slim peer exchange failed");
    join_accept_thread();
    for (unsigned char *p : recovered_ptrs)
      delete[] p;
    return false;
  }

  const auto ex_end = std::chrono::high_resolution_clock::now();
  const double exchange_net_time = std::chrono::duration<double>(ex_end - ex_begin).count();

  {
    char buf[256];
    snprintf(buf, sizeof(buf), "exchange done proxy=%s:%d partition=%d time=%f", m_ip.c_str(), m_port, partition_id,
             exchange_net_time);
    phase2_trace(buf);
  }

  join_accept_thread();

  response->set_cross_rack_time(exchange_net_time);

  if (recovery_request->phase2_do_write_back())
  {
    double total_write_net = 0.0;
    double total_write_disk = 0.0;
    const int write_idx = partition_id;
    if (write_idx >= 0 && write_idx < f)
    {
      double wnet = 0.0, wdisk = 0.0;
      if (!RecoveryToDatanodeBreakdown(recovery_request->failed_block_keys(write_idx).c_str(),
                                       failed_ids[write_idx], reinterpret_cast<char *>(recovered_ptrs[write_idx]),
                                       recovery_request->replaced_node_ips(write_idx).c_str(),
                                       recovery_request->replaced_node_ports(write_idx), &wnet, &wdisk))
      {
        for (unsigned char *p : recovered_ptrs)
          delete[] p;
        return false;
      }
      total_write_net += wnet;
      total_write_disk += wdisk;
    }
    response->set_dest_data_node_network_time(total_write_net);
    response->set_dest_data_node_disk_io_time(total_write_disk);
  }

  for (unsigned char *p : recovered_ptrs)
    delete[] p;

  {
    char buf[256];
    snprintf(buf, sizeof(buf), "ok proxy=%s:%d partition=%d", m_ip.c_str(), m_port, partition_id);
    phase2_trace(buf);
  }
  std::cout << "[Proxy" << m_self_cluster_id << "][gLRC ILP Phase2] partition " << partition_id
            << " shards=" << local_shard_count << " byte_len=" << byte_len << " ok" << std::endl;
  return true;
}

} // namespace ECProject
