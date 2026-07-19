#include "proxy.h"
#include "config.h"
#include "glrc_pipeline_plan.h"
#include "glrc_pipeline_codec.h"
#include "link_bandwidth.h"
#include "unilrc_encoder.h"
#include <asio.hpp>
#include <algorithm>
#include <atomic>
#include <chrono>
#include <cstdlib>
#include <cstdint>
#include <cstdio>
#include <cstring>
#include <iostream>
#include <memory>
#include <condition_variable>
#include <mutex>
#include <optional>
#include <queue>
#include <string>
#include <thread>
#include <unordered_map>
#include <unordered_set>
#include <vector>

#if defined(__linux__) || defined(__APPLE__)
#include <sys/socket.h>
#ifndef SO_REUSEPORT
#define SO_REUSEPORT 15
#endif
#endif

namespace ECProject
{
namespace
{
constexpr uint32_t kPipelineMagic = 0x504C5031u;    // "PLP1" shard payload
constexpr uint32_t kPipelineStreamEnd = 0x504C4544u; // "PLED" end of shard stream
// A healthy 64 MiB pipeline finishes in a few seconds even with fan-in
// contention.  Do not leave all subsequent trials blocked for two minutes
// when one peer has already failed.
constexpr int kPipelineSocketTimeoutSec = 15;

void set_pipeline_socket_timeouts(asio::ip::tcp::socket &socket, int seconds)
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

enum class PipelineFrameType
{
  SHARD,
  END,
  ERROR
};

struct PipelineStreamEndHeader
{
  uint32_t magic;
  uint32_t eq_slot;
  uint32_t shard_count;
  uint32_t reserved;
};

thread_local std::string g_glrc_pipeline_last_error;
thread_local int g_pipeline_dn_read_offset = 0;
thread_local int g_pipeline_dn_read_length = 0;
static std::mutex g_pipeline_bind_mutex;
static std::unordered_set<int> g_pipeline_active_listen_ports;

struct PipelineHopReadyAcceptor
{
  std::shared_ptr<asio::io_context> io;
  std::shared_ptr<asio::ip::tcp::acceptor> acceptor;
  int listen_port = 0;
};

struct PipelineHubReadyAcceptor
{
  int ci = 0;
  std::shared_ptr<asio::io_context> io;
  std::shared_ptr<asio::ip::tcp::acceptor> acceptor;
  int listen_port = 0;
};

struct PipelineReadySession
{
  int exchange_epoch = -1;
  std::vector<PipelineHubReadyAcceptor> hub_acceptors;
  std::map<std::string, PipelineHopReadyAcceptor> hop_acceptors;
};

static std::mutex g_pipeline_ready_mutex;
static PipelineReadySession g_pipeline_ready_session;

void close_pipeline_acceptor(asio::ip::tcp::acceptor &acceptor);

static std::string hop_ready_key(int epoch, int chain_id, int hop_idx)
{
  return std::to_string(epoch) + ":" + std::to_string(chain_id) + ":" + std::to_string(hop_idx);
}

static void reset_pipeline_ready_session()
{
  std::lock_guard<std::mutex> lock(g_pipeline_ready_mutex);
  for (auto &h : g_pipeline_ready_session.hub_acceptors)
  {
    if (h.acceptor)
      close_pipeline_acceptor(*h.acceptor);
  }
  for (auto &kv : g_pipeline_ready_session.hop_acceptors)
  {
    if (kv.second.acceptor)
      close_pipeline_acceptor(*kv.second.acceptor);
  }
  g_pipeline_ready_session = PipelineReadySession{};
}

static bool take_hop_ready_acceptor(int epoch, int chain_id, int hop_idx, int listen_port,
                                    std::shared_ptr<asio::io_context> &io_out,
                                    std::shared_ptr<asio::ip::tcp::acceptor> &acceptor_out)
{
  const std::string key = hop_ready_key(epoch, chain_id, hop_idx);
  std::lock_guard<std::mutex> lock(g_pipeline_ready_mutex);
  if (g_pipeline_ready_session.exchange_epoch != epoch)
    return false;
  auto it = g_pipeline_ready_session.hop_acceptors.find(key);
  if (it == g_pipeline_ready_session.hop_acceptors.end() || !it->second.acceptor ||
      it->second.listen_port != listen_port)
    return false;
  io_out = std::move(it->second.io);
  acceptor_out = std::move(it->second.acceptor);
  g_pipeline_ready_session.hop_acceptors.erase(it);
  return true;
}

static std::vector<PipelineHubReadyAcceptor> take_hub_ready_acceptors(int epoch)
{
  std::lock_guard<std::mutex> lock(g_pipeline_ready_mutex);
  if (g_pipeline_ready_session.exchange_epoch != epoch)
    return {};
  std::vector<PipelineHubReadyAcceptor> out = std::move(g_pipeline_ready_session.hub_acceptors);
  g_pipeline_ready_session.hub_acceptors.clear();
  return out;
}

void pipeline_trace(const char *msg)
{
  FILE *f = fopen("/users/chendh/DdlRT/logs/pipeline_trace.log", "a");
  if (f)
  {
    fprintf(f, "%s\n", msg);
    fclose(f);
  }
}

void set_pipeline_error(const std::string &msg)
{
  g_glrc_pipeline_last_error = msg;
  pipeline_trace(msg.c_str());
}

int resolve_pipeline_window(const Config *cfg, int shard_count)
{
  if (shard_count <= 0)
    return 1;
  if (cfg == nullptr)
    return shard_count;
  const int w = cfg->GlrcPipelineWindow;
  if (w == 0)
    return shard_count;
  if (w <= 1)
    return 1;
  return std::min(w, shard_count);
}

struct PipelineShardView
{
  int global_begin = 0;
  int local_count = 0;
  int global_S = 0;
  int stripe_len = 0;

  int global_shard(int local_i) const { return global_begin + local_i; }
  int byte_off(int local_i) const { return global_shard(local_i) * stripe_len; }

  bool valid_geometry(int block_size) const
  {
    return local_count > 0 && global_S > 0 && stripe_len > 0 && global_begin >= 0 &&
           global_begin + local_count <= global_S &&
           static_cast<size_t>(global_S) * static_cast<size_t>(stripe_len) == static_cast<size_t>(block_size);
  }
};

PipelineShardView resolve_pipeline_shard_view(const proxy_proto::RecoveryRequest *req, int block_size,
                                              const Config *cfg)
{
  PipelineShardView v;
  v.global_S = req->pipeline_global_shard_count() > 0 ? req->pipeline_global_shard_count()
              : (req->pipeline_shard_count() > 0 ? req->pipeline_shard_count() : cfg->GlrcShardCount);
  v.local_count = req->pipeline_shard_count() > 0 ? req->pipeline_shard_count() : v.global_S;
  v.global_begin = req->pipeline_shard_global_begin();
  if (v.global_begin + v.local_count > v.global_S)
    v.local_count = std::max(0, v.global_S - v.global_begin);
  v.stripe_len = (v.global_S > 0 && block_size % v.global_S == 0) ? block_size / v.global_S : 0;
  return v;
}

struct PipelineQueuedShard
{
  int shard_id = -1;
  std::vector<unsigned char> payload;
  bool end_of_stream = false;
  int end_shard_count = 0;
};

class PipelineBoundedQueue
{
public:
  explicit PipelineBoundedQueue(size_t capacity) : capacity_(capacity > 0 ? capacity : 1) {}

  void push(PipelineQueuedShard item)
  {
    std::unique_lock<std::mutex> lock(mu_);
    not_full_.wait(lock, [&] { return closed_ || q_.size() < capacity_; });
    if (closed_)
      return;
    q_.push(std::move(item));
    not_empty_.notify_one();
  }

  bool pop(PipelineQueuedShard &out)
  {
    std::unique_lock<std::mutex> lock(mu_);
    not_empty_.wait(lock, [&] { return closed_ || !q_.empty(); });
    if (q_.empty())
      return false;
    out = std::move(q_.front());
    q_.pop();
    not_full_.notify_one();
    return true;
  }

  void close()
  {
    std::lock_guard<std::mutex> lock(mu_);
    closed_ = true;
    not_empty_.notify_all();
    not_full_.notify_all();
  }

private:
  size_t capacity_;
  std::queue<PipelineQueuedShard> q_;
  std::mutex mu_;
  std::condition_variable not_full_;
  std::condition_variable not_empty_;
  bool closed_ = false;
};

int pipeline_proxy_index(int proxy_grpc_port)
{
  // In one-proxy-per-host deployments listener ports only need to be unique
  // within that host. Reusing band zero keeps the port range below ephemeral
  // ports even for large n (for example n≈105).
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

int pipeline_compose_listen_port(int proxy_grpc_port, int slot_core, bool hub_band)
{
  const int proxy_idx = pipeline_proxy_index(proxy_grpc_port);
  const int half = PROXY_PIPELINE_PER_PROXY_BAND / 2;
  int slot = hub_band ? (half + (slot_core % half)) : (slot_core % half);
  int port = PROXY_PIPELINE_EXCHANGE_BASE + proxy_idx * PROXY_PIPELINE_PER_PROXY_BAND + slot;
  if (port == 55555)
  {
    slot = hub_band ? (half + ((slot_core + 1) % half)) : ((slot_core + 1) % half);
    port = PROXY_PIPELINE_EXCHANGE_BASE + proxy_idx * PROXY_PIPELINE_PER_PROXY_BAND + slot;
  }
  return port;
}

int avoid_reserved_pipeline_port(int port, int proxy_grpc_port, int slot_core, bool hub_band)
{
  (void)slot_core;
  (void)hub_band;
  if (port != 55555)
    return port;
  return pipeline_compose_listen_port(proxy_grpc_port, slot_core + 1, hub_band);
}

int pipeline_hop_listen_port(int proxy_grpc_port, int exchange_epoch, int chain_id, int hop_index)
{
  const int slot_core = (exchange_epoch * 17 + chain_id * 32 + hop_index) % (PROXY_PIPELINE_PER_PROXY_BAND / 2);
  return pipeline_compose_listen_port(proxy_grpc_port, slot_core, false);
}

int pipeline_hub_listen_port(int hub_proxy_grpc_port, int exchange_epoch, int eq_slot)
{
  const int slot_core = (exchange_epoch * 17 + eq_slot * 32 + 1) % (PROXY_PIPELINE_PER_PROXY_BAND / 2);
  return pipeline_compose_listen_port(hub_proxy_grpc_port, slot_core, true);
}

int resolve_hop_listen_port(const proxy_proto::RecoveryRequest *recovery_request, int hop_index,
                            int fallback_grpc_port, int exchange_epoch, int chain_id)
{
  if (recovery_request->pipeline_hop_listen_ports_size() > hop_index &&
      recovery_request->pipeline_hop_listen_ports(hop_index) > 0)
    return recovery_request->pipeline_hop_listen_ports(hop_index);
  return pipeline_hop_listen_port(fallback_grpc_port, exchange_epoch, chain_id, hop_index);
}

int resolve_chain_hub_listen_port(const proxy_proto::RecoveryRequest *recovery_request, int hub_grpc_port,
                                  int exchange_epoch, int eq_slot)
{
  if (recovery_request->pipeline_chain_hub_listen_port() > 0)
    return recovery_request->pipeline_chain_hub_listen_port();
  return pipeline_hub_listen_port(hub_grpc_port, exchange_epoch, eq_slot);
}

int resolve_hub_listener_port(const proxy_proto::RecoveryRequest *recovery_request, int listener_index,
                              int hub_grpc_port, int exchange_epoch, int eq_slot)
{
  if (recovery_request->pipeline_hub_listener_ports_size() > listener_index &&
      recovery_request->pipeline_hub_listener_ports(listener_index) > 0)
    return recovery_request->pipeline_hub_listener_ports(listener_index);
  return pipeline_hub_listen_port(hub_grpc_port, exchange_epoch, eq_slot);
}

int resolve_my_hop_bind_port(const proxy_proto::RecoveryRequest *recovery_request, int proxy_grpc_port,
                             int exchange_epoch, int chain_id, int hop_index)
{
  if (recovery_request->pipeline_my_listen_port() > 0)
    return recovery_request->pipeline_my_listen_port();
  if (recovery_request->pipeline_hop_listen_ports_size() > hop_index &&
      recovery_request->pipeline_hop_listen_ports(hop_index) > 0)
    return recovery_request->pipeline_hop_listen_ports(hop_index);
  return pipeline_hop_listen_port(proxy_grpc_port, exchange_epoch, chain_id, hop_index);
}

struct PipelineShardHeader
{
  uint32_t magic;
  uint32_t eq_slot;
  uint32_t shard_id;
  uint32_t stripe_len;
};

bool write_all(asio::ip::tcp::socket &socket, const char *data, size_t len, asio::error_code &ec)
{
  ec.clear();
  size_t off = 0;
  while (off < len && !ec)
    off += asio::write(socket, asio::buffer(data + off, len - off), ec);
  return !ec && off == len;
}

bool read_all(asio::ip::tcp::socket &socket, char *data, size_t len, asio::error_code &ec)
{
  ec.clear();
  size_t off = 0;
  while (off < len && !ec)
    off += asio::read(socket, asio::buffer(data + off, len - off), ec);
  return !ec && off == len;
}

GlrcPipelineEqCodec pipeline_codec_from_request(const proxy_proto::RecoveryRequest *recovery_request)
{
  return recovery_request->pipeline_equation_is_local() != 0 ? GlrcPipelineEqCodec::LOCAL_XOR
                                                             : GlrcPipelineEqCodec::GLOBAL_CAUCHY;
}

GlrcPipelineEqCodec pipeline_hub_chain_codec(const proxy_proto::RecoveryRequest *recovery_request, int chain_idx)
{
  if (recovery_request->pipeline_hub_chain_equation_is_local_size() > chain_idx)
    return recovery_request->pipeline_hub_chain_equation_is_local(chain_idx) != 0
               ? GlrcPipelineEqCodec::LOCAL_XOR
               : GlrcPipelineEqCodec::GLOBAL_CAUCHY;
  return GlrcPipelineEqCodec::GLOBAL_CAUCHY;
}

bool send_pipeline_shard(asio::ip::tcp::socket &socket, int eq_slot, int shard_id, int stripe_len,
                         const unsigned char *payload, SharedBandwidthLimiter *bw, asio::error_code &ec)
{
  PipelineShardHeader hdr{};
  hdr.magic = kPipelineMagic;
  hdr.eq_slot = static_cast<uint32_t>(eq_slot);
  hdr.shard_id = static_cast<uint32_t>(shard_id);
  hdr.stripe_len = static_cast<uint32_t>(stripe_len);
  if (!write_all(socket, reinterpret_cast<const char *>(&hdr), sizeof(hdr), ec))
    return false;
  if (bw != nullptr && bw->bandwidth_mbps() > 0.0)
  {
    tcp_write_with_shared_bandwidth(socket, reinterpret_cast<const char *>(payload), stripe_len, bw, ec);
    return !ec;
  }
  return write_all(socket, reinterpret_cast<const char *>(payload), stripe_len, ec);
}

bool send_pipeline_stream_end(asio::ip::tcp::socket &socket, int eq_slot, int shard_count, SharedBandwidthLimiter *bw,
                            asio::error_code &ec)
{
  PipelineStreamEndHeader end_hdr{};
  end_hdr.magic = kPipelineStreamEnd;
  end_hdr.eq_slot = static_cast<uint32_t>(eq_slot);
  end_hdr.shard_count = static_cast<uint32_t>(shard_count);
  end_hdr.reserved = 0;
  if (bw != nullptr && bw->bandwidth_mbps() > 0.0)
  {
    tcp_write_with_shared_bandwidth(socket, reinterpret_cast<const char *>(&end_hdr), sizeof(end_hdr), bw, ec);
    return !ec;
  }
  return write_all(socket, reinterpret_cast<const char *>(&end_hdr), sizeof(end_hdr), ec);
}

PipelineFrameType recv_pipeline_frame(asio::ip::tcp::socket &socket, PipelineShardHeader &hdr,
                                      PipelineStreamEndHeader &end_hdr, std::vector<unsigned char> &payload,
                                      SharedBandwidthLimiter *bw, asio::error_code &ec)
{
  uint32_t magic = 0;
  if (!read_all(socket, reinterpret_cast<char *>(&magic), sizeof(magic), ec))
    return PipelineFrameType::ERROR;
  if (magic == kPipelineStreamEnd)
  {
    end_hdr.magic = magic;
    if (!read_all(socket, reinterpret_cast<char *>(&end_hdr.eq_slot),
                  sizeof(PipelineStreamEndHeader) - sizeof(uint32_t), ec))
      return PipelineFrameType::ERROR;
    return PipelineFrameType::END;
  }
  if (magic != kPipelineMagic)
  {
    ec = asio::error::invalid_argument;
    return PipelineFrameType::ERROR;
  }
  hdr.magic = magic;
  if (!read_all(socket, reinterpret_cast<char *>(&hdr) + sizeof(uint32_t), sizeof(PipelineShardHeader) - sizeof(uint32_t),
                ec))
    return PipelineFrameType::ERROR;
  if (hdr.stripe_len == 0)
  {
    ec = asio::error::invalid_argument;
    return PipelineFrameType::ERROR;
  }
  payload.resize(hdr.stripe_len);
  if (bw != nullptr && bw->bandwidth_mbps() > 0.0)
  {
    // Fan-in RX (hub/local-sink): drain first, then account.  Primary-before-read
    // stops TCP drain and deadlocks multi-chain hub fan-in under node RX sharing.
    tcp_read_with_shared_bandwidth(socket, reinterpret_cast<char *>(payload.data()), hdr.stripe_len, bw, ec,
                                   SharedBandwidthPace::DrainThenAccount);
    return ec ? PipelineFrameType::ERROR : PipelineFrameType::SHARD;
  }
  if (!read_all(socket, reinterpret_cast<char *>(payload.data()), hdr.stripe_len, ec))
    return PipelineFrameType::ERROR;
  return PipelineFrameType::SHARD;
}

void close_pipeline_socket(asio::ip::tcp::socket &socket)
{
  asio::error_code ec;
  socket.shutdown(asio::ip::tcp::socket::shutdown_both, ec);
  socket.close(ec);
}

void close_pipeline_acceptor(asio::ip::tcp::acceptor &acceptor)
{
  asio::error_code ignore_ec;
#if defined(__linux__) || defined(__APPLE__)
  if (acceptor.is_open())
  {
    const asio::ip::tcp::endpoint bound_ep = acceptor.local_endpoint(ignore_ec);
    if (!ignore_ec)
    {
      std::lock_guard<std::mutex> lock(g_pipeline_bind_mutex);
      g_pipeline_active_listen_ports.erase(bound_ep.port());
    }
    struct linger lin;
    lin.l_onoff = 1;
    lin.l_linger = 0;
    ::setsockopt(acceptor.native_handle(), SOL_SOCKET, SO_LINGER, &lin, sizeof(lin));
  }
#endif
  acceptor.cancel(ignore_ec);
  acceptor.close(ignore_ec);
}

bool open_pipeline_acceptor(asio::io_context &io, int listen_port, asio::ip::tcp::acceptor &acceptor,
                            asio::error_code &ec)
{
  auto close_acceptor = [&acceptor]() {
    asio::error_code ignore_ec;
    acceptor.close(ignore_ec);
  };

  ec.clear();
  // Keep retries short: a stuck in-process listener should be closed by TEARDOWN;
  // long sleeps here make trials look "hung" after Address already in use.
  const int max_retries = 40; // ~2s
  const asio::ip::tcp::endpoint ep(asio::ip::tcp::v4(), listen_port);

  for (int retry = 0; retry < max_retries; ++retry)
  {
    ec.clear();
    acceptor.open(ep.protocol(), ec);
    if (ec)
    {
      close_acceptor();
      return false;
    }
    acceptor.set_option(asio::socket_base::reuse_address(true), ec);
    if (ec)
    {
      close_acceptor();
      return false;
    }
    {
      std::lock_guard<std::mutex> lock(g_pipeline_bind_mutex);
      // Drop stale in-process reservation if the acceptor was lost without close.
      if (retry == 0)
        g_pipeline_active_listen_ports.erase(listen_port);
      acceptor.bind(ep, ec);
    }
    if (!ec)
    {
      acceptor.listen(asio::socket_base::max_listen_connections, ec);
      if (!ec)
      {
        std::lock_guard<std::mutex> lock(g_pipeline_bind_mutex);
        g_pipeline_active_listen_ports.insert(listen_port);
        return true;
      }
    }
    close_acceptor();
    if (ec != asio::error::address_in_use)
      return false;
    {
      std::lock_guard<std::mutex> lock(g_pipeline_bind_mutex);
      if (g_pipeline_active_listen_ports.count(listen_port) > 0)
      {
        char tb[128];
        snprintf(tb, sizeof(tb), "pipeline bind wait port=%d still active in-process retry=%d", listen_port, retry);
        pipeline_trace(tb);
      }
    }
    std::this_thread::sleep_for(std::chrono::milliseconds(50));
  }
  ec = asio::error::address_in_use;
  return false;
}

bool connect_pipeline_socket(asio::ip::tcp::socket &socket, const std::string &next_ip, int next_port,
                             asio::error_code &ec, int max_retries = 150)
{
  // ~3s of connection_refused retries.  Was 1500 (~30s) which made mis-routed
  // head→hub connects look like a full hang on f>=2 trials.
  const auto endpoint = asio::ip::tcp::endpoint(asio::ip::address::from_string(next_ip), next_port);
  for (int attempt = 0; attempt < max_retries; attempt++)
  {
    ec.clear();
    socket.connect(endpoint, ec);
    if (!ec)
      return true;
    if (ec != asio::error::connection_refused)
      break;
    std::this_thread::sleep_for(std::chrono::milliseconds(20));
  }
  set_pipeline_error("pipeline connect failed " + next_ip + ":" + std::to_string(next_port) + " " + ec.message());
  return false;
}

std::string tcp_remote_ip(asio::ip::tcp::socket &socket)
{
  asio::error_code ec;
  const asio::ip::tcp::endpoint ep = socket.remote_endpoint(ec);
  if (ec)
    return {};
  return ep.address().to_string();
}

bool accept_pipeline_socket(asio::ip::tcp::acceptor &acceptor, asio::ip::tcp::socket &socket, asio::error_code &ec,
                            int timeout_sec = 45, const std::atomic<bool> *cancel = nullptr)
{
  const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(timeout_sec);
  acceptor.non_blocking(true);
  while (std::chrono::steady_clock::now() < deadline)
  {
    if (cancel != nullptr && cancel->load())
    {
      ec = asio::error::operation_aborted;
      return false;
    }
    ec.clear();
    acceptor.accept(socket, ec);
    if (!ec)
      return true;
    if (ec != asio::error::would_block)
      return false;
    std::this_thread::sleep_for(std::chrono::milliseconds(20));
  }
  ec = asio::error::timed_out;
  return false;
}

bool pipeline_chain_is_local_direct(const proxy_proto::RecoveryRequest *recovery_request)
{
  return !recovery_request->pipeline_local_failed_block_key().empty();
}

bool pipeline_hop_connects_to_hub(const proxy_proto::RecoveryRequest *recovery_request, int my_idx, int hops_n)
{
  const bool hub_is_tail = recovery_request->pipeline_chain_hub_is_tail_flag() != 0;
  // Local chain: last survivor hop forwards to sink R (stored as hub_proxy_*).
  if (pipeline_chain_is_local_direct(recovery_request))
    return my_idx == hops_n - 1;
  if (hub_is_tail)
    return my_idx == hops_n - 2;
  return my_idx == hops_n - 1;
}

int hop_count(const proxy_proto::RecoveryRequest *req)
{
  return req->pipeline_hop_block_ids_size();
}

bool read_local_stripe(ProxyImpl *self, const std::string &key, const std::string &ip, int port, int block_size,
                       int off, int len, unsigned char *out, double *disk_start, double *disk_end, double *net_start,
                       double *net_end)
{
  char *buf = static_cast<char *>(std::aligned_alloc(32, block_size));
  if (buf == nullptr)
    return false;
  std::memset(buf, 0, block_size);
  double ds = 0.0, de = 0.0, ns = 0.0, ne = 0.0;
  const bool ok = self->GetFromDatanodeStripeRangeBreakdown(key, buf, block_size, off, len, ip.c_str(), port, &ds,
                                                            &de, &ns, &ne, nullptr, nullptr, nullptr);
  if (ok)
    std::memcpy(out, buf + off, len);
  free(buf);
  if (disk_start)
    *disk_start = ds;
  if (disk_end)
    *disk_end = de;
  if (net_start)
    *net_start = ns;
  if (net_end)
    *net_end = ne;
  return ok;
}

struct DatanodeReadStream
{
  asio::io_context io;
  asio::ip::tcp::socket socket{io};
  bool ok = false;
  std::string ip;
  int port = 0;
};

bool open_datanode_read_stream(ProxyImpl *self, const std::string &key, const std::string &ip, int port,
                               int block_size, DatanodeReadStream &out, int read_offset = 0,
                               int read_length = 0)
{
  const int saved_offset = g_pipeline_dn_read_offset;
  const int saved_length = g_pipeline_dn_read_length;
  g_pipeline_dn_read_offset = read_offset;
  g_pipeline_dn_read_length = read_length;
  const bool opened = self->openDatanodeGetStream(key, ip, port, block_size, out.io, out.socket);
  g_pipeline_dn_read_offset = saved_offset;
  g_pipeline_dn_read_length = saved_length;
  if (!opened)
    return false;
  out.ip = ip;
  out.port = port;
  out.ok = true;
  return true;
}

bool read_datanode_stream_block(ProxyImpl *self, DatanodeReadStream &stream, unsigned char *dst, size_t len)
{
  if (!stream.ok)
    return false;
  asio::error_code read_ec;
  // DN TCP stream is paced on datanode egress when remote; never on proxy ingress.
  tcp_read_with_shared_bandwidth(stream.socket, reinterpret_cast<char *>(dst), len, nullptr, read_ec);
  if (read_ec)
  {
    std::cerr << "[gLRC Pipeline] datanode stream read failed @" << stream.ip << ":" << stream.port
              << " len=" << len << " ec=" << read_ec.message() << std::endl;
  }
  (void)self;
  return !read_ec;
}

bool read_datanode_stream_shard(ProxyImpl *self, DatanodeReadStream &stream, unsigned char *dst, size_t len)
{
  return read_datanode_stream_block(self, stream, dst, len);
}

void clear_glrc_pipeline_session_state()
{
  reset_pipeline_ready_session();
  {
    std::lock_guard<std::mutex> lock(g_pipeline_bind_mutex);
    g_pipeline_active_listen_ports.clear();
  }
  g_glrc_pipeline_last_error.clear();
}

} // namespace

std::string glrc_pipeline_take_last_error()
{
  std::string out = g_glrc_pipeline_last_error;
  g_glrc_pipeline_last_error.clear();
  return out;
}

bool ProxyImpl::openDatanodeGetStream(const std::string &block_key, const std::string &ip, int port, int block_size,
                                      asio::io_context &io, asio::ip::tcp::socket &socket)
{
  grpc::ClientContext context;
  datanode_proto::GetInfo get_info;
  datanode_proto::RequestResult result;
  get_info.set_block_key(block_key);
  get_info.set_block_size(block_size);
  get_info.set_read_offset(g_pipeline_dn_read_offset);
  get_info.set_read_length(g_pipeline_dn_read_length > 0 ? g_pipeline_dn_read_length : block_size);
  get_info.set_proxy_ip(m_ip);
  get_info.set_proxy_port(m_port);
  const std::string node_ip_port = ip + ":" + std::to_string(port);
  auto dn_it = m_datanode_ptrs.find(node_ip_port);
  if (dn_it == m_datanode_ptrs.end() || !dn_it->second)
    return false;
  context.set_deadline(std::chrono::system_clock::now() + std::chrono::seconds(120));
  grpc::Status stat = dn_it->second->handleGetBreakdown(&context, get_info, &result);
  if (!stat.ok())
    return false;
  asio::ip::tcp::resolver resolver(io);
  asio::error_code dn_ec;
  asio::connect(socket, resolver.resolve({ip, std::to_string(result.data_port())}), dn_ec);
  if (dn_ec)
    return false;
  set_pipeline_socket_timeouts(socket, kPipelineSocketTimeoutSec);
  return true;
}

bool ProxyImpl::glrcIlpPipelineRecovery(const proxy_proto::RecoveryRequest *recovery_request,
                                        proxy_proto::RecoveryReply *response)
{
  const int role = recovery_request->pipeline_role();
  if (role == static_cast<int>(GlrcPipelineRole::HUB))
    return glrcIlpPipelineHubRecovery(recovery_request, response);
  if (role == static_cast<int>(GlrcPipelineRole::CHAIN_HEAD))
    return glrcIlpPipelineChainHeadRecovery(recovery_request, response);
  if (role == static_cast<int>(GlrcPipelineRole::HOP_SERVER))
    return glrcIlpPipelineHopServerRecovery(recovery_request, response);
  if (role == static_cast<int>(GlrcPipelineRole::LOCAL_DIRECT))
    return glrcIlpPipelineLocalDirectRecovery(recovery_request, response);
  if (role == static_cast<int>(GlrcPipelineRole::TEARDOWN))
    return glrcIlpPipelineTeardownRecovery(recovery_request, response);
  if (role == static_cast<int>(GlrcPipelineRole::READY))
    return glrcIlpPipelineReadyRecovery(recovery_request, response);
  set_pipeline_error("invalid pipeline_role");
  return false;
}

bool ProxyImpl::glrcIlpPipelineTeardownRecovery(const proxy_proto::RecoveryRequest *recovery_request,
                                                proxy_proto::RecoveryReply *response)
{
  (void)recovery_request;
  clear_glrc_pipeline_session_state();
  // Drop accumulated NIC timelines so the next trial cannot inherit a next_slot_
  // left far in the future by a cancelled/hung recovery.
  resetPeerLinkBandwidth();
  response->Clear();
  pipeline_trace("pipeline session teardown");
  return true;
}

bool ProxyImpl::glrcIlpPipelineReadyRecovery(const proxy_proto::RecoveryRequest *recovery_request,
                                             proxy_proto::RecoveryReply *response)
{
  response->Clear();
  const int epoch = recovery_request->pipeline_exchange_epoch();
  // Coordinator always sets pipeline_role=READY; distinguish hub/sink vs hop by metadata.
  const bool bind_hub_or_sink = recovery_request->pipeline_hub_chain_eq_slots_size() > 0;
  const bool bind_hop =
      !bind_hub_or_sink && recovery_request->pipeline_my_hop_index() >= 1 &&
      recovery_request->pipeline_hop_block_ids_size() > recovery_request->pipeline_my_hop_index();

  if (bind_hub_or_sink)
  {
    const int hub_chain_n = recovery_request->pipeline_hub_chain_eq_slots_size();
    std::vector<PipelineHubReadyAcceptor> bound;
    bound.reserve(static_cast<size_t>(hub_chain_n));
    for (int ci = 0; ci < hub_chain_n; ci++)
    {
      if (recovery_request->pipeline_hub_chain_local_only_flags_size() > ci &&
          recovery_request->pipeline_hub_chain_local_only_flags(ci) != 0)
        continue;
      const int eq_slot = recovery_request->pipeline_hub_chain_eq_slots(ci);
      const int listen_port = resolve_hub_listener_port(recovery_request, ci, m_port, epoch, eq_slot);
      PipelineHubReadyAcceptor pb;
      pb.ci = ci;
      pb.listen_port = listen_port;
      pb.io = std::make_shared<asio::io_context>();
      pb.acceptor = std::make_shared<asio::ip::tcp::acceptor>(*pb.io);
      asio::error_code bind_ec;
      if (!open_pipeline_acceptor(*pb.io, listen_port, *pb.acceptor, bind_ec))
      {
        for (auto &prev : bound)
          if (prev.acceptor)
            close_pipeline_acceptor(*prev.acceptor);
        set_pipeline_error("pipeline ready hub/sink bind failed port=" + std::to_string(listen_port) + " " +
                           bind_ec.message());
        return false;
      }
      bound.push_back(std::move(pb));
    }
    {
      std::lock_guard<std::mutex> lock(g_pipeline_ready_mutex);
      if (g_pipeline_ready_session.exchange_epoch != epoch)
      {
        for (auto &prev : g_pipeline_ready_session.hub_acceptors)
          if (prev.acceptor)
            close_pipeline_acceptor(*prev.acceptor);
        for (auto &kv : g_pipeline_ready_session.hop_acceptors)
          if (kv.second.acceptor)
            close_pipeline_acceptor(*kv.second.acceptor);
        g_pipeline_ready_session = PipelineReadySession{};
        g_pipeline_ready_session.exchange_epoch = epoch;
      }
      for (auto &pb : bound)
        g_pipeline_ready_session.hub_acceptors.push_back(std::move(pb));
    }
    pipeline_trace("pipeline ready hub/sink listeners bound");
    return true;
  }

  if (bind_hop)
  {
    const int chain_id = recovery_request->pipeline_chain_id();
    const int my_idx = recovery_request->pipeline_my_hop_index();
    const int listen_port = resolve_my_hop_bind_port(recovery_request, m_port, epoch, chain_id, my_idx);
    PipelineHopReadyAcceptor hop;
    hop.listen_port = listen_port;
    hop.io = std::make_shared<asio::io_context>();
    hop.acceptor = std::make_shared<asio::ip::tcp::acceptor>(*hop.io);
    asio::error_code bind_ec;
    if (!open_pipeline_acceptor(*hop.io, listen_port, *hop.acceptor, bind_ec))
    {
      set_pipeline_error("pipeline ready hop bind failed proxy=" + m_ip + ":" + std::to_string(m_port) +
                         " epoch=" + std::to_string(epoch) + " chain=" + std::to_string(chain_id) +
                         " hop=" + std::to_string(my_idx) + " port=" + std::to_string(listen_port) + " " +
                         bind_ec.message());
      return false;
    }
    const std::string key = hop_ready_key(epoch, chain_id, my_idx);
    {
      std::lock_guard<std::mutex> lock(g_pipeline_ready_mutex);
      if (g_pipeline_ready_session.exchange_epoch != epoch)
      {
        for (auto &prev : g_pipeline_ready_session.hub_acceptors)
          if (prev.acceptor)
            close_pipeline_acceptor(*prev.acceptor);
        for (auto &kv : g_pipeline_ready_session.hop_acceptors)
          if (kv.second.acceptor)
            close_pipeline_acceptor(*kv.second.acceptor);
        g_pipeline_ready_session = PipelineReadySession{};
        g_pipeline_ready_session.exchange_epoch = epoch;
      }
      g_pipeline_ready_session.hop_acceptors[key] = std::move(hop);
    }
    pipeline_trace("pipeline ready hop listener bound");
    return true;
  }

  return true;
}

bool ProxyImpl::glrcIlpPipelineChainHeadRecovery(const proxy_proto::RecoveryRequest *recovery_request,
                                                 proxy_proto::RecoveryReply *response)
{
  try
  {
  const int hops_n = hop_count(recovery_request);
  if (hops_n <= 0)
  {
    set_pipeline_error("pipeline chain head missing hops");
    return false;
  }

  const int block_size = m_sys_config->BlockSize;
  const PipelineShardView shards = resolve_pipeline_shard_view(recovery_request, block_size, m_sys_config);
  if (!shards.valid_geometry(block_size))
  {
    set_pipeline_error("invalid pipeline shard geometry");
    return false;
  }
  const int shard_count = shards.local_count;
  const int stripe_len = shards.stripe_len;
  const int chain_id = recovery_request->pipeline_chain_id();
  const int eq_slot = recovery_request->pipeline_eq_slot();
  const int epoch = recovery_request->pipeline_exchange_epoch();
  const bool hub_is_tail = recovery_request->pipeline_chain_hub_is_tail_flag() != 0;

  const unsigned char head_coef =
      recovery_request->pipeline_hop_coefs_size() > 0
          ? static_cast<unsigned char>(recovery_request->pipeline_hop_coefs(0) & 0xff)
          : 1;
  const GlrcPipelineEqCodec eq_codec = pipeline_codec_from_request(recovery_request);

  double min_disk = 0.0, max_disk = 0.0, min_net = 0.0, max_net = 0.0;
  auto update_min = [](double &slot, double v) {
    if (v > 0.0 && (slot == 0.0 || v < slot))
      slot = v;
  };
  auto update_max = [](double &slot, double v) {
    if (v > slot)
      slot = v;
  };

  std::cout << "[Proxy" << m_self_cluster_id << "][gLRC Pipeline] chain_head stream chain=" << chain_id
            << " eq_slot=" << eq_slot
            << (eq_codec == GlrcPipelineEqCodec::LOCAL_XOR ? " xor" : " cauchy")
            << " hops=" << hops_n << " shards=" << shard_count
            << " window=" << resolve_pipeline_window(m_sys_config, shard_count) << std::endl;

  // With hub_is_tail the plan appends hub as the last hop.  Head must send into
  // the hub listener when there is no intermediate hop-server:
  //   hops_n==1: [hub]           (degenerate)
  //   hops_n==2: [survivor, hub] → head connects to hub listen port (NOT hop port)
  //   hops_n>2:  head → hop[1] …
  // Old code only treated hops_n==1 as hub, so hops_n==2 connected to a hop
  // listen port nobody accepts → ~30s connect retry / apparent hang (common on
  // short global chains in f>=2 plans).
  const bool next_is_hub = hub_is_tail && hops_n <= 2;
  const bool next_is_local_sink = !hub_is_tail && hops_n == 1 && pipeline_chain_is_local_direct(recovery_request);
  const bool sends_to_fanin = next_is_hub || next_is_local_sink;
  const std::string next_ip =
      sends_to_fanin ? recovery_request->pipeline_hub_proxy_ip()
                     : recovery_request->pipeline_hop_proxy_ips(1);
  const int listen_port =
      sends_to_fanin
          ? resolve_chain_hub_listen_port(recovery_request, recovery_request->pipeline_hub_proxy_port(), epoch, eq_slot)
          : resolve_hop_listen_port(recovery_request, 1, recovery_request->pipeline_hop_proxy_ports(1), epoch,
                                    chain_id);

  const std::string head_key = recovery_request->pipeline_hop_block_keys(0);
  const std::string head_ip = recovery_request->pipeline_hop_datanode_ips(0);
  const int head_port = recovery_request->pipeline_hop_datanode_ports(0);
  // Hop→hop: node egress. Into hub/sink: omit sender egress; fan-in RX paces with
  // DrainThenAccount (avoids dual-end charge and Primary-before-read stalls).
  SharedBandwidthLimiter *egress_bw =
      sends_to_fanin ? nullptr : egressBandwidthForPeer(next_ip);

  asio::io_context io;
  asio::error_code ec;
  asio::ip::tcp::socket out_socket(io);
  if (!connect_pipeline_socket(out_socket, next_ip, listen_port, ec))
    return false;
  set_pipeline_socket_timeouts(out_socket, kPipelineSocketTimeoutSec);
  std::cout << "[Proxy" << m_self_cluster_id << "][gLRC Pipeline] chain_head connected chain=" << chain_id
            << " -> " << next_ip << ":" << listen_port << " streaming=" << shard_count << std::endl;

  const int pipe_window = resolve_pipeline_window(m_sys_config, shard_count);
  DatanodeReadStream head_read_stream;
  if (!open_datanode_read_stream(this, head_key, head_ip, head_port, block_size, head_read_stream,
                                 shards.byte_off(0), shard_count * stripe_len))
  {
    close_pipeline_socket(out_socket);
    set_pipeline_error("pipeline chain head stream setup failed key=" + head_key);
    return false;
  }
  auto run_chain_head_shard = [&](int shard, unsigned char *dst) -> bool {
    const int off = shards.byte_off(shard);

    {
      char tb[320];
      snprintf(tb, sizeof(tb), "chain_head read shard=%d key=%s @ %s:%d off=%d len=%d", shard, head_key.c_str(),
               head_ip.c_str(), head_port, off, stripe_len);
      pipeline_trace(tb);
    }
    if (!read_datanode_stream_shard(this, head_read_stream, dst, static_cast<size_t>(stripe_len)))
    {
      const std::string msg = "pipeline chain head local read failed key=" + head_key + " @" + head_ip + ":" +
                              std::to_string(head_port) + " shard=" + std::to_string(shard);
      set_pipeline_error(msg);
      std::cerr << "[Proxy" << m_self_cluster_id << "][gLRC Pipeline] " << msg << std::endl;
      return false;
    }
    return true;
  };

  if (pipe_window <= 1)
  {
    for (int shard = 0; shard < shard_count; shard++)
    {
      std::vector<unsigned char> raw(static_cast<size_t>(stripe_len), 0);
      if (!run_chain_head_shard(shard, raw.data()))
      {
        close_pipeline_socket(out_socket);
        return false;
      }
      std::vector<unsigned char> encoded(static_cast<size_t>(stripe_len), 0);
      glrc_pipeline_init_partial_range(encoded.data(), raw.data(), head_coef, stripe_len, eq_codec);
      if (!send_pipeline_shard(out_socket, eq_slot, shard, stripe_len, encoded.data(), egress_bw, ec))
      {
        close_pipeline_socket(out_socket);
        set_pipeline_error("pipeline chain head send failed: " + ec.message());
        return false;
      }
    }
  }
  else
  {
    PipelineBoundedQueue send_q(static_cast<size_t>(pipe_window));
    std::atomic<bool> pipeline_failed{false};
    std::mutex send_err_mu;
    std::string send_err;
    int shards_sent = 0;

    std::thread sender([&]() {
      asio::error_code sender_ec;
      PipelineQueuedShard item;
      while (send_q.pop(item))
      {
        if (item.end_of_stream)
          break;
        if (!send_pipeline_shard(out_socket, eq_slot, item.shard_id, stripe_len, item.payload.data(), egress_bw,
                                 sender_ec))
        {
          std::lock_guard<std::mutex> lock(send_err_mu);
          send_err = "pipeline chain head send failed shard=" + std::to_string(item.shard_id) + " " +
                     sender_ec.message();
          pipeline_failed.store(true);
          send_q.close();
          return;
        }
        shards_sent++;
      }
      if (!pipeline_failed.load())
      {
        if (!send_pipeline_stream_end(out_socket, eq_slot, shard_count, egress_bw, sender_ec))
        {
          std::lock_guard<std::mutex> lock(send_err_mu);
          send_err = "pipeline chain head stream end failed: " + sender_ec.message();
          pipeline_failed.store(true);
        }
      }
    });

    for (int shard = 0; shard < shard_count && !pipeline_failed.load(); shard++)
    {
      std::vector<unsigned char> raw(static_cast<size_t>(stripe_len), 0);
      if (!run_chain_head_shard(shard, raw.data()))
      {
        pipeline_failed.store(true);
        break;
      }
      PipelineQueuedShard item;
      item.shard_id = shard;
      item.payload.assign(static_cast<size_t>(stripe_len), 0);
      glrc_pipeline_init_partial_range(item.payload.data(), raw.data(), head_coef, stripe_len, eq_codec);
      send_q.push(std::move(item));
    }
    PipelineQueuedShard end_marker;
    end_marker.end_of_stream = true;
    send_q.push(std::move(end_marker));
    send_q.close();
    if (sender.joinable())
      sender.join();

    if (pipeline_failed.load() || shards_sent != shard_count)
    {
      close_pipeline_socket(out_socket);
      // Prefer the concrete send/read error.  Do not overwrite a prior
      // set_pipeline_error from run_chain_head_shard with a generic mismatch.
      if (!send_err.empty())
        set_pipeline_error(send_err);
      else if (!pipeline_failed.load() && shards_sent != shard_count)
        set_pipeline_error("pipeline chain head shard send count mismatch sent=" + std::to_string(shards_sent) + "/" +
                           std::to_string(shard_count));
      return false;
    }
    close_pipeline_socket(out_socket);
    close_pipeline_socket(head_read_stream.socket);
    response->set_disk_io_start_time(min_disk);
    response->set_disk_io_end_time(max_disk);
    response->set_network_start_time(min_net);
    response->set_network_end_time(max_net);
    std::cout << "[Proxy" << m_self_cluster_id << "][gLRC Pipeline] chain_head done chain=" << chain_id
              << " eq_slot=" << eq_slot << " hub_tail=" << hub_is_tail << " shards=" << shard_count
              << " window=" << pipe_window << std::endl;
    return true;
  }

  if (!send_pipeline_stream_end(out_socket, eq_slot, shard_count, egress_bw, ec))
  {
    close_pipeline_socket(out_socket);
    set_pipeline_error("pipeline chain head stream end failed: " + ec.message());
    return false;
  }
  close_pipeline_socket(out_socket);
  close_pipeline_socket(head_read_stream.socket);

  response->set_disk_io_start_time(min_disk);
  response->set_disk_io_end_time(max_disk);
  response->set_network_start_time(min_net);
  response->set_network_end_time(max_net);
  std::cout << "[Proxy" << m_self_cluster_id << "][gLRC Pipeline] chain_head done chain=" << chain_id
            << " eq_slot=" << eq_slot << " hub_tail=" << hub_is_tail << " shards=" << shard_count << std::endl;
  return true;
  }
  catch (const std::exception &e)
  {
    set_pipeline_error(std::string("pipeline chain_head exception: ") + e.what());
    std::cerr << "[Proxy" << m_self_cluster_id << "][gLRC Pipeline] chain_head exception: " << e.what() << std::endl;
    return false;
  }
}

bool ProxyImpl::glrcIlpPipelineHopServerRecovery(const proxy_proto::RecoveryRequest *recovery_request,
                                                 proxy_proto::RecoveryReply *response)
{
  try
  {
  const int hops_n = hop_count(recovery_request);
  const int my_idx = recovery_request->pipeline_my_hop_index();
  if (hops_n <= 1 || my_idx <= 0 || my_idx >= hops_n)
  {
    set_pipeline_error("invalid pipeline hop_server index");
    return false;
  }

  const int block_size = m_sys_config->BlockSize;
  const PipelineShardView shards = resolve_pipeline_shard_view(recovery_request, block_size, m_sys_config);
  if (!shards.valid_geometry(block_size))
  {
    set_pipeline_error("invalid pipeline hop_server geometry");
    return false;
  }
  const int shard_count = shards.local_count;
  const int stripe_len = shards.stripe_len;
  const int chain_id = recovery_request->pipeline_chain_id();
  const int eq_slot = recovery_request->pipeline_eq_slot();
  const int epoch = recovery_request->pipeline_exchange_epoch();
  const unsigned char my_coef =
      recovery_request->pipeline_hop_coefs_size() > my_idx
          ? static_cast<unsigned char>(recovery_request->pipeline_hop_coefs(my_idx) & 0xff)
          : 1;
  const GlrcPipelineEqCodec eq_codec = pipeline_codec_from_request(recovery_request);

  const int listen_port = resolve_my_hop_bind_port(recovery_request, m_port, epoch, chain_id, my_idx);
  {
    char tb[160];
    snprintf(tb, sizeof(tb), "hop bind attempt chain=%d hop=%d port=%d my_listen=%d", chain_id, my_idx, listen_port,
             recovery_request->pipeline_my_listen_port());
    pipeline_trace(tb);
  }
  std::cout << "[Proxy" << m_self_cluster_id << "][gLRC Pipeline] hop_server listen chain=" << chain_id
            << " hop=" << my_idx
            << (eq_codec == GlrcPipelineEqCodec::LOCAL_XOR ? " xor" : " cauchy")
            << " port=" << listen_port << std::endl;
  std::shared_ptr<asio::io_context> hop_io;
  std::shared_ptr<asio::ip::tcp::acceptor> hop_acceptor;
  asio::error_code ec;
  if (!take_hop_ready_acceptor(epoch, chain_id, my_idx, listen_port, hop_io, hop_acceptor))
  {
    hop_io = std::make_shared<asio::io_context>();
    hop_acceptor = std::make_shared<asio::ip::tcp::acceptor>(*hop_io);
    if (!open_pipeline_acceptor(*hop_io, listen_port, *hop_acceptor, ec))
    {
      set_pipeline_error("pipeline hop bind failed port=" + std::to_string(listen_port) + " " + ec.message());
      return false;
    }
  }
  struct PipelineAcceptorGuard
  {
    asio::ip::tcp::acceptor *acceptor = nullptr;
    ~PipelineAcceptorGuard()
    {
      if (acceptor)
        close_pipeline_acceptor(*acceptor);
    }
  } acceptor_guard{hop_acceptor.get()};

  double min_disk = 0.0, max_disk = 0.0, min_net = 0.0, max_net = 0.0;
  auto update_min = [](double &slot, double v) {
    if (v > 0.0 && (slot == 0.0 || v < slot))
      slot = v;
  };
  auto update_max = [](double &slot, double v) {
    if (v > slot)
      slot = v;
  };

  const bool connects_to_hub = pipeline_hop_connects_to_hub(recovery_request, my_idx, hops_n);
  std::string downstream_ip;

  asio::ip::tcp::socket out_socket(*hop_io);
  if (connects_to_hub)
  {
    downstream_ip = recovery_request->pipeline_hub_proxy_ip();
    const int hub_port =
        resolve_chain_hub_listen_port(recovery_request, recovery_request->pipeline_hub_proxy_port(), epoch, eq_slot);
    if (!connect_pipeline_socket(out_socket, downstream_ip, hub_port, ec))
      return false;
    set_pipeline_socket_timeouts(out_socket, kPipelineSocketTimeoutSec);
  }
  else
  {
    downstream_ip = recovery_request->pipeline_hop_proxy_ips(my_idx + 1);
    const int next_grpc_port = recovery_request->pipeline_hop_proxy_ports(my_idx + 1);
    const int next_port = resolve_hop_listen_port(recovery_request, my_idx + 1, next_grpc_port, epoch, chain_id);
    if (!connect_pipeline_socket(out_socket, downstream_ip, next_port, ec))
      return false;
    set_pipeline_socket_timeouts(out_socket, kPipelineSocketTimeoutSec);
  }
  std::cout << "[Proxy" << m_self_cluster_id << "][gLRC Pipeline] hop_server downstream ready chain=" << chain_id
            << " hop=" << my_idx << std::endl;

  asio::ip::tcp::socket in_socket(*hop_io);
  if (!accept_pipeline_socket(*hop_acceptor, in_socket, ec, 45))
  {
    close_pipeline_socket(out_socket);
    set_pipeline_error("pipeline hop accept failed: " + ec.message());
    return false;
  }
  set_pipeline_socket_timeouts(in_socket, kPipelineSocketTimeoutSec);
  set_pipeline_socket_timeouts(out_socket, kPipelineSocketTimeoutSec);
  std::cout << "[Proxy" << m_self_cluster_id << "][gLRC Pipeline] hop_server accepted upstream chain=" << chain_id
            << " hop=" << my_idx << " window=" << resolve_pipeline_window(m_sys_config, shard_count) << std::endl;

  const std::string hop_key = recovery_request->pipeline_hop_block_keys(my_idx);
  const std::string hop_ip = recovery_request->pipeline_hop_datanode_ips(my_idx);
  const int hop_port = recovery_request->pipeline_hop_datanode_ports(my_idx);
  // Hop→hop: node egress. Last hop into hub/sink: omit egress; hub/sink RX uses
  // DrainThenAccount. Intermediate hop RX is not software-paced.
  SharedBandwidthLimiter *egress_bw =
      connects_to_hub ? nullptr : egressBandwidthForPeer(downstream_ip);
  SharedBandwidthLimiter *ingress_bw = nullptr;

  const int pipe_window = resolve_pipeline_window(m_sys_config, shard_count);
  int shards_forwarded = 0;
  DatanodeReadStream hop_read_stream;
  if (!open_datanode_read_stream(this, hop_key, hop_ip, hop_port, block_size, hop_read_stream,
                                 shards.byte_off(0), shard_count * stripe_len))
  {
    close_pipeline_socket(in_socket);
    close_pipeline_socket(out_socket);
    close_pipeline_acceptor(*hop_acceptor);
    set_pipeline_error("pipeline hop local stream setup failed hop=" + std::to_string(my_idx));
    return false;
  }

  auto compute_inbound_shard = [&](int shard, std::vector<unsigned char> &partial) -> bool {
    if (shard < 0 || shard >= shard_count || (int)partial.size() != stripe_len)
    {
      set_pipeline_error("pipeline hop shard mismatch");
      return false;
    }
    std::vector<unsigned char> local(static_cast<size_t>(stripe_len), 0);
    if (!read_datanode_stream_shard(this, hop_read_stream, local.data(), static_cast<size_t>(stripe_len)))
    {
      set_pipeline_error("pipeline hop local read failed hop=" + std::to_string(my_idx) + " shard=" +
                         std::to_string(shard) + " dn=" + hop_ip + ":" + std::to_string(hop_port) +
                         " key=" + hop_key);
      return false;
    }
    glrc_pipeline_accumulate_range(partial.data(), local.data(), my_coef, stripe_len, eq_codec);
return true;
  };

  auto process_inbound_shard = [&](int shard, std::vector<unsigned char> &partial) -> bool {
    if (!compute_inbound_shard(shard, partial))
      return false;
    if (!send_pipeline_shard(out_socket, eq_slot, shard, stripe_len, partial.data(), egress_bw, ec))
    {
      set_pipeline_error("pipeline hop forward failed: " + ec.message());
      return false;
    }
    shards_forwarded++;
    return true;
  };

  if (pipe_window <= 1)
  {
    while (true)
    {
      PipelineShardHeader hdr{};
      PipelineStreamEndHeader end_hdr{};
      std::vector<unsigned char> partial;
      const PipelineFrameType frame = recv_pipeline_frame(in_socket, hdr, end_hdr, partial, ingress_bw, ec);
      if (frame == PipelineFrameType::ERROR)
      {
        close_pipeline_socket(in_socket);
        close_pipeline_socket(out_socket);
        set_pipeline_error("pipeline hop recv frame failed: " + ec.message());
        return false;
      }
      if (frame == PipelineFrameType::END)
      {
        if ((int)end_hdr.shard_count != shards_forwarded)
        {
          close_pipeline_socket(in_socket);
          close_pipeline_socket(out_socket);
          set_pipeline_error("pipeline hop stream shard count mismatch");
          return false;
        }
        if (!send_pipeline_stream_end(out_socket, eq_slot, shard_count, egress_bw, ec))
        {
          close_pipeline_socket(in_socket);
          close_pipeline_socket(out_socket);
          set_pipeline_error("pipeline hop stream end forward failed");
          return false;
        }
        break;
      }
      const int shard = static_cast<int>(hdr.shard_id);
      if (!process_inbound_shard(shard, partial))
      {
        close_pipeline_socket(in_socket);
        close_pipeline_socket(out_socket);
        return false;
      }
    }
  }
  else
  {
    PipelineBoundedQueue inbound_q(static_cast<size_t>(pipe_window));
    PipelineBoundedQueue outbound_q(static_cast<size_t>(pipe_window));
    std::atomic<bool> reader_failed{false};
    std::atomic<bool> pipeline_failed{false};
    std::mutex reader_err_mu;
    std::string reader_err;
    int upstream_end_count = -1;

    std::thread reader([&]() {
      asio::error_code reader_ec;
      while (!reader_failed.load())
      {
        PipelineShardHeader hdr{};
        PipelineStreamEndHeader end_hdr{};
        std::vector<unsigned char> partial;
        const PipelineFrameType frame =
            recv_pipeline_frame(in_socket, hdr, end_hdr, partial, ingress_bw, reader_ec);
        if (frame == PipelineFrameType::ERROR)
        {
          std::lock_guard<std::mutex> lock(reader_err_mu);
          reader_err = "pipeline hop recv frame failed: " + reader_ec.message();
          reader_failed.store(true);
          inbound_q.close();
          outbound_q.close();
          return;
        }
        if (frame == PipelineFrameType::END)
        {
          PipelineQueuedShard end_item;
          end_item.end_of_stream = true;
          end_item.end_shard_count = static_cast<int>(end_hdr.shard_count);
          inbound_q.push(std::move(end_item));
          inbound_q.close();
          return;
        }
        PipelineQueuedShard item;
        item.shard_id = static_cast<int>(hdr.shard_id);
        item.payload = std::move(partial);
        inbound_q.push(std::move(item));
      }
      inbound_q.close();
    });

    std::thread sender([&]() {
      asio::error_code sender_ec;
      PipelineQueuedShard send_item;
      while (outbound_q.pop(send_item))
      {
        if (send_item.end_of_stream)
          break;
        if (!send_pipeline_shard(out_socket, eq_slot, send_item.shard_id, stripe_len, send_item.payload.data(),
                                 egress_bw, sender_ec))
        {
          set_pipeline_error("pipeline hop forward failed: " + sender_ec.message());
          pipeline_failed.store(true);
          inbound_q.close();
          outbound_q.close();
          return;
        }
        shards_forwarded++;
      }
      if (!pipeline_failed.load())
      {
        if (!send_pipeline_stream_end(out_socket, eq_slot, shard_count, egress_bw, sender_ec))
        {
          set_pipeline_error("pipeline hop stream end forward failed");
          pipeline_failed.store(true);
        }
      }
    });

    PipelineQueuedShard item;
    while (inbound_q.pop(item))
    {
      if (item.end_of_stream)
      {
        upstream_end_count = item.end_shard_count;
        break;
      }
      if (!compute_inbound_shard(item.shard_id, item.payload))
      {
        pipeline_failed.store(true);
        reader_failed.store(true);
        break;
      }
      outbound_q.push(std::move(item));
    }
    PipelineQueuedShard outbound_end;
    outbound_end.end_of_stream = true;
    outbound_q.push(std::move(outbound_end));
    inbound_q.close();
    outbound_q.close();
    if (reader.joinable())
      reader.join();
    if (sender.joinable())
      sender.join();

    if (pipeline_failed.load() || reader_failed.load())
    {
      close_pipeline_socket(in_socket);
      close_pipeline_socket(out_socket);
      if (!reader_err.empty())
        set_pipeline_error(reader_err);
      return false;
    }
    if (upstream_end_count >= 0 && upstream_end_count != shards_forwarded)
    {
      close_pipeline_socket(in_socket);
      close_pipeline_socket(out_socket);
      set_pipeline_error("pipeline hop stream shard count mismatch");
      return false;
    }

  }
  close_pipeline_socket(in_socket);
  close_pipeline_socket(out_socket);
  close_pipeline_socket(hop_read_stream.socket);


  response->set_disk_io_start_time(min_disk);
  response->set_disk_io_end_time(max_disk);
  response->set_network_start_time(min_net);
  response->set_network_end_time(max_net);
  std::cout << "[Proxy" << m_self_cluster_id << "][gLRC Pipeline] hop_server done chain=" << chain_id
            << " hop=" << my_idx << " shards=" << shards_forwarded << std::endl;
  return true;
  }
  catch (const std::exception &e)
  {
    set_pipeline_error(std::string("pipeline hop_server exception: ") + e.what());
    return false;
  }
}

bool ProxyImpl::glrcIlpPipelineLocalDirectRecovery(const proxy_proto::RecoveryRequest *recovery_request,
                                                   proxy_proto::RecoveryReply *response)
{
  // Local single-fail sink R: receive the survivor-chain pipeline stream
  // (already XOR-accumulated along N1→…→Nk) and write the recovered block.
  try
  {
    const int hops_n = hop_count(recovery_request);
    if (hops_n <= 0)
    {
      set_pipeline_error("pipeline local sink missing survivor hops");
      return false;
    }

    const int block_size = m_sys_config->BlockSize;
    const PipelineShardView shards =
        resolve_pipeline_shard_view(recovery_request, block_size, m_sys_config);
    if (!shards.valid_geometry(block_size))
    {
      set_pipeline_error("invalid pipeline local sink shard geometry");
      return false;
    }
    const int shard_count = shards.local_count;
    const int stripe_len = shards.stripe_len;
    const int chain_id = recovery_request->pipeline_chain_id();
    const int eq_slot = recovery_request->pipeline_eq_slot();
    const int epoch = recovery_request->pipeline_exchange_epoch();
    const int failed_id = recovery_request->pipeline_local_failed_block_id();
    const char *failed_key = recovery_request->pipeline_local_failed_block_key().c_str();
    const char *rep_ip = recovery_request->pipeline_local_replaced_node_ip().c_str();
    const int rep_port = recovery_request->pipeline_local_replaced_node_port();
    if (failed_key == nullptr || failed_key[0] == '\0' || rep_ip == nullptr || rep_port <= 0)
    {
      set_pipeline_error("pipeline local sink missing writeback target");
      return false;
    }

    const int listen_port =
        resolve_chain_hub_listen_port(recovery_request, m_port, epoch, eq_slot);
    std::cout << "[Proxy" << m_self_cluster_id << "][gLRC Pipeline] local sink R chain=" << chain_id
              << " hops=" << hops_n << " listen=" << listen_port << " failed=" << failed_id << std::endl;
    pipeline_trace("local sink begin");

    asio::io_context accept_io;
    asio::ip::tcp::acceptor acceptor(accept_io);
    asio::error_code bind_ec;
    if (!open_pipeline_acceptor(accept_io, listen_port, acceptor, bind_ec))
    {
      set_pipeline_error("pipeline local sink bind failed port=" + std::to_string(listen_port) + " " +
                         bind_ec.message());
      return false;
    }

    // Open writeback stream while waiting for the chain (overlaps setup).
    datanode_proto::RequestResult recovery_result;
    asio::io_context write_io;
    asio::ip::tcp::socket write_socket(write_io);
    std::thread recovery_grpc_thread;
    grpc::ClientContext recovery_context;
    datanode_proto::MergeParityInfo recovery_info;
    recovery_info.set_block_key(std::string(failed_key));
    recovery_info.set_block_id(failed_id);
    const bool full_block_write =
        shards.global_begin == 0 && shards.local_count == shards.global_S;
    recovery_info.set_recovery_offset(full_block_write ? 0 : shards.byte_off(0));
    recovery_info.set_recovery_size(
        full_block_write ? 0 : shard_count * stripe_len);
    recovery_info.set_proxy_ip(m_ip);
    recovery_info.set_proxy_port(m_port);
    const std::string node_ip_port = std::string(rep_ip) + ":" + std::to_string(rep_port);
    auto dn_it = m_datanode_ptrs.find(node_ip_port);
    if (dn_it == m_datanode_ptrs.end() || !dn_it->second)
    {
      close_pipeline_acceptor(acceptor);
      set_pipeline_error("pipeline local sink write datanode stub missing");
      return false;
    }
    recovery_context.set_deadline(std::chrono::system_clock::now() + std::chrono::seconds(120));
    datanode_proto::datanodeService::Stub *dn_stub = dn_it->second.get();
    recovery_grpc_thread = std::thread([dn_stub, &recovery_context, &recovery_info, &recovery_result]() {
      (void)dn_stub->handleRecoveryBreakdown(&recovery_context, recovery_info, &recovery_result);
    });
    {
      asio::ip::tcp::resolver resolver(write_io);
      asio::error_code con_error;
      asio::connect(write_socket,
                    resolver.resolve({std::string(rep_ip), std::to_string(rep_port + ECProject::DATANODE_PORT_SHIFT)}),
                    con_error);
      if (con_error)
      {
        if (recovery_grpc_thread.joinable())
          recovery_grpc_thread.join();
        close_pipeline_acceptor(acceptor);
        set_pipeline_error("pipeline local sink write stream connect failed");
        return false;
      }
      set_pipeline_socket_timeouts(write_socket, kPipelineSocketTimeoutSec);
    }

    const auto t0 = std::chrono::high_resolution_clock::now();
    asio::error_code ec;
    asio::ip::tcp::socket in_socket(accept_io);
    if (!accept_pipeline_socket(acceptor, in_socket, ec, 45))
    {
      close_pipeline_acceptor(acceptor);
      asio::error_code ignore_ec;
      write_socket.close(ignore_ec);
      if (recovery_grpc_thread.joinable())
        recovery_grpc_thread.join();
      set_pipeline_error("pipeline local sink accept failed: " + ec.message());
      return false;
    }
    close_pipeline_acceptor(acceptor);
    set_pipeline_socket_timeouts(in_socket, kPipelineSocketTimeoutSec);

    // The local-direct tail enters the failed node through this proxy and must
    // share the same node RX timeline as Phase2 helper and peer-exchange
    // traffic. DrainThenAccount in recv_pipeline_frame avoids wait-before-read
    // backpressure while preserving the aggregate ingress budget.
    SharedBandwidthLimiter *ingress_bw = nodeIngressBandwidth();
    SharedBandwidthLimiter *write_bw = egressBandwidthForDatanodeWrite(rep_ip, rep_port);

    std::unordered_map<int, std::vector<unsigned char>> pending;
    int next_shard = 0;
    int shards_received = 0;
    const auto write_begin = std::chrono::high_resolution_clock::now();

    auto flush_ready = [&]() -> bool {
      while (pending.count(next_shard) > 0)
      {
        auto it = pending.find(next_shard);
        asio::error_code write_ec;
        tcp_write_with_shared_bandwidth(write_socket, reinterpret_cast<const char *>(it->second.data()),
                                        static_cast<size_t>(stripe_len), write_bw, write_ec);
        if (write_ec)
        {
          set_pipeline_error("pipeline local sink write failed shard=" + std::to_string(next_shard) + " " +
                             write_ec.message());
          return false;
        }
        pending.erase(it);
        next_shard++;
      }
      return true;
    };

    while (true)
    {
      PipelineShardHeader hdr{};
      PipelineStreamEndHeader end_hdr{};
      std::vector<unsigned char> payload;
      const PipelineFrameType frame = recv_pipeline_frame(in_socket, hdr, end_hdr, payload, ingress_bw, ec);
      if (frame == PipelineFrameType::ERROR)
      {
        set_pipeline_error("pipeline local sink recv failed: " + ec.message());
        asio::error_code ignore_ec;
        write_socket.close(ignore_ec);
        close_pipeline_socket(in_socket);
        if (recovery_grpc_thread.joinable())
          recovery_grpc_thread.join();
        return false;
      }
      if (frame == PipelineFrameType::END)
      {
        if ((int)end_hdr.shard_count != shards_received)
        {
          set_pipeline_error("pipeline local sink shard count mismatch");
          asio::error_code ignore_ec;
          write_socket.close(ignore_ec);
          close_pipeline_socket(in_socket);
          if (recovery_grpc_thread.joinable())
            recovery_grpc_thread.join();
          return false;
        }
        break;
      }
      const int shard = static_cast<int>(hdr.shard_id);
      if (shard < 0 || shard >= shard_count || (int)payload.size() != stripe_len)
      {
        set_pipeline_error("pipeline local sink bad shard payload");
        asio::error_code ignore_ec;
        write_socket.close(ignore_ec);
        close_pipeline_socket(in_socket);
        if (recovery_grpc_thread.joinable())
          recovery_grpc_thread.join();
        return false;
      }
      pending[shard] = std::move(payload);
      shards_received++;
      if (!flush_ready())
      {
        asio::error_code ignore_ec;
        write_socket.close(ignore_ec);
        close_pipeline_socket(in_socket);
        if (recovery_grpc_thread.joinable())
          recovery_grpc_thread.join();
        return false;
      }
    }

    if (!flush_ready() || next_shard != shard_count)
    {
      set_pipeline_error("pipeline local sink incomplete write next=" + std::to_string(next_shard));
      asio::error_code ignore_ec;
      write_socket.close(ignore_ec);
      close_pipeline_socket(in_socket);
      if (recovery_grpc_thread.joinable())
        recovery_grpc_thread.join();
      return false;
    }

    asio::error_code ignore_ec;
    write_socket.shutdown(asio::ip::tcp::socket::shutdown_both, ignore_ec);
    write_socket.close(ignore_ec);
    close_pipeline_socket(in_socket);
    if (recovery_grpc_thread.joinable())
      recovery_grpc_thread.join();

    const auto t1 = std::chrono::high_resolution_clock::now();
    const double write_sec = std::chrono::duration<double>(t1 - write_begin).count();
    const double total_sec = std::chrono::duration<double>(t1 - t0).count();
    response->set_network_start_time(std::chrono::duration<double>(t0.time_since_epoch()).count());
    response->set_network_end_time(std::chrono::duration<double>(t1.time_since_epoch()).count());
    response->set_dest_data_node_network_time(write_sec);
    response->set_dest_data_node_disk_io_time(recovery_result.disk_io_end_time() -
                                              recovery_result.disk_io_start_time());
    std::cout << "[Proxy" << m_self_cluster_id << "][gLRC Pipeline] local sink done chain=" << chain_id
              << " shards=" << shards_received << " wall=" << total_sec << "s write=" << write_sec << "s"
              << " local_write=" << (write_bw == nullptr ? "yes" : "no") << std::endl;
    pipeline_trace("local sink write-back ok");
    return true;
  }
  catch (const std::exception &e)
  {
    set_pipeline_error(std::string("pipeline local sink exception: ") + e.what());
    return false;
  }
}

bool ProxyImpl::glrcIlpPipelineHubRecovery(const proxy_proto::RecoveryRequest *recovery_request,
                                         proxy_proto::RecoveryReply *response)
{
  const auto hub_wall_start = std::chrono::steady_clock::now();
  const int f = recovery_request->failed_block_ids_size();
  const int block_size = m_sys_config->BlockSize;
  const PipelineShardView shards = resolve_pipeline_shard_view(recovery_request, block_size, m_sys_config);
  if (f <= 0 || !shards.valid_geometry(block_size))
  {
    set_pipeline_error("invalid pipeline hub geometry");
    return false;
  }

  const int shard_count = shards.local_count;
  const int stripe_len = shards.stripe_len;
  const int epoch = recovery_request->pipeline_exchange_epoch();
  const int hub_chain_n = recovery_request->pipeline_hub_chain_eq_slots_size();
  if (hub_chain_n <= 0)
  {
    set_pipeline_error("pipeline hub missing eq_slot metadata");
    return false;
  }

  std::vector<int> failed_ids(f);
  std::vector<int> eq_indices(f);
  for (int i = 0; i < f; i++)
  {
    failed_ids[i] = recovery_request->failed_block_ids(i);
    eq_indices[i] = recovery_request->selected_equation_indices(i);
  }
  std::vector<unsigned char> hub_decode_inverse;
  if (!glrc_ilp_prepare_inverse(m_sys_config->k, m_sys_config->r, m_sys_config->z, failed_ids, eq_indices,
                                hub_decode_inverse))
  {
    set_pipeline_error("pipeline hub inverse prepare failed");
    return false;
  }

  std::vector<std::vector<std::vector<unsigned char>>> rhs_bufs(
      hub_chain_n, std::vector<std::vector<unsigned char>>(shard_count));
  std::vector<std::vector<bool>> rhs_ready(hub_chain_n, std::vector<bool>(shard_count, false));
  std::mutex rhs_mutex;
  std::condition_variable rhs_cv;

  struct HubListenCtx
  {
    int eq_slot = 0;
    bool hub_is_tail = false;
    bool local_only = false;
    unsigned char hub_coef = 0;
    GlrcPipelineEqCodec eq_codec = GlrcPipelineEqCodec::GLOBAL_CAUCHY;
  };
  std::vector<HubListenCtx> ctxs(hub_chain_n);
  const std::string hub_key = recovery_request->pipeline_hub_block_key();
  const std::string hub_dn_ip =
      recovery_request->pipeline_hop_datanode_ips_size() > 0 ? recovery_request->pipeline_hop_datanode_ips(0) : "";
  const int hub_dn_port =
      recovery_request->pipeline_hop_datanode_ports_size() > 0 ? recovery_request->pipeline_hop_datanode_ports(0) : 0;

  for (int i = 0; i < hub_chain_n; i++)
  {
    ctxs[i].eq_slot = recovery_request->pipeline_hub_chain_eq_slots(i);
    ctxs[i].hub_is_tail =
        recovery_request->pipeline_hub_is_chain_tail_flags_size() > i && recovery_request->pipeline_hub_is_chain_tail_flags(i) != 0;
    ctxs[i].hub_coef = recovery_request->pipeline_hub_chain_hub_coefs_size() > i
                           ? static_cast<unsigned char>(recovery_request->pipeline_hub_chain_hub_coefs(i) & 0xff)
                           : 0;
    ctxs[i].local_only = recovery_request->pipeline_hub_chain_local_only_flags_size() > i &&
                         recovery_request->pipeline_hub_chain_local_only_flags(i) != 0;
    ctxs[i].eq_codec = pipeline_hub_chain_codec(recovery_request, i);
  }

  std::atomic<bool> hub_failed{false};
  std::string hub_fail_msg;

  // One shared hub-block DN stream for all hub_is_tail / local_only chains.
  // Re-reading the same 64MB once per chain was adding CPU/disk contention on
  // top of the intentional N-chain network bottleneck (target ≈ N×B/w).
  bool any_hub_local_read = false;
  for (int ci = 0; ci < hub_chain_n; ci++)
  {
    if (ctxs[ci].local_only || (ctxs[ci].hub_is_tail && ctxs[ci].hub_coef != 0))
    {
      any_hub_local_read = true;
      break;
    }
  }
  std::vector<std::vector<unsigned char>> shared_hub_shards(static_cast<size_t>(shard_count));
  std::vector<char> shared_hub_ready(static_cast<size_t>(shard_count), 0);
  std::mutex shared_hub_mu;
  std::condition_variable shared_hub_cv;
  std::atomic<bool> shared_hub_failed{false};
  std::string shared_hub_fail_msg;
  std::thread shared_hub_reader;
  if (any_hub_local_read)
  {
    shared_hub_reader = std::thread([&, stripe_len, shard_count, block_size, hub_key, hub_dn_ip, hub_dn_port]() {
      try
      {
        asio::io_context hub_dn_io;
        asio::ip::tcp::socket hub_stream(hub_dn_io);
        const int saved_offset = g_pipeline_dn_read_offset;
        const int saved_length = g_pipeline_dn_read_length;
        g_pipeline_dn_read_offset = shards.byte_off(0);
        g_pipeline_dn_read_length = shard_count * stripe_len;
        const bool hub_stream_opened =
            openDatanodeGetStream(hub_key, hub_dn_ip, hub_dn_port, block_size, hub_dn_io, hub_stream);
        g_pipeline_dn_read_offset = saved_offset;
        g_pipeline_dn_read_length = saved_length;
        if (!hub_stream_opened)
        {
          shared_hub_fail_msg = "shared hub DN stream setup failed";
          shared_hub_failed.store(true);
          hub_failed.store(true);
          shared_hub_cv.notify_all();
          rhs_cv.notify_all();
          return;
        }
        for (int shard = 0; shard < shard_count && !hub_failed.load(); shard++)
        {
          std::vector<unsigned char> local(static_cast<size_t>(stripe_len), 0);
          asio::error_code read_ec;
          // Local DN↔proxy is not rate-limited (see ingressBandwidthForDatanodeRead).
          tcp_read_with_shared_bandwidth(hub_stream, reinterpret_cast<char *>(local.data()),
                                         static_cast<size_t>(stripe_len), nullptr, read_ec);
          if (read_ec)
          {
            shared_hub_fail_msg = "shared hub DN read failed: " + read_ec.message();
            shared_hub_failed.store(true);
            hub_failed.store(true);
            shared_hub_cv.notify_all();
            rhs_cv.notify_all();
            close_pipeline_socket(hub_stream);
            return;
          }
          {
            std::lock_guard<std::mutex> lock(shared_hub_mu);
            shared_hub_shards[static_cast<size_t>(shard)] = std::move(local);
            shared_hub_ready[static_cast<size_t>(shard)] = 1;
          }
          shared_hub_cv.notify_all();
        }
        close_pipeline_socket(hub_stream);
      }
      catch (const std::exception &e)
      {
        shared_hub_fail_msg = std::string("shared hub reader exception: ") + e.what();
        shared_hub_failed.store(true);
        hub_failed.store(true);
        shared_hub_cv.notify_all();
        rhs_cv.notify_all();
      }
    });
  }
  auto wait_shared_hub_shard = [&](int shard, const unsigned char *&out_ptr) -> bool {
    std::unique_lock<std::mutex> lock(shared_hub_mu);
    shared_hub_cv.wait(lock, [&]() {
      return hub_failed.load() || shared_hub_failed.load() ||
             (shard >= 0 && shard < shard_count && shared_hub_ready[static_cast<size_t>(shard)] != 0);
    });
    if (hub_failed.load() || shared_hub_failed.load() || shard < 0 || shard >= shard_count)
      return false;
    // Shard buffers are immutable after ready; safe to use without holding the lock.
    out_ptr = shared_hub_shards[static_cast<size_t>(shard)].data();
    return out_ptr != nullptr || stripe_len == 0;
  };

  struct HubPreboundAcceptor
  {
    int ci = 0;
    int listen_port = 0;
    std::shared_ptr<asio::io_context> io;
    std::shared_ptr<asio::ip::tcp::acceptor> acceptor;
  };
  std::unordered_map<int, HubPreboundAcceptor> prebound_acceptors;
  for (auto &ready_pb : take_hub_ready_acceptors(epoch))
  {
    HubPreboundAcceptor pb;
    pb.ci = ready_pb.ci;
    pb.listen_port = ready_pb.listen_port;
    pb.io = std::move(ready_pb.io);
    pb.acceptor = std::move(ready_pb.acceptor);
    prebound_acceptors.emplace(pb.ci, std::move(pb));
  }
  for (int ci = 0; ci < hub_chain_n; ci++)
  {
    if (ctxs[ci].local_only || prebound_acceptors.count(ci) > 0)
      continue;
    HubPreboundAcceptor pb;
    pb.ci = ci;
    pb.listen_port = resolve_hub_listener_port(recovery_request, ci, m_port, epoch, ctxs[ci].eq_slot);
    pb.io = std::make_shared<asio::io_context>();
    pb.acceptor = std::make_shared<asio::ip::tcp::acceptor>(*pb.io);
    asio::error_code bind_ec;
    if (!open_pipeline_acceptor(*pb.io, pb.listen_port, *pb.acceptor, bind_ec))
    {
      for (auto &kv : prebound_acceptors)
        close_pipeline_acceptor(*kv.second.acceptor);
      hub_failed.store(true);
      shared_hub_cv.notify_all();
      if (shared_hub_reader.joinable())
        shared_hub_reader.join();
      set_pipeline_error("hub bind failed port=" + std::to_string(pb.listen_port) + " " + bind_ec.message());
      return false;
    }
    prebound_acceptors.emplace(ci, std::move(pb));
  }
  if (!prebound_acceptors.empty())
  {
    char tb[96];
    snprintf(tb, sizeof(tb), "hub prebound listeners=%zu", prebound_acceptors.size());
    pipeline_trace(tb);
  }

  std::vector<std::thread> listeners;
  for (int ci = 0; ci < hub_chain_n; ci++)
  {
    const int eq_slot = ctxs[ci].eq_slot;
    const int listen_port = resolve_hub_listener_port(recovery_request, ci, m_port, epoch, eq_slot);
    const auto prebound_it = prebound_acceptors.find(ci);
    const bool has_prebound = prebound_it != prebound_acceptors.end();
    std::shared_ptr<asio::io_context> bound_io = has_prebound ? prebound_it->second.io : nullptr;
    std::shared_ptr<asio::ip::tcp::acceptor> bound_acceptor =
        has_prebound ? prebound_it->second.acceptor : nullptr;
    listeners.emplace_back([this, ci, eq_slot, listen_port, bound_io, bound_acceptor, stripe_len, shard_count,
                            &ctxs, &rhs_bufs, &rhs_ready, &rhs_mutex, &rhs_cv, &hub_failed, &hub_fail_msg,
                            &wait_shared_hub_shard, &shared_hub_failed, &shared_hub_fail_msg]() {
      try
      {
        asio::io_context io;
        asio::error_code ec;
        if (hub_failed.load())
          return;

        if (ctxs[ci].local_only)
        {
          for (int shard = 0; shard < shard_count && !hub_failed.load(); shard++)
          {
            std::vector<unsigned char> partial(static_cast<size_t>(stripe_len), 0);
            const unsigned char *local = nullptr;
            if (!wait_shared_hub_shard(shard, local))
            {
              hub_fail_msg = shared_hub_fail_msg.empty() ? "hub local_only shared read failed"
                                                         : shared_hub_fail_msg;
              hub_failed.store(true);
              rhs_cv.notify_all();
              return;
            }
            glrc_pipeline_init_partial_range(partial.data(), local, ctxs[ci].hub_coef, stripe_len,
                                             ctxs[ci].eq_codec);
            {
              std::lock_guard<std::mutex> lock(rhs_mutex);
              rhs_bufs[ci][shard] = std::move(partial);
              rhs_ready[ci][shard] = true;
            }
            rhs_cv.notify_all();
          }
          return;
        }

        if (hub_failed.load())
          return;
        asio::io_context &accept_io = bound_io != nullptr ? *bound_io : io;
        asio::ip::tcp::acceptor *acceptor_ptr = nullptr;
        asio::ip::tcp::acceptor stack_acceptor(accept_io);
        if (bound_acceptor != nullptr)
        {
          acceptor_ptr = bound_acceptor.get();
        }
        else
        {
          if (!open_pipeline_acceptor(accept_io, listen_port, stack_acceptor, ec))
          {
            hub_fail_msg = "hub bind failed port=" + std::to_string(listen_port) + " " + ec.message();
            hub_failed.store(true);
            rhs_cv.notify_all();
            return;
          }
          acceptor_ptr = &stack_acceptor;
        }
        struct PipelineAcceptorGuard
        {
          asio::ip::tcp::acceptor *acceptor = nullptr;
          ~PipelineAcceptorGuard()
          {
            if (acceptor)
              close_pipeline_acceptor(*acceptor);
          }
        } acceptor_guard{acceptor_ptr};
        asio::ip::tcp::socket in_socket(accept_io);
        if (!accept_pipeline_socket(*acceptor_ptr, in_socket, ec, 45, &hub_failed))
        {
          hub_fail_msg = "hub accept failed: " + ec.message();
          hub_failed.store(true);
          rhs_cv.notify_all();
          return;
        }
        set_pipeline_socket_timeouts(in_socket, kPipelineSocketTimeoutSec);
        // Always drain inbound TCP promptly.  Per-shard receiver sleeps caused
        // backpressure, connection resets, and shard-count mismatches.  The
        // aggregate node RX budget is applied once below as N * block/BW.
        SharedBandwidthLimiter *hub_chain_ingress = nullptr;

        int shards_received = 0;
        const bool needs_tail_local = ctxs[ci].hub_is_tail && ctxs[ci].hub_coef != 0;
        const int hub_pipe_window = resolve_pipeline_window(m_sys_config, shard_count);

        auto finalize_hub_shard = [&](int shard, std::vector<unsigned char> &partial) -> bool {
          if (shard < 0 || shard >= shard_count || (int)partial.size() != stripe_len)
          {
            hub_fail_msg = "hub shard mismatch";
            hub_failed.store(true);
            rhs_cv.notify_all();
            return false;
          }
          if (needs_tail_local)
          {
            const unsigned char *local = nullptr;
            if (!wait_shared_hub_shard(shard, local))
            {
              hub_fail_msg = shared_hub_fail_msg.empty() ? "hub tail shared local read failed"
                                                         : shared_hub_fail_msg;
              hub_failed.store(true);
              rhs_cv.notify_all();
              return false;
            }
            glrc_pipeline_accumulate_range(partial.data(), local, ctxs[ci].hub_coef, stripe_len,
                                           ctxs[ci].eq_codec);
          }
          {
            std::lock_guard<std::mutex> lock(rhs_mutex);
            rhs_bufs[ci][shard] = std::move(partial);
            rhs_ready[ci][shard] = true;
          }
          rhs_cv.notify_all();
          shards_received++;
          return true;
        };

        if (hub_pipe_window <= 1 || !needs_tail_local)
        {
          while (!hub_failed.load())
          {
            PipelineShardHeader hdr{};
            PipelineStreamEndHeader end_hdr{};
            std::vector<unsigned char> partial;
            const PipelineFrameType frame =
                recv_pipeline_frame(in_socket, hdr, end_hdr, partial, hub_chain_ingress, ec);
            if (frame == PipelineFrameType::ERROR)
            {
              hub_fail_msg = "hub recv frame failed: " + ec.message();
              hub_failed.store(true);
              rhs_cv.notify_all();
              close_pipeline_socket(in_socket);
              return;
            }
            if (frame == PipelineFrameType::END)
            {
              if ((int)end_hdr.shard_count != shards_received)
              {
                hub_fail_msg = "hub stream shard count mismatch";
                hub_failed.store(true);
                rhs_cv.notify_all();
              }
              break;
            }
            const int shard = static_cast<int>(hdr.shard_id);
            if (!finalize_hub_shard(shard, partial))
            {
              close_pipeline_socket(in_socket);
              return;
            }
          }
        }
        else
        {
          PipelineBoundedQueue hub_inbound_q(static_cast<size_t>(hub_pipe_window));
          std::atomic<bool> hub_reader_failed{false};
          std::mutex hub_reader_err_mu;
          std::string hub_reader_err;
          int upstream_end_count = -1;

          std::thread hub_reader([&]() {
            while (!hub_reader_failed.load())
            {
              PipelineShardHeader hdr{};
              PipelineStreamEndHeader end_hdr{};
              std::vector<unsigned char> partial;
              const PipelineFrameType frame =
                  recv_pipeline_frame(in_socket, hdr, end_hdr, partial, hub_chain_ingress, ec);
              if (frame == PipelineFrameType::ERROR)
              {
                std::lock_guard<std::mutex> lock(hub_reader_err_mu);
                hub_reader_err = "hub recv frame failed: " + ec.message();
                hub_reader_failed.store(true);
                hub_inbound_q.close();
                return;
              }
              if (frame == PipelineFrameType::END)
              {
                PipelineQueuedShard end_item;
                end_item.end_of_stream = true;
                end_item.end_shard_count = static_cast<int>(end_hdr.shard_count);
                hub_inbound_q.push(std::move(end_item));
                hub_inbound_q.close();
                return;
              }
              PipelineQueuedShard item;
              item.shard_id = static_cast<int>(hdr.shard_id);
              item.payload = std::move(partial);
              hub_inbound_q.push(std::move(item));
            }
            hub_inbound_q.close();
          });

          PipelineQueuedShard hub_item;
          while (hub_inbound_q.pop(hub_item))
          {
            if (hub_item.end_of_stream)
            {
              upstream_end_count = hub_item.end_shard_count;
              break;
            }
            if (!finalize_hub_shard(hub_item.shard_id, hub_item.payload))
            {
              hub_reader_failed.store(true);
              break;
            }
          }
          hub_inbound_q.close();
          if (hub_reader.joinable())
            hub_reader.join();
          if (hub_reader_failed.load())
          {
            if (!hub_reader_err.empty())
              hub_fail_msg = hub_reader_err;
            hub_failed.store(true);
            rhs_cv.notify_all();
            close_pipeline_socket(in_socket);
            return;
          }
          if (upstream_end_count >= 0 && upstream_end_count != shards_received)
          {
            hub_fail_msg = "hub stream shard count mismatch";
            hub_failed.store(true);
            rhs_cv.notify_all();
          }
        }
        close_pipeline_socket(in_socket);
      }
      catch (const std::exception &e)
      {
        hub_fail_msg = std::string("hub listener exception: ") + e.what();
        hub_failed.store(true);
        rhs_cv.notify_all();
      }
    });
  }

  double min_decode_start = 0.0, max_decode_end = 0.0;
  double total_decode_time = 0.0;
  double total_write_net = 0.0;
  double total_write_disk = 0.0;
  std::atomic<bool> any_write_failed{false};
  std::mutex hub_metrics_mu;
  std::vector<bool> shard_scheduled(shard_count, false);
  int shards_finished = 0;

  // Generalize the local-direct sink's one persistent DN stream to every
  // hub-recovered block.  The old implementation opened one gRPC + TCP stream
  // and flushed the file for every 1 MiB shard (f * shard_count times), adding
  // roughly one block-transfer tail after fan-in and allowing out-of-order
  // stripe writes.  Each target now has one ordered full-block writer.
  struct HubWriteStream
  {
    int failed_index = 0;
    asio::io_context io;
    asio::ip::tcp::socket socket;
    grpc::ClientContext grpc_context;
    datanode_proto::MergeParityInfo recovery_info;
    datanode_proto::RequestResult recovery_result;
    grpc::Status grpc_status;
    std::thread grpc_thread;
    std::thread writer_thread;
    std::mutex mu;
    std::condition_variable cv;
    std::map<int, std::vector<unsigned char>> pending;
    int next_shard = 0;
    bool decode_done = false;
    bool failed = false;
    double first_write_offset = -1.0;
    double network_time = 0.0;

    explicit HubWriteStream(int index) : failed_index(index), socket(io) {}
  };
  std::vector<std::unique_ptr<HubWriteStream>> write_streams;
  write_streams.reserve(static_cast<size_t>(f));

  auto notify_all_write_streams = [&]() {
    for (auto &ws : write_streams)
      ws->cv.notify_all();
  };

  for (int i = 0; i < f; i++)
  {
    auto ws = std::make_unique<HubWriteStream>(i);
    ws->recovery_info.set_block_key(recovery_request->failed_block_keys(i));
    ws->recovery_info.set_block_id(failed_ids[i]);
    // Full recovery keeps atomic publish. Hybrid [p,S) uses the datanode's
    // non-truncating range mode so the Phase2-owned [0,p) prefix survives.
    const bool full_block_write =
        shards.global_begin == 0 && shards.local_count == shards.global_S;
    ws->recovery_info.set_recovery_offset(
        full_block_write ? 0 : shards.byte_off(0));
    ws->recovery_info.set_recovery_size(
        full_block_write ? 0 : shard_count * stripe_len);
    ws->recovery_info.set_proxy_ip(m_ip);
    ws->recovery_info.set_proxy_port(m_port);

    const std::string rep_ip = recovery_request->replaced_node_ips(i);
    const int rep_port = recovery_request->replaced_node_ports(i);
    const std::string node_ip_port = rep_ip + ":" + std::to_string(rep_port);
    auto dn_it = m_datanode_ptrs.find(node_ip_port);
    if (dn_it == m_datanode_ptrs.end() || !dn_it->second)
    {
      hub_fail_msg = "pipeline hub persistent write datanode stub missing target=" + node_ip_port;
      hub_failed.store(true);
      break;
    }

    ws->grpc_context.set_deadline(std::chrono::system_clock::now() + std::chrono::seconds(120));
    datanode_proto::datanodeService::Stub *dn_stub = dn_it->second.get();
    HubWriteStream *ws_ptr = ws.get();
    ws->grpc_thread = std::thread([dn_stub, ws_ptr]() {
      ws_ptr->grpc_status =
          dn_stub->handleRecoveryBreakdown(&ws_ptr->grpc_context, ws_ptr->recovery_info,
                                           &ws_ptr->recovery_result);
    });

    asio::ip::tcp::resolver resolver(ws->io);
    asio::error_code connect_ec;
    asio::connect(ws->socket,
                  resolver.resolve({rep_ip, std::to_string(rep_port + ECProject::DATANODE_PORT_SHIFT)}),
                  connect_ec);
    if (connect_ec)
    {
      hub_fail_msg = "pipeline hub persistent write connect failed target=" + node_ip_port + " " +
                     connect_ec.message();
      hub_failed.store(true);
      asio::error_code ignore_ec;
      ws->socket.close(ignore_ec);
      write_streams.push_back(std::move(ws));
      break;
    }
    set_pipeline_socket_timeouts(ws->socket, kPipelineSocketTimeoutSec);

    SharedBandwidthLimiter *write_bw = egressBandwidthForDatanodeWrite(rep_ip.c_str(), rep_port);
    ws->writer_thread = std::thread([&, ws_ptr, write_bw]() {
      const auto write_start = std::chrono::steady_clock::now();
      while (!hub_failed.load())
      {
        std::vector<unsigned char> payload;
        {
          std::unique_lock<std::mutex> lock(ws_ptr->mu);
          ws_ptr->cv.wait(lock, [&]() {
            return hub_failed.load() || ws_ptr->failed ||
                   ws_ptr->pending.count(ws_ptr->next_shard) > 0 ||
                   ws_ptr->decode_done;
          });
          if (hub_failed.load() || ws_ptr->failed)
            break;
          auto it = ws_ptr->pending.find(ws_ptr->next_shard);
          if (it == ws_ptr->pending.end())
          {
            if (ws_ptr->decode_done)
            {
              if (ws_ptr->next_shard != shard_count)
              {
                ws_ptr->failed = true;
                hub_fail_msg = "pipeline hub persistent write missing shard target=" +
                               std::to_string(ws_ptr->failed_index) + " next=" +
                               std::to_string(ws_ptr->next_shard);
                hub_failed.store(true);
                rhs_cv.notify_all();
                notify_all_write_streams();
              }
              break;
            }
            continue;
          }
          payload = std::move(it->second);
          ws_ptr->pending.erase(it);
        }

        asio::error_code write_ec;
        if (ws_ptr->first_write_offset < 0.0)
          ws_ptr->first_write_offset =
              std::chrono::duration<double>(std::chrono::steady_clock::now() - hub_wall_start).count();
        tcp_write_with_shared_bandwidth(ws_ptr->socket,
                                        reinterpret_cast<const char *>(payload.data()),
                                        payload.size(), write_bw, write_ec);
        if (write_ec)
        {
          ws_ptr->failed = true;
          hub_fail_msg = "pipeline hub persistent write failed target=" +
                         std::to_string(ws_ptr->failed_index) + " shard=" +
                         std::to_string(ws_ptr->next_shard) + " " + write_ec.message();
          hub_failed.store(true);
          rhs_cv.notify_all();
          notify_all_write_streams();
          break;
        }
        ws_ptr->next_shard++;
      }

      asio::error_code ignore_ec;
      ws_ptr->socket.shutdown(asio::ip::tcp::socket::shutdown_both, ignore_ec);
      ws_ptr->socket.close(ignore_ec);
      ws_ptr->network_time =
          std::chrono::duration<double>(std::chrono::steady_clock::now() - write_start).count();
    });
    write_streams.push_back(std::move(ws));
  }

  if (hub_failed.load())
  {
    notify_all_write_streams();
    for (auto &ws : write_streams)
    {
      asio::error_code ignore_ec;
      ws->socket.close(ignore_ec);
      if (ws->writer_thread.joinable())
        ws->writer_thread.join();
      if (ws->grpc_thread.joinable())
        ws->grpc_thread.join();
    }
    for (auto &th : listeners)
      if (th.joinable())
        th.join();
    if (shared_hub_reader.joinable())
      shared_hub_reader.join();
    set_pipeline_error(hub_fail_msg);
    return false;
  }

  auto shard_ready_locked = [&](int shard) -> bool {
    for (int ci = 0; ci < hub_chain_n; ci++)
      if (!rhs_ready[ci][shard])
        return false;
    return true;
  };
  auto find_ready_shard_locked = [&]() -> int {
    for (int shard = 0; shard < shard_count; shard++)
      if (!shard_scheduled[shard] && shard_ready_locked(shard))
        return shard;
    return -1;
  };

  const int worker_window = resolve_pipeline_window(m_sys_config, shard_count);
  // A full 64-shard window controls buffering, not useful CPU parallelism.
  // Launching 64 decoders at once delays shard 0 by ~0.5s under contention,
  // so writeback cannot overlap fan-in.  A small pool keeps the first result
  // flowing while still hiding decode behind network transfer.
  constexpr int kHubDecodeWorkers = 4;
  const int decode_worker_count =
      std::max(1, std::min({worker_window, shard_count, kHubDecodeWorkers}));

  std::vector<std::thread> decode_workers;
  decode_workers.reserve(static_cast<size_t>(decode_worker_count));
  for (int wi = 0; wi < decode_worker_count; wi++)
  {
    decode_workers.emplace_back([&, wi]() {
      (void)wi;
      while (!hub_failed.load())
      {
        int shard = -1;
        std::vector<std::vector<unsigned char>> shard_rhs;
        {
          std::unique_lock<std::mutex> lock(rhs_mutex);
          rhs_cv.wait(lock, [&]() {
            return hub_failed.load() || shards_finished >= shard_count || find_ready_shard_locked() >= 0;
          });
          if (hub_failed.load() || shards_finished >= shard_count)
            return;
          shard = find_ready_shard_locked();
          if (shard < 0)
            continue;
          shard_scheduled[shard] = true;
          shard_rhs.resize(hub_chain_n);
          for (int ci = 0; ci < hub_chain_n; ci++)
            shard_rhs[ci] = std::move(rhs_bufs[ci][shard]);
        }

        std::vector<unsigned char *> rhs_ptrs(hub_chain_n, nullptr);
        for (int ci = 0; ci < hub_chain_n; ci++)
          rhs_ptrs[ci] = shard_rhs[ci].data();

        std::vector<std::vector<unsigned char>> shard_recovered;
        const auto decode_begin = std::chrono::high_resolution_clock::now();
        const bool decode_ok = decode_glrc_ilp_rhs_compact(rhs_ptrs, hub_decode_inverse, f, stripe_len,
                                                           shard_recovered);
        const auto decode_end = std::chrono::high_resolution_clock::now();

        if (!decode_ok)
        {
          hub_fail_msg = "hub decode_glrc_ilp_rhs_range failed";
          hub_failed.store(true);
          rhs_cv.notify_all();
          return;
        }

        const double decode_duration = std::chrono::duration<double>(decode_end - decode_begin).count();
        {
          std::lock_guard<std::mutex> lock(hub_metrics_mu);
          total_decode_time += decode_duration;
          min_decode_start = 0.0;
          max_decode_end = total_decode_time;
        }

        for (int i = 0; i < f; i++)
        {
          HubWriteStream *ws = write_streams[static_cast<size_t>(i)].get();
          {
            std::lock_guard<std::mutex> lock(ws->mu);
            ws->pending.emplace(shard, std::move(shard_recovered[i]));
          }
          ws->cv.notify_one();
        }

        {
          std::lock_guard<std::mutex> lock(rhs_mutex);
          shards_finished++;
        }
        rhs_cv.notify_all();
        char tb[128];
        snprintf(tb, sizeof(tb), "hub decode shard=%d/%d (ready-worker)", shard, shard_count);
        pipeline_trace(tb);
      }
    });
  }
  for (auto &th : decode_workers)
  {
    if (th.joinable())
      th.join();
  }

  double max_write_net = 0.0;
  for (auto &ws : write_streams)
  {
    {
      std::lock_guard<std::mutex> lock(ws->mu);
      ws->decode_done = true;
    }
    ws->cv.notify_one();
  }
  for (auto &ws : write_streams)
  {
    if (ws->writer_thread.joinable())
      ws->writer_thread.join();
    if (ws->grpc_thread.joinable())
      ws->grpc_thread.join();
    if (ws->failed || !ws->grpc_status.ok() || !ws->recovery_result.message() ||
        ws->next_shard != shard_count)
    {
      any_write_failed.store(true);
      if (hub_fail_msg.empty())
        hub_fail_msg = "pipeline hub persistent write-back failed target=" +
                       std::to_string(ws->failed_index);
    }
    total_write_net += ws->network_time;
    max_write_net = std::max(max_write_net, ws->network_time);
    total_write_disk += ws->recovery_result.disk_io_end_time() -
                        ws->recovery_result.disk_io_start_time();
  }

  if (any_write_failed.load())
  {
    if (hub_fail_msg.empty())
      hub_fail_msg = "pipeline hub persistent write-back failed";
    hub_failed.store(true);
  }

  std::cout << "[Proxy" << m_self_cluster_id << "][gLRC Pipeline] hub write timing f=" << f
            << " chains=" << hub_chain_n;
  for (const auto &ws : write_streams)
    std::cout << " target" << ws->failed_index << "_first=" << ws->first_write_offset
              << "s target" << ws->failed_index << "_wall=" << ws->network_time << "s";
  std::cout << std::endl;

  for (auto &th : listeners)
  {
    if (th.joinable())
      th.join();
  }
  if (shared_hub_reader.joinable())
    shared_hub_reader.join();
  for (auto &kv : prebound_acceptors)
    close_pipeline_acceptor(*kv.second.acceptor);
  prebound_acceptors.clear();

  if (hub_failed.load() || shared_hub_failed.load())
  {
    if (hub_fail_msg.empty() && !shared_hub_fail_msg.empty())
      hub_fail_msg = shared_hub_fail_msg;
    set_pipeline_error(hub_fail_msg.empty() ? "pipeline hub failed" : hub_fail_msg);
    return false;
  }

  // Model one shared hub NIC ingress deterministically without scheduling
  // sleeps on live TCP readers.  N simultaneous full-block chains consume
  // N*B bytes from the same RX budget, while decode/writeback can overlap.
  const double hub_ingress_floor =
      node_block_transfer_seconds(static_cast<size_t>(shard_count) *
                                      static_cast<size_t>(stripe_len) *
                                      static_cast<size_t>(hub_chain_n),
                                  m_sys_config->NodeBlockBandwidthMBps);
  const double hub_elapsed = std::chrono::duration<double>(
                                 std::chrono::steady_clock::now() - hub_wall_start)
                                 .count();
  sleep_for_bandwidth_remainder(hub_ingress_floor, hub_elapsed);

  response->set_decode_start_time(min_decode_start);
  response->set_decode_end_time(max_decode_end);
  // Expose the actual shared-hub egress critical path separately from the
  // sum-of-work diagnostic below. The coordinator uses this wall time when
  // balancing Hybrid's failed-node hotspot against the Pipeline hub.
  const double hub_metric_end =
      std::chrono::duration<double>(std::chrono::high_resolution_clock::now().time_since_epoch()).count();
  response->set_network_start_time(hub_metric_end - max_write_net);
  response->set_network_end_time(hub_metric_end);
  response->set_dest_data_node_network_time(total_write_net);
  response->set_dest_data_node_disk_io_time(total_write_disk);
  std::cout << "[Proxy" << m_self_cluster_id << "][gLRC Pipeline] hub decode success chains=" << hub_chain_n
            << " shards=" << shard_count << std::endl;
  return true;
}

} // namespace ECProject
