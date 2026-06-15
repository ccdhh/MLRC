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
#include <cstdint>
#include <cstdio>
#include <cstring>
#include <iostream>
#include <mutex>
#include <string>
#include <thread>
#include <vector>

#if defined(__linux__) || defined(__APPLE__)
#include <sys/socket.h>
#endif

namespace ECProject
{
namespace
{
constexpr uint32_t kPipelineMagic = 0x504C5031u;    // "PLP1" shard payload
constexpr uint32_t kPipelineStreamEnd = 0x504C4544u; // "PLED" end of shard stream
constexpr int kPipelineSocketTimeoutSec = 120;

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
static std::mutex g_pipeline_bind_mutex;

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

int pipeline_proxy_index(int proxy_grpc_port)
{
  const int idx = (proxy_grpc_port - PROXY_GRPC_BASE) / PROXY_GRPC_STRIDE;
  return idx < 0 ? 0 : idx;
}

int avoid_reserved_pipeline_port(int port, int proxy_idx, int slot)
{
  // Pipeline bands can numerically collide with the coordinator listen port (55555).
  if (port == 55555)
    return PROXY_PIPELINE_EXCHANGE_BASE + proxy_idx * PROXY_PIPELINE_PER_PROXY_BAND + ((slot + 1) % PROXY_PIPELINE_PER_PROXY_BAND);
  return port;
}

int pipeline_hop_listen_port(int proxy_grpc_port, int exchange_epoch, int chain_id, int hop_index)
{
  const int proxy_idx = pipeline_proxy_index(proxy_grpc_port);
  const int slot = (exchange_epoch * 17 + chain_id * 32 + hop_index) % (PROXY_PIPELINE_PER_PROXY_BAND / 2);
  const int port = PROXY_PIPELINE_EXCHANGE_BASE + proxy_idx * PROXY_PIPELINE_PER_PROXY_BAND + slot;
  return avoid_reserved_pipeline_port(port, proxy_idx, slot);
}

int pipeline_hub_listen_port(int hub_proxy_grpc_port, int exchange_epoch, int eq_slot)
{
  const int proxy_idx = pipeline_proxy_index(hub_proxy_grpc_port);
  const int slot = PROXY_PIPELINE_PER_PROXY_BAND / 2 +
                     (exchange_epoch * 17 + eq_slot * 32 + 1) % (PROXY_PIPELINE_PER_PROXY_BAND / 2);
  const int port = PROXY_PIPELINE_EXCHANGE_BASE + proxy_idx * PROXY_PIPELINE_PER_PROXY_BAND + slot;
  return avoid_reserved_pipeline_port(port, proxy_idx, slot);
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
    tcp_read_with_shared_bandwidth(socket, reinterpret_cast<char *>(payload.data()), hdr.stripe_len, bw, ec);
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

bool open_pipeline_acceptor(asio::io_context &io, int listen_port, asio::ip::tcp::acceptor &acceptor,
                            asio::error_code &ec)
{
  ec.clear();
  const asio::ip::tcp::endpoint ep(asio::ip::tcp::v4(), listen_port);
  acceptor.open(ep.protocol(), ec);
  if (ec)
    return false;
  acceptor.set_option(asio::socket_base::reuse_address(true), ec);
  if (ec)
    return false;
  {
    std::lock_guard<std::mutex> lock(g_pipeline_bind_mutex);
    acceptor.bind(ep, ec);
  }
  if (ec)
    return false;
  acceptor.listen(asio::socket_base::max_listen_connections, ec);
  return !ec;
}

bool connect_pipeline_socket(asio::ip::tcp::socket &socket, const std::string &next_ip, int next_port,
                             asio::error_code &ec, int max_retries = 120)
{
  const auto endpoint = asio::ip::tcp::endpoint(asio::ip::address::from_string(next_ip), next_port);
  for (int attempt = 0; attempt < max_retries; attempt++)
  {
    ec.clear();
    socket.connect(endpoint, ec);
    if (!ec)
      return true;
    if (ec != asio::error::connection_refused)
      break;
    std::this_thread::sleep_for(std::chrono::milliseconds(50));
  }
  set_pipeline_error("pipeline connect failed " + next_ip + ":" + std::to_string(next_port) + " " + ec.message());
  return false;
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
  if (pipeline_chain_is_local_direct(recovery_request))
    return false;
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
                       double *net_end, SharedBandwidthLimiter *ingress_bw)
{
  char *buf = static_cast<char *>(std::aligned_alloc(32, block_size));
  if (buf == nullptr)
    return false;
  std::memset(buf, 0, block_size);
  double ds = 0.0, de = 0.0, ns = 0.0, ne = 0.0;
  const bool ok = self->GetFromDatanodeStripeRangeBreakdown(key, buf, block_size, off, len, ip.c_str(), port, &ds,
                                                            &de, &ns, &ne, nullptr, nullptr, ingress_bw);
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

} // namespace

std::string glrc_pipeline_take_last_error()
{
  std::string out = g_glrc_pipeline_last_error;
  g_glrc_pipeline_last_error.clear();
  return out;
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
  set_pipeline_error("invalid pipeline_role");
  return false;
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
  int shard_count = recovery_request->pipeline_shard_count() > 0 ? recovery_request->pipeline_shard_count()
                                                                 : m_sys_config->GlrcShardCount;
  if (shard_count <= 0 || block_size % shard_count != 0)
  {
    set_pipeline_error("invalid pipeline shard geometry");
    return false;
  }
  const int stripe_len = block_size / shard_count;
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
            << " hops=" << hops_n << " shards=" << shard_count << std::endl;

  const bool next_is_hub = hub_is_tail && hops_n == 1;
  const std::string next_ip =
      (hops_n == 1)
          ? recovery_request->pipeline_hub_proxy_ip()
          : (next_is_hub ? recovery_request->pipeline_hub_proxy_ip()
                         : recovery_request->pipeline_hop_proxy_ips(1));
  const int listen_port =
      (hops_n == 1 || next_is_hub)
          ? resolve_chain_hub_listen_port(recovery_request, recovery_request->pipeline_hub_proxy_port(), epoch, eq_slot)
          : resolve_hop_listen_port(recovery_request, 1, recovery_request->pipeline_hop_proxy_ports(1), epoch,
                                    chain_id);

  const std::string head_key = recovery_request->pipeline_hop_block_keys(0);
  const std::string head_ip = recovery_request->pipeline_hop_datanode_ips(0);
  const int head_port = recovery_request->pipeline_hop_datanode_ports(0);

  // Coordinator starts hop wiring before chain_head RPC; brief wait before downstream TCP connect.
  std::this_thread::sleep_for(std::chrono::milliseconds(500));

  asio::io_context io;
  asio::error_code ec;
  asio::ip::tcp::socket out_socket(io);
  if (!connect_pipeline_socket(out_socket, next_ip, listen_port, ec, 120))
    return false;
  set_pipeline_socket_timeouts(out_socket, kPipelineSocketTimeoutSec);
  std::cout << "[Proxy" << m_self_cluster_id << "][gLRC Pipeline] chain_head connected chain=" << chain_id
            << " -> " << next_ip << ":" << listen_port << " streaming=" << shard_count << std::endl;

  char *block_buf = static_cast<char *>(std::aligned_alloc(32, block_size));
  if (block_buf == nullptr)
  {
    close_pipeline_socket(out_socket);
    set_pipeline_error("pipeline chain head alloc failed");
    return false;
  }
  std::memset(block_buf, 0, block_size);

  for (int shard = 0; shard < shard_count; shard++)
  {
    const int off = shard * stripe_len;
    double shard_disk_s = 0.0, shard_disk_e = 0.0, shard_net_s = 0.0, shard_net_e = 0.0;
    {
      char tb[320];
      snprintf(tb, sizeof(tb), "chain_head read shard=%d key=%s @ %s:%d off=%d len=%d", shard, head_key.c_str(),
               head_ip.c_str(), head_port, off, stripe_len);
      pipeline_trace(tb);
    }
    if (!GetFromDatanodeStripeRangeBreakdown(head_key, block_buf, block_size, off, stripe_len, head_ip.c_str(),
                                             head_port, &shard_disk_s, &shard_disk_e, &shard_net_s, &shard_net_e,
                                             nullptr, nullptr, nullptr))
    {
      free(block_buf);
      close_pipeline_socket(out_socket);
      const std::string msg = "pipeline chain head local read failed key=" + head_key + " @" + head_ip + ":" +
                              std::to_string(head_port) + " shard=" + std::to_string(shard);
      set_pipeline_error(msg);
      std::cerr << "[Proxy" << m_self_cluster_id << "][gLRC Pipeline] " << msg << std::endl;
      return false;
    }
    update_min(min_disk, shard_disk_s);
    update_max(max_disk, shard_disk_e);
    update_min(min_net, shard_net_s);
    update_max(max_net, shard_net_e);

    std::vector<unsigned char> encoded(static_cast<size_t>(stripe_len), 0);
    glrc_pipeline_init_partial_range(encoded.data(), reinterpret_cast<unsigned char *>(block_buf + off), head_coef,
                                     stripe_len, eq_codec);
    if (!send_pipeline_shard(out_socket, eq_slot, shard, stripe_len, encoded.data(), nullptr, ec))
    {
      free(block_buf);
      close_pipeline_socket(out_socket);
      set_pipeline_error("pipeline chain head send failed: " + ec.message());
      return false;
    }
    {
      char tb[128];
      snprintf(tb, sizeof(tb), "chain_head sent shard=%d chain=%d", shard, chain_id);
      pipeline_trace(tb);
    }
  }
  free(block_buf);

  if (!send_pipeline_stream_end(out_socket, eq_slot, shard_count, nullptr, ec))
  {
    close_pipeline_socket(out_socket);
    set_pipeline_error("pipeline chain head stream end failed: " + ec.message());
    return false;
  }
  close_pipeline_socket(out_socket);

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
  int shard_count = recovery_request->pipeline_shard_count() > 0 ? recovery_request->pipeline_shard_count()
                                                                 : m_sys_config->GlrcShardCount;
  const int stripe_len = block_size / shard_count;
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
  asio::io_context io;
  asio::error_code ec;
  asio::ip::tcp::acceptor acceptor(io);
  if (!open_pipeline_acceptor(io, listen_port, acceptor, ec))
  {
    set_pipeline_error("pipeline hop bind failed port=" + std::to_string(listen_port) + " " + ec.message());
    return false;
  }
  struct PipelineAcceptorGuard
  {
    asio::ip::tcp::acceptor *acceptor = nullptr;
    ~PipelineAcceptorGuard()
    {
      if (acceptor)
      {
        asio::error_code close_ec;
        acceptor->close(close_ec);
      }
    }
  } acceptor_guard{&acceptor};

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

  asio::ip::tcp::socket out_socket(io);
  if (connects_to_hub)
  {
    const std::string hub_ip = recovery_request->pipeline_hub_proxy_ip();
    const int hub_port =
        resolve_chain_hub_listen_port(recovery_request, recovery_request->pipeline_hub_proxy_port(), epoch, eq_slot);
    if (!connect_pipeline_socket(out_socket, hub_ip, hub_port, ec, 120))
      return false;
    set_pipeline_socket_timeouts(out_socket, kPipelineSocketTimeoutSec);
  }
  else
  {
    const std::string next_ip = recovery_request->pipeline_hop_proxy_ips(my_idx + 1);
    const int next_grpc_port = recovery_request->pipeline_hop_proxy_ports(my_idx + 1);
    const int next_port = resolve_hop_listen_port(recovery_request, my_idx + 1, next_grpc_port, epoch, chain_id);
    if (!connect_pipeline_socket(out_socket, next_ip, next_port, ec, 120))
      return false;
    set_pipeline_socket_timeouts(out_socket, kPipelineSocketTimeoutSec);
  }
  std::cout << "[Proxy" << m_self_cluster_id << "][gLRC Pipeline] hop_server downstream ready chain=" << chain_id
            << " hop=" << my_idx << std::endl;

  asio::ip::tcp::socket in_socket(io);
  if (!accept_pipeline_socket(acceptor, in_socket, ec, 45))
  {
    close_pipeline_socket(out_socket);
    set_pipeline_error("pipeline hop accept failed: " + ec.message());
    return false;
  }
  set_pipeline_socket_timeouts(in_socket, kPipelineSocketTimeoutSec);
  set_pipeline_socket_timeouts(out_socket, kPipelineSocketTimeoutSec);
  std::cout << "[Proxy" << m_self_cluster_id << "][gLRC Pipeline] hop_server accepted upstream chain=" << chain_id
            << " hop=" << my_idx << std::endl;

  char *block_buf = static_cast<char *>(std::aligned_alloc(32, block_size));
  if (block_buf == nullptr)
  {
    close_pipeline_socket(in_socket);
    close_pipeline_socket(out_socket);
    set_pipeline_error("pipeline hop alloc failed");
    return false;
  }
  std::memset(block_buf, 0, block_size);

  const std::string hop_key = recovery_request->pipeline_hop_block_keys(my_idx);
  const std::string hop_ip = recovery_request->pipeline_hop_datanode_ips(my_idx);
  const int hop_port = recovery_request->pipeline_hop_datanode_ports(my_idx);

  int shards_forwarded = 0;
  while (true)
  {
    PipelineShardHeader hdr{};
    PipelineStreamEndHeader end_hdr{};
    std::vector<unsigned char> partial;
    const PipelineFrameType frame = recv_pipeline_frame(in_socket, hdr, end_hdr, partial, nullptr, ec);
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
      if (!send_pipeline_stream_end(out_socket, eq_slot, shard_count, nullptr, ec))
      {
        close_pipeline_socket(in_socket);
        close_pipeline_socket(out_socket);
        set_pipeline_error("pipeline hop stream end forward failed");
        return false;
      }
      break;
    }

    const int shard = static_cast<int>(hdr.shard_id);
    if (shard < 0 || shard >= shard_count || (int)partial.size() != stripe_len)
    {
      close_pipeline_socket(in_socket);
      close_pipeline_socket(out_socket);
      set_pipeline_error("pipeline hop shard mismatch");
      return false;
    }
    const int off = shard * stripe_len;

    double ds = 0.0, de = 0.0, ns = 0.0, ne = 0.0;
    if (!GetFromDatanodeStripeRangeBreakdown(hop_key, block_buf, block_size, off, stripe_len, hop_ip.c_str(),
                                             hop_port, &ds, &de, &ns, &ne, nullptr, nullptr, nullptr))
    {
      free(block_buf);
      close_pipeline_socket(in_socket);
      close_pipeline_socket(out_socket);
      set_pipeline_error("pipeline hop local read failed hop=" + std::to_string(my_idx) + " shard=" +
                         std::to_string(shard));
      return false;
    }
    update_min(min_disk, ds);
    update_max(max_disk, de);
    update_min(min_net, ns);
    update_max(max_net, ne);

    glrc_pipeline_accumulate_range(partial.data(), reinterpret_cast<unsigned char *>(block_buf + off), my_coef,
                                   stripe_len, eq_codec);

    if (!send_pipeline_shard(out_socket, eq_slot, shard, stripe_len, partial.data(), nullptr, ec))
    {
      free(block_buf);
      close_pipeline_socket(in_socket);
      close_pipeline_socket(out_socket);
      set_pipeline_error("pipeline hop forward failed: " + ec.message());
      return false;
    }
    shards_forwarded++;
  }

  free(block_buf);
  close_pipeline_socket(in_socket);
  close_pipeline_socket(out_socket);

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
  try
  {
  const int hops_n = hop_count(recovery_request);
  if (hops_n <= 0)
  {
    set_pipeline_error("pipeline local_direct missing hops");
    return false;
  }

  const int block_size = m_sys_config->BlockSize;
  int shard_count = recovery_request->pipeline_shard_count() > 0 ? recovery_request->pipeline_shard_count()
                                                                 : m_sys_config->GlrcShardCount;
  const int stripe_len = block_size / shard_count;
  const int chain_id = recovery_request->pipeline_chain_id();
  const int epoch = recovery_request->pipeline_exchange_epoch();
  const int tail_idx = hops_n - 1;
  const unsigned char tail_coef =
      recovery_request->pipeline_hop_coefs_size() > tail_idx
          ? static_cast<unsigned char>(recovery_request->pipeline_hop_coefs(tail_idx) & 0xff)
          : 1;
  const GlrcPipelineEqCodec eq_codec = pipeline_codec_from_request(recovery_request);
  std::cout << "[Proxy" << m_self_cluster_id << "][gLRC Pipeline] local_direct start chain="
            << recovery_request->pipeline_chain_id() << " hops=" << hops_n
            << (eq_codec == GlrcPipelineEqCodec::LOCAL_XOR ? " xor" : " cauchy") << std::endl;
  pipeline_trace("local_direct begin");

  char *block_buf = static_cast<char *>(std::aligned_alloc(32, block_size));
  if (block_buf == nullptr)
  {
    set_pipeline_error("pipeline local_direct alloc failed");
    return false;
  }
  std::memset(block_buf, 0, block_size);

  const int failed_id = recovery_request->pipeline_local_failed_block_id();
  const char *failed_key = recovery_request->pipeline_local_failed_block_key().c_str();
  const char *rep_ip = recovery_request->pipeline_local_replaced_node_ip().c_str();
  const int rep_port = recovery_request->pipeline_local_replaced_node_port();

  double min_disk = 0.0, max_disk = 0.0, min_net = 0.0, max_net = 0.0;
  double total_wnet = 0.0, total_wdisk = 0.0;
  auto update_min = [](double &slot, double v) {
    if (v > 0.0 && (slot == 0.0 || v < slot))
      slot = v;
  };
  auto update_max = [](double &slot, double v) {
    if (v > slot)
      slot = v;
  };

  std::vector<unsigned char> stripe(static_cast<size_t>(stripe_len), 0);

  for (int shard = 0; shard < shard_count; shard++)
  {
    const int off = shard * stripe_len;
    std::fill(stripe.begin(), stripe.end(), 0);

    if (hops_n == 1)
    {
      const std::string &head_key = recovery_request->pipeline_hop_block_keys(0);
      const std::string &head_ip = recovery_request->pipeline_hop_datanode_ips(0);
      const int head_port = recovery_request->pipeline_hop_datanode_ports(0);
      double ds = 0.0, de = 0.0, ns = 0.0, ne = 0.0;
      if (!GetFromDatanodeStripeRangeBreakdown(head_key, block_buf, block_size, off, stripe_len, head_ip.c_str(),
                                               head_port, &ds, &de, &ns, &ne, nullptr, nullptr, nullptr))
      {
        free(block_buf);
        set_pipeline_error("pipeline local_direct single-hop read failed shard=" + std::to_string(shard));
        return false;
      }
      update_min(min_disk, ds);
      update_max(max_disk, de);
      update_min(min_net, ns);
      update_max(max_net, ne);
      glrc_pipeline_accumulate_range(stripe.data(), reinterpret_cast<unsigned char *>(block_buf + off), tail_coef,
                                     stripe_len, eq_codec);
    }
    else
    {
      for (int hi = 0; hi < hops_n; hi++)
      {
        const unsigned char coef =
            recovery_request->pipeline_hop_coefs_size() > hi
                ? static_cast<unsigned char>(recovery_request->pipeline_hop_coefs(hi) & 0xff)
                : 1;
        const std::string &hop_key = recovery_request->pipeline_hop_block_keys(hi);
        const std::string &hop_ip = recovery_request->pipeline_hop_datanode_ips(hi);
        const int hop_port = recovery_request->pipeline_hop_datanode_ports(hi);
        double ds = 0.0, de = 0.0, ns = 0.0, ne = 0.0;
        if (!GetFromDatanodeStripeRangeBreakdown(hop_key, block_buf, block_size, off, stripe_len, hop_ip.c_str(),
                                                 hop_port, &ds, &de, &ns, &ne, nullptr, nullptr, nullptr))
        {
          free(block_buf);
          set_pipeline_error("pipeline local_direct read failed hop=" + std::to_string(hi) + " shard=" +
                             std::to_string(shard));
          return false;
        }
        update_min(min_disk, ds);
        update_max(max_disk, de);
        update_min(min_net, ns);
        update_max(max_net, ne);
        if (hi == 0)
          glrc_pipeline_init_partial_range(stripe.data(), reinterpret_cast<unsigned char *>(block_buf + off), coef,
                                           stripe_len, eq_codec);
        else
          glrc_pipeline_accumulate_range(stripe.data(), reinterpret_cast<unsigned char *>(block_buf + off), coef,
                                         stripe_len, eq_codec);
      }
    }

    double wnet = 0.0, wdisk = 0.0;
    if (!RecoveryToDatanodeStripeBreakdown(failed_key, failed_id, reinterpret_cast<char *>(stripe.data()), rep_ip,
                                           rep_port, off, stripe_len, &wnet, &wdisk))
    {
      free(block_buf);
      set_pipeline_error("pipeline local_direct stripe write-back failed shard=" + std::to_string(shard));
      pipeline_trace("local_direct write-back failed");
      return false;
    }
    total_wnet += wnet;
    total_wdisk += wdisk;
    char tb[128];
    snprintf(tb, sizeof(tb), "local_direct wrote shard=%d/%d", shard, shard_count);
    pipeline_trace(tb);
  }
  free(block_buf);

  pipeline_trace("local_direct write-back ok");

  response->set_disk_io_start_time(min_disk);
  response->set_disk_io_end_time(max_disk);
  response->set_network_start_time(min_net);
  response->set_network_end_time(max_net);
  response->set_dest_data_node_network_time(total_wnet);
  response->set_dest_data_node_disk_io_time(total_wdisk);
  std::cout << "[Proxy" << m_self_cluster_id << "][gLRC Pipeline] local_direct chain=" << chain_id
            << " failed=" << recovery_request->pipeline_local_failed_block_id() << std::endl;
  return true;
  }
  catch (const std::exception &e)
  {
    set_pipeline_error(std::string("pipeline local_direct exception: ") + e.what());
    return false;
  }
}

bool ProxyImpl::glrcIlpPipelineHubRecovery(const proxy_proto::RecoveryRequest *recovery_request,
                                         proxy_proto::RecoveryReply *response)
{
  const int f = recovery_request->failed_block_ids_size();
  const int block_size = m_sys_config->BlockSize;
  int shard_count = recovery_request->pipeline_shard_count() > 0 ? recovery_request->pipeline_shard_count()
                                                                 : m_sys_config->GlrcShardCount;
  if (f <= 0 || shard_count <= 0 || block_size % shard_count != 0)
  {
    set_pipeline_error("invalid pipeline hub geometry");
    return false;
  }

  const int stripe_len = block_size / shard_count;
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
  std::vector<std::thread> listeners;
  for (int ci = 0; ci < hub_chain_n; ci++)
  {
    const int eq_slot = ctxs[ci].eq_slot;
    const int listen_port = resolve_hub_listener_port(recovery_request, ci, m_port, epoch, eq_slot);
    listeners.emplace_back([this, ci, eq_slot, listen_port, stripe_len, shard_count, block_size, hub_key, hub_dn_ip,
                            hub_dn_port, &ctxs, &rhs_bufs, &rhs_ready, &rhs_mutex, &rhs_cv, &hub_failed,
                            &hub_fail_msg]() {
      try
      {
        asio::io_context io;
        asio::error_code ec;
        if (hub_failed.load())
          return;
        if (ctxs[ci].local_only)
        {
          char *block_buf = static_cast<char *>(std::aligned_alloc(32, block_size));
          if (block_buf == nullptr)
          {
            hub_fail_msg = "hub local_only alloc failed";
            hub_failed.store(true);
            rhs_cv.notify_all();
            return;
          }
          std::memset(block_buf, 0, block_size);
          for (int shard = 0; shard < shard_count && !hub_failed.load(); shard++)
          {
            const int off = shard * stripe_len;
            std::vector<unsigned char> partial(stripe_len, 0);
            double ds = 0.0, de = 0.0, ns = 0.0, ne = 0.0;
            if (!GetFromDatanodeStripeRangeBreakdown(hub_key, block_buf, block_size, off, stripe_len,
                                                     hub_dn_ip.c_str(), hub_dn_port, &ds, &de, &ns, &ne, nullptr,
                                                     nullptr, nullptr))
            {
              free(block_buf);
              hub_fail_msg = "hub local_only read failed";
              hub_failed.store(true);
              rhs_cv.notify_all();
              return;
            }
            glrc_pipeline_init_partial_range(partial.data(), reinterpret_cast<unsigned char *>(block_buf + off),
                                             ctxs[ci].hub_coef, stripe_len, ctxs[ci].eq_codec);
            {
              std::lock_guard<std::mutex> lock(rhs_mutex);
              rhs_bufs[ci][shard] = std::move(partial);
              rhs_ready[ci][shard] = true;
            }
            rhs_cv.notify_all();
          }
          free(block_buf);
          return;
        }

        char *block_buf = static_cast<char *>(std::aligned_alloc(32, block_size));
        if (block_buf == nullptr)
        {
          hub_fail_msg = "hub listener alloc failed";
          hub_failed.store(true);
          rhs_cv.notify_all();
          return;
        }
        std::memset(block_buf, 0, block_size);
        struct BlockBufGuard
        {
          char *p = nullptr;
          ~BlockBufGuard()
          {
            if (p)
              free(p);
          }
        } block_buf_guard{block_buf};
        if (hub_failed.load())
          return;
        asio::ip::tcp::acceptor acceptor(io);
        if (!open_pipeline_acceptor(io, listen_port, acceptor, ec))
        {
          hub_fail_msg = "hub bind failed port=" + std::to_string(listen_port) + " " + ec.message();
          hub_failed.store(true);
          rhs_cv.notify_all();
          return;
        }
        struct PipelineAcceptorGuard
        {
          asio::ip::tcp::acceptor *acceptor = nullptr;
          ~PipelineAcceptorGuard()
          {
            if (acceptor)
            {
              asio::error_code close_ec;
              acceptor->close(close_ec);
            }
          }
        } acceptor_guard{&acceptor};
        asio::ip::tcp::socket in_socket(io);
        if (!accept_pipeline_socket(acceptor, in_socket, ec, 45, &hub_failed))
        {
          hub_fail_msg = "hub accept failed: " + ec.message();
          hub_failed.store(true);
          rhs_cv.notify_all();
          return;
        }
        set_pipeline_socket_timeouts(in_socket, kPipelineSocketTimeoutSec);

        int shards_received = 0;
        while (!hub_failed.load())
        {
          PipelineShardHeader hdr{};
          PipelineStreamEndHeader end_hdr{};
          std::vector<unsigned char> partial;
          const PipelineFrameType frame = recv_pipeline_frame(in_socket, hdr, end_hdr, partial, nullptr, ec);
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
          if (shard < 0 || shard >= shard_count || (int)partial.size() != stripe_len)
          {
            hub_fail_msg = "hub shard mismatch";
            hub_failed.store(true);
            rhs_cv.notify_all();
            close_pipeline_socket(in_socket);
            return;
          }
          const int off = shard * stripe_len;

          if (ctxs[ci].hub_is_tail && ctxs[ci].hub_coef != 0)
          {
            double ds = 0.0, de = 0.0, ns = 0.0, ne = 0.0;
            if (!GetFromDatanodeStripeRangeBreakdown(hub_key, block_buf, block_size, off, stripe_len,
                                                     hub_dn_ip.c_str(), hub_dn_port, &ds, &de, &ns, &ne, nullptr,
                                                     nullptr, nullptr))
            {
              hub_fail_msg = "hub tail local read failed";
              hub_failed.store(true);
              rhs_cv.notify_all();
              close_pipeline_socket(in_socket);
              return;
            }
            glrc_pipeline_accumulate_range(partial.data(), reinterpret_cast<unsigned char *>(block_buf + off),
                                           ctxs[ci].hub_coef, stripe_len, ctxs[ci].eq_codec);
          }

          {
            std::lock_guard<std::mutex> lock(rhs_mutex);
            rhs_bufs[ci][shard] = std::move(partial);
            rhs_ready[ci][shard] = true;
          }
          rhs_cv.notify_all();
          shards_received++;
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
  double total_write_net = 0.0;
  double total_write_disk = 0.0;
  for (int shard = 0; shard < shard_count; shard++)
  {
    const int off = shard * stripe_len;
    std::unique_lock<std::mutex> lock(rhs_mutex);
    rhs_cv.wait(lock, [&]() {
      if (hub_failed.load())
        return true;
      for (int ci = 0; ci < hub_chain_n; ci++)
        if (!rhs_ready[ci][shard])
          return false;
      return true;
    });
    if (hub_failed.load())
      break;

    std::vector<unsigned char *> rhs_ptrs(hub_chain_n, nullptr);
    for (int ci = 0; ci < hub_chain_n; ci++)
    {
      rhs_ptrs[ci] = new unsigned char[block_size];
      std::memset(rhs_ptrs[ci], 0, block_size);
      std::memcpy(rhs_ptrs[ci] + off, rhs_bufs[ci][shard].data(), stripe_len);
    }

    std::vector<unsigned char *> shard_recovered;
    const auto decode_begin = std::chrono::high_resolution_clock::now();
    const bool decode_ok = decode_glrc_ilp_rhs_range(m_sys_config->k, m_sys_config->r, m_sys_config->z, block_size,
                                                     rhs_ptrs.data(), failed_ids, eq_indices, off, stripe_len,
                                                     shard_recovered);
    const auto decode_end = std::chrono::high_resolution_clock::now();
    for (unsigned char *p : rhs_ptrs)
      delete[] p;

    if (!decode_ok)
    {
      hub_fail_msg = "hub decode_glrc_ilp_rhs_range failed";
      hub_failed.store(true);
      for (unsigned char *p : shard_recovered)
        delete[] p;
      break;
    }

    const double decode_begin_ts =
        std::chrono::duration_cast<std::chrono::duration<double>>(decode_begin.time_since_epoch()).count();
    const double decode_end_ts =
        std::chrono::duration_cast<std::chrono::duration<double>>(decode_end.time_since_epoch()).count();
    if (shard == 0 || decode_begin_ts < min_decode_start)
      min_decode_start = decode_begin_ts;
    if (decode_end_ts > max_decode_end)
      max_decode_end = decode_end_ts;

    // Release lock while writing stripes so listeners can mark rhs_ready for the next shard.
    lock.unlock();

    std::vector<std::thread> write_threads;
    std::vector<double> wnets(f, 0.0), wdisks(f, 0.0);
    std::atomic<bool> write_failed{false};
    for (int i = 0; i < f; i++)
    {
      write_threads.emplace_back([this, i, &shard_recovered, off, stripe_len, recovery_request, &failed_ids, &wnets,
                                  &wdisks, &write_failed]() {
        double wnet = 0.0, wdisk = 0.0;
        if (!RecoveryToDatanodeStripeBreakdown(recovery_request->failed_block_keys(i).c_str(), failed_ids[i],
                                               reinterpret_cast<char *>(shard_recovered[i] + off),
                                               recovery_request->replaced_node_ips(i).c_str(),
                                               recovery_request->replaced_node_ports(i), off, stripe_len, &wnet, &wdisk))
          write_failed.store(true);
        wnets[i] = wnet;
        wdisks[i] = wdisk;
      });
    }
    for (auto &th : write_threads)
    {
      if (th.joinable())
        th.join();
    }
    for (unsigned char *p : shard_recovered)
      delete[] p;

    if (write_failed.load())
    {
      hub_fail_msg = "pipeline hub stripe write-back failed shard=" + std::to_string(shard);
      hub_failed.store(true);
      break;
    }
    for (int i = 0; i < f; i++)
    {
      total_write_net += wnets[i];
      total_write_disk += wdisks[i];
    }
    {
      char tb[128];
      snprintf(tb, sizeof(tb), "hub wrote shard=%d/%d", shard, shard_count);
      pipeline_trace(tb);
    }
  }

  for (auto &th : listeners)
  {
    if (th.joinable())
      th.join();
  }

  if (hub_failed.load())
  {
    set_pipeline_error(hub_fail_msg.empty() ? "pipeline hub failed" : hub_fail_msg);
    return false;
  }

  response->set_decode_start_time(min_decode_start);
  response->set_decode_end_time(max_decode_end);
  response->set_dest_data_node_network_time(total_write_net);
  response->set_dest_data_node_disk_io_time(total_write_disk);
  std::cout << "[Proxy" << m_self_cluster_id << "][gLRC Pipeline] hub decode success chains=" << hub_chain_n
            << " shards=" << shard_count << " streaming_writeback=1" << std::endl;
  return true;
}

} // namespace ECProject
