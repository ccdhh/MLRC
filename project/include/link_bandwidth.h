#ifndef LINK_BANDWIDTH_H
#define LINK_BANDWIDTH_H

#include <asio.hpp>
#include <algorithm>
#include <chrono>
#include <cstddef>
#include <mutex>
#include <thread>

namespace ECProject
{

inline double node_block_transfer_seconds(size_t nbytes, double node_block_bandwidth_mbps)
{
  if (node_block_bandwidth_mbps <= 0.0 || nbytes == 0)
    return 0.0;
  return static_cast<double>(nbytes) / (node_block_bandwidth_mbps * 1024.0 * 1024.0);
}

inline void sleep_for_bandwidth_remainder(double expected_sec, double elapsed_sec)
{
  if (expected_sec > elapsed_sec)
    std::this_thread::sleep_for(std::chrono::duration<double>(expected_sec - elapsed_sec));
}

/**
 * Node NIC pacing:
 * - Primary (TX/egress): wait_for_transfer BEFORE write — fair-shares outbound flows.
 * - DrainThenAccount (RX/ingress): read/drain FIRST, then join the ingress timeline with
 *   I/O-time credit so a lone flow paced by the sender is not charged 2×, while multiple
 *   inbound flows still serialize on one node RX budget.
 *
 * Never wait_for_transfer before TCP read on pipelines: that stops draining and causes
 * upstream close / EOF under multi-hop dual-end pacing.
 */
enum class SharedBandwidthPace
{
  Primary,
  DrainThenAccount
};

class SharedBandwidthLimiter
{
public:
  explicit SharedBandwidthLimiter(double bandwidth_mbps)
      : bandwidth_mbps_(bandwidth_mbps), next_slot_(std::chrono::steady_clock::now()) {}

  void reset()
  {
    std::lock_guard<std::mutex> lock(mu_);
    next_slot_ = std::chrono::steady_clock::now();
  }

  /** Reserve nbytes and sleep until this transfer may start (TX path). */
  void wait_for_transfer(size_t nbytes)
  {
    if (bandwidth_mbps_ <= 0.0 || nbytes == 0)
      return;
    const double sec = node_block_transfer_seconds(nbytes, bandwidth_mbps_);
    const auto duration = std::chrono::duration<double>(sec);
    std::chrono::steady_clock::time_point start;
    {
      std::lock_guard<std::mutex> lock(mu_);
      const auto now = std::chrono::steady_clock::now();
      // Cancelled/hung trials can leave next_slot_ minutes ahead; snap back.
      if (next_slot_ > now + std::chrono::seconds(30))
        next_slot_ = now;
      start = (next_slot_ > now) ? next_slot_ : now;
      next_slot_ = start + std::chrono::duration_cast<std::chrono::steady_clock::duration>(duration);
    }
    const auto now = std::chrono::steady_clock::now();
    if (start > now)
      std::this_thread::sleep_until(start);
  }

  /**
   * RX path after TCP drain: join the shared ingress timeline.
   * Credits io_elapsed so a flow already paced by the sender's egress is not charged
   * a second full ideal interval; concurrent inbound flows still serialize on next_slot_.
   */
  void account_after_io(size_t nbytes, double io_elapsed_sec)
  {
    if (bandwidth_mbps_ <= 0.0 || nbytes == 0)
      return;
    const double ideal = node_block_transfer_seconds(nbytes, bandwidth_mbps_);
    const double credit = std::min(std::max(0.0, io_elapsed_sec), ideal);
    double sleep_sec = 0.0;
    {
      std::lock_guard<std::mutex> lock(mu_);
      const auto now = std::chrono::steady_clock::now();
      if (next_slot_ > now + std::chrono::seconds(30))
        next_slot_ = now;
      const auto start = (next_slot_ > now) ? next_slot_ : now;
      next_slot_ = start + std::chrono::duration_cast<std::chrono::steady_clock::duration>(
                               std::chrono::duration<double>(ideal));
      // Time until our fair-share slot ends, minus credit for time already spent in read().
      const double slot_end_from_now =
          std::chrono::duration<double>(next_slot_ - now).count();
      sleep_sec = std::max(0.0, slot_end_from_now - credit);
    }
    if (sleep_sec > 0.0)
      std::this_thread::sleep_for(std::chrono::duration<double>(sleep_sec));
  }

  double bandwidth_mbps() const { return bandwidth_mbps_; }

private:
  double bandwidth_mbps_;
  std::mutex mu_;
  std::chrono::steady_clock::time_point next_slot_;
};

inline void tcp_write_with_node_bandwidth(asio::ip::tcp::socket &socket, const char *buf, size_t len,
                                          double node_block_bandwidth_mbps, asio::error_code &ec)
{
  ec.clear();
  if (node_block_bandwidth_mbps <= 0.0)
  {
    asio::write(socket, asio::buffer(buf, len), ec);
    return;
  }
  const size_t chunk_size = 64 * 1024;
  size_t offset = 0;
  while (offset < len && !ec)
  {
    const size_t n = std::min(chunk_size, len - offset);
    const auto t0 = std::chrono::steady_clock::now();
    asio::write(socket, asio::buffer(buf + offset, n), ec);
    const double elapsed = std::chrono::duration<double>(std::chrono::steady_clock::now() - t0).count();
    sleep_for_bandwidth_remainder(node_block_transfer_seconds(n, node_block_bandwidth_mbps), elapsed);
    offset += n;
  }
}

inline void tcp_read_with_node_bandwidth(asio::ip::tcp::socket &socket, char *buf, size_t len,
                                         double node_block_bandwidth_mbps, asio::error_code &ec)
{
  ec.clear();
  if (node_block_bandwidth_mbps <= 0.0)
  {
    asio::read(socket, asio::buffer(buf, len), ec);
    return;
  }
  const size_t chunk_size = 64 * 1024;
  size_t offset = 0;
  while (offset < len && !ec)
  {
    const size_t n = std::min(chunk_size, len - offset);
    const auto t0 = std::chrono::steady_clock::now();
    asio::read(socket, asio::buffer(buf + offset, n), ec);
    const double elapsed = std::chrono::duration<double>(std::chrono::steady_clock::now() - t0).count();
    sleep_for_bandwidth_remainder(node_block_transfer_seconds(n, node_block_bandwidth_mbps), elapsed);
    offset += n;
  }
}

inline void tcp_write_with_shared_bandwidth(asio::ip::tcp::socket &socket, const char *buf, size_t len,
                                            SharedBandwidthLimiter *limiter, asio::error_code &ec,
                                            SharedBandwidthPace pace = SharedBandwidthPace::Primary)
{
  ec.clear();
  if (limiter == nullptr || limiter->bandwidth_mbps() <= 0.0)
  {
    asio::write(socket, asio::buffer(buf, len), ec);
    return;
  }
  const size_t chunk_size = 1024 * 1024;
  size_t offset = 0;
  while (offset < len && !ec)
  {
    const size_t n = std::min(chunk_size, len - offset);
    if (pace == SharedBandwidthPace::Primary)
    {
      limiter->wait_for_transfer(n);
      asio::write(socket, asio::buffer(buf + offset, n), ec);
    }
    else
    {
      const auto t0 = std::chrono::steady_clock::now();
      asio::write(socket, asio::buffer(buf + offset, n), ec);
      const double elapsed = std::chrono::duration<double>(std::chrono::steady_clock::now() - t0).count();
      if (!ec)
        limiter->account_after_io(n, elapsed);
    }
    offset += n;
  }
}

inline void tcp_read_with_shared_bandwidth(asio::ip::tcp::socket &socket, char *buf, size_t len,
                                           SharedBandwidthLimiter *limiter, asio::error_code &ec,
                                           SharedBandwidthPace pace = SharedBandwidthPace::Primary)
{
  ec.clear();
  if (limiter == nullptr || limiter->bandwidth_mbps() <= 0.0)
  {
    asio::read(socket, asio::buffer(buf, len), ec);
    return;
  }
  const size_t chunk_size = 1024 * 1024;
  size_t offset = 0;
  while (offset < len && !ec)
  {
    const size_t n = std::min(chunk_size, len - offset);
    if (pace == SharedBandwidthPace::Primary)
    {
      // Prefer DrainThenAccount for TCP reads; Primary-before-read is kept for non-pipeline
      // callers but is unsafe on multi-hop pipelines.
      limiter->wait_for_transfer(n);
      asio::read(socket, asio::buffer(buf + offset, n), ec);
    }
    else
    {
      const auto t0 = std::chrono::steady_clock::now();
      asio::read(socket, asio::buffer(buf + offset, n), ec);
      const double elapsed = std::chrono::duration<double>(std::chrono::steady_clock::now() - t0).count();
      if (!ec)
        limiter->account_after_io(n, elapsed);
    }
    offset += n;
  }
}

} // namespace ECProject

#endif
