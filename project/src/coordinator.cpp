#include "coordinator.h"
#include "glrc_repair_ilp.h"
#include "glrc_pipeline_plan.h"
#include "glrc_shard_plan.h"
#include "config.h"
#include "tinyxml2.h"
#include <random>
#include <unistd.h>
#include "lrc.h"
#include <sys/time.h>
#include <chrono>
#include <thread>
#include <functional>
#include <atomic>
#include <mutex>
#include <netinet/in.h>
#include <sys/socket.h>
#include <unordered_map>
#include <unordered_set>
#include <unordered_set>

namespace {
std::atomic<uint32_t> g_glrc_phase2_exchange_epoch{0};
std::atomic<uint32_t> g_glrc_pipeline_exchange_epoch{0};
std::mutex g_glrc_phase2_recovery_mutex;
std::mutex g_glrc_pipeline_recovery_mutex;

static void pipeline_plan_trace(const char *msg)
{
  FILE *f = fopen("/users/chendh/DdlRT/logs/pipeline_trace.log", "a");
  if (f)
  {
    fprintf(f, "%s\n", msg);
    fclose(f);
  }
}

bool pipeline_port_bindable(int port);

struct GlrcPipelineChainPorts;

class GlrcPipelinePortAllocator
{
public:
  explicit GlrcPipelinePortAllocator(int exchange_epoch) : exchange_epoch_(exchange_epoch) {}

  int allocate_hop_port(const std::string &proxy_ip, int proxy_grpc_port, int chain_id, int hop_index)
  {
    return allocate(proxy_ip, proxy_grpc_port, false, chain_id, hop_index);
  }

  int allocate_hub_port(const std::string &proxy_ip, int proxy_grpc_port, int chain_id)
  {
    (void)chain_id;
    return allocate(proxy_ip, proxy_grpc_port, true, 0, 0);
  }

  static void release_inflight_ports(const std::unordered_map<int, GlrcPipelineChainPorts> &chain_ports,
                                     const std::string &hub_proxy_ip, int hub_proxy_port,
                                     const ECProject::GlrcPipelinePlan &pipeline_plan);
  /** Clear all coordinator-side port reservations (one recovery session ended). */
  static void reset_port_session();

private:
  static std::mutex s_alloc_mutex;
  /** Ports reserved until the recovery that allocated them finishes and cools down. */
  static std::unordered_map<std::string, std::unordered_set<int>> s_inflight_ports;
  static std::unordered_map<std::string, int> s_hop_slot_cursor;
  static std::unordered_map<std::string, int> s_hub_slot_cursor;

  int pipeline_proxy_index(int proxy_grpc_port) const
  {
    const int idx = (proxy_grpc_port - ECProject::PROXY_GRPC_BASE) / ECProject::PROXY_GRPC_STRIDE;
    if (idx < 0 || idx > ECProject::PROXY_GRPC_MAX_INDEX)
      return -1;
    return idx;
  }

  int allocate(const std::string &proxy_ip, int proxy_grpc_port, bool hub_band, int chain_id, int hop_index)
  {
    std::lock_guard<std::mutex> lock(s_alloc_mutex);
    const int proxy_idx = pipeline_proxy_index(proxy_grpc_port);
    if (proxy_idx < 0)
      return -1;
    const std::string proxy_key = proxy_ip + ":" + std::to_string(proxy_grpc_port);
    const int band = ECProject::PROXY_PIPELINE_PER_PROXY_BAND;
    const int half = band / 2;
    const int max_port = ECProject::PROXY_PIPELINE_EXCHANGE_BASE + (proxy_idx + 1) * band - 1;
    int &cursor = hub_band ? s_hub_slot_cursor[proxy_key] : s_hop_slot_cursor[proxy_key];
    for (int attempt = 0; attempt < half; ++attempt)
    {
      const int slot_in_half =
          (exchange_epoch_ * 31 + chain_id * 17 + hop_index * 11 + cursor + attempt) % half;
      const int slot = hub_band ? (half + slot_in_half) : slot_in_half;
      int port = ECProject::PROXY_PIPELINE_EXCHANGE_BASE + proxy_idx * band + slot;
      if (port == 55555)
        port++;
      if (port <= 0 || port > 65535 || port > max_port)
        continue;
      if (s_inflight_ports[proxy_key].count(port) > 0)
        continue;
      if (!pipeline_port_bindable(port))
        continue;
      if (used_ports_[proxy_key].insert(port).second)
      {
        s_inflight_ports[proxy_key].insert(port);
        cursor = (cursor + attempt + 1) % half;
        return port;
      }
    }
    return -1;
  }

  int exchange_epoch_;
  std::unordered_map<std::string, std::unordered_set<int>> used_ports_;
};

std::mutex GlrcPipelinePortAllocator::s_alloc_mutex;
std::unordered_map<std::string, std::unordered_set<int>> GlrcPipelinePortAllocator::s_inflight_ports;
std::unordered_map<std::string, int> GlrcPipelinePortAllocator::s_hop_slot_cursor;
std::unordered_map<std::string, int> GlrcPipelinePortAllocator::s_hub_slot_cursor;

struct GlrcPipelineChainPorts
{
  std::vector<int> hop_listen_ports;
  int hub_listen_port = 0;
};

bool pipeline_port_bindable(int port)
{
  if (port <= 0 || port > 65535)
    return true;
  const int fd = ::socket(AF_INET, SOCK_STREAM, 0);
  if (fd < 0)
    return false;
  int opt = 1;
  ::setsockopt(fd, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt));
  sockaddr_in addr{};
  addr.sin_family = AF_INET;
  addr.sin_addr.s_addr = htonl(INADDR_ANY);
  addr.sin_port = htons(static_cast<uint16_t>(port));
  const bool ok = (::bind(fd, reinterpret_cast<sockaddr *>(&addr), sizeof(addr)) == 0);
  ::close(fd);
  return ok;
}

bool wait_pipeline_ports_released(const std::unordered_map<int, GlrcPipelineChainPorts> &chain_ports,
                                  const ECProject::GlrcPipelinePlan &pipeline_plan, int timeout_ms)
{
  std::unordered_set<int> ports;
  for (const ECProject::GlrcPipelineChainPlan &chain : pipeline_plan.hub_chains)
  {
    const auto cp_it = chain_ports.find(chain.chain_id);
    if (cp_it == chain_ports.end())
      continue;
    for (int port : cp_it->second.hop_listen_ports)
      ports.insert(port);
    if (cp_it->second.hub_listen_port > 0)
      ports.insert(cp_it->second.hub_listen_port);
  }
  if (ports.empty())
    return true;

  const auto deadline = std::chrono::steady_clock::now() + std::chrono::milliseconds(timeout_ms);
  while (std::chrono::steady_clock::now() < deadline)
  {
    bool all_released = true;
    for (int port : ports)
    {
      if (!pipeline_port_bindable(port))
      {
        all_released = false;
        break;
      }
    }
    if (all_released)
      return true;
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
  }

  char tb[160];
  snprintf(tb, sizeof(tb), "pipeline port release wait timed out ports=%zu", ports.size());
  pipeline_plan_trace(tb);
  return false;
}

void GlrcPipelinePortAllocator::release_inflight_ports(
    const std::unordered_map<int, GlrcPipelineChainPorts> &chain_ports, const std::string &hub_proxy_ip,
    int hub_proxy_port, const ECProject::GlrcPipelinePlan &pipeline_plan)
{
  std::lock_guard<std::mutex> lock(s_alloc_mutex);
  for (const ECProject::GlrcPipelineChainPlan &chain : pipeline_plan.hub_chains)
  {
    const auto cp_it = chain_ports.find(chain.chain_id);
    if (cp_it == chain_ports.end())
      continue;
    for (size_t hi = 0; hi < chain.hops.size() && hi < cp_it->second.hop_listen_ports.size(); ++hi)
    {
      const std::string key = chain.hops[hi].proxy_ip + ":" + std::to_string(chain.hops[hi].proxy_port);
      s_inflight_ports[key].erase(cp_it->second.hop_listen_ports[hi]);
    }
    const std::string hub_key = hub_proxy_ip + ":" + std::to_string(hub_proxy_port);
    s_inflight_ports[hub_key].erase(cp_it->second.hub_listen_port);
  }
}

void GlrcPipelinePortAllocator::reset_port_session()
{
  std::lock_guard<std::mutex> lock(s_alloc_mutex);
  s_inflight_ports.clear();
}

void fill_pipeline_chain_fields(proxy_proto::RecoveryRequest &req, const ECProject::GlrcPipelineChainPlan &chain,
                                const ECProject::GlrcPipelinePlan &plan, int shard_count, int z,
                                ECProject::GlrcPipelineRole role, int my_hop_index,
                                const GlrcPipelineChainPorts *chain_ports = nullptr)
{
  req.set_pipeline_role(static_cast<int>(role));
  req.set_pipeline_chain_id(chain.chain_id);
  req.set_pipeline_equation_index(chain.equation_index);
  req.set_pipeline_eq_slot(chain.eq_slot);
  req.set_pipeline_exchange_epoch(plan.exchange_epoch);
  req.set_pipeline_shard_count(shard_count);
  req.set_pipeline_hub_block_id(plan.hub_block_id);
  req.set_pipeline_hub_proxy_ip(plan.hub_proxy_ip);
  req.set_pipeline_hub_proxy_port(plan.hub_proxy_port);
  req.set_pipeline_chain_hub_is_tail_flag(chain.hub_is_chain_tail ? 1 : 0);
  req.set_pipeline_equation_is_local(chain.equation_index < z ? 1 : 0);
  req.set_pipeline_my_hop_index(my_hop_index);
  for (const ECProject::GlrcPipelineHopInfo &hop : chain.hops)
  {
    req.add_pipeline_hop_block_ids(hop.block_id);
    req.add_pipeline_hop_proxy_ips(hop.proxy_ip);
    req.add_pipeline_hop_proxy_ports(hop.proxy_port);
    req.add_pipeline_hop_datanode_ips(hop.datanode_ip);
    req.add_pipeline_hop_datanode_ports(hop.datanode_port);
    req.add_pipeline_hop_block_keys(hop.block_key);
    req.add_pipeline_hop_coefs(hop.coef);
  }
  if (chain_ports != nullptr)
  {
    for (int port : chain_ports->hop_listen_ports)
      req.add_pipeline_hop_listen_ports(port);
    if (chain_ports->hub_listen_port > 0)
      req.set_pipeline_chain_hub_listen_port(chain_ports->hub_listen_port);
    if (role == ECProject::GlrcPipelineRole::HOP_SERVER && my_hop_index >= 0 &&
        my_hop_index < (int)chain_ports->hop_listen_ports.size())
      req.set_pipeline_my_listen_port(chain_ports->hop_listen_ports[my_hop_index]);
  }
  if (chain.local_direct)
  {
    req.set_pipeline_local_failed_block_id(chain.local_direct_failed_block_id);
    req.set_pipeline_local_failed_block_key(chain.local_direct_failed_block_key);
    req.set_pipeline_local_replaced_node_ip(chain.local_direct_replaced_ip);
    req.set_pipeline_local_replaced_node_port(chain.local_direct_replaced_port);
  }
}

std::string proxy_key_from_hop(const ECProject::GlrcPipelineHopInfo &hop)
{
  return hop.proxy_ip + ":" + std::to_string(hop.proxy_port);
}

double breakdown_metric_span(double start, double end)
{
  if (start > 0.0 && end >= start && (end - start) < 600.0)
    return end - start;
  return 0.0;
}
} // namespace

template <typename T>
inline T ceil(T const &A, T const &B)
{
  return T((A + B - 1) / B);
};

template <typename T>
inline std::vector<size_t> argsort(const std::vector<T> &v)
{
  std::vector<size_t> idx(v.size());
  std::iota(idx.begin(), idx.end(), 0);
  std::sort(idx.begin(), idx.end(), [&v](size_t i1, size_t i2)
            { return v[i1] < v[i2]; });
  return idx;
};

inline int rand_num(int range)
{
  std::random_device rd;
  std::mt19937 gen(rd());
  std::uniform_int_distribution<int> dis(0, range - 1);
  int num = dis(gen);
  return num;
};

namespace ECProject
{
  grpc::Status CoordinatorImpl::setParameter(
      grpc::ServerContext *context,
      const coordinator_proto::Parameter *parameter,
      coordinator_proto::RepIfSetParaSuccess *setParameterReply)
  {
    ECSchema system_metadata(parameter->partial_decoding(),
                             (ECProject::EncodeType)parameter->encodetype(),
                             (ECProject::SingleStripePlacementType)parameter->s_stripe_placementtype(),
                             (ECProject::MultiStripesPlacementType)parameter->m_stripe_placementtype(),
                             parameter->k_datablock(),
                             parameter->l_localparityblock(),
                             parameter->g_m_globalparityblock(),
                             parameter->b_datapergroup(),
                             parameter->x_stripepermergegroup());
    m_encode_parameters = system_metadata;
    setParameterReply->set_ifsetparameter(true);
    m_cur_cluster_id = 0;
    m_cur_stripe_id = 0;
    m_object_commit_table.clear();
    m_object_updating_table.clear();
    m_stripe_deleting_table.clear();
    for (auto it = m_cluster_table.begin(); it != m_cluster_table.end(); it++)
    {
      Cluster &t_cluster = it->second;
      t_cluster.blocks.clear();
      t_cluster.stripes.clear();
    }
    for (auto it = m_node_table.begin(); it != m_node_table.end(); it++)
    {
      Node &t_node = it->second;
      t_node.stripes.clear();
    }
    m_stripe_table.clear();
    m_merge_groups.clear();
    m_free_clusters.clear();
    m_agg_start_cid = 0;
    std::cout << "setParameter success" << std::endl;
    return grpc::Status::OK;
  }

  grpc::Status CoordinatorImpl::sayHelloToCoordinator(
      grpc::ServerContext *context,
      const coordinator_proto::RequestToCoordinator *helloRequestToCoordinator,
      coordinator_proto::ReplyFromCoordinator *helloReplyFromCoordinator)
  {
    std::string prefix("Hello ");
    helloReplyFromCoordinator->set_message(prefix + helloRequestToCoordinator->name());
    std::cout << prefix + helloRequestToCoordinator->name() << std::endl;
    return grpc::Status::OK;
  }

  grpc::Status CoordinatorImpl::uploadOriginKeyValue(
      grpc::ServerContext *context,
      const coordinator_proto::RequestProxyIPPort *keyValueSize,
      coordinator_proto::ReplyProxyIPPort *proxyIPPort)
  {

    std::string key = keyValueSize->key();
    m_mutex.lock();
    m_object_commit_table.erase(key);
    m_mutex.unlock();
    int valuesizebytes = keyValueSize->valuesizebytes();

    ObjectInfo new_object;

    int k = m_encode_parameters.k_datablock;
    int g_m = m_encode_parameters.g_m_globalparityblock;
    int l = m_encode_parameters.l_localparityblock;
    // int b = m_encode_parameters.b_datapergroup;
    new_object.object_size = valuesizebytes;
    int block_size = ceil(valuesizebytes, k);

    proxy_proto::ObjectAndPlacement object_placement;
    object_placement.set_key(key);
    object_placement.set_valuesizebyte(valuesizebytes);
    object_placement.set_k(k);
    object_placement.set_g_m(g_m);
    object_placement.set_l(l);
    object_placement.set_encode_type((int)m_encode_parameters.encodetype);
    object_placement.set_block_size(block_size);

    Stripe t_stripe;
    t_stripe.stripe_id = m_cur_stripe_id++;
    t_stripe.k = k;
    t_stripe.l = l;
    t_stripe.g_m = g_m;
    t_stripe.object_keys.push_back(key);
    t_stripe.object_sizes.push_back(valuesizebytes);
    m_stripe_table[t_stripe.stripe_id] = t_stripe;
    new_object.map2stripe = t_stripe.stripe_id;

    int s_cluster_id = generate_placement(t_stripe.stripe_id, block_size);

    Stripe &stripe = m_stripe_table[t_stripe.stripe_id];
    object_placement.set_stripe_id(stripe.stripe_id);
    for (int i = 0; i < int(stripe.blocks.size()); i++)
    {
      object_placement.add_datanodeip(m_node_table[stripe.blocks[i]->map2node].node_ip);
      object_placement.add_datanodeport(m_node_table[stripe.blocks[i]->map2node].node_port);
      object_placement.add_blockkeys(stripe.blocks[i]->block_key);
    }

    grpc::ClientContext cont;
    proxy_proto::SetReply set_reply;
    std::string selected_proxy_ip = m_cluster_table[s_cluster_id].proxy_ip;
    int selected_proxy_port = m_cluster_table[s_cluster_id].proxy_port;
    std::string chosen_proxy = selected_proxy_ip + ":" + std::to_string(selected_proxy_port);
    grpc::Status status = m_proxy_ptrs[chosen_proxy]->encodeAndSetObject(&cont, object_placement, &set_reply);
    proxyIPPort->set_proxyip(selected_proxy_ip);
    proxyIPPort->set_proxyport(selected_proxy_port + ECProject::PROXY_PORT_SHIFT); // use another port to accept data
    if (status.ok())
    {
      m_mutex.lock();
      m_object_updating_table[key] = new_object;
      m_mutex.unlock();
    }
    else
    {
      std::cout << "[SET] Send object placement failed!" << std::endl;
    }

    return grpc::Status::OK;
  }

  void CoordinatorImpl::initialize_optimal_lrc_stripe_placement(Stripe *stripe)
  {
    // range 0~k-1: data blocks
    // range k~k+r-1: global parity blocks
    // range k+r~k+r+z-1: local parity blocks
    Block *blocks_info = new Block[stripe->n];
    // a stripe is only created by a single client
    assert(stripe->object_keys.size() == 1);
    // choose a cluster: round robin
    int t_cluster_id = stripe->stripe_id % m_sys_config->ClusterNum;
    int group_size = stripe->r + 1;
    int local_group_size = int(stripe->k / stripe->z);
    int group_num_of_one_local_group = local_group_size / group_size;
    if (local_group_size % group_size != 0)
      group_num_of_one_local_group++;

    for (int i = 0; i < stripe->n; i++)
    {
      blocks_info[i].block_size = m_sys_config->BlockSize;
      blocks_info[i].map2stripe = stripe->stripe_id;
      blocks_info[i].map2key = stripe->object_keys[0];
      if (i < stripe->k)
      {
        std::string tmp = "_D";
        if (i < 10)
          tmp = "_D0";
        blocks_info[i].block_key = std::to_string(stripe->stripe_id) + tmp + std::to_string(i);
        blocks_info[i].block_id = i;
        blocks_info[i].block_type = 'D';
        blocks_info[i].map2group = (i % local_group_size / group_size) + i / local_group_size * group_num_of_one_local_group;
      }
      else if (i >= stripe->k && i < stripe->k + stripe->r)
      {
        std::string tmp = "_G";
        if (i - stripe->k < 10)
          tmp = "_G0";
        blocks_info[i].block_key = std::to_string(stripe->stripe_id) + tmp + std::to_string(i - stripe->k);
        blocks_info[i].block_id = i;
        blocks_info[i].block_type = 'G';
        blocks_info[i].map2group = stripe->z * group_num_of_one_local_group;
      }
      else
      {
        std::string tmp = "_L";
        if (i - stripe->k - stripe->r < 10)
          tmp = "_L0";
        blocks_info[i].block_key = std::to_string(stripe->stripe_id) + tmp + std::to_string(i - stripe->k - stripe->r);
        blocks_info[i].block_id = i;
        blocks_info[i].block_type = 'L';
        blocks_info[i].map2group = (i - stripe->k - stripe->r + 1) * group_num_of_one_local_group - 1;
      }
      blocks_info[i].map2cluster = (t_cluster_id + blocks_info[i].map2group) % m_sys_config->ClusterNum;
      int t_node_id = randomly_select_a_node(blocks_info[i].map2cluster, stripe->stripe_id);
      blocks_info[i].map2node = t_node_id;
      update_stripe_info_in_node(t_node_id, stripe->stripe_id, i);
      m_cluster_table[blocks_info[i].map2cluster].blocks.push_back(&blocks_info[i]);
      m_cluster_table[blocks_info[i].map2cluster].stripes.insert(stripe->stripe_id);
      stripe->blocks.push_back(&blocks_info[i]);
      stripe->place2clusters.insert(blocks_info[i].map2cluster);
      add_to_map(stripe->group_to_blocks, blocks_info[i].map2group, i);
    }

    stripe->num_groups = stripe->group_to_blocks.size();
  }

  void CoordinatorImpl::initialize_glrc_stripe_placement(Stripe *stripe)
  {
    // Split (k+r) across z groups: groups 0..z-2 each hold q_max=data blocks;
    // group z-1 (global) holds remaining data + all global parities (defective when T%z!=0).
    // Local parity Li: group i.
    Block *blocks_info = new Block[stripe->n];
    assert(stripe->object_keys.size() == 1);
    int t_cluster_id = stripe->stripe_id % m_sys_config->ClusterNum;

    for (int i = 0; i < stripe->n; i++)
    {
      blocks_info[i].block_size = m_sys_config->BlockSize;
      blocks_info[i].map2stripe = stripe->stripe_id;
      blocks_info[i].map2key = stripe->object_keys[0];
      if (i < stripe->k)
      {
        std::string tmp = "_D";
        if (i < 10)
          tmp = "_D0";
        blocks_info[i].block_key =
            std::to_string(stripe->stripe_id) + tmp + std::to_string(i);
        blocks_info[i].block_id = i;
        blocks_info[i].block_type = 'D';
        blocks_info[i].map2group =
            glrc_data_group_id(i, stripe->k, stripe->r, stripe->z);
      }
      else if (i >= stripe->k && i < stripe->k + stripe->r)
      {
        std::string tmp = "_G";
        if (i - stripe->k < 10)
          tmp = "_G0";
        blocks_info[i].block_key = std::to_string(stripe->stripe_id) + tmp +
                                 std::to_string(i - stripe->k);
        blocks_info[i].block_id = i;
        blocks_info[i].block_type = 'G';
        blocks_info[i].map2group = stripe->z - 1;
      }
      else
      {
        int local_idx = i - stripe->k - stripe->r;
        std::string tmp = "_L";
        if (local_idx < 10)
          tmp = "_L0";
        blocks_info[i].block_key = std::to_string(stripe->stripe_id) + tmp +
                                 std::to_string(local_idx);
        blocks_info[i].block_id = i;
        blocks_info[i].block_type = 'L';
        blocks_info[i].map2group = local_idx;
      }
      blocks_info[i].map2cluster =
          (t_cluster_id + blocks_info[i].map2group) % m_sys_config->ClusterNum;
      int t_node_id =
          randomly_select_a_node(blocks_info[i].map2cluster, stripe->stripe_id);
      blocks_info[i].map2node = t_node_id;
      update_stripe_info_in_node(t_node_id, stripe->stripe_id, i);
      m_cluster_table[blocks_info[i].map2cluster].blocks.push_back(&blocks_info[i]);
      m_cluster_table[blocks_info[i].map2cluster].stripes.insert(stripe->stripe_id);
      stripe->blocks.push_back(&blocks_info[i]);
      stripe->place2clusters.insert(blocks_info[i].map2cluster);
      add_to_map(stripe->group_to_blocks, blocks_info[i].map2group, i);
    }

    stripe->num_groups = stripe->z;
  }

  void CoordinatorImpl::initialize_uniform_lrc_stripe_placement(Stripe *stripe)
  {
    // range 0~k-1: data blocks
    // range k~k+r-1: global parity blocks
    // range k+r~k+r+z-1: local parity blocks
    Block *blocks_info = new Block[stripe->n];
    // a stripe is only created by a single client
    assert(stripe->object_keys.size() == 1);
    // choose a cluster: round robin
    int t_cluster_id = stripe->stripe_id % m_sys_config->ClusterNum;

    int group_size = stripe->r + 1;
    int local_group_size = int((stripe->k + stripe->r) / stripe->z);
    int larger_local_group_num = int((stripe->k + stripe->r) % stripe->z);
    int group_num = -1;
    int block_num = 0;

    for (int i = 0; i < stripe->z; i++)
    {
      if (i + larger_local_group_num == stripe->z)
      {
        local_group_size++;
      }
      for (int j = 0; j < local_group_size; j++)
      {
        if (j % group_size == 0)
        {
          group_num++;
        }
        blocks_info[block_num++].map2group = group_num;
      }
      blocks_info[stripe->k + stripe->r + i].map2group = group_num;
    }
    for (int i = 0; i < stripe->n; i++)
    {
      blocks_info[i].block_size = m_sys_config->BlockSize;
      blocks_info[i].map2stripe = stripe->stripe_id;
      blocks_info[i].map2key = stripe->object_keys[0];
      if (i < stripe->k)
      {
        std::string tmp = "_D";
        if (i < 10)
          tmp = "_D0";
        blocks_info[i].block_key = std::to_string(stripe->stripe_id) + tmp + std::to_string(i);
        blocks_info[i].block_id = i;
        blocks_info[i].block_type = 'D';
      }
      else if (i >= stripe->k && i < stripe->k + stripe->r)
      {
        std::string tmp = "_G";
        if (i - stripe->k < 10)
          tmp = "_G0";
        blocks_info[i].block_key = std::to_string(stripe->stripe_id) + tmp + std::to_string(i - stripe->k);
        blocks_info[i].block_id = i;
        blocks_info[i].block_type = 'G';
      }
      else
      {
        std::string tmp = "_L";
        if (i - stripe->k - stripe->r < 10)
          tmp = "_L0";
        blocks_info[i].block_key = std::to_string(stripe->stripe_id) + tmp + std::to_string(i - stripe->k - stripe->r);
        blocks_info[i].block_id = i;
        blocks_info[i].block_type = 'L';
      }
      blocks_info[i].map2cluster = (t_cluster_id + blocks_info[i].map2group) % m_sys_config->ClusterNum;
      int t_node_id = randomly_select_a_node(blocks_info[i].map2cluster, stripe->stripe_id);
      blocks_info[i].map2node = t_node_id;
      update_stripe_info_in_node(t_node_id, stripe->stripe_id, i);
      m_cluster_table[blocks_info[i].map2cluster].blocks.push_back(&blocks_info[i]);
      m_cluster_table[blocks_info[i].map2cluster].stripes.insert(stripe->stripe_id);
      stripe->blocks.push_back(&blocks_info[i]);
      stripe->place2clusters.insert(blocks_info[i].map2cluster);
      add_to_map(stripe->group_to_blocks, blocks_info[i].map2group, i);
    }

    stripe->num_groups = stripe->group_to_blocks.size();
  }

  void CoordinatorImpl::initialize_unilrc_and_azurelrc_stripe_placement(Stripe *stripe)
  {
    std::string code_type = m_sys_config->CodeType;

    // range 0~k-1: data blocks
    // range k~k+r-1: global parity blocks
    // range k+r~k+r+z-1: local parity blocks
    Block *blocks_info = new Block[stripe->n];
    // a stripe is only created by a single client
    assert(stripe->object_keys.size() == 1);
    // choose a cluster: round robin
    int t_cluster_id = stripe->stripe_id % m_sys_config->ClusterNum;
    for (int i = 0; i < stripe->n; i++)
    {
      blocks_info[i].block_size = m_sys_config->BlockSize;
      blocks_info[i].map2stripe = stripe->stripe_id;
      blocks_info[i].map2key = stripe->object_keys[0];
      if (i < stripe->k)
      {
        std::string tmp = "_D";
        if (i < 10)
          tmp = "_D0";
        blocks_info[i].block_key = std::to_string(stripe->stripe_id) + tmp + std::to_string(i);
        blocks_info[i].block_id = i;
        blocks_info[i].block_type = 'D';
        blocks_info[i].map2group = int(i / (stripe->k / stripe->z));
      }
      else if (i >= stripe->k && i < stripe->k + stripe->r)
      {
        std::string tmp = "_G";
        if (i - stripe->k < 10)
          tmp = "_G0";
        blocks_info[i].block_key = std::to_string(stripe->stripe_id) + tmp + std::to_string(i - stripe->k);
        blocks_info[i].block_id = i;
        blocks_info[i].block_type = 'G';
        if (code_type == "UniLRC")
        {
          blocks_info[i].map2group = int((i - stripe->k) / (stripe->r / stripe->z));
        }
        else if (code_type == "AzureLRC")
        {
          blocks_info[i].map2group = int(stripe->z);
        }
      }
      else
      {
        std::string tmp = "_L";
        if (i - stripe->k - stripe->r < 10)
          tmp = "_L0";
        blocks_info[i].block_key = std::to_string(stripe->stripe_id) + tmp + std::to_string(i - stripe->k - stripe->r);
        blocks_info[i].block_id = i;
        blocks_info[i].block_type = 'L';
        blocks_info[i].map2group = int((i - stripe->k - stripe->r) / (stripe->z / stripe->z));
      }
      blocks_info[i].map2cluster = (t_cluster_id + blocks_info[i].map2group) % m_sys_config->ClusterNum;
      int t_node_id = randomly_select_a_node(blocks_info[i].map2cluster, stripe->stripe_id);
      blocks_info[i].map2node = t_node_id;
      update_stripe_info_in_node(t_node_id, stripe->stripe_id, i);
      m_cluster_table[blocks_info[i].map2cluster].blocks.push_back(&blocks_info[i]);
      m_cluster_table[blocks_info[i].map2cluster].stripes.insert(stripe->stripe_id);
      stripe->blocks.push_back(&blocks_info[i]);
      stripe->place2clusters.insert(blocks_info[i].map2cluster);
      add_to_map(stripe->group_to_blocks, blocks_info[i].map2group, i);
    }

    stripe->num_groups = stripe->group_to_blocks.size();
  }

  void CoordinatorImpl::add_to_map(std::map<int, std::vector<int>> &map, int key, int value)
  {
    if (map.find(key) == map.end())
      map[key] = std::vector<int>();
    map[key].push_back(value);
  }

  int CoordinatorImpl::getClusterAppendSize(Stripe *stripe, const std::map<int, std::pair<int, int>> &block_to_slice_sizes, int curr_group_id, int parity_slice_size)
  {
    int cluster_append_size = 0;

    for (int i = curr_group_id * stripe->k / stripe->z; i < (curr_group_id + 1) * stripe->k / stripe->z; i++)
    {
      if (block_to_slice_sizes.find(i) != block_to_slice_sizes.end())
        cluster_append_size += block_to_slice_sizes.at(i).first;
    }

    cluster_append_size += parity_slice_size * (stripe->r + stripe->z) / stripe->z;
    return cluster_append_size;
  }

  // add repeated fields to plan
  void addBlockToAppendPlan(proxy_proto::AppendStripeDataPlacement &plan,
                            const Block *block,
                            const Node &node,
                            const std::pair<int, int> &slice_info)
  {
    plan.add_datanodeip(node.node_ip);
    plan.add_datanodeport(node.node_port);
    plan.add_blockkeys(block->block_key);
    plan.add_blockids(block->block_id);
    plan.add_offsets(slice_info.second);
    plan.add_sizes(slice_info.first);
  }

  std::vector<proxy_proto::AppendStripeDataPlacement> CoordinatorImpl::generateAppendPlan(Stripe *stripe, int curr_logical_offset, int append_size)
  {
    std::vector<proxy_proto::AppendStripeDataPlacement> append_plans;
    std::string append_mode = m_sys_config->AppendMode;
    int unit_size = m_sys_config->UnitSize;
    int remain_size = stripe->k * m_sys_config->BlockSize - curr_logical_offset;
    assert(remain_size >= append_size && "append size is larger than the remaining size of the stripe!");

    // int curr_group_id = (curr_logical_offset / (unit_size * stripe->k / stripe->z)) % stripe->z;
    int curr_block_id = (curr_logical_offset / unit_size) % stripe->k;
    // compute how many units that need to be appended
    int num_units = (curr_logical_offset + append_size - 1) / unit_size - curr_logical_offset / unit_size + 1;
    // int num_data_groups = std::min((curr_logical_offset + append_size - 1) / (unit_size * stripe->k / stripe->z) - curr_logical_offset / (unit_size * stripe->k / stripe->z) + 1, stripe->z);
    int num_unit_stripes = (curr_logical_offset + append_size - 1) / (unit_size * stripe->k) - curr_logical_offset / (unit_size * stripe->k) + 1;

    // compute the size and offset of the parity slice
    // TODO: optimize the append size that below a unit_size but placed into two units within a unit_stripe
    int parity_slice_size = -1;
    int parity_slice_offset = -1;
    switch (append_mode[0])
    {
    case 'R': // REP_MODE
      parity_slice_size = append_size;
      break;
    case 'U': // UNILRC_MODE
      parity_slice_size = num_unit_stripes * unit_size;
      parity_slice_offset = curr_logical_offset / (unit_size * stripe->k) * unit_size;
      if (num_units == 1)
      {
        parity_slice_size = append_size;
        parity_slice_offset += curr_logical_offset % unit_size;
      }
      if (num_unit_stripes > 1 && (curr_logical_offset + append_size - 1) % (unit_size * stripe->k) < unit_size - 1)
      {
        parity_slice_size = (num_unit_stripes - 1) * unit_size + (curr_logical_offset + append_size - 1) % (unit_size * stripe->k) + 1;
      }
      break;
    case 'C': // CACHED_MODE
      parity_slice_size = num_unit_stripes * unit_size;
      parity_slice_offset = curr_logical_offset / (unit_size * stripe->k) * unit_size;
      break;
    default:
      std::cout << "[ERROR] Invalid append mode: " << append_mode << std::endl;
      return append_plans;
    }

    // key: block_id, value: (slice_size, physical_offset)
    std::map<int, std::pair<int, int>> block_to_slice_sizes;
    int tmp_size = append_size;
    int tmp_offset = curr_logical_offset;
    bool is_merge_parity = curr_logical_offset + append_size == m_sys_config->BlockSize * stripe->k;

    // add data slices to block_to_slice_sizes
    while (tmp_size > 0)
    {
      int sub_slice_size = unit_size;
      // first slice
      if (tmp_size == append_size && curr_logical_offset % unit_size != 0)
      {
        sub_slice_size = std::min(unit_size - curr_logical_offset % unit_size, append_size);
      }
      else
      {
        sub_slice_size = std::min(unit_size, tmp_size);
      }
      if (block_to_slice_sizes.find(curr_block_id) == block_to_slice_sizes.end())
      {
        block_to_slice_sizes[curr_block_id].first = sub_slice_size;
        block_to_slice_sizes[curr_block_id].second = tmp_offset % unit_size + unit_size * (tmp_offset / (stripe->k * unit_size));
      }
      else
      {
        block_to_slice_sizes[curr_block_id].first += sub_slice_size;
      }
      curr_block_id = (curr_block_id + 1) % stripe->k;
      tmp_size -= sub_slice_size;
      tmp_offset += sub_slice_size;
    }

    // add parity slices to block_to_slice_sizes
    for (int i = stripe->k; i < stripe->n; i++)
    {
      block_to_slice_sizes[i].first = parity_slice_size;
      block_to_slice_sizes[i].second = parity_slice_offset;
    }

    for (int i = 0; i < stripe->z; i++)
    {
      proxy_proto::AppendStripeDataPlacement plan;
      plan.set_key(m_toolbox->gen_append_key(stripe->stripe_id, i));
      plan.set_stripe_id(stripe->stripe_id);
      plan.set_append_size(getClusterAppendSize(stripe, block_to_slice_sizes, i, parity_slice_size));
      plan.set_is_merge_parity(is_merge_parity);
      plan.set_cluster_id(stripe->blocks[stripe->group_to_blocks[i][0]]->map2cluster);
      plan.set_append_mode(append_mode);
      if (curr_logical_offset == 0 && append_size == m_sys_config->BlockSize * stripe->k)
      {
        plan.set_is_serialized(false);
        plan.set_is_merge_parity(false);
      }
      else
      {
        plan.set_is_serialized(true);
      }

      // Add data slices to plan
      for (int j = i * stripe->k / stripe->z;
           j < (i + 1) * stripe->k / stripe->z; j++)
      {
        if (block_to_slice_sizes.find(j) != block_to_slice_sizes.end())
        {
          addBlockToAppendPlan(plan, stripe->blocks[j],
                               m_node_table[stripe->blocks[j]->map2node],
                               block_to_slice_sizes.at(j));
        }
      }

      // Add global parity slices to plan
      for (int j = stripe->k + i * stripe->r / stripe->z;
           j < stripe->k + (i + 1) * stripe->r / stripe->z; j++)
      {
        addBlockToAppendPlan(plan, stripe->blocks[j],
                             m_node_table[stripe->blocks[j]->map2node],
                             block_to_slice_sizes.at(j));
      }

      // Add local parity slices to plan
      for (int j = stripe->k + stripe->r + i * stripe->z / stripe->z;
           j < stripe->k + stripe->r + (i + 1) * stripe->z / stripe->z; j++)
      {
        addBlockToAppendPlan(plan, stripe->blocks[j],
                             m_node_table[stripe->blocks[j]->map2node],
                             block_to_slice_sizes.at(j));
      }

      append_plans.push_back(plan);
    }

    return append_plans;
  }

  void CoordinatorImpl::notify_proxies_ready(const proxy_proto::AppendStripeDataPlacement &plan)
  {
    grpc::ClientContext cont;
    proxy_proto::SetReply set_reply;
    std::string chosen_proxy = m_cluster_table[plan.cluster_id()].proxy_ip + ":" + std::to_string(m_cluster_table[plan.cluster_id()].proxy_port);
    grpc::Status status = m_proxy_ptrs[chosen_proxy]->scheduleAppend2Datanode(&cont, plan, &set_reply);
    if (status.ok())
    {
      m_mutex.lock();
      m_object_updating_table[plan.key()] = ObjectInfo(plan.append_size(), plan.stripe_id());
      m_mutex.unlock();
    }
    else
    {
      std::cout << "[APPEND434] Send append plan" << plan.key() << " failed! " << std::endl;
    }
  }

  // Only processing the appending within a single stripe
  grpc::Status CoordinatorImpl::uploadAppendValue(
      grpc::ServerContext *context,
      const coordinator_proto::RequestProxyIPPort *keyValueSize,
      coordinator_proto::ReplyProxyIPsPorts *proxyIPPort)
  {
    std::string clientID = keyValueSize->key();
    int appendSizeBytes = keyValueSize->valuesizebytes();
    std::string append_mode = keyValueSize->append_mode();

    // 1. record metadata
    // logical offset within the block stripe
    if (m_cur_offset_table.find(clientID) == m_cur_offset_table.end())
    {
      // first append
      m_cur_offset_table[clientID] = StripeOffset(m_cur_stripe_id++, 0);
    }
    StripeOffset curStripeOffset = m_cur_offset_table[clientID];

    assert(curStripeOffset.offset + appendSizeBytes <= m_sys_config->BlockSize * m_sys_config->k && "append size is larger than the remaining size of the stripe!");

    // 2. generate data placement
    Stripe *stripe = nullptr;
    if (curStripeOffset.offset == 0)
    {
      // first append
      Stripe t_stripe;
      t_stripe.stripe_id = curStripeOffset.stripe_id;
      t_stripe.n = m_sys_config->n;
      t_stripe.k = m_sys_config->k;
      t_stripe.r = m_sys_config->r;
      t_stripe.z = m_sys_config->z;
      t_stripe.object_keys.push_back(clientID);
      initialize_unilrc_and_azurelrc_stripe_placement(&t_stripe);
      m_stripe_table[t_stripe.stripe_id] = t_stripe;
      stripe = &m_stripe_table[t_stripe.stripe_id];
    }
    else
    {
      // append to the existing stripe
      stripe = &m_stripe_table[curStripeOffset.stripe_id];
    }

    std::vector<proxy_proto::AppendStripeDataPlacement> append_plans = generateAppendPlan(stripe, curStripeOffset.offset, appendSizeBytes);
    if (append_plans.empty())
    {
      std::cout << "[ERROR] Invalid append mode: " << append_mode << std::endl;
      return grpc::Status(grpc::StatusCode::INVALID_ARGUMENT, "Invalid append mode");
    }

    for (const auto &plan : append_plans)
    {
      m_mutex.lock();
      m_object_commit_table.erase(plan.key());
      m_mutex.unlock();
    }

    // 3. notify proxies to receive data
    // need multiple proxies to receive data, so need multiple threads
    std::vector<std::thread> threads;
    int sum_append_size = 0;
    for (const auto &plan : append_plans)
    {
      threads.push_back(std::thread(&CoordinatorImpl::notify_proxies_ready, this, plan));
      proxyIPPort->add_append_keys(plan.key());
      proxyIPPort->add_proxyips(m_cluster_table[plan.cluster_id()].proxy_ip);
      proxyIPPort->add_proxyports(m_cluster_table[plan.cluster_id()].proxy_port + ECProject::PROXY_PORT_SHIFT); // use another port to accept data
      proxyIPPort->add_cluster_slice_sizes(plan.append_size());
      sum_append_size += plan.append_size();
    }
    for (auto &thread : threads)
    {
      thread.join();
    }
    proxyIPPort->set_sum_append_size(sum_append_size);

    m_cur_offset_table[clientID].offset += appendSizeBytes;
    // std::cout << "[Coordinator] stripe_id: " << m_cur_offset_table[clientID].stripe_id << " offset: " << m_cur_offset_table[clientID].offset << " is_erase " << (m_cur_offset_table[clientID].offset == m_sys_config->BlockSize * m_sys_config->k) << std::endl;
    if (m_cur_offset_table[clientID].offset == m_sys_config->BlockSize * m_sys_config->k)
    {
      m_cur_offset_table.erase(clientID);
    }

    return grpc::Status::OK;
  }

  std::vector<proxy_proto::AppendStripeDataPlacement> CoordinatorImpl::generate_add_plans(Stripe *stripe)
  {
    std::vector<proxy_proto::AppendStripeDataPlacement> add_plans;
    for (int i = 0; i < stripe->num_groups; i++)
    {
      proxy_proto::AppendStripeDataPlacement plan;
      int mapped_cluster_id = stripe->blocks[stripe->group_to_blocks[i][0]]->map2cluster;
      size_t append_size = stripe->group_to_blocks[i].size() * m_sys_config->BlockSize;

      plan.set_key(m_toolbox->gen_append_key(stripe->stripe_id, i));
      plan.set_stripe_id(stripe->stripe_id);
      plan.set_append_size(append_size);
      plan.set_is_merge_parity(false);
      plan.set_cluster_id(mapped_cluster_id);
      plan.set_append_mode("UNILRC_MODE");
      plan.set_is_serialized(false);

      for (int j = 0; j < stripe->group_to_blocks[i].size(); j++)
      {
        addBlockToAppendPlan(plan, stripe->blocks[stripe->group_to_blocks[i][j]], m_node_table[stripe->blocks[stripe->group_to_blocks[i][j]]->map2node], std::make_pair(m_sys_config->BlockSize, 0));
      }

      add_plans.push_back(plan);
    }

    return add_plans;
  }

  std::vector<proxy_proto::AppendStripeDataPlacement> CoordinatorImpl::generate_sub_add_plans(Stripe *stripe, size_t subset_size)
  {
    int data_block_num = subset_size / m_sys_config->BlockSize;
    int k = m_sys_config->k;
    int r = m_sys_config->r;
    int z = m_sys_config->z;
    std::vector<proxy_proto::AppendStripeDataPlacement> add_plans;
    for (int i = 0; i < stripe->num_groups; i++)
    {
      proxy_proto::AppendStripeDataPlacement plan;
      int block_num = 0;
      for (int j = 0; j < stripe->group_to_blocks[i].size(); j++)
      {
        int block_id = stripe->group_to_blocks[i][j];
        if(block_id < k && block_id >= data_block_num)
        {
          continue;
        }
        addBlockToAppendPlan(plan, stripe->blocks[stripe->group_to_blocks[i][j]], m_node_table[stripe->blocks[stripe->group_to_blocks[i][j]]->map2node], std::make_pair(m_sys_config->BlockSize, 0));
        block_num++;
      }

      size_t append_size = block_num * m_sys_config->BlockSize;
      if(append_size == 0)
      {
        //plan.set_append_size(0);
        //add_plans.push_back(plan);
        continue; // no data to append
      }

      int mapped_cluster_id = stripe->blocks[stripe->group_to_blocks[i][0]]->map2cluster;

      plan.set_key(m_toolbox->gen_append_key(stripe->stripe_id, i));
      plan.set_stripe_id(stripe->stripe_id);
      plan.set_is_merge_parity(false);
      plan.set_cluster_id(mapped_cluster_id);
      plan.set_append_mode("UNILRC_MODE");
      plan.set_is_serialized(false);
      plan.set_append_size(append_size);

      add_plans.push_back(plan);
    }

    return add_plans;
  }

  void CoordinatorImpl::print_stripe_data_placement(Stripe &stripe)
  {
    std::cout << "Stripe " << stripe.stripe_id << " data placement: " << std::endl;
    for (int i = 0; i < stripe.num_groups; i++)
    {
      std::cout << "Group " << i << ": (" << stripe.group_to_blocks[i].size() << " blocks, mapped to cluster " << stripe.blocks[stripe.group_to_blocks[i][0]]->map2cluster << ") ";
      for (int j = 0; j < stripe.group_to_blocks[i].size(); j++)
      {
        std::cout << stripe.blocks[stripe.group_to_blocks[i][j]]->block_key << " ";
      }
      std::cout << std::endl;
    }
  }

  // set only the full block stripe
  grpc::Status CoordinatorImpl::uploadSetValue(
      grpc::ServerContext *context,
      const coordinator_proto::RequestProxyIPPort *keyValueSize,
      coordinator_proto::ReplyProxyIPsPorts *proxyIPPort)
  {
    std::string clientID = keyValueSize->key();
    size_t setSizeBytes = keyValueSize->valuesizebytes();
    std::string code_type = m_sys_config->CodeType;
    assert(setSizeBytes == static_cast<size_t>(m_sys_config->BlockSize) * static_cast<size_t>(m_sys_config->k) && "set size is not equal to the block stripe size!");
    assert((code_type == "UniLRC" || code_type == "AzureLRC" || code_type == "OptimalLRC" ||
            code_type == "UniformLRC" || code_type == "gLRC") &&
           "Error: code type must be UniLRC, AzureLRC, OptimalLRC, UniformLRC, or gLRC");

    Stripe t_stripe;
    t_stripe.stripe_id = m_cur_stripe_id++;
    t_stripe.n = m_sys_config->n;
    t_stripe.k = m_sys_config->k;
    t_stripe.r = m_sys_config->r;
    t_stripe.z = m_sys_config->z;
    t_stripe.object_keys.push_back(clientID);
    if (code_type == "UniLRC" || code_type == "AzureLRC")
    {
      initialize_unilrc_and_azurelrc_stripe_placement(&t_stripe);
    }
    else if (code_type == "OptimalLRC")
    {
      initialize_optimal_lrc_stripe_placement(&t_stripe);
    }
    else if (code_type == "gLRC")
    {
      initialize_glrc_stripe_placement(&t_stripe);
    }
    else if (code_type == "UniformLRC")
    {
      initialize_uniform_lrc_stripe_placement(&t_stripe);
    }

    print_stripe_data_placement(t_stripe);

    std::vector<proxy_proto::AppendStripeDataPlacement> add_plans = generate_add_plans(&t_stripe);

    for (const auto &plan : add_plans)
    {
      m_mutex.lock();
      m_object_commit_table.erase(plan.key());
      m_mutex.unlock();
    }

    std::vector<std::thread> threads;
    size_t sum_append_size = 0;
    for (const auto &plan : add_plans)
    {
      threads.push_back(std::thread(&CoordinatorImpl::notify_proxies_ready, this, plan));
      proxyIPPort->add_append_keys(plan.key());
      proxyIPPort->add_proxyips(m_cluster_table[plan.cluster_id()].proxy_ip);
      proxyIPPort->add_proxyports(m_cluster_table[plan.cluster_id()].proxy_port + ECProject::PROXY_PORT_SHIFT); // use another port to accept data
      proxyIPPort->add_cluster_slice_sizes(plan.append_size());
      sum_append_size += plan.append_size();
    }
    for (auto &thread : threads)
    {
      thread.join();
    }
    proxyIPPort->set_sum_append_size(sum_append_size);

    m_stripe_table[t_stripe.stripe_id] = std::move(t_stripe);

    return grpc::Status::OK;
  }

  grpc::Status CoordinatorImpl::uploadSubsetValue(
      grpc::ServerContext *context,
      const coordinator_proto::RequestProxyIPPort *keyValueSize,
      coordinator_proto::ReplyProxyIPsPorts *proxyIPPort)
  {
    std::string clientID = keyValueSize->key();
    size_t setSizeBytes = keyValueSize->valuesizebytes();
    std::string code_type = m_sys_config->CodeType;
    assert(setSizeBytes <= static_cast<size_t>(m_sys_config->BlockSize) * static_cast<size_t>(m_sys_config->k) && "subset size is larger than the block size!");
    assert((code_type == "UniLRC" || code_type == "AzureLRC" || code_type == "OptimalLRC" ||
            code_type == "UniformLRC" || code_type == "gLRC") &&
           "Error: code type must be UniLRC, AzureLRC, OptimalLRC, UniformLRC, or gLRC");

    Stripe t_stripe;
    t_stripe.stripe_id = m_cur_stripe_id++;
    t_stripe.n = m_sys_config->n;
    t_stripe.k = m_sys_config->k;
    t_stripe.r = m_sys_config->r;
    t_stripe.z = m_sys_config->z;
    t_stripe.object_keys.push_back(clientID);
    if (code_type == "UniLRC" || code_type == "AzureLRC")
    {
      initialize_unilrc_and_azurelrc_stripe_placement(&t_stripe);
    }
    else if (code_type == "OptimalLRC")
    {
      initialize_optimal_lrc_stripe_placement(&t_stripe);
    }
    else if (code_type == "gLRC")
    {
      initialize_glrc_stripe_placement(&t_stripe);
    }
    else if (code_type == "UniformLRC")
    {
      initialize_uniform_lrc_stripe_placement(&t_stripe);
    }

    print_stripe_data_placement(t_stripe);

    std::vector<proxy_proto::AppendStripeDataPlacement> add_plans = generate_sub_add_plans(&t_stripe, setSizeBytes);

    for (const auto &plan : add_plans)
    {
      m_mutex.lock();
      m_object_commit_table.erase(plan.key());
      m_mutex.unlock();
    }

    std::vector<std::thread> threads;
    size_t sum_append_size = 0;
    for (const auto &plan : add_plans)
    {
      threads.push_back(std::thread(&CoordinatorImpl::notify_proxies_ready, this, plan));
      proxyIPPort->add_append_keys(plan.key());
      proxyIPPort->add_proxyips(m_cluster_table[plan.cluster_id()].proxy_ip);
      proxyIPPort->add_proxyports(m_cluster_table[plan.cluster_id()].proxy_port + ECProject::PROXY_PORT_SHIFT); // use another port to accept data
      proxyIPPort->add_cluster_slice_sizes(plan.append_size());
      //proxyIPPort->add_group_ids(group_id);
      sum_append_size += plan.append_size();
      //group_id++;
    }
    for (auto &thread : threads)
    {
      thread.join();
    }
    proxyIPPort->set_sum_append_size(sum_append_size);

    m_stripe_table[t_stripe.stripe_id] = std::move(t_stripe);

    return grpc::Status::OK;
  }
  
  std::vector<int> CoordinatorImpl::get_recovery_group_ids(std::string code_type, int k, int r, int z, int failed_block_id)
  {
    std::vector<int> recovery_group_ids;
    if (code_type == "AzureLRC")
    {
      if (failed_block_id >= k && failed_block_id < k + r)
      {
        for (int i = 1; i <= z; i++)
        {
          recovery_group_ids.push_back(i);
        }
      }
      else if (failed_block_id >= k + r)
      {
        recovery_group_ids.push_back(failed_block_id - k - r);
      }
      else
      {
        recovery_group_ids.push_back(failed_block_id / (k / z));
      }
    }
    else if (code_type == "UniLRC")
    {
      if (failed_block_id >= k && failed_block_id < k + r)
      {
        recovery_group_ids.push_back((failed_block_id - k) / (r / z));
      }
      else if (failed_block_id >= k + r)
      {
        recovery_group_ids.push_back(failed_block_id - k - r);
      }
      else
      {
        recovery_group_ids.push_back(failed_block_id / (k / z));
      }
    }
    else if (code_type == "OptimalLRC")
    {
      if (failed_block_id >= k && failed_block_id < k + r)
      {
        int group_num = (k / z / (r + 1) + (bool)(k / z % (r + 1))) * z + 1;
        recovery_group_ids.push_back(group_num - 1);
        for (int i = 0; i < group_num / z; i++)
        {
          recovery_group_ids.push_back(i);
        }
      }
      else if (failed_block_id >= k + r)
      {
        int local_group_size = k / z;
        int local_group_id = (failed_block_id - k - r);
        int group_num_of_one_local_group = local_group_size / (r + 1) + 1;
        int group_num = z * group_num_of_one_local_group + 1;
        recovery_group_ids.push_back((local_group_id + 1) * group_num_of_one_local_group - 1);
        for (int i = local_group_id * group_num_of_one_local_group; i < (local_group_id + 1) * group_num_of_one_local_group - 1; i++)
        {
          recovery_group_ids.push_back(i);
        }
        recovery_group_ids.push_back(group_num - 1);
      }
      else
      {
        int local_group_size = k / z;
        int group_num_of_one_local_group = local_group_size / (r + 1) + 1;
        int local_group_id = failed_block_id / local_group_size;
        int group_id_in_local_group = failed_block_id % local_group_size / (r + 1);
        recovery_group_ids.push_back(local_group_id * group_num_of_one_local_group + group_id_in_local_group);
        for (int i = 0; i < group_num_of_one_local_group; i++)
        {
          if (i != group_id_in_local_group)
          {
            recovery_group_ids.push_back(local_group_id * group_num_of_one_local_group + i);
          }
        }
        int group_num = z * group_num_of_one_local_group + 1;
        recovery_group_ids.push_back(group_num - 1);
      }
    }
    else if (code_type == "gLRC")
    {
      if (failed_block_id < k)
      {
        recovery_group_ids.push_back(glrc_data_group_id(failed_block_id, k, r, z));
      }
      else if (failed_block_id < k + r)
      {
        recovery_group_ids.push_back(z - 1);
      }
      else
      {
        recovery_group_ids.push_back(failed_block_id - k - r);
      }
    }
    else if (code_type == "UniformLRC")
    {
      if (failed_block_id >= k + r)
      {
        int larger_local_group_num = (k + r) % z;
        int local_group_id = failed_block_id - k - r;
        int local_group_size = (k + r) / z;
        int group_num_of_one_local_group = local_group_size / r + bool(local_group_size % r);
        if (local_group_id + larger_local_group_num < z)
        {
          recovery_group_ids.push_back((local_group_id + 1) * group_num_of_one_local_group - 1);
          for (int i = local_group_id * group_num_of_one_local_group; i < (local_group_id + 1) * group_num_of_one_local_group - 1; i++)
          {
            recovery_group_ids.push_back(i);
          }
        }
        else
        {
          int smaller_local_group_num = z - larger_local_group_num;
          int group_num_of_all_small_group = smaller_local_group_num * group_num_of_one_local_group;
          local_group_size++;
          group_num_of_one_local_group = local_group_size / r + (bool)(local_group_size % r);
          local_group_id = local_group_id - smaller_local_group_num;
          recovery_group_ids.push_back(group_num_of_all_small_group + (local_group_id + 1) * group_num_of_one_local_group - 1);
          for (int i = group_num_of_all_small_group + local_group_id * group_num_of_one_local_group; i < group_num_of_all_small_group + (local_group_id + 1) * group_num_of_one_local_group - 1; i++)
          {
            recovery_group_ids.push_back(i);
          }
        }
      }
      else if (failed_block_id < k + r)
      {
        int larger_local_group_num = (k + r) % z;
        int smaller_local_group_num = z - larger_local_group_num;
        int local_group_size = (k + r) / z;
        int group_num_of_one_local_group = local_group_size / r + bool(local_group_size % r);
        int block_num_of_smaller_local_group = (z - larger_local_group_num) * local_group_size;
        int group_num_of_smaller_local_group = smaller_local_group_num * group_num_of_one_local_group;
        int local_group_id = 0;
        if (failed_block_id < block_num_of_smaller_local_group)
        {
          local_group_id = failed_block_id / local_group_size;
          int block_num_in_previous_local_group = local_group_id * local_group_size;
          int group_id = local_group_id * group_num_of_one_local_group + (failed_block_id - block_num_in_previous_local_group) / r;
          recovery_group_ids.push_back(group_id);
          for (int i = local_group_id * group_num_of_one_local_group; i < local_group_id * group_num_of_one_local_group + group_num_of_one_local_group; i++)
          {
            if (i != group_id)
            {
              recovery_group_ids.push_back(i);
            }
          }
        }
        else
        {
          local_group_size++;
          group_num_of_one_local_group = local_group_size / r + bool(local_group_size % r);
          local_group_id = (failed_block_id - block_num_of_smaller_local_group) / local_group_size;
          int block_num_in_previous_local_group = local_group_id * local_group_size + block_num_of_smaller_local_group;
          int group_id = local_group_id * group_num_of_one_local_group + (failed_block_id - block_num_in_previous_local_group) / r;
          recovery_group_ids.push_back(group_id + group_num_of_smaller_local_group);
          for (int i = local_group_id * group_num_of_one_local_group; i < local_group_id * group_num_of_one_local_group + group_num_of_one_local_group; i++)
          {
            if (i != group_id)
            {
              recovery_group_ids.push_back(i + group_num_of_smaller_local_group);
            }
          }
        }
      }
    }

    return recovery_group_ids;
  }

  void CoordinatorImpl::init_recovery_group_lookup_table()
  {
    for (int i = 0; i < m_sys_config->n; i++)
    {
      m_recovery_group_lookup_table[i] = get_recovery_group_ids(m_sys_config->CodeType, m_sys_config->k, m_sys_config->r, m_sys_config->z, i);
    }
  }

  std::vector<int> CoordinatorImpl::get_data_block_num_per_group(int k, int r, int z, std::string code_type)
  {
    std::vector<int> data_block_num_per_group;
    if (code_type == "AzureLRC")
    {
      for (int i = 0; i < z; i++)
      {
        data_block_num_per_group.push_back((k / z));
      }
      data_block_num_per_group.push_back(0);
    }
    else if (code_type == "OptimalLRC")
    {
      int group_size = r + 1;
      int local_group_size = (k / z);
      int group_num_of_one_local_group = local_group_size / group_size + 1;
      int group_num = z * group_num_of_one_local_group + 1;
      for (int i = 0; i < group_num - 1; i++)
      {
        if ((i + 1) % group_num_of_one_local_group)
        {
          data_block_num_per_group.push_back(group_size);
        }
        else
        {
          data_block_num_per_group.push_back(local_group_size % group_size);
        }
      }
      data_block_num_per_group.push_back(0);
    }
    else if (code_type == "gLRC")
    {
      std::vector<int> glrc_data;
      glrc_fill_data_blocks_per_group(glrc_data, k, r, z);
      for (int i = 0; i < z; i++)
        data_block_num_per_group.push_back(glrc_data[i]);
    }
    else if (code_type == "UniformLRC")
    {
      /*int group_size = r + 1;
      int local_group_size = int((k + r) / z);
      int larger_local_group_num = int((k + r) % z);

      int group_num_of_one_local_group = local_group_size / group_size + (bool)(local_group_size % group_size);
      for (int i = 0; i < z - 1; i++)
      {
        if (i + larger_local_group_num == z)
        {
          local_group_size++;
          group_num_of_one_local_group = local_group_size / group_size + (bool)(local_group_size % group_size);
        }
        for (int j = 0; j < group_num_of_one_local_group; j++)
        {
          if (j == group_num_of_one_local_group - 1)
          {
            data_block_num_per_group.push_back(local_group_size % group_size);
          }
          else
          {
            data_block_num_per_group.push_back(group_size);
          }
        }
      }
      data_block_num_per_group.push_back(local_group_size - r);
      for(int i = 0; i < group_num_of_one_local_group -1; i++)
      {
        data_block_num_per_group.push_back(0);
      }*/

      for(int i = 0; i < z -1; i++){
        data_block_num_per_group.push_back((k+r) / z);
      }
      data_block_num_per_group.push_back(0);
    }
    else if (code_type == "UniLRC")
    {
      int local_data_num = k / z;
      for (int i = 0; i < z; i++)
      {
        data_block_num_per_group.push_back(local_data_num);
      }
    }
    return data_block_num_per_group;
  }

  
  void CoordinatorImpl::getStripeFromProxy(std::string client_ip, int client_port, std::string proxy_ip, int proxy_port, int stripe_id, int group_id, std::vector<int> block_ids)
  {
    std::cout << "[GET] getting stripe " << stripe_id << " from proxy " << proxy_ip << ":" << proxy_port << std::endl;
    for(int i = 0; i < block_ids.size(); i++){
      std::cout << "block_id: " << block_ids[i] << std::endl;
    }
    grpc::ClientContext cont;
    proxy_proto::StripeAndBlockIDs stripe_block_ids;
    proxy_proto::GetReply stripe_reply;
    stripe_block_ids.set_stripe_id(stripe_id);
    stripe_block_ids.set_clientip(client_ip);
    stripe_block_ids.set_clientport(client_port);
    stripe_block_ids.set_group_id(group_id);

    for (int i = 0; i < block_ids.size(); i++)
    {
      stripe_block_ids.add_block_ids(block_ids[i]);
      stripe_block_ids.add_block_keys(m_stripe_table[stripe_id].blocks[block_ids[i]]->block_key);
      stripe_block_ids.add_datanodeips(m_node_table[m_stripe_table[stripe_id].blocks[block_ids[i]]->map2node].node_ip);
      stripe_block_ids.add_datanodeports(m_node_table[m_stripe_table[stripe_id].blocks[block_ids[i]]->map2node].node_port);
    }
    grpc::Status status = m_proxy_ptrs[proxy_ip + ":" + std::to_string(proxy_port)]->getBlocks(&cont, stripe_block_ids, &stripe_reply);
    if (status.ok())
    {
      std::cout << "[GET] getting stripe " << stripe_id << " from proxy " << proxy_ip << ":" << proxy_port << " succeeded!" << std::endl;
    }
    else
    {
      std::cout << "[GET] getting stripe " << stripe_id << " from proxy " << proxy_ip << ":" << proxy_port << " failed!" << std::endl;
    }
  }


  grpc::Status 
  CoordinatorImpl::getStripe(
      grpc::ServerContext *context,
      const coordinator_proto::KeyAndClientIP *keyClient,
      coordinator_proto::ReplyProxyIPsPorts *proxyIPPort)
  {

    //std::chrono::high_resolution_clock::time_point start = std::chrono::high_resolution_clock::now();
    int stripe_id = std::stoi(keyClient->key());
    Stripe &t_stripe = m_stripe_table[stripe_id];
    int k = t_stripe.k;
    int num_data_groups = t_stripe.num_groups;
    std::string code_type = m_sys_config->CodeType;
    if(code_type != "UniLRC"){
      num_data_groups--;
    }
    //std::cout << "[GET] getting stripe " << stripe_id << " with " << num_data_groups << " data groups" << std::endl;
    std::vector<int> block_num_per_group = get_data_block_num_per_group(k, m_sys_config->r, m_sys_config->z, code_type);
    std::vector<int> get_cluster_ids;
    for (int i = 0; i < num_data_groups; i++)
    {
      get_cluster_ids.push_back(t_stripe.blocks[t_stripe.group_to_blocks[i][0]]->map2cluster);
      //std::cout << "group " << i << " is mapped to cluster " << get_cluster_ids[i] << std::endl;
    }
    for (int i = 0; i < num_data_groups; i++)
    {
      proxyIPPort->add_proxyips(m_cluster_table[get_cluster_ids[i]].proxy_ip);
      proxyIPPort->add_proxyports(m_cluster_table[get_cluster_ids[i]].proxy_port);
      proxyIPPort->add_cluster_slice_sizes(block_num_per_group[i]);
    }
    /*for(int i = 0; i < t_stripe.num_groups; i++){
      m_proxy_ptrs[proxyIPPort->proxyips(i) + ":" + std::to_string(proxyIPPort->proxyports(i))]->getStripe(stripe_id, t_stripe.group_to_blocks[i]);
    }*/
    std::vector<std::thread> threads;
    for (int i = 0; i < num_data_groups; i++)
    {
      std::vector<int> block_ids;
      for (int j = 0; j < t_stripe.group_to_blocks[i].size(); j++)
      {
        if(t_stripe.blocks[t_stripe.group_to_blocks[i][j]]->block_id < k){
          block_ids.push_back(t_stripe.group_to_blocks[i][j]);
        }
      }
      threads.push_back(std::thread(&CoordinatorImpl::getStripeFromProxy, this, keyClient->clientip(), keyClient->clientport(), 
        proxyIPPort->proxyips(i), proxyIPPort->proxyports(i), stripe_id, i, block_ids));
    }
    for (auto &thread : threads)
    {
      thread.detach();
    }
    /*std::chrono::high_resolution_clock::time_point end = std::chrono::high_resolution_clock::now();
    std::chrono::duration<double> duration = std::chrono::duration_cast<std::chrono::duration<double>>(end - start);
    std::cout << "[GET] getting stripe " << stripe_id << " took " << duration.count() << " seconds" << std::endl;*/

    return grpc::Status::OK;
  }
  
  grpc::Status
  CoordinatorImpl::getBlocks(
      grpc::ServerContext *context,
      const coordinator_proto::BlockIDsAndClientIP *blockIDsClient,
      coordinator_proto::ReplyProxyIPsPorts *proxyIPPort
  )
  {
    std::string client_ip = blockIDsClient->clientip();
    int client_port = blockIDsClient->clientport();
    int start_block_id = blockIDsClient->start_block_id();
    int end_block_id = blockIDsClient->end_block_id();
    std::vector<int> stripe_ids;
    std::vector<int> block_ids;
    std::vector<int> relative_block_ids;
    for(int i = start_block_id; i <= end_block_id; i++){
      int stripe_id = i / m_sys_config->k;
      stripe_ids.push_back(stripe_id);
      block_ids.push_back(i % m_sys_config->k);
      relative_block_ids.push_back(i - start_block_id);
    }
    std::vector<int> get_cluster_ids;
    std::vector<int> unique_cluster_ids;
    for (int i = 0; i < stripe_ids.size(); i++)
    {
      get_cluster_ids.push_back(m_stripe_table[stripe_ids[i]].blocks[block_ids[i]]->map2cluster);
      if(std::find(unique_cluster_ids.begin(), unique_cluster_ids.end(), get_cluster_ids[i]) == unique_cluster_ids.end()){
        unique_cluster_ids.push_back(get_cluster_ids[i]);
      }
    }
    proxy_proto::StripeAndBlockIDs stripe_block_ids[unique_cluster_ids.size()];
    for(int i = 0; i < stripe_ids.size(); i++){
      int idx = std::find(unique_cluster_ids.begin(), unique_cluster_ids.end(), get_cluster_ids[i]) - unique_cluster_ids.begin();
      stripe_block_ids[idx].add_block_ids(relative_block_ids[i]);
      stripe_block_ids[idx].add_block_keys(m_stripe_table[stripe_ids[i]].blocks[block_ids[i]]->block_key);
      stripe_block_ids[idx].add_datanodeips(m_node_table[m_stripe_table[stripe_ids[i]].blocks[block_ids[i]]->map2node].node_ip);
      stripe_block_ids[idx].add_datanodeports(m_node_table[m_stripe_table[stripe_ids[i]].blocks[block_ids[i]]->map2node].node_port);
    }
    std::vector<std::thread> get_threads;
    for(int i = 0; i < unique_cluster_ids.size(); i++){
      get_threads.push_back(std::thread([this, &stripe_block_ids, &client_ip, &client_port, &proxyIPPort, &unique_cluster_ids, i](){
        grpc::ClientContext cont;
        proxy_proto::GetReply stripe_reply;
        stripe_block_ids[i].set_clientip(client_ip);
        stripe_block_ids[i].set_clientport(client_port);
        grpc::Status status = m_proxy_ptrs[m_cluster_table[unique_cluster_ids[i]].proxy_ip + ":" + std::to_string(m_cluster_table[unique_cluster_ids[i]].proxy_port)]->getBlocks(&cont, stripe_block_ids[i], &stripe_reply);
        if (status.ok())
        {
          std::cout << "[GET] getting blocks from proxy " << m_cluster_table[unique_cluster_ids[i]].proxy_ip << ":" << m_cluster_table[unique_cluster_ids[i]].proxy_port << " succeeded!" << std::endl;
        }
        else
        {
          std::cout << "[GET] getting blocks from proxy " << m_cluster_table[unique_cluster_ids[i]].proxy_ip << ":" << m_cluster_table[unique_cluster_ids[i]].proxy_port << " failed!" << std::endl;
        }
      }));
    }
    for (auto &thread : get_threads)
    {
      thread.join();
    }
    return grpc::Status::OK;

  }

  grpc::Status
  CoordinatorImpl::getDegradedReadBlocks(
      grpc::ServerContext *context,
      const coordinator_proto::BlockIDsAndClientIP *blockIDsClient,
      coordinator_proto::ReplyProxyIPsPorts *proxyIPPort
  )
  {
    std::string client_ip = blockIDsClient->clientip();
    int client_port = blockIDsClient->clientport();
    int start_block_id = blockIDsClient->start_block_id();
    int end_block_id = blockIDsClient->end_block_id();
    std::vector<int> stripe_ids;
    std::vector<int> block_ids;
    std::vector<int> relative_block_ids;
    for(int i = start_block_id; i <= end_block_id; i++){
      int stripe_id = i / m_sys_config->k;
      stripe_ids.push_back(stripe_id);
      block_ids.push_back(i % m_sys_config->k);
      relative_block_ids.push_back(i - start_block_id);
    }
    for(int i = 0; i < stripe_ids.size(); i++){
      degraded_read_one_block_for_workload(stripe_ids[i], block_ids[i], client_ip, client_port, relative_block_ids[i]);
    }
    return grpc::Status::OK;

  }


  grpc::Status
  CoordinatorImpl::getValue(
      grpc::ServerContext *context,
      const coordinator_proto::KeyAndClientIP *keyClient,
      coordinator_proto::RepIfGetSuccess *getReplyClient)
  {
    try
    {
      std::string key = keyClient->key();
      std::string client_ip = keyClient->clientip();
      int client_port = keyClient->clientport();
      ObjectInfo object_info;
      m_mutex.lock();
      object_info = m_object_commit_table.at(key);
      m_mutex.unlock();
      int k = m_encode_parameters.k_datablock;
      int g_m = m_encode_parameters.g_m_globalparityblock;
      int l = m_encode_parameters.l_localparityblock;
      // int b = m_encode_parameters.b_datapergroup;

      grpc::ClientContext decode_and_get;
      proxy_proto::ObjectAndPlacement object_placement;
      grpc::Status status;
      proxy_proto::GetReply get_reply;
      getReplyClient->set_valuesizebytes(object_info.object_size);
      object_placement.set_key(key);
      object_placement.set_valuesizebyte(object_info.object_size);
      object_placement.set_k(k);
      object_placement.set_l(l);
      object_placement.set_g_m(g_m);
      object_placement.set_stripe_id(object_info.map2stripe);
      object_placement.set_encode_type(m_encode_parameters.encodetype);
      object_placement.set_clientip(client_ip);
      object_placement.set_clientport(client_port);
      Stripe &t_stripe = m_stripe_table[object_info.map2stripe];
      std::unordered_set<int> t_cluster_set;
      for (int i = 0; i < int(t_stripe.blocks.size()); i++)
      {
        if (t_stripe.blocks[i]->map2key == key)
        {
          object_placement.add_datanodeip(m_node_table[t_stripe.blocks[i]->map2node].node_ip);
          object_placement.add_datanodeport(m_node_table[t_stripe.blocks[i]->map2node].node_port);
          object_placement.add_blockkeys(t_stripe.blocks[i]->block_key);
          object_placement.add_blockids(t_stripe.blocks[i]->block_id);
          t_cluster_set.insert(t_stripe.blocks[i]->map2cluster);
        }
      }
      // randomly select a cluster
      int idx = rand_num(int(t_cluster_set.size()));
      int r_cluster_id = *(std::next(t_cluster_set.begin(), idx));
      std::string chosen_proxy = m_cluster_table[r_cluster_id].proxy_ip + ":" + std::to_string(m_cluster_table[r_cluster_id].proxy_port);
      status = m_proxy_ptrs[chosen_proxy]->decodeAndGetObject(&decode_and_get, object_placement, &get_reply);
      if (status.ok())
      {
        std::cout << "[GET] getting value of " << key << std::endl;
      }
    }
    catch (std::exception &e)
    {
      std::cout << "getValue exception" << std::endl;
      std::cout << e.what() << std::endl;
    }
    return grpc::Status::OK;
  }

  int CoordinatorImpl::get_cluster_id_by_group_id(Stripe &t_stripe, int group_id)
  {
    int block_id = t_stripe.group_to_blocks[group_id][0];
    return t_stripe.blocks[block_id]->map2cluster;
  }

  bool CoordinatorImpl::recovery_one_block_breakdown(int stripe_id, int failed_block_id, 
    std::vector<double> &disk_io_start_time, std::vector<double> &disk_io_end_time, std::vector<double> &decode_start_time, std::vector<double> &decode_end_time,
    std::vector<double> &network_start_time, std::vector<double> &network_end_time, double &cross_rack_network_time, double &cross_rack_xor_time,
    std::vector<double> &grpc_notify_time, std::vector<double> &grpc_start_time, std::vector<double> &data_node_grpc_notify_time, std::vector<double> &data_node_grpc_start_time,
    double &dest_data_node_network_time, double &dest_data_node_disk_io_time)
  {
    std::string code_type = m_sys_config->CodeType;
    Stripe &t_stripe = m_stripe_table[stripe_id];
    std::vector<int> recovery_group_ids = get_recovery_group_ids(m_sys_config->CodeType, m_sys_config->k, m_sys_config->r, m_sys_config->z, failed_block_id);
    grpc::Status status;

    if (recovery_group_ids.size() == 1)
    {
      //assert((code_type == "UniLRC") || (code_type == "AzureLRC" && (failed_block_id < m_sys_config->k || failed_block_id >= m_sys_config->k + m_sys_config->r)));

      grpc::ClientContext recovery_context;
      proxy_proto::RecoveryRequest recovery_request;
      proxy_proto::RecoveryReply recovery_reply;

      int chosen_cluster_id = get_cluster_id_by_group_id(t_stripe, recovery_group_ids[0]);
      std::string chosen_proxy = m_cluster_table[chosen_cluster_id].proxy_ip + ":" + std::to_string(m_cluster_table[chosen_cluster_id].proxy_port);
      recovery_request.set_failed_block_id(failed_block_id);
      recovery_request.set_failed_block_key(t_stripe.blocks[failed_block_id]->block_key);
      int t_node_id = randomly_select_a_node(chosen_cluster_id, stripe_id);
      recovery_request.set_replaced_node_ip(m_node_table[t_node_id].node_ip);
      recovery_request.set_replaced_node_port(m_node_table[t_node_id].node_port);
      recovery_request.set_cross_rack_num(0);
      std::vector<int> blockids = t_stripe.group_to_blocks[recovery_group_ids[0]];
      for (int i = 0; i < int(blockids.size()); i++)
      {
        if (blockids[i] == failed_block_id)
          continue;

        Block *t_block = t_stripe.blocks[blockids[i]];
        recovery_request.add_datanodeip(m_node_table[t_block->map2node].node_ip);
        recovery_request.add_datanodeport(m_node_table[t_block->map2node].node_port);
        recovery_request.add_blockkeys(t_block->block_key);
        recovery_request.add_blockids(t_block->block_id);
      }
      std::chrono::high_resolution_clock::time_point start = std::chrono::high_resolution_clock::now();
      grpc_notify_time.push_back(std::chrono::duration_cast<std::chrono::duration<double>>(start.time_since_epoch()).count());
      status = m_proxy_ptrs[chosen_proxy]->recoveryBreakdown(&recovery_context, recovery_request, &recovery_reply);
      if (status.ok())
      {
        disk_io_start_time.push_back(recovery_reply.disk_io_start_time());
        disk_io_end_time.push_back(recovery_reply.disk_io_end_time());
        decode_start_time.push_back(recovery_reply.decode_start_time());
        decode_end_time.push_back(recovery_reply.decode_end_time());
        network_start_time.push_back(recovery_reply.network_start_time());
        network_end_time.push_back(recovery_reply.network_end_time());
        cross_rack_network_time = recovery_reply.cross_rack_time();
        cross_rack_xor_time = recovery_reply.cross_rack_xor_time();
        data_node_grpc_notify_time.push_back(recovery_reply.data_node_grpc_notify_time());
        data_node_grpc_start_time.push_back(recovery_reply.data_node_grpc_start_time());
        dest_data_node_network_time = recovery_reply.dest_data_node_network_time();
        dest_data_node_disk_io_time = recovery_reply.dest_data_node_disk_io_time();
        grpc_start_time.push_back(recovery_reply.grpc_start_time());
        std::cout << "[Coordinator] recovery of " << stripe_id << "_" << failed_block_id << " success!" << std::endl;
        return true;
      }
      else
      {
        std::cout << "[Coordinator] recovery of " << stripe_id << "_" << failed_block_id << " failed!" << std::endl;
        return false;
      }
    }
    else
    {
      int dest_group_id = t_stripe.blocks[failed_block_id]->map2group;
      int dest_cluster_id = get_cluster_id_by_group_id(t_stripe, dest_group_id);
      std::string dest_proxy_ip = m_cluster_table[dest_cluster_id].proxy_ip;
      int dest_proxy_port = m_cluster_table[dest_cluster_id].proxy_port;
      std::vector<int> chosen_cluster_ids;
      for(int i = 0; i < recovery_group_ids.size(); i++){
        chosen_cluster_ids.push_back(get_cluster_id_by_group_id(t_stripe, recovery_group_ids[i]));
      }
      std::vector<std::string> chosen_proxies;
      for(int i = 0; i < chosen_cluster_ids.size(); i++){
        chosen_proxies.push_back(m_cluster_table[chosen_cluster_ids[i]].proxy_ip + ":" + std::to_string(m_cluster_table[chosen_cluster_ids[i]].proxy_port));
      }
      std::vector<std::thread> threads;
      for(int i = 0; i < recovery_group_ids.size(); i++){
        if(recovery_group_ids[i] == dest_group_id){
          continue;
        }
        threads.push_back(std::thread([&t_stripe, &chosen_proxies, &recovery_group_ids, i, failed_block_id, dest_proxy_ip, dest_proxy_port, this,
          &disk_io_start_time, &disk_io_end_time, &decode_start_time, &decode_end_time, &network_start_time, &network_end_time, 
          &grpc_notify_time, &grpc_start_time, &data_node_grpc_notify_time, &data_node_grpc_start_time
        ](){
          grpc::ClientContext degraded_read_context;
          proxy_proto::DegradedReadRequest degraded_read_request;
          proxy_proto::DegradedReadReply degraded_read_reply;
          degraded_read_request.set_clientip(dest_proxy_ip);
          degraded_read_request.set_clientport(dest_proxy_port + ECProject::PROXY_PORT_SHIFT);
          degraded_read_request.set_failed_block_id(failed_block_id);
          degraded_read_request.set_failed_block_key(t_stripe.blocks[failed_block_id]->block_key);
          std::vector<int> blockids = t_stripe.group_to_blocks[recovery_group_ids[i]];
          for (int j = 0; j < int(blockids.size()); j++)
          {
            if(m_sys_config->CodeType == "AzureLRC" && degraded_read_request.blockids_size() == (m_sys_config->k / m_sys_config->z))
              break;

            if ((m_sys_config->CodeType == "AzureLRC" && blockids[j] >= m_sys_config->k + m_sys_config->r) || blockids[j] == failed_block_id)
              continue;

            Block *t_block = t_stripe.blocks[blockids[j]];
            degraded_read_request.add_datanodeip(m_node_table[t_block->map2node].node_ip);
            degraded_read_request.add_datanodeport(m_node_table[t_block->map2node].node_port);
            degraded_read_request.add_blockkeys(t_block->block_key);
            degraded_read_request.add_blockids(t_block->block_id);
          }
          std::chrono::high_resolution_clock::time_point start = std::chrono::high_resolution_clock::now();

          grpc::Status status = m_proxy_ptrs[chosen_proxies[i]]->degradedReadBreakdown(&degraded_read_context, degraded_read_request, &degraded_read_reply);
          if (status.ok())
          {
            std::lock_guard<std::mutex> lock(m_mutex);
            disk_io_start_time.push_back(degraded_read_reply.disk_io_start_time());
            disk_io_end_time.push_back(degraded_read_reply.disk_io_end_time());
            decode_start_time.push_back(degraded_read_reply.decode_start_time());
            decode_end_time.push_back(degraded_read_reply.decode_end_time());
            network_start_time.push_back(degraded_read_reply.network_start_time());
            network_end_time.push_back(degraded_read_reply.network_end_time());
            grpc_start_time.push_back(degraded_read_reply.grpc_start_time());
            data_node_grpc_notify_time.push_back(degraded_read_reply.data_node_grpc_notify_time());
            data_node_grpc_start_time.push_back(degraded_read_reply.data_node_grpc_start_time());
            grpc_notify_time.push_back(std::chrono::duration_cast<std::chrono::duration<double>>(start.time_since_epoch()).count());
    
            std::cout << "[Coordinator] partial degraded read of " << failed_block_id << " success!" << std::endl;
          }
          else
          {
            std::cout << "[Coordinator] partial degraded read of " << failed_block_id << " failed!" << std::endl;
          }
        }));

      }
      int cross_rack_num = recovery_group_ids.size() - 1;
      threads.push_back(std::thread([this, &t_stripe, cross_rack_num, dest_group_id, dest_cluster_id, dest_proxy_ip, dest_proxy_port, stripe_id, failed_block_id,
        &disk_io_start_time, &disk_io_end_time, &decode_start_time, &decode_end_time, &network_start_time, &network_end_time, 
        &grpc_notify_time, &grpc_start_time, &data_node_grpc_notify_time, &data_node_grpc_start_time, &cross_rack_network_time, &cross_rack_xor_time,
        &dest_data_node_network_time, &dest_data_node_disk_io_time
        ](){
        grpc::ClientContext recovery_context;
        proxy_proto::RecoveryRequest recovery_request;
        proxy_proto::RecoveryReply recovery_reply;
        recovery_request.set_failed_block_id(failed_block_id);
        recovery_request.set_failed_block_key(t_stripe.blocks[failed_block_id]->block_key);
        int t_node_id = randomly_select_a_node(dest_cluster_id, stripe_id);
        recovery_request.set_replaced_node_ip(m_node_table[t_node_id].node_ip);
        recovery_request.set_replaced_node_port(m_node_table[t_node_id].node_port);
        recovery_request.set_cross_rack_num(cross_rack_num);
        std::vector<int> blockids = t_stripe.group_to_blocks[dest_group_id];
        for (int i = 0; i < int(blockids.size()); i++)
        {
          if(m_sys_config->CodeType == "AzureLRC" && recovery_request.blockids_size() == (m_sys_config->k / m_sys_config->z))
            break;

          if (blockids[i] == failed_block_id)
            continue;

          Block *t_block = t_stripe.blocks[blockids[i]];
          recovery_request.add_datanodeip(m_node_table[t_block->map2node].node_ip);
          recovery_request.add_datanodeport(m_node_table[t_block->map2node].node_port);
          recovery_request.add_blockkeys(t_block->block_key);
          recovery_request.add_blockids(t_block->block_id);
        }
        //std::cout << "[Coordinator] start recovery of " << stripe_id << "_" << failed_block_id << std::endl;
        std::chrono::high_resolution_clock::time_point start = std::chrono::high_resolution_clock::now();

        grpc::Status status = m_proxy_ptrs[dest_proxy_ip + ":" + std::to_string(dest_proxy_port)]->recoveryBreakdown(&recovery_context, recovery_request, &recovery_reply);
        if (status.ok())
        {
          std::lock_guard<std::mutex> lock(m_mutex);
          grpc_notify_time.push_back(std::chrono::duration_cast<std::chrono::duration<double>>(start.time_since_epoch()).count());
          disk_io_start_time.push_back(recovery_reply.disk_io_start_time());
          disk_io_end_time.push_back(recovery_reply.disk_io_end_time());
          decode_start_time.push_back(recovery_reply.decode_start_time());
          decode_end_time.push_back(recovery_reply.decode_end_time());
          network_start_time.push_back(recovery_reply.network_start_time());
          network_end_time.push_back(recovery_reply.network_end_time());
          grpc_start_time.push_back(recovery_reply.grpc_start_time());
          cross_rack_network_time = recovery_reply.cross_rack_time();
          cross_rack_xor_time = recovery_reply.cross_rack_xor_time();
          data_node_grpc_notify_time.push_back(recovery_reply.data_node_grpc_notify_time());
          data_node_grpc_start_time.push_back(recovery_reply.data_node_grpc_start_time());
          dest_data_node_network_time = recovery_reply.dest_data_node_network_time();
          dest_data_node_disk_io_time = recovery_reply.dest_data_node_disk_io_time();
          std::cout << "[Coordinator] recovery of " << stripe_id << "_" << failed_block_id << " success!" << std::endl;
        }
        else
        {
          std::cout << "[Coordinator] recovery of " << stripe_id << "_" << failed_block_id << " failed!" << std::endl;
        }
      }
      ));
      for(int i = 0; i < threads.size(); i++){
        threads[i].join();
      }
    }
    return true;
  }

  grpc::Status CoordinatorImpl::decodeTest(
    grpc::ServerContext *context,
    const coordinator_proto::KeyAndClientIP *keyClient,
    coordinator_proto::DegradedReadReply *degradedReadReply)
  {
    std::string code_type = m_sys_config->CodeType;
    int k = m_sys_config->k;
    int r = m_sys_config->r;
    int z = m_sys_config->z;
    int block_size = m_sys_config->BlockSize;
    int stripe_id = std::stoi(keyClient->key().substr(0, keyClient->key().find('_')));
    int failed_block_id = std::stoi(keyClient->key().substr(keyClient->key().find('_') + 1));
    Stripe &t_stripe = m_stripe_table[stripe_id];

    std::vector<int> recovery_group_ids = get_recovery_group_ids(code_type, k, r, z, failed_block_id);
    std::vector<int> recovery_block_ids;
    for(int i = 0; i < recovery_group_ids.size(); i++){
      std::vector<int> blockids = t_stripe.group_to_blocks[recovery_group_ids[i]];
      for(int j = 0; j < blockids.size(); j++){
        if(m_sys_config->CodeType == "AzureLRC" && recovery_block_ids.size() == (k / z))
          break;
        if ((m_sys_config->CodeType == "AzureLRC" && blockids[j] >= m_sys_config->k + m_sys_config->r) || blockids[j] == failed_block_id)
          continue;
        if(blockids[j] != failed_block_id){
          recovery_block_ids.push_back(blockids[j]);
        }
      }
    }
    int block_num = recovery_block_ids.size();
    unsigned char *recovery_data = static_cast<unsigned char*>(std::aligned_alloc(32, m_sys_config->BlockSize * block_num));
    std::vector<unsigned char *> recovery_data_ptrs;
    for(int i = 0; i < block_num; i++){
      recovery_data_ptrs.push_back(recovery_data + i * block_size);
    }
    
    unsigned char *res = static_cast<unsigned char*>(std::aligned_alloc(32, m_sys_config->BlockSize));
    std::chrono::high_resolution_clock::time_point start = std::chrono::high_resolution_clock::now();
    if(code_type == "AzureLRC"){
      decode_azure_lrc(k, r, z, block_num, &recovery_block_ids, recovery_data_ptrs.data(), res, block_size, failed_block_id);
    }
    else if(code_type == "UniLRC"){
      decode_unilrc(k, r, z, block_num, &recovery_block_ids, recovery_data_ptrs.data(), res, block_size);
    }
    else if(code_type == "OptimalLRC"){
      decode_optimal_lrc(k, r, z, block_num, &recovery_block_ids, recovery_data_ptrs.data(), res, block_size, failed_block_id);
    }
    else if(code_type == "UniformLRC"){
      decode_uniform_lrc(k, r, z, block_num, &recovery_block_ids, recovery_data_ptrs.data(), res, block_size, failed_block_id);
    }
    else if(code_type == "gLRC"){
      decode_glrc(k, r, z, block_num, &recovery_block_ids, recovery_data_ptrs.data(), res, block_size, failed_block_id);
    }
    else{
      std::cout << "[Coordinator] decodeTest: unknown code type!" << std::endl;
      return grpc::Status(grpc::INVALID_ARGUMENT, "unknown code type");
    }
    std::chrono::high_resolution_clock::time_point end = std::chrono::high_resolution_clock::now();
    std::chrono::duration<double> duration = std::chrono::duration_cast<std::chrono::duration<double>>(end - start);
    std::cout << "[Coordinator] decodeTest took " << duration.count() << " seconds" << std::endl;
    degradedReadReply->set_decode_time(duration.count());
    delete[] res;
    delete[] recovery_data;

    return grpc::Status::OK;
  } 

  bool CoordinatorImpl::recovery_one_block(int stripe_id, int failed_block_id)
  {
    std::string code_type = m_sys_config->CodeType;
    Stripe &t_stripe = m_stripe_table[stripe_id];
    std::vector<int> recovery_group_ids = get_recovery_group_ids(m_sys_config->CodeType, m_sys_config->k, m_sys_config->r, m_sys_config->z, failed_block_id);
    grpc::Status status;

    if (recovery_group_ids.size() == 1)
    {
      //assert((code_type == "UniLRC") || (code_type == "AzureLRC" && (failed_block_id < m_sys_config->k || failed_block_id >= m_sys_config->k + m_sys_config->r)));

      grpc::ClientContext recovery_context;
      proxy_proto::RecoveryRequest recovery_request;
      proxy_proto::RecoveryReply recovery_reply;

      int chosen_cluster_id = get_cluster_id_by_group_id(t_stripe, recovery_group_ids[0]);
      std::string chosen_proxy = m_cluster_table[chosen_cluster_id].proxy_ip + ":" + std::to_string(m_cluster_table[chosen_cluster_id].proxy_port);
      recovery_request.set_failed_block_id(failed_block_id);
      recovery_request.set_failed_block_key(t_stripe.blocks[failed_block_id]->block_key);
      int t_node_id = randomly_select_a_node(chosen_cluster_id, stripe_id);
      recovery_request.set_replaced_node_ip(m_node_table[t_node_id].node_ip);
      recovery_request.set_replaced_node_port(m_node_table[t_node_id].node_port);
      recovery_request.set_cross_rack_num(0);
      std::vector<int> blockids = t_stripe.group_to_blocks[recovery_group_ids[0]];
      for (int i = 0; i < int(blockids.size()); i++)
      {
        if (blockids[i] == failed_block_id)
          continue;

        Block *t_block = t_stripe.blocks[blockids[i]];
        recovery_request.add_datanodeip(m_node_table[t_block->map2node].node_ip);
        recovery_request.add_datanodeport(m_node_table[t_block->map2node].node_port);
        recovery_request.add_blockkeys(t_block->block_key);
        recovery_request.add_blockids(t_block->block_id);
      }

      status = m_proxy_ptrs[chosen_proxy]->recovery(&recovery_context, recovery_request, &recovery_reply);
      if (status.ok())
      {
        std::cout << "[Coordinator] recovery of " << stripe_id << "_" << failed_block_id << " success!" << std::endl;
        return true;
      }
      else
      {
        std::cout << "[Coordinator] recovery of " << stripe_id << "_" << failed_block_id << " failed!" << std::endl;
        return false;
      }
    }
    else
    {
      int dest_group_id = t_stripe.blocks[failed_block_id]->map2group;
      int dest_cluster_id = get_cluster_id_by_group_id(t_stripe, dest_group_id);
      std::string dest_proxy_ip = m_cluster_table[dest_cluster_id].proxy_ip;
      int dest_proxy_port = m_cluster_table[dest_cluster_id].proxy_port;
      std::vector<int> chosen_cluster_ids;
      for(int i = 0; i < recovery_group_ids.size(); i++){
        chosen_cluster_ids.push_back(get_cluster_id_by_group_id(t_stripe, recovery_group_ids[i]));
      }
      std::vector<std::string> chosen_proxies;
      for(int i = 0; i < chosen_cluster_ids.size(); i++){
        chosen_proxies.push_back(m_cluster_table[chosen_cluster_ids[i]].proxy_ip + ":" + std::to_string(m_cluster_table[chosen_cluster_ids[i]].proxy_port));
      }
      std::vector<std::thread> threads;
      for(int i = 0; i < recovery_group_ids.size(); i++){
        if(recovery_group_ids[i] == dest_group_id){
          continue;
        }
        threads.push_back(std::thread([&t_stripe, &chosen_proxies, &recovery_group_ids, i, failed_block_id, dest_proxy_ip, dest_proxy_port, this](){
          grpc::ClientContext degraded_read_context;
          proxy_proto::DegradedReadRequest degraded_read_request;
          proxy_proto::DegradedReadReply degraded_read_reply;
          degraded_read_request.set_clientip(dest_proxy_ip);
          degraded_read_request.set_clientport(dest_proxy_port + ECProject::PROXY_PORT_SHIFT);
          degraded_read_request.set_failed_block_id(failed_block_id);
          degraded_read_request.set_failed_block_key(t_stripe.blocks[failed_block_id]->block_key);
          std::vector<int> blockids = t_stripe.group_to_blocks[recovery_group_ids[i]];
          for (int j = 0; j < int(blockids.size()); j++)
          {
            if(m_sys_config->CodeType == "AzureLRC" && degraded_read_request.blockids_size() == (m_sys_config->k / m_sys_config->z))
              break;

            if ((m_sys_config->CodeType == "AzureLRC" && blockids[j] >= m_sys_config->k + m_sys_config->r) || blockids[j] == failed_block_id)
              continue;

            Block *t_block = t_stripe.blocks[blockids[j]];
            degraded_read_request.add_datanodeip(m_node_table[t_block->map2node].node_ip);
            degraded_read_request.add_datanodeport(m_node_table[t_block->map2node].node_port);
            degraded_read_request.add_blockkeys(t_block->block_key);
            degraded_read_request.add_blockids(t_block->block_id);
          }
          grpc::Status status = m_proxy_ptrs[chosen_proxies[i]]->degradedRead(&degraded_read_context, degraded_read_request, &degraded_read_reply);
          if (status.ok())
          {
            std::cout << "[Coordinator] partial degraded read of " << failed_block_id << " success!" << std::endl;
          }
          else
          {
            std::cout << "[Coordinator] partial degraded read of " << failed_block_id << " failed!" << std::endl;
          }
        }));

      }
      int cross_rack_num = recovery_group_ids.size() - 1;
      threads.push_back(std::thread([this, &t_stripe, cross_rack_num, dest_group_id, dest_cluster_id, dest_proxy_ip, dest_proxy_port, stripe_id, failed_block_id, &recovery_group_ids](){
        grpc::ClientContext recovery_context;
        proxy_proto::RecoveryRequest recovery_request;
        proxy_proto::RecoveryReply recovery_reply;
        recovery_request.set_failed_block_id(failed_block_id);
        recovery_request.set_failed_block_key(t_stripe.blocks[failed_block_id]->block_key);
        int t_node_id = randomly_select_a_node(dest_cluster_id, stripe_id);
        recovery_request.set_replaced_node_ip(m_node_table[t_node_id].node_ip);
        recovery_request.set_replaced_node_port(m_node_table[t_node_id].node_port);
        recovery_request.set_cross_rack_num(cross_rack_num);
        for(int i = 0; i < recovery_group_ids.size(); i++){
          if(recovery_group_ids[i] == dest_group_id){
            continue;
          }
          int cluster_id = get_cluster_id_by_group_id(t_stripe, recovery_group_ids[i]);
          std::string proxy_ip = m_cluster_table[cluster_id].proxy_ip;
          int proxy_port = m_cluster_table[cluster_id].proxy_port;
          recovery_request.add_proxyip(proxy_ip);
          recovery_request.add_proxyport(proxy_port);
        }
        std::vector<int> blockids = t_stripe.group_to_blocks[dest_group_id];
        for (int i = 0; i < int(blockids.size()); i++)
        {
          if(m_sys_config->CodeType == "AzureLRC" && recovery_request.blockids_size() == (m_sys_config->k / m_sys_config->z))
            break;

          if (blockids[i] == failed_block_id)
            continue;

          Block *t_block = t_stripe.blocks[blockids[i]];
          recovery_request.add_datanodeip(m_node_table[t_block->map2node].node_ip);
          recovery_request.add_datanodeport(m_node_table[t_block->map2node].node_port);
          recovery_request.add_blockkeys(t_block->block_key);
          recovery_request.add_blockids(t_block->block_id);
        }
        //std::cout << "[Coordinator] start recovery of " << stripe_id << "_" << failed_block_id << std::endl;
        grpc::Status status = m_proxy_ptrs[dest_proxy_ip + ":" + std::to_string(dest_proxy_port)]->recovery(&recovery_context, recovery_request, &recovery_reply);
        if (status.ok())
        {
          std::cout << "[Coordinator] recovery of " << stripe_id << "_" << failed_block_id << " success!" << std::endl;
        }
        else
        {
          std::cout << "[Coordinator] recovery of " << stripe_id << "_" << failed_block_id << " failed!" << std::endl;
        }
      }
      ));
      for(int i = 0; i < threads.size(); i++){
        threads[i].join();
      }
    }
    return true;
  }


  grpc::Status CoordinatorImpl::getRecoveryBreakdown(
      grpc::ServerContext *context,
      const coordinator_proto::KeyAndClientIP *keyClient,
      coordinator_proto::RecoveryReply *recoveryReply)
  {
    std::chrono::time_point<std::chrono::high_resolution_clock> START = std::chrono::high_resolution_clock::now();
    recoveryReply->set_grpc_start_time(std::chrono::duration_cast<std::chrono::duration<double>>(START.time_since_epoch()).count());
    int stripe_id = std::stoi(keyClient->key().substr(0, keyClient->key().find('_')));
    int failed_block_id = std::stoi(keyClient->key().substr(keyClient->key().find('_') + 1));
    std::vector<double> disk_io_start_time, disk_io_end_time;
    std::vector<double> decode_start_time, decode_end_time;
    std::vector<double> network_start_time, network_end_time;
    double cross_rack_network_time, cross_rack_xor_time;
    std::vector<double> grpc_notify_time, grpc_start_time;
    std::vector<double> data_node_grpc_notify_time, data_node_grpc_start_time;
    double dest_data_node_network_time, dest_data_node_disk_io_time;

    bool if_success = recovery_one_block_breakdown(stripe_id, failed_block_id, 
      disk_io_start_time, disk_io_end_time, decode_start_time, decode_end_time,
      network_start_time, network_end_time, cross_rack_network_time, cross_rack_xor_time,
      grpc_notify_time, grpc_start_time, data_node_grpc_notify_time, data_node_grpc_start_time,
      dest_data_node_network_time, dest_data_node_disk_io_time);

    if (if_success)
    {
      double max_disk_io_time = *std::max_element(disk_io_end_time.begin(), disk_io_end_time.end()) - *std::min_element(disk_io_start_time.begin(), disk_io_start_time.end());
      recoveryReply->set_disk_read_time(max_disk_io_time);
      double max_decode_time = *std::max_element(decode_end_time.begin(), decode_end_time.end()) - *std::min_element(decode_start_time.begin(), decode_start_time.end());
      recoveryReply->set_decode_time(max_decode_time + cross_rack_xor_time);
      double max_network_time = *std::max_element(network_end_time.begin(), network_end_time.end()) - *std::min_element(network_start_time.begin(), network_start_time.end());
      double max_grpc_delay = *std::max_element(grpc_start_time.begin(), grpc_start_time.end()) - *std::min_element(grpc_notify_time.begin(), grpc_notify_time.end());
      double max_data_node_grpc_delay = *std::max_element(data_node_grpc_start_time.begin(), data_node_grpc_start_time.end()) - *std::min_element(data_node_grpc_notify_time.begin(), data_node_grpc_notify_time.end());
      recoveryReply->set_network_time(max_network_time + cross_rack_network_time + dest_data_node_network_time + max_grpc_delay + max_data_node_grpc_delay);
      recoveryReply->set_disk_write_time(dest_data_node_disk_io_time);
      
      return grpc::Status::OK;
    }
    else
    {
      return grpc::Status(grpc::StatusCode::INTERNAL, "Recovery failed!");
    }
  }

  grpc::Status CoordinatorImpl::getRecovery(
      grpc::ServerContext *context,
      const coordinator_proto::KeyAndClientIP *keyClient,
      coordinator_proto::RecoveryReply *recoveryReply)
  {
    int stripe_id = std::stoi(keyClient->key().substr(0, keyClient->key().find('_')));
    int failed_block_id = std::stoi(keyClient->key().substr(keyClient->key().find('_') + 1));
    bool if_success = recovery_one_block(stripe_id, failed_block_id);

    if (if_success)
    {
      return grpc::Status::OK;
    }
    else
    {
      return grpc::Status(grpc::StatusCode::INTERNAL, "Recovery failed!");
    }
  }

  bool CoordinatorImpl::degraded_read_one_block_breakdown(int stripe_id, int failed_block_id, std::string client_ip, int client_port, 
    std::vector<double> &disk_io_start_time, std::vector<double> &disk_io_end_time, std::vector<double> &decode_start_time, std::vector<double> &decode_end_time,
    std::vector<double> &network_start_time, std::vector<double> &network_end_time, double &cross_rack_network_time, double &cross_rack_xor_time,
    std::vector<double> &grpc_notify_time, std::vector<double> &grpc_start_time, std::vector<double> &data_node_grpc_notify_time, std::vector<double> &data_node_grpc_start_time)
  {
    std::string code_type = m_sys_config->CodeType;
    Stripe &t_stripe = m_stripe_table[stripe_id];
    std::vector<int> recovery_group_ids = get_recovery_group_ids(m_sys_config->CodeType, m_sys_config->k, m_sys_config->r, m_sys_config->z, failed_block_id);
    grpc::Status status;

    if (recovery_group_ids.size() == 1)
    {
      //assert((code_type == "UniLRC") || (code_type == "AzureLRC" && (failed_block_id < m_sys_config->k || failed_block_id >= m_sys_config->k + m_sys_config->r)));

      grpc::ClientContext recovery_context;
      proxy_proto::RecoveryRequest recovery_request;
      proxy_proto::DegradedReadReply degraded_read_reply;

      int chosen_cluster_id = get_cluster_id_by_group_id(t_stripe, recovery_group_ids[0]);
      std::string chosen_proxy = m_cluster_table[chosen_cluster_id].proxy_ip + ":" + std::to_string(m_cluster_table[chosen_cluster_id].proxy_port);
      recovery_request.set_failed_block_id(failed_block_id);
      recovery_request.set_failed_block_key(t_stripe.blocks[failed_block_id]->block_key);
      recovery_request.set_replaced_node_ip(client_ip);
      recovery_request.set_replaced_node_port(client_port);
      recovery_request.set_cross_rack_num(0);
      std::vector<int> blockids = t_stripe.group_to_blocks[recovery_group_ids[0]];
      for (int i = 0; i < int(blockids.size()); i++)
      {
        if (blockids[i] == failed_block_id)
          continue;

        Block *t_block = t_stripe.blocks[blockids[i]];
        recovery_request.add_datanodeip(m_node_table[t_block->map2node].node_ip);
        recovery_request.add_datanodeport(m_node_table[t_block->map2node].node_port);
        recovery_request.add_blockkeys(t_block->block_key);
        recovery_request.add_blockids(t_block->block_id);
      }

      std::chrono::high_resolution_clock::time_point grpc_notify = std::chrono::high_resolution_clock::now();
      grpc_notify_time.push_back(std::chrono::duration_cast<std::chrono::duration<double>>(grpc_notify.time_since_epoch()).count());
      status = m_proxy_ptrs[chosen_proxy]->degradedRead2ClientBreakdown(&recovery_context, recovery_request, &degraded_read_reply);
      if (status.ok())
      {
        disk_io_start_time.push_back(degraded_read_reply.disk_io_start_time());
        disk_io_end_time.push_back(degraded_read_reply.disk_io_end_time());
        decode_start_time.push_back(degraded_read_reply.decode_start_time());
        decode_end_time.push_back(degraded_read_reply.decode_end_time());
        network_start_time.push_back(degraded_read_reply.network_start_time());
        network_end_time.push_back(degraded_read_reply.network_end_time());
        cross_rack_network_time = degraded_read_reply.cross_rack_time();
        cross_rack_xor_time = degraded_read_reply.cross_rack_xor_time();
        grpc_start_time.push_back(degraded_read_reply.grpc_start_time());
        data_node_grpc_notify_time.push_back(degraded_read_reply.data_node_grpc_notify_time());
        data_node_grpc_start_time.push_back(degraded_read_reply.data_node_grpc_start_time());
        std::cout << "[Coordinator] degraded read of " << stripe_id << "_" << failed_block_id << " success!" << std::endl;
        return true;
      }
      else
      {
        std::cout << "[Coordinator] degraded read of " << stripe_id << "_" << failed_block_id << " failed!" << std::endl;
        return false;
      }
    }
    else
    {
      int dest_group_id = t_stripe.blocks[failed_block_id]->map2group;
      int dest_cluster_id = get_cluster_id_by_group_id(t_stripe, dest_group_id);
      std::string dest_proxy_ip = m_cluster_table[dest_cluster_id].proxy_ip;
      int dest_proxy_port = m_cluster_table[dest_cluster_id].proxy_port;
      std::vector<int> chosen_cluster_ids;
      for(int i = 0; i < recovery_group_ids.size(); i++){
        chosen_cluster_ids.push_back(get_cluster_id_by_group_id(t_stripe, recovery_group_ids[i]));
      }
      std::vector<std::string> chosen_proxies;
      for(int i = 0; i < chosen_cluster_ids.size(); i++){
        chosen_proxies.push_back(m_cluster_table[chosen_cluster_ids[i]].proxy_ip + ":" + std::to_string(m_cluster_table[chosen_cluster_ids[i]].proxy_port));
      }
      std::vector<std::thread> threads;
      for(int i = 0; i < recovery_group_ids.size(); i++){
        if(recovery_group_ids[i] == dest_group_id){
          continue;
        }
        threads.push_back(std::thread([&t_stripe, &chosen_proxies, &recovery_group_ids, i, failed_block_id, dest_proxy_ip, dest_proxy_port, this, 
          &disk_io_start_time, &disk_io_end_time, &decode_start_time, &decode_end_time, &network_start_time, &network_end_time, 
          &grpc_notify_time, &grpc_start_time, &data_node_grpc_notify_time, &data_node_grpc_start_time](){
          grpc::ClientContext degraded_read_context;
          proxy_proto::DegradedReadRequest degraded_read_request;
          proxy_proto::DegradedReadReply degraded_read_reply;
          degraded_read_request.set_clientip(dest_proxy_ip);
          degraded_read_request.set_clientport(dest_proxy_port + ECProject::PROXY_PORT_SHIFT);
          degraded_read_request.set_failed_block_id(failed_block_id);
          degraded_read_request.set_failed_block_key(t_stripe.blocks[failed_block_id]->block_key);
          std::vector<int> blockids = t_stripe.group_to_blocks[recovery_group_ids[i]];
          for (int j = 0; j < int(blockids.size()); j++)
          {
            if(m_sys_config->CodeType == "AzureLRC" && degraded_read_request.blockids_size() == (m_sys_config->k / m_sys_config->z))
              break;

            if ((m_sys_config->CodeType == "AzureLRC" && blockids[j] >= m_sys_config->k + m_sys_config->r) || blockids[j] == failed_block_id)
              continue;

            Block *t_block = t_stripe.blocks[blockids[j]];
            degraded_read_request.add_datanodeip(this->m_node_table[t_block->map2node].node_ip);
            degraded_read_request.add_datanodeport(this->m_node_table[t_block->map2node].node_port);
            degraded_read_request.add_blockkeys(t_block->block_key);
            degraded_read_request.add_blockids(t_block->block_id);
          }
          std::cout << "[Coordinator] start partial degraded read of " << failed_block_id << std::endl;
          std::chrono::high_resolution_clock::time_point grpc_notify = std::chrono::high_resolution_clock::now();
          grpc_notify_time.push_back(std::chrono::duration_cast<std::chrono::duration<double>>(grpc_notify.time_since_epoch()).count());
          grpc::Status status = this->m_proxy_ptrs[chosen_proxies[i]]->degradedReadBreakdown(&degraded_read_context, degraded_read_request, &degraded_read_reply);
          if (status.ok())
          {
            disk_io_start_time.push_back(degraded_read_reply.disk_io_start_time());
            disk_io_end_time.push_back(degraded_read_reply.disk_io_end_time());
            decode_start_time.push_back(degraded_read_reply.decode_start_time());
            decode_end_time.push_back(degraded_read_reply.decode_end_time());
            network_start_time.push_back(degraded_read_reply.network_start_time());
            network_end_time.push_back(degraded_read_reply.network_end_time());
            grpc_start_time.push_back(degraded_read_reply.grpc_start_time());
            data_node_grpc_notify_time.push_back(degraded_read_reply.data_node_grpc_notify_time());
            data_node_grpc_start_time.push_back(degraded_read_reply.data_node_grpc_start_time());
            std::cout << "[Coordinator] partial degraded read of " << failed_block_id << " success!" << std::endl;
          }
          else
          {
            std::cout << "[Coordinator] partial degraded read of " << failed_block_id << " failed!" << std::endl;
          }
        }));

      }
      int cross_rack_num = recovery_group_ids.size() - 1;
      threads.push_back(std::thread([this, &t_stripe, cross_rack_num, dest_group_id, dest_cluster_id, dest_proxy_ip, dest_proxy_port, stripe_id, failed_block_id, client_ip, client_port, 
        &disk_io_start_time, &disk_io_end_time, &decode_start_time, &decode_end_time, &network_start_time, &network_end_time, &cross_rack_network_time, &cross_rack_xor_time,
        &grpc_notify_time, &grpc_start_time, &data_node_grpc_notify_time, &data_node_grpc_start_time](){
        grpc::ClientContext recovery_context;
        proxy_proto::RecoveryRequest recovery_request;
        proxy_proto::DegradedReadReply recovery_reply;
        recovery_request.set_failed_block_id(failed_block_id);
        recovery_request.set_failed_block_key(t_stripe.blocks[failed_block_id]->block_key);
        recovery_request.set_replaced_node_ip(client_ip);
        recovery_request.set_replaced_node_port(client_port);
        recovery_request.set_cross_rack_num(cross_rack_num);
        std::vector<int> blockids = t_stripe.group_to_blocks[dest_group_id];
        for (int i = 0; i < int(blockids.size()); i++)
        {
          if(m_sys_config->CodeType == "AzureLRC" && recovery_request.blockids_size() == (m_sys_config->k / m_sys_config->z))
            break;

          if (blockids[i] == failed_block_id)
            continue;

          Block *t_block = t_stripe.blocks[blockids[i]];
          recovery_request.add_datanodeip(this->m_node_table[t_block->map2node].node_ip);
          recovery_request.add_datanodeport(this->m_node_table[t_block->map2node].node_port);
          recovery_request.add_blockkeys(t_block->block_key);
          recovery_request.add_blockids(t_block->block_id);
        }
        std::cout << "[Coordinator] start recovery of " << stripe_id << "_" << failed_block_id << std::endl;
        std::chrono::high_resolution_clock::time_point grpc_notify = std::chrono::high_resolution_clock::now();
        grpc_notify_time.push_back(std::chrono::duration_cast<std::chrono::duration<double>>(grpc_notify.time_since_epoch()).count());
        grpc::Status status = this->m_proxy_ptrs[dest_proxy_ip + ":" + std::to_string(dest_proxy_port)]->degradedRead2ClientBreakdown(&recovery_context, recovery_request, &recovery_reply);
        if (status.ok())
        {
          disk_io_start_time.push_back(recovery_reply.disk_io_start_time());
          disk_io_end_time.push_back(recovery_reply.disk_io_end_time());
          decode_start_time.push_back(recovery_reply.decode_start_time());
          decode_end_time.push_back(recovery_reply.decode_end_time());
          network_start_time.push_back(recovery_reply.network_start_time());
          network_end_time.push_back(recovery_reply.network_end_time());
          cross_rack_network_time = recovery_reply.cross_rack_time();
          cross_rack_xor_time = recovery_reply.cross_rack_xor_time();
          grpc_start_time.push_back(recovery_reply.grpc_start_time());
          data_node_grpc_notify_time.push_back(recovery_reply.data_node_grpc_notify_time());
          data_node_grpc_start_time.push_back(recovery_reply.data_node_grpc_start_time());
          std::cout << "[Coordinator] degraded read of " << stripe_id << "_" << failed_block_id << " success!" << std::endl;
        }
        else
        {
          std::cout << "[Coordinator] degraded read of " << stripe_id << "_" << failed_block_id << " failed!" << std::endl;
        }
      }
      ));
      for(int i = 0; i < threads.size(); i++){
        threads[i].join();
      }
    }
    return true;
  }

  bool CoordinatorImpl::degraded_read_one_block(int stripe_id, int failed_block_id, std::string client_ip, int client_port)
  {
    std::string code_type = m_sys_config->CodeType;
    Stripe &t_stripe = m_stripe_table[stripe_id];
    std::vector<int> recovery_group_ids = get_recovery_group_ids(m_sys_config->CodeType, m_sys_config->k, m_sys_config->r, m_sys_config->z, failed_block_id);
    grpc::Status status;

    if (recovery_group_ids.size() == 1)
    {
      //assert((code_type == "UniLRC") || (code_type == "AzureLRC" && (failed_block_id < m_sys_config->k || failed_block_id >= m_sys_config->k + m_sys_config->r)));

      grpc::ClientContext recovery_context;
      proxy_proto::RecoveryRequest recovery_request;
      proxy_proto::DegradedReadReply degraded_read_reply;

      int chosen_cluster_id = get_cluster_id_by_group_id(t_stripe, recovery_group_ids[0]);
      std::string chosen_proxy = m_cluster_table[chosen_cluster_id].proxy_ip + ":" + std::to_string(m_cluster_table[chosen_cluster_id].proxy_port);
      recovery_request.set_failed_block_id(failed_block_id);
      recovery_request.set_failed_block_key(t_stripe.blocks[failed_block_id]->block_key);
      recovery_request.set_replaced_node_ip(client_ip);
      recovery_request.set_replaced_node_port(client_port);
      recovery_request.set_cross_rack_num(0);
      std::vector<int> blockids = t_stripe.group_to_blocks[recovery_group_ids[0]];
      for (int i = 0; i < int(blockids.size()); i++)
      {
        if (blockids[i] == failed_block_id)
          continue;

        Block *t_block = t_stripe.blocks[blockids[i]];
        recovery_request.add_datanodeip(m_node_table[t_block->map2node].node_ip);
        recovery_request.add_datanodeport(m_node_table[t_block->map2node].node_port);
        recovery_request.add_blockkeys(t_block->block_key);
        recovery_request.add_blockids(t_block->block_id);
      }
      status = m_proxy_ptrs[chosen_proxy]->degradedRead2Client(&recovery_context, recovery_request, &degraded_read_reply);
      if (status.ok())
      {
        std::cout << "[Coordinator] degraded read of " << stripe_id << "_" << failed_block_id << " success!" << std::endl;
        return true;
      }
      else
      {
        std::cout << "[Coordinator] degraded read of " << stripe_id << "_" << failed_block_id << " failed!" << std::endl;
        return false;
      }
    }
    else
    {
      int dest_group_id = t_stripe.blocks[failed_block_id]->map2group;
      int dest_cluster_id = get_cluster_id_by_group_id(t_stripe, dest_group_id);
      std::string dest_proxy_ip = m_cluster_table[dest_cluster_id].proxy_ip;
      int dest_proxy_port = m_cluster_table[dest_cluster_id].proxy_port;
      std::vector<int> chosen_cluster_ids;
      for(int i = 0; i < recovery_group_ids.size(); i++){
        chosen_cluster_ids.push_back(get_cluster_id_by_group_id(t_stripe, recovery_group_ids[i]));
      }
      std::vector<std::string> chosen_proxies;
      for(int i = 0; i < chosen_cluster_ids.size(); i++){
        chosen_proxies.push_back(m_cluster_table[chosen_cluster_ids[i]].proxy_ip + ":" + std::to_string(m_cluster_table[chosen_cluster_ids[i]].proxy_port));
      }
      std::vector<std::thread> threads;
      for(int i = 0; i < recovery_group_ids.size(); i++){
        if(recovery_group_ids[i] == dest_group_id){
          continue;
        }
        threads.push_back(std::thread([&t_stripe, &chosen_proxies, &recovery_group_ids, i, failed_block_id, dest_proxy_ip, dest_proxy_port, this](){
          grpc::ClientContext degraded_read_context;
          proxy_proto::DegradedReadRequest degraded_read_request;
          proxy_proto::DegradedReadReply degraded_read_reply;
          degraded_read_request.set_clientip(dest_proxy_ip);
          degraded_read_request.set_clientport(dest_proxy_port + ECProject::PROXY_PORT_SHIFT);
          degraded_read_request.set_failed_block_id(failed_block_id);
          degraded_read_request.set_failed_block_key(t_stripe.blocks[failed_block_id]->block_key);
          std::vector<int> blockids = t_stripe.group_to_blocks[recovery_group_ids[i]];
          for (int j = 0; j < int(blockids.size()); j++)
          {
            if(m_sys_config->CodeType == "AzureLRC" && degraded_read_request.blockids_size() == (m_sys_config->k / m_sys_config->z))
              break;

            if ((m_sys_config->CodeType == "AzureLRC" && blockids[j] >= m_sys_config->k + m_sys_config->r) || blockids[j] == failed_block_id)
              continue;

            Block *t_block = t_stripe.blocks[blockids[j]];
            degraded_read_request.add_datanodeip(this->m_node_table[t_block->map2node].node_ip);
            degraded_read_request.add_datanodeport(this->m_node_table[t_block->map2node].node_port);
            degraded_read_request.add_blockkeys(t_block->block_key);
            degraded_read_request.add_blockids(t_block->block_id);
          }
          std::cout << "[Coordinator] start partial degraded read of " << failed_block_id << std::endl;
          grpc::Status status = this->m_proxy_ptrs[chosen_proxies[i]]->degradedRead(&degraded_read_context, degraded_read_request, &degraded_read_reply);
          if (status.ok())
          {
            std::cout << "[Coordinator] partial degraded read of " << failed_block_id << " success!" << std::endl;
          }
          else
          {
            std::cout << "[Coordinator] partial degraded read of " << failed_block_id << " failed!" << std::endl;
          }
        }));

      }
      int cross_rack_num = recovery_group_ids.size() - 1;
      threads.push_back(std::thread([this, &t_stripe, cross_rack_num, dest_group_id, dest_cluster_id, dest_proxy_ip, dest_proxy_port, stripe_id, failed_block_id, client_ip, client_port](){
        grpc::ClientContext recovery_context;
        proxy_proto::RecoveryRequest recovery_request;
        proxy_proto::DegradedReadReply recovery_reply;
        recovery_request.set_failed_block_id(failed_block_id);
        recovery_request.set_failed_block_key(t_stripe.blocks[failed_block_id]->block_key);
        recovery_request.set_replaced_node_ip(client_ip);
        recovery_request.set_replaced_node_port(client_port);
        recovery_request.set_cross_rack_num(cross_rack_num);
        std::vector<int> blockids = t_stripe.group_to_blocks[dest_group_id];
        for (int i = 0; i < int(blockids.size()); i++)
        {
          if(m_sys_config->CodeType == "AzureLRC" && recovery_request.blockids_size() == (m_sys_config->k / m_sys_config->z))
            break;

          if (blockids[i] == failed_block_id)
            continue;

          Block *t_block = t_stripe.blocks[blockids[i]];
          recovery_request.add_datanodeip(this->m_node_table[t_block->map2node].node_ip);
          recovery_request.add_datanodeport(this->m_node_table[t_block->map2node].node_port);
          recovery_request.add_blockkeys(t_block->block_key);
          recovery_request.add_blockids(t_block->block_id);
        }
        std::cout << "[Coordinator] start recovery of " << stripe_id << "_" << failed_block_id << std::endl;
        grpc::Status status = this->m_proxy_ptrs[dest_proxy_ip + ":" + std::to_string(dest_proxy_port)]->degradedRead2Client(&recovery_context, recovery_request, &recovery_reply);
        if (status.ok())
        {
          std::cout << "[Coordinator] degraded read of " << stripe_id << "_" << failed_block_id << " success!" << std::endl;
        }
        else
        {
          std::cout << "[Coordinator] degraded read of " << stripe_id << "_" << failed_block_id << " failed!" << std::endl;
        }
      }
      ));
      for(int i = 0; i < threads.size(); i++){
        threads[i].join();
      }
    }
    return true;
  }

  bool CoordinatorImpl::degraded_read_one_block_for_workload(int stripe_id, int failed_block_id, std::string client_ip, int client_port, int block_id)
  {
    std::string code_type = m_sys_config->CodeType;
    Stripe &t_stripe = m_stripe_table[stripe_id];
    std::vector<int> recovery_group_ids = get_recovery_group_ids(m_sys_config->CodeType, m_sys_config->k, m_sys_config->r, m_sys_config->z, failed_block_id);
    grpc::Status status;

    if (recovery_group_ids.size() == 1)
    {
      //assert((code_type == "UniLRC") || (code_type == "AzureLRC" && (failed_block_id < m_sys_config->k || failed_block_id >= m_sys_config->k + m_sys_config->r)));

      grpc::ClientContext recovery_context;
      proxy_proto::RecoveryRequest recovery_request;
      proxy_proto::DegradedReadReply degraded_read_reply;

      int chosen_cluster_id = get_cluster_id_by_group_id(t_stripe, recovery_group_ids[0]);
      std::string chosen_proxy = m_cluster_table[chosen_cluster_id].proxy_ip + ":" + std::to_string(m_cluster_table[chosen_cluster_id].proxy_port);
      recovery_request.set_failed_block_id(failed_block_id);
      recovery_request.set_failed_block_key(t_stripe.blocks[failed_block_id]->block_key);
      recovery_request.set_replaced_node_ip(client_ip);
      recovery_request.set_replaced_node_port(client_port);
      recovery_request.set_cross_rack_num(0);
      recovery_request.set_is_to_send_block_id(true);
      recovery_request.set_block_id_to_send(block_id);
      std::vector<int> blockids = t_stripe.group_to_blocks[recovery_group_ids[0]];
      for (int i = 0; i < int(blockids.size()); i++)
      {
        if (blockids[i] == failed_block_id)
          continue;

        Block *t_block = t_stripe.blocks[blockids[i]];
        recovery_request.add_datanodeip(m_node_table[t_block->map2node].node_ip);
        recovery_request.add_datanodeport(m_node_table[t_block->map2node].node_port);
        recovery_request.add_blockkeys(t_block->block_key);
        recovery_request.add_blockids(t_block->block_id);
      }
      status = m_proxy_ptrs[chosen_proxy]->degradedRead2Client(&recovery_context, recovery_request, &degraded_read_reply);
      if (status.ok())
      {
        std::cout << "[Coordinator] degraded read of " << stripe_id << "_" << failed_block_id << " success!" << std::endl;
        return true;
      }
      else
      {
        std::cout << "[Coordinator] degraded read of " << stripe_id << "_" << failed_block_id << " failed!" << std::endl;
        return false;
      }
    }
    else
    {
      int dest_group_id = t_stripe.blocks[failed_block_id]->map2group;
      int dest_cluster_id = get_cluster_id_by_group_id(t_stripe, dest_group_id);
      std::string dest_proxy_ip = m_cluster_table[dest_cluster_id].proxy_ip;
      int dest_proxy_port = m_cluster_table[dest_cluster_id].proxy_port;
      std::vector<int> chosen_cluster_ids;
      for(int i = 0; i < recovery_group_ids.size(); i++){
        chosen_cluster_ids.push_back(get_cluster_id_by_group_id(t_stripe, recovery_group_ids[i]));
      }
      std::vector<std::string> chosen_proxies;
      for(int i = 0; i < chosen_cluster_ids.size(); i++){
        chosen_proxies.push_back(m_cluster_table[chosen_cluster_ids[i]].proxy_ip + ":" + std::to_string(m_cluster_table[chosen_cluster_ids[i]].proxy_port));
      }
      std::vector<std::thread> threads;
      for(int i = 0; i < recovery_group_ids.size(); i++){
        if(recovery_group_ids[i] == dest_group_id){
          continue;
        }
        threads.push_back(std::thread([&t_stripe, &chosen_proxies, &recovery_group_ids, i, failed_block_id, dest_proxy_ip, dest_proxy_port, this](){
          grpc::ClientContext degraded_read_context;
          proxy_proto::DegradedReadRequest degraded_read_request;
          proxy_proto::DegradedReadReply degraded_read_reply;
          degraded_read_request.set_clientip(dest_proxy_ip);
          degraded_read_request.set_clientport(dest_proxy_port + ECProject::PROXY_PORT_SHIFT);
          degraded_read_request.set_failed_block_id(failed_block_id);
          degraded_read_request.set_failed_block_key(t_stripe.blocks[failed_block_id]->block_key);
          std::vector<int> blockids = t_stripe.group_to_blocks[recovery_group_ids[i]];
          for (int j = 0; j < int(blockids.size()); j++)
          {
            if(m_sys_config->CodeType == "AzureLRC" && degraded_read_request.blockids_size() == (m_sys_config->k / m_sys_config->z))
              break;

            if ((m_sys_config->CodeType == "AzureLRC" && blockids[j] >= m_sys_config->k + m_sys_config->r) || blockids[j] == failed_block_id)
              continue;

            Block *t_block = t_stripe.blocks[blockids[j]];
            degraded_read_request.add_datanodeip(this->m_node_table[t_block->map2node].node_ip);
            degraded_read_request.add_datanodeport(this->m_node_table[t_block->map2node].node_port);
            degraded_read_request.add_blockkeys(t_block->block_key);
            degraded_read_request.add_blockids(t_block->block_id);
          }
          std::cout << "[Coordinator] start partial degraded read of " << failed_block_id << std::endl;
          grpc::Status status = this->m_proxy_ptrs[chosen_proxies[i]]->degradedRead(&degraded_read_context, degraded_read_request, &degraded_read_reply);
          if (status.ok())
          {
            std::cout << "[Coordinator] partial degraded read of " << failed_block_id << " success!" << std::endl;
          }
          else
          {
            std::cout << "[Coordinator] partial degraded read of " << failed_block_id << " failed!" << std::endl;
          }
        }));

      }
      int cross_rack_num = recovery_group_ids.size() - 1;
      threads.push_back(std::thread([this, &t_stripe, cross_rack_num, dest_group_id, dest_cluster_id, dest_proxy_ip, dest_proxy_port, stripe_id, failed_block_id, client_ip, client_port, block_id](){
        grpc::ClientContext recovery_context;
        proxy_proto::RecoveryRequest recovery_request;
        proxy_proto::DegradedReadReply recovery_reply;
        recovery_request.set_failed_block_id(failed_block_id);
        recovery_request.set_failed_block_key(t_stripe.blocks[failed_block_id]->block_key);
        recovery_request.set_replaced_node_ip(client_ip);
        recovery_request.set_replaced_node_port(client_port);
        recovery_request.set_cross_rack_num(cross_rack_num);
        recovery_request.set_is_to_send_block_id(true);
        recovery_request.set_block_id_to_send(block_id);
        std::vector<int> blockids = t_stripe.group_to_blocks[dest_group_id];
        for (int i = 0; i < int(blockids.size()); i++)
        {
          if(m_sys_config->CodeType == "AzureLRC" && recovery_request.blockids_size() == (m_sys_config->k / m_sys_config->z))
            break;

          if (blockids[i] == failed_block_id)
            continue;

          Block *t_block = t_stripe.blocks[blockids[i]];
          recovery_request.add_datanodeip(this->m_node_table[t_block->map2node].node_ip);
          recovery_request.add_datanodeport(this->m_node_table[t_block->map2node].node_port);
          recovery_request.add_blockkeys(t_block->block_key);
          recovery_request.add_blockids(t_block->block_id);
        }
        std::cout << "[Coordinator] start recovery of " << stripe_id << "_" << failed_block_id << std::endl;
        grpc::Status status = this->m_proxy_ptrs[dest_proxy_ip + ":" + std::to_string(dest_proxy_port)]->degradedRead2Client(&recovery_context, recovery_request, &recovery_reply);
        if (status.ok())
        {
          std::cout << "[Coordinator] degraded read of " << stripe_id << "_" << failed_block_id << " success!" << std::endl;
        }
        else
        {
          std::cout << "[Coordinator] degraded read of " << stripe_id << "_" << failed_block_id << " failed!" << std::endl;
        }
      }
      ));
      for(int i = 0; i < threads.size(); i++){
        threads[i].join();
      }
    }
    return true;
  }


  grpc::Status CoordinatorImpl::getDegradedReadBlockBreakdown(
    grpc::ServerContext *context,
    const coordinator_proto::KeyAndClientIP *keyClient,
    coordinator_proto::DegradedReadReply *degradedReadReply)
  {
    std::chrono::high_resolution_clock::time_point start = std::chrono::high_resolution_clock::now();
    double start_time = std::chrono::duration_cast<std::chrono::duration<double>>(start.time_since_epoch()).count();
    degradedReadReply->set_grpc_start_time(start_time);
    std::cout << start_time << std::endl;
    int stripe_id = std::stoi(keyClient->key().substr(0, keyClient->key().find('_')));
    int failed_block_id = std::stoi(keyClient->key().substr(keyClient->key().find('_') + 1));
    std::string client_ip = keyClient->clientip();
    int client_port = keyClient->clientport();
    std::vector<double> disk_io_start_time;
    std::vector<double> disk_io_end_time;
    std::vector<double> decode_start_time;
    std::vector<double> decode_end_time;
    std::vector<double> network_start_time;
    std::vector<double> network_end_time;
    std::vector<double> grpc_notify_time;
    std::vector<double> grpc_start_time;
    std::vector<double> data_node_grpc_notify_time;
    std::vector<double> data_node_grpc_start_time;
    double cross_rack_network_time;
    double cross_rack_xor_time;

    /*std::thread t(&CoordinatorImpl::degraded_read_one_block, this, stripe_id, failed_block_id, client_ip, client_port);
    t.join();*/
    bool if_success = degraded_read_one_block_breakdown(stripe_id, failed_block_id, client_ip, client_port,
      disk_io_start_time, disk_io_end_time, decode_start_time, decode_end_time,
      network_start_time, network_end_time, cross_rack_network_time, cross_rack_xor_time,
      grpc_notify_time, grpc_start_time, data_node_grpc_notify_time, data_node_grpc_start_time); 
    if (if_success)
    {
      double max_disk_io_time = *std::max_element(disk_io_end_time.begin(), disk_io_end_time.end()) - *std::min_element(disk_io_start_time.begin(), disk_io_start_time.end());
      double max_decode_time = *std::max_element(decode_end_time.begin(), decode_end_time.end()) - *std::min_element(decode_start_time.begin(), decode_start_time.end()) + cross_rack_xor_time;
      double max_network_time = *std::max_element(network_end_time.begin(), network_end_time.end()) - *std::min_element(network_start_time.begin(), network_start_time.end()) + cross_rack_network_time;
      max_network_time += (*std::max_element(grpc_start_time.begin(), grpc_start_time.end()) - *std::min_element(grpc_notify_time.begin(), grpc_notify_time.end()));
      max_network_time += (*std::max_element(data_node_grpc_start_time.begin(), data_node_grpc_start_time.end()) - *std::min_element(data_node_grpc_notify_time.begin(), data_node_grpc_notify_time.end()));

      degradedReadReply->set_disk_io_time(max_disk_io_time);
      degradedReadReply->set_decode_time(max_decode_time);
      degradedReadReply->set_network_time(max_network_time);
      return grpc::Status::OK;
    }
    else
    {
      return grpc::Status(grpc::StatusCode::INTERNAL, "Degraded read failed!");
    }
  }


  grpc::Status CoordinatorImpl::getDegradedReadBlock(
      grpc::ServerContext *context,
      const coordinator_proto::KeyAndClientIP *keyClient,
      coordinator_proto::DegradedReadReply *degradedReadReply)
  {
    int stripe_id = std::stoi(keyClient->key().substr(0, keyClient->key().find('_')));
    int failed_block_id = std::stoi(keyClient->key().substr(keyClient->key().find('_') + 1));
    std::string client_ip = keyClient->clientip();
    int client_port = keyClient->clientport();

    double dest_proxy_network_time;
    bool if_success = degraded_read_one_block(stripe_id, failed_block_id, client_ip, client_port);
    if (if_success)
    {
      return grpc::Status::OK;
    }
    else
    {
      return grpc::Status(grpc::StatusCode::INTERNAL, "Degraded read failed!");
    }
  }

  grpc::Status CoordinatorImpl::fullNodeRecovery(
    grpc::ServerContext *context,
    const coordinator_proto::NodeIdFromClient *request,
    coordinator_proto::RepBlockNum* response)
  {
    int node_id = request->node_id();
    std::string node_ip = m_node_table[node_id].node_ip;
    int node_port = m_node_table[node_id].node_port;
    std::vector<int> stripe_ids;
    std::vector<int> block_ids;
    for (auto it = m_stripe_table.begin(); it != m_stripe_table.end(); it++)
    {
      for (int i = 0; i < int(it->second.blocks.size()); i++)
      {
        if (it->second.blocks[i]->map2node == node_id)
        {
          stripe_ids.push_back(it->first);
          block_ids.push_back(it->second.blocks[i]->block_id);
        }
      }
    }
    if(stripe_ids.size() == 0){
      std::cout << "[Coordinator] no blocks on node " << node_id << std::endl;
      return grpc::Status::OK;
    }
    std::cout << "[Coordinator] start full node recovery of " << node_id << " containing " << block_ids.size() << " blocks" << std::endl;
    response->set_block_num(block_ids.size());
    //recovery_full_node(stripe_ids, block_ids);
    //std::vector<std::thread> recovery_threads;
    std::vector<bool> recovery_results(stripe_ids.size(), false);
    for (int i = 0; i < stripe_ids.size(); i++) {
        bool result = this->recovery_one_block(stripe_ids[i], block_ids[i]);
        recovery_results[i] = result; // 
    }
  
        
    // 
    bool all_success = std::all_of(recovery_results.begin(), recovery_results.end(), [](bool res) { return res; });
    if (all_success) {
        std::cout << "All recovery operations succeeded!" << std::endl;
    } else {
        std::cout << "Some recovery operations failed!" << std::endl;
    }


    /*bool ifSuccess = recovery_full_node(stripe_ids, block_ids);
    if (ifSuccess)
    {
      return grpc::Status::OK;
    }
    else
    {
      return grpc::Status(grpc::StatusCode::INTERNAL, "Full node recovery failed!");
    }*/
    return grpc::Status::OK;
  } 

  bool CoordinatorImpl::recovery_glrc_ilp_breakdown(int stripe_id, const std::vector<int> &failed_block_ids,
                                                    coordinator_proto::RecoveryReply *recovery_reply)
  {
    if (m_sys_config->CodeType != "gLRC")
    {
      recovery_reply->set_message("recovery_glrc_ilp_breakdown requires CodeType gLRC");
      recovery_reply->set_success(false);
      return false;
    }
    const auto repair_start = std::chrono::high_resolution_clock::now();
    Stripe &t_stripe = m_stripe_table[stripe_id];
    for (int fid : failed_block_ids)
    {
      if (fid < 0 || fid >= (int)t_stripe.blocks.size() || t_stripe.blocks[fid] == nullptr)
      {
        recovery_reply->set_message("invalid failed block id: " + std::to_string(fid));
        recovery_reply->set_success(false);
        return false;
      }
    }
    const std::string equation_policy =
        glrc_normalize_equation_policy(m_sys_config->GlrcEquationPolicy);
    GlrcIlpRepairPlan plan;
    if (!glrc_solve_repair_plan(m_sys_config->k, m_sys_config->r, m_sys_config->z, failed_block_ids,
                                equation_policy, plan))
    {
      recovery_reply->set_message(plan.error_message);
      recovery_reply->set_success(false);
      recovery_reply->set_ilp_time(plan.ilp_solve_time_sec);
      recovery_reply->set_equation_policy(equation_policy);
      std::cout << "[Coordinator] gLRC repair plan failed (" << equation_policy
                << "): " << plan.error_message << std::endl;
      return false;
    }

    recovery_reply->set_ilp_time(plan.ilp_solve_time_sec);
    recovery_reply->set_helper_block_count(plan.helper_block_count);
    recovery_reply->set_equation_policy(equation_policy);
    for (const auto &eq : plan.selected_equations)
      recovery_reply->add_selected_equations(eq);
    for (int hid : plan.helper_block_ids)
      recovery_reply->add_helper_block_ids(hid);

    const int ck = m_sys_config->k, cr = m_sys_config->r, cz = m_sys_config->z;
    std::cout << "[Coordinator] gLRC repair plan (" << equation_policy << "): f=" << failed_block_ids.size()
              << " failed_blocks=" << glrc_format_block_list(failed_block_ids, ck, cr, cz)
              << " helpers=" << plan.helper_block_count << " repair_equations:";
    for (const auto &eq : plan.selected_equations)
      std::cout << " " << eq;
    std::cout << std::endl;

    // Phase1: pull ALL ILP helpers to the repair proxy paired with the anchor failed
    // block's datanode (first failed id). Each datanode has its own proxy process;
    // encode/append still use the legacy cluster proxy, but gLRC ILP repair runs here.
    const int anchor_failed_id = failed_block_ids[0];
    const int head_node = t_stripe.blocks[anchor_failed_id]->map2node;
    std::string chosen_proxy = repair_proxy_key_for_node(head_node);
    std::cout << "[Coordinator] gLRC ILP Phase1: anchor="
              << glrc_block_label(anchor_failed_id, ck, cr, cz) << " repair_proxy=" << chosen_proxy
              << " (all " << plan.helper_block_count << " helpers -> single proxy)" << std::endl;

    auto proxy_it = m_proxy_ptrs.find(chosen_proxy);
    if (proxy_it == m_proxy_ptrs.end() || !proxy_it->second)
    {
      recovery_reply->set_message("repair proxy stub not found: " + chosen_proxy);
      recovery_reply->set_success(false);
      return false;
    }

    grpc::ClientContext recovery_context;
    recovery_context.set_deadline(std::chrono::system_clock::now() + std::chrono::seconds(600));
    proxy_proto::RecoveryRequest recovery_request;
    proxy_proto::RecoveryReply recovery_reply_proxy;

    recovery_request.set_glrc_ilp_recovery(true);
    recovery_request.set_cross_rack_num(0);
    recovery_request.set_failed_block_id(anchor_failed_id);
    recovery_request.set_failed_block_key(t_stripe.blocks[anchor_failed_id]->block_key);

    for (int fid : failed_block_ids)
    {
      recovery_request.add_failed_block_ids(fid);
      recovery_request.add_failed_block_keys(t_stripe.blocks[fid]->block_key);
      int node_id = t_stripe.blocks[fid]->map2node;
      recovery_request.add_replaced_node_ips(m_node_table[node_id].node_ip);
      recovery_request.add_replaced_node_ports(m_node_table[node_id].node_port);
    }
    for (const auto &eq : plan.selected_equations)
      recovery_request.add_selected_equations(eq);
    for (int eq_idx : plan.selected_equation_indices)
      recovery_request.add_selected_equation_indices(eq_idx);

    for (int hid : plan.helper_block_ids)
    {
      Block *t_block = t_stripe.blocks[hid];
      recovery_request.add_datanodeip(m_node_table[t_block->map2node].node_ip);
      recovery_request.add_datanodeport(m_node_table[t_block->map2node].node_port);
      recovery_request.add_blockkeys(t_block->block_key);
      recovery_request.add_blockids(t_block->block_id);
    }

    grpc::Status status =
        proxy_it->second->recoveryBreakdown(&recovery_context, recovery_request, &recovery_reply_proxy);
    if (!status.ok())
    {
      recovery_reply->set_message("proxy recoveryBreakdown failed: " + status.error_message());
      recovery_reply->set_success(false);
      return false;
    }

    double max_disk_io_time = breakdown_metric_span(recovery_reply_proxy.disk_io_start_time(),
                                                    recovery_reply_proxy.disk_io_end_time());
    double max_decode_time = breakdown_metric_span(recovery_reply_proxy.decode_start_time(),
                                                   recovery_reply_proxy.decode_end_time());
    double max_network_time = breakdown_metric_span(recovery_reply_proxy.network_start_time(),
                                                    recovery_reply_proxy.network_end_time());
    double dest_data_node_network_time = recovery_reply_proxy.dest_data_node_network_time();
    double dest_data_node_disk_io_time = recovery_reply_proxy.dest_data_node_disk_io_time();

    recovery_reply->set_disk_read_time(max_disk_io_time);
    recovery_reply->set_decode_time(max_decode_time + recovery_reply_proxy.cross_rack_xor_time());
    recovery_reply->set_network_time(max_network_time + recovery_reply_proxy.cross_rack_time() +
                                     dest_data_node_network_time);
    recovery_reply->set_disk_write_time(dest_data_node_disk_io_time);
    recovery_reply->set_repair_mode("phase1");
    recovery_reply->set_success(true);
    recovery_reply->set_message("ok");
    const auto repair_end = std::chrono::high_resolution_clock::now();
    recovery_reply->set_total_time(
        std::chrono::duration<double>(repair_end - repair_start).count());
    std::cout << "[Coordinator] gLRC ILP Phase1 recovery of stripe " << stripe_id << " success via "
              << chosen_proxy << std::endl;
    return true;
  }

  bool CoordinatorImpl::recovery_glrc_ilp_phase2_breakdown(int stripe_id,
                                                         const std::vector<int> &failed_block_ids,
                                                         coordinator_proto::RecoveryReply *recovery_reply)
  {
    if (m_sys_config->CodeType != "gLRC")
    {
      recovery_reply->set_message("recovery_glrc_ilp_phase2 requires CodeType gLRC");
      recovery_reply->set_success(false);
      return false;
    }
    const auto repair_start = std::chrono::high_resolution_clock::now();
    double orchestration_wait_sec = 0.0;
    auto orchestration_sleep = [&](int ms) {
      orchestration_wait_sec += static_cast<double>(ms) / 1000.0;
      std::this_thread::sleep_for(std::chrono::milliseconds(ms));
    };
    std::lock_guard<std::mutex> phase2_recovery_lock(g_glrc_phase2_recovery_mutex);
    Stripe &t_stripe = m_stripe_table[stripe_id];
    for (int fid : failed_block_ids)
    {
      if (fid < 0 || fid >= (int)t_stripe.blocks.size() || t_stripe.blocks[fid] == nullptr)
      {
        recovery_reply->set_message("invalid failed block id: " + std::to_string(fid));
        recovery_reply->set_success(false);
        return false;
      }
    }

    const std::string equation_policy =
        glrc_normalize_equation_policy(m_sys_config->GlrcEquationPolicy);
    GlrcIlpRepairPlan plan;
    if (!glrc_solve_repair_plan(m_sys_config->k, m_sys_config->r, m_sys_config->z, failed_block_ids,
                                equation_policy, plan))
    {
      recovery_reply->set_message(plan.error_message);
      recovery_reply->set_success(false);
      recovery_reply->set_ilp_time(plan.ilp_solve_time_sec);
      recovery_reply->set_equation_policy(equation_policy);
      return false;
    }

    const int f = (int)failed_block_ids.size();
    const int shard_count = m_sys_config->GlrcShardCount;
    std::vector<int> failed_cluster_ids;
    std::vector<std::string> failed_proxy_ips;
    std::vector<int> failed_proxy_ports;
    for (int fid : failed_block_ids)
    {
      const int nid = t_stripe.blocks[fid]->map2node;
      failed_cluster_ids.push_back(m_node_table[nid].cluster_id);
      failed_proxy_ips.push_back(m_node_table[nid].repair_proxy_ip);
      failed_proxy_ports.push_back(m_node_table[nid].repair_proxy_port);
    }

    GlrcPhase2ShardPlan shard_plan;
    std::string shard_err;
    if (!GlrcPhase2ShardPlan::build(shard_count, m_sys_config->BlockSize, failed_block_ids, failed_cluster_ids,
                                    failed_proxy_ips, failed_proxy_ports, shard_plan, shard_err))
    {
      recovery_reply->set_message(shard_err);
      recovery_reply->set_success(false);
      recovery_reply->set_ilp_time(plan.ilp_solve_time_sec);
      return false;
    }

    recovery_reply->set_ilp_time(plan.ilp_solve_time_sec);
    recovery_reply->set_helper_block_count(plan.helper_block_count);
    recovery_reply->set_repair_mode("phase2");
    recovery_reply->set_equation_policy(equation_policy);
    for (const auto &eq : plan.selected_equations)
      recovery_reply->add_selected_equations(eq);
    for (int hid : plan.helper_block_ids)
      recovery_reply->add_helper_block_ids(hid);
    int max_shards = 0;
    for (const auto &p : shard_plan.partitions)
    {
      recovery_reply->add_partition_shard_counts(p.shard_count);
      max_shards = std::max(max_shards, p.shard_count);
    }
    recovery_reply->set_max_partition_shard_count(max_shards);

    const int ck = m_sys_config->k, cr = m_sys_config->r, cz = m_sys_config->z;
    std::vector<int> partition_shards;
    partition_shards.reserve(f);
    for (const auto &p : shard_plan.partitions)
      partition_shards.push_back(p.shard_count);
    const double est_bw = m_sys_config->NodeBlockBandwidthMBps;
    const double est_system_net =
        (est_bw > 0.0)
            ? glrc_phase2_est_system_network_sec(plan.helper_block_count, f, partition_shards,
                                                 shard_plan.stripe_byte_len, est_bw)
            : 0.0;
    std::cout << "[Coordinator] gLRC Phase2 plan (" << equation_policy << "): f=" << f
              << " failed_blocks=" << glrc_format_block_list(failed_block_ids, ck, cr, cz)
              << " helpers=" << plan.helper_block_count << " repair_equations:";
    for (const auto &eq : plan.selected_equations)
      std::cout << " " << eq;
    std::cout << " shards=" << shard_count << " block_bw=" << est_bw << "MB/s"
              << " est_network_max_block>=" << est_system_net
              << "s (H*shards/BW read + (f-1)*shards/BW exchange duplex, max block):";
    for (const auto &p : shard_plan.partitions)
    {
      const double est_b =
          (est_bw > 0.0)
              ? glrc_phase2_est_block_network_sec(plan.helper_block_count, f, p.shard_count,
                                                  shard_plan.stripe_byte_len, est_bw)
              : 0.0;
      std::cout << " " << glrc_block_label(p.failed_block_id, ck, cr, cz) << "(" << p.shard_count
                << "shards,est~" << est_b << "s)";
    }
    std::cout << std::endl;

    struct PartitionOutcome
    {
      bool ok = false;
      proxy_proto::RecoveryReply reply;
    };
    std::vector<PartitionOutcome> outcomes(f);
    const uint32_t exchange_epoch = g_glrc_phase2_exchange_epoch.fetch_add(1);

    auto run_partition = [&](int pi) {
      const GlrcPartitionShardPlan &part = shard_plan.partitions[pi];
      std::string chosen_proxy = part.proxy_ip + ":" + std::to_string(part.proxy_port);
      auto proxy_it = m_proxy_ptrs.find(chosen_proxy);
      if (proxy_it == m_proxy_ptrs.end() || !proxy_it->second)
      {
        outcomes[pi].ok = false;
        std::cout << "[Coordinator] Phase2 partition " << pi << " ("
                  << glrc_block_label(part.failed_block_id, m_sys_config->k, m_sys_config->r, m_sys_config->z)
                  << ") proxy not found: " << chosen_proxy << std::endl;
        return;
      }

      grpc::ClientContext ctx;
      ctx.set_deadline(std::chrono::system_clock::now() + std::chrono::seconds(300));
      proxy_proto::RecoveryRequest req;
      proxy_proto::RecoveryReply &rep = outcomes[pi].reply;

      req.set_glrc_ilp_recovery(true);
      req.set_glrc_ilp_phase2(true);
      req.set_cross_rack_num(0);
      req.set_phase2_partition_id(pi);
      req.set_phase2_shard_count(shard_plan.shard_count);
      req.set_phase2_stripe_byte_len(shard_plan.stripe_byte_len);
      req.set_phase2_shard_begin(part.shard_begin);
      req.set_phase2_shard_count_local(part.shard_count);
      req.set_phase2_byte_off(part.byte_off);
      req.set_phase2_byte_len(part.byte_len);
      req.set_phase2_do_write_back(m_sys_config->GlrcPhase2WriteBack);
      req.set_phase2_exchange_epoch(static_cast<int32_t>(exchange_epoch));

      for (int fid : failed_block_ids)
      {
        req.add_failed_block_ids(fid);
        req.add_failed_block_keys(t_stripe.blocks[fid]->block_key);
        int node_id = t_stripe.blocks[fid]->map2node;
        req.add_replaced_node_ips(m_node_table[node_id].node_ip);
        req.add_replaced_node_ports(m_node_table[node_id].node_port);
      }
      for (const auto &eq : plan.selected_equations)
        req.add_selected_equations(eq);
      for (int eq_idx : plan.selected_equation_indices)
        req.add_selected_equation_indices(eq_idx);
      for (int hid : plan.helper_block_ids)
      {
        Block *t_block = t_stripe.blocks[hid];
        req.add_datanodeip(m_node_table[t_block->map2node].node_ip);
        req.add_datanodeport(m_node_table[t_block->map2node].node_port);
        req.add_blockkeys(t_block->block_key);
        req.add_blockids(t_block->block_id);
      }

      for (int pj = 0; pj < (int)shard_plan.partitions.size(); pj++)
      {
        if (pj == pi)
          continue;
        const GlrcPartitionShardPlan &peer = shard_plan.partitions[pj];
        req.add_phase2_peer_proxy_ips(peer.proxy_ip);
        req.add_phase2_peer_proxy_ports(peer.proxy_port);
        req.add_phase2_peer_partition_ids(pj);
        req.add_phase2_peer_shard_begins(peer.shard_begin);
        req.add_phase2_peer_shard_counts(peer.shard_count);
      }

      grpc::Status st = proxy_it->second->recoveryBreakdown(&ctx, req, &rep);
      outcomes[pi].ok = st.ok();
      if (!st.ok())
      {
        std::cout << "[Coordinator] Phase2 partition " << pi << " grpc error: " << st.error_message()
                  << std::endl;
      }
    };

    std::vector<std::thread> partition_threads;
    partition_threads.reserve(f);
    std::vector<bool> partition_started(f, false);
    // Phase A: listener partitions (id >= 1) bind exchange acceptors first.
    if (f >= 2)
    {
      for (int pi = f - 1; pi >= 1; pi--)
      {
        partition_threads.emplace_back(run_partition, pi);
        partition_started[pi] = true;
        if (pi > 1)
          orchestration_sleep(100);
      }
      orchestration_sleep(800);
    }
    // Phase B: client-heavy partition 0 (and f==1) starts after acceptors are ready.
    for (int pi = 0; pi < f; pi++)
    {
      if (!partition_started[pi])
        partition_threads.emplace_back(run_partition, pi);
    }
    for (auto &th : partition_threads)
      th.join();

    orchestration_sleep(500);

    bool all_ok = true;
    double max_disk = 0.0, max_net = 0.0, max_decode = 0.0, sum_write_net = 0.0, sum_write_disk = 0.0;
    for (int pi = 0; pi < f; pi++)
    {
      if (!outcomes[pi].ok)
      {
        all_ok = false;
        continue;
      }
      const auto &rep = outcomes[pi].reply;
      max_disk = std::max(max_disk, breakdown_metric_span(rep.disk_io_start_time(), rep.disk_io_end_time()));
      max_net = std::max(max_net, breakdown_metric_span(rep.network_start_time(), rep.network_end_time()) +
                                    rep.cross_rack_time() + rep.dest_data_node_network_time());
      max_decode = std::max(max_decode, breakdown_metric_span(rep.decode_start_time(), rep.decode_end_time()) +
                                         rep.cross_rack_xor_time());
      sum_write_net += rep.dest_data_node_network_time();
      sum_write_disk += rep.dest_data_node_disk_io_time();
    }

    if (!all_ok)
    {
      recovery_reply->set_success(false);
      recovery_reply->set_message("one or more phase2 partition recoveries failed");
      return false;
    }

    recovery_reply->set_disk_read_time(max_disk);
    recovery_reply->set_network_time(max_net);
    recovery_reply->set_decode_time(max_decode);
    recovery_reply->set_disk_write_time(sum_write_disk);
    recovery_reply->set_success(true);
    recovery_reply->set_message("ok");
    const auto repair_end = std::chrono::high_resolution_clock::now();
    double repair_time_sec =
        std::chrono::duration<double>(repair_end - repair_start).count() - orchestration_wait_sec;
    if (repair_time_sec < 0.0)
      repair_time_sec = 0.0;
    recovery_reply->set_total_time(repair_time_sec);
    std::cout << "[Coordinator] gLRC ILP Phase2 recovery stripe " << stripe_id << " success" << std::endl;
    return true;
  }

  bool CoordinatorImpl::recovery_glrc_ilp_pipeline_breakdown(int stripe_id,
                                                            const std::vector<int> &failed_block_ids,
                                                            coordinator_proto::RecoveryReply *recovery_reply)
  {
    if (m_sys_config->CodeType != "gLRC")
    {
      recovery_reply->set_message("recovery_glrc_ilp_pipeline requires CodeType gLRC");
      recovery_reply->set_success(false);
      return false;
    }
    // One pipeline recovery at a time: overlapping recoveries reuse listen ports before acceptors close.
    std::lock_guard<std::mutex> pipeline_recovery_lock(g_glrc_pipeline_recovery_mutex);
    const auto repair_start = std::chrono::high_resolution_clock::now();
    double orchestration_wait_sec = 0.0;
    Stripe &t_stripe = m_stripe_table[stripe_id];
    for (int fid : failed_block_ids)
    {
      if (fid < 0 || fid >= (int)t_stripe.blocks.size() || t_stripe.blocks[fid] == nullptr)
      {
        recovery_reply->set_message("invalid failed block id: " + std::to_string(fid));
        recovery_reply->set_success(false);
        return false;
      }
    }

    const std::string equation_policy =
        glrc_normalize_equation_policy(m_sys_config->GlrcEquationPolicy);
    GlrcIlpRepairPlan plan;
    if (!glrc_solve_repair_plan(m_sys_config->k, m_sys_config->r, m_sys_config->z, failed_block_ids,
                                equation_policy, plan))
    {
      recovery_reply->set_message(plan.error_message);
      recovery_reply->set_success(false);
      recovery_reply->set_ilp_time(plan.ilp_solve_time_sec);
      recovery_reply->set_equation_policy(equation_policy);
      return false;
    }

    std::vector<GlrcEquation> all_equations;
    std::vector<int> candidate_indices;
    glrc_build_recovery_equations(m_sys_config->k, m_sys_config->r, m_sys_config->z, failed_block_ids,
                                  all_equations, candidate_indices);
    std::unordered_set<int> failed_set(failed_block_ids.begin(), failed_block_ids.end());

    std::unordered_map<int, int> helper_equation_count;
    for (int eq_idx : plan.selected_equation_indices)
    {
      if (eq_idx < 0 || eq_idx >= (int)all_equations.size())
        continue;
      for (int b : all_equations[eq_idx].involved_blocks)
      {
        if (!failed_set.count(b))
          helper_equation_count[b]++;
      }
    }

    int hub_block_id = -1;
    int best_count = -1;
    for (const auto &kv : helper_equation_count)
    {
      if (kv.second > best_count || (kv.second == best_count && (hub_block_id < 0 || kv.first < hub_block_id)))
      {
        hub_block_id = kv.first;
        best_count = kv.second;
      }
    }
    if (hub_block_id < 0 && !plan.helper_block_ids.empty())
      hub_block_id = plan.helper_block_ids[0];
    if (hub_block_id < 0)
    {
      recovery_reply->set_message("pipeline hub selection failed");
      recovery_reply->set_success(false);
      return false;
    }

    const int n_blocks = m_sys_config->k + m_sys_config->r + m_sys_config->z;
    std::vector<GlrcPipelineNodeLookup> node_lookup(n_blocks);
    for (int bid = 0; bid < n_blocks; bid++)
    {
      if (bid >= (int)t_stripe.blocks.size() || t_stripe.blocks[bid] == nullptr)
        continue;
      Block *blk = t_stripe.blocks[bid];
      const int node_id = blk->map2node;
      auto node_it = m_node_table.find(node_id);
      if (node_it == m_node_table.end())
        continue;
      GlrcPipelineNodeLookup &nl = node_lookup[bid];
      nl.node_id = node_id;
      nl.datanode_ip = node_it->second.node_ip;
      nl.datanode_port = node_it->second.node_port;
      nl.proxy_ip = node_it->second.repair_proxy_ip;
      nl.proxy_port = node_it->second.repair_proxy_port;
      nl.block_key = blk->block_key;
    }

    GlrcPipelinePlan pipeline_plan;
    const int exchange_epoch = static_cast<int>(g_glrc_pipeline_exchange_epoch.fetch_add(1));
    std::string plan_error;
    if (!glrc_build_pipeline_plan(m_sys_config->k, m_sys_config->r, m_sys_config->z, failed_block_ids, plan,
                                  all_equations, hub_block_id, node_lookup, exchange_epoch, pipeline_plan,
                                  plan_error))
    {
      recovery_reply->set_message(plan_error);
      recovery_reply->set_success(false);
      return false;
    }

    const std::string hub_proxy_key = pipeline_plan.hub_proxy_ip + ":" + std::to_string(pipeline_plan.hub_proxy_port);
    recovery_reply->set_ilp_time(plan.ilp_solve_time_sec);
    recovery_reply->set_helper_block_count(plan.helper_block_count);
    recovery_reply->set_repair_mode("pipeline");
    recovery_reply->set_equation_policy(equation_policy);
    for (const auto &eq : plan.selected_equations)
      recovery_reply->add_selected_equations(eq);
    for (int hid : plan.helper_block_ids)
      recovery_reply->add_helper_block_ids(hid);

    const int ck = m_sys_config->k, cr = m_sys_config->r, cz = m_sys_config->z;
    const int shard_count = m_sys_config->GlrcShardCount;
    std::cout << "[Coordinator] gLRC Pipeline chain plan (" << equation_policy << "): f=" << failed_block_ids.size()
              << " failed_blocks=" << glrc_format_block_list(failed_block_ids, ck, cr, cz)
              << " hub=" << glrc_block_label(hub_block_id, ck, cr, cz) << " hub_proxy=" << hub_proxy_key
              << " hub_chains=" << pipeline_plan.hub_chains.size()
              << " local_direct_chains=" << pipeline_plan.local_direct_chains.size() << " shards=" << shard_count
              << " epoch=" << exchange_epoch << std::endl;

    struct PipelineRpcResult
    {
      bool ok = false;
      proxy_proto::RecoveryReply reply;
      std::string error;
    };

    std::mutex result_mutex;
    std::vector<std::thread> workers;
    std::vector<PipelineRpcResult> results;
    auto launch_pipeline_rpc = [&](const std::string &proxy_key, const proxy_proto::RecoveryRequest &req) {
      workers.emplace_back([this, proxy_key, req, &result_mutex, &results]() {
        PipelineRpcResult out;
        auto proxy_it = m_proxy_ptrs.find(proxy_key);
        if (proxy_it == m_proxy_ptrs.end() || !proxy_it->second)
        {
          out.ok = false;
          out.error = "pipeline proxy not found: " + proxy_key;
        }
        else
        {
          grpc::ClientContext ctx;
          ctx.set_deadline(std::chrono::system_clock::now() + std::chrono::seconds(600));
          grpc::Status st = proxy_it->second->recoveryBreakdown(&ctx, req, &out.reply);
          out.ok = st.ok();
          if (!st.ok())
            out.error = st.error_message();
        }
        std::lock_guard<std::mutex> lock(result_mutex);
        results.push_back(std::move(out));
      });
    };

    const auto make_base_request = [&]() {
      proxy_proto::RecoveryRequest req;
      req.set_glrc_ilp_recovery(true);
      req.set_glrc_ilp_pipeline(true);
      req.set_cross_rack_num(0);
      req.set_pipeline_shard_count(shard_count);
      req.set_pipeline_hub_block_id(hub_block_id);
      req.set_pipeline_exchange_epoch(exchange_epoch);
      for (int fid : failed_block_ids)
      {
        req.add_failed_block_ids(fid);
        req.add_failed_block_keys(t_stripe.blocks[fid]->block_key);
        int node_id = t_stripe.blocks[fid]->map2node;
        req.add_replaced_node_ips(m_node_table[node_id].node_ip);
        req.add_replaced_node_ports(m_node_table[node_id].node_port);
      }
      for (const auto &eq : plan.selected_equations)
        req.add_selected_equations(eq);
      for (int eq_idx : plan.selected_equation_indices)
        req.add_selected_equation_indices(eq_idx);
      return req;
    };

    std::unordered_set<int> local_direct_failed;
    for (const GlrcPipelineChainPlan &chain : pipeline_plan.local_direct_chains)
      local_direct_failed.insert(chain.local_direct_failed_block_id);

    GlrcPipelinePortAllocator port_alloc(exchange_epoch);
    std::unordered_map<int, GlrcPipelineChainPorts> chain_ports;

    std::unordered_set<std::string> pipeline_proxy_keys;
    pipeline_proxy_keys.insert(hub_proxy_key);
    for (const GlrcPipelineChainPlan &chain : pipeline_plan.hub_chains)
    {
      for (const GlrcPipelineHopInfo &hop : chain.hops)
        pipeline_proxy_keys.insert(proxy_key_from_hop(hop));
    }
    for (const GlrcPipelineChainPlan &chain : pipeline_plan.local_direct_chains)
    {
      const int fid = chain.local_direct_failed_block_id;
      if (fid >= 0 && fid < (int)node_lookup.size())
        pipeline_proxy_keys.insert(node_lookup[fid].proxy_ip + ":" + std::to_string(node_lookup[fid].proxy_port));
    }

    auto orchestration_sleep = [&](int ms) {
      orchestration_wait_sec += static_cast<double>(ms) / 1000.0;
      std::this_thread::sleep_for(std::chrono::milliseconds(ms));
    };

    bool workers_joined = false;
    auto join_pipeline_workers = [&]() {
      if (workers_joined)
        return;
      for (auto &th : workers)
      {
        if (th.joinable())
          th.join();
      }
      workers_joined = true;
    };

    auto finalize_pipeline_session = [&]() {
      join_pipeline_workers();
      // Session teardown (not counted in repair_time).
      std::this_thread::sleep_for(std::chrono::milliseconds(500));
      proxy_proto::RecoveryRequest tear_req;
      tear_req.set_glrc_ilp_recovery(true);
      tear_req.set_glrc_ilp_pipeline(true);
      tear_req.set_pipeline_role(static_cast<int>(GlrcPipelineRole::TEARDOWN));
      for (const std::string &pk : pipeline_proxy_keys)
      {
        auto pit = m_proxy_ptrs.find(pk);
        if (pit == m_proxy_ptrs.end() || !pit->second)
          continue;
        proxy_proto::RecoveryReply tear_rep;
        grpc::ClientContext ctx;
        ctx.set_deadline(std::chrono::system_clock::now() + std::chrono::seconds(30));
        pit->second->recoveryBreakdown(&ctx, tear_req, &tear_rep);
      }
      wait_pipeline_ports_released(chain_ports, pipeline_plan, 60000);
      GlrcPipelinePortAllocator::reset_port_session();
      pipeline_plan_trace("pipeline session finalize ok");
    };
    struct PipelineSessionGuard
    {
      std::function<void()> fn;
      bool armed = false;
      ~PipelineSessionGuard()
      {
        if (armed && fn)
          fn();
      }
    } session_guard;
    session_guard.fn = finalize_pipeline_session;
    session_guard.armed = !pipeline_plan.hub_chains.empty() || !pipeline_plan.local_direct_chains.empty();

    for (const GlrcPipelineChainPlan &chain : pipeline_plan.hub_chains)
    {
      GlrcPipelineChainPorts cp;
      for (size_t hi = 0; hi < chain.hops.size(); ++hi)
      {
        const ECProject::GlrcPipelineHopInfo &hop = chain.hops[hi];
        const int hop_port =
            port_alloc.allocate_hop_port(hop.proxy_ip, hop.proxy_port, chain.chain_id, static_cast<int>(hi));
        if (hop_port <= 0)
        {
          recovery_reply->set_message("pipeline port allocation failed for hop proxy " + hop.proxy_ip);
          return false;
        }
        cp.hop_listen_ports.push_back(hop_port);
      }
      cp.hub_listen_port =
          port_alloc.allocate_hub_port(pipeline_plan.hub_proxy_ip, pipeline_plan.hub_proxy_port, chain.chain_id);
      if (cp.hub_listen_port <= 0)
      {
        recovery_reply->set_message("pipeline port allocation failed for hub proxy " + pipeline_plan.hub_proxy_ip);
        return false;
      }
      chain_ports[chain.chain_id] = std::move(cp);
    }

    {
      std::unordered_map<std::string, std::unordered_set<int>> assigned_ports;
      auto check_port = [&](const std::string &proxy_ip, int proxy_grpc_port, int port, const char *tag) -> bool {
        if (port <= 0)
          return false;
        const std::string key = proxy_ip + ":" + std::to_string(proxy_grpc_port);
        if (!assigned_ports[key].insert(port).second)
        {
          char buf[256];
          snprintf(buf, sizeof(buf), "pipeline duplicate port=%d proxy=%s tag=%s epoch=%d", port, key.c_str(), tag,
                   exchange_epoch);
          pipeline_plan_trace(buf);
          recovery_reply->set_message(std::string("pipeline duplicate listen port on proxy ") + key);
          return false;
        }
        return true;
      };
      for (const GlrcPipelineChainPlan &chain : pipeline_plan.hub_chains)
      {
        const auto cp_it = chain_ports.find(chain.chain_id);
        if (cp_it == chain_ports.end())
          continue;
        for (size_t hi = 0; hi < chain.hops.size() && hi < cp_it->second.hop_listen_ports.size(); ++hi)
        {
          if (!check_port(chain.hops[hi].proxy_ip, chain.hops[hi].proxy_port, cp_it->second.hop_listen_ports[hi],
                          "hop"))
          {
            return false;
          }
        }
        if (!check_port(pipeline_plan.hub_proxy_ip, pipeline_plan.hub_proxy_port, cp_it->second.hub_listen_port, "hub"))
          return false;
      }
    }

    if (!pipeline_plan.hub_chains.empty())
    {
      proxy_proto::RecoveryRequest hub_req = make_base_request();
      hub_req.set_pipeline_role(static_cast<int>(GlrcPipelineRole::HUB));
      hub_req.set_pipeline_hub_proxy_ip(pipeline_plan.hub_proxy_ip);
      hub_req.set_pipeline_hub_proxy_port(pipeline_plan.hub_proxy_port);
      hub_req.set_pipeline_hub_block_key(pipeline_plan.hub_block_key);
      hub_req.add_pipeline_hop_datanode_ips(pipeline_plan.hub_datanode_ip);
      hub_req.add_pipeline_hop_datanode_ports(pipeline_plan.hub_datanode_port);

      hub_req.clear_failed_block_ids();
      hub_req.clear_failed_block_keys();
      hub_req.clear_replaced_node_ips();
      hub_req.clear_replaced_node_ports();
      hub_req.clear_selected_equation_indices();
      for (int fid : failed_block_ids)
      {
        if (local_direct_failed.count(fid))
          continue;
        hub_req.add_failed_block_ids(fid);
        hub_req.add_failed_block_keys(t_stripe.blocks[fid]->block_key);
        int node_id = t_stripe.blocks[fid]->map2node;
        hub_req.add_replaced_node_ips(m_node_table[node_id].node_ip);
        hub_req.add_replaced_node_ports(m_node_table[node_id].node_port);
      }
      for (const GlrcPipelineChainPlan &chain : pipeline_plan.hub_chains)
      {
        hub_req.add_selected_equation_indices(chain.equation_index);
        hub_req.add_pipeline_hub_chain_eq_slots(chain.eq_slot);
        hub_req.add_pipeline_hub_is_chain_tail_flags(chain.hub_is_chain_tail ? 1 : 0);
        hub_req.add_pipeline_hub_chain_equation_is_local(chain.equation_index < cz ? 1 : 0);
        hub_req.add_pipeline_hub_chain_local_only_flags(0);
        unsigned char hub_coef = 0;
        if (chain.hub_is_chain_tail && !chain.hops.empty())
          hub_coef = chain.hops.back().coef;
        hub_req.add_pipeline_hub_chain_hub_coefs(hub_coef);
        const auto cp_it = chain_ports.find(chain.chain_id);
        if (cp_it != chain_ports.end())
          hub_req.add_pipeline_hub_listener_ports(cp_it->second.hub_listen_port);
      }
      launch_pipeline_rpc(hub_proxy_key, hub_req);
    }

    // Start the hub first: it pre-binds all hub listener ports before waiting for chain inputs.
    // This removes the race where tail hops connect before the hub is listening.
    orchestration_sleep(800);

    for (const GlrcPipelineChainPlan &chain : pipeline_plan.hub_chains)
    {
      const int last_hop_server =
          chain.hub_is_chain_tail ? static_cast<int>(chain.hops.size()) - 2
                                  : static_cast<int>(chain.hops.size()) - 1;
      for (int hi = last_hop_server; hi >= 1; hi--)
      {
        proxy_proto::RecoveryRequest req = make_base_request();
        const GlrcPipelineChainPorts *cp = nullptr;
        const auto cp_it = chain_ports.find(chain.chain_id);
        if (cp_it != chain_ports.end())
          cp = &cp_it->second;
        fill_pipeline_chain_fields(req, chain, pipeline_plan, shard_count, cz, GlrcPipelineRole::HOP_SERVER, hi, cp);
        launch_pipeline_rpc(proxy_key_from_hop(chain.hops[hi]), req);
        orchestration_sleep(50);
      }
    }

    orchestration_sleep(800);

    for (const GlrcPipelineChainPlan &chain : pipeline_plan.hub_chains)
    {
      proxy_proto::RecoveryRequest req = make_base_request();
      const GlrcPipelineChainPorts *cp = nullptr;
      const auto cp_it = chain_ports.find(chain.chain_id);
      if (cp_it != chain_ports.end())
        cp = &cp_it->second;
      fill_pipeline_chain_fields(req, chain, pipeline_plan, shard_count, cz, GlrcPipelineRole::CHAIN_HEAD, 0, cp);
      launch_pipeline_rpc(proxy_key_from_hop(chain.hops[0]), req);
    }

    for (const GlrcPipelineChainPlan &chain : pipeline_plan.local_direct_chains)
    {
      const int fid = chain.local_direct_failed_block_id;
      if (fid < 0 || fid >= (int)node_lookup.size())
        continue;
      const GlrcPipelineNodeLookup &nl = node_lookup[fid];
      const std::string failed_proxy = nl.proxy_ip + ":" + std::to_string(nl.proxy_port);
      proxy_proto::RecoveryRequest req = make_base_request();
      fill_pipeline_chain_fields(req, chain, pipeline_plan, shard_count, cz, GlrcPipelineRole::LOCAL_DIRECT, 0);
      launch_pipeline_rpc(failed_proxy, req);
    }

    join_pipeline_workers();
    const auto repair_end = std::chrono::high_resolution_clock::now();
    double repair_time_sec =
        std::chrono::duration<double>(repair_end - repair_start).count() - orchestration_wait_sec;
    if (repair_time_sec < 0.0)
      repair_time_sec = 0.0;
    recovery_reply->set_total_time(repair_time_sec);

    double max_disk = 0.0;
    double max_net = 0.0;
    double max_decode = 0.0;
    double total_write_net = 0.0;
    double total_write_disk = 0.0;
    for (const PipelineRpcResult &res : results)
    {
      if (!res.ok)
      {
        recovery_reply->set_message("pipeline rpc failed: " + res.error);
        recovery_reply->set_success(false);
        return false;
      }
      max_disk = std::max(max_disk, breakdown_metric_span(res.reply.disk_io_start_time(), res.reply.disk_io_end_time()));
      max_net = std::max(max_net, breakdown_metric_span(res.reply.network_start_time(), res.reply.network_end_time()));
      max_decode = std::max(max_decode, breakdown_metric_span(res.reply.decode_start_time(), res.reply.decode_end_time()));
      total_write_net += res.reply.dest_data_node_network_time();
      total_write_disk += res.reply.dest_data_node_disk_io_time();
    }

    recovery_reply->set_disk_read_time(max_disk);
    recovery_reply->set_network_time(max_net + total_write_net);
    recovery_reply->set_decode_time(max_decode);
    recovery_reply->set_disk_write_time(total_write_disk);
    recovery_reply->set_success(true);
    recovery_reply->set_message("ok");
    std::cout << "[Coordinator] gLRC Pipeline chain recovery stripe " << stripe_id << " success via hub "
              << glrc_block_label(hub_block_id, ck, cr, cz) << std::endl;
    return true;
  }


  grpc::Status CoordinatorImpl::multiBlockRecovery(
    grpc::ServerContext *context,
    const coordinator_proto::StripeIdAndBlockIDsFromClient *request,
    coordinator_proto::RecoveryReply *replyClient)
  {
    replyClient->Clear();
    auto wall_start = std::chrono::high_resolution_clock::now();
    replyClient->set_grpc_start_time(
        std::chrono::duration_cast<std::chrono::duration<double>>(wall_start.time_since_epoch()).count());

    int stripe_id = request->stripe_id();
    std::vector<int> block_ids;
    for (int i = 0; i < request->block_ids_size(); i++)
      block_ids.push_back(request->block_ids(i));

    if (block_ids.empty())
    {
      replyClient->set_success(false);
      replyClient->set_message("empty failed block list");
      return grpc::Status(grpc::StatusCode::INVALID_ARGUMENT, "empty failed block list");
    }

    if (m_sys_config->CodeType != "gLRC")
    {
      replyClient->set_success(false);
      replyClient->set_message("multiBlockRecovery ILP path only supports gLRC");
      return grpc::Status(grpc::StatusCode::UNIMPLEMENTED, "only gLRC supported");
    }

    bool ok = false;
    if (m_sys_config->GlrcRepairMode == "phase2")
      ok = recovery_glrc_ilp_phase2_breakdown(stripe_id, block_ids, replyClient);
    else if (m_sys_config->GlrcRepairMode == "pipeline")
      ok = recovery_glrc_ilp_pipeline_breakdown(stripe_id, block_ids, replyClient);
    else
      ok = recovery_glrc_ilp_breakdown(stripe_id, block_ids, replyClient);

    if (!ok)
      return grpc::Status(grpc::StatusCode::INTERNAL, replyClient->message());

    return grpc::Status::OK;
  }

  grpc::Status CoordinatorImpl::delByKey(
      grpc::ServerContext *context,
      const coordinator_proto::KeyFromClient *del_key,
      coordinator_proto::RepIfDeling *delReplyClient)
  {
    try
    {
      std::string key = del_key->key();
      ObjectInfo object_info;
      m_mutex.lock();
      object_info = m_object_commit_table.at(key);
      m_object_updating_table[key] = m_object_commit_table[key];
      m_mutex.unlock();

      grpc::ClientContext context;
      proxy_proto::NodeAndBlock node_block;
      grpc::Status status;
      proxy_proto::DelReply del_reply;
      Stripe &t_stripe = m_stripe_table[object_info.map2stripe];
      std::unordered_set<int> t_cluster_set;
      for (int i = 0; i < int(t_stripe.blocks.size()); i++)
      {
        if (t_stripe.blocks[i]->map2key == key)
        {
          node_block.add_datanodeip(m_node_table[t_stripe.blocks[i]->map2node].node_ip);
          node_block.add_datanodeport(m_node_table[t_stripe.blocks[i]->map2node].node_port);
          node_block.add_blockkeys(t_stripe.blocks[i]->block_key);
          t_cluster_set.insert(t_stripe.blocks[i]->map2cluster);
        }
      }
      node_block.set_stripe_id(-1); // as a flag to distinguish delete key or stripe
      node_block.set_key(key);
      // randomly select a cluster
      int idx = rand_num(int(t_cluster_set.size()));
      int r_cluster_id = *(std::next(t_cluster_set.begin(), idx));
      std::string chosen_proxy = m_cluster_table[r_cluster_id].proxy_ip + ":" + std::to_string(m_cluster_table[r_cluster_id].proxy_port);
      status = m_proxy_ptrs[chosen_proxy]->deleteBlock(&context, node_block, &del_reply);
      delReplyClient->set_ifdeling(true);
      if (status.ok())
      {
        std::cout << "[DEL] deleting value of " << key << std::endl;
      }
    }
    catch (const std::exception &e)
    {
      std::cout << "deleteByKey exception" << std::endl;
      std::cout << e.what() << std::endl;
    }
    return grpc::Status::OK;
  }

  grpc::Status CoordinatorImpl::delByStripe(
      grpc::ServerContext *context,
      const coordinator_proto::StripeIdFromClient *stripeid,
      coordinator_proto::RepIfDeling *delReplyClient)
  {
    try
    {
      int t_stripe_id = stripeid->stripe_id();
      m_mutex.lock();
      m_stripe_deleting_table.push_back(t_stripe_id);
      m_mutex.unlock();

      grpc::ClientContext context;
      proxy_proto::NodeAndBlock node_block;
      grpc::Status status;
      proxy_proto::DelReply del_reply;
      Stripe &t_stripe = m_stripe_table[t_stripe_id];
      std::unordered_set<int> t_cluster_set;
      for (int i = 0; i < int(t_stripe.blocks.size()); i++)
      {
        if (t_stripe.blocks[i]->map2stripe == t_stripe_id)
        {
          node_block.add_datanodeip(m_node_table[t_stripe.blocks[i]->map2node].node_ip);
          node_block.add_datanodeport(m_node_table[t_stripe.blocks[i]->map2node].node_port);
          node_block.add_blockkeys(t_stripe.blocks[i]->block_key);
          t_cluster_set.insert(t_stripe.blocks[i]->map2cluster);
        }
      }
      node_block.set_stripe_id(t_stripe_id);
      node_block.set_key("");
      // randomly select a cluster
      int idx = rand_num(int(t_cluster_set.size()));
      int r_cluster_id = *(std::next(t_cluster_set.begin(), idx));
      std::string chosen_proxy = m_cluster_table[r_cluster_id].proxy_ip + ":" + std::to_string(m_cluster_table[r_cluster_id].proxy_port);
      status = m_proxy_ptrs[chosen_proxy]->deleteBlock(&context, node_block, &del_reply);
      delReplyClient->set_ifdeling(true);
      if (status.ok())
      {
        std::cout << "[DEL] deleting value of Stripe " << t_stripe_id << std::endl;
      }
    }
    catch (const std::exception &e)
    {
      std::cout << "deleteByStripe exception" << std::endl;
      std::cout << e.what() << std::endl;
    }
    return grpc::Status::OK;
  }

  grpc::Status CoordinatorImpl::listStripes(
      grpc::ServerContext *context,
      const coordinator_proto::RequestToCoordinator *req,
      coordinator_proto::RepStripeIds *listReplyClient)
  {
    try
    {
      for (auto it = m_stripe_table.begin(); it != m_stripe_table.end(); it++)
      {
        listReplyClient->add_stripe_ids(it->first);
      }
    }
    catch (const std::exception &e)
    {
      std::cerr << e.what() << '\n';
    }

    return grpc::Status::OK;
  }

  grpc::Status CoordinatorImpl::checkalive(
      grpc::ServerContext *context,
      const coordinator_proto::RequestToCoordinator *helloRequestToCoordinator,
      coordinator_proto::ReplyFromCoordinator *helloReplyFromCoordinator)
  {

    std::cout << "[Coordinator Check] alive " << helloRequestToCoordinator->name() << std::endl;
    return grpc::Status::OK;
  }
  grpc::Status CoordinatorImpl::reportCommitAbort(
      grpc::ServerContext *context,
      const coordinator_proto::CommitAbortKey *commit_abortkey,
      coordinator_proto::ReplyFromCoordinator *helloReplyFromCoordinator)
  {
    std::string key = commit_abortkey->key();
    ECProject::OpperateType opp = (ECProject::OpperateType)commit_abortkey->opp();
    int stripe_id = commit_abortkey->stripe_id();
    std::unique_lock<std::mutex> lck(m_mutex);
    try
    {
      if (commit_abortkey->ifcommitmetadata())
      {
        if (opp == SET || opp == APPEND)
        {
          m_object_commit_table[key] = m_object_updating_table[key];
          cv.notify_all();
          m_object_updating_table.erase(key);
        }
        else if (opp == DEL) // delete the metadata
        {
          if (stripe_id < 0) // delete key
          {
            if (IF_DEBUG)
            {
              std::cout << "[DEL] Proxy report delete key finish!" << std::endl;
            }
            ObjectInfo object_info = m_object_commit_table.at(key);
            stripe_id = object_info.map2stripe;
            m_object_commit_table.erase(key); // update commit table
            cv.notify_all();
            m_object_updating_table.erase(key);
            Stripe &t_stripe = m_stripe_table[stripe_id];
            std::vector<Block *>::iterator it1;
            for (it1 = t_stripe.blocks.begin(); it1 != t_stripe.blocks.end();)
            {
              if ((*it1)->map2key == key)
              {
                it1 = t_stripe.blocks.erase(it1);
              }
              else
              {
                it1++;
              }
            }
            if (t_stripe.blocks.empty()) // update stripe table
            {
              m_stripe_table.erase(stripe_id);
            }
            std::map<int, Cluster>::iterator it2; // update cluster table
            for (it2 = m_cluster_table.begin(); it2 != m_cluster_table.end(); it2++)
            {
              Cluster &t_cluster = it2->second;
              for (it1 = t_cluster.blocks.begin(); it1 != t_cluster.blocks.end();)
              {
                if ((*it1)->map2key == key)
                {
                  update_stripe_info_in_node(false, (*it1)->map2node, (*it1)->map2stripe); // update node table
                  it1 = t_cluster.blocks.erase(it1);
                }
                else
                {
                  it1++;
                }
              }
            }
          } // delete stripe
          else
          {
            if (IF_DEBUG)
            {
              std::cout << "[DEL] Proxy report delete stripe finish!" << std::endl;
            }
            auto its = std::find(m_stripe_deleting_table.begin(), m_stripe_deleting_table.end(), stripe_id);
            if (its != m_stripe_deleting_table.end())
            {
              m_stripe_deleting_table.erase(its);
            }
            cv.notify_all();
            // update stripe table
            m_stripe_table.erase(stripe_id);
            std::unordered_set<std::string> object_keys_set;
            // update cluster table
            std::map<int, Cluster>::iterator it2;
            for (it2 = m_cluster_table.begin(); it2 != m_cluster_table.end(); it2++)
            {
              Cluster &t_cluster = it2->second;
              for (auto it1 = t_cluster.blocks.begin(); it1 != t_cluster.blocks.end();)
              {
                if ((*it1)->map2stripe == stripe_id)
                {
                  object_keys_set.insert((*it1)->map2key);
                  it1 = t_cluster.blocks.erase(it1);
                }
                else
                {
                  it1++;
                }
              }
            }
            // update node table
            for (auto it3 = m_node_table.begin(); it3 != m_node_table.end(); it3++)
            {
              Node &t_node = it3->second;
              auto it4 = t_node.stripes.find(stripe_id);
              if (it4 != t_node.stripes.end())
              {
                t_node.stripes.erase(stripe_id);
              }
            }
            // update commit table
            for (auto it5 = object_keys_set.begin(); it5 != object_keys_set.end(); it5++)
            {
              auto it6 = m_object_commit_table.find(*it5);
              if (it6 != m_object_commit_table.end())
              {
                m_object_commit_table.erase(it6);
              }
            }
            // merge group
          }
          // if (IF_DEBUG)
          // {
          //   std::cout << "[DEL] Data placement after delete:" << std::endl;
          //   for (int i = 0; i < m_num_of_Clusters; i++)
          //   {
          //     Cluster &t_cluster = m_cluster_table[i];
          //     if (int(t_cluster.blocks.size()) > 0)
          //     {
          //       std::cout << "Cluster " << i << ": ";
          //       for (auto it = t_cluster.blocks.begin(); it != t_cluster.blocks.end(); it++)
          //       {
          //         std::cout << "[" << (*it)->block_key << ":S" << (*it)->map2stripe << "G" << (*it)->map2group << "N" << (*it)->map2node << "] ";
          //       }
          //       std::cout << std::endl;
          //     }
          //   }
          //   std::cout << std::endl;
          // }
        }
      }
      else
      {
        m_object_updating_table.erase(key);
      }
    }
    catch (std::exception &e)
    {
      std::cout << "reportCommitAbort exception" << std::endl;
      std::cout << e.what() << std::endl;
    }
    return grpc::Status::OK;
  }

  grpc::Status
  CoordinatorImpl::checkCommitAbort(grpc::ServerContext *context,
                                    const coordinator_proto::AskIfSuccess *key_opp,
                                    coordinator_proto::RepIfSuccess *reply)
  {
    std::unique_lock<std::mutex> lck(m_mutex);
    std::string key = key_opp->key();
    ECProject::OpperateType opp = (ECProject::OpperateType)key_opp->opp();
    int stripe_id = key_opp->stripe_id();
    if (opp == SET || opp == APPEND)
    {
      while (m_object_commit_table.find(key) == m_object_commit_table.end())
      {
        cv.wait(lck);
      }
    }
    else if (opp == DEL)
    {
      if (stripe_id < 0)
      {
        while (m_object_commit_table.find(key) != m_object_commit_table.end())
        {
          cv.wait(lck);
        }
      }
      else
      {
        auto it = std::find(m_stripe_deleting_table.begin(), m_stripe_deleting_table.end(), stripe_id);
        while (it != m_stripe_deleting_table.end())
        {
          cv.wait(lck);
          it = std::find(m_stripe_deleting_table.begin(), m_stripe_deleting_table.end(), stripe_id);
        }
      }
    }
    reply->set_ifcommit(true);
    return grpc::Status::OK;
  }

  std::string CoordinatorImpl::repair_proxy_key_for_node(int node_id) const
  {
    const auto it = m_node_table.find(node_id);
    if (it == m_node_table.end())
      return "";
    return it->second.repair_proxy_ip + ":" + std::to_string(it->second.repair_proxy_port);
  }

  // Register every datanode repair proxy (and legacy cluster proxies if distinct).
  bool CoordinatorImpl::init_proxyinfo()
  {
    std::unordered_set<std::string> registered;
    auto register_proxy = [&](const std::string &proxy_ip_and_port) {
      if (proxy_ip_and_port.empty() || registered.count(proxy_ip_and_port))
        return;
      registered.insert(proxy_ip_and_port);
      auto _stub = proxy_proto::proxyService::NewStub(
          grpc::CreateChannel(proxy_ip_and_port, grpc::InsecureChannelCredentials()));
      proxy_proto::CheckaliveCMD Cmd;
      proxy_proto::RequestResult result;
      grpc::ClientContext clientContext;
      Cmd.set_name("coordinator");
      grpc::Status status = _stub->checkalive(&clientContext, Cmd, &result);
      if (status.ok())
        std::cout << "[Proxy Check] ok from " << proxy_ip_and_port << std::endl;
      else
        std::cout << "[Proxy Check] failed to connect " << proxy_ip_and_port << std::endl;
      m_proxy_ptrs.insert(std::make_pair(proxy_ip_and_port, std::move(_stub)));
    };

    for (const auto &kv : m_node_table)
      register_proxy(repair_proxy_key_for_node(kv.first));

    for (auto cur = m_cluster_table.begin(); cur != m_cluster_table.end(); cur++)
    {
      const std::string cluster_proxy =
          cur->second.proxy_ip + ":" + std::to_string(cur->second.proxy_port);
      register_proxy(cluster_proxy);
    }
    return true;
  }
  bool CoordinatorImpl::init_clusterinfo(std::string m_clusterinfo_path)
  {
    std::cout << "Cluster_information_path:" << m_clusterinfo_path << std::endl;
    tinyxml2::XMLDocument xml;
    xml.LoadFile(m_clusterinfo_path.c_str());
    tinyxml2::XMLElement *root = xml.RootElement();
    int node_id = 0;
    m_num_of_Clusters = 0;
    for (tinyxml2::XMLElement *cluster = root->FirstChildElement(); cluster != nullptr; cluster = cluster->NextSiblingElement())
    {
      std::string cluster_id(cluster->Attribute("id"));
      std::string proxy(cluster->Attribute("proxy"));
      std::cout << "cluster_id: " << cluster_id << " , proxy: " << proxy << std::endl;
      Cluster t_cluster;
      m_cluster_table[std::stoi(cluster_id)] = t_cluster;
      m_cluster_table[std::stoi(cluster_id)].cluster_id = std::stoi(cluster_id);
      auto pos = proxy.find(':');
      m_cluster_table[std::stoi(cluster_id)].proxy_ip = proxy.substr(0, pos);
      m_cluster_table[std::stoi(cluster_id)].proxy_port = std::stoi(proxy.substr(pos + 1, proxy.size()));
      const int cid = std::stoi(cluster_id);
      for (tinyxml2::XMLElement *node = cluster->FirstChildElement()->FirstChildElement(); node != nullptr;
           node = node->NextSiblingElement())
      {
        std::string node_uri(node->Attribute("uri"));
        std::cout << "____node: " << node_uri;
        m_cluster_table[cid].nodes.push_back(node_id);
        m_node_table[node_id].node_id = node_id;
        auto uri_pos = node_uri.find(':');
        m_node_table[node_id].node_ip = node_uri.substr(0, uri_pos);
        m_node_table[node_id].node_port = std::stoi(node_uri.substr(uri_pos + 1, node_uri.size()));
        m_node_table[node_id].cluster_id = cid;
        if (const char *dn_proxy = node->Attribute("proxy"))
        {
          std::string px(dn_proxy);
          auto px_pos = px.find(':');
          m_node_table[node_id].repair_proxy_ip = px.substr(0, px_pos);
          m_node_table[node_id].repair_proxy_port = std::stoi(px.substr(px_pos + 1, px.size()));
          std::cout << " proxy=" << px;
        }
        else
        {
          m_node_table[node_id].repair_proxy_ip = m_cluster_table[cid].proxy_ip;
          m_node_table[node_id].repair_proxy_port = m_cluster_table[cid].proxy_port;
        }
        std::cout << std::endl;
        node_id++;
      }
      m_num_of_Clusters++;
    }
    return true;
  }

  int CoordinatorImpl::randomly_select_a_cluster(int stripe_id)
  {
    std::random_device rd;
    std::mt19937 gen(rd());
    std::uniform_int_distribution<int> dis_cluster(0, m_num_of_Clusters - 1);
    int r_cluster_id = dis_cluster(gen);
    while (m_cluster_table[r_cluster_id].stripes.find(stripe_id) != m_cluster_table[r_cluster_id].stripes.end())
    {
      r_cluster_id = dis_cluster(gen);
    }
    return r_cluster_id;
  }

  // randomly select a node in the selected cluster
  // with the constraint that the node has not been selected for the same stripe
  int CoordinatorImpl::randomly_select_a_node(int cluster_id, int stripe_id)
  {
    std::random_device rd;
    std::mt19937 gen(rd());
    std::uniform_int_distribution<int> dis_node(0, m_cluster_table[cluster_id].nodes.size() - 1);
    int r_node_id = m_cluster_table[cluster_id].nodes[dis_node(gen)];
    while (m_node_table[r_node_id].stripes.find(stripe_id) != m_node_table[r_node_id].stripes.end())
    {
      r_node_id = m_cluster_table[cluster_id].nodes[dis_node(gen)];
    }
    return r_node_id;
  }

  void CoordinatorImpl::update_stripe_info_in_node(int t_node_id, int stripe_id, int index)
  {
    assert(m_node_table[t_node_id].stripes.find(stripe_id) == m_node_table[t_node_id].stripes.end() && "The node has been selected for the stripe");
    m_node_table[t_node_id].stripes[stripe_id] = index;
  }

  // maintain the block number of the stripe in the node
  // TODO: Still don't konw why the stripe_block_num is start from 1
  void
  CoordinatorImpl::update_stripe_info_in_node(bool add_or_sub, int t_node_id, int stripe_id)
  {
    int stripe_block_num = 1;
    if (m_node_table[t_node_id].stripes.find(stripe_id) != m_node_table[t_node_id].stripes.end())
    {
      stripe_block_num = m_node_table[t_node_id].stripes[stripe_id];
    }
    if (add_or_sub)
    {
      m_node_table[t_node_id].stripes[stripe_id] = stripe_block_num + 1;
    }
    else
    {
      if (stripe_block_num == 1)
      {
        m_node_table[t_node_id].stripes.erase(stripe_id);
      }
      else
      {
        m_node_table[t_node_id].stripes[stripe_id] = stripe_block_num - 1;
      }
    }
  }

  int CoordinatorImpl::generate_placement(int stripe_id, int block_size)
  {
    Stripe &stripe_info = m_stripe_table[stripe_id];
    int k = stripe_info.k;
    int l = stripe_info.l;
    int g_m = stripe_info.g_m;
    int b = m_encode_parameters.b_datapergroup;
    ECProject::EncodeType encode_type = m_encode_parameters.encodetype;
    ECProject::SingleStripePlacementType s_placement_type = m_encode_parameters.s_stripe_placementtype;
    ECProject::MultiStripesPlacementType m_placement_type = m_encode_parameters.m_stripe_placementtype;

    // generate stripe information
    int index = stripe_info.object_keys.size() - 1;
    std::string object_key = stripe_info.object_keys[index];
    Block *blocks_info = new Block[k + g_m + l];
    for (int i = 0; i < k + g_m + l; i++)
    {
      blocks_info[i].block_size = block_size;
      blocks_info[i].map2stripe = stripe_id;
      blocks_info[i].map2key = object_key;
      if (i < k)
      {
        std::string tmp = "_D";
        if (i < 10)
          tmp = "_D0";
        blocks_info[i].block_key = object_key + tmp + std::to_string(i);
        blocks_info[i].block_id = i;
        blocks_info[i].block_type = 'D';
        blocks_info[i].map2group = int(i / b);
        stripe_info.blocks.push_back(&blocks_info[i]);
      }
      else if (i >= k && i < k + g_m)
      {
        blocks_info[i].block_key = "Stripe" + std::to_string(stripe_id) + "_G" + std::to_string(i - k);
        blocks_info[i].block_id = i;
        blocks_info[i].block_type = 'G';
        blocks_info[i].map2group = l;
        stripe_info.blocks.push_back(&blocks_info[i]);
      }
      else
      {
        blocks_info[i].block_key = "Stripe" + std::to_string(stripe_id) + "_L" + std::to_string(i - k - g_m);
        blocks_info[i].block_id = i;
        blocks_info[i].block_type = 'L';
        blocks_info[i].map2group = i - k - g_m;
        stripe_info.blocks.push_back(&blocks_info[i]);
      }
    }

    if (encode_type == Azure_LRC || encode_type == Optimal_Cauchy_LRC)
    {
      if (s_placement_type == Optimal)
      {
        if (m_placement_type == Ran)
        {
          int idx = m_merge_groups.size() - 1;
          if (idx < 0 || int(m_merge_groups[idx].size()) == m_encode_parameters.x_stripepermergegroup)
          {
            std::vector<int> temp;
            temp.push_back(stripe_id);
            m_merge_groups.push_back(temp);
          }
          else
          {
            m_merge_groups[idx].push_back(stripe_id);
          }

          int g_cluster_id = -1;
          for (int i = 0; i < l; i++)
          {
            for (int j = i * b; j < (i + 1) * b; j += g_m + 1)
            {
              bool flag = false;
              if (j + g_m + 1 >= (i + 1) * b)
                flag = true;
              // randomly select a cluster
              int t_cluster_id = randomly_select_a_cluster(stripe_id);
              Cluster &t_cluster = m_cluster_table[t_cluster_id];
              // place every g+1 data blocks from each group to a single cluster
              for (int o = j; o < j + g_m + 1 && o < (i + 1) * b; o++)
              {
                // randomly select a node in the selected cluster
                int t_node_id = randomly_select_a_node(t_cluster_id, stripe_id);
                blocks_info[o].map2cluster = t_cluster_id;
                blocks_info[o].map2node = t_node_id;
                update_stripe_info_in_node(true, t_node_id, stripe_id);
                t_cluster.blocks.push_back(&blocks_info[o]);
                t_cluster.stripes.insert(stripe_id);
                stripe_info.place2clusters.insert(t_cluster_id);
              }
              // place local parity blocks
              if (flag)
              {
                if (j + g_m + 1 != (i + 1) * b) // b % (g + 1) != 0
                {
                  // randomly select a node in the selected cluster
                  int t_node_id = randomly_select_a_node(t_cluster_id, stripe_id);
                  blocks_info[k + g_m + i].map2cluster = t_cluster_id;
                  blocks_info[k + g_m + i].map2node = t_node_id;
                  update_stripe_info_in_node(true, t_node_id, stripe_id);
                  t_cluster.blocks.push_back(&blocks_info[k + g_m + i]);
                  t_cluster.stripes.insert(stripe_id);
                  stripe_info.place2clusters.insert(t_cluster_id);
                }
                else // place the local parity blocks together with global ones
                {
                  if (g_cluster_id == -1) // randomly select a new cluster
                  {
                    g_cluster_id = randomly_select_a_cluster(stripe_id);
                  }
                  Cluster &g_cluster = m_cluster_table[g_cluster_id];
                  int t_node_id = randomly_select_a_node(g_cluster_id, stripe_id);
                  blocks_info[k + g_m + i].map2cluster = g_cluster_id;
                  blocks_info[k + g_m + i].map2node = t_node_id;
                  update_stripe_info_in_node(true, t_node_id, stripe_id);
                  g_cluster.blocks.push_back(&blocks_info[k + g_m + i]);
                  g_cluster.stripes.insert(stripe_id);
                  stripe_info.place2clusters.insert(g_cluster_id);
                }
              }
            }
          }
          if (g_cluster_id == -1) // randomly select a new cluster
          {
            g_cluster_id = randomly_select_a_cluster(stripe_id);
          }
          Cluster &g_cluster = m_cluster_table[g_cluster_id];
          // place the global parity blocks to the selected cluster
          for (int i = 0; i < g_m; i++)
          {
            int t_node_id = randomly_select_a_node(g_cluster_id, stripe_id);
            blocks_info[k + i].map2cluster = g_cluster_id;
            blocks_info[k + i].map2node = t_node_id;
            update_stripe_info_in_node(true, t_node_id, stripe_id);
            g_cluster.blocks.push_back(&blocks_info[k + i]);
            g_cluster.stripes.insert(stripe_id);
            stripe_info.place2clusters.insert(g_cluster_id);
          }
        }
        else if (m_placement_type == DIS)
        {
          int required_cluster_num = ceil(b + 1, g_m + 1) * l + 1;
          int idx = m_merge_groups.size() - 1;
          if (b % (g_m + 1) == 0)
            required_cluster_num -= l;
          if (int(m_free_clusters.size()) < required_cluster_num || m_free_clusters.empty() || idx < 0 ||
              int(m_merge_groups[idx].size()) == m_encode_parameters.x_stripepermergegroup)
          {
            m_free_clusters.clear();
            m_free_clusters.shrink_to_fit();
            for (int i = 0; i < m_num_of_Clusters; i++)
            {
              m_free_clusters.push_back(i);
            }
            std::vector<int> temp;
            temp.push_back(stripe_id);
            m_merge_groups.push_back(temp);
          }
          else
          {
            m_merge_groups[idx].push_back(stripe_id);
          }

          int g_cluster_id = -1;
          for (int i = 0; i < l; i++)
          {
            for (int j = i * b; j < (i + 1) * b; j += g_m + 1)
            {
              bool flag = false;
              if (j + g_m + 1 >= (i + 1) * b)
                flag = true;
              // randomly select a cluster
              int t_cluster_id = m_free_clusters[rand_num(int(m_free_clusters.size()))];
              auto iter = std::find(m_free_clusters.begin(), m_free_clusters.end(), t_cluster_id);
              if (iter != m_free_clusters.end())
              {
                m_free_clusters.erase(iter);
              } // remove the selected cluster from the free list
              Cluster &t_cluster = m_cluster_table[t_cluster_id];
              // place every g+1 data blocks from each group to a single cluster
              for (int o = j; o < j + g_m + 1 && o < (i + 1) * b; o++)
              {
                // randomly select a node in the selected cluster
                int t_node_id = randomly_select_a_node(t_cluster_id, stripe_id);
                blocks_info[o].map2cluster = t_cluster_id;
                blocks_info[o].map2node = t_node_id;
                update_stripe_info_in_node(true, t_node_id, stripe_id);
                t_cluster.blocks.push_back(&blocks_info[o]);
                t_cluster.stripes.insert(stripe_id);
                stripe_info.place2clusters.insert(t_cluster_id);
              }
              // place local parity blocks
              if (flag)
              {
                if (j + g_m + 1 != (i + 1) * b) // b % (g + 1) != 0
                {
                  // randomly select a node in the selected cluster
                  int t_node_id = randomly_select_a_node(t_cluster_id, stripe_id);
                  blocks_info[k + g_m + i].map2cluster = t_cluster_id;
                  blocks_info[k + g_m + i].map2node = t_node_id;
                  update_stripe_info_in_node(true, t_node_id, stripe_id);
                  t_cluster.blocks.push_back(&blocks_info[k + g_m + i]);
                  t_cluster.stripes.insert(stripe_id);
                  stripe_info.place2clusters.insert(t_cluster_id);
                }
                else // place the local parity blocks together with global ones
                {
                  if (g_cluster_id == -1) // randomly select a new cluster
                  {
                    g_cluster_id = m_free_clusters[rand_num(int(m_free_clusters.size()))];
                    auto iter = std::find(m_free_clusters.begin(), m_free_clusters.end(), g_cluster_id);
                    if (iter != m_free_clusters.end())
                    {
                      m_free_clusters.erase(iter);
                    }
                  }
                  Cluster &g_cluster = m_cluster_table[g_cluster_id];
                  int t_node_id = randomly_select_a_node(g_cluster_id, stripe_id);
                  blocks_info[k + g_m + i].map2cluster = g_cluster_id;
                  blocks_info[k + g_m + i].map2node = t_node_id;
                  update_stripe_info_in_node(true, t_node_id, stripe_id);
                  g_cluster.blocks.push_back(&blocks_info[k + g_m + i]);
                  g_cluster.stripes.insert(stripe_id);
                  stripe_info.place2clusters.insert(g_cluster_id);
                }
              }
            }
          }
          if (g_cluster_id == -1) // randomly select a new cluster
          {
            g_cluster_id = m_free_clusters[rand_num(int(m_free_clusters.size()))];
            auto iter = std::find(m_free_clusters.begin(), m_free_clusters.end(), g_cluster_id);
            if (iter != m_free_clusters.end())
            {
              m_free_clusters.erase(iter);
            }
          }
          Cluster &g_cluster = m_cluster_table[g_cluster_id];
          // place the global parity blocks to the selected cluster
          for (int i = 0; i < g_m; i++)
          {
            int t_node_id = randomly_select_a_node(g_cluster_id, stripe_id);
            blocks_info[k + i].map2cluster = g_cluster_id;
            blocks_info[k + i].map2node = t_node_id;
            update_stripe_info_in_node(true, t_node_id, stripe_id);
            g_cluster.blocks.push_back(&blocks_info[k + i]);
            g_cluster.stripes.insert(stripe_id);
            stripe_info.place2clusters.insert(g_cluster_id);
          }
        }
        else if (m_placement_type == AGG)
        {
          int agg_clusters_num = ceil(b + 1, g_m + 1) * l + 1;
          if (b % (g_m + 1) == 0)
          {
            agg_clusters_num -= l;
          }
          int idx = m_merge_groups.size() - 1;
          if (idx < 0 || int(m_merge_groups[idx].size()) == m_encode_parameters.x_stripepermergegroup)
          {
            std::vector<int> temp;
            temp.push_back(stripe_id);
            m_merge_groups.push_back(temp);
            m_agg_start_cid = rand_num(m_num_of_Clusters - agg_clusters_num);
          }
          else
          {
            m_merge_groups[idx].push_back(stripe_id);
          }
          int t_cluster_id = m_agg_start_cid - 1;
          int g_cluster_id = -1;
          for (int i = 0; i < l; i++)
          {
            for (int j = i * b; j < (i + 1) * b; j += g_m + 1)
            {
              bool flag = false;
              if (j + g_m + 1 >= (i + 1) * b)
                flag = true;
              t_cluster_id += 1;
              Cluster &t_cluster = m_cluster_table[t_cluster_id];
              // place every g+1 data blocks from each group to a single cluster
              for (int o = j; o < j + g_m + 1 && o < (i + 1) * b; o++)
              {
                // randomly select a node in the selected cluster
                int t_node_id = randomly_select_a_node(t_cluster_id, stripe_id);
                blocks_info[o].map2cluster = t_cluster_id;
                blocks_info[o].map2node = t_node_id;
                update_stripe_info_in_node(true, t_node_id, stripe_id);
                t_cluster.blocks.push_back(&blocks_info[o]);
                t_cluster.stripes.insert(stripe_id);
                stripe_info.place2clusters.insert(t_cluster_id);
              }
              // place local parity blocks
              if (flag)
              {
                if (j + g_m + 1 != (i + 1) * b) // b % (g + 1) != 0
                {
                  // randomly select a node in the selected cluster
                  int t_node_id = randomly_select_a_node(t_cluster_id, stripe_id);
                  blocks_info[k + g_m + i].map2cluster = t_cluster_id;
                  blocks_info[k + g_m + i].map2node = t_node_id;
                  update_stripe_info_in_node(true, t_node_id, stripe_id);
                  t_cluster.blocks.push_back(&blocks_info[k + g_m + i]);
                  t_cluster.stripes.insert(stripe_id);
                  stripe_info.place2clusters.insert(t_cluster_id);
                }
                else // place the local parity blocks together with global ones
                {
                  if (g_cluster_id == -1)
                  {
                    g_cluster_id = t_cluster_id + 1;
                    t_cluster_id++;
                  }
                  Cluster &g_cluster = m_cluster_table[g_cluster_id];
                  int t_node_id = randomly_select_a_node(g_cluster_id, stripe_id);
                  blocks_info[k + g_m + i].map2cluster = g_cluster_id;
                  blocks_info[k + g_m + i].map2node = t_node_id;
                  update_stripe_info_in_node(true, t_node_id, stripe_id);
                  g_cluster.blocks.push_back(&blocks_info[k + g_m + i]);
                  g_cluster.stripes.insert(stripe_id);
                  stripe_info.place2clusters.insert(g_cluster_id);
                }
              }
            }
          }
          if (g_cluster_id == -1)
          {
            g_cluster_id = t_cluster_id + 1;
          }
          Cluster &g_cluster = m_cluster_table[g_cluster_id];
          // place the global parity blocks to the selected cluster
          for (int i = 0; i < g_m; i++)
          {
            int t_node_id = randomly_select_a_node(g_cluster_id, stripe_id);
            blocks_info[k + i].map2cluster = g_cluster_id;
            blocks_info[k + i].map2node = t_node_id;
            update_stripe_info_in_node(true, t_node_id, stripe_id);
            g_cluster.blocks.push_back(&blocks_info[k + i]);
            g_cluster.stripes.insert(stripe_id);
            stripe_info.place2clusters.insert(g_cluster_id);
          }
        }
        else if (m_placement_type == OPT)
        {
          int required_cluster_num = ceil(b + 1, g_m + 1) * l + 1;
          int agg_clusters_num = l + 1;
          if (b % (g_m + 1) == 0)
          {
            agg_clusters_num = 1;
            required_cluster_num -= l;
          }
          int idx = m_merge_groups.size() - 1;
          if (int(m_free_clusters.size()) < required_cluster_num - agg_clusters_num || m_free_clusters.empty() ||
              idx < 0 || int(m_merge_groups[idx].size()) == m_encode_parameters.x_stripepermergegroup)
          {
            m_agg_start_cid = rand_num(m_num_of_Clusters - agg_clusters_num);
            m_free_clusters.clear();
            m_free_clusters.shrink_to_fit();
            for (int i = 0; i < m_agg_start_cid; i++)
            {
              m_free_clusters.push_back(i);
            }
            for (int i = m_agg_start_cid + agg_clusters_num; i < m_num_of_Clusters; i++)
            {
              m_free_clusters.push_back(i);
            }
            std::vector<int> temp;
            temp.push_back(stripe_id);
            m_merge_groups.push_back(temp);
          }
          else
          {
            m_merge_groups[idx].push_back(stripe_id);
          }

          int agg_cluster_id = m_agg_start_cid - 1;
          int t_cluster_id = -1;
          int g_cluster_id = m_agg_start_cid + agg_clusters_num - 1;
          for (int i = 0; i < l; i++)
          {
            for (int j = i * b; j < (i + 1) * b; j += g_m + 1)
            {
              bool flag = false;
              if (j + g_m + 1 >= (i + 1) * b)
                flag = true;
              if (flag && j + g_m + 1 != (i + 1) * b)
              {
                t_cluster_id = ++agg_cluster_id;
              }
              else
              {
                t_cluster_id = m_free_clusters[rand_num(int(m_free_clusters.size()))];
                auto iter = std::find(m_free_clusters.begin(), m_free_clusters.end(), t_cluster_id);
                if (iter != m_free_clusters.end())
                {
                  m_free_clusters.erase(iter);
                }
              }
              Cluster &t_cluster = m_cluster_table[t_cluster_id];
              // place every g+1 data blocks from each group to a single cluster
              for (int o = j; o < j + g_m + 1 && o < (i + 1) * b; o++)
              {
                // randomly select a node in the selected cluster
                int t_node_id = randomly_select_a_node(t_cluster_id, stripe_id);
                blocks_info[o].map2cluster = t_cluster_id;
                blocks_info[o].map2node = t_node_id;
                update_stripe_info_in_node(true, t_node_id, stripe_id);
                t_cluster.blocks.push_back(&blocks_info[o]);
                t_cluster.stripes.insert(stripe_id);
                stripe_info.place2clusters.insert(t_cluster_id);
              }
              // place local parity blocks
              if (flag)
              {
                if (j + g_m + 1 != (i + 1) * b) // b % (g + 1) != 0
                {
                  // randomly select a node in the selected cluster
                  int t_node_id = randomly_select_a_node(t_cluster_id, stripe_id);
                  blocks_info[k + g_m + i].map2cluster = t_cluster_id;
                  blocks_info[k + g_m + i].map2node = t_node_id;
                  update_stripe_info_in_node(true, t_node_id, stripe_id);
                  t_cluster.blocks.push_back(&blocks_info[k + g_m + i]);
                  t_cluster.stripes.insert(stripe_id);
                  stripe_info.place2clusters.insert(t_cluster_id);
                }
                else // place the local parity blocks together with global ones
                {
                  Cluster &g_cluster = m_cluster_table[g_cluster_id];
                  int t_node_id = randomly_select_a_node(g_cluster_id, stripe_id);
                  blocks_info[k + g_m + i].map2cluster = g_cluster_id;
                  blocks_info[k + g_m + i].map2node = t_node_id;
                  update_stripe_info_in_node(true, t_node_id, stripe_id);
                  g_cluster.blocks.push_back(&blocks_info[k + g_m + i]);
                  g_cluster.stripes.insert(stripe_id);
                  stripe_info.place2clusters.insert(g_cluster_id);
                }
              }
            }
          }
          Cluster &g_cluster = m_cluster_table[g_cluster_id];
          // place the global parity blocks to the selected cluster
          for (int i = 0; i < g_m; i++)
          {
            int t_node_id = randomly_select_a_node(g_cluster_id, stripe_id);
            blocks_info[k + i].map2cluster = g_cluster_id;
            blocks_info[k + i].map2node = t_node_id;
            update_stripe_info_in_node(true, t_node_id, stripe_id);
            g_cluster.blocks.push_back(&blocks_info[k + i]);
            g_cluster.stripes.insert(stripe_id);
            stripe_info.place2clusters.insert(g_cluster_id);
          }
        }
      }
    }

    if (IF_DEBUG)
    {
      std::cout << std::endl;
      std::cout << "Data placement result:" << std::endl;
      for (int i = 0; i < m_num_of_Clusters; i++)
      {
        Cluster &t_cluster = m_cluster_table[i];
        if (int(t_cluster.blocks.size()) > 0)
        {
          std::cout << "Cluster " << i << ": ";
          for (auto it = t_cluster.blocks.begin(); it != t_cluster.blocks.end(); it++)
          {
            std::cout << "[" << (*it)->block_key << ":S" << (*it)->map2stripe << "G" << (*it)->map2group << "N" << (*it)->map2node << "] ";
          }
          std::cout << std::endl;
        }
      }
      std::cout << std::endl;
      std::cout << "Merge Group: ";
      for (auto it1 = m_merge_groups.begin(); it1 != m_merge_groups.end(); it1++)
      {
        std::cout << "[ ";
        for (auto it2 = (*it1).begin(); it2 != (*it1).end(); it2++)
        {
          std::cout << (*it2) << " ";
        }
        std::cout << "] ";
      }
      std::cout << std::endl;
    }

    // randomly select a cluster
    int r_idx = rand_num(int(stripe_info.place2clusters.size()));
    int selected_cluster_id = *(std::next(stripe_info.place2clusters.begin(), r_idx));
    if (IF_DEBUG)
    {
      std::cout << "[SET] Select the proxy in cluster " << selected_cluster_id << " to encode and set!" << std::endl;
    }
    return selected_cluster_id;
  }

  void CoordinatorImpl::blocks_in_cluster(std::map<char, std::vector<ECProject::Block *>> &block_info, int cluster_id, int stripe_id)
  {
    std::vector<ECProject::Block *> tt, td, tl, tg;
    Cluster &cluster = m_cluster_table[cluster_id];
    std::vector<Block *>::iterator it;
    for (it = cluster.blocks.begin(); it != cluster.blocks.end(); it++)
    {
      Block *block = *it;
      if (block->map2stripe == stripe_id)
      {
        tt.push_back(block);
        if (block->block_type == 'D')
        {
          td.push_back(block);
        }
        else if (block->block_type == 'L')
        {
          tl.push_back(block);
        }
        else
        {
          tg.push_back(block);
        }
      }
    }
    block_info['T'] = tt;
    block_info['D'] = td;
    block_info['L'] = tl;
    block_info['G'] = tg;
  }

  void CoordinatorImpl::find_max_group(int &max_group_id, int &max_group_num, int cluster_id, int stripe_id)
  {
    int group_cnt[5] = {0};
    Cluster &cluster = m_cluster_table[cluster_id];
    std::vector<Block *>::iterator it;
    for (it = cluster.blocks.begin(); it != cluster.blocks.end(); it++)
    {
      if ((*it)->map2stripe == stripe_id)
      {
        group_cnt[(*it)->map2group]++;
      }
    }
    for (int i = 0; i <= m_encode_parameters.l_localparityblock; i++)
    {
      if (group_cnt[i] > max_group_num)
      {
        max_group_id = i;
        max_group_num = group_cnt[i];
      }
    }
  }

  int CoordinatorImpl::count_block_num(char type, int cluster_id, int stripe_id, int group_id)
  {
    int cnt = 0;
    Cluster &cluster = m_cluster_table[cluster_id];
    std::vector<Block *>::iterator it;
    for (it = cluster.blocks.begin(); it != cluster.blocks.end(); it++)
    {
      Block *block = *it;
      if (block->map2stripe == stripe_id)
      {
        if (group_id == -1)
        {
          if (type == 'T')
          {
            cnt++;
          }
          else if (block->block_type == type)
          {
            cnt++;
          }
        }
        else if (int(block->map2group) == group_id)
        {
          if (type == 'T')
          {
            cnt++;
          }
          else if (block->block_type == type)
          {
            cnt++;
          }
        }
      }
    }
    if (cnt == 0)
    {
      cluster.stripes.erase(stripe_id);
    }
    return cnt;
  }

  // find out if any specific type of block from the stripe exists in the cluster
  bool CoordinatorImpl::find_block(char type, int cluster_id, int stripe_id)
  {
    Cluster &cluster = m_cluster_table[cluster_id];
    std::vector<Block *>::iterator it;
    for (it = cluster.blocks.begin(); it != cluster.blocks.end(); it++)
    {
      if (stripe_id != -1 && int((*it)->map2stripe) == stripe_id && (*it)->block_type == type)
      {
        return true;
      }
      else if (stripe_id == -1 && (*it)->block_type == type)
      {
        return true;
      }
    }
    return false;
  }
} // namespace ECProject
