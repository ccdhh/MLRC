#include "coordinator.h"
#include "lrc.h"
#include "meta_definition.h"
#include "tinyxml2.h"
#include <algorithm>
#include <chrono>
#include <cmath>
#include <fstream>
#include <random>
#include <sstream>
#include <string>
#include <sys/time.h>
#include <unistd.h>
#include <vector>
template <typename T> inline T ceil(T const &A, T const &B) {
  return T((A + B - 1) / B);
};

template <typename T>
inline std::vector<size_t> argsort(const std::vector<T> &v) {
  std::vector<size_t> idx(v.size());
  std::iota(idx.begin(), idx.end(), 0);
  std::sort(idx.begin(), idx.end(),
            [&v](size_t i1, size_t i2) { return v[i1] < v[i2]; });
  return idx;
};

inline int rand_num(int range) {
  std::random_device rd;
  std::mt19937 gen(rd());
  std::uniform_int_distribution<int> dis(0, range - 1);
  int num = dis(gen);
  return num;
};

namespace ECProject {
grpc::Status CoordinatorImpl::setParameter(
    grpc::ServerContext *context, const coordinator_proto::Parameter *parameter,
    coordinator_proto::RepIfSetParaSuccess *setParameterReply) {
  ECSchema system_metadata(
      parameter->partial_decoding(),
      (ECProject::EncodeType)parameter->encodetype(),
      (ECProject::SingleStripePlacementType)parameter->s_stripe_placementtype(),
      (ECProject::MultiStripesPlacementType)parameter->m_stripe_placementtype(),
      parameter->k_datablock(), parameter->l_localparityblock(),
      parameter->g_m_globalparityblock(), parameter->b_datapergroup(),
      parameter->x_stripepermergegroup());
  m_encode_parameters = system_metadata;
  setParameterReply->set_ifsetparameter(true);
  m_cur_cluster_id = 0;
  m_cur_stripe_id = 0;
  m_object_commit_table.clear();
  m_object_updating_table.clear();
  m_stripe_deleting_table.clear();
  for (auto it = m_cluster_table.begin(); it != m_cluster_table.end(); it++) {
    Cluster &t_cluster = it->second;
    t_cluster.blocks.clear();
    t_cluster.stripes.clear();
  }
  for (auto it = m_node_table.begin(); it != m_node_table.end(); it++) {
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
    coordinator_proto::ReplyFromCoordinator *helloReplyFromCoordinator) {
  std::string prefix("Hello ");
  helloReplyFromCoordinator->set_message(prefix +
                                         helloRequestToCoordinator->name());
  std::cout << prefix + helloRequestToCoordinator->name() << std::endl;
  return grpc::Status::OK;
}

grpc::Status CoordinatorImpl::uploadOriginKeyValue(
    grpc::ServerContext *context,
    const coordinator_proto::RequestProxyIPPort *keyValueSize,
    coordinator_proto::ReplyProxyIPPort *proxyIPPort) {

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
  for (int i = 0; i < int(stripe.blocks.size()); i++) {
    object_placement.add_datanodeip(
        m_node_table[stripe.blocks[i]->map2node].node_ip);
    object_placement.add_datanodeport(
        m_node_table[stripe.blocks[i]->map2node].node_port);
    object_placement.add_blockkeys(stripe.blocks[i]->block_key);
  }

  grpc::ClientContext cont;
  proxy_proto::SetReply set_reply;
  std::string selected_proxy_ip = m_cluster_table[s_cluster_id].proxy_ip;
  int selected_proxy_port = m_cluster_table[s_cluster_id].proxy_port;
  std::string chosen_proxy =
      selected_proxy_ip + ":" + std::to_string(selected_proxy_port);
  grpc::Status status = m_proxy_ptrs[chosen_proxy]->encodeAndSetObject(
      &cont, object_placement, &set_reply);
  proxyIPPort->set_proxyip(selected_proxy_ip);
  proxyIPPort->set_proxyport(
      selected_proxy_port +
      ECProject::PROXY_PORT_SHIFT); // use another port to accept data
  if (status.ok()) {
    m_mutex.lock();
    m_object_updating_table[key] = new_object;
    m_mutex.unlock();
  } else {
    std::cout << "[SET] Send object placement failed!" << std::endl;
  }

  return grpc::Status::OK;
}

void CoordinatorImpl::initialize_optimal_lrc_stripe_placement(Stripe *stripe) {
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

  for (int i = 0; i < stripe->n; i++) {
    blocks_info[i].block_size = m_sys_config->BlockSize;
    blocks_info[i].map2stripe = stripe->stripe_id;
    blocks_info[i].map2key = stripe->object_keys[0];
    if (i < stripe->k) {
      std::string tmp = "_D";
      if (i < 10)
        tmp = "_D0";
      blocks_info[i].block_key =
          std::to_string(stripe->stripe_id) + tmp + std::to_string(i);
      blocks_info[i].block_id = i;
      blocks_info[i].block_type = 'D';
      blocks_info[i].map2group =
          (i % local_group_size / group_size) +
          i / local_group_size * group_num_of_one_local_group;
    } else if (i >= stripe->k && i < stripe->k + stripe->r) {
      std::string tmp = "_G";
      if (i - stripe->k < 10)
        tmp = "_G0";
      blocks_info[i].block_key = std::to_string(stripe->stripe_id) + tmp +
                                 std::to_string(i - stripe->k);
      blocks_info[i].block_id = i;
      blocks_info[i].block_type = 'G';
      blocks_info[i].map2group = stripe->z * group_num_of_one_local_group;
    } else {
      std::string tmp = "_L";
      if (i - stripe->k - stripe->r < 10)
        tmp = "_L0";
      blocks_info[i].block_key = std::to_string(stripe->stripe_id) + tmp +
                                 std::to_string(i - stripe->k - stripe->r);
      blocks_info[i].block_id = i;
      blocks_info[i].block_type = 'L';
      blocks_info[i].map2group =
          (i - stripe->k - stripe->r + 1) * group_num_of_one_local_group - 1;
    }
    blocks_info[i].map2cluster =
        (t_cluster_id + blocks_info[i].map2group) % m_sys_config->ClusterNum;
    int t_node_id =
        randomly_select_a_node(blocks_info[i].map2cluster, stripe->stripe_id);
    blocks_info[i].map2node = t_node_id;
    update_stripe_info_in_node(t_node_id, stripe->stripe_id, i);
    m_cluster_table[blocks_info[i].map2cluster].blocks.push_back(
        &blocks_info[i]);
    m_cluster_table[blocks_info[i].map2cluster].stripes.insert(
        stripe->stripe_id);
    stripe->blocks.push_back(&blocks_info[i]);
    stripe->place2clusters.insert(blocks_info[i].map2cluster);
    add_to_map(stripe->group_to_blocks, blocks_info[i].map2group, i);
  }

  stripe->num_groups = stripe->group_to_blocks.size();
}

void CoordinatorImpl::initialize_uniform_lrc_stripe_placement(Stripe *stripe) {
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

  for (int i = 0; i < stripe->z; i++) {
    if (i + larger_local_group_num == stripe->z) {
      local_group_size++;
    }
    for (int j = 0; j < local_group_size; j++) {
      if (j % group_size == 0) {
        group_num++;
      }
      blocks_info[block_num++].map2group = group_num;
    }
    blocks_info[stripe->k + stripe->r + i].map2group = group_num;
  }
  for (int i = 0; i < stripe->n; i++) {
    blocks_info[i].block_size = m_sys_config->BlockSize;
    blocks_info[i].map2stripe = stripe->stripe_id;
    blocks_info[i].map2key = stripe->object_keys[0];
    if (i < stripe->k) {
      std::string tmp = "_D";
      if (i < 10)
        tmp = "_D0";
      blocks_info[i].block_key =
          std::to_string(stripe->stripe_id) + tmp + std::to_string(i);
      blocks_info[i].block_id = i;
      blocks_info[i].block_type = 'D';
    } else if (i >= stripe->k && i < stripe->k + stripe->r) {
      std::string tmp = "_G";
      if (i - stripe->k < 10)
        tmp = "_G0";
      blocks_info[i].block_key = std::to_string(stripe->stripe_id) + tmp +
                                 std::to_string(i - stripe->k);
      blocks_info[i].block_id = i;
      blocks_info[i].block_type = 'G';
    } else {
      std::string tmp = "_L";
      if (i - stripe->k - stripe->r < 10)
        tmp = "_L0";
      blocks_info[i].block_key = std::to_string(stripe->stripe_id) + tmp +
                                 std::to_string(i - stripe->k - stripe->r);
      blocks_info[i].block_id = i;
      blocks_info[i].block_type = 'L';
    }
    blocks_info[i].map2cluster =
        (t_cluster_id + blocks_info[i].map2group) % m_sys_config->ClusterNum;
    int t_node_id =
        randomly_select_a_node(blocks_info[i].map2cluster, stripe->stripe_id);
    blocks_info[i].map2node = t_node_id;
    update_stripe_info_in_node(t_node_id, stripe->stripe_id, i);
    m_cluster_table[blocks_info[i].map2cluster].blocks.push_back(
        &blocks_info[i]);
    m_cluster_table[blocks_info[i].map2cluster].stripes.insert(
        stripe->stripe_id);
    stripe->blocks.push_back(&blocks_info[i]);
    stripe->place2clusters.insert(blocks_info[i].map2cluster);
    add_to_map(stripe->group_to_blocks, blocks_info[i].map2group, i);
  }

  stripe->num_groups = stripe->group_to_blocks.size();
}

void CoordinatorImpl::initialize_equiox_stripe_placement(Stripe *stripe) {
  std::string code_type = m_sys_config->CodeType;
  std::vector<std::vector<int>> OA_1_Information =
      Get_OA_Information("OA_1.txt");
  std::vector<std::vector<int>> OA_2_Information =
      Get_OA_Information("OA_2.txt");
  int OA1_row_num = OA_1_Information.size();
  int OA2_row_num = OA_2_Information.size();
  Block *blocks_info = new Block[stripe->n];
  assert(stripe->object_keys.size() == 1);
  int OA1_row =
      (static_cast<int>(floor(
           (stripe->stripe_id / (std::pow(2, stripe->N) * OA2_row_num)))) %
       OA1_row_num) +
      1; // 机架行数
  int cluster_num =
      ceil(static_cast<double>(stripe->n) /
           static_cast<double>(stripe->r)); // 分组组数（带校验块）
  int b =
      (stripe->n - 1) % stripe->r + 1; // 分组后剩余块数，用来决定多少个m-1组
  std::vector<int> use_OA1_list;
  int initial_list =
      ((stripe->stripe_id) % static_cast<int>(std::pow(2, stripe->N))) + 1;  //所用OA1这行的第几个条带
  int use_OA1_list_num_1 =
      static_cast<int>((initial_list - 1) * (cluster_num - 1) + initial_list+1);
  std::vector<int> temp_vector;
  for (int i = 1; i <= initial_list - 1; i++) {
    int num = initial_list - i;
    int count = 0;
    while (!(num % 2)) {
      count++;
      num /= 2;
    }
    temp_vector.push_back(stripe->num_arry[count]);
  }
  int sum = 0;
  std::for_each(temp_vector.begin(), temp_vector.end(),
                [&sum](int num) { sum += num; });
  int list_num = static_cast<int>(use_OA1_list_num_1-sum);//条带的第一组放置列
  for (int i = 0; i < cluster_num - 1; i++)
    use_OA1_list.push_back(
        OA_1_Information[OA1_row - 1][initial_list + i -
                                      1]); // 这里校验组所用列应该单独拿出来
  int temp_num = 0;
  int temp_num1 = 0;
  for (int i = 0; i < stripe->n; i++) {
    blocks_info[i].block_size = m_sys_config->BlockSize;
    blocks_info[i].map2stripe = stripe->stripe_id;
    blocks_info[i].map2key = stripe->object_keys[0];
    if (i < stripe->k) // 说明是数据块
    {
      std::string tmp = "_D";
      if (i < 10)
        tmp = "_D0";
      blocks_info[i].block_key =
          std::to_string(stripe->stripe_id) + tmp + std::to_string(i);
      blocks_info[i].block_id = i;
      blocks_info[i].block_type = 'D';
      blocks_info[i].map2group = [stripe, b, cluster_num, i]() -> int {
        int small_group_num = stripe->r - b;
        int small_group_total = small_group_num * (stripe->r - 1);
        if (i < small_group_total)
          return i / (stripe->r - 1);
        else
          return small_group_num + (i - small_group_total) / stripe->r;
      }();
      blocks_info[i].map2cluster =
          (((stripe->stripe_id) % 4) * (cluster_num - 1) + 1 +
           blocks_info[i].map2group) %
          m_sys_config->ClusterNum;
    } else if (i >= stripe->k && i < stripe->k + stripe->r) {
      std::string tmp = "_G";
      if (i - stripe->k < 10)
        tmp = "_G0";
      blocks_info[i].block_key = std::to_string(stripe->stripe_id) + tmp +
                                 std::to_string(i - stripe->k);
      blocks_info[i].block_id = i;
      blocks_info[i].block_type = 'G';
      if (code_type == "UniLRC")
        blocks_info[i].map2group =
            int((i - stripe->k) / (stripe->r / stripe->z));
      else if (code_type == "AzureLRC")
        blocks_info[i].map2group = int(stripe->z);
      else if (code_type == "RS") {
        int idx = i - stripe->k;
        blocks_info[i].map2group = [stripe, b, cluster_num, idx]() -> int {
          int small_group_num = stripe->r - b;
          int small_group_total = small_group_num * (stripe->r - 1);
          if (idx < small_group_total)
            return idx / (stripe->r - 1);
          else
            return small_group_num + (idx - small_group_total) / stripe->r;
        }();
      }
      blocks_info[i].map2cluster =
          (use_OA1_list[0] - 1) % m_sys_config->ClusterNum;
    }
    int OA2_row = floor(stripe->stripe_id / 4);
    if (blocks_info[i].map2group < stripe->r - b) {
      blocks_info[i].map2node = OA_2_Information[OA2_row - 1][temp_num];
      temp_num = (temp_num + 1) % (stripe->r - 1);
    } else {
      blocks_info[i].map2node = OA_2_Information[OA2_row - 1][temp_num1];
      temp_num1 = (temp_num1 + 1) % stripe->r;
    }
    update_stripe_info_in_node(blocks_info[i].map2node, stripe->stripe_id, i);
    m_cluster_table[blocks_info[i].map2cluster].blocks.push_back(
        &blocks_info[i]);
    m_cluster_table[blocks_info[i].map2cluster].stripes.insert(
        stripe->stripe_id);
    stripe->blocks.push_back(&blocks_info[i]);
    stripe->place2clusters.insert(blocks_info[i].map2cluster);
    add_to_map(stripe->group_to_blocks, blocks_info[i].map2group, i);
  }
  stripe->num_groups = stripe->group_to_blocks.size();
}

void CoordinatorImpl::initialize_unilrc_and_azurelrc_stripe_placement(
    Stripe *stripe) {
  std::string code_type = m_sys_config->CodeType;

  // range 0~k-1: data blocks
  // range k~k+r-1: global parity blocks
  // range k+r~k+r+z-1: local parity blocks
  Block *blocks_info = new Block[stripe->n];
  // a stripe is only created by a single client
  assert(stripe->object_keys.size() == 1);
  // choose a cluster: round robin
  int t_cluster_id = stripe->stripe_id % m_sys_config->ClusterNum;
  for (int i = 0; i < stripe->n; i++) {
    blocks_info[i].block_size = m_sys_config->BlockSize;
    blocks_info[i].map2stripe = stripe->stripe_id;
    blocks_info[i].map2key = stripe->object_keys[0];
    if (i < stripe->k) {
      std::string tmp = "_D";
      if (i < 10)
        tmp = "_D0";
      blocks_info[i].block_key =
          std::to_string(stripe->stripe_id) + tmp + std::to_string(i);
      blocks_info[i].block_id = i;
      blocks_info[i].block_type = 'D';
      blocks_info[i].map2group = int(i / (stripe->k / stripe->z));
    } else if (i >= stripe->k && i < stripe->k + stripe->r) {
      std::string tmp = "_G";
      if (i - stripe->k < 10)
        tmp = "_G0";
      blocks_info[i].block_key = std::to_string(stripe->stripe_id) + tmp +
                                 std::to_string(i - stripe->k);
      blocks_info[i].block_id = i;
      blocks_info[i].block_type = 'G';
      if (code_type == "UniLRC") {
        blocks_info[i].map2group =
            int((i - stripe->k) / (stripe->r / stripe->z)); // 放置方式
      } else if (code_type == "AzureLRC") {
        blocks_info[i].map2group = int(stripe->z);
      }
    } else {
      std::string tmp = "_L";
      if (i - stripe->k - stripe->r < 10)
        tmp = "_L0";
      blocks_info[i].block_key = std::to_string(stripe->stripe_id) + tmp +
                                 std::to_string(i - stripe->k - stripe->r);
      blocks_info[i].block_id = i;
      blocks_info[i].block_type = 'L';
      blocks_info[i].map2group =
          int((i - stripe->k - stripe->r) / (stripe->z / stripe->z));
    }
    blocks_info[i].map2cluster =
        (t_cluster_id + blocks_info[i].map2group) % m_sys_config->ClusterNum;
    int t_node_id =
        randomly_select_a_node(blocks_info[i].map2cluster, stripe->stripe_id);
    blocks_info[i].map2node = t_node_id;
    update_stripe_info_in_node(t_node_id, stripe->stripe_id, i);
    m_cluster_table[blocks_info[i].map2cluster].blocks.push_back(
        &blocks_info[i]);
    m_cluster_table[blocks_info[i].map2cluster].stripes.insert(
        stripe->stripe_id);
    stripe->blocks.push_back(&blocks_info[i]);
    stripe->place2clusters.insert(blocks_info[i].map2cluster);
    add_to_map(stripe->group_to_blocks, blocks_info[i].map2group, i);
  }

  stripe->num_groups = stripe->group_to_blocks.size();
}

void CoordinatorImpl::add_to_map(std::map<int, std::vector<int>> &map, int key,
                                 int value) {
  if (map.find(key) == map.end())
    map[key] = std::vector<int>();
  map[key].push_back(value);
}

int CoordinatorImpl::getClusterAppendSize(
    Stripe *stripe,
    const std::map<int, std::pair<int, int>> &block_to_slice_sizes,
    int curr_group_id, int parity_slice_size) {
  int cluster_append_size = 0;

  for (int i = curr_group_id * stripe->k / stripe->z;
       i < (curr_group_id + 1) * stripe->k / stripe->z; i++) {
    if (block_to_slice_sizes.find(i) != block_to_slice_sizes.end())
      cluster_append_size += block_to_slice_sizes.at(i).first;
  }

  cluster_append_size +=
      parity_slice_size * (stripe->r + stripe->z) / stripe->z;
  return cluster_append_size;
}

// add repeated fields to plan
void addBlockToAppendPlan(proxy_proto::AppendStripeDataPlacement &plan,
                          const Block *block, const Node &node,
                          const std::pair<int, int> &slice_info) {
  plan.add_datanodeip(node.node_ip);
  plan.add_datanodeport(node.node_port);
  plan.add_blockkeys(block->block_key);
  plan.add_blockids(block->block_id);
  plan.add_offsets(slice_info.second);
  plan.add_sizes(slice_info.first);
}

std::vector<proxy_proto::AppendStripeDataPlacement>
CoordinatorImpl::generateAppendPlan(Stripe *stripe, int curr_logical_offset,
                                    int append_size) {
  std::vector<proxy_proto::AppendStripeDataPlacement> append_plans;
  std::string append_mode = m_sys_config->AppendMode;
  int unit_size = m_sys_config->UnitSize;
  int remain_size = stripe->k * m_sys_config->BlockSize - curr_logical_offset;
  assert(remain_size >= append_size &&
         "append size is larger than the remaining size of the stripe!");

  // int curr_group_id = (curr_logical_offset / (unit_size * stripe->k /
  // stripe->z)) % stripe->z;
  int curr_block_id = (curr_logical_offset / unit_size) % stripe->k;
  // compute how many units that need to be appended
  int num_units = (curr_logical_offset + append_size - 1) / unit_size -
                  curr_logical_offset / unit_size + 1;
  // int num_data_groups = std::min((curr_logical_offset + append_size - 1) /
  // (unit_size * stripe->k / stripe->z) - curr_logical_offset / (unit_size *
  // stripe->k / stripe->z) + 1, stripe->z);
  int num_unit_stripes =
      (curr_logical_offset + append_size - 1) / (unit_size * stripe->k) -
      curr_logical_offset / (unit_size * stripe->k) + 1;

  // compute the size and offset of the parity slice
  // TODO: optimize the append size that below a unit_size but placed into two
  // units within a unit_stripe
  int parity_slice_size = -1;
  int parity_slice_offset = -1;
  switch (append_mode[0]) {
  case 'R': // REP_MODE
    parity_slice_size = append_size;
    break;
  case 'U': // UNILRC_MODE
    parity_slice_size = num_unit_stripes * unit_size;
    parity_slice_offset =
        curr_logical_offset / (unit_size * stripe->k) * unit_size;
    if (num_units == 1) {
      parity_slice_size = append_size;
      parity_slice_offset += curr_logical_offset % unit_size;
    }
    if (num_unit_stripes > 1 &&
        (curr_logical_offset + append_size - 1) % (unit_size * stripe->k) <
            unit_size - 1) {
      parity_slice_size =
          (num_unit_stripes - 1) * unit_size +
          (curr_logical_offset + append_size - 1) % (unit_size * stripe->k) + 1;
    }
    break;
  case 'C': // CACHED_MODE
    parity_slice_size = num_unit_stripes * unit_size;
    parity_slice_offset =
        curr_logical_offset / (unit_size * stripe->k) * unit_size;
    break;
  default:
    std::cout << "[ERROR] Invalid append mode: " << append_mode << std::endl;
    return append_plans;
  }

  // key: block_id, value: (slice_size, physical_offset)
  std::map<int, std::pair<int, int>> block_to_slice_sizes;
  int tmp_size = append_size;
  int tmp_offset = curr_logical_offset;
  bool is_merge_parity =
      curr_logical_offset + append_size == m_sys_config->BlockSize * stripe->k;

  // add data slices to block_to_slice_sizes
  while (tmp_size > 0) {
    int sub_slice_size = unit_size;
    // first slice
    if (tmp_size == append_size && curr_logical_offset % unit_size != 0) {
      sub_slice_size =
          std::min(unit_size - curr_logical_offset % unit_size, append_size);
    } else {
      sub_slice_size = std::min(unit_size, tmp_size);
    }
    if (block_to_slice_sizes.find(curr_block_id) ==
        block_to_slice_sizes.end()) {
      block_to_slice_sizes[curr_block_id].first = sub_slice_size;
      block_to_slice_sizes[curr_block_id].second =
          tmp_offset % unit_size +
          unit_size * (tmp_offset / (stripe->k * unit_size));
    } else {
      block_to_slice_sizes[curr_block_id].first += sub_slice_size;
    }
    curr_block_id = (curr_block_id + 1) % stripe->k;
    tmp_size -= sub_slice_size;
    tmp_offset += sub_slice_size;
  }

  // add parity slices to block_to_slice_sizes
  for (int i = stripe->k; i < stripe->n; i++) {
    block_to_slice_sizes[i].first = parity_slice_size;
    block_to_slice_sizes[i].second = parity_slice_offset;
  }

  for (int i = 0; i < stripe->z; i++) {
    proxy_proto::AppendStripeDataPlacement plan;
    plan.set_key(m_toolbox->gen_append_key(stripe->stripe_id, i));
    plan.set_stripe_id(stripe->stripe_id);
    plan.set_append_size(getClusterAppendSize(stripe, block_to_slice_sizes, i,
                                              parity_slice_size));
    plan.set_is_merge_parity(is_merge_parity);
    plan.set_cluster_id(
        stripe->blocks[stripe->group_to_blocks[i][0]]->map2cluster);
    plan.set_append_mode(append_mode);
    if (curr_logical_offset == 0 &&
        append_size == m_sys_config->BlockSize * stripe->k) {
      plan.set_is_serialized(false);
      plan.set_is_merge_parity(false);
    } else {
      plan.set_is_serialized(true);
    }

    // Add data slices to plan
    for (int j = i * stripe->k / stripe->z; j < (i + 1) * stripe->k / stripe->z;
         j++) {
      if (block_to_slice_sizes.find(j) != block_to_slice_sizes.end()) {
        addBlockToAppendPlan(plan, stripe->blocks[j],
                             m_node_table[stripe->blocks[j]->map2node],
                             block_to_slice_sizes.at(j));
      }
    }

    // Add global parity slices to plan
    for (int j = stripe->k + i * stripe->r / stripe->z;
         j < stripe->k + (i + 1) * stripe->r / stripe->z; j++) {
      addBlockToAppendPlan(plan, stripe->blocks[j],
                           m_node_table[stripe->blocks[j]->map2node],
                           block_to_slice_sizes.at(j));
    }

    // Add local parity slices to plan
    for (int j = stripe->k + stripe->r + i * stripe->z / stripe->z;
         j < stripe->k + stripe->r + (i + 1) * stripe->z / stripe->z; j++) {
      addBlockToAppendPlan(plan, stripe->blocks[j],
                           m_node_table[stripe->blocks[j]->map2node],
                           block_to_slice_sizes.at(j));
    }

    append_plans.push_back(plan);
  }

  return append_plans;
}

void CoordinatorImpl::notify_proxies_ready(
    const proxy_proto::AppendStripeDataPlacement &plan) {
  grpc::ClientContext cont;
  proxy_proto::SetReply set_reply;
  std::string chosen_proxy =
      m_cluster_table[plan.cluster_id()].proxy_ip + ":" +
      std::to_string(m_cluster_table[plan.cluster_id()].proxy_port);
  grpc::Status status = m_proxy_ptrs[chosen_proxy]->scheduleAppend2Datanode(
      &cont, plan, &set_reply);
  if (status.ok()) {
    m_mutex.lock();
    m_object_updating_table[plan.key()] =
        ObjectInfo(plan.append_size(), plan.stripe_id());
    m_mutex.unlock();
  } else {
    std::cout << "[APPEND434] Send append plan" << plan.key() << " failed! "
              << std::endl;
  }
}

// Only processing the appending within a single stripe
grpc::Status CoordinatorImpl::uploadAppendValue(
    grpc::ServerContext *context,
    const coordinator_proto::RequestProxyIPPort *keyValueSize,
    coordinator_proto::ReplyProxyIPsPorts *proxyIPPort) {
  std::string clientID = keyValueSize->key();
  int appendSizeBytes = keyValueSize->valuesizebytes();
  std::string append_mode = keyValueSize->append_mode();

  // 1. record metadata
  // logical offset within the block stripe
  if (m_cur_offset_table.find(clientID) == m_cur_offset_table.end()) {
    // first append
    m_cur_offset_table[clientID] = StripeOffset(m_cur_stripe_id++, 0);
  }
  StripeOffset curStripeOffset = m_cur_offset_table[clientID];

  assert(curStripeOffset.offset + appendSizeBytes <=
             m_sys_config->BlockSize * m_sys_config->k &&
         "append size is larger than the remaining size of the stripe!");

  // 2. generate data placement
  Stripe *stripe = nullptr;
  if (curStripeOffset.offset == 0) {
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
  } else {
    // append to the existing stripe
    stripe = &m_stripe_table[curStripeOffset.stripe_id];
  }

  std::vector<proxy_proto::AppendStripeDataPlacement> append_plans =
      generateAppendPlan(stripe, curStripeOffset.offset, appendSizeBytes);
  if (append_plans.empty()) {
    std::cout << "[ERROR] Invalid append mode: " << append_mode << std::endl;
    return grpc::Status(grpc::StatusCode::INVALID_ARGUMENT,
                        "Invalid append mode");
  }

  for (const auto &plan : append_plans) {
    m_mutex.lock();
    m_object_commit_table.erase(plan.key());
    m_mutex.unlock();
  }

  // 3. notify proxies to receive data
  // need multiple proxies to receive data, so need multiple threads
  std::vector<std::thread> threads;
  int sum_append_size = 0;
  for (const auto &plan : append_plans) {
    threads.push_back(
        std::thread(&CoordinatorImpl::notify_proxies_ready, this, plan));
    proxyIPPort->add_append_keys(plan.key());
    proxyIPPort->add_proxyips(m_cluster_table[plan.cluster_id()].proxy_ip);
    proxyIPPort->add_proxyports(
        m_cluster_table[plan.cluster_id()].proxy_port +
        ECProject::PROXY_PORT_SHIFT); // use another port to accept data
    proxyIPPort->add_cluster_slice_sizes(plan.append_size());
    sum_append_size += plan.append_size();
  }
  for (auto &thread : threads) {
    thread.join();
  }
  proxyIPPort->set_sum_append_size(sum_append_size);

  m_cur_offset_table[clientID].offset += appendSizeBytes;
  // std::cout << "[Coordinator] stripe_id: " <<
  // m_cur_offset_table[clientID].stripe_id << " offset: " <<
  // m_cur_offset_table[clientID].offset << " is_erase " <<
  // (m_cur_offset_table[clientID].offset == m_sys_config->BlockSize *
  // m_sys_config->k) << std::endl;
  if (m_cur_offset_table[clientID].offset ==
      m_sys_config->BlockSize * m_sys_config->k) {
    m_cur_offset_table.erase(clientID);
  }

  return grpc::Status::OK;
}

std::vector<proxy_proto::AppendStripeDataPlacement>
CoordinatorImpl::generate_add_plans(Stripe *stripe) {
  std::vector<proxy_proto::AppendStripeDataPlacement> add_plans;
  for (int i = 0; i < stripe->num_groups; i++) {
    proxy_proto::AppendStripeDataPlacement plan;
    int mapped_cluster_id =
        stripe->blocks[stripe->group_to_blocks[i][0]]->map2cluster;
    size_t append_size =
        stripe->group_to_blocks[i].size() * m_sys_config->BlockSize;

    plan.set_key(m_toolbox->gen_append_key(stripe->stripe_id, i));
    plan.set_stripe_id(stripe->stripe_id);
    plan.set_append_size(append_size);
    plan.set_is_merge_parity(false);
    plan.set_cluster_id(mapped_cluster_id);
    plan.set_append_mode("UNILRC_MODE");
    plan.set_is_serialized(false);

    for (int j = 0; j < stripe->group_to_blocks[i].size(); j++) {
      addBlockToAppendPlan(
          plan, stripe->blocks[stripe->group_to_blocks[i][j]],
          m_node_table[stripe->blocks[stripe->group_to_blocks[i][j]]->map2node],
          std::make_pair(m_sys_config->BlockSize, 0));
    }

    add_plans.push_back(plan);
  }

  return add_plans;
}

std::vector<proxy_proto::AppendStripeDataPlacement>
CoordinatorImpl::generate_sub_add_plans(Stripe *stripe, size_t subset_size) {
  int data_block_num = subset_size / m_sys_config->BlockSize;
  int k = m_sys_config->k;
  int r = m_sys_config->r;
  int z = m_sys_config->z;
  std::vector<proxy_proto::AppendStripeDataPlacement> add_plans;
  for (int i = 0; i < stripe->num_groups; i++) {
    proxy_proto::AppendStripeDataPlacement plan;
    int block_num = 0;
    for (int j = 0; j < stripe->group_to_blocks[i].size(); j++) {
      int block_id = stripe->group_to_blocks[i][j];
      if (block_id < k && block_id >= data_block_num) {
        continue;
      }
      addBlockToAppendPlan(
          plan, stripe->blocks[stripe->group_to_blocks[i][j]],
          m_node_table[stripe->blocks[stripe->group_to_blocks[i][j]]->map2node],
          std::make_pair(m_sys_config->BlockSize, 0));
      block_num++;
    }

    size_t append_size = block_num * m_sys_config->BlockSize;
    if (append_size == 0) {
      // plan.set_append_size(0);
      // add_plans.push_back(plan);
      continue; // no data to append
    }

    int mapped_cluster_id =
        stripe->blocks[stripe->group_to_blocks[i][0]]->map2cluster;

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

void CoordinatorImpl::print_stripe_data_placement(Stripe &stripe) {
  std::cout << "Stripe " << stripe.stripe_id
            << " data placement: " << std::endl;
  for (int i = 0; i < stripe.num_groups; i++) {
    std::cout << "Group " << i << ": (" << stripe.group_to_blocks[i].size()
              << " blocks, mapped to cluster "
              << stripe.blocks[stripe.group_to_blocks[i][0]]->map2cluster
              << ") ";
    for (int j = 0; j < stripe.group_to_blocks[i].size(); j++) {
      std::cout << stripe.blocks[stripe.group_to_blocks[i][j]]->block_key
                << " ";
    }
    std::cout << std::endl;
  }
}

// set only the full block stripe
grpc::Status CoordinatorImpl::uploadSetValue(
    grpc::ServerContext *context,
    const coordinator_proto::RequestProxyIPPort *keyValueSize,
    coordinator_proto::ReplyProxyIPsPorts *proxyIPPort) {
  std::string clientID = keyValueSize->key();
  size_t setSizeBytes = keyValueSize->valuesizebytes();
  std::string code_type = m_sys_config->CodeType;
  assert(setSizeBytes == static_cast<size_t>(m_sys_config->BlockSize) *
                             static_cast<size_t>(m_sys_config->k) &&
         "set size is not equal to the block stripe size!");
  assert(
      (code_type == "UniLRC" || code_type == "AzureLRC" ||
       code_type == "OptimalLRC" || code_type == "UniformLRC" ||
       code_type == "RS") &&
      "Error: code type must be UniLRC, AzureLRC, OptimalLRC, or UniformLRC!");

  Stripe t_stripe;
  t_stripe.stripe_id = m_cur_stripe_id++;
  t_stripe.n = m_sys_config->n;
  t_stripe.k = m_sys_config->k;
  t_stripe.r = m_sys_config->r;
  t_stripe.z = m_sys_config->z;
  t_stripe.N = m_sys_config->N;
  t_stripe.num_arry = m_sys_config->num_arry;
  t_stripe.object_keys.push_back(clientID);
  if (code_type == "UniLRC" || code_type == "AzureLRC") {
    initialize_unilrc_and_azurelrc_stripe_placement(&t_stripe);
  } else if (code_type == "OptimalLRC") {
    initialize_optimal_lrc_stripe_placement(&t_stripe);
  } else if (code_type == "UniformLRC") {
    initialize_uniform_lrc_stripe_placement(&t_stripe);
  } else if (code_type == "RS") {
    initialize_equiox_stripe_placement(&t_stripe);
  }

  print_stripe_data_placement(t_stripe);

  std::vector<proxy_proto::AppendStripeDataPlacement> add_plans =
      generate_add_plans(&t_stripe);

  for (const auto &plan : add_plans) {
    m_mutex.lock();
    m_object_commit_table.erase(plan.key());
    m_mutex.unlock();
  }

  std::vector<std::thread> threads;
  size_t sum_append_size = 0;
  for (const auto &plan : add_plans) {
    threads.push_back(
        std::thread(&CoordinatorImpl::notify_proxies_ready, this, plan));
    proxyIPPort->add_append_keys(plan.key());
    proxyIPPort->add_proxyips(m_cluster_table[plan.cluster_id()].proxy_ip);
    proxyIPPort->add_proxyports(
        m_cluster_table[plan.cluster_id()].proxy_port +
        ECProject::PROXY_PORT_SHIFT); // use another port to accept data
    proxyIPPort->add_cluster_slice_sizes(plan.append_size());
    sum_append_size += plan.append_size();
  }
  for (auto &thread : threads) {
    thread.join();
  }
  proxyIPPort->set_sum_append_size(sum_append_size);

  m_stripe_table[t_stripe.stripe_id] = std::move(t_stripe);

  return grpc::Status::OK;
}

grpc::Status CoordinatorImpl::uploadSubsetValue(
    grpc::ServerContext *context,
    const coordinator_proto::RequestProxyIPPort *keyValueSize,
    coordinator_proto::ReplyProxyIPsPorts *proxyIPPort) {
  std::string clientID = keyValueSize->key();
  size_t setSizeBytes = keyValueSize->valuesizebytes();
  std::string code_type = m_sys_config->CodeType;
  assert(setSizeBytes <= static_cast<size_t>(m_sys_config->BlockSize) *
                             static_cast<size_t>(m_sys_config->k) &&
         "subset size is larger than the block size!");
  assert(
      (code_type == "UniLRC" || code_type == "AzureLRC" ||
       code_type == "OptimalLRC" || code_type == "UniformLRC") &&
      "Error: code type must be UniLRC, AzureLRC, OptimalLRC, or UniformLRC!");

  Stripe t_stripe;
  t_stripe.stripe_id = m_cur_stripe_id++;
  t_stripe.n = m_sys_config->n;
  t_stripe.k = m_sys_config->k;
  t_stripe.r = m_sys_config->r;
  t_stripe.z = m_sys_config->z;
  t_stripe.object_keys.push_back(clientID);
  if (code_type == "UniLRC" || code_type == "AzureLRC") {
    initialize_unilrc_and_azurelrc_stripe_placement(&t_stripe);
  } else if (code_type == "OptimalLRC") {
    initialize_optimal_lrc_stripe_placement(&t_stripe);
  } else if (code_type == "UniformLRC") {
    initialize_uniform_lrc_stripe_placement(&t_stripe);
  }

  print_stripe_data_placement(t_stripe);

  std::vector<proxy_proto::AppendStripeDataPlacement> add_plans =
      generate_sub_add_plans(&t_stripe, setSizeBytes);

  for (const auto &plan : add_plans) {
    m_mutex.lock();
    m_object_commit_table.erase(plan.key());
    m_mutex.unlock();
  }

  std::vector<std::thread> threads;
  size_t sum_append_size = 0;
  for (const auto &plan : add_plans) {
    threads.push_back(
        std::thread(&CoordinatorImpl::notify_proxies_ready, this, plan));
    proxyIPPort->add_append_keys(plan.key());
    proxyIPPort->add_proxyips(m_cluster_table[plan.cluster_id()].proxy_ip);
    proxyIPPort->add_proxyports(
        m_cluster_table[plan.cluster_id()].proxy_port +
        ECProject::PROXY_PORT_SHIFT); // use another port to accept data
    proxyIPPort->add_cluster_slice_sizes(plan.append_size());
    // proxyIPPort->add_group_ids(group_id);
    sum_append_size += plan.append_size();
    // group_id++;
  }
  for (auto &thread : threads) {
    thread.join();
  }
  proxyIPPort->set_sum_append_size(sum_append_size);

  m_stripe_table[t_stripe.stripe_id] = std::move(t_stripe);

  return grpc::Status::OK;
}

std::vector<int> CoordinatorImpl::get_recovery_group_ids(std::string code_type,
                                                         int k, int r, int z,
                                                         int failed_block_id) {
  std::vector<int> recovery_group_ids;
  if (code_type == "AzureLRC") {
    if (failed_block_id >= k && failed_block_id < k + r) {
      for (int i = 1; i <= z; i++) {
        recovery_group_ids.push_back(i);
      }
    } else if (failed_block_id >= k + r) {
      recovery_group_ids.push_back(failed_block_id - k - r);
    } else {
      recovery_group_ids.push_back(failed_block_id / (k / z));
    }
  } else if (code_type == "UniLRC") {
    if (failed_block_id >= k && failed_block_id < k + r) {
      recovery_group_ids.push_back((failed_block_id - k) / (r / z));
    } else if (failed_block_id >= k + r) {
      recovery_group_ids.push_back(failed_block_id - k - r);
    } else {
      recovery_group_ids.push_back(failed_block_id / (k / z));
    }
  } else if (code_type == "OptimalLRC") {
    if (failed_block_id >= k && failed_block_id < k + r) {
      int group_num = (k / z / (r + 1) + (bool)(k / z % (r + 1))) * z + 1;
      recovery_group_ids.push_back(group_num - 1);
      for (int i = 0; i < group_num / z; i++) {
        recovery_group_ids.push_back(i);
      }
    } else if (failed_block_id >= k + r) {
      int local_group_size = k / z;
      int local_group_id = (failed_block_id - k - r);
      int group_num_of_one_local_group = local_group_size / (r + 1) + 1;
      int group_num = z * group_num_of_one_local_group + 1;
      recovery_group_ids.push_back(
          (local_group_id + 1) * group_num_of_one_local_group - 1);
      for (int i = local_group_id * group_num_of_one_local_group;
           i < (local_group_id + 1) * group_num_of_one_local_group - 1; i++) {
        recovery_group_ids.push_back(i);
      }
      recovery_group_ids.push_back(group_num - 1);
    } else {
      int local_group_size = k / z;
      int group_num_of_one_local_group = local_group_size / (r + 1) + 1;
      int local_group_id = failed_block_id / local_group_size;
      int group_id_in_local_group =
          failed_block_id % local_group_size / (r + 1);
      recovery_group_ids.push_back(local_group_id *
                                       group_num_of_one_local_group +
                                   group_id_in_local_group);
      for (int i = 0; i < group_num_of_one_local_group; i++) {
        if (i != group_id_in_local_group) {
          recovery_group_ids.push_back(
              local_group_id * group_num_of_one_local_group + i);
        }
      }
      int group_num = z * group_num_of_one_local_group + 1;
      recovery_group_ids.push_back(group_num - 1);
    }
  } else if (code_type == "UniformLRC") {
    if (failed_block_id >= k + r) {
      int larger_local_group_num = (k + r) % z;
      int local_group_id = failed_block_id - k - r;
      int local_group_size = (k + r) / z;
      int group_num_of_one_local_group =
          local_group_size / r + bool(local_group_size % r);
      if (local_group_id + larger_local_group_num < z) {
        recovery_group_ids.push_back(
            (local_group_id + 1) * group_num_of_one_local_group - 1);
        for (int i = local_group_id * group_num_of_one_local_group;
             i < (local_group_id + 1) * group_num_of_one_local_group - 1; i++) {
          recovery_group_ids.push_back(i);
        }
      } else {
        int smaller_local_group_num = z - larger_local_group_num;
        int group_num_of_all_small_group =
            smaller_local_group_num * group_num_of_one_local_group;
        local_group_size++;
        group_num_of_one_local_group =
            local_group_size / r + (bool)(local_group_size % r);
        local_group_id = local_group_id - smaller_local_group_num;
        recovery_group_ids.push_back(
            group_num_of_all_small_group +
            (local_group_id + 1) * group_num_of_one_local_group - 1);
        for (int i = group_num_of_all_small_group +
                     local_group_id * group_num_of_one_local_group;
             i < group_num_of_all_small_group +
                     (local_group_id + 1) * group_num_of_one_local_group - 1;
             i++) {
          recovery_group_ids.push_back(i);
        }
      }
    } else if (failed_block_id < k + r) {
      int larger_local_group_num = (k + r) % z;
      int smaller_local_group_num = z - larger_local_group_num;
      int local_group_size = (k + r) / z;
      int group_num_of_one_local_group =
          local_group_size / r + bool(local_group_size % r);
      int block_num_of_smaller_local_group =
          (z - larger_local_group_num) * local_group_size;
      int group_num_of_smaller_local_group =
          smaller_local_group_num * group_num_of_one_local_group;
      int local_group_id = 0;
      if (failed_block_id < block_num_of_smaller_local_group) {
        local_group_id = failed_block_id / local_group_size;
        int block_num_in_previous_local_group =
            local_group_id * local_group_size;
        int group_id =
            local_group_id * group_num_of_one_local_group +
            (failed_block_id - block_num_in_previous_local_group) / r;
        recovery_group_ids.push_back(group_id);
        for (int i = local_group_id * group_num_of_one_local_group;
             i < local_group_id * group_num_of_one_local_group +
                     group_num_of_one_local_group;
             i++) {
          if (i != group_id) {
            recovery_group_ids.push_back(i);
          }
        }
      } else {
        local_group_size++;
        group_num_of_one_local_group =
            local_group_size / r + bool(local_group_size % r);
        local_group_id = (failed_block_id - block_num_of_smaller_local_group) /
                         local_group_size;
        int block_num_in_previous_local_group =
            local_group_id * local_group_size +
            block_num_of_smaller_local_group;
        int group_id =
            local_group_id * group_num_of_one_local_group +
            (failed_block_id - block_num_in_previous_local_group) / r;
        recovery_group_ids.push_back(group_id +
                                     group_num_of_smaller_local_group);
        for (int i = local_group_id * group_num_of_one_local_group;
             i < local_group_id * group_num_of_one_local_group +
                     group_num_of_one_local_group;
             i++) {
          if (i != group_id) {
            recovery_group_ids.push_back(i + group_num_of_smaller_local_group);
          }
        }
      }
    }
  }

  return recovery_group_ids;
}

void CoordinatorImpl::init_recovery_group_lookup_table() {
  for (int i = 0; i < m_sys_config->n; i++) {
    m_recovery_group_lookup_table[i] =
        get_recovery_group_ids(m_sys_config->CodeType, m_sys_config->k,
                               m_sys_config->r, m_sys_config->z, i);
  }
}

std::vector<int>
CoordinatorImpl::get_data_block_num_per_group(int k, int r, int z,
                                              std::string code_type) {
  std::vector<int> data_block_num_per_group;
  if (code_type == "AzureLRC") {
    for (int i = 0; i < z; i++) {
      data_block_num_per_group.push_back((k / z));
    }
    data_block_num_per_group.push_back(0);
  } else if (code_type == "OptimalLRC") {
    int group_size = r + 1;
    int local_group_size = (k / z);
    int group_num_of_one_local_group = local_group_size / group_size + 1;
    int group_num = z * group_num_of_one_local_group + 1;
    for (int i = 0; i < group_num - 1; i++) {
      if ((i + 1) % group_num_of_one_local_group) {
        data_block_num_per_group.push_back(group_size);
      } else {
        data_block_num_per_group.push_back(local_group_size % group_size);
      }
    }
    data_block_num_per_group.push_back(0);
  } else if (code_type == "UniformLRC") {
    /*int group_size = r + 1;
    int local_group_size = int((k + r) / z);
    int larger_local_group_num = int((k + r) % z);

    int group_num_of_one_local_group = local_group_size / group_size +
    (bool)(local_group_size % group_size); for (int i = 0; i < z - 1; i++)
    {
      if (i + larger_local_group_num == z)
      {
        local_group_size++;
        group_num_of_one_local_group = local_group_size / group_size +
    (bool)(local_group_size % group_size);
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

    for (int i = 0; i < z - 1; i++) {
      data_block_num_per_group.push_back((k + r) / z);
    }
    data_block_num_per_group.push_back(0);
  } else if (code_type == "UniLRC") {
    int local_data_num = k / z;
    for (int i = 0; i < z; i++) {
      data_block_num_per_group.push_back(local_data_num);
    }
  }
  return data_block_num_per_group;
}

void CoordinatorImpl::getStripeFromProxy(std::string client_ip, int client_port,
                                         std::string proxy_ip, int proxy_port,
                                         int stripe_id, int group_id,
                                         std::vector<int> block_ids) {
  std::cout << "[GET] getting stripe " << stripe_id << " from proxy "
            << proxy_ip << ":" << proxy_port << std::endl;
  for (int i = 0; i < block_ids.size(); i++) {
    std::cout << "block_id: " << block_ids[i] << std::endl;
  }
  grpc::ClientContext cont;
  proxy_proto::StripeAndBlockIDs stripe_block_ids;
  proxy_proto::GetReply stripe_reply;
  stripe_block_ids.set_stripe_id(stripe_id);
  stripe_block_ids.set_clientip(client_ip);
  stripe_block_ids.set_clientport(client_port);
  stripe_block_ids.set_group_id(group_id);

  for (int i = 0; i < block_ids.size(); i++) {
    stripe_block_ids.add_block_ids(block_ids[i]);
    stripe_block_ids.add_block_keys(
        m_stripe_table[stripe_id].blocks[block_ids[i]]->block_key);
    stripe_block_ids.add_datanodeips(
        m_node_table[m_stripe_table[stripe_id].blocks[block_ids[i]]->map2node]
            .node_ip);
    stripe_block_ids.add_datanodeports(
        m_node_table[m_stripe_table[stripe_id].blocks[block_ids[i]]->map2node]
            .node_port);
  }
  grpc::Status status =
      m_proxy_ptrs[proxy_ip + ":" + std::to_string(proxy_port)]->getBlocks(
          &cont, stripe_block_ids, &stripe_reply);
  if (status.ok()) {
    std::cout << "[GET] getting stripe " << stripe_id << " from proxy "
              << proxy_ip << ":" << proxy_port << " succeeded!" << std::endl;
  } else {
    std::cout << "[GET] getting stripe " << stripe_id << " from proxy "
              << proxy_ip << ":" << proxy_port << " failed!" << std::endl;
  }
}

grpc::Status
CoordinatorImpl::getStripe(grpc::ServerContext *context,
                           const coordinator_proto::KeyAndClientIP *keyClient,
                           coordinator_proto::ReplyProxyIPsPorts *proxyIPPort) {

  // std::chrono::high_resolution_clock::time_point start =
  // std::chrono::high_resolution_clock::now();
  int stripe_id = std::stoi(keyClient->key());
  Stripe &t_stripe = m_stripe_table[stripe_id];
  int k = t_stripe.k;
  int num_data_groups = t_stripe.num_groups;
  std::string code_type = m_sys_config->CodeType;
  if (code_type != "UniLRC") {
    num_data_groups--;
  }
  // std::cout << "[GET] getting stripe " << stripe_id << " with " <<
  // num_data_groups << " data groups" << std::endl;
  std::vector<int> block_num_per_group = get_data_block_num_per_group(
      k, m_sys_config->r, m_sys_config->z, code_type);
  std::vector<int> get_cluster_ids;
  for (int i = 0; i < num_data_groups; i++) {
    get_cluster_ids.push_back(
        t_stripe.blocks[t_stripe.group_to_blocks[i][0]]->map2cluster);
    // std::cout << "group " << i << " is mapped to cluster " <<
    // get_cluster_ids[i] << std::endl;
  }
  for (int i = 0; i < num_data_groups; i++) {
    proxyIPPort->add_proxyips(m_cluster_table[get_cluster_ids[i]].proxy_ip);
    proxyIPPort->add_proxyports(m_cluster_table[get_cluster_ids[i]].proxy_port);
    proxyIPPort->add_cluster_slice_sizes(block_num_per_group[i]);
  }
  /*for(int i = 0; i < t_stripe.num_groups; i++){
    m_proxy_ptrs[proxyIPPort->proxyips(i) + ":" +
  std::to_string(proxyIPPort->proxyports(i))]->getStripe(stripe_id,
  t_stripe.group_to_blocks[i]);
  }*/
  std::vector<std::thread> threads;
  for (int i = 0; i < num_data_groups; i++) {
    std::vector<int> block_ids;
    for (int j = 0; j < t_stripe.group_to_blocks[i].size(); j++) {
      if (t_stripe.blocks[t_stripe.group_to_blocks[i][j]]->block_id < k) {
        block_ids.push_back(t_stripe.group_to_blocks[i][j]);
      }
    }
    threads.push_back(std::thread(
        &CoordinatorImpl::getStripeFromProxy, this, keyClient->clientip(),
        keyClient->clientport(), proxyIPPort->proxyips(i),
        proxyIPPort->proxyports(i), stripe_id, i, block_ids));
  }
  for (auto &thread : threads) {
    thread.detach();
  }
  /*std::chrono::high_resolution_clock::time_point end =
  std::chrono::high_resolution_clock::now(); std::chrono::duration<double>
  duration = std::chrono::duration_cast<std::chrono::duration<double>>(end -
  start); std::cout << "[GET] getting stripe " << stripe_id << " took " <<
  duration.count() << " seconds" << std::endl;*/

  return grpc::Status::OK;
}

grpc::Status CoordinatorImpl::getBlocks(
    grpc::ServerContext *context,
    const coordinator_proto::BlockIDsAndClientIP *blockIDsClient,
    coordinator_proto::ReplyProxyIPsPorts *proxyIPPort) {
  std::string client_ip = blockIDsClient->clientip();
  int client_port = blockIDsClient->clientport();
  int start_block_id = blockIDsClient->start_block_id();
  int end_block_id = blockIDsClient->end_block_id();
  std::vector<int> stripe_ids;
  std::vector<int> block_ids;
  std::vector<int> relative_block_ids;
  for (int i = start_block_id; i <= end_block_id; i++) {
    int stripe_id = i / m_sys_config->k;
    stripe_ids.push_back(stripe_id);
    block_ids.push_back(i % m_sys_config->k);
    relative_block_ids.push_back(i - start_block_id);
  }
  std::vector<int> get_cluster_ids;
  std::vector<int> unique_cluster_ids;
  for (int i = 0; i < stripe_ids.size(); i++) {
    get_cluster_ids.push_back(
        m_stripe_table[stripe_ids[i]].blocks[block_ids[i]]->map2cluster);
    if (std::find(unique_cluster_ids.begin(), unique_cluster_ids.end(),
                  get_cluster_ids[i]) == unique_cluster_ids.end()) {
      unique_cluster_ids.push_back(get_cluster_ids[i]);
    }
  }
  proxy_proto::StripeAndBlockIDs stripe_block_ids[unique_cluster_ids.size()];
  for (int i = 0; i < stripe_ids.size(); i++) {
    int idx = std::find(unique_cluster_ids.begin(), unique_cluster_ids.end(),
                        get_cluster_ids[i]) -
              unique_cluster_ids.begin();
    stripe_block_ids[idx].add_block_ids(relative_block_ids[i]);
    stripe_block_ids[idx].add_block_keys(
        m_stripe_table[stripe_ids[i]].blocks[block_ids[i]]->block_key);
    stripe_block_ids[idx].add_datanodeips(
        m_node_table
            [m_stripe_table[stripe_ids[i]].blocks[block_ids[i]]->map2node]
                .node_ip);
    stripe_block_ids[idx].add_datanodeports(
        m_node_table
            [m_stripe_table[stripe_ids[i]].blocks[block_ids[i]]->map2node]
                .node_port);
  }
  std::vector<std::thread> get_threads;
  for (int i = 0; i < unique_cluster_ids.size(); i++) {
    get_threads.push_back(std::thread([this, &stripe_block_ids, &client_ip,
                                       &client_port, &proxyIPPort,
                                       &unique_cluster_ids, i]() {
      grpc::ClientContext cont;
      proxy_proto::GetReply stripe_reply;
      stripe_block_ids[i].set_clientip(client_ip);
      stripe_block_ids[i].set_clientport(client_port);
      grpc::Status status =
          m_proxy_ptrs[m_cluster_table[unique_cluster_ids[i]].proxy_ip + ":" +
                       std::to_string(
                           m_cluster_table[unique_cluster_ids[i]].proxy_port)]
              ->getBlocks(&cont, stripe_block_ids[i], &stripe_reply);
      if (status.ok()) {
        std::cout << "[GET] getting blocks from proxy "
                  << m_cluster_table[unique_cluster_ids[i]].proxy_ip << ":"
                  << m_cluster_table[unique_cluster_ids[i]].proxy_port
                  << " succeeded!" << std::endl;
      } else {
        std::cout << "[GET] getting blocks from proxy "
                  << m_cluster_table[unique_cluster_ids[i]].proxy_ip << ":"
                  << m_cluster_table[unique_cluster_ids[i]].proxy_port
                  << " failed!" << std::endl;
      }
    }));
  }
  for (auto &thread : get_threads) {
    thread.join();
  }
  return grpc::Status::OK;
}

grpc::Status CoordinatorImpl::getDegradedReadBlocks(
    grpc::ServerContext *context,
    const coordinator_proto::BlockIDsAndClientIP *blockIDsClient,
    coordinator_proto::ReplyProxyIPsPorts *proxyIPPort) {
  std::string client_ip = blockIDsClient->clientip();
  int client_port = blockIDsClient->clientport();
  int start_block_id = blockIDsClient->start_block_id();
  int end_block_id = blockIDsClient->end_block_id();
  std::vector<int> stripe_ids;
  std::vector<int> block_ids;
  std::vector<int> relative_block_ids;
  for (int i = start_block_id; i <= end_block_id; i++) {
    int stripe_id = i / m_sys_config->k;
    stripe_ids.push_back(stripe_id);
    block_ids.push_back(i % m_sys_config->k);
    relative_block_ids.push_back(i - start_block_id);
  }
  for (int i = 0; i < stripe_ids.size(); i++) {
    degraded_read_one_block_for_workload(stripe_ids[i], block_ids[i], client_ip,
                                         client_port, relative_block_ids[i]);
  }
  return grpc::Status::OK;
}

grpc::Status
CoordinatorImpl::getValue(grpc::ServerContext *context,
                          const coordinator_proto::KeyAndClientIP *keyClient,
                          coordinator_proto::RepIfGetSuccess *getReplyClient) {
  try {
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
    for (int i = 0; i < int(t_stripe.blocks.size()); i++) {
      if (t_stripe.blocks[i]->map2key == key) {
        object_placement.add_datanodeip(
            m_node_table[t_stripe.blocks[i]->map2node].node_ip);
        object_placement.add_datanodeport(
            m_node_table[t_stripe.blocks[i]->map2node].node_port);
        object_placement.add_blockkeys(t_stripe.blocks[i]->block_key);
        object_placement.add_blockids(t_stripe.blocks[i]->block_id);
        t_cluster_set.insert(t_stripe.blocks[i]->map2cluster);
      }
    }
    // randomly select a cluster
    int idx = rand_num(int(t_cluster_set.size()));
    int r_cluster_id = *(std::next(t_cluster_set.begin(), idx));
    std::string chosen_proxy =
        m_cluster_table[r_cluster_id].proxy_ip + ":" +
        std::to_string(m_cluster_table[r_cluster_id].proxy_port);
    status = m_proxy_ptrs[chosen_proxy]->decodeAndGetObject(
        &decode_and_get, object_placement, &get_reply);
    if (status.ok()) {
      std::cout << "[GET] getting value of " << key << std::endl;
    }
  } catch (std::exception &e) {
    std::cout << "getValue exception" << std::endl;
    std::cout << e.what() << std::endl;
  }
  return grpc::Status::OK;
}

int CoordinatorImpl::get_cluster_id_by_group_id(Stripe &t_stripe,
                                                int group_id) {
  int block_id = t_stripe.group_to_blocks[group_id][0];
  return t_stripe.blocks[block_id]->map2cluster;
}

bool CoordinatorImpl::recovery_one_block_breakdown(
    int stripe_id, int failed_block_id, std::vector<double> &disk_io_start_time,
    std::vector<double> &disk_io_end_time,
    std::vector<double> &decode_start_time,
    std::vector<double> &decode_end_time,
    std::vector<double> &network_start_time,
    std::vector<double> &network_end_time, double &cross_rack_network_time,
    double &cross_rack_xor_time, std::vector<double> &grpc_notify_time,
    std::vector<double> &grpc_start_time,
    std::vector<double> &data_node_grpc_notify_time,
    std::vector<double> &data_node_grpc_start_time,
    double &dest_data_node_network_time, double &dest_data_node_disk_io_time) {
  std::string code_type = m_sys_config->CodeType;
  Stripe &t_stripe = m_stripe_table[stripe_id];
  std::vector<int> recovery_group_ids =
      get_recovery_group_ids(m_sys_config->CodeType, m_sys_config->k,
                             m_sys_config->r, m_sys_config->z, failed_block_id);
  grpc::Status status;

  if (recovery_group_ids.size() == 1) {
    // assert((code_type == "UniLRC") || (code_type == "AzureLRC" &&
    // (failed_block_id < m_sys_config->k || failed_block_id >= m_sys_config->k
    // + m_sys_config->r)));

    grpc::ClientContext recovery_context;
    proxy_proto::RecoveryRequest recovery_request;
    proxy_proto::RecoveryReply recovery_reply;

    int chosen_cluster_id =
        get_cluster_id_by_group_id(t_stripe, recovery_group_ids[0]);
    std::string chosen_proxy =
        m_cluster_table[chosen_cluster_id].proxy_ip + ":" +
        std::to_string(m_cluster_table[chosen_cluster_id].proxy_port);
    recovery_request.set_failed_block_id(failed_block_id);
    recovery_request.set_failed_block_key(
        t_stripe.blocks[failed_block_id]->block_key);
    int t_node_id = randomly_select_a_node(chosen_cluster_id, stripe_id);
    recovery_request.set_replaced_node_ip(m_node_table[t_node_id].node_ip);
    recovery_request.set_replaced_node_port(m_node_table[t_node_id].node_port);
    recovery_request.set_cross_rack_num(0);
    std::vector<int> blockids = t_stripe.group_to_blocks[recovery_group_ids[0]];
    for (int i = 0; i < int(blockids.size()); i++) {
      if (blockids[i] == failed_block_id)
        continue;

      Block *t_block = t_stripe.blocks[blockids[i]];
      recovery_request.add_datanodeip(m_node_table[t_block->map2node].node_ip);
      recovery_request.add_datanodeport(
          m_node_table[t_block->map2node].node_port);
      recovery_request.add_blockkeys(t_block->block_key);
      recovery_request.add_blockids(t_block->block_id);
    }
    std::chrono::high_resolution_clock::time_point start =
        std::chrono::high_resolution_clock::now();
    grpc_notify_time.push_back(
        std::chrono::duration_cast<std::chrono::duration<double>>(
            start.time_since_epoch())
            .count());
    status = m_proxy_ptrs[chosen_proxy]->recoveryBreakdown(
        &recovery_context, recovery_request, &recovery_reply);
    if (status.ok()) {
      disk_io_start_time.push_back(recovery_reply.disk_io_start_time());
      disk_io_end_time.push_back(recovery_reply.disk_io_end_time());
      decode_start_time.push_back(recovery_reply.decode_start_time());
      decode_end_time.push_back(recovery_reply.decode_end_time());
      network_start_time.push_back(recovery_reply.network_start_time());
      network_end_time.push_back(recovery_reply.network_end_time());
      cross_rack_network_time = recovery_reply.cross_rack_time();
      cross_rack_xor_time = recovery_reply.cross_rack_xor_time();
      data_node_grpc_notify_time.push_back(
          recovery_reply.data_node_grpc_notify_time());
      data_node_grpc_start_time.push_back(
          recovery_reply.data_node_grpc_start_time());
      dest_data_node_network_time =
          recovery_reply.dest_data_node_network_time();
      dest_data_node_disk_io_time =
          recovery_reply.dest_data_node_disk_io_time();
      grpc_start_time.push_back(recovery_reply.grpc_start_time());
      std::cout << "[Coordinator] recovery of " << stripe_id << "_"
                << failed_block_id << " success!" << std::endl;
      return true;
    } else {
      std::cout << "[Coordinator] recovery of " << stripe_id << "_"
                << failed_block_id << " failed!" << std::endl;
      return false;
    }
  } else {
    int dest_group_id = t_stripe.blocks[failed_block_id]->map2group;
    int dest_cluster_id = get_cluster_id_by_group_id(t_stripe, dest_group_id);
    std::string dest_proxy_ip = m_cluster_table[dest_cluster_id].proxy_ip;
    int dest_proxy_port = m_cluster_table[dest_cluster_id].proxy_port;
    std::vector<int> chosen_cluster_ids;
    for (int i = 0; i < recovery_group_ids.size(); i++) {
      chosen_cluster_ids.push_back(
          get_cluster_id_by_group_id(t_stripe, recovery_group_ids[i]));
    }
    std::vector<std::string> chosen_proxies;
    for (int i = 0; i < chosen_cluster_ids.size(); i++) {
      chosen_proxies.push_back(
          m_cluster_table[chosen_cluster_ids[i]].proxy_ip + ":" +
          std::to_string(m_cluster_table[chosen_cluster_ids[i]].proxy_port));
    }
    std::vector<std::thread> threads;
    for (int i = 0; i < recovery_group_ids.size(); i++) {
      if (recovery_group_ids[i] == dest_group_id) {
        continue;
      }
      threads.push_back(std::thread([&t_stripe, &chosen_proxies,
                                     &recovery_group_ids, i, failed_block_id,
                                     dest_proxy_ip, dest_proxy_port, this,
                                     &disk_io_start_time, &disk_io_end_time,
                                     &decode_start_time, &decode_end_time,
                                     &network_start_time, &network_end_time,
                                     &grpc_notify_time, &grpc_start_time,
                                     &data_node_grpc_notify_time,
                                     &data_node_grpc_start_time]() {
        grpc::ClientContext degraded_read_context;
        proxy_proto::DegradedReadRequest degraded_read_request;
        proxy_proto::DegradedReadReply degraded_read_reply;
        degraded_read_request.set_clientip(dest_proxy_ip);
        degraded_read_request.set_clientport(dest_proxy_port +
                                             ECProject::PROXY_PORT_SHIFT);
        degraded_read_request.set_failed_block_id(failed_block_id);
        degraded_read_request.set_failed_block_key(
            t_stripe.blocks[failed_block_id]->block_key);
        std::vector<int> blockids =
            t_stripe.group_to_blocks[recovery_group_ids[i]];
        for (int j = 0; j < int(blockids.size()); j++) {
          if (m_sys_config->CodeType == "AzureLRC" &&
              degraded_read_request.blockids_size() ==
                  (m_sys_config->k / m_sys_config->z))
            break;

          if ((m_sys_config->CodeType == "AzureLRC" &&
               blockids[j] >= m_sys_config->k + m_sys_config->r) ||
              blockids[j] == failed_block_id)
            continue;

          Block *t_block = t_stripe.blocks[blockids[j]];
          degraded_read_request.add_datanodeip(
              m_node_table[t_block->map2node].node_ip);
          degraded_read_request.add_datanodeport(
              m_node_table[t_block->map2node].node_port);
          degraded_read_request.add_blockkeys(t_block->block_key);
          degraded_read_request.add_blockids(t_block->block_id);
        }
        std::chrono::high_resolution_clock::time_point start =
            std::chrono::high_resolution_clock::now();

        grpc::Status status =
            m_proxy_ptrs[chosen_proxies[i]]->degradedReadBreakdown(
                &degraded_read_context, degraded_read_request,
                &degraded_read_reply);
        if (status.ok()) {
          std::lock_guard<std::mutex> lock(m_mutex);
          disk_io_start_time.push_back(
              degraded_read_reply.disk_io_start_time());
          disk_io_end_time.push_back(degraded_read_reply.disk_io_end_time());
          decode_start_time.push_back(degraded_read_reply.decode_start_time());
          decode_end_time.push_back(degraded_read_reply.decode_end_time());
          network_start_time.push_back(
              degraded_read_reply.network_start_time());
          network_end_time.push_back(degraded_read_reply.network_end_time());
          grpc_start_time.push_back(degraded_read_reply.grpc_start_time());
          data_node_grpc_notify_time.push_back(
              degraded_read_reply.data_node_grpc_notify_time());
          data_node_grpc_start_time.push_back(
              degraded_read_reply.data_node_grpc_start_time());
          grpc_notify_time.push_back(
              std::chrono::duration_cast<std::chrono::duration<double>>(
                  start.time_since_epoch())
                  .count());

          std::cout << "[Coordinator] partial degraded read of "
                    << failed_block_id << " success!" << std::endl;
        } else {
          std::cout << "[Coordinator] partial degraded read of "
                    << failed_block_id << " failed!" << std::endl;
        }
      }));
    }
    int cross_rack_num = recovery_group_ids.size() - 1;
    threads.push_back(std::thread(
        [this, &t_stripe, cross_rack_num, dest_group_id, dest_cluster_id,
         dest_proxy_ip, dest_proxy_port, stripe_id, failed_block_id,
         &disk_io_start_time, &disk_io_end_time, &decode_start_time,
         &decode_end_time, &network_start_time, &network_end_time,
         &grpc_notify_time, &grpc_start_time, &data_node_grpc_notify_time,
         &data_node_grpc_start_time, &cross_rack_network_time,
         &cross_rack_xor_time, &dest_data_node_network_time,
         &dest_data_node_disk_io_time]() {
          grpc::ClientContext recovery_context;
          proxy_proto::RecoveryRequest recovery_request;
          proxy_proto::RecoveryReply recovery_reply;
          recovery_request.set_failed_block_id(failed_block_id);
          recovery_request.set_failed_block_key(
              t_stripe.blocks[failed_block_id]->block_key);
          int t_node_id = randomly_select_a_node(dest_cluster_id, stripe_id);
          recovery_request.set_replaced_node_ip(
              m_node_table[t_node_id].node_ip);
          recovery_request.set_replaced_node_port(
              m_node_table[t_node_id].node_port);
          recovery_request.set_cross_rack_num(cross_rack_num);
          std::vector<int> blockids = t_stripe.group_to_blocks[dest_group_id];
          for (int i = 0; i < int(blockids.size()); i++) {
            if (m_sys_config->CodeType == "AzureLRC" &&
                recovery_request.blockids_size() ==
                    (m_sys_config->k / m_sys_config->z))
              break;

            if (blockids[i] == failed_block_id)
              continue;

            Block *t_block = t_stripe.blocks[blockids[i]];
            recovery_request.add_datanodeip(
                m_node_table[t_block->map2node].node_ip);
            recovery_request.add_datanodeport(
                m_node_table[t_block->map2node].node_port);
            recovery_request.add_blockkeys(t_block->block_key);
            recovery_request.add_blockids(t_block->block_id);
          }
          // std::cout << "[Coordinator] start recovery of " << stripe_id << "_"
          // << failed_block_id << std::endl;
          std::chrono::high_resolution_clock::time_point start =
              std::chrono::high_resolution_clock::now();

          grpc::Status status =
              m_proxy_ptrs[dest_proxy_ip + ":" +
                           std::to_string(dest_proxy_port)]
                  ->recoveryBreakdown(&recovery_context, recovery_request,
                                      &recovery_reply);
          if (status.ok()) {
            std::lock_guard<std::mutex> lock(m_mutex);
            grpc_notify_time.push_back(
                std::chrono::duration_cast<std::chrono::duration<double>>(
                    start.time_since_epoch())
                    .count());
            disk_io_start_time.push_back(recovery_reply.disk_io_start_time());
            disk_io_end_time.push_back(recovery_reply.disk_io_end_time());
            decode_start_time.push_back(recovery_reply.decode_start_time());
            decode_end_time.push_back(recovery_reply.decode_end_time());
            network_start_time.push_back(recovery_reply.network_start_time());
            network_end_time.push_back(recovery_reply.network_end_time());
            grpc_start_time.push_back(recovery_reply.grpc_start_time());
            cross_rack_network_time = recovery_reply.cross_rack_time();
            cross_rack_xor_time = recovery_reply.cross_rack_xor_time();
            data_node_grpc_notify_time.push_back(
                recovery_reply.data_node_grpc_notify_time());
            data_node_grpc_start_time.push_back(
                recovery_reply.data_node_grpc_start_time());
            dest_data_node_network_time =
                recovery_reply.dest_data_node_network_time();
            dest_data_node_disk_io_time =
                recovery_reply.dest_data_node_disk_io_time();
            std::cout << "[Coordinator] recovery of " << stripe_id << "_"
                      << failed_block_id << " success!" << std::endl;
          } else {
            std::cout << "[Coordinator] recovery of " << stripe_id << "_"
                      << failed_block_id << " failed!" << std::endl;
          }
        }));
    for (int i = 0; i < threads.size(); i++) {
      threads[i].join();
    }
  }
  return true;
}

grpc::Status CoordinatorImpl::decodeTest(
    grpc::ServerContext *context,
    const coordinator_proto::KeyAndClientIP *keyClient,
    coordinator_proto::DegradedReadReply *degradedReadReply) {
  std::string code_type = m_sys_config->CodeType;
  int k = m_sys_config->k;
  int r = m_sys_config->r;
  int z = m_sys_config->z;
  int block_size = m_sys_config->BlockSize;
  int stripe_id =
      std::stoi(keyClient->key().substr(0, keyClient->key().find('_')));
  int failed_block_id =
      std::stoi(keyClient->key().substr(keyClient->key().find('_') + 1));
  Stripe &t_stripe = m_stripe_table[stripe_id];

  std::vector<int> recovery_group_ids =
      get_recovery_group_ids(code_type, k, r, z, failed_block_id);
  std::vector<int> recovery_block_ids;
  for (int i = 0; i < recovery_group_ids.size(); i++) {
    std::vector<int> blockids = t_stripe.group_to_blocks[recovery_group_ids[i]];
    for (int j = 0; j < blockids.size(); j++) {
      if (m_sys_config->CodeType == "AzureLRC" &&
          recovery_block_ids.size() == (k / z))
        break;
      if ((m_sys_config->CodeType == "AzureLRC" &&
           blockids[j] >= m_sys_config->k + m_sys_config->r) ||
          blockids[j] == failed_block_id)
        continue;
      if (blockids[j] != failed_block_id) {
        recovery_block_ids.push_back(blockids[j]);
      }
    }
  }
  int block_num = recovery_block_ids.size();
  unsigned char *recovery_data = static_cast<unsigned char *>(
      std::aligned_alloc(32, m_sys_config->BlockSize * block_num));
  std::vector<unsigned char *> recovery_data_ptrs;
  for (int i = 0; i < block_num; i++) {
    recovery_data_ptrs.push_back(recovery_data + i * block_size);
  }

  unsigned char *res = static_cast<unsigned char *>(
      std::aligned_alloc(32, m_sys_config->BlockSize));
  std::chrono::high_resolution_clock::time_point start =
      std::chrono::high_resolution_clock::now();
  if (code_type == "AzureLRC") {
    decode_azure_lrc(k, r, z, block_num, &recovery_block_ids,
                     recovery_data_ptrs.data(), res, block_size,
                     failed_block_id);
  } else if (code_type == "UniLRC") {
    decode_unilrc(k, r, z, block_num, &recovery_block_ids,
                  recovery_data_ptrs.data(), res, block_size);
  } else if (code_type == "OptimalLRC") {
    decode_optimal_lrc(k, r, z, block_num, &recovery_block_ids,
                       recovery_data_ptrs.data(), res, block_size,
                       failed_block_id);
  } else if (code_type == "UniformLRC") {
    decode_uniform_lrc(k, r, z, block_num, &recovery_block_ids,
                       recovery_data_ptrs.data(), res, block_size,
                       failed_block_id);
  } else {
    std::cout << "[Coordinator] decodeTest: unknown code type!" << std::endl;
    return grpc::Status(grpc::INVALID_ARGUMENT, "unknown code type");
  }
  std::chrono::high_resolution_clock::time_point end =
      std::chrono::high_resolution_clock::now();
  std::chrono::duration<double> duration =
      std::chrono::duration_cast<std::chrono::duration<double>>(end - start);
  std::cout << "[Coordinator] decodeTest took " << duration.count()
            << " seconds" << std::endl;
  degradedReadReply->set_decode_time(duration.count());
  delete[] res;
  delete[] recovery_data;

  return grpc::Status::OK;
}

bool CoordinatorImpl::recovery_one_block(int stripe_id, int failed_block_id) {
  std::string code_type = m_sys_config->CodeType;
  Stripe &t_stripe = m_stripe_table[stripe_id];
  std::vector<int> recovery_group_ids =
      get_recovery_group_ids(m_sys_config->CodeType, m_sys_config->k,
                             m_sys_config->r, m_sys_config->z, failed_block_id);
  grpc::Status status;

  if (recovery_group_ids.size() == 1) {
    // assert((code_type == "UniLRC") || (code_type == "AzureLRC" &&
    // (failed_block_id < m_sys_config->k || failed_block_id >= m_sys_config->k
    // + m_sys_config->r)));

    grpc::ClientContext recovery_context;
    proxy_proto::RecoveryRequest recovery_request;
    proxy_proto::RecoveryReply recovery_reply;

    int chosen_cluster_id =
        get_cluster_id_by_group_id(t_stripe, recovery_group_ids[0]);
    std::string chosen_proxy =
        m_cluster_table[chosen_cluster_id].proxy_ip + ":" +
        std::to_string(m_cluster_table[chosen_cluster_id].proxy_port);
    recovery_request.set_failed_block_id(failed_block_id);
    recovery_request.set_failed_block_key(
        t_stripe.blocks[failed_block_id]->block_key);
    int t_node_id = randomly_select_a_node(chosen_cluster_id, stripe_id);
    recovery_request.set_replaced_node_ip(m_node_table[t_node_id].node_ip);
    recovery_request.set_replaced_node_port(m_node_table[t_node_id].node_port);
    recovery_request.set_cross_rack_num(0);
    std::vector<int> blockids = t_stripe.group_to_blocks[recovery_group_ids[0]];
    for (int i = 0; i < int(blockids.size()); i++) {
      if (blockids[i] == failed_block_id)
        continue;

      Block *t_block = t_stripe.blocks[blockids[i]];
      recovery_request.add_datanodeip(m_node_table[t_block->map2node].node_ip);
      recovery_request.add_datanodeport(
          m_node_table[t_block->map2node].node_port);
      recovery_request.add_blockkeys(t_block->block_key);
      recovery_request.add_blockids(t_block->block_id);
    }

    status = m_proxy_ptrs[chosen_proxy]->recovery(
        &recovery_context, recovery_request, &recovery_reply);
    if (status.ok()) {
      std::cout << "[Coordinator] recovery of " << stripe_id << "_"
                << failed_block_id << " success!" << std::endl;
      return true;
    } else {
      std::cout << "[Coordinator] recovery of " << stripe_id << "_"
                << failed_block_id << " failed!" << std::endl;
      return false;
    }
  } else {
    int dest_group_id = t_stripe.blocks[failed_block_id]->map2group;
    int dest_cluster_id = get_cluster_id_by_group_id(t_stripe, dest_group_id);
    std::string dest_proxy_ip = m_cluster_table[dest_cluster_id].proxy_ip;
    int dest_proxy_port = m_cluster_table[dest_cluster_id].proxy_port;
    std::vector<int> chosen_cluster_ids;
    for (int i = 0; i < recovery_group_ids.size(); i++) {
      chosen_cluster_ids.push_back(
          get_cluster_id_by_group_id(t_stripe, recovery_group_ids[i]));
    }
    std::vector<std::string> chosen_proxies;
    for (int i = 0; i < chosen_cluster_ids.size(); i++) {
      chosen_proxies.push_back(
          m_cluster_table[chosen_cluster_ids[i]].proxy_ip + ":" +
          std::to_string(m_cluster_table[chosen_cluster_ids[i]].proxy_port));
    }
    std::vector<std::thread> threads;
    for (int i = 0; i < recovery_group_ids.size(); i++) {
      if (recovery_group_ids[i] == dest_group_id) {
        continue;
      }
      threads.push_back(std::thread([&t_stripe, &chosen_proxies,
                                     &recovery_group_ids, i, failed_block_id,
                                     dest_proxy_ip, dest_proxy_port, this]() {
        grpc::ClientContext degraded_read_context;
        proxy_proto::DegradedReadRequest degraded_read_request;
        proxy_proto::DegradedReadReply degraded_read_reply;
        degraded_read_request.set_clientip(dest_proxy_ip);
        degraded_read_request.set_clientport(dest_proxy_port +
                                             ECProject::PROXY_PORT_SHIFT);
        degraded_read_request.set_failed_block_id(failed_block_id);
        degraded_read_request.set_failed_block_key(
            t_stripe.blocks[failed_block_id]->block_key);
        std::vector<int> blockids =
            t_stripe.group_to_blocks[recovery_group_ids[i]];
        for (int j = 0; j < int(blockids.size()); j++) {
          if (m_sys_config->CodeType == "AzureLRC" &&
              degraded_read_request.blockids_size() ==
                  (m_sys_config->k / m_sys_config->z))
            break;

          if ((m_sys_config->CodeType == "AzureLRC" &&
               blockids[j] >= m_sys_config->k + m_sys_config->r) ||
              blockids[j] == failed_block_id)
            continue;

          Block *t_block = t_stripe.blocks[blockids[j]];
          degraded_read_request.add_datanodeip(
              m_node_table[t_block->map2node].node_ip);
          degraded_read_request.add_datanodeport(
              m_node_table[t_block->map2node].node_port);
          degraded_read_request.add_blockkeys(t_block->block_key);
          degraded_read_request.add_blockids(t_block->block_id);
        }
        grpc::Status status = m_proxy_ptrs[chosen_proxies[i]]->degradedRead(
            &degraded_read_context, degraded_read_request,
            &degraded_read_reply);
        if (status.ok()) {
          std::cout << "[Coordinator] partial degraded read of "
                    << failed_block_id << " success!" << std::endl;
        } else {
          std::cout << "[Coordinator] partial degraded read of "
                    << failed_block_id << " failed!" << std::endl;
        }
      }));
    }
    int cross_rack_num = recovery_group_ids.size() - 1;
    threads.push_back(std::thread([this, &t_stripe, cross_rack_num,
                                   dest_group_id, dest_cluster_id,
                                   dest_proxy_ip, dest_proxy_port, stripe_id,
                                   failed_block_id, &recovery_group_ids]() {
      grpc::ClientContext recovery_context;
      proxy_proto::RecoveryRequest recovery_request;
      proxy_proto::RecoveryReply recovery_reply;
      recovery_request.set_failed_block_id(failed_block_id);
      recovery_request.set_failed_block_key(
          t_stripe.blocks[failed_block_id]->block_key);
      int t_node_id = randomly_select_a_node(dest_cluster_id, stripe_id);
      recovery_request.set_replaced_node_ip(m_node_table[t_node_id].node_ip);
      recovery_request.set_replaced_node_port(
          m_node_table[t_node_id].node_port);
      recovery_request.set_cross_rack_num(cross_rack_num);
      for (int i = 0; i < recovery_group_ids.size(); i++) {
        if (recovery_group_ids[i] == dest_group_id) {
          continue;
        }
        int cluster_id =
            get_cluster_id_by_group_id(t_stripe, recovery_group_ids[i]);
        std::string proxy_ip = m_cluster_table[cluster_id].proxy_ip;
        int proxy_port = m_cluster_table[cluster_id].proxy_port;
        recovery_request.add_proxyip(proxy_ip);
        recovery_request.add_proxyport(proxy_port);
      }
      std::vector<int> blockids = t_stripe.group_to_blocks[dest_group_id];
      for (int i = 0; i < int(blockids.size()); i++) {
        if (m_sys_config->CodeType == "AzureLRC" &&
            recovery_request.blockids_size() ==
                (m_sys_config->k / m_sys_config->z))
          break;

        if (blockids[i] == failed_block_id)
          continue;

        Block *t_block = t_stripe.blocks[blockids[i]];
        recovery_request.add_datanodeip(
            m_node_table[t_block->map2node].node_ip);
        recovery_request.add_datanodeport(
            m_node_table[t_block->map2node].node_port);
        recovery_request.add_blockkeys(t_block->block_key);
        recovery_request.add_blockids(t_block->block_id);
      }
      // std::cout << "[Coordinator] start recovery of " << stripe_id << "_" <<
      // failed_block_id << std::endl;
      grpc::Status status =
          m_proxy_ptrs[dest_proxy_ip + ":" + std::to_string(dest_proxy_port)]
              ->recovery(&recovery_context, recovery_request, &recovery_reply);
      if (status.ok()) {
        std::cout << "[Coordinator] recovery of " << stripe_id << "_"
                  << failed_block_id << " success!" << std::endl;
      } else {
        std::cout << "[Coordinator] recovery of " << stripe_id << "_"
                  << failed_block_id << " failed!" << std::endl;
      }
    }));
    for (int i = 0; i < threads.size(); i++) {
      threads[i].join();
    }
  }
  return true;
}

grpc::Status CoordinatorImpl::getRecoveryBreakdown(
    grpc::ServerContext *context,
    const coordinator_proto::KeyAndClientIP *keyClient,
    coordinator_proto::RecoveryReply *recoveryReply) {
  std::chrono::time_point<std::chrono::high_resolution_clock> START =
      std::chrono::high_resolution_clock::now();
  recoveryReply->set_grpc_start_time(
      std::chrono::duration_cast<std::chrono::duration<double>>(
          START.time_since_epoch())
          .count());
  int stripe_id =
      std::stoi(keyClient->key().substr(0, keyClient->key().find('_')));
  int failed_block_id =
      std::stoi(keyClient->key().substr(keyClient->key().find('_') + 1));
  std::vector<double> disk_io_start_time, disk_io_end_time;
  std::vector<double> decode_start_time, decode_end_time;
  std::vector<double> network_start_time, network_end_time;
  double cross_rack_network_time, cross_rack_xor_time;
  std::vector<double> grpc_notify_time, grpc_start_time;
  std::vector<double> data_node_grpc_notify_time, data_node_grpc_start_time;
  double dest_data_node_network_time, dest_data_node_disk_io_time;

  bool if_success = recovery_one_block_breakdown(
      stripe_id, failed_block_id, disk_io_start_time, disk_io_end_time,
      decode_start_time, decode_end_time, network_start_time, network_end_time,
      cross_rack_network_time, cross_rack_xor_time, grpc_notify_time,
      grpc_start_time, data_node_grpc_notify_time, data_node_grpc_start_time,
      dest_data_node_network_time, dest_data_node_disk_io_time);

  if (if_success) {
    double max_disk_io_time =
        *std::max_element(disk_io_end_time.begin(), disk_io_end_time.end()) -
        *std::min_element(disk_io_start_time.begin(), disk_io_start_time.end());
    recoveryReply->set_disk_read_time(max_disk_io_time);
    double max_decode_time =
        *std::max_element(decode_end_time.begin(), decode_end_time.end()) -
        *std::min_element(decode_start_time.begin(), decode_start_time.end());
    recoveryReply->set_decode_time(max_decode_time + cross_rack_xor_time);
    double max_network_time =
        *std::max_element(network_end_time.begin(), network_end_time.end()) -
        *std::min_element(network_start_time.begin(), network_start_time.end());
    double max_grpc_delay =
        *std::max_element(grpc_start_time.begin(), grpc_start_time.end()) -
        *std::min_element(grpc_notify_time.begin(), grpc_notify_time.end());
    double max_data_node_grpc_delay =
        *std::max_element(data_node_grpc_start_time.begin(),
                          data_node_grpc_start_time.end()) -
        *std::min_element(data_node_grpc_notify_time.begin(),
                          data_node_grpc_notify_time.end());
    recoveryReply->set_network_time(max_network_time + cross_rack_network_time +
                                    dest_data_node_network_time +
                                    max_grpc_delay + max_data_node_grpc_delay);
    recoveryReply->set_disk_write_time(dest_data_node_disk_io_time);

    return grpc::Status::OK;
  } else {
    return grpc::Status(grpc::StatusCode::INTERNAL, "Recovery failed!");
  }
}

grpc::Status
CoordinatorImpl::getRecovery(grpc::ServerContext *context,
                             const coordinator_proto::KeyAndClientIP *keyClient,
                             coordinator_proto::RecoveryReply *recoveryReply) {
  int stripe_id =
      std::stoi(keyClient->key().substr(0, keyClient->key().find('_')));
  int failed_block_id =
      std::stoi(keyClient->key().substr(keyClient->key().find('_') + 1));
  bool if_success = recovery_one_block(stripe_id, failed_block_id);

  if (if_success) {
    return grpc::Status::OK;
  } else {
    return grpc::Status(grpc::StatusCode::INTERNAL, "Recovery failed!");
  }
}

bool CoordinatorImpl::degraded_read_one_block_breakdown(
    int stripe_id, int failed_block_id, std::string client_ip, int client_port,
    std::vector<double> &disk_io_start_time,
    std::vector<double> &disk_io_end_time,
    std::vector<double> &decode_start_time,
    std::vector<double> &decode_end_time,
    std::vector<double> &network_start_time,
    std::vector<double> &network_end_time, double &cross_rack_network_time,
    double &cross_rack_xor_time, std::vector<double> &grpc_notify_time,
    std::vector<double> &grpc_start_time,
    std::vector<double> &data_node_grpc_notify_time,
    std::vector<double> &data_node_grpc_start_time) {
  std::string code_type = m_sys_config->CodeType;
  Stripe &t_stripe = m_stripe_table[stripe_id];
  std::vector<int> recovery_group_ids =
      get_recovery_group_ids(m_sys_config->CodeType, m_sys_config->k,
                             m_sys_config->r, m_sys_config->z, failed_block_id);
  grpc::Status status;

  if (recovery_group_ids.size() == 1) {
    // assert((code_type == "UniLRC") || (code_type == "AzureLRC" &&
    // (failed_block_id < m_sys_config->k || failed_block_id >= m_sys_config->k
    // + m_sys_config->r)));

    grpc::ClientContext recovery_context;
    proxy_proto::RecoveryRequest recovery_request;
    proxy_proto::DegradedReadReply degraded_read_reply;

    int chosen_cluster_id =
        get_cluster_id_by_group_id(t_stripe, recovery_group_ids[0]);
    std::string chosen_proxy =
        m_cluster_table[chosen_cluster_id].proxy_ip + ":" +
        std::to_string(m_cluster_table[chosen_cluster_id].proxy_port);
    recovery_request.set_failed_block_id(failed_block_id);
    recovery_request.set_failed_block_key(
        t_stripe.blocks[failed_block_id]->block_key);
    recovery_request.set_replaced_node_ip(client_ip);
    recovery_request.set_replaced_node_port(client_port);
    recovery_request.set_cross_rack_num(0);
    std::vector<int> blockids = t_stripe.group_to_blocks[recovery_group_ids[0]];
    for (int i = 0; i < int(blockids.size()); i++) {
      if (blockids[i] == failed_block_id)
        continue;

      Block *t_block = t_stripe.blocks[blockids[i]];
      recovery_request.add_datanodeip(m_node_table[t_block->map2node].node_ip);
      recovery_request.add_datanodeport(
          m_node_table[t_block->map2node].node_port);
      recovery_request.add_blockkeys(t_block->block_key);
      recovery_request.add_blockids(t_block->block_id);
    }

    std::chrono::high_resolution_clock::time_point grpc_notify =
        std::chrono::high_resolution_clock::now();
    grpc_notify_time.push_back(
        std::chrono::duration_cast<std::chrono::duration<double>>(
            grpc_notify.time_since_epoch())
            .count());
    status = m_proxy_ptrs[chosen_proxy]->degradedRead2ClientBreakdown(
        &recovery_context, recovery_request, &degraded_read_reply);
    if (status.ok()) {
      disk_io_start_time.push_back(degraded_read_reply.disk_io_start_time());
      disk_io_end_time.push_back(degraded_read_reply.disk_io_end_time());
      decode_start_time.push_back(degraded_read_reply.decode_start_time());
      decode_end_time.push_back(degraded_read_reply.decode_end_time());
      network_start_time.push_back(degraded_read_reply.network_start_time());
      network_end_time.push_back(degraded_read_reply.network_end_time());
      cross_rack_network_time = degraded_read_reply.cross_rack_time();
      cross_rack_xor_time = degraded_read_reply.cross_rack_xor_time();
      grpc_start_time.push_back(degraded_read_reply.grpc_start_time());
      data_node_grpc_notify_time.push_back(
          degraded_read_reply.data_node_grpc_notify_time());
      data_node_grpc_start_time.push_back(
          degraded_read_reply.data_node_grpc_start_time());
      std::cout << "[Coordinator] degraded read of " << stripe_id << "_"
                << failed_block_id << " success!" << std::endl;
      return true;
    } else {
      std::cout << "[Coordinator] degraded read of " << stripe_id << "_"
                << failed_block_id << " failed!" << std::endl;
      return false;
    }
  } else {
    int dest_group_id = t_stripe.blocks[failed_block_id]->map2group;
    int dest_cluster_id = get_cluster_id_by_group_id(t_stripe, dest_group_id);
    std::string dest_proxy_ip = m_cluster_table[dest_cluster_id].proxy_ip;
    int dest_proxy_port = m_cluster_table[dest_cluster_id].proxy_port;
    std::vector<int> chosen_cluster_ids;
    for (int i = 0; i < recovery_group_ids.size(); i++) {
      chosen_cluster_ids.push_back(
          get_cluster_id_by_group_id(t_stripe, recovery_group_ids[i]));
    }
    std::vector<std::string> chosen_proxies;
    for (int i = 0; i < chosen_cluster_ids.size(); i++) {
      chosen_proxies.push_back(
          m_cluster_table[chosen_cluster_ids[i]].proxy_ip + ":" +
          std::to_string(m_cluster_table[chosen_cluster_ids[i]].proxy_port));
    }
    std::vector<std::thread> threads;
    for (int i = 0; i < recovery_group_ids.size(); i++) {
      if (recovery_group_ids[i] == dest_group_id) {
        continue;
      }
      threads.push_back(std::thread([&t_stripe, &chosen_proxies,
                                     &recovery_group_ids, i, failed_block_id,
                                     dest_proxy_ip, dest_proxy_port, this,
                                     &disk_io_start_time, &disk_io_end_time,
                                     &decode_start_time, &decode_end_time,
                                     &network_start_time, &network_end_time,
                                     &grpc_notify_time, &grpc_start_time,
                                     &data_node_grpc_notify_time,
                                     &data_node_grpc_start_time]() {
        grpc::ClientContext degraded_read_context;
        proxy_proto::DegradedReadRequest degraded_read_request;
        proxy_proto::DegradedReadReply degraded_read_reply;
        degraded_read_request.set_clientip(dest_proxy_ip);
        degraded_read_request.set_clientport(dest_proxy_port +
                                             ECProject::PROXY_PORT_SHIFT);
        degraded_read_request.set_failed_block_id(failed_block_id);
        degraded_read_request.set_failed_block_key(
            t_stripe.blocks[failed_block_id]->block_key);
        std::vector<int> blockids =
            t_stripe.group_to_blocks[recovery_group_ids[i]];
        for (int j = 0; j < int(blockids.size()); j++) {
          if (m_sys_config->CodeType == "AzureLRC" &&
              degraded_read_request.blockids_size() ==
                  (m_sys_config->k / m_sys_config->z))
            break;

          if ((m_sys_config->CodeType == "AzureLRC" &&
               blockids[j] >= m_sys_config->k + m_sys_config->r) ||
              blockids[j] == failed_block_id)
            continue;

          Block *t_block = t_stripe.blocks[blockids[j]];
          degraded_read_request.add_datanodeip(
              this->m_node_table[t_block->map2node].node_ip);
          degraded_read_request.add_datanodeport(
              this->m_node_table[t_block->map2node].node_port);
          degraded_read_request.add_blockkeys(t_block->block_key);
          degraded_read_request.add_blockids(t_block->block_id);
        }
        std::cout << "[Coordinator] start partial degraded read of "
                  << failed_block_id << std::endl;
        std::chrono::high_resolution_clock::time_point grpc_notify =
            std::chrono::high_resolution_clock::now();
        grpc_notify_time.push_back(
            std::chrono::duration_cast<std::chrono::duration<double>>(
                grpc_notify.time_since_epoch())
                .count());
        grpc::Status status =
            this->m_proxy_ptrs[chosen_proxies[i]]->degradedReadBreakdown(
                &degraded_read_context, degraded_read_request,
                &degraded_read_reply);
        if (status.ok()) {
          disk_io_start_time.push_back(
              degraded_read_reply.disk_io_start_time());
          disk_io_end_time.push_back(degraded_read_reply.disk_io_end_time());
          decode_start_time.push_back(degraded_read_reply.decode_start_time());
          decode_end_time.push_back(degraded_read_reply.decode_end_time());
          network_start_time.push_back(
              degraded_read_reply.network_start_time());
          network_end_time.push_back(degraded_read_reply.network_end_time());
          grpc_start_time.push_back(degraded_read_reply.grpc_start_time());
          data_node_grpc_notify_time.push_back(
              degraded_read_reply.data_node_grpc_notify_time());
          data_node_grpc_start_time.push_back(
              degraded_read_reply.data_node_grpc_start_time());
          std::cout << "[Coordinator] partial degraded read of "
                    << failed_block_id << " success!" << std::endl;
        } else {
          std::cout << "[Coordinator] partial degraded read of "
                    << failed_block_id << " failed!" << std::endl;
        }
      }));
    }
    int cross_rack_num = recovery_group_ids.size() - 1;
    threads.push_back(std::thread(
        [this, &t_stripe, cross_rack_num, dest_group_id, dest_cluster_id,
         dest_proxy_ip, dest_proxy_port, stripe_id, failed_block_id, client_ip,
         client_port, &disk_io_start_time, &disk_io_end_time,
         &decode_start_time, &decode_end_time, &network_start_time,
         &network_end_time, &cross_rack_network_time, &cross_rack_xor_time,
         &grpc_notify_time, &grpc_start_time, &data_node_grpc_notify_time,
         &data_node_grpc_start_time]() {
          grpc::ClientContext recovery_context;
          proxy_proto::RecoveryRequest recovery_request;
          proxy_proto::DegradedReadReply recovery_reply;
          recovery_request.set_failed_block_id(failed_block_id);
          recovery_request.set_failed_block_key(
              t_stripe.blocks[failed_block_id]->block_key);
          recovery_request.set_replaced_node_ip(client_ip);
          recovery_request.set_replaced_node_port(client_port);
          recovery_request.set_cross_rack_num(cross_rack_num);
          std::vector<int> blockids = t_stripe.group_to_blocks[dest_group_id];
          for (int i = 0; i < int(blockids.size()); i++) {
            if (m_sys_config->CodeType == "AzureLRC" &&
                recovery_request.blockids_size() ==
                    (m_sys_config->k / m_sys_config->z))
              break;

            if (blockids[i] == failed_block_id)
              continue;

            Block *t_block = t_stripe.blocks[blockids[i]];
            recovery_request.add_datanodeip(
                this->m_node_table[t_block->map2node].node_ip);
            recovery_request.add_datanodeport(
                this->m_node_table[t_block->map2node].node_port);
            recovery_request.add_blockkeys(t_block->block_key);
            recovery_request.add_blockids(t_block->block_id);
          }
          std::cout << "[Coordinator] start recovery of " << stripe_id << "_"
                    << failed_block_id << std::endl;
          std::chrono::high_resolution_clock::time_point grpc_notify =
              std::chrono::high_resolution_clock::now();
          grpc_notify_time.push_back(
              std::chrono::duration_cast<std::chrono::duration<double>>(
                  grpc_notify.time_since_epoch())
                  .count());
          grpc::Status status =
              this->m_proxy_ptrs[dest_proxy_ip + ":" +
                                 std::to_string(dest_proxy_port)]
                  ->degradedRead2ClientBreakdown(
                      &recovery_context, recovery_request, &recovery_reply);
          if (status.ok()) {
            disk_io_start_time.push_back(recovery_reply.disk_io_start_time());
            disk_io_end_time.push_back(recovery_reply.disk_io_end_time());
            decode_start_time.push_back(recovery_reply.decode_start_time());
            decode_end_time.push_back(recovery_reply.decode_end_time());
            network_start_time.push_back(recovery_reply.network_start_time());
            network_end_time.push_back(recovery_reply.network_end_time());
            cross_rack_network_time = recovery_reply.cross_rack_time();
            cross_rack_xor_time = recovery_reply.cross_rack_xor_time();
            grpc_start_time.push_back(recovery_reply.grpc_start_time());
            data_node_grpc_notify_time.push_back(
                recovery_reply.data_node_grpc_notify_time());
            data_node_grpc_start_time.push_back(
                recovery_reply.data_node_grpc_start_time());
            std::cout << "[Coordinator] degraded read of " << stripe_id << "_"
                      << failed_block_id << " success!" << std::endl;
          } else {
            std::cout << "[Coordinator] degraded read of " << stripe_id << "_"
                      << failed_block_id << " failed!" << std::endl;
          }
        }));
    for (int i = 0; i < threads.size(); i++) {
      threads[i].join();
    }
  }
  return true;
}

bool CoordinatorImpl::degraded_read_one_block(int stripe_id,
                                              int failed_block_id,
                                              std::string client_ip,
                                              int client_port) {
  std::string code_type = m_sys_config->CodeType;
  Stripe &t_stripe = m_stripe_table[stripe_id];
  std::vector<int> recovery_group_ids =
      get_recovery_group_ids(m_sys_config->CodeType, m_sys_config->k,
                             m_sys_config->r, m_sys_config->z, failed_block_id);
  grpc::Status status;

  if (recovery_group_ids.size() == 1) {
    // assert((code_type == "UniLRC") || (code_type == "AzureLRC" &&
    // (failed_block_id < m_sys_config->k || failed_block_id >= m_sys_config->k
    // + m_sys_config->r)));

    grpc::ClientContext recovery_context;
    proxy_proto::RecoveryRequest recovery_request;
    proxy_proto::DegradedReadReply degraded_read_reply;

    int chosen_cluster_id =
        get_cluster_id_by_group_id(t_stripe, recovery_group_ids[0]);
    std::string chosen_proxy =
        m_cluster_table[chosen_cluster_id].proxy_ip + ":" +
        std::to_string(m_cluster_table[chosen_cluster_id].proxy_port);
    recovery_request.set_failed_block_id(failed_block_id);
    recovery_request.set_failed_block_key(
        t_stripe.blocks[failed_block_id]->block_key);
    recovery_request.set_replaced_node_ip(client_ip);
    recovery_request.set_replaced_node_port(client_port);
    recovery_request.set_cross_rack_num(0);
    std::vector<int> blockids = t_stripe.group_to_blocks[recovery_group_ids[0]];
    for (int i = 0; i < int(blockids.size()); i++) {
      if (blockids[i] == failed_block_id)
        continue;

      Block *t_block = t_stripe.blocks[blockids[i]];
      recovery_request.add_datanodeip(m_node_table[t_block->map2node].node_ip);
      recovery_request.add_datanodeport(
          m_node_table[t_block->map2node].node_port);
      recovery_request.add_blockkeys(t_block->block_key);
      recovery_request.add_blockids(t_block->block_id);
    }
    status = m_proxy_ptrs[chosen_proxy]->degradedRead2Client(
        &recovery_context, recovery_request, &degraded_read_reply);
    if (status.ok()) {
      std::cout << "[Coordinator] degraded read of " << stripe_id << "_"
                << failed_block_id << " success!" << std::endl;
      return true;
    } else {
      std::cout << "[Coordinator] degraded read of " << stripe_id << "_"
                << failed_block_id << " failed!" << std::endl;
      return false;
    }
  } else {
    int dest_group_id = t_stripe.blocks[failed_block_id]->map2group;
    int dest_cluster_id = get_cluster_id_by_group_id(t_stripe, dest_group_id);
    std::string dest_proxy_ip = m_cluster_table[dest_cluster_id].proxy_ip;
    int dest_proxy_port = m_cluster_table[dest_cluster_id].proxy_port;
    std::vector<int> chosen_cluster_ids;
    for (int i = 0; i < recovery_group_ids.size(); i++) {
      chosen_cluster_ids.push_back(
          get_cluster_id_by_group_id(t_stripe, recovery_group_ids[i]));
    }
    std::vector<std::string> chosen_proxies;
    for (int i = 0; i < chosen_cluster_ids.size(); i++) {
      chosen_proxies.push_back(
          m_cluster_table[chosen_cluster_ids[i]].proxy_ip + ":" +
          std::to_string(m_cluster_table[chosen_cluster_ids[i]].proxy_port));
    }
    std::vector<std::thread> threads;
    for (int i = 0; i < recovery_group_ids.size(); i++) {
      if (recovery_group_ids[i] == dest_group_id) {
        continue;
      }
      threads.push_back(std::thread([&t_stripe, &chosen_proxies,
                                     &recovery_group_ids, i, failed_block_id,
                                     dest_proxy_ip, dest_proxy_port, this]() {
        grpc::ClientContext degraded_read_context;
        proxy_proto::DegradedReadRequest degraded_read_request;
        proxy_proto::DegradedReadReply degraded_read_reply;
        degraded_read_request.set_clientip(dest_proxy_ip);
        degraded_read_request.set_clientport(dest_proxy_port +
                                             ECProject::PROXY_PORT_SHIFT);
        degraded_read_request.set_failed_block_id(failed_block_id);
        degraded_read_request.set_failed_block_key(
            t_stripe.blocks[failed_block_id]->block_key);
        std::vector<int> blockids =
            t_stripe.group_to_blocks[recovery_group_ids[i]];
        for (int j = 0; j < int(blockids.size()); j++) {
          if (m_sys_config->CodeType == "AzureLRC" &&
              degraded_read_request.blockids_size() ==
                  (m_sys_config->k / m_sys_config->z))
            break;

          if ((m_sys_config->CodeType == "AzureLRC" &&
               blockids[j] >= m_sys_config->k + m_sys_config->r) ||
              blockids[j] == failed_block_id)
            continue;

          Block *t_block = t_stripe.blocks[blockids[j]];
          degraded_read_request.add_datanodeip(
              this->m_node_table[t_block->map2node].node_ip);
          degraded_read_request.add_datanodeport(
              this->m_node_table[t_block->map2node].node_port);
          degraded_read_request.add_blockkeys(t_block->block_key);
          degraded_read_request.add_blockids(t_block->block_id);
        }
        std::cout << "[Coordinator] start partial degraded read of "
                  << failed_block_id << std::endl;
        grpc::Status status =
            this->m_proxy_ptrs[chosen_proxies[i]]->degradedRead(
                &degraded_read_context, degraded_read_request,
                &degraded_read_reply);
        if (status.ok()) {
          std::cout << "[Coordinator] partial degraded read of "
                    << failed_block_id << " success!" << std::endl;
        } else {
          std::cout << "[Coordinator] partial degraded read of "
                    << failed_block_id << " failed!" << std::endl;
        }
      }));
    }
    int cross_rack_num = recovery_group_ids.size() - 1;
    threads.push_back(
        std::thread([this, &t_stripe, cross_rack_num, dest_group_id,
                     dest_cluster_id, dest_proxy_ip, dest_proxy_port, stripe_id,
                     failed_block_id, client_ip, client_port]() {
          grpc::ClientContext recovery_context;
          proxy_proto::RecoveryRequest recovery_request;
          proxy_proto::DegradedReadReply recovery_reply;
          recovery_request.set_failed_block_id(failed_block_id);
          recovery_request.set_failed_block_key(
              t_stripe.blocks[failed_block_id]->block_key);
          recovery_request.set_replaced_node_ip(client_ip);
          recovery_request.set_replaced_node_port(client_port);
          recovery_request.set_cross_rack_num(cross_rack_num);
          std::vector<int> blockids = t_stripe.group_to_blocks[dest_group_id];
          for (int i = 0; i < int(blockids.size()); i++) {
            if (m_sys_config->CodeType == "AzureLRC" &&
                recovery_request.blockids_size() ==
                    (m_sys_config->k / m_sys_config->z))
              break;

            if (blockids[i] == failed_block_id)
              continue;

            Block *t_block = t_stripe.blocks[blockids[i]];
            recovery_request.add_datanodeip(
                this->m_node_table[t_block->map2node].node_ip);
            recovery_request.add_datanodeport(
                this->m_node_table[t_block->map2node].node_port);
            recovery_request.add_blockkeys(t_block->block_key);
            recovery_request.add_blockids(t_block->block_id);
          }
          std::cout << "[Coordinator] start recovery of " << stripe_id << "_"
                    << failed_block_id << std::endl;
          grpc::Status status =
              this->m_proxy_ptrs[dest_proxy_ip + ":" +
                                 std::to_string(dest_proxy_port)]
                  ->degradedRead2Client(&recovery_context, recovery_request,
                                        &recovery_reply);
          if (status.ok()) {
            std::cout << "[Coordinator] degraded read of " << stripe_id << "_"
                      << failed_block_id << " success!" << std::endl;
          } else {
            std::cout << "[Coordinator] degraded read of " << stripe_id << "_"
                      << failed_block_id << " failed!" << std::endl;
          }
        }));
    for (int i = 0; i < threads.size(); i++) {
      threads[i].join();
    }
  }
  return true;
}

bool CoordinatorImpl::degraded_read_one_block_for_workload(
    int stripe_id, int failed_block_id, std::string client_ip, int client_port,
    int block_id) {
  std::string code_type = m_sys_config->CodeType;
  Stripe &t_stripe = m_stripe_table[stripe_id];
  std::vector<int> recovery_group_ids =
      get_recovery_group_ids(m_sys_config->CodeType, m_sys_config->k,
                             m_sys_config->r, m_sys_config->z, failed_block_id);
  grpc::Status status;

  if (recovery_group_ids.size() == 1) {
    // assert((code_type == "UniLRC") || (code_type == "AzureLRC" &&
    // (failed_block_id < m_sys_config->k || failed_block_id >= m_sys_config->k
    // + m_sys_config->r)));

    grpc::ClientContext recovery_context;
    proxy_proto::RecoveryRequest recovery_request;
    proxy_proto::DegradedReadReply degraded_read_reply;

    int chosen_cluster_id =
        get_cluster_id_by_group_id(t_stripe, recovery_group_ids[0]);
    std::string chosen_proxy =
        m_cluster_table[chosen_cluster_id].proxy_ip + ":" +
        std::to_string(m_cluster_table[chosen_cluster_id].proxy_port);
    recovery_request.set_failed_block_id(failed_block_id);
    recovery_request.set_failed_block_key(
        t_stripe.blocks[failed_block_id]->block_key);
    recovery_request.set_replaced_node_ip(client_ip);
    recovery_request.set_replaced_node_port(client_port);
    recovery_request.set_cross_rack_num(0);
    recovery_request.set_is_to_send_block_id(true);
    recovery_request.set_block_id_to_send(block_id);
    std::vector<int> blockids = t_stripe.group_to_blocks[recovery_group_ids[0]];
    for (int i = 0; i < int(blockids.size()); i++) {
      if (blockids[i] == failed_block_id)
        continue;

      Block *t_block = t_stripe.blocks[blockids[i]];
      recovery_request.add_datanodeip(m_node_table[t_block->map2node].node_ip);
      recovery_request.add_datanodeport(
          m_node_table[t_block->map2node].node_port);
      recovery_request.add_blockkeys(t_block->block_key);
      recovery_request.add_blockids(t_block->block_id);
    }
    status = m_proxy_ptrs[chosen_proxy]->degradedRead2Client(
        &recovery_context, recovery_request, &degraded_read_reply);
    if (status.ok()) {
      std::cout << "[Coordinator] degraded read of " << stripe_id << "_"
                << failed_block_id << " success!" << std::endl;
      return true;
    } else {
      std::cout << "[Coordinator] degraded read of " << stripe_id << "_"
                << failed_block_id << " failed!" << std::endl;
      return false;
    }
  } else {
    int dest_group_id = t_stripe.blocks[failed_block_id]->map2group;
    int dest_cluster_id = get_cluster_id_by_group_id(t_stripe, dest_group_id);
    std::string dest_proxy_ip = m_cluster_table[dest_cluster_id].proxy_ip;
    int dest_proxy_port = m_cluster_table[dest_cluster_id].proxy_port;
    std::vector<int> chosen_cluster_ids;
    for (int i = 0; i < recovery_group_ids.size(); i++) {
      chosen_cluster_ids.push_back(
          get_cluster_id_by_group_id(t_stripe, recovery_group_ids[i]));
    }
    std::vector<std::string> chosen_proxies;
    for (int i = 0; i < chosen_cluster_ids.size(); i++) {
      chosen_proxies.push_back(
          m_cluster_table[chosen_cluster_ids[i]].proxy_ip + ":" +
          std::to_string(m_cluster_table[chosen_cluster_ids[i]].proxy_port));
    }
    std::vector<std::thread> threads;
    for (int i = 0; i < recovery_group_ids.size(); i++) {
      if (recovery_group_ids[i] == dest_group_id) {
        continue;
      }
      threads.push_back(std::thread([&t_stripe, &chosen_proxies,
                                     &recovery_group_ids, i, failed_block_id,
                                     dest_proxy_ip, dest_proxy_port, this]() {
        grpc::ClientContext degraded_read_context;
        proxy_proto::DegradedReadRequest degraded_read_request;
        proxy_proto::DegradedReadReply degraded_read_reply;
        degraded_read_request.set_clientip(dest_proxy_ip);
        degraded_read_request.set_clientport(dest_proxy_port +
                                             ECProject::PROXY_PORT_SHIFT);
        degraded_read_request.set_failed_block_id(failed_block_id);
        degraded_read_request.set_failed_block_key(
            t_stripe.blocks[failed_block_id]->block_key);
        std::vector<int> blockids =
            t_stripe.group_to_blocks[recovery_group_ids[i]];
        for (int j = 0; j < int(blockids.size()); j++) {
          if (m_sys_config->CodeType == "AzureLRC" &&
              degraded_read_request.blockids_size() ==
                  (m_sys_config->k / m_sys_config->z))
            break;

          if ((m_sys_config->CodeType == "AzureLRC" &&
               blockids[j] >= m_sys_config->k + m_sys_config->r) ||
              blockids[j] == failed_block_id)
            continue;

          Block *t_block = t_stripe.blocks[blockids[j]];
          degraded_read_request.add_datanodeip(
              this->m_node_table[t_block->map2node].node_ip);
          degraded_read_request.add_datanodeport(
              this->m_node_table[t_block->map2node].node_port);
          degraded_read_request.add_blockkeys(t_block->block_key);
          degraded_read_request.add_blockids(t_block->block_id);
        }
        std::cout << "[Coordinator] start partial degraded read of "
                  << failed_block_id << std::endl;
        grpc::Status status =
            this->m_proxy_ptrs[chosen_proxies[i]]->degradedRead(
                &degraded_read_context, degraded_read_request,
                &degraded_read_reply);
        if (status.ok()) {
          std::cout << "[Coordinator] partial degraded read of "
                    << failed_block_id << " success!" << std::endl;
        } else {
          std::cout << "[Coordinator] partial degraded read of "
                    << failed_block_id << " failed!" << std::endl;
        }
      }));
    }
    int cross_rack_num = recovery_group_ids.size() - 1;
    threads.push_back(
        std::thread([this, &t_stripe, cross_rack_num, dest_group_id,
                     dest_cluster_id, dest_proxy_ip, dest_proxy_port, stripe_id,
                     failed_block_id, client_ip, client_port, block_id]() {
          grpc::ClientContext recovery_context;
          proxy_proto::RecoveryRequest recovery_request;
          proxy_proto::DegradedReadReply recovery_reply;
          recovery_request.set_failed_block_id(failed_block_id);
          recovery_request.set_failed_block_key(
              t_stripe.blocks[failed_block_id]->block_key);
          recovery_request.set_replaced_node_ip(client_ip);
          recovery_request.set_replaced_node_port(client_port);
          recovery_request.set_cross_rack_num(cross_rack_num);
          recovery_request.set_is_to_send_block_id(true);
          recovery_request.set_block_id_to_send(block_id);
          std::vector<int> blockids = t_stripe.group_to_blocks[dest_group_id];
          for (int i = 0; i < int(blockids.size()); i++) {
            if (m_sys_config->CodeType == "AzureLRC" &&
                recovery_request.blockids_size() ==
                    (m_sys_config->k / m_sys_config->z))
              break;

            if (blockids[i] == failed_block_id)
              continue;

            Block *t_block = t_stripe.blocks[blockids[i]];
            recovery_request.add_datanodeip(
                this->m_node_table[t_block->map2node].node_ip);
            recovery_request.add_datanodeport(
                this->m_node_table[t_block->map2node].node_port);
            recovery_request.add_blockkeys(t_block->block_key);
            recovery_request.add_blockids(t_block->block_id);
          }
          std::cout << "[Coordinator] start recovery of " << stripe_id << "_"
                    << failed_block_id << std::endl;
          grpc::Status status =
              this->m_proxy_ptrs[dest_proxy_ip + ":" +
                                 std::to_string(dest_proxy_port)]
                  ->degradedRead2Client(&recovery_context, recovery_request,
                                        &recovery_reply);
          if (status.ok()) {
            std::cout << "[Coordinator] degraded read of " << stripe_id << "_"
                      << failed_block_id << " success!" << std::endl;
          } else {
            std::cout << "[Coordinator] degraded read of " << stripe_id << "_"
                      << failed_block_id << " failed!" << std::endl;
          }
        }));
    for (int i = 0; i < threads.size(); i++) {
      threads[i].join();
    }
  }
  return true;
}

grpc::Status CoordinatorImpl::getDegradedReadBlockBreakdown(
    grpc::ServerContext *context,
    const coordinator_proto::KeyAndClientIP *keyClient,
    coordinator_proto::DegradedReadReply *degradedReadReply) {
  std::chrono::high_resolution_clock::time_point start =
      std::chrono::high_resolution_clock::now();
  double start_time = std::chrono::duration_cast<std::chrono::duration<double>>(
                          start.time_since_epoch())
                          .count();
  degradedReadReply->set_grpc_start_time(start_time);
  std::cout << start_time << std::endl;
  int stripe_id =
      std::stoi(keyClient->key().substr(0, keyClient->key().find('_')));
  int failed_block_id =
      std::stoi(keyClient->key().substr(keyClient->key().find('_') + 1));
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

  /*std::thread t(&CoordinatorImpl::degraded_read_one_block, this, stripe_id,
  failed_block_id, client_ip, client_port); t.join();*/
  bool if_success = degraded_read_one_block_breakdown(
      stripe_id, failed_block_id, client_ip, client_port, disk_io_start_time,
      disk_io_end_time, decode_start_time, decode_end_time, network_start_time,
      network_end_time, cross_rack_network_time, cross_rack_xor_time,
      grpc_notify_time, grpc_start_time, data_node_grpc_notify_time,
      data_node_grpc_start_time);
  if (if_success) {
    double max_disk_io_time =
        *std::max_element(disk_io_end_time.begin(), disk_io_end_time.end()) -
        *std::min_element(disk_io_start_time.begin(), disk_io_start_time.end());
    double max_decode_time =
        *std::max_element(decode_end_time.begin(), decode_end_time.end()) -
        *std::min_element(decode_start_time.begin(), decode_start_time.end()) +
        cross_rack_xor_time;
    double max_network_time =
        *std::max_element(network_end_time.begin(), network_end_time.end()) -
        *std::min_element(network_start_time.begin(),
                          network_start_time.end()) +
        cross_rack_network_time;
    max_network_time +=
        (*std::max_element(grpc_start_time.begin(), grpc_start_time.end()) -
         *std::min_element(grpc_notify_time.begin(), grpc_notify_time.end()));
    max_network_time += (*std::max_element(data_node_grpc_start_time.begin(),
                                           data_node_grpc_start_time.end()) -
                         *std::min_element(data_node_grpc_notify_time.begin(),
                                           data_node_grpc_notify_time.end()));

    degradedReadReply->set_disk_io_time(max_disk_io_time);
    degradedReadReply->set_decode_time(max_decode_time);
    degradedReadReply->set_network_time(max_network_time);
    return grpc::Status::OK;
  } else {
    return grpc::Status(grpc::StatusCode::INTERNAL, "Degraded read failed!");
  }
}

grpc::Status CoordinatorImpl::getDegradedReadBlock(
    grpc::ServerContext *context,
    const coordinator_proto::KeyAndClientIP *keyClient,
    coordinator_proto::DegradedReadReply *degradedReadReply) {
  int stripe_id =
      std::stoi(keyClient->key().substr(0, keyClient->key().find('_')));
  int failed_block_id =
      std::stoi(keyClient->key().substr(keyClient->key().find('_') + 1));
  std::string client_ip = keyClient->clientip();
  int client_port = keyClient->clientport();

  double dest_proxy_network_time;
  bool if_success = degraded_read_one_block(stripe_id, failed_block_id,
                                            client_ip, client_port);
  if (if_success) {
    return grpc::Status::OK;
  } else {
    return grpc::Status(grpc::StatusCode::INTERNAL, "Degraded read failed!");
  }
}

grpc::Status CoordinatorImpl::fullNodeRecovery(
    grpc::ServerContext *context,
    const coordinator_proto::NodeIdFromClient *request,
    coordinator_proto::RepBlockNum *response) {
  int node_id = request->node_id();
  std::string node_ip = m_node_table[node_id].node_ip;
  int node_port = m_node_table[node_id].node_port;
  std::vector<int> stripe_ids;
  std::vector<int> block_ids;
  for (auto it = m_stripe_table.begin(); it != m_stripe_table.end(); it++) {
    for (int i = 0; i < int(it->second.blocks.size()); i++) {
      if (it->second.blocks[i]->map2node == node_id) {
        stripe_ids.push_back(it->first);
        block_ids.push_back(it->second.blocks[i]->block_id);
      }
    }
  }
  if (stripe_ids.size() == 0) {
    std::cout << "[Coordinator] no blocks on node " << node_id << std::endl;
    return grpc::Status::OK;
  }
  std::cout << "[Coordinator] start full node recovery of " << node_id
            << " containing " << block_ids.size() << " blocks" << std::endl;
  response->set_block_num(block_ids.size());
  // recovery_full_node(stripe_ids, block_ids);
  // std::vector<std::thread> recovery_threads;
  std::vector<bool> recovery_results(stripe_ids.size(), false);
  for (int i = 0; i < stripe_ids.size(); i++) {
    bool result = this->recovery_one_block(stripe_ids[i], block_ids[i]);
    recovery_results[i] = result; // 保存结果
  }

  // 检查结果
  bool all_success =
      std::all_of(recovery_results.begin(), recovery_results.end(),
                  [](bool res) { return res; });
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
    return grpc::Status(grpc::StatusCode::INTERNAL, "Full node recovery
  failed!");
  }*/
  return grpc::Status::OK;
}

grpc::Status CoordinatorImpl::multiBlockRecovery(
    grpc::ServerContext *context,
    const coordinator_proto::StripeIdAndBlockIDsFromClient *request,
    coordinator_proto::RecoveryReply *replyClient) {
  int stripe_id = request->stripe_id();
  int block_num = request->block_ids_size();
  std::vector<int> block_ids;
  for (int i = 0; i < block_num; i++) {
    block_ids.push_back(request->block_ids(i));
  }
  std::vector<int> node_ids;
  for (int i = 0; i < block_ids.size(); i++) {
    node_ids.push_back(
        m_stripe_table[stripe_id].blocks[block_ids[i]]->map2node);
  }
  int chosen_cluster_id = randomly_select_a_cluster(stripe_id);
  int chosen_node_id = randomly_select_a_node(chosen_cluster_id, stripe_id);
  std::vector<int> decode_block_ids;
  std::vector<std::vector<int>> decode_factors;
  bool ifGetDecodePlanSuccess = ECProject::get_multi_decode_plan(
      m_sys_config->k, m_sys_config->r, m_sys_config->z, m_sys_config->CodeType,
      block_ids, decode_block_ids, decode_factors);
  if (!ifGetDecodePlanSuccess) {
    std::cout << "[Coordinator] get multi decode plan failed!" << std::endl;
    return grpc::Status(grpc::StatusCode::INTERNAL,
                        "Get multi decode plan failed!");
  }
  std::cout << "[Coordinator] get multi decode plan success!" << std::endl;
  // TODO: notify source proxies and dest proxy, including partial decoding...
  std::vector<int> source_proxies;
  std::vector<std::vector<int>> source_datanodes;
  std::vector<std::vector<int>> decode_blocks_split_for_proxies;
  std::vector<std::vector<int>> decode_factors_split_for_proxies;
  for (int i = 0; i < decode_block_ids.size(); i++) {
    int source_proxy =
        m_stripe_table[stripe_id].blocks[decode_block_ids[i]]->map2cluster;
    auto it =
        std::find(source_proxies.begin(), source_proxies.end(), source_proxy);
    if (it == source_proxies.end()) {
      source_proxies.push_back(source_proxy);
      std::vector<int> t_source_datanodes;
      t_source_datanodes.push_back(
          m_stripe_table[stripe_id].blocks[decode_block_ids[i]]->map2node);
      source_datanodes.push_back(t_source_datanodes);
      std::vector<int> t_decode_blocks;
      t_decode_blocks.push_back(decode_block_ids[i]);
      decode_blocks_split_for_proxies.push_back(t_decode_blocks);
      std::vector<int> t_decode_factors;
      t_decode_factors.push_back(decode_factors[i][0]);
      decode_factors_split_for_proxies.push_back(t_decode_factors);
    } else {
      int index = std::distance(source_proxies.begin(), it);
      source_datanodes[index].push_back(
          m_stripe_table[stripe_id].blocks[decode_block_ids[i]]->map2node);
      decode_blocks_split_for_proxies[index].push_back(decode_block_ids[i]);
      decode_factors_split_for_proxies[index].push_back(decode_factors[i][0]);
    }
  }
}

grpc::Status
CoordinatorImpl::delByKey(grpc::ServerContext *context,
                          const coordinator_proto::KeyFromClient *del_key,
                          coordinator_proto::RepIfDeling *delReplyClient) {
  try {
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
    for (int i = 0; i < int(t_stripe.blocks.size()); i++) {
      if (t_stripe.blocks[i]->map2key == key) {
        node_block.add_datanodeip(
            m_node_table[t_stripe.blocks[i]->map2node].node_ip);
        node_block.add_datanodeport(
            m_node_table[t_stripe.blocks[i]->map2node].node_port);
        node_block.add_blockkeys(t_stripe.blocks[i]->block_key);
        t_cluster_set.insert(t_stripe.blocks[i]->map2cluster);
      }
    }
    node_block.set_stripe_id(
        -1); // as a flag to distinguish delete key or stripe
    node_block.set_key(key);
    // randomly select a cluster
    int idx = rand_num(int(t_cluster_set.size()));
    int r_cluster_id = *(std::next(t_cluster_set.begin(), idx));
    std::string chosen_proxy =
        m_cluster_table[r_cluster_id].proxy_ip + ":" +
        std::to_string(m_cluster_table[r_cluster_id].proxy_port);
    status = m_proxy_ptrs[chosen_proxy]->deleteBlock(&context, node_block,
                                                     &del_reply);
    delReplyClient->set_ifdeling(true);
    if (status.ok()) {
      std::cout << "[DEL] deleting value of " << key << std::endl;
    }
  } catch (const std::exception &e) {
    std::cout << "deleteByKey exception" << std::endl;
    std::cout << e.what() << std::endl;
  }
  return grpc::Status::OK;
}

grpc::Status CoordinatorImpl::delByStripe(
    grpc::ServerContext *context,
    const coordinator_proto::StripeIdFromClient *stripeid,
    coordinator_proto::RepIfDeling *delReplyClient) {
  try {
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
    for (int i = 0; i < int(t_stripe.blocks.size()); i++) {
      if (t_stripe.blocks[i]->map2stripe == t_stripe_id) {
        node_block.add_datanodeip(
            m_node_table[t_stripe.blocks[i]->map2node].node_ip);
        node_block.add_datanodeport(
            m_node_table[t_stripe.blocks[i]->map2node].node_port);
        node_block.add_blockkeys(t_stripe.blocks[i]->block_key);
        t_cluster_set.insert(t_stripe.blocks[i]->map2cluster);
      }
    }
    node_block.set_stripe_id(t_stripe_id);
    node_block.set_key("");
    // randomly select a cluster
    int idx = rand_num(int(t_cluster_set.size()));
    int r_cluster_id = *(std::next(t_cluster_set.begin(), idx));
    std::string chosen_proxy =
        m_cluster_table[r_cluster_id].proxy_ip + ":" +
        std::to_string(m_cluster_table[r_cluster_id].proxy_port);
    status = m_proxy_ptrs[chosen_proxy]->deleteBlock(&context, node_block,
                                                     &del_reply);
    delReplyClient->set_ifdeling(true);
    if (status.ok()) {
      std::cout << "[DEL] deleting value of Stripe " << t_stripe_id
                << std::endl;
    }
  } catch (const std::exception &e) {
    std::cout << "deleteByStripe exception" << std::endl;
    std::cout << e.what() << std::endl;
  }
  return grpc::Status::OK;
}

grpc::Status
CoordinatorImpl::listStripes(grpc::ServerContext *context,
                             const coordinator_proto::RequestToCoordinator *req,
                             coordinator_proto::RepStripeIds *listReplyClient) {
  try {
    for (auto it = m_stripe_table.begin(); it != m_stripe_table.end(); it++) {
      listReplyClient->add_stripe_ids(it->first);
    }
  } catch (const std::exception &e) {
    std::cerr << e.what() << '\n';
  }

  return grpc::Status::OK;
}

grpc::Status CoordinatorImpl::checkalive(
    grpc::ServerContext *context,
    const coordinator_proto::RequestToCoordinator *helloRequestToCoordinator,
    coordinator_proto::ReplyFromCoordinator *helloReplyFromCoordinator) {

  std::cout << "[Coordinator Check] alive " << helloRequestToCoordinator->name()
            << std::endl;
  return grpc::Status::OK;
}
grpc::Status CoordinatorImpl::reportCommitAbort(
    grpc::ServerContext *context,
    const coordinator_proto::CommitAbortKey *commit_abortkey,
    coordinator_proto::ReplyFromCoordinator *helloReplyFromCoordinator) {
  std::string key = commit_abortkey->key();
  ECProject::OpperateType opp = (ECProject::OpperateType)commit_abortkey->opp();
  int stripe_id = commit_abortkey->stripe_id();
  std::unique_lock<std::mutex> lck(m_mutex);
  try {
    if (commit_abortkey->ifcommitmetadata()) {
      if (opp == SET || opp == APPEND) {
        m_object_commit_table[key] = m_object_updating_table[key];
        cv.notify_all();
        m_object_updating_table.erase(key);
      } else if (opp == DEL) // delete the metadata
      {
        if (stripe_id < 0) // delete key
        {
          if (IF_DEBUG) {
            std::cout << "[DEL] Proxy report delete key finish!" << std::endl;
          }
          ObjectInfo object_info = m_object_commit_table.at(key);
          stripe_id = object_info.map2stripe;
          m_object_commit_table.erase(key); // update commit table
          cv.notify_all();
          m_object_updating_table.erase(key);
          Stripe &t_stripe = m_stripe_table[stripe_id];
          std::vector<Block *>::iterator it1;
          for (it1 = t_stripe.blocks.begin(); it1 != t_stripe.blocks.end();) {
            if ((*it1)->map2key == key) {
              it1 = t_stripe.blocks.erase(it1);
            } else {
              it1++;
            }
          }
          if (t_stripe.blocks.empty()) // update stripe table
          {
            m_stripe_table.erase(stripe_id);
          }
          std::map<int, Cluster>::iterator it2; // update cluster table
          for (it2 = m_cluster_table.begin(); it2 != m_cluster_table.end();
               it2++) {
            Cluster &t_cluster = it2->second;
            for (it1 = t_cluster.blocks.begin();
                 it1 != t_cluster.blocks.end();) {
              if ((*it1)->map2key == key) {
                update_stripe_info_in_node(
                    false, (*it1)->map2node,
                    (*it1)->map2stripe); // update node table
                it1 = t_cluster.blocks.erase(it1);
              } else {
                it1++;
              }
            }
          }
        } // delete stripe
        else {
          if (IF_DEBUG) {
            std::cout << "[DEL] Proxy report delete stripe finish!"
                      << std::endl;
          }
          auto its = std::find(m_stripe_deleting_table.begin(),
                               m_stripe_deleting_table.end(), stripe_id);
          if (its != m_stripe_deleting_table.end()) {
            m_stripe_deleting_table.erase(its);
          }
          cv.notify_all();
          // update stripe table
          m_stripe_table.erase(stripe_id);
          std::unordered_set<std::string> object_keys_set;
          // update cluster table
          std::map<int, Cluster>::iterator it2;
          for (it2 = m_cluster_table.begin(); it2 != m_cluster_table.end();
               it2++) {
            Cluster &t_cluster = it2->second;
            for (auto it1 = t_cluster.blocks.begin();
                 it1 != t_cluster.blocks.end();) {
              if ((*it1)->map2stripe == stripe_id) {
                object_keys_set.insert((*it1)->map2key);
                it1 = t_cluster.blocks.erase(it1);
              } else {
                it1++;
              }
            }
          }
          // update node table
          for (auto it3 = m_node_table.begin(); it3 != m_node_table.end();
               it3++) {
            Node &t_node = it3->second;
            auto it4 = t_node.stripes.find(stripe_id);
            if (it4 != t_node.stripes.end()) {
              t_node.stripes.erase(stripe_id);
            }
          }
          // update commit table
          for (auto it5 = object_keys_set.begin(); it5 != object_keys_set.end();
               it5++) {
            auto it6 = m_object_commit_table.find(*it5);
            if (it6 != m_object_commit_table.end()) {
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
        //       for (auto it = t_cluster.blocks.begin(); it !=
        //       t_cluster.blocks.end(); it++)
        //       {
        //         std::cout << "[" << (*it)->block_key << ":S" <<
        //         (*it)->map2stripe << "G" << (*it)->map2group << "N" <<
        //         (*it)->map2node << "] ";
        //       }
        //       std::cout << std::endl;
        //     }
        //   }
        //   std::cout << std::endl;
        // }
      }
    } else {
      m_object_updating_table.erase(key);
    }
  } catch (std::exception &e) {
    std::cout << "reportCommitAbort exception" << std::endl;
    std::cout << e.what() << std::endl;
  }
  return grpc::Status::OK;
}

grpc::Status CoordinatorImpl::checkCommitAbort(
    grpc::ServerContext *context,
    const coordinator_proto::AskIfSuccess *key_opp,
    coordinator_proto::RepIfSuccess *reply) {
  std::unique_lock<std::mutex> lck(m_mutex);
  std::string key = key_opp->key();
  ECProject::OpperateType opp = (ECProject::OpperateType)key_opp->opp();
  int stripe_id = key_opp->stripe_id();
  if (opp == SET || opp == APPEND) {
    while (m_object_commit_table.find(key) == m_object_commit_table.end()) {
      cv.wait(lck);
    }
  } else if (opp == DEL) {
    if (stripe_id < 0) {
      while (m_object_commit_table.find(key) != m_object_commit_table.end()) {
        cv.wait(lck);
      }
    } else {
      auto it = std::find(m_stripe_deleting_table.begin(),
                          m_stripe_deleting_table.end(), stripe_id);
      while (it != m_stripe_deleting_table.end()) {
        cv.wait(lck);
        it = std::find(m_stripe_deleting_table.begin(),
                       m_stripe_deleting_table.end(), stripe_id);
      }
    }
  }
  reply->set_ifcommit(true);
  return grpc::Status::OK;
}

// Check the connnection to all proxies of all clusters
bool CoordinatorImpl::init_proxyinfo() {
  for (auto cur = m_cluster_table.begin(); cur != m_cluster_table.end();
       cur++) {
    std::string proxy_ip_and_port =
        cur->second.proxy_ip + ":" + std::to_string(cur->second.proxy_port);
    auto _stub = proxy_proto::proxyService::NewStub(grpc::CreateChannel(
        proxy_ip_and_port, grpc::InsecureChannelCredentials()));
    proxy_proto::CheckaliveCMD Cmd;
    proxy_proto::RequestResult result;
    grpc::ClientContext clientContext;
    Cmd.set_name("coordinator");
    grpc::Status status;
    status = _stub->checkalive(&clientContext, Cmd, &result);
    if (status.ok()) {
      std::cout << "[Proxy Check] ok from " << proxy_ip_and_port << std::endl;
    } else {
      std::cout << "[Proxy Check] failed to connect " << proxy_ip_and_port
                << std::endl;
    }
    m_proxy_ptrs.insert(std::make_pair(proxy_ip_and_port, std::move(_stub)));
  }
  return true;
}
bool CoordinatorImpl::init_clusterinfo(std::string m_clusterinfo_path) {
  std::cout << "Cluster_information_path:" << m_clusterinfo_path << std::endl;
  tinyxml2::XMLDocument xml;
  xml.LoadFile(m_clusterinfo_path.c_str());
  tinyxml2::XMLElement *root = xml.RootElement();
  int node_id = 0;
  m_num_of_Clusters = 0;
  for (tinyxml2::XMLElement *cluster = root->FirstChildElement();
       cluster != nullptr; cluster = cluster->NextSiblingElement()) {
    std::string cluster_id(cluster->Attribute("id"));
    std::string proxy(cluster->Attribute("proxy"));
    std::cout << "cluster_id: " << cluster_id << " , proxy: " << proxy
              << std::endl;
    Cluster t_cluster;
    m_cluster_table[std::stoi(cluster_id)] = t_cluster;
    m_cluster_table[std::stoi(cluster_id)].cluster_id = std::stoi(cluster_id);
    auto pos = proxy.find(':');
    m_cluster_table[std::stoi(cluster_id)].proxy_ip = proxy.substr(0, pos);
    m_cluster_table[std::stoi(cluster_id)].proxy_port =
        std::stoi(proxy.substr(pos + 1, proxy.size()));
    for (tinyxml2::XMLElement *node =
             cluster->FirstChildElement()->FirstChildElement();
         node != nullptr; node = node->NextSiblingElement()) {
      std::string node_uri(node->Attribute("uri"));
      std::cout << "____node: " << node_uri << std::endl;
      m_cluster_table[std::stoi(cluster_id)].nodes.push_back(node_id);
      m_node_table[node_id].node_id = node_id;
      auto pos = node_uri.find(':');
      m_node_table[node_id].node_ip = node_uri.substr(0, pos);
      m_node_table[node_id].node_port =
          std::stoi(node_uri.substr(pos + 1, node_uri.size()));
      m_node_table[node_id].cluster_id = std::stoi(cluster_id);
      node_id++;
    }
    m_num_of_Clusters++;
  }
  return true;
}

int CoordinatorImpl::randomly_select_a_cluster(int stripe_id) {
  std::random_device rd;
  std::mt19937 gen(rd());
  std::uniform_int_distribution<int> dis_cluster(0, m_num_of_Clusters - 1);
  int r_cluster_id = dis_cluster(gen);
  while (m_cluster_table[r_cluster_id].stripes.find(stripe_id) !=
         m_cluster_table[r_cluster_id].stripes.end()) {
    r_cluster_id = dis_cluster(gen);
  }
  return r_cluster_id;
}

// randomly select a node in the selected cluster
// with the constraint that the node has not been selected for the same stripe
int CoordinatorImpl::randomly_select_a_node(int cluster_id, int stripe_id) {
  std::random_device rd;
  std::mt19937 gen(rd());
  std::uniform_int_distribution<int> dis_node(
      0, m_cluster_table[cluster_id].nodes.size() - 1);
  int r_node_id = m_cluster_table[cluster_id].nodes[dis_node(gen)];
  while (m_node_table[r_node_id].stripes.find(stripe_id) !=
         m_node_table[r_node_id].stripes.end()) {
    r_node_id = m_cluster_table[cluster_id].nodes[dis_node(gen)];
  }
  return r_node_id;
}

void CoordinatorImpl::update_stripe_info_in_node(int t_node_id, int stripe_id,
                                                 int index) {
  assert(m_node_table[t_node_id].stripes.find(stripe_id) ==
             m_node_table[t_node_id].stripes.end() &&
         "The node has been selected for the stripe");
  m_node_table[t_node_id].stripes[stripe_id] = index;
}

// maintain the block number of the stripe in the node
// TODO: Still don't konw why the stripe_block_num is start from 1
void CoordinatorImpl::update_stripe_info_in_node(bool add_or_sub, int t_node_id,
                                                 int stripe_id) {
  int stripe_block_num = 1;
  if (m_node_table[t_node_id].stripes.find(stripe_id) !=
      m_node_table[t_node_id].stripes.end()) {
    stripe_block_num = m_node_table[t_node_id].stripes[stripe_id];
  }
  if (add_or_sub) {
    m_node_table[t_node_id].stripes[stripe_id] = stripe_block_num + 1;
  } else {
    if (stripe_block_num == 1) {
      m_node_table[t_node_id].stripes.erase(stripe_id);
    } else {
      m_node_table[t_node_id].stripes[stripe_id] = stripe_block_num - 1;
    }
  }
}

int CoordinatorImpl::generate_placement(int stripe_id, int block_size) {
  Stripe &stripe_info = m_stripe_table[stripe_id];
  int k = stripe_info.k;
  int l = stripe_info.l;
  int g_m = stripe_info.g_m;
  int b = m_encode_parameters.b_datapergroup;
  ECProject::EncodeType encode_type = m_encode_parameters.encodetype;
  ECProject::SingleStripePlacementType s_placement_type =
      m_encode_parameters.s_stripe_placementtype;
  ECProject::MultiStripesPlacementType m_placement_type =
      m_encode_parameters.m_stripe_placementtype;

  // generate stripe information
  int index = stripe_info.object_keys.size() - 1;
  std::string object_key = stripe_info.object_keys[index];
  Block *blocks_info = new Block[k + g_m + l];
  for (int i = 0; i < k + g_m + l; i++) {
    blocks_info[i].block_size = block_size;
    blocks_info[i].map2stripe = stripe_id;
    blocks_info[i].map2key = object_key;
    if (i < k) {
      std::string tmp = "_D";
      if (i < 10)
        tmp = "_D0";
      blocks_info[i].block_key = object_key + tmp + std::to_string(i);
      blocks_info[i].block_id = i;
      blocks_info[i].block_type = 'D';
      blocks_info[i].map2group = int(i / b);
      stripe_info.blocks.push_back(&blocks_info[i]);
    } else if (i >= k && i < k + g_m) {
      blocks_info[i].block_key =
          "Stripe" + std::to_string(stripe_id) + "_G" + std::to_string(i - k);
      blocks_info[i].block_id = i;
      blocks_info[i].block_type = 'G';
      blocks_info[i].map2group = l;
      stripe_info.blocks.push_back(&blocks_info[i]);
    } else {
      blocks_info[i].block_key = "Stripe" + std::to_string(stripe_id) + "_L" +
                                 std::to_string(i - k - g_m);
      blocks_info[i].block_id = i;
      blocks_info[i].block_type = 'L';
      blocks_info[i].map2group = i - k - g_m;
      stripe_info.blocks.push_back(&blocks_info[i]);
    }
  }

  if (encode_type == Azure_LRC || encode_type == Optimal_Cauchy_LRC) {
    if (s_placement_type == Optimal) {
      if (m_placement_type == Ran) {
        int idx = m_merge_groups.size() - 1;
        if (idx < 0 || int(m_merge_groups[idx].size()) ==
                           m_encode_parameters.x_stripepermergegroup) {
          std::vector<int> temp;
          temp.push_back(stripe_id);
          m_merge_groups.push_back(temp);
        } else {
          m_merge_groups[idx].push_back(stripe_id);
        }

        int g_cluster_id = -1;
        for (int i = 0; i < l; i++) {
          for (int j = i * b; j < (i + 1) * b; j += g_m + 1) {
            bool flag = false;
            if (j + g_m + 1 >= (i + 1) * b)
              flag = true;
            // randomly select a cluster
            int t_cluster_id = randomly_select_a_cluster(stripe_id);
            Cluster &t_cluster = m_cluster_table[t_cluster_id];
            // place every g+1 data blocks from each group to a single cluster
            for (int o = j; o < j + g_m + 1 && o < (i + 1) * b; o++) {
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
            if (flag) {
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
              } else // place the local parity blocks together with global ones
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
        for (int i = 0; i < g_m; i++) {
          int t_node_id = randomly_select_a_node(g_cluster_id, stripe_id);
          blocks_info[k + i].map2cluster = g_cluster_id;
          blocks_info[k + i].map2node = t_node_id;
          update_stripe_info_in_node(true, t_node_id, stripe_id);
          g_cluster.blocks.push_back(&blocks_info[k + i]);
          g_cluster.stripes.insert(stripe_id);
          stripe_info.place2clusters.insert(g_cluster_id);
        }
      } else if (m_placement_type == DIS) {
        int required_cluster_num = ceil(b + 1, g_m + 1) * l + 1;
        int idx = m_merge_groups.size() - 1;
        if (b % (g_m + 1) == 0)
          required_cluster_num -= l;
        if (int(m_free_clusters.size()) < required_cluster_num ||
            m_free_clusters.empty() || idx < 0 ||
            int(m_merge_groups[idx].size()) ==
                m_encode_parameters.x_stripepermergegroup) {
          m_free_clusters.clear();
          m_free_clusters.shrink_to_fit();
          for (int i = 0; i < m_num_of_Clusters; i++) {
            m_free_clusters.push_back(i);
          }
          std::vector<int> temp;
          temp.push_back(stripe_id);
          m_merge_groups.push_back(temp);
        } else {
          m_merge_groups[idx].push_back(stripe_id);
        }

        int g_cluster_id = -1;
        for (int i = 0; i < l; i++) {
          for (int j = i * b; j < (i + 1) * b; j += g_m + 1) {
            bool flag = false;
            if (j + g_m + 1 >= (i + 1) * b)
              flag = true;
            // randomly select a cluster
            int t_cluster_id =
                m_free_clusters[rand_num(int(m_free_clusters.size()))];
            auto iter = std::find(m_free_clusters.begin(),
                                  m_free_clusters.end(), t_cluster_id);
            if (iter != m_free_clusters.end()) {
              m_free_clusters.erase(iter);
            } // remove the selected cluster from the free list
            Cluster &t_cluster = m_cluster_table[t_cluster_id];
            // place every g+1 data blocks from each group to a single cluster
            for (int o = j; o < j + g_m + 1 && o < (i + 1) * b; o++) {
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
            if (flag) {
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
              } else // place the local parity blocks together with global ones
              {
                if (g_cluster_id == -1) // randomly select a new cluster
                {
                  g_cluster_id =
                      m_free_clusters[rand_num(int(m_free_clusters.size()))];
                  auto iter = std::find(m_free_clusters.begin(),
                                        m_free_clusters.end(), g_cluster_id);
                  if (iter != m_free_clusters.end()) {
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
          auto iter = std::find(m_free_clusters.begin(), m_free_clusters.end(),
                                g_cluster_id);
          if (iter != m_free_clusters.end()) {
            m_free_clusters.erase(iter);
          }
        }
        Cluster &g_cluster = m_cluster_table[g_cluster_id];
        // place the global parity blocks to the selected cluster
        for (int i = 0; i < g_m; i++) {
          int t_node_id = randomly_select_a_node(g_cluster_id, stripe_id);
          blocks_info[k + i].map2cluster = g_cluster_id;
          blocks_info[k + i].map2node = t_node_id;
          update_stripe_info_in_node(true, t_node_id, stripe_id);
          g_cluster.blocks.push_back(&blocks_info[k + i]);
          g_cluster.stripes.insert(stripe_id);
          stripe_info.place2clusters.insert(g_cluster_id);
        }
      } else if (m_placement_type == AGG) {
        int agg_clusters_num = ceil(b + 1, g_m + 1) * l + 1;
        if (b % (g_m + 1) == 0) {
          agg_clusters_num -= l;
        }
        int idx = m_merge_groups.size() - 1;
        if (idx < 0 || int(m_merge_groups[idx].size()) ==
                           m_encode_parameters.x_stripepermergegroup) {
          std::vector<int> temp;
          temp.push_back(stripe_id);
          m_merge_groups.push_back(temp);
          m_agg_start_cid = rand_num(m_num_of_Clusters - agg_clusters_num);
        } else {
          m_merge_groups[idx].push_back(stripe_id);
        }
        int t_cluster_id = m_agg_start_cid - 1;
        int g_cluster_id = -1;
        for (int i = 0; i < l; i++) {
          for (int j = i * b; j < (i + 1) * b; j += g_m + 1) {
            bool flag = false;
            if (j + g_m + 1 >= (i + 1) * b)
              flag = true;
            t_cluster_id += 1;
            Cluster &t_cluster = m_cluster_table[t_cluster_id];
            // place every g+1 data blocks from each group to a single cluster
            for (int o = j; o < j + g_m + 1 && o < (i + 1) * b; o++) {
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
            if (flag) {
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
              } else // place the local parity blocks together with global ones
              {
                if (g_cluster_id == -1) {
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
        if (g_cluster_id == -1) {
          g_cluster_id = t_cluster_id + 1;
        }
        Cluster &g_cluster = m_cluster_table[g_cluster_id];
        // place the global parity blocks to the selected cluster
        for (int i = 0; i < g_m; i++) {
          int t_node_id = randomly_select_a_node(g_cluster_id, stripe_id);
          blocks_info[k + i].map2cluster = g_cluster_id;
          blocks_info[k + i].map2node = t_node_id;
          update_stripe_info_in_node(true, t_node_id, stripe_id);
          g_cluster.blocks.push_back(&blocks_info[k + i]);
          g_cluster.stripes.insert(stripe_id);
          stripe_info.place2clusters.insert(g_cluster_id);
        }
      } else if (m_placement_type == OPT) {
        int required_cluster_num = ceil(b + 1, g_m + 1) * l + 1;
        int agg_clusters_num = l + 1;
        if (b % (g_m + 1) == 0) {
          agg_clusters_num = 1;
          required_cluster_num -= l;
        }
        int idx = m_merge_groups.size() - 1;
        if (int(m_free_clusters.size()) <
                required_cluster_num - agg_clusters_num ||
            m_free_clusters.empty() || idx < 0 ||
            int(m_merge_groups[idx].size()) ==
                m_encode_parameters.x_stripepermergegroup) {
          m_agg_start_cid = rand_num(m_num_of_Clusters - agg_clusters_num);
          m_free_clusters.clear();
          m_free_clusters.shrink_to_fit();
          for (int i = 0; i < m_agg_start_cid; i++) {
            m_free_clusters.push_back(i);
          }
          for (int i = m_agg_start_cid + agg_clusters_num;
               i < m_num_of_Clusters; i++) {
            m_free_clusters.push_back(i);
          }
          std::vector<int> temp;
          temp.push_back(stripe_id);
          m_merge_groups.push_back(temp);
        } else {
          m_merge_groups[idx].push_back(stripe_id);
        }

        int agg_cluster_id = m_agg_start_cid - 1;
        int t_cluster_id = -1;
        int g_cluster_id = m_agg_start_cid + agg_clusters_num - 1;
        for (int i = 0; i < l; i++) {
          for (int j = i * b; j < (i + 1) * b; j += g_m + 1) {
            bool flag = false;
            if (j + g_m + 1 >= (i + 1) * b)
              flag = true;
            if (flag && j + g_m + 1 != (i + 1) * b) {
              t_cluster_id = ++agg_cluster_id;
            } else {
              t_cluster_id =
                  m_free_clusters[rand_num(int(m_free_clusters.size()))];
              auto iter = std::find(m_free_clusters.begin(),
                                    m_free_clusters.end(), t_cluster_id);
              if (iter != m_free_clusters.end()) {
                m_free_clusters.erase(iter);
              }
            }
            Cluster &t_cluster = m_cluster_table[t_cluster_id];
            // place every g+1 data blocks from each group to a single cluster
            for (int o = j; o < j + g_m + 1 && o < (i + 1) * b; o++) {
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
            if (flag) {
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
              } else // place the local parity blocks together with global ones
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
        for (int i = 0; i < g_m; i++) {
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

  if (IF_DEBUG) {
    std::cout << std::endl;
    std::cout << "Data placement result:" << std::endl;
    for (int i = 0; i < m_num_of_Clusters; i++) {
      Cluster &t_cluster = m_cluster_table[i];
      if (int(t_cluster.blocks.size()) > 0) {
        std::cout << "Cluster " << i << ": ";
        for (auto it = t_cluster.blocks.begin(); it != t_cluster.blocks.end();
             it++) {
          std::cout << "[" << (*it)->block_key << ":S" << (*it)->map2stripe
                    << "G" << (*it)->map2group << "N" << (*it)->map2node
                    << "] ";
        }
        std::cout << std::endl;
      }
    }
    std::cout << std::endl;
    std::cout << "Merge Group: ";
    for (auto it1 = m_merge_groups.begin(); it1 != m_merge_groups.end();
         it1++) {
      std::cout << "[ ";
      for (auto it2 = (*it1).begin(); it2 != (*it1).end(); it2++) {
        std::cout << (*it2) << " ";
      }
      std::cout << "] ";
    }
    std::cout << std::endl;
  }

  // randomly select a cluster
  int r_idx = rand_num(int(stripe_info.place2clusters.size()));
  int selected_cluster_id =
      *(std::next(stripe_info.place2clusters.begin(), r_idx));
  if (IF_DEBUG) {
    std::cout << "[SET] Select the proxy in cluster " << selected_cluster_id
              << " to encode and set!" << std::endl;
  }
  return selected_cluster_id;
}

void CoordinatorImpl::blocks_in_cluster(
    std::map<char, std::vector<ECProject::Block *>> &block_info, int cluster_id,
    int stripe_id) {
  std::vector<ECProject::Block *> tt, td, tl, tg;
  Cluster &cluster = m_cluster_table[cluster_id];
  std::vector<Block *>::iterator it;
  for (it = cluster.blocks.begin(); it != cluster.blocks.end(); it++) {
    Block *block = *it;
    if (block->map2stripe == stripe_id) {
      tt.push_back(block);
      if (block->block_type == 'D') {
        td.push_back(block);
      } else if (block->block_type == 'L') {
        tl.push_back(block);
      } else {
        tg.push_back(block);
      }
    }
  }
  block_info['T'] = tt;
  block_info['D'] = td;
  block_info['L'] = tl;
  block_info['G'] = tg;
}

void CoordinatorImpl::find_max_group(int &max_group_id, int &max_group_num,
                                     int cluster_id, int stripe_id) {
  int group_cnt[5] = {0};
  Cluster &cluster = m_cluster_table[cluster_id];
  std::vector<Block *>::iterator it;
  for (it = cluster.blocks.begin(); it != cluster.blocks.end(); it++) {
    if ((*it)->map2stripe == stripe_id) {
      group_cnt[(*it)->map2group]++;
    }
  }
  for (int i = 0; i <= m_encode_parameters.l_localparityblock; i++) {
    if (group_cnt[i] > max_group_num) {
      max_group_id = i;
      max_group_num = group_cnt[i];
    }
  }
}

int CoordinatorImpl::count_block_num(char type, int cluster_id, int stripe_id,
                                     int group_id) {
  int cnt = 0;
  Cluster &cluster = m_cluster_table[cluster_id];
  std::vector<Block *>::iterator it;
  for (it = cluster.blocks.begin(); it != cluster.blocks.end(); it++) {
    Block *block = *it;
    if (block->map2stripe == stripe_id) {
      if (group_id == -1) {
        if (type == 'T') {
          cnt++;
        } else if (block->block_type == type) {
          cnt++;
        }
      } else if (int(block->map2group) == group_id) {
        if (type == 'T') {
          cnt++;
        } else if (block->block_type == type) {
          cnt++;
        }
      }
    }
  }
  if (cnt == 0) {
    cluster.stripes.erase(stripe_id);
  }
  return cnt;
}

// find out if any specific type of block from the stripe exists in the cluster
bool CoordinatorImpl::find_block(char type, int cluster_id, int stripe_id) {
  Cluster &cluster = m_cluster_table[cluster_id];
  std::vector<Block *>::iterator it;
  for (it = cluster.blocks.begin(); it != cluster.blocks.end(); it++) {
    if (stripe_id != -1 && int((*it)->map2stripe) == stripe_id &&
        (*it)->block_type == type) {
      return true;
    } else if (stripe_id == -1 && (*it)->block_type == type) {
      return true;
    }
  }
  return false;
}
std::vector<std::vector<int>>
CoordinatorImpl::Get_OA_Information(const std::string &filename) {
  std::vector<std::vector<int>> matrix;
  std::ifstream infile(filename);
  if (!infile.is_open()) {
    throw std::runtime_error("Cannot open file: " + filename);
  }

  std::string line;
  while (std::getline(infile, line)) {
    std::istringstream iss(line);
    std::vector<int> row;
    int value;
    while (iss >> value) {
      row.push_back(value);
    }
    if (!row.empty())
      matrix.push_back(row);
  }

  return matrix;
}
} // namespace ECProject
