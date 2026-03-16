# RS 编码 + EQUIOX_MODE 放置流程说明

本文档描述在 **RS 编码** 且 **AppendMode = EQUIOX_MODE** 时，整条带写入（`Client::set()`）从客户端到协调者、代理、数据节点的**具体放置方式**，以及 gRPC / ASIO 的调用关系，保证能**直接放置成功**。

---

## 一、前提与配置

- **parameterConfiguration.xml**：`CodeType=RS`，`AppendMode=EQUIOX_MODE`，`k`、`r` 已配置，`z=0`。
- **clusterInformation.xml**：各 cluster 的 proxy 与 datanode 的 IP:Port 已配置。
- **num_arry**：由 config 的 `get_num_arry()` 根据 N、k、r 计算；**num_arry[0]==1** 为第一种 RS 放置，**num_arry[0]==2** 为第二种（两校验组 + 纯数据组）。

---

## 二、整体数据流（能直接放置成功的顺序）

```
Client::set()
  │
  ├─1. gRPC: uploadSetValue(key, valueSize=BlockSize*k, append_mode="EQUIOX_MODE")
  │      → Coordinator 做放置、生成 add_plans、通知各 Proxy 准备接收
  │      ← reply: append_keys[], proxyips[], proxyports[], cluster_slice_sizes[], sum_append_size
  │
  ├─2. 本地：按 reply 把 buffer 按组切分 → 按组得到 data/parity 指针 → RS 编码写回 parity
  │
  └─3. 多线程：每个组用 ASIO TCP 把该组整段数据发到对应 Proxy → Proxy 再按 plan 写各 Datanode
        最后 gRPC checkCommitAbort 确认本组提交
```

下面按步骤展开。

---

## 三、步骤 1：Client → Coordinator（gRPC）

### 3.1 Client 发起

- **接口**：`m_coordinator_ptr->uploadSetValue(&context, request, &reply)`
- **request**：
  - `key` = m_clientID（客户端标识）
  - `valuesizebytes` = BlockSize × k（整条带数据量，字节）
  - `append_mode` = **"EQUIOX_MODE"**

### 3.2 Coordinator 处理（uploadSetValue）

1. **建条带**：`t_stripe`（stripe_id, k, r, z, N, num_arry, object_keys 等）。
2. **做放置**：`initialize_equiox_stripe_placement(&t_stripe)`  
   - RS 且 num_arry[0]==1：单校验组 + 数据组（奇/偶分组），OA1 第一列给校验，数据组从 list_num_rs 列起；节点按 OA2 规则。  
   - RS 且 num_arry[0]==2：两校验组（2 校验+r-3 数据 / r-2 校验+1 数据）+ 纯数据组；OA1 第 1、2 列给两校验组，数据组从 list_num_rs 列起；节点按 OA2 规则。  
   - 结果写入 `t_stripe.blocks`、`group_to_blocks`、`map2cluster`、`map2node` 等。
3. **生成下发计划**：`generate_add_plans(&t_stripe)`  
   - 按 `stripe->num_groups` 为每组生成一个 `AppendStripeDataPlacement`。  
   - 每组内按 `group_to_blocks[i]` 的顺序，把该组所有 block 的 (datanodeip, datanodeport, blockkeys, blockids, offsets, sizes) 加入 plan；**append_mode** 在 RS 时设为 **m_sys_config->AppendMode**（即 EQUIOX_MODE）。
4. **通知各 Proxy 准备接收**：对每个 plan 起线程 `notify_proxies_ready(plan)`，内部用 gRPC 调用对应 Proxy 的 `scheduleAppend2Datanode`，Proxy 在 **proxy_port + PROXY_PORT_SHIFT** 上挂起 `acceptor.accept()` 等待 TCP 数据。
5. **填 reply 返回 Client**：
   - `append_keys[i]` = plan.key()（如 stripe_id_groupid）
   - `proxyips[i]`、`proxyports[i]` = 该组所在 cluster 的 proxy IP 与 **proxy_port + PROXY_PORT_SHIFT**（接收数据用）
   - `cluster_slice_sizes[i]` = 该组字节数（该组块数 × BlockSize）
   - `sum_append_size` = 所有组字节之和（= n × BlockSize）

要点：**每组对应一个 (append_key, proxy_ip, proxy_port, cluster_slice_size)**，且 **plan 中块顺序与 group_to_blocks[i] 一致**，这样后面 Client 按“组内先数据后校验”排布才能和 Proxy 期望的块顺序一致。

---

## 四、步骤 2：Client 本地切分与编码

### 4.1 按 reply 切分整块 buffer

- **buffer**：`m_pre_allocated_buffer`，大小 **BlockSize × n**（n = k + r）。
- **切分**：`m_toolbox->splitCharPointer(m_pre_allocated_buffer, &reply)`  
  - 按 `reply.cluster_slice_sizes(i)` 依次切，得到 `cluster_slice_data[0..num_groups-1]`，每组一段连续内存，长度 = 该组块数 × BlockSize。

### 4.2 每组内：数据块数 / 全局校验块数 / 本地校验块数（RS 需与 stripe 一致）

Client 必须与 Coordinator **同一条带、同一分组**：

- **RS 时**：从 `reply.append_keys(0)` 解析 `stripe_id`（格式 `"stripeid_0"`），调用  
  `get_rs_block_num_per_group_from_stripe_id(k, r, z, stripe_id, data_block_num_per_group, global_parity_block_num_per_group, local_parity_block_num_per_group)`，  
  用与 Coordinator 相同的 `initial_list = (stripe_id % 2^N) + 1` 和 num_arry[0]==1/2 公式，得到每组的数据/全局校验/本地校验块数。
- **num_arry[0]==1**：组 0 为 0 数据 + r 全局校验；数据组按 initial_list 奇偶为「前 r-b 组 r-1 后若干组 r」或「前若干组 r 后 r-b 组 r-1」。
- **num_arry[0]==2**：组 0 为 r-3 数据 + 2 全局校验；组 1 为 1 数据 + (r-2) 全局校验；其余为纯数据组 (r-1) 或 r。
- **非 RS**：仍用 `get_data_block_num_per_group` / `get_global_parity_block_num_per_group` / `get_local_parity_block_num_per_group`。

- **get_global_parity_block_num_per_group(...)**（非 RS 或未用 stripe_id 时）  
  - num_arry[0]==1：组 0 为 r，其余 0。  
  - num_arry[0]==2：组 0 为 2，组 1 为 r-2，其余 0。
- **get_local_parity_block_num_per_group(...)**  
  - RS 下 z=0，全部 0；仅保证返回长度与组数一致。

### 4.3 每组内指针：数据 / 全局校验 / 本地校验

- **split_for_set_data_and_parity(reply, cluster_slice_data, data_block_num_per_group, global_parity_block_num_per_group, local_parity_block_num_per_group, data_ptr_array, global_parity_ptr_array, local_parity_ptr_array)**  
  - 对每个组 i：把 `cluster_slice_data[i]` 按 `[数据块 | 全局校验块 | 本地校验块]` 的顺序切成指针，依次 push 到 `data_ptr_array`、`global_parity_ptr_array`、`local_parity_ptr_array`。  
  - 组内顺序与 Coordinator 的 **group_to_blocks[i]** 顺序一致：先数据块再校验块（RS 两种 case 下当前实现均满足）。

### 4.4 RS 编码

- **encode_rs(k, r, z, data_ptr_array.data(), parity_ptr_array.data(), BlockSize)**  
  - `parity_ptr_array` = global_parity_ptr_array + local_parity_ptr_array（RS 下仅全局 r 个有效）。  
  - 编码结果写回 `parity_ptr_array` 指向的内存，即写回 `m_pre_allocated_buffer` 中对应段；因此 **cluster_slice_data[i]** 在发送时已含编码后的该组数据+校验。

至此，**m_pre_allocated_buffer 被按组、按块顺序填好**，与各 plan 的 block 顺序一致，可直接按组发送。

---

## 五、步骤 3：Client → Proxy（ASIO TCP）→ Datanode（gRPC）

### 5.1 Client 按组发送（ASIO）

对每个组 i（0..reply.append_keys_size()-1）：

- **async_append_to_proxies(cluster_slice_data[i], reply.append_keys(i), reply.cluster_slice_sizes(i), reply.proxyips(i), reply.proxyports(i), i, if_commit_arr)**  
  - **ASIO**：`resolver.resolve(proxy_ip, proxy_port)` → `connect(sock_data, endpoints)` → `write(sock_data, asio::buffer(cluster_slice_data[i], cluster_slice_size))` → shutdown_send + close。  
  - **proxy_port** 为 Coordinator 返回的 **proxy_port + PROXY_PORT_SHIFT**，即 Proxy 监听数据的那一侧端口。  
  - 发送内容：该组整段 **cluster_slice_data[i]**，长度 **cluster_slice_sizes(i)**，与 plan 的 **append_size** 一致。

### 5.2 Proxy 接收并写 Datanode

- **scheduleAppend2Datanode**（由 notify_proxies_ready 通过 gRPC 已调用）：  
  - 在 **proxy_port + PROXY_PORT_SHIFT** 上 `acceptor.accept(socket_data)`，收到 **cluster_append_size** 字节。  
  - 用 **placement_copy**（AppendStripeDataPlacement）的 **sizes** 把这段内存切成与 block 一一对应的 slices。  
  - 对每个 j：**AppendToDatanode(blockkeys(j), blockids(j), sizes(j), slices[j], offsets(j), datanodeip(j), datanodeport(j), is_serialized)**，通过 gRPC 调用 Datanode 写块。  
  - 写完后 **reportCommitAbort** 到 Coordinator，标记该 append_key 已提交。

### 5.3 Client 确认提交

- **m_coordinator_ptr->checkCommitAbort(key=append_key, opp=APPEND, &reply)**  
  - 若 `reply.ifcommit()` 为 true，则 `if_commit_arr[index]=true`。  
  - 当所有组都 if_commit_arr[i]==true 时，Client 认为本次 **set() 放置成功**。

---

## 六、为何能“直接放置成功”（关键对应关系）

1. **组数与块数一致**  
   Coordinator 的 `stripe->num_groups`、`group_to_blocks` 与 Client 的每组块数一致。RS 时 Client 从 `reply.append_keys(0)` 解析 `stripe_id` 后调用 `get_rs_block_num_per_group_from_stripe_id(stripe_id)`，用与 Coordinator 相同的 `initial_list=(stripe_id%2^N)+1` 和 num_arry[0]==1/2 公式，故组数及每组 data/global_parity/local_parity 块数与 `group_to_blocks` 一致，`reply.append_keys_size()`、`cluster_slice_sizes` 与 Client 的组视图一致。

2. **每组字节数一致**  
   - Coordinator：`append_size = group_to_blocks[i].size() * BlockSize`。  
   - Client：`cluster_slice_sizes(i)` 来自同一批 add_plans，且 `sum_append_size = n * BlockSize`。  
   - Proxy：按 plan 的 sizes 切收到的 `cluster_append_size`，与 plan 中块数×BlockSize 一致。

3. **组内块顺序一致**  
   - Coordinator：plan 中块顺序 = `group_to_blocks[i][0], group_to_blocks[i][1], ...`。  
   - Client：每组内 layout 为 [数据块][全局校验块][本地校验块]，与当前 RS 两种 case 的 group_to_blocks 顺序一致。  
   - Proxy：slices[j] 按 plan 的 blockkeys/blockids 顺序写往对应 Datanode，与 Client 发来的段内顺序一致。

4. **RS 时 append_mode 一致**  
   - Client request：`append_mode="EQUIOX_MODE"`。  
   - Coordinator 在 **generate_add_plans** 中，当 CodeType==RS 时使用 **m_sys_config->AppendMode**（EQUIOX_MODE），plan 下发给 Proxy 的 append_mode 与之一致，便于后续若有按 mode 的分支也能对齐。

5. **端口一致**  
   - Coordinator 返回的 proxyports 已是 **proxy_port + PROXY_PORT_SHIFT**，Client 用该端口建 TCP 连接；Proxy 在相同端口 accept，一一对应。

---

## 七、小结（可直接放置成功的检查清单）

- **配置**：CodeType=RS，AppendMode=EQUIOX_MODE，k/r/ClusterNum/DatanodeNumPerCluster 正确；num_arry 已算（含 num_arry[0]==1 或 2）。
- **Coordinator**：RS 走 `initialize_equiox_stripe_placement`（含 num_arry[0] 分支）；`generate_add_plans` 对 RS 使用 `AppendMode`；`notify_proxies_ready` 在数据到达前调用，Proxy 已 accept 等待。
- **Client**：RS 的 get_data/global_parity/local_parity 三种 block_num_per_group 与 coordinator 分组一致；split_for_set_data_and_parity 后 encode_rs；按 reply 的 proxyips/proxyports/cluster_slice_sizes 用 ASIO 按组发送整段数据。
- **Proxy**：用 plan 的 sizes 切收到的段，按 blockkeys/blockids 写 Datanode，再 reportCommitAbort；Client 用 checkCommitAbort 确认每组提交。

按上述流程实现并保证组数、块数、块顺序、字节数、端口与 append_mode 一致，即可在 RS 编码 + EQUIOX_MODE 下**直接放置成功**。

---

## 八、RS + EQUIOX_MODE 数据实际放置成功性确认（结论）

**结论：在当前实现下，RS 码 + EQUIOX_MODE 的数据实际放置可以成功。**

逐项核对结果：

| 检查项 | 实现情况 |
|--------|----------|
| 组数一致 | Coordinator：`stripe->num_groups = group_to_blocks.size()`（RS 为 1+num_data_groups 或 2+num_pure）。Client：`get_rs_block_num_per_group_from_stripe_id` 输出向量长度与之相同，且 `reply.append_keys_size()` = 组数。 |
| 每组块数一致 | 每组 `data_block_num_per_group[i] + global_parity_block_num_per_group[i] + local_parity_block_num_per_group[i]` = `group_to_blocks[i].size()`，且 `cluster_slice_sizes(i) = group_to_blocks[i].size() * BlockSize`。 |
| stripe_id 一致 | `append_keys(0)` 格式为 `gen_append_key(stripe_id, 0)` = `"stripeid_0"`，Client 解析得到的 stripe_id 与 Coordinator 本条带一致，故 initial_list 奇偶与 Coordinator 一致。 |
| 组内块顺序一致 | Coordinator 的 `group_to_blocks[i]` 按块下标 i 递增添加（`add_to_map(..., map2group, i)`），组内顺序为「先数据块再校验块」或「仅数据/仅校验」。Client 的 `split_for_set_data_and_parity` 按 [数据\|全局校验\|本地校验] 切分，与 group_to_blocks 顺序一致；Proxy 按 plan 的 sizes 顺序写 blockkeys/blockids，与接收段顺序一致。 |
| RS 编码输入顺序 | `data_ptr_array` 为按组拼接的组内数据指针，组内为块下标序，整体即 [D0..D(k-1)]；`parity_ptr_array` 为 [G0..G(r-1)]，满足 `encode_rs(k, r, z, data_ptr_array, parity_ptr_array, BlockSize)` 的语义。 |
| 发送与接收长度 | Client 发送 `cluster_slice_data[i]` 长度 `cluster_slice_sizes(i)`；Proxy 读 `cluster_append_size = plan.append_size()`，二者相等。 |

因此，在配置正确（CodeType=RS、AppendMode=EQUIOX_MODE、num_arry 与 N 正确）、且 `reply.append_keys_size() > 0` 时，当前 Client/Coordinator/Proxy 实现下 **RS + EQUIOX_MODE 的数据放置可以成功**。
