# 仅在合并阶段做带宽限制

系统本身**没有**「只对 merge 生效」的内置开关；`wondershaper` 是整机网卡限速。做法是**按时间顺序**控制何时限速：

## 推荐流程

1. **实验开始**：不要执行 `limit_*`，保证条带放置全速。
2. **放置结束**：`main_client` 会打印吞吐并出现提示；若要做合并带宽实验：
   - 在**另一终端**（或另一台能 pdsh 到各 proxy 的机器）执行：
     ```bash
     sh limit_all_intra10Gb_inter1Gb.sh
     ```
   - 再回到 `main_client` 输入 `Y` 开始 `start_merge()`。
3. **合并结束**：解除限速，避免影响后续操作：
   ```bash
   sh unlimit_all_proxy.sh
   ```

## 脚本对应关系

| 操作           | 脚本 |
|----------------|------|
| 全集群 proxy 限速 | `limit_all_intra10Gb_inter1Gb.sh` |
| 全集群 proxy 解除 | `unlimit_all_proxy.sh` |

单机调试可用：`limit_intra10Gb_inter1Gb.sh` / `unlimit_both_interfaces.sh`。

## 注意

- 限速脚本需在**各 proxy 节点**的 `/users/qiliang/UniLRC/` 下存在（可先 `update_all.sh` 同步）。
- 网卡名默认 `enp6s0f0` / `enp6s0f1`，与现场不一致时请改 `limit_intra10Gb_inter1Gb.sh`。
