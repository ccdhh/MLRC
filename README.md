# DdlRT gLRC Recovery System

DdlRT is a prototype system for erasure-code recovery experiments. The current implementation focuses on gLRC and consists of four components:

- `client`: writes stripes, generates failure patterns, and starts recovery trials;
- `coordinator`: maintains stripe placement, selects recovery equations, and orchestrates recovery;
- `proxy`: executes Phase1, Phase2, Pipeline, or Hybrid recovery;
- `datanode`: stores blocks and provides streaming read/write interfaces.

A real-system deployment runs one datanode and one proxy on every storage host. Each host has an independent ingress and egress bandwidth budget. The `cluster` entries in the configuration represent logical gLRC placement groups, not physical racks, and do not introduce rack-level shared bandwidth.

## 1. Repository Layout

- `project/`: C++ implementation, Protocol Buffers, configuration, and build output;
- `project/config/parameterConfiguration.xml`: coding and recovery parameters;
- `project/config/clusterInformation.xml`: node and logical-group topology;
- `Real-system/`: topology generation, deployment, startup, and experiment scripts;
- `third_party/`: gRPC, Asio, GF-Complete, and Jerasure source packages;
- `logs/client_runs/`: complete output from each client experiment;
- `doc/`: supplementary design documents.

All commands below assume that the repository root is the current directory:

```bash
cd ~/DdlRT
```

## 2. Environment Setup

### 2.1 System Dependencies

Install the required tools on the build host:

```bash
sudo apt update
sudo apt install -y \
  build-essential cmake autoconf automake libtool pkg-config \
  nasm rsync python3
```

`nasm` is required by the ISA-L/AVX2 codec implementation.

### 2.2 Third-Party Libraries

The required source packages are included in the repository. Install them once with:

```bash
bash install_third_party.sh
```

This installs Asio, gRPC, GF-Complete, and Jerasure under `project/third_party/`.

### 2.3 Passwordless SSH

The coordinator host must be able to log in to the client and all storage hosts without prompting for a password:

```bash
./Real-system/setup_ssh.sh
```

To use a specific private key:

```bash
export DDRT_SSH_KEY=~/.ssh/cloudlab_ddlrt
```

Optional deployment overrides:

```bash
export DDRT_SSH_USER=chendh
export DDRT_REMOTE_ROOT=~/DdlRT
```

## 3. Host Inventory

The default host inventory is:

```text
~/optimallrc/conf/Real-system_hosts
```

The first non-comment entry is the coordinator, the second is the client, and all remaining entries are storage hosts:

```text
10.10.1.1   node0   coordinator
10.10.1.2   node1   client
10.10.1.3   node2   storage-0
10.10.1.4   node3   storage-1
...
```

To use another inventory:

```bash
export DDRT_HOSTS_FILE=/path/to/Real-system_hosts
```

The current `(n,k,r,z)=(28,24,2,2)` experiment requires:

- one coordinator host;
- one client host;
- 28 storage hosts.

Each storage host uses:

- datanode port `17600`;
- proxy port `50405`.

## 4. Coding and Recovery Configuration

Edit:

```text
project/config/parameterConfiguration.xml
```

### 4.1 Coding Parameters

```xml
<CodeType>gLRC</CodeType>
<AppendMode>UNILRC_MODE</AppendMode>
<k>24</k>
<r>2</r>
<z>2</z>
```

The parameters mean:

- `k`: number of data blocks;
- `r`: number of global parity blocks;
- `z`: number of local parity blocks;
- total stripe width: `n = k + r + z`;
- maximum number of tolerable failures: `f_max = r + z`.

gLRC distributes the `k+r` payload blocks as evenly as possible across `z` logical groups and adds one local parity block to each group.

### 4.2 Recovery Modes

Select the execution architecture with `GlrcRepairMode`:

```xml
<GlrcRepairMode>phase1</GlrcRepairMode>
```

Available modes:

- `phase1`: one anchor proxy fetches all helpers, decodes full blocks, and writes them back; used as the centralized baseline;
- `phase2`: shards are decoded in parallel on the failed nodes' proxies, followed by proxy-to-proxy exchange;
- `pipeline`: local-first equations are executed as parallel shard pipelines;
- `hybrid`: the first `p` shards use Phase2 while the remaining shards use Pipeline concurrently.

Examples:

```xml
<GlrcRepairMode>phase2</GlrcRepairMode>
```

```xml
<GlrcRepairMode>pipeline</GlrcRepairMode>
```

```xml
<GlrcRepairMode>hybrid</GlrcRepairMode>
<GlrcHybridP>auto</GlrcHybridP>
```

`GlrcHybridP` accepts:

- `auto`: select `p` using the analytical hotspot model;
- an integer: use a fixed number of Phase2 shards.

### 4.3 Equation Selection Policies

Select the equation policy with:

```xml
<GlrcEquationPolicy>local-then-global</GlrcEquationPolicy>
```

Available policies:

- `local-then-global`: prefer local equations and add global equations when necessary;
- `local-first`: local-first policy intended for Pipeline and Hybrid recovery;
- `ilp-min-helper`: search all solvable combinations and minimize the helper-block union.

Common experiment combinations:

- Phase1 baseline: `phase1 + local-then-global`;
- Phase2 baseline: `phase2 + local-then-global`;
- Pipeline: `pipeline + local-first`;
- Hybrid: `hybrid + local-first`;
- minimum-helper comparison: any execution mode with `ilp-min-helper`.

### 4.4 Shards, Window, and Bandwidth

```xml
<BlockSize>67108864</BlockSize>
<NodeBlockBandwidthMBps>125</NodeBlockBandwidthMBps>
<GlrcShardCount>128</GlrcShardCount>
<GlrcPipelineWindow>0</GlrcPipelineWindow>
<GlrcPhase2WriteBack>true</GlrcPhase2WriteBack>
```

- `BlockSize`: block size in bytes; the current value is 64 MiB;
- `NodeBlockBandwidthMBps=125`: models independent 1 Gbps ingress and egress per host;
- `GlrcShardCount`: number of shards per block; it must divide `BlockSize`;
- `GlrcPipelineWindow=0`: allow all shards to be in flight;
- `GlrcPipelineWindow=8` or `16`: bound in-flight shards to reduce large-window contention;
- `GlrcPhase2WriteBack`: control whether Phase2 writes recovered blocks to datanodes.

Traffic between a datanode and its colocated proxy is not software-throttled. Cross-host traffic is governed by the node-bandwidth model.

## 5. Generate the Topology

After changing `k`, `r`, `z`, or the host inventory, regenerate the topology:

```bash
./Real-system/generate_cluster_config.py \
  --hosts ~/optimallrc/conf/Real-system_hosts
```

The generator updates:

- `project/config/clusterInformation.xml`;
- `DatanodeNumPerCluster`;
- `ClusterNum`;
- `CoordinatorIP`;
- `ClientIP`.

Current real-system experiments use one stripe. For a large configuration that is not divisible by `z`, ensure that the logical topology has enough nodes for the actual balanced gLRC group sizes.

## 6. Build, Deploy, and Start

### 6.1 Run Each Step Separately

Build:

```bash
bash compile.sh
```

Synchronize the repository, binaries, and configuration to every host:

```bash
./Real-system/deploy.sh
```

Stop stale processes and start the coordinator, datanodes, and proxies:

```bash
./Real-system/start.sh
```

Check process status:

```bash
./Real-system/status.sh
```

### 6.2 Complete Build-to-Run Command

```bash
bash compile.sh && \
export DDRT_SSH_KEY=~/.ssh/cloudlab_ddlrt && \
./Real-system/deploy.sh && \
./Real-system/start.sh && \
./Real-system/run_client.sh
```

Changing only the recovery mode or equation policy does not require recompilation, but the updated configuration must be deployed and all services restarted:

```bash
./Real-system/deploy.sh && \
./Real-system/start.sh
```

## 7. Run Recovery Experiments

### 7.1 Interactive Mode

```bash
./Real-system/run_client.sh
```

The client prompts for:

- the number of failed blocks per trial, `f`;
- the number of trials.

### 7.2 Random Failures

Run ten trials with three random failures per trial:

```bash
./Real-system/run_client.sh -f 3 -n 10
```

### 7.3 Deterministic Random Seed

Using the same seed across recovery modes produces the same trial failure sequence:

```bash
./Real-system/run_client.sh -f 3 -n 10 --seed 20260721
```

Equivalent environment-variable form:

```bash
GLRC_RANDOM_SEED=20260721 \
./Real-system/run_client.sh -f 3 -n 10
```

The measured-trial random stream is independent of whether warmup is enabled.

### 7.4 Fixed Failed Blocks

Repeat the same failure pattern:

```bash
./Real-system/run_client.sh \
  --failed-blocks D0,D7,G1 \
  -n 10
```

Supported block labels:

- data blocks: `D0` through `D{k-1}`;
- global parities: `G0` through `G{r-1}`;
- local parities: `L0` through `L{z-1}`;
- numeric block IDs, for example `0,7,25`.

The fixed list determines `f`, so `-f` is not required.

### 7.5 Maximum-Tolerance Failure Mode

Maximum-tolerance mode uses:

```text
f = z + r
```

Run it with:

```bash
./Real-system/run_client.sh --max-failure -n 10 --seed 20260721
```

Equivalent form:

```bash
./Real-system/run_client.sh \
  --failure-mode max \
  -n 10 \
  --seed 20260721
```

### 7.6 Disable Warmup

Phase2 and Pipeline run one unmeasured warmup recovery by default. Disable it for debugging with:

```bash
./Real-system/run_client.sh -f 3 -n 10 --no-warmup
```

## 8. Experiment Logs

`run_client.sh` displays client output and automatically saves it to:

```text
logs/client_runs/glrc_TIMESTAMP_PID.log
```

Each log includes:

- start time and command-line arguments;
- coding configuration, recovery mode, and equation policy;
- the effective random seed;
- failed blocks in every trial;
- helpers and selected equations;
- network, decode, write-back, and total recovery times;
- Hybrid `p` and critical-path breakdown;
- final statistics and process exit status.

Use another log directory:

```bash
DDRT_CLIENT_LOG_DIR=/path/to/logs \
./Real-system/run_client.sh -f 3 -n 10
```

Disable automatic client logging:

```bash
DDRT_CLIENT_LOG=0 \
./Real-system/run_client.sh -f 3 -n 10
```

Service logs are stored under `logs/` on the corresponding hosts:

- coordinator: `logs/coordinator.log`;
- proxy: `logs/proxy.log`;
- datanode: `logs/datanode.log`;
- Phase2 trace: `logs/phase2_trace.log`;
- Pipeline trace: `logs/pipeline_trace.log`.

## 9. Stop and Clean Up

Stop all services while preserving block data:

```bash
./Real-system/stop.sh
```

Stop all services and remove experiment storage:

```bash
./Real-system/stop.sh --clear-storage
```

The default storage root is:

```text
/tmp/ddlrt_storage
```

Override it with:

```bash
export DDRT_STORAGE_ROOT=/tmp/ddlrt_storage
```

Never set the storage root to `/`.

## 10. Timing Metrics

The main timing fields are:

- `setup_time`: planning and listener readiness;
- `data_plane_time`: streaming, shard decoding, and write-back wall time;
- `repair_time`: the normalized data-plane recovery time;
- `client_wall_time`: complete client-to-coordinator RPC time;
- `network_transfer_time`: mode-specific network critical path;
- `decode_time`: decoding time;
- `disk_write_time`: write work that may overlap network processing.

For Pipeline and Hybrid, compare the analytical model against `data_plane_time` or `repair_time`. Per-proxy network, decode, and write values may overlap and must not be summed to obtain total recovery time.

Hybrid additionally reports:

- `hybrid_p`;
- `phase2_network_hot`;
- `max_chain_node_egress_hot`;
- `failed_node_hot`;
- `atomic_publish`;
- `theoretical_network_critical`;
- `hotspot_residual`.

## 11. Troubleshooting

### SSH `Permission denied`

```bash
./Real-system/setup_ssh.sh
export DDRT_SSH_KEY=~/.ssh/cloudlab_ddlrt
```

Then run `deploy.sh` again.

### `Connection refused`

Check all processes:

```bash
./Real-system/status.sh
```

Then inspect `logs/proxy.log` or `logs/datanode.log` on the affected host. If later trials report `Connection refused`, a proxy may have exited during an earlier trial.

### Configuration Changes Do Not Take Effect

Deploy the updated configuration and restart all services. Rebuild first only if source code changed:

```bash
./Real-system/deploy.sh && \
./Real-system/start.sh
```

### Start a Clean Experiment

```bash
./Real-system/stop.sh --clear-storage
./Real-system/start.sh
./Real-system/run_client.sh -f 3 -n 10 --seed 20260721
```

## 12. Large Configurations

For `(n,k,r,z)=(105,96,5,4)`:

```xml
<k>96</k>
<r>5</r>
<z>4</z>
```

This configuration has:

- 105 total blocks;
- a maximum of nine simultaneous failures;
- 101 payload blocks and four local parity blocks;
- single-stripe logical group sizes of `26,26,26,27`;
- 105 storage hosts, each running one datanode and one proxy.

After changing the parameters and inventory, regenerate the topology, rebuild if needed, deploy, and restart. Phase1 sends approximately 96 helper blocks to one anchor proxy and is therefore unsuitable for large-scale performance experiments. Phase2, Pipeline, and Hybrid are the recommended distributed recovery modes.
