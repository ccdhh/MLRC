# DdlRT gLRC Recovery System

DdlRT is a prototype for evaluating gLRC recovery. It contains a client, coordinator, proxies, and datanodes, and supports four recovery architectures:

- `phase1`: centralized full-block recovery at one anchor proxy;
- `phase2`: distributed shard decoding and proxy exchange;
- `pipeline`: parallel local-first recovery chains;
- `hybrid`: concurrent Phase2 prefix and Pipeline suffix recovery.

In the real-system deployment, each storage host runs one datanode and one proxy. Every host has an independent ingress and egress bandwidth budget. Configuration `cluster` entries represent logical gLRC groups, not physical racks.

## Repository Layout

- `project/`: C++ source, Protocol Buffers, configuration, and build output;
- `project/config/parameterConfiguration.xml`: coding and recovery parameters;
- `project/config/clusterInformation.xml`: generated node topology;
- `Real-system/`: deployment and experiment scripts;
- `third_party/`: bundled dependencies;
- `logs/client_runs/`: automatically saved client experiment logs.

## Environment Setup

Run all commands from the repository root:

```bash
cd ~/DdlRT
```

Install build dependencies:

```bash
sudo apt update
sudo apt install -y \
  build-essential cmake autoconf automake libtool pkg-config \
  nasm rsync python3
```

Install the bundled third-party libraries once:

```bash
bash install_third_party.sh
```

Configure passwordless SSH from the coordinator to all experiment hosts:

```bash
./Real-system/setup_ssh.sh
export DDRT_SSH_KEY=~/.ssh/cloudlab_ddlrt
```

Optional overrides:

```bash
export DDRT_SSH_USER=chendh
export DDRT_REMOTE_ROOT=~/DdlRT
export DDRT_HOSTS_FILE=/path/to/Real-system_hosts
```

## Host Inventory and Topology

The default inventory is:

```text
~/optimallrc/conf/Real-system_hosts
```

The first entry is the coordinator, the second is the client, and all remaining entries are storage hosts:

```text
10.10.1.1   node0   coordinator
10.10.1.2   node1   client
10.10.1.3   node2   storage-0
...
```

After changing the inventory or `k/r/z`, regenerate the topology:

```bash
./Real-system/generate_cluster_config.py \
  --hosts ~/optimallrc/conf/Real-system_hosts
```

The script updates `clusterInformation.xml`, coordinator/client addresses, and logical-group sizes.

## Experiment Configuration

Edit:

```text
project/config/parameterConfiguration.xml
```

### Coding Parameters

```xml
<CodeType>gLRC</CodeType>
<AppendMode>UNILRC_MODE</AppendMode>
<k>24</k>
<r>2</r>
<z>2</z>
```

The stripe width is `n = k + r + z`, and maximum fault tolerance is `r + z`.

### Recovery Mode

```xml
<GlrcRepairMode>hybrid</GlrcRepairMode>
```

Valid values are `phase1`, `phase2`, `pipeline`, and `hybrid`.

For Hybrid recovery:

```xml
<GlrcHybridP>auto</GlrcHybridP>
```

Use `auto` for analytical selection or an integer for a fixed Phase2 shard count.

### Equation Policy

```xml
<GlrcEquationPolicy>local-first</GlrcEquationPolicy>
```

Available policies:

- `local-then-global`: prefer local equations, then global equations;
- `local-first`: recommended for Pipeline and Hybrid;
- `ilp-min-helper`: minimize the helper-block union.

Typical combinations:

- Phase1/Phase2 baseline: `local-then-global`;
- Pipeline/Hybrid: `local-first`;
- helper-count comparison: `ilp-min-helper`.

### Data-Plane Parameters

```xml
<BlockSize>67108864</BlockSize>
<NodeBlockBandwidthMBps>125</NodeBlockBandwidthMBps>
<GlrcShardCount>128</GlrcShardCount>
<GlrcPipelineWindow>0</GlrcPipelineWindow>
<GlrcPhase2WriteBack>true</GlrcPhase2WriteBack>
```

`NodeBlockBandwidthMBps=125` models independent 1 Gbps ingress and egress per host. `BlockSize` must be divisible by `GlrcShardCount`. Set `GlrcPipelineWindow` to `8` or `16` if a full window causes contention.

## Build, Deploy, and Run

Run each step separately:

```bash
bash compile.sh
./Real-system/deploy.sh
./Real-system/start.sh
./Real-system/status.sh
./Real-system/run_client.sh
```

Complete build-to-run command:

```bash
bash compile.sh && \
export DDRT_SSH_KEY=~/.ssh/cloudlab_ddlrt && \
./Real-system/deploy.sh && \
./Real-system/start.sh && \
./Real-system/run_client.sh
```

Configuration-only changes do not require recompilation, but they must be deployed and services restarted:

```bash
./Real-system/deploy.sh && ./Real-system/start.sh
```

## Running Trials

Random failures:

```bash
./Real-system/run_client.sh -f 3 -n 10
```

Reproducible random sequence:

```bash
./Real-system/run_client.sh -f 3 -n 10 --seed 20260721
```

Fixed failures:

```bash
./Real-system/run_client.sh --failed-blocks D0,D7,G1 -n 10
```

Block labels may use `D`, `G`, or `L`, or numeric block IDs. The fixed list determines `f`.

Maximum-tolerance failures (`f = r + z`):

```bash
./Real-system/run_client.sh --max-failure -n 10 --seed 20260721
```

Disable Phase2/Pipeline warmup:

```bash
./Real-system/run_client.sh -f 3 -n 10 --no-warmup
```

## Logs and Cleanup

Client output is automatically saved to:

```text
logs/client_runs/glrc_TIMESTAMP_PID.log
```

Use another directory or disable logging:

```bash
DDRT_CLIENT_LOG_DIR=/path/to/logs ./Real-system/run_client.sh -f 3 -n 10
DDRT_CLIENT_LOG=0 ./Real-system/run_client.sh -f 3 -n 10
```

Service logs are stored under `logs/` on the corresponding hosts:

- `coordinator.log`;
- `proxy.log`;
- `datanode.log`;
- `phase2_trace.log`;
- `pipeline_trace.log`.

Stop services or clear storage:

```bash
./Real-system/stop.sh
./Real-system/stop.sh --clear-storage
```

The default storage root is `/tmp/ddlrt_storage`; override it with `DDRT_STORAGE_ROOT`.

## Timing

Use `data_plane_time` or `repair_time` for performance comparisons. `setup_time` covers planning and listener readiness, while `client_wall_time` covers the complete RPC.

Per-proxy network, decode, and write metrics may overlap and must not be added together. Hybrid additionally reports its selected `p`, Phase2 and Pipeline hotspots, atomic publish time, theoretical network critical path, and residual overhead.

## Troubleshooting

If deployment reports `Permission denied`:

```bash
./Real-system/setup_ssh.sh
export DDRT_SSH_KEY=~/.ssh/cloudlab_ddlrt
```

If recovery reports `Connection refused`, check whether a proxy exited in an earlier trial:

```bash
./Real-system/status.sh
```

Then inspect `logs/proxy.log` and `logs/datanode.log` on the affected host.
