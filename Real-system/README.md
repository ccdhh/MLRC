# Real-system deployment

This directory deploys one `datanode` and one repair `proxy` on each storage
host.  The first two non-comment entries in the hosts file are reserved for
the coordinator and client; every later entry is a storage host.

For the current `(n,k,r,z)=(105,96,5,4)` experiment, the hosts file lists
coordinator + client + 105 storage pairs (`172.16.3.x`). Storage hosts use
`17600` for the datanode and `50405` for the proxy.  Reusing these ports is
safe because every pair has a different IP address.

## Prepare and start

Run all commands below from node0:

```bash
# Step 0: enable node-to-node SSH (required once per experiment).
./Real-system/setup_ssh.sh

# Generate project/config/{parameterConfiguration,clusterInformation}.xml.
./Real-system/generate_cluster_config.py \
  --hosts ~/optimallrc/conf/Real-system_hosts

# Compile before copying binaries. NASM is needed because compile.sh enables AVX2.
sudo apt install -y nasm
bash compile.sh

# Copy the same repository revision, binaries, and topology to all nodes.
./Real-system/deploy.sh

# Start one coordinator and 105 datanode/proxy pairs.
./Real-system/start.sh
./Real-system/status.sh

# Run the repair client from node1.
# Interactive (prompts for f and trial count):
./Real-system/run_client.sh
# Or non-interactive:
./Real-system/run_client.sh -f 3 -n 10
```


If `./Real-system/deploy.sh` fails with `Permission denied (publickey)`, run
`./Real-system/setup_ssh.sh` first. The real system does not enable inter-node SSH
by default; the setup script installs the experiment-wide `geni-get` key.

`deploy.sh` assumes passwordless SSH from node0 using the current username.
Set `DDRT_SSH_USER` or `DDRT_REMOTE_ROOT` when the remote username or
repository directory differs:

```bash
DDRT_SSH_USER=chendh DDRT_REMOTE_ROOT=~/DdlRT ./Real-system/deploy.sh
```

Stop processes without deleting block data:

```bash
./Real-system/stop.sh
```

Start a completely fresh experiment, removing storage only on storage hosts:

```bash
./Real-system/stop.sh --clear-storage
```

## Bandwidth and scaling

`NodeBlockBandwidthMBps=125` models 1 Gbps ingress and egress.  The datanode
and its paired proxy share an IP and are recognized as local, so that pair is
not software-throttled.  Cross-host traffic is capped by the existing shared
bandwidth limiters.

The start script sets `DDRT_ONE_PROXY_PER_HOST=1`.  This reuses one safe
pipeline/phase2 listener-port band per real host, rather than assigning a
different port range per proxy as the single-host simulator does.  It avoids
the simulator's port-range limit for future large `n` deployments.

## Pipeline timing

For `GlrcRepairMode=pipeline`, client output separates the following
wall-clock values:

- `setup_time`: planning, listener-port allocation, and listener readiness;
- `data_plane_time` / `repair_time`: shard streaming, decode, and write-back;
- `teardown_time`: listener cleanup after the data plane finishes;
- `client_wall_time`: the full client-to-coordinator RPC duration.

`data_plane_time` is the value to compare against the shard-pipeline model.
The legacy per-proxy read/decode/write values can overlap and are diagnostic
only; they must not be summed or compared directly with the wall-clock time.

For a future parameter set, put at least `n + 2` entries in a hosts file,
update `k`, `r`, and `z`, then regenerate the topology before compiling and
deploying.  The generator uses `z` placement clusters for gLRC and validates
that the supplied storage-host count can hold the entire stripe.
