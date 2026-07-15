# CloudLab deployment

This directory deploys one `datanode` and one repair `proxy` on each storage
host.  The first two non-comment entries in the hosts file are reserved for
the coordinator and client; every later entry is a storage host.

For the current `(n,k,r,z)=(28,24,2,2)` experiment, the supplied inventory
maps `10.10.1.1` to coordinator, `10.10.1.2` to client, and
`10.10.1.3`–`10.10.1.30` to 28 storage pairs.  Storage hosts use
`17600` for the datanode and `50405` for the proxy.  Reusing these ports is
safe because every pair has a different IP address.

## Prepare and start

Run all commands below from node0:

```bash
# Generate project/config/{parameterConfiguration,clusterInformation}.xml.
./cloudlab/generate_cluster_config.py \
  --hosts ~/optimallrc/conf/cloudlab_hosts

# Compile before copying binaries. NASM is needed because compile.sh enables AVX2.
sudo apt install -y nasm
bash compile.sh

# Copy the same repository revision, binaries, and topology to all nodes.
./cloudlab/deploy.sh

# Start one coordinator and 28 datanode/proxy pairs.
./cloudlab/start.sh
./cloudlab/status.sh

# Run the repair client from node1.
./cloudlab/run_client.sh
```

`deploy.sh` assumes passwordless SSH from node0 using the current username.
Set `DDRT_SSH_USER` or `DDRT_REMOTE_ROOT` when the remote username or
repository directory differs:

```bash
DDRT_SSH_USER=chendh DDRT_REMOTE_ROOT=~/DdlRT ./cloudlab/deploy.sh
```

Stop processes without deleting block data:

```bash
./cloudlab/stop.sh
```

Start a completely fresh experiment, removing storage only on storage hosts:

```bash
./cloudlab/stop.sh --clear-storage
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

For a future parameter set, put at least `n + 2` entries in a hosts file,
update `k`, `r`, and `z`, then regenerate the topology before compiling and
deploying.  The generator uses `z` placement clusters for gLRC and validates
that the supplied storage-host count can hold the entire stripe.
