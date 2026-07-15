#!/usr/bin/env python3
"""Generate a one-proxy-per-host CloudLab topology for DdlRT.

The hosts file uses one entry per line:
  <IP-or-hostname> [optional labels...]
The first entry is the coordinator, the second is the client, and subsequent
entries are storage hosts.  Exactly one datanode and one repair proxy are
assigned to each storage host.
"""

from __future__ import annotations

import argparse
import math
import re
import sys
import xml.etree.ElementTree as ET
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
DEFAULT_PARAMETERS = ROOT / "project" / "config" / "parameterConfiguration.xml"
DEFAULT_CLUSTER = ROOT / "project" / "config" / "clusterInformation.xml"

DATANODE_PORT = 17600
PROXY_PORT = 50405


def hosts_from_file(path: Path) -> list[str]:
    hosts: list[str] = []
    for line in path.read_text().splitlines():
        line = line.strip()
        if not line or line.startswith("#"):
            continue
        hosts.append(line.split()[0])
    if len(hosts) < 3:
        raise ValueError("hosts file needs coordinator, client, and at least one storage host")
    return hosts


def xml_value(root: ET.Element, tag: str) -> str:
    value = root.findtext(tag)
    if value is None or not value.strip():
        raise ValueError(f"missing <{tag}> in parameter configuration")
    return value.strip()


def replace_xml_value(path: Path, tag: str, value: str) -> None:
    text = path.read_text()
    pattern = rf"(<{re.escape(tag)}>)[^<]*(</{re.escape(tag)}>)"
    result, replacements = re.subn(pattern, rf"\g<1>{value}\g<2>", text, count=1)
    if replacements != 1:
        raise ValueError(f"missing <{tag}> in {path}")
    path.write_text(result)


def build_cluster_xml(storage_hosts: list[str], cluster_count: int, output: Path) -> int:
    root = ET.Element("clusters")
    per_cluster = math.ceil(len(storage_hosts) / cluster_count)
    for cluster_id in range(cluster_count):
        start = cluster_id * per_cluster
        members = storage_hosts[start : start + per_cluster]
        if not members:
            raise ValueError(f"cluster {cluster_id} has no storage host")
        cluster = ET.SubElement(
            root,
            "cluster",
            {"id": str(cluster_id), "proxy": f"{members[0]}:{PROXY_PORT}"},
        )
        datanodes = ET.SubElement(cluster, "datanodes")
        for host in members:
            ET.SubElement(
                datanodes,
                "datanode",
                {"uri": f"{host}:{DATANODE_PORT}", "proxy": f"{host}:{PROXY_PORT}"},
            )
    ET.indent(root, space="  ")
    output.parent.mkdir(parents=True, exist_ok=True)
    ET.ElementTree(root).write(output, encoding="utf-8", xml_declaration=True)
    output.write_text(output.read_text() + "\n")
    return per_cluster


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--hosts", type=Path, required=True)
    parser.add_argument("--parameters", type=Path, default=DEFAULT_PARAMETERS)
    parser.add_argument("--cluster-config", type=Path, default=DEFAULT_CLUSTER)
    parser.add_argument(
        "--clusters",
        type=int,
        help="Placement-cluster count; defaults to z for gLRC, otherwise the current ClusterNum.",
    )
    args = parser.parse_args()

    hosts = hosts_from_file(args.hosts)
    coordinator, client, *storage = hosts
    parameter_tree = ET.parse(args.parameters)
    parameter_root = parameter_tree.getroot()
    code_type = xml_value(parameter_root, "CodeType")
    n = int(xml_value(parameter_root, "k")) + int(xml_value(parameter_root, "r"))
    if code_type != "RS":
        n += int(xml_value(parameter_root, "z"))
    if len(storage) < n:
        raise ValueError(f"need {n} storage hosts for n={n}, found {len(storage)}")
    if len(storage) > n:
        print(f"warning: using first {n} of {len(storage)} storage hosts", file=sys.stderr)
        storage = storage[:n]

    if args.clusters is not None:
        cluster_count = args.clusters
    elif code_type == "gLRC":
        cluster_count = int(xml_value(parameter_root, "z"))
    else:
        cluster_count = int(xml_value(parameter_root, "ClusterNum"))
    if cluster_count < 1 or cluster_count > len(storage):
        raise ValueError(f"invalid cluster count {cluster_count} for {len(storage)} storage hosts")

    per_cluster = build_cluster_xml(storage, cluster_count, args.cluster_config)
    replace_xml_value(args.parameters, "DatanodeNumPerCluster", str(per_cluster))
    replace_xml_value(args.parameters, "ClusterNum", str(cluster_count))
    replace_xml_value(args.parameters, "CoordinatorIP", coordinator)
    replace_xml_value(args.parameters, "ClientIP", client)

    print(f"coordinator={coordinator} client={client}")
    print(f"storage_hosts={len(storage)} clusters={cluster_count} max_hosts_per_cluster={per_cluster}")
    print(f"wrote {args.cluster_config}")
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except (OSError, ValueError, ET.ParseError) as exc:
        print(f"generate_cluster_config.py: {exc}", file=sys.stderr)
        raise SystemExit(1)
