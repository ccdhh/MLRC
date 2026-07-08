#!/usr/bin/env python3
"""Generate local gLRC clusterInformation.xml and list proxy/datanode endpoints.

Port layout must stay in sync with project/include/config.h:
  PROXY_GRPC_BASE = 50405, PROXY_GRPC_STRIDE = 2
  datanode ports: 17600 + cluster_id * 100 + node_index
"""

from __future__ import annotations

import argparse
import sys
import xml.etree.ElementTree as ET
from pathlib import Path

BASE_PROXY = 50405
PROXY_STRIDE = 2
DATANODE_PORT_BASE = 17600
DATANODE_CLUSTER_OFFSET = 100

ROOT = Path(__file__).resolve().parents[1]
DEFAULT_SYS_XML = ROOT / "project" / "config" / "parameterConfiguration.xml"
DEFAULT_CLUSTER_XML = ROOT / "project" / "config" / "clusterInformation.xml"


def read_xml_tag(path: Path, tag: str, default: str | None = None) -> str:
    if not path.is_file():
        raise FileNotFoundError(f"missing config: {path}")
    tree = ET.parse(path)
    elem = tree.getroot().find(tag)
    if elem is None or elem.text is None:
        if default is not None:
            return default
        raise ValueError(f"missing <{tag}> in {path}")
    return elem.text.strip()


def load_topology(sys_xml: Path) -> tuple[int, int]:
    cluster_num = int(read_xml_tag(sys_xml, "ClusterNum"))
    datanodes_per_cluster = int(read_xml_tag(sys_xml, "DatanodeNumPerCluster"))
    if cluster_num < 1 or datanodes_per_cluster < 1:
        raise ValueError("ClusterNum and DatanodeNumPerCluster must be positive")
    return cluster_num, datanodes_per_cluster


def datanode_port(cluster_id: int, node_index: int) -> int:
    return DATANODE_PORT_BASE + cluster_id * DATANODE_CLUSTER_OFFSET + node_index


def repair_proxy_port(cluster_id: int, node_index: int, datanodes_per_cluster: int) -> int:
    global_index = cluster_id * datanodes_per_cluster + node_index
    return BASE_PROXY + global_index * PROXY_STRIDE


def build_topology(host: str, cluster_num: int, datanodes_per_cluster: int):
    clusters = []
    proxies: list[str] = []
    datanodes: list[str] = []

    for cluster_id in range(cluster_num):
        nodes = []
        for node_index in range(datanodes_per_cluster):
            dn_port = datanode_port(cluster_id, node_index)
            px_port = repair_proxy_port(cluster_id, node_index, datanodes_per_cluster)
            dn = f"{host}:{dn_port}"
            px = f"{host}:{px_port}"
            nodes.append({"uri": dn, "proxy": px})
            proxies.append(px)
            datanodes.append(dn)
        cluster_proxy = nodes[0]["proxy"]
        clusters.append({"id": cluster_id, "proxy": cluster_proxy, "nodes": nodes})

    return clusters, proxies, datanodes


def write_cluster_xml(path: Path, clusters) -> None:
    root = ET.Element("clusters")
    root.text = "\n\t"

    for idx, cluster in enumerate(clusters):
        cluster_elem = ET.SubElement(
            root,
            "cluster",
            {"id": str(cluster["id"]), "proxy": cluster["proxy"]},
        )
        cluster_elem.text = "\n\t\t"
        datanodes_elem = ET.SubElement(cluster_elem, "datanodes")
        datanodes_elem.text = "\n\t\t\t"

        for node_idx, node in enumerate(cluster["nodes"]):
            datanode_elem = ET.SubElement(
                datanodes_elem,
                "datanode",
                {"uri": node["uri"], "proxy": node["proxy"]},
            )
            if node_idx == len(cluster["nodes"]) - 1:
                datanode_elem.tail = "\n\t\t"
            else:
                datanode_elem.tail = "\n\t\t\t"

        datanodes_elem.tail = "\n\t"
        cluster_elem.tail = "\n" if idx == len(clusters) - 1 else "\n\t"

    path.parent.mkdir(parents=True, exist_ok=True)
    ET.ElementTree(root).write(path, encoding="utf-8", xml_declaration=True)


def main() -> int:
    parser = argparse.ArgumentParser(description="Generate local gLRC cluster config.")
    parser.add_argument("--host", default="127.0.0.1", help="Host/IP for all local endpoints")
    parser.add_argument("--sys-xml", type=Path, default=DEFAULT_SYS_XML)
    parser.add_argument("--cluster-xml", type=Path, default=DEFAULT_CLUSTER_XML)
    parser.add_argument("--list-proxies", action="store_true", help="Print repair proxy ip:port lines")
    parser.add_argument("--list-datanodes", action="store_true", help="Print datanode ip:port lines")
    args = parser.parse_args()

    cluster_num, datanodes_per_cluster = load_topology(args.sys_xml)
    clusters, proxies, datanodes = build_topology(args.host, cluster_num, datanodes_per_cluster)

    if args.list_proxies:
        for px in proxies:
            print(px)
        return 0
    if args.list_datanodes:
        for dn in datanodes:
            print(dn)
        return 0

    write_cluster_xml(args.cluster_xml, clusters)
    print(
        f"Wrote {args.cluster_xml} "
        f"(clusters={cluster_num}, datanodes_per_cluster={datanodes_per_cluster}, host={args.host})"
    )
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except Exception as exc:
        print(f"generate_local_cluster.py: {exc}", file=sys.stderr)
        raise SystemExit(1)
