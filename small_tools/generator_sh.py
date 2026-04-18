import os
import socket
import netifaces

current_path = os.getcwd()
parent_path = os.path.dirname(current_path)
cluster_number = 16
datanode_number_per_cluster = 8
datanode_port_start = 17600
cluster_id_start = 0
iftest = False

proxy_ip_list = [
    ["10.10.1.3",50405],
    ["10.10.1.4",50405],
    ["10.10.1.5",50405],
    ["10.10.1.6",50405],
    ["10.10.1.7",50405],
    ["10.10.1.8",50405],
    ["10.10.1.9",50405],
    ["10.10.1.10",50405],
    ["10.10.1.11",50405],
    ["10.10.1.12",50405],
    ["10.10.1.13",50405],
    ["10.10.1.14",50405],
    ["10.10.1.15",50405],
    ["10.10.1.16",50405],
    ["10.10.1.17",50405],
    ["10.10.1.18",50405],
]
coordinator_ip = "10.10.1.2"

proxy_num = len(proxy_ip_list)

#cluster_informtion = {cluster_id:{'proxy':0.0.0.0:50005,'datanode':[[ip,port],...]},}

def get_local_ip(interface_name):
    addresses = netifaces.ifaddresses(interface_name)
    return addresses[netifaces.AF_INET][0]['addr']

def get_interface_with_ip_prefix(prefix="10.10.1"):
    interfaces = netifaces.interfaces()
    for interface in interfaces:
        try:
            addresses = netifaces.ifaddresses(interface)
            if netifaces.AF_INET in addresses:  # 检查是否有IPv4地址
                for addr in addresses[netifaces.AF_INET]:
                    ip = addr['addr']
                    if ip.startswith(prefix):  # 检查IP地址是否以指定前缀开头
                        return ip
        except Exception as e:
            print(f"Error processing interface {interface}: {e}")
    return None, None


cluster_informtion = {}
def generate_cluster_info_dict():
    for i in range(proxy_num):
        new_cluster = {}
        
        new_cluster["proxy"] = proxy_ip_list[i][0]+":"+str(proxy_ip_list[i][1])
        datanode_list = []
        for j in range(datanode_number_per_cluster):
            port = datanode_port_start + j
            if iftest:
                port = datanode_port_start + i*100 + j
            datanode_list.append([proxy_ip_list[i][0], port])
        new_cluster["datanode"] = datanode_list
        cluster_informtion[i] = new_cluster
            
def generate_run_proxy_datanode_file():
    local_ip = get_interface_with_ip_prefix(prefix="10.10.1")
    #local_ip = "0.0.0.0" # for test
    local_ip_last_segment = local_ip.split('.')[-1]
    cluster_id = int(local_ip_last_segment) - 3
    #cluster_id = 0 # for test
    file_name = parent_path + '/run_proxy_datanode.sh'
    with open(file_name, 'w') as f:
        f.write("pkill -9 run_datanode\n")
        f.write("pkill -9 run_proxy\n")
        f.write("\n")
        print("cluster_id",cluster_id)
        for each_datanode in cluster_informtion[cluster_id]["datanode"]:
            f.write("./project/cmake/build/run_datanode "+str(each_datanode[0])+":"+str(each_datanode[1])+" & \n")
        f.write("\n")
        
        f.write("sleep 5s\n")
        
        f.write("\n")
        f.write("./project/cmake/build/run_proxy "+str(cluster_informtion[cluster_id]["proxy"])+" "+" & \n")
        f.write("\n")
        
def generater_cluster_information_xml():
    file_name = parent_path + '/project/config/clusterInformation.xml'
    import xml.etree.ElementTree as ET
    root = ET.Element('clusters')
    root.text = "\n\t"
    for cluster_id in cluster_informtion.keys():
        cluster = ET.SubElement(root, 'cluster', {'id': str(cluster_id), 'proxy': cluster_informtion[cluster_id]["proxy"]})
        cluster.text = "\n\t\t"
        datanodes = ET.SubElement(cluster, 'datanodes')
        datanodes.text = "\n\t\t\t"
        for index,each_datanode in enumerate(cluster_informtion[cluster_id]["datanode"]):
            datanode = ET.SubElement(datanodes, 'datanode', {'uri': str(each_datanode[0])+":"+str(each_datanode[1])})
            #datanode.text = '\n\t\t\t'
            if index == len(cluster_informtion[cluster_id]["datanode"]) - 1:
                datanode.tail = '\n\t\t'
            else:
                datanode.tail = '\n\t\t\t'
        datanodes.tail = '\n\t'
        if cluster_id == len(cluster_informtion)-1:
            cluster.tail = '\n'
        else:
            cluster.tail = '\n\t'
    #root.tail = '\n'
    tree = ET.ElementTree(root)
    tree.write(file_name, encoding="utf-8", xml_declaration=True)
            
def cluster_generate_run_proxy_datanode_file(ip, port, i):
    file_name = parent_path + '/run_cluster_sh/' + str(i) +'/cluster_run_proxy_datanode.sh'
    with open(file_name, 'w') as f:
        f.write("pkill -9 run_datanode\n")
        f.write("pkill -9 run_proxy\n")
        f.write("\n")
        for each_datanode in cluster_informtion[0]["datanode"]:
            f.write("./project/cmake/build/run_datanode "+ip+":"+str(each_datanode[1])+" & \n")
        f.write("\n") 
        f.write("./project/cmake/build/run_proxy "+ip+":"+str(port)+" "+coordinator_ip+" & \n")   
        f.write("\n")

if __name__ == "__main__":
    generate_cluster_info_dict()
    # print(cluster_informtion)
    local_ip = get_interface_with_ip_prefix(prefix="10.10.1")
    #local_ip = "0.0.0.0" # for test
    local_ip_last_segment = local_ip.split('.')[-1]
    if int (local_ip_last_segment) >= 3: 
        generate_run_proxy_datanode_file()
    #generater_cluster_information_xml()
    #generate_run_proxy_datanode_file() # for test
    # WARNING:
    # This writes project/config/clusterInformation.xml in legacy
    # "same proxy IP + multi-port datanodes" format.
    # Keep disabled by default to avoid accidentally overwriting
    # real-physical-node deployment configs.
    if os.environ.get("GEN_LEGACY_CLUSTER_XML", "0") == "1":
        generater_cluster_information_xml()
    else:
        print("Skip writing clusterInformation.xml (set GEN_LEGACY_CLUSTER_XML=1 to enable).")
    
    # cnt = 0
    # for proxy in proxy_ip_list:
        #cluster_generate_run_proxy_datanode_file(proxy[0], proxy[1], cnt)
        #cnt += 1
    
