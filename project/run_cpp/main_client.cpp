#include "client.h"
#include "toolbox.h"
#include <fstream>
#include <sys/time.h>
#include <unistd.h>
#include <sys/stat.h>
#include "config.h"
#include <iomanip>
#include <iostream>
#include <chrono>
#include <algorithm>
#include <random>
#include "unilrc_encoder.h"

int main(int argc, char **argv)
{
    char buff[256];
    getcwd(buff, 256);
    std::string cwf = std::string(argv[0]);
    std::string sys_config_path = std::string(buff) + cwf.substr(1, cwf.rfind('/') - 1) + "/../../config/parameterConfiguration.xml";
    //std::string sys_config_path = "/home/GuanTian/lql/UniLRC/project/config/parameterConfiguration.xml";
    std::cout << "Current working directory: " << sys_config_path << std::endl;

    const ECProject::Config *config = ECProject::Config::getInstance(sys_config_path);
    std::string client_ip = "10.10.1.1";
    int client_port = 44444;
    ECProject::Client client(client_ip, client_port, config->CoordinatorIP + ":" + std::to_string(config->CoordinatorPort), sys_config_path);
    std::cout << client.sayHelloToCoordinatorByGrpc("Client ID: " + client_ip + ":" + std::to_string(client_port)) << std::endl;

    std::vector<int> parameters = client.get_parameters();
    int k = parameters[0];
    int r = parameters[1];
    int z = parameters[2];
    std::string code_type;
    if(parameters[4] == 0){
        code_type = "AzureLRC";
    }
    else if(parameters[4] == 1){
        code_type = "OptimalLRC";
    }
    else if(parameters[4] == 2){
        code_type = "UniformLRC";
    }
    else if(parameters[4] == 3){
        code_type = "UniLRC";
    }
    else if(parameters[4] == 4){
        code_type = "RS";
    }
    else{
        std::cout << "Code type error" << std::endl;
        return -1;
    }
    double block_size = static_cast<double> (parameters[3]) / 1024 / 1024; //MB
    int n = k + r + z;


    
    size_t total_write_size = 4000; //MB
    int stripe_num = total_write_size / (block_size * n);
    std::cout << "Starting set stripe operation" << std::endl;
    std::chrono::high_resolution_clock::time_point set_start = std::chrono::high_resolution_clock::now();
    for(int i = 0; i < stripe_num; i++){
        client.set();
    }
    std::chrono::high_resolution_clock::time_point set_end = std::chrono::high_resolution_clock::now();
    std::cout << "Set stripe operation finished" << std::endl;
    std::cout << "Conducting experiments, please wait..." << std::endl;
    std::chrono::duration<double> set_time = std::chrono::duration_cast<std::chrono::duration<double>>(set_end - set_start);
    std::cout << "write throughput: " << (static_cast<double> (total_write_size) / set_time.count() / 1024) << "MB/s" << std::endl;
    
    std::string output_file_name = "test_"  + code_type + + "_" + std::to_string(k) + "_" + std::to_string(r) + "_" + std::to_string(z) + ".txt";
    std::ofstream output_file(output_file_name);
    if (!output_file.is_open())
    {
        std::cerr << "Error opening file: " << output_file_name << std::endl;
        return 1;
    }
    freopen(output_file_name.c_str(), "w", stdout);
    std::mt19937 rng(std::random_device{}());

    std::uniform_int_distribution<int> dist_500(0, k*stripe_num - 500);
    std::uniform_real_distribution<double> dist_double(0.0, 1.0);
    
    /*std::string trace_file_path = std::string(buff) + cwf.substr(1, cwf.rfind('/') - 1) + "/../../../trace/ibm_test_trace.csv";
    std::fstream trace_file(trace_file_path);
    std::string trace_line;
    while(std::getline(trace_file, trace_line)){
        std::string operation;
        int operation_size;
        std::istringstream iss(trace_line);
        std::getline(iss, operation, ',');
        iss >> operation_size;
        std::chrono::high_resolution_clock::time_point t1 = std::chrono::high_resolution_clock::now();
        if(operation == "GET"){
            int start_block_id = dist_500(rng);
            client.get_blocks(start_block_id, start_block_id + operation_size - 1);
        }
        else if(operation == "PUT"){
            client.sub_set(operation_size);
        }
        else{
            std::cerr << "Unknown operation: " << operation << std::endl;
            return -1;
        }
        std::chrono::high_resolution_clock::time_point t2 = std::chrono::high_resolution_clock::now();
        std::chrono::duration<double> time_span = std::chrono::duration_cast<std::chrono::duration<double>>(t2 - t1);
        std::cout << operation << " operation time: " << time_span.count() << " seconds" << std::endl;
    }*/

    /*std::string trace_file_path = std::string(buff) + cwf.substr(1, cwf.rfind('/') - 1) + "/../../../trace/ycsb_final.txt";
    std::fstream trace_file(trace_file_path);
    std::string trace_line;
    while(std::getline(trace_file, trace_line)){
        std::string operation;
        std::istringstream iss(trace_line);
        std::getline(iss, operation, ' ');
        std::chrono::high_resolution_clock::time_point t1 = std::chrono::high_resolution_clock::now();
        if(operation == "R"){
            int block_id;
            iss >> block_id;
            client.get_blocks(block_id, block_id);
        }
        else if(operation == "U"){
            client.sub_set(1);
        }
        else{
            std::cerr << "Unknown operation: " << operation << std::endl;
            return -1;
        }
        std::chrono::high_resolution_clock::time_point t2 = std::chrono::high_resolution_clock::now();
        std::chrono::duration<double> time_span = std::chrono::duration_cast<std::chrono::duration<double>>(t2 - t1);
        std::cout << operation << " operation time: " << time_span.count() << " seconds" << std::endl;
    }*/

    
    //for read test
    std::cout << "Normal read test start" << std::endl;
    std::vector<std::chrono::duration<double>> read_time_spans;
    for(int i = 0; i < 5; i++){
        size_t data_size;
        int id = i;
        std::string key = std::to_string(id);
        std::chrono::high_resolution_clock::time_point t1 = std::chrono::high_resolution_clock::now();
        std::shared_ptr<char[]> data = client.get(key, data_size);
        if(!data){
            std::cout << "Get operation failed" << std::endl;
            continue;
        }
        std::chrono::high_resolution_clock::time_point t2 = std::chrono::high_resolution_clock::now();
        std::chrono::duration<double> time_span = std::chrono::duration_cast<std::chrono::duration<double>>(t2 - t1);
        read_time_spans.push_back(time_span);
        //std::cout << "get time: " << time_span.count() << std::endl;
    }
    std::chrono::duration<double> read_total_time_span = std::accumulate(read_time_spans.begin(), read_time_spans.end(), std::chrono::duration<double>(0));
    std::cout << "Total time: " << read_total_time_span.count() << std::endl;
    std::cout << "Average time: " << read_total_time_span.count() / read_time_spans.size() << std::endl;
    std::cout << "Throughput: " << read_time_spans.size() / read_total_time_span.count() << std::endl;
    std::cout << "Speed" << static_cast<size_t>(block_size) * k / (read_total_time_span.count() / read_time_spans.size()) << "MB/s" << std::endl;
    std::chrono::duration<double> read_max_time_span = *std::max_element(read_time_spans.begin(), read_time_spans.end());
    std::chrono::duration<double> read_min_time_span = *std::min_element(read_time_spans.begin(), read_time_spans.end());
    std::cout << "Max speed: " << static_cast<size_t>(block_size) * k / read_min_time_span.count() << "MB/s" << std::endl;
    std::cout << "Min speed: " << static_cast<size_t>(block_size) * k / read_max_time_span.count() << "MB/s" << std::endl;
    std::cout << "Normal read test end" << std::endl;
    std::cout << std::endl;
    
    //for degraded read test
    std::vector<std::chrono::duration<double>> degraded_read_time_spans;
    std::cout << "Degraded read test start" << std::endl;
    for(int i = 0; i < k; i++){
        size_t data_size;
        int id = i;
        std::string key = std::to_string(id);
        std::chrono::high_resolution_clock::time_point t1 = std::chrono::high_resolution_clock::now();
        std::shared_ptr<char[]> data = client.get_degraded_read_block(0, i);
        if(!data){
            std::cout << "Degraded read operation failed" << std::endl;
            continue;
        }
        std::chrono::high_resolution_clock::time_point t2 = std::chrono::high_resolution_clock::now();
        std::chrono::duration<double> time_span = std::chrono::duration_cast<std::chrono::duration<double>>(t2 - t1);
        degraded_read_time_spans.push_back(time_span);
        //std::cout << "get time: " << time_span.count() << std::endl;
    }
    std::chrono::duration<double> degraded_read_total_time_span = std::accumulate(degraded_read_time_spans.begin(), degraded_read_time_spans.end(), std::chrono::duration<double>(0));
    std::cout << "Average time: " << degraded_read_total_time_span.count() / degraded_read_time_spans.size() << std::endl;
    std::chrono::duration<double> degraded_read_max_time_span = *std::max_element(degraded_read_time_spans.begin(), degraded_read_time_spans.end());
    std::chrono::duration<double> degraded_read_min_time_span = *std::min_element(degraded_read_time_spans.begin(), degraded_read_time_spans.end());
    std::cout << "Max time: "<< degraded_read_max_time_span.count() << std::endl;
    std::cout << "Min time: "<< degraded_read_min_time_span.count() << std::endl;
    std::cout << "Throughput: " << degraded_read_time_spans.size() / degraded_read_total_time_span.count() << std::endl;
    std::cout << "Speed" << static_cast<size_t>(block_size)  / (degraded_read_total_time_span.count() / degraded_read_time_spans.size()) << "MB/s" << std::endl;
    std::cout << "Max speed: " << static_cast<size_t>(block_size)  / degraded_read_min_time_span.count() << "MB/s" << std::endl;
    std::cout << "Min speed: " << static_cast<size_t>(block_size)  / degraded_read_max_time_span.count() << "MB/s" << std::endl;
    std::cout << "Degraded read test end" << std::endl;
    std::cout << std::endl;
    
    //for single block recovery
    /*
    std::cout << "Single block recovery test start" << std::endl;
    std::vector<std::chrono::duration<double>> block_recovery_time_spans;
    for(int i = 0; i < n; i++){
        std::chrono::high_resolution_clock::time_point t1 = std::chrono::high_resolution_clock::now();
        client.recovery(0, i);
        std::chrono::high_resolution_clock::time_point t2 = std::chrono::high_resolution_clock::now();
        std::chrono::duration<double> time_span = std::chrono::duration_cast<std::chrono::duration<double>>(t2 - t1);
        block_recovery_time_spans.push_back(time_span);
        //std::cout << "single block repair time: " << time_span.count() << std::endl;
    }
    std::chrono::duration<double> block_recovery_total_time_span = std::accumulate(block_recovery_time_spans.begin(), block_recovery_time_spans.end(), std::chrono::duration<double>(0));
    std::chrono::duration<double> block_recovery_max_time_span = *std::max_element(block_recovery_time_spans.begin(), block_recovery_time_spans.end());
    std::chrono::duration<double> block_recovery_min_time_span = *std::min_element(block_recovery_time_spans.begin(), block_recovery_time_spans.end());
    //std::cout << "Total time: " << total_time_span.count() << std::endl;
    std::cout << "Average time: " << block_recovery_total_time_span.count() / block_recovery_time_spans.size() << std::endl;
    std::cout << "Max time: "<< block_recovery_max_time_span.count() << std::endl;
    std::cout << "Min time: "<< block_recovery_min_time_span.count() << std::endl;
    std::cout << "Single block recovery test end" << std::endl;
    std::cout << std::endl;
    */
    /*
    //for full node repair
    std::cout << "Full node repair test start" << std::endl;
    int node_num = 5;
    std::vector<int> node_ids;
    while(node_ids.size() < node_num){
        int random_id = rand() % (19 * 30);
        if(std::find(node_ids.begin(), node_ids.end(), random_id) == node_ids.end()){
            node_ids.push_back(random_id);
        }
    }

    std::vector<double> full_node_recovery_speeds;
    for(int i = 0; i < node_num; i++){
        std::chrono::high_resolution_clock::time_point t1 = std::chrono::high_resolution_clock::now();
        int block_num = client.recovery_full_node(node_ids[i]);
        std::chrono::high_resolution_clock::time_point t2 = std::chrono::high_resolution_clock::now();
        std::chrono::duration<double> time_span = std::chrono::duration_cast<std::chrono::duration<double>>(t2 - t1);
        //std::cout << "full node repair time: " << time_span.count() << std::endl;
        //std::cout << "block num: " << block_num << std::endl;
        double total_size = block_num * block_size; //MB
        //std::cout << "Speed: " << total_size / time_span.count() << "MB/s" << std::endl;
        full_node_recovery_speeds.push_back(total_size / time_span.count());
    }
    std::cout << "Average speed: " << std::accumulate(full_node_recovery_speeds.begin(), full_node_recovery_speeds.end(), 0.0) / full_node_recovery_speeds.size() << "MB/s" << std::endl;
    std::cout << "Max speed: " << *std::max_element(full_node_recovery_speeds.begin(), full_node_recovery_speeds.end()) << "MB/s" << std::endl;
    std::cout << "Min speed: " << *std::min_element(full_node_recovery_speeds.begin(), full_node_recovery_speeds.end()) << "MB/s" << std::endl;
    std::cout << "Full node repair test end" << std::endl;
    std::cout << std::endl;
    //for decode test
    std::cout << "Decode test start" << std::endl;
    std::vector<double> decode_time_spans;
    for(int i = 0; i < n; i++){
        double decode_time_span;
        client.decode_test(0, i, client_ip, client_port, decode_time_span);
        decode_time_spans.push_back(decode_time_span);
    }
    std::cout << "Average decode time: " << std::endl;
    std::cout << std::accumulate(decode_time_spans.begin(), decode_time_spans.end(), 0.0) / decode_time_spans.size() << std::endl;
    std::cout << "Decode test end" << std::endl;
    std::cout << std::endl;*/
    return 0;
}
