#include "unilrc_encoder.h"
#include "glrc_repair_ilp.h"
#include <iostream>
#include <unordered_map>
#include <unordered_set>
#include <cstring>

extern "C" {
#ifdef ENABLE_AVX2_ASM
    void gf_vect_dot_prod_avx2(int len, int vec, unsigned char *g_tbls, unsigned char **buffs, unsigned char*dests);
    void gf_2vect_dot_prod_avx2(int len, int vec, unsigned char *g_tbls, unsigned char **buffs, unsigned char**dests);
    void gf_3vect_dot_prod_avx2(int len, int vec, unsigned char *g_tbls, unsigned char **buffs, unsigned char**dests);
    void gf_4vect_dot_prod_avx2(int len, int vec, unsigned char *g_tbls, unsigned char **buffs, unsigned char**dests);
    void gf_5vect_dot_prod_avx2(int len, int vec, unsigned char *g_tbls, unsigned char **buffs, unsigned char**dests);
    void gf_6vect_dot_prod_avx2(int len, int vec, unsigned char *g_tbls, unsigned char **buffs, unsigned char**dests);
    int xor_gen_avx(int vects, int len, void **array);
#endif
}

int ECProject::xor_avx(int vects, int len, void **array)
{
#ifdef ENABLE_AVX2_ASM
    return xor_gen_avx(vects, len, array);
#else
    if (vects <= 0 || len <= 0 || array == nullptr) return 0;
    unsigned char *dest = static_cast<unsigned char*>(array[vects - 1]);
    if (!dest) return 0;
    for (int i = 0; i < len; i++) {
        unsigned char acc = 0;
        for (int v = 0; v < vects - 1; v++) {
            unsigned char *src = static_cast<unsigned char*>(array[v]);
            acc ^= src ? src[i] : 0;
        }
        dest[i] = acc;
    }
    return 0;
#endif
}

unsigned char
ECProject::gf_inv(unsigned char a)
{
    if (a == 0)
        return 0;
    return ECProject::gff_base[255 - ECProject::gflog_base[a]];
}

void ECProject::gf_gen_local_vector(unsigned char *a, int k, int p)
{
    int i;

    for (i = 0; i < k; i++)
    {
        a[i] = ECProject::gf_inv(i ^ (k + p));
    }
}

void
ECProject::gf_gen_rs_matrix1(unsigned char *a, int m, int k)
{
        int i, j;
        unsigned char p, gen = 2;

        memset(a, 0, k * m);
        for (i = 0; i < k; i++)
                a[k * i + i] = 1;

        for (i = k; i < m; i++) {
                p = 1;
                for (j = 0; j < k; j++) {
                        a[k * i + j] = p;
                        p = gf_mul(p, gen);
                }
                gen = gf_mul(gen, 2);
        }
}

void
ECProject::gf_gen_cauchy_matrix1(unsigned char *a, int m, int k)
{
        int i, j;
        unsigned char *p;

        // Identity matrix in high position
        memset(a, 0, k * m);
        for (i = 0; i < k; i++)
                a[k * i + i] = 1;

        // For the rest choose 1/(i + j) | i != j
        p = &a[k * k];
        for (i = k; i < m; i++)
                for (j = 0; j < k; j++)
                        *p++ = gf_inv(i ^ j);
}

unsigned char
ECProject::gf_mul(unsigned char a, unsigned char b)
{
#ifndef GF_LARGE_TABLES
        int i;

        if ((a == 0) || (b == 0))
                return 0;

        return gff_base[(i = gflog_base[a] + gflog_base[b]) > 254 ? i - 255 : i];
#else
        return gf_mul_table_base[b * 256 + a];
#endif
}

unsigned char
ECProject::gf_pow(unsigned char base, unsigned int exp)
{
        unsigned char result = 1;
        while (exp > 0) {
                if (exp & 1)
                        result = gf_mul(result, base);
                base = gf_mul(base, base);
                exp >>= 1;
        }
        return result;
}

int
ECProject::gf_invert_matrix(unsigned char *in_mat, unsigned char *out_mat, const int n)
{
        int i, j, k;
        unsigned char temp;

        // Set out_mat[] to the identity matrix
        for (i = 0; i < n * n; i++) // memset(out_mat, 0, n*n)
                out_mat[i] = 0;

        for (i = 0; i < n; i++)
                out_mat[i * n + i] = 1;

        // Inverse
        for (i = 0; i < n; i++) {
                // Check for 0 in pivot element
                if (in_mat[i * n + i] == 0) {
                        // Find a row with non-zero in current column and swap
                        for (j = i + 1; j < n; j++)
                                if (in_mat[j * n + i])
                                        break;

                        if (j == n) // Couldn't find means it's singular
                                return -1;

                        for (k = 0; k < n; k++) { // Swap rows i,j
                                temp = in_mat[i * n + k];
                                in_mat[i * n + k] = in_mat[j * n + k];
                                in_mat[j * n + k] = temp;

                                temp = out_mat[i * n + k];
                                out_mat[i * n + k] = out_mat[j * n + k];
                                out_mat[j * n + k] = temp;
                        }
                }

                temp = gf_inv(in_mat[i * n + i]); // 1/pivot
                for (j = 0; j < n; j++) {         // Scale row i by 1/pivot
                        in_mat[i * n + j] = gf_mul(in_mat[i * n + j], temp);
                        out_mat[i * n + j] = gf_mul(out_mat[i * n + j], temp);
                }

                for (j = 0; j < n; j++) {
                        if (j == i)
                                continue;

                        temp = in_mat[j * n + i];
                        for (k = 0; k < n; k++) {
                                out_mat[j * n + k] ^= gf_mul(temp, out_mat[i * n + k]);
                                in_mat[j * n + k] ^= gf_mul(temp, in_mat[i * n + k]);
                        }
                }
        }
        return 0;
}

void ECProject::encode_rs(int k,int r,int z,unsigned char **data_ptrs,unsigned char **parity_ptrs,int block_size)
{
    for (int i = 0; i < r; i++) {
        memset(parity_ptrs[i], 0, block_size);
    }

    int m = k + r; 
    unsigned char *encode_matrix = new unsigned char[m * k];
    gf_gen_rs_matrix1(encode_matrix, m, k); 

    unsigned char *g_tbls = new unsigned char[k * r * 32];
    ec_init_tables(k, r, &encode_matrix[k * k], g_tbls); 

    ec_encode_data_avx2(block_size, k, r, g_tbls, data_ptrs, parity_ptrs);

    delete[] encode_matrix;
    delete[] g_tbls;
}
void ECProject::partial_encode_rs(int k,int r,int z,int data_block_num,unsigned char **data_ptrs,unsigned char **parity_ptrs,int block_size)
{
    for (int i = 0; i < r; i++) {
        memset(parity_ptrs[i], 0, block_size);
    }
    int m = k + r; 
    unsigned char *encode_matrix = new unsigned char[m * k];
    gf_gen_rs_matrix1(encode_matrix, m, k);

    unsigned char *sub_matrix = new unsigned char[r * data_block_num];
    for (int i = 0; i < r; i++) {
        memcpy(sub_matrix + i * data_block_num,       
               encode_matrix + (k + i) * k,        
               data_block_num);                     
    }

    unsigned char *g_tbls = new unsigned char[data_block_num * r * 32];
    ec_init_tables(data_block_num, r, sub_matrix, g_tbls);

    ec_encode_data_avx2(block_size,
                        data_block_num,  
                        r,              
                        g_tbls,
                        data_ptrs,
                        parity_ptrs);

    delete[] encode_matrix;
    delete[] sub_matrix;
    delete[] g_tbls;
}

void ECProject::encode_unilrc(int k, int r, int z, unsigned char **data_ptrs, unsigned char **parity_ptrs, int block_size)
{
    for(int i = 0; i < r + z; i++){
        memset(parity_ptrs[i], 0, block_size);
    }
    int m = k + r;
    unsigned char *encode_matrix = new unsigned char[(m + z) * k];
    gen_unilrc_matrix(encode_matrix, k, r, z);

    unsigned char *g_tbls = new unsigned char[k * (r + z) * 32];
    ec_init_tables(k, r + z, &encode_matrix[k * k], g_tbls);
    ec_encode_data_avx2(block_size, k, r + z, g_tbls, data_ptrs, parity_ptrs);

    delete[] encode_matrix;
    delete[] g_tbls;
}

void ECProject::partial_encode_unilrc(int k, int r, int z, int data_block_num, unsigned char **data_ptrs, unsigned char **parity_ptrs, int block_size)
{
    for(int i = 0; i < r + z; i++){
        memset(parity_ptrs[i], 0, block_size);
    }
    int m = k + r;
    unsigned char *encode_matrix = new unsigned char[(m + z) * k];
    gen_unilrc_matrix(encode_matrix, k, r, z);

    unsigned char *sub_matrix = new unsigned char[(r + z) * data_block_num];
    for (int i = 0; i < r + z; i++) {
        memcpy(sub_matrix + i * data_block_num, 
               encode_matrix + (k + i) * k,      
               data_block_num);                   
    }

    unsigned char *g_tbls = new unsigned char[data_block_num * (r + z) * 32];
    ec_init_tables(data_block_num, r + z, sub_matrix, g_tbls);

    ec_encode_data_avx2(block_size, 
                        data_block_num,  
                        r + z,           
                        g_tbls, 
                        data_ptrs,       
                        parity_ptrs);

    delete[] encode_matrix;
    delete[] sub_matrix;
    delete[] g_tbls;
}

void ECProject::encode_azure_lrc(int k, int r, int z, unsigned char **data_ptrs, unsigned char **parity_ptrs, int block_size)
{
    for(int i = 0; i < r + z; i++){
        memset(parity_ptrs[i], 0, block_size);
    }
    int m = k + r;
    unsigned char *encode_matrix = new unsigned char[(m + z)* k];
    gen_azure_lrc_matrix(encode_matrix, k, r, z);

    unsigned char *g_tbls = new unsigned char[k * (r + z) * 32];
    ec_init_tables(k, r + z, &encode_matrix[k * k], g_tbls);
    ec_encode_data_avx2(block_size, k, r + z, g_tbls, data_ptrs, parity_ptrs);

    delete[] encode_matrix;
    delete[] g_tbls;
}

void ECProject::partial_encode_azure_lrc(int k, int r, int z, int data_block_num, unsigned char **data_ptrs, unsigned char **parity_ptrs, int block_size)
{
    for(int i = 0; i < r + z; i++){
        memset(parity_ptrs[i], 0, block_size);
    }
    int m = k + r;
    unsigned char *encode_matrix = new unsigned char[(m + z)* k];
    gen_azure_lrc_matrix(encode_matrix, k, r, z);

    unsigned char *sub_matrix = new unsigned char[(r + z) * data_block_num];
    for (int i = 0; i < r + z; i++) {
        memcpy(sub_matrix + i * data_block_num, 
               encode_matrix + (k + i) * k,      
               data_block_num);                   
    }

    unsigned char *g_tbls = new unsigned char[data_block_num * (r + z) * 32];
    ec_init_tables(data_block_num, r + z, sub_matrix, g_tbls);

    ec_encode_data_avx2(block_size, 
                        data_block_num,  
                        r + z,           
                        g_tbls, 
                        data_ptrs,       
                        parity_ptrs);

    delete[] encode_matrix;
    delete[] sub_matrix;
    delete[] g_tbls;
}

void ECProject::encode_optimal_lrc(int k, int r, int z, unsigned char **data_ptrs, unsigned char **parity_ptrs, int block_size)
{
    for(int i = 0; i < r + z; i++){
        memset(parity_ptrs[i], 0, block_size);
    }
    int m = k + r;
    unsigned char *encode_matrix = new unsigned char[(m + z) * k];
    gen_optimal_lrc_matrix(encode_matrix, k, r, z);

    unsigned char *g_tbls = new unsigned char[k * (r + z)* 32];
    ec_init_tables(k, r + z, &encode_matrix[k * k], g_tbls);
    ec_encode_data_avx2(block_size, k, r + z, g_tbls, data_ptrs, parity_ptrs);
    delete[] encode_matrix;
    delete[] g_tbls;
}

void ECProject::partial_encode_optimal_lrc(int k, int r, int z, int data_block_num, unsigned char **data_ptrs, unsigned char **parity_ptrs, int block_size)
{
    for(int i = 0; i < r + z; i++){
        memset(parity_ptrs[i], 0, block_size);
    }
    int m = k + r;
    unsigned char *encode_matrix = new unsigned char[(m + z) * k];
    gen_optimal_lrc_matrix(encode_matrix, k, r, z);

    unsigned char *sub_matrix = new unsigned char[(r + z) * data_block_num];
    for (int i = 0; i < r + z; i++) {
        memcpy(sub_matrix + i * data_block_num, 
               encode_matrix + (k + i) * k,      
               data_block_num);                   
    }

    unsigned char *g_tbls = new unsigned char[data_block_num * (r + z) * 32];
    ec_init_tables(data_block_num, r + z, sub_matrix, g_tbls);

    ec_encode_data_avx2(block_size, 
                        data_block_num,  
                        r + z,           
                        g_tbls, 
                        data_ptrs,       
                        parity_ptrs);

    delete[] encode_matrix;
    delete[] sub_matrix;
    delete[] g_tbls;
}

void ECProject::encode_uniform_lrc(int k, int r, int z, unsigned char **data_ptrs, unsigned char **parity_ptrs, int block_size)
{
    for(int i = 0; i < r + z; i++){
        memset(parity_ptrs[i], 0, block_size);
    }
    int m = k + r;
    unsigned char *encode_matrix = new unsigned char[(m + z) * k];
    gen_uniform_lrc_matrix(encode_matrix, k, r, z);

    unsigned char *g_tbls = new unsigned char[k * (r + z)* 32];
    ec_init_tables(k, r + z, &encode_matrix[k * k], g_tbls);
    ec_encode_data_avx2(block_size, k, r + z, g_tbls, data_ptrs, parity_ptrs);
    delete[] encode_matrix;
    delete[] g_tbls;
}

void ECProject::partial_encode_uniform_lrc(int k, int r, int z, int data_block_num, unsigned char **data_ptrs, unsigned char **parity_ptrs, int block_size)
{
    for(int i = 0; i < r + z; i++){
        memset(parity_ptrs[i], 0, block_size);
    }
    int m = k + r;
    unsigned char *encode_matrix = new unsigned char[(m + z) * k];
    gen_uniform_lrc_matrix(encode_matrix, k, r, z);

    unsigned char *sub_matrix = new unsigned char[(r + z) * data_block_num];
    for (int i = 0; i < r + z; i++) {
        memcpy(sub_matrix + i * data_block_num, 
               encode_matrix + (k + i) * k,      
               data_block_num);                   
    }

    unsigned char *g_tbls = new unsigned char[data_block_num * (r + z) * 32];
    ec_init_tables(data_block_num, r + z, sub_matrix, g_tbls);

    ec_encode_data_avx2(block_size, 
                        data_block_num,  
                        r + z,           
                        g_tbls, 
                        data_ptrs,       
                        parity_ptrs);

    delete[] encode_matrix;
    delete[] sub_matrix;
    delete[] g_tbls;
    
}

// ===== Matrix generation helpers implementations =====
void ECProject::gen_unilrc_matrix(unsigned char *encode_matrix, int k, int r, int z)
{
    int m = k + r;
    memset(encode_matrix, 0, (m + z) * k);
    gf_gen_rs_matrix1(encode_matrix, m, k);
    for(int i = 0; i < k; i++){
        int row = i / (k / z);
        encode_matrix[(m + row) * k + i] = 1;
    }
    for(int i = 0; i < z; i++){
        for(int j = 0; j < k; j++){
            for(int l = 0; l < r / z; l++){
                encode_matrix[(m + i) * k + j] ^= encode_matrix[(k + i * r / z + l) * k + j];
            }
        }
    }
}

void ECProject::gen_azure_lrc_matrix(unsigned char *encode_matrix, int k, int r, int z)
{
    int m = k + r;
    memset(encode_matrix, 0, (m + z) * k);
    gf_gen_rs_matrix1(encode_matrix, m, k);
    for(int i = 0; i < k; i++){
        int row = i / (k / z);
        encode_matrix[(m + row) * k + i] = 1;
    }
}

void ECProject::gen_optimal_lrc_matrix(unsigned char *encode_matrix, int k, int r, int z)
{
    int m = k + r;
    memset(encode_matrix, 0, (m + z) * k);
    gf_gen_cauchy_matrix1(encode_matrix, m, k);
    unsigned char *local_vector = new unsigned char[k];
    gf_gen_local_vector(local_vector, k, r);
    for(int i = 0; i < k; i++){
        int row = i / (k / z);
        encode_matrix[(m + row) * k + i] = local_vector[i];
    }
    for(int i = 0; i < z; i++){
        for(int j = 0; j < k; j++){
            for(int l = 0; l < r; l++){
                encode_matrix[(m + i) * k + j] ^= encode_matrix[(k + l) * k + j];
            }
        }
    }
    delete[] local_vector;
}

void ECProject::gen_uniform_lrc_matrix(unsigned char *encode_matrix, int k, int r, int z)
{
    // UniformLRC and the large-group gLRC variant use the same algebraic
    // code. They differ only in placement/recovery execution (UniformLRC may
    // further split a large group into rack-aware fine groups).
    gen_glrc_matrix(encode_matrix, k, r, z);
}


void ECProject::decode_unilrc(const int k, const int r, const int z, const int block_num,
                              const std::vector<int> *block_indexes, unsigned char **block_ptrs, unsigned char *res_ptr, int block_size)
{
    memset(res_ptr, 0, block_size);
    unsigned char *vect_ptrs[block_num + 1];
    for(int i = 0; i < block_num; i++){
        vect_ptrs[i] = block_ptrs[i];
    }
    vect_ptrs[block_num] = res_ptr;
    xor_avx(block_num + 1, block_size, (void **)vect_ptrs);
}

void ECProject::decode_azure_lrc(const int k, const int r, const int z, const int block_num,
                                 const std::vector<int> *block_indexes, unsigned char **block_ptrs, unsigned char *res_ptr, int block_size,
                                 int failed_block_id)
{
    memset(res_ptr, 0, block_size);
    if (failed_block_id < k || failed_block_id >= k + r){
        unsigned char *vect_ptrs[block_num + 1];
        for(int i = 0; i < block_num; i++){
            vect_ptrs[i] = block_ptrs[i];
        }
        vect_ptrs[block_num] = res_ptr;
        xor_avx(block_num + 1, block_size, (void **)vect_ptrs);
    }
    else
    {
        int m = k + r;
        unsigned char *encode_matrix = new unsigned char[m * k];
        memset(encode_matrix, 0,  m * k);
        gf_gen_rs_matrix1(encode_matrix, m, k);
        unsigned char *decode_matrix = new unsigned char[k * k];
        memset(decode_matrix, 0, k * k);
        unsigned char *temp_matrix = new unsigned char[k * k];
        memset(temp_matrix, 0, k * k);
        int used_row[k];
        std::unordered_map<int, int> idx_to_row;
        for(int i = k / z, j = 0; j < k && i < k + r; i++){
            if(i != failed_block_id){
                used_row[j] = i;
                idx_to_row[i] = j;
                j++;
            }
        }
        for(int i = 0; i < k; i++){
            for(int j = 0; j < k; j++){
                temp_matrix[i * k + j] = encode_matrix[used_row[i] * k + j];
            }
        }
        unsigned char *invert_matrix = new unsigned char[k * k];
        gf_invert_matrix(temp_matrix, invert_matrix, k);
        unsigned char * vect_all = new unsigned char[k];
        gf_mul_vect_matrix(encode_matrix + failed_block_id * k, invert_matrix, vect_all, k);
        unsigned char *decode_vector = new unsigned char[block_num];
        for(int i = 0; i < block_num; i++){
            decode_vector[i] = vect_all[idx_to_row[block_indexes->at(i)]];
        }
        unsigned char *g_tbls = new unsigned char[block_num * 32];
        ec_init_tables(block_num, 1, decode_vector, g_tbls);
        unsigned char **res_ptr_ptr = new unsigned char *[1];
        res_ptr_ptr[0] = res_ptr;
        ec_encode_data_avx2(block_size, block_num, 1, g_tbls, block_ptrs, res_ptr_ptr);
        delete[] encode_matrix;
        delete[] decode_matrix;
        delete[] temp_matrix;
        delete[] invert_matrix;
        delete[] vect_all;
        delete[] decode_vector;
        delete[] g_tbls;
        delete[] res_ptr_ptr;
    }
}

void ECProject::decode_optimal_lrc(const int k, const int r, const int z, const int block_num,
                                   const std::vector<int> *block_indexes, unsigned char **block_ptrs, unsigned char *res_ptr, int block_size, int failed_block_id)
{
    memset(res_ptr, 0, block_size);
    unsigned char *local_vector;
    local_vector = new unsigned char[k];
    gf_gen_local_vector(local_vector, k, r);
    unsigned char *decode_vector = new unsigned char[block_num];
    if(block_indexes->at(0) >= k){
        for(int i = 0; i < block_num; i++){
            decode_vector[i] = 1;
        }
    }
    else{
        for(int i = 0; i < block_num; i++){
            decode_vector[i] = local_vector[block_indexes->at(i)];
        }
    }
    if(failed_block_id < k){
        unsigned char factor = gf_inv(local_vector[failed_block_id]);
        for(int i = 0; i < block_num; i++){
            decode_vector[i] = gf_mul(decode_vector[i], factor);
        }
    }

    unsigned char *g_tbls = new unsigned char[block_num * 32];
    unsigned char **res_ptr_ptr = new unsigned char *[1];
    res_ptr_ptr[0] = res_ptr;
    ec_init_tables(block_num, 1, decode_vector, g_tbls);
    ec_encode_data_avx2(block_size, block_num, 1, g_tbls, block_ptrs, res_ptr_ptr);
    
    delete[] local_vector;
    delete[] decode_vector;
    delete[] g_tbls;
    delete[] res_ptr_ptr;
}
void ECProject::decode_uniform_lrc(const int k, const int r, const int z, const int block_num,
                                   const std::vector<int> *block_indexes, unsigned char **block_ptrs, unsigned char *res_ptr, int block_size, int failed_block_id)
{
    memset(res_ptr, 0, block_size);
    unsigned char *local_vector;
    local_vector = new unsigned char[k];
    gf_gen_local_vector(local_vector, k, r);
    unsigned char *decode_vector = new unsigned char[block_num];

    for(int i = 0; i < block_num; i++){
        if(block_indexes->at(i) < k){
            decode_vector[i] = local_vector[block_indexes->at(i)];
        }
        else{
            decode_vector[i] = 1;
        }
    }
    if(failed_block_id < k){
        unsigned char factor = gf_inv(local_vector[failed_block_id]);
        for(int i = 0; i < block_num; i++){
            decode_vector[i] = gf_mul(decode_vector[i], factor);
        }
    }

    unsigned char *g_tbls = new unsigned char[block_num * 32];
    unsigned char **res_ptr_ptr = new unsigned char *[1];
    res_ptr_ptr[0] = res_ptr;
    ec_init_tables(block_num, 1, decode_vector, g_tbls);
    ec_encode_data_avx2(block_size, block_num, 1, g_tbls, block_ptrs, res_ptr_ptr);

    delete[] local_vector;
    delete[] decode_vector;
    delete[] g_tbls;
    delete[] res_ptr_ptr;
}

void
ECProject::ec_encode_data_avx2(int len, int k, int rows, unsigned char *g_tbls, unsigned char **data,
                    unsigned char **coding)
{

#ifndef ENABLE_AVX2_ASM
        // Portable fallback: no AVX2 assembly available/allowed
        ec_encode_data_base(len, k, rows, g_tbls, data, coding);
        return;
#else
        if (len < 32) {
                ec_encode_data_base(len, k, rows, g_tbls, data, coding);
                return;
        }

        while (rows >= 6) {
                gf_6vect_dot_prod_avx2(len, k, g_tbls, data, coding);
                g_tbls += 6 * k * 32;
                coding += 6;
                rows -= 6;
        }
        switch (rows) {
        case 5:
                gf_5vect_dot_prod_avx2(len, k, g_tbls, data, coding);
                break;
        case 4:
                gf_4vect_dot_prod_avx2(len, k, g_tbls, data, coding);
                break;
        case 3:
                gf_3vect_dot_prod_avx2(len, k, g_tbls, data, coding);
                break;
        case 2:
                gf_2vect_dot_prod_avx2(len, k, g_tbls, data, coding);
                break;
        case 1:
                gf_vect_dot_prod_avx2(len, k, g_tbls, data, *coding);
                break;
        case 0:
                break;
        }
#endif
}

void
ECProject::ec_encode_data_base(int len, int srcs, int dests, unsigned char *v, unsigned char **src,
                    unsigned char **dest)
{
        int i, j, l;
        unsigned char s;

        for (l = 0; l < dests; l++) {
                for (i = 0; i < len; i++) {
                        s = 0;
                        for (j = 0; j < srcs; j++)
                                s ^= gf_mul(src[j][i], v[j * 32 + l * srcs * 32 + 1]);

                        dest[l][i] = s;
                }
        }
}

void
ECProject::encode_data(int len, int k, int rows, unsigned char *matrix, unsigned char **data,
                    unsigned char **coding)
{
        unsigned char *g_tbls = new unsigned char[k * rows * 32];

        ec_init_tables(k, rows, matrix, g_tbls);
        ec_encode_data_avx2(len, k, rows, g_tbls, data, coding);
        delete[] g_tbls;
}

void
ECProject::ec_init_tables(int k, int rows, unsigned char *a, unsigned char *g_tbls)
{
        int i, j;

        for (i = 0; i < rows; i++) {
                for (j = 0; j < k; j++) {
                        gf_vect_mul_init(*a++, g_tbls);
                        g_tbls += 32;
                }
        }
}

void
ECProject::gf_vect_mul_init(unsigned char c, unsigned char *tbl)
{
        unsigned char c2 = (c << 1) ^ ((c & 0x80) ? 0x1d : 0);   // Mult by GF{2}
        unsigned char c4 = (c2 << 1) ^ ((c2 & 0x80) ? 0x1d : 0); // Mult by GF{2}
        unsigned char c8 = (c4 << 1) ^ ((c4 & 0x80) ? 0x1d : 0); // Mult by GF{2}

#if (__WORDSIZE == 64 || _WIN64 || __x86_64__) && (__BYTE_ORDER__ == __ORDER_LITTLE_ENDIAN__)
        unsigned long long v1, v2, v4, v8, *t;
        unsigned long long v10, v20, v40, v80;
        unsigned char c17, c18, c20, c24;

        t = (unsigned long long *) tbl;

        v1 = c * 0x0100010001000100ull;
        v2 = c2 * 0x0101000001010000ull;
        v4 = c4 * 0x0101010100000000ull;
        v8 = c8 * 0x0101010101010101ull;

        v4 = v1 ^ v2 ^ v4;
        t[0] = v4;
        t[1] = v8 ^ v4;

        c17 = (c8 << 1) ^ ((c8 & 0x80) ? 0x1d : 0);   // Mult by GF{2}
        c18 = (c17 << 1) ^ ((c17 & 0x80) ? 0x1d : 0); // Mult by GF{2}
        c20 = (c18 << 1) ^ ((c18 & 0x80) ? 0x1d : 0); // Mult by GF{2}
        c24 = (c20 << 1) ^ ((c20 & 0x80) ? 0x1d : 0); // Mult by GF{2}

        v10 = c17 * 0x0100010001000100ull;
        v20 = c18 * 0x0101000001010000ull;
        v40 = c20 * 0x0101010100000000ull;
        v80 = c24 * 0x0101010101010101ull;

        v40 = v10 ^ v20 ^ v40;
        t[2] = v40;
        t[3] = v80 ^ v40;

#else // 32-bit or other
        unsigned char c3, c5, c6, c7, c9, c10, c11, c12, c13, c14, c15;
        unsigned char c17, c18, c19, c20, c21, c22, c23, c24, c25, c26, c27, c28, c29, c30, c31;

        c3 = c2 ^ c;
        c5 = c4 ^ c;
        c6 = c4 ^ c2;
        c7 = c4 ^ c3;

        c9 = c8 ^ c;
        c10 = c8 ^ c2;
        c11 = c8 ^ c3;
        c12 = c8 ^ c4;
        c13 = c8 ^ c5;
        c14 = c8 ^ c6;
        c15 = c8 ^ c7;

        tbl[0] = 0;
        tbl[1] = c;
        tbl[2] = c2;
        tbl[3] = c3;
        tbl[4] = c4;
        tbl[5] = c5;
        tbl[6] = c6;
        tbl[7] = c7;
        tbl[8] = c8;
        tbl[9] = c9;
        tbl[10] = c10;
        tbl[11] = c11;
        tbl[12] = c12;
        tbl[13] = c13;
        tbl[14] = c14;
        tbl[15] = c15;

        c17 = (c8 << 1) ^ ((c8 & 0x80) ? 0x1d : 0);   // Mult by GF{2}
        c18 = (c17 << 1) ^ ((c17 & 0x80) ? 0x1d : 0); // Mult by GF{2}
        c19 = c18 ^ c17;
        c20 = (c18 << 1) ^ ((c18 & 0x80) ? 0x1d : 0); // Mult by GF{2}
        c21 = c20 ^ c17;
        c22 = c20 ^ c18;
        c23 = c20 ^ c19;
        c24 = (c20 << 1) ^ ((c20 & 0x80) ? 0x1d : 0); // Mult by GF{2}
        c25 = c24 ^ c17;
        c26 = c24 ^ c18;
        c27 = c24 ^ c19;
        c28 = c24 ^ c20;
        c29 = c24 ^ c21;
        c30 = c24 ^ c22;
        c31 = c24 ^ c23;

        tbl[16] = 0;
        tbl[17] = c17;
        tbl[18] = c18;
        tbl[19] = c19;
        tbl[20] = c20;
        tbl[21] = c21;
        tbl[22] = c22;
        tbl[23] = c23;
        tbl[24] = c24;
        tbl[25] = c25;
        tbl[26] = c26;
        tbl[27] = c27;
        tbl[28] = c28;
        tbl[29] = c29;
        tbl[30] = c30;
        tbl[31] = c31;

#endif //__WORDSIZE == 64 || _WIN64 || __x86_64__
}

void
ECProject::gf_mul_vect_matrix(unsigned char* vect, unsigned char* matrix, unsigned char *dest, int k){
    for(int i = 0; i < k; i++){
        dest[i] = 0;
        for(int j = 0; j < k; j++){
            dest[i] ^= gf_mul(vect[j], matrix[j * k + i]);
        }
    }
}

bool
ECProject::get_multi_decode_plan(int k, int r, int z, std::string code_type, const std::vector<int> failed_block_indexes, std::vector<int> &decode_block_indexes, std::vector<std::vector<int>> &decode_factors)
{
    int m = k + r;
    int nrows = k + r + z;
    unsigned char gen_matrix[k * (k + r + z)];
    memset(gen_matrix, 0, k * (k + r + z));
    if(code_type == "UniLRC"){
        gen_unilrc_matrix(gen_matrix, k, r, z);
    }
    else if(code_type == "AzureLRC"){
        gen_azure_lrc_matrix(gen_matrix, k, r, z);
    }
    else if(code_type == "OptimalLRC"){
        gen_optimal_lrc_matrix(gen_matrix, k, r, z);
    }
    else if(code_type == "gLRC"){
        gen_glrc_matrix(gen_matrix, k, r, z);
    }
    else if(code_type == "UniformLRC"){
        gen_uniform_lrc_matrix(gen_matrix, k, r, z);
    }
    else{
        std::cerr << "Error: Unsupported code type " << code_type << std::endl;
        return false;
    }

    // build failed set
    std::unordered_map<int, bool> failed_map;
    for (int idx : failed_block_indexes) failed_map[idx] = true;

    // Prefer first m rows (global rows). Collect candidate rows (non-failed).
    std::vector<int> candidates;
    for (int i = 0; i < m; i++) {
        if (!failed_map.count(i)) candidates.push_back(i);
    }
    // If not enough, append remaining non-failed rows (local parity rows)
    if ((int)candidates.size() < k) {
        for (int i = m; i < nrows; i++) {
            if (!failed_map.count(i)) candidates.push_back(i);
            if ((int)candidates.size() >= k) break;
        }
    }

    // If still fewer than k rows, cannot form left-inverse
    if ((int)candidates.size() < k) {
        decode_block_indexes.clear();
        decode_factors.clear();
        return false;
    }

    // Find k linearly independent rows among candidates using Gaussian elimination (GF)
    int M = (int)candidates.size();
    unsigned char *mat = new unsigned char[M * k];
    // copy candidate rows into mat (row-major: M x k)
    for (int i = 0; i < M; i++) {
        int gro = candidates[i];
        for (int c = 0; c < k; c++) mat[i * k + c] = gen_matrix[gro * k + c];
    }

    int cur = 0; // current pivot row index in mat
    for (int col = 0; col < k && cur < M; col++) {
        // find row with non-zero in this column
        int sel = -1;
        for (int row = cur; row < M; row++) {
            if (mat[row * k + col] != 0) { sel = row; break; }
        }
        if (sel == -1) continue; // no pivot in this column

        // swap sel and cur (both in mat and candidates)
        if (sel != cur) {
            for (int c = 0; c < k; c++) {
                unsigned char tmp = mat[cur * k + c];
                mat[cur * k + c] = mat[sel * k + c];
                mat[sel * k + c] = tmp;
            }
            int tmpidx = candidates[cur];
            candidates[cur] = candidates[sel];
            candidates[sel] = tmpidx;
        }

        // normalize pivot row: make pivot == 1
        unsigned char pivot = mat[cur * k + col];
        unsigned char inv_pivot = gf_inv(pivot);
        for (int c = col; c < k; c++) mat[cur * k + c] = gf_mul(mat[cur * k + c], inv_pivot);

        // eliminate this column in all other rows
        for (int row = 0; row < M; row++) {
            if (row == cur) continue;
            unsigned char factor = mat[row * k + col];
            if (factor == 0) continue;
            for (int c = col; c < k; c++) {
                mat[row * k + c] ^= gf_mul(factor, mat[cur * k + c]);
            }
        }

        cur++;
    }

    // check if we found k independent rows (need cur >= k)
    if (cur < k) {
        delete[] mat;
        decode_block_indexes.clear();
        decode_factors.clear();
        return false;
    }

    // selected rows are candidates[0..k-1]
    std::vector<int> chosen(k);
    for (int i = 0; i < k; i++) chosen[i] = candidates[i];

    // build tempM from chosen rows (k x k) and compute inverse
    unsigned char *tempM = new unsigned char[k * k];
    for (int row = 0; row < k; row++) {
        int global_row = chosen[row];
        for (int col = 0; col < k; col++) {
            tempM[row * k + col] = gen_matrix[global_row * k + col];
        }
    }
    unsigned char *invM = new unsigned char[k * k];
    if (gf_invert_matrix(tempM, invM, k) != 0) {
        // unexpected: invert failed though rows are independent; return empty
        delete[] mat;
        delete[] tempM;
        delete[] invM;
        decode_block_indexes.clear();
        decode_factors.clear();
        return false;
    }

    // decode_block_indexes = chosen (sources)
    decode_block_indexes = chosen;

    // For each failed block, compute coefficients c = G_row_failed * invM
    decode_factors.clear();
    for (int fidx : failed_block_indexes) {
        std::vector<int> factors(k);
        unsigned char *coeff = new unsigned char[k];
        gf_mul_vect_matrix(gen_matrix + fidx * k, invM, coeff, k);
        for (int i = 0; i < k; i++) factors[i] = (int)coeff[i];
        decode_factors.push_back(std::move(factors));
        delete[] coeff;
    }

    delete[] mat;
    delete[] tempM;
    delete[] invM;
    // done
    return true;
}

void ECProject::encode_glrc(int k, int r, int z, unsigned char **data_ptrs, unsigned char **parity_ptrs, int block_size)
{
    for (int i = 0; i < r + z; i++)
        memset(parity_ptrs[i], 0, block_size);
    int m = k + r;
    unsigned char *encode_matrix = new unsigned char[(m + z) * k];
    gen_glrc_matrix(encode_matrix, k, r, z);

    unsigned char *g_tbls = new unsigned char[k * (r + z) * 32];
    ec_init_tables(k, r + z, &encode_matrix[k * k], g_tbls);
    ec_encode_data_avx2(block_size, k, r + z, g_tbls, data_ptrs, parity_ptrs);
    delete[] encode_matrix;
    delete[] g_tbls;
}

void ECProject::partial_encode_glrc(int k, int r, int z, int data_block_num, unsigned char **data_ptrs, unsigned char **parity_ptrs, int block_size)
{
    for (int i = 0; i < r + z; i++)
        memset(parity_ptrs[i], 0, block_size);
    int m = k + r;
    unsigned char *encode_matrix = new unsigned char[(m + z) * k];
    gen_glrc_matrix(encode_matrix, k, r, z);

    unsigned char *sub_matrix = new unsigned char[(r + z) * data_block_num];
    for (int i = 0; i < r + z; i++)
        memcpy(sub_matrix + i * data_block_num, encode_matrix + (k + i) * k, data_block_num);

    unsigned char *g_tbls = new unsigned char[data_block_num * (r + z) * 32];
    ec_init_tables(data_block_num, r + z, sub_matrix, g_tbls);
    ec_encode_data_avx2(block_size, data_block_num, r + z, g_tbls, data_ptrs, parity_ptrs);

    delete[] encode_matrix;
    delete[] sub_matrix;
    delete[] g_tbls;
}

int ECProject::glrc_payload_blocks_in_group(int group_id, int k, int r, int z)
{
    if (z <= 0 || group_id < 0 || group_id >= z)
        return 0;
    const int total_payload = k + r;
    const int base = total_payload / z;
    const int larger_group_count = total_payload % z;
    const int first_larger_group = z - larger_group_count;
    return base + (larger_group_count > 0 && group_id >= first_larger_group ? 1 : 0);
}

int ECProject::glrc_payload_group_id(int payload_block_index, int k, int r, int z)
{
    const int total_payload = k + r;
    if (z <= 1 || total_payload <= 0)
        return 0;
    if (payload_block_index < 0)
        return 0;
    if (payload_block_index >= total_payload)
        return z - 1;

    const int base = total_payload / z;
    const int larger_group_count = total_payload % z;
    const int smaller_group_count = z - larger_group_count;
    const int smaller_payload_count = smaller_group_count * base;
    if (base > 0 && payload_block_index < smaller_payload_count)
        return payload_block_index / base;
    return smaller_group_count + (payload_block_index - smaller_payload_count) / (base + 1);
}

int ECProject::glrc_data_group_id(int data_block_index, int k, int r, int z)
{
    return glrc_payload_group_id(data_block_index, k, r, z);
}

int ECProject::glrc_data_blocks_in_group(int group_id, int k, int r, int z)
{
    if (z <= 0 || group_id < 0 || group_id >= z)
        return 0;
    int payload_start = 0;
    for (int g = 0; g < group_id; g++)
        payload_start += glrc_payload_blocks_in_group(g, k, r, z);
    const int payload_end = payload_start + glrc_payload_blocks_in_group(group_id, k, r, z);
    return std::max(0, std::min(payload_end, k) - payload_start);
}

int ECProject::glrc_global_blocks_in_group(int group_id, int k, int r, int z)
{
    if (z <= 0 || group_id < 0 || group_id >= z)
        return 0;
    int payload_start = 0;
    for (int g = 0; g < group_id; g++)
        payload_start += glrc_payload_blocks_in_group(g, k, r, z);
    const int payload_end = payload_start + glrc_payload_blocks_in_group(group_id, k, r, z);
    return std::max(0, payload_end - std::max(payload_start, k));
}

unsigned char ECProject::glrc_local_block_coefficient(int block_id, int k, int r)
{
    if (block_id >= 0 && block_id < k)
        return gf_inv(block_id ^ (k + r));
    return 1;
}

void ECProject::glrc_fill_data_blocks_per_group(std::vector<int> &data_blocks_per_group, int k, int r, int z)
{
    data_blocks_per_group.clear();
    for (int i = 0; i < z; i++)
        data_blocks_per_group.push_back(glrc_data_blocks_in_group(i, k, r, z));
}
void ECProject::gen_glrc_matrix(unsigned char *encode_matrix, int k, int r, int z)
{
    int m = k + r;
    memset(encode_matrix, 0, (m + z) * k);
    gf_gen_cauchy_matrix1(encode_matrix, m, k);
    unsigned char *local_vector = new unsigned char[k];
    gf_gen_local_vector(local_vector, k, r);
    for (int j = 0; j < k; j++)
    {
        int row = glrc_data_group_id(j, k, r, z);
        encode_matrix[(m + row) * k + j] = local_vector[j];
    }
    for (int l = 0; l < r; l++)
    {
        const int group_id = glrc_payload_group_id(k + l, k, r, z);
        for (int j = 0; j < k; j++)
            encode_matrix[(m + group_id) * k + j] ^= encode_matrix[(k + l) * k + j];
    }
    delete[] local_vector;
}
void ECProject::decode_glrc(const int k, const int r, const int z, const int block_num,
                           const std::vector<int> *block_indexes, unsigned char **block_ptrs, unsigned char *res_ptr, int block_size,
                           int failed_block_id)
{
    memset(res_ptr, 0, block_size);
    unsigned char *decode_vector = new unsigned char[block_num];
    for (int i = 0; i < block_num; i++)
        decode_vector[i] = glrc_local_block_coefficient(block_indexes->at(i), k, r);
    const unsigned char factor =
        gf_inv(glrc_local_block_coefficient(failed_block_id, k, r));
    for (int i = 0; i < block_num; i++)
        decode_vector[i] = gf_mul(decode_vector[i], factor);

    unsigned char *g_tbls = new unsigned char[block_num * 32];
    unsigned char **res_ptr_ptr = new unsigned char *[1];
    res_ptr_ptr[0] = res_ptr;
    ec_init_tables(block_num, 1, decode_vector, g_tbls);
    ec_encode_data_avx2(block_size, block_num, 1, g_tbls, block_ptrs, res_ptr_ptr);

    delete[] decode_vector;
    delete[] g_tbls;
    delete[] res_ptr_ptr;
}
namespace
{
  static void build_block_coef_row(int k, int r, int z, int n, int eq_index,
                                   const unsigned char *encode_matrix,
                                   const std::vector<std::vector<int>> &groups,
                                   std::vector<unsigned char> &coef_row)
  {
    coef_row.assign(n, 0);
    if (eq_index < z)
    {
      for (int b : groups[eq_index])
        coef_row[b] = ECProject::glrc_local_block_coefficient(b, k, r);
    }
    else
    {
      int g = eq_index - z;
      for (int i = 0; i < k; i++)
        coef_row[i] = encode_matrix[(k + g) * k + i];
      coef_row[k + g] = 1;
    }
  }

  struct GlrcIlpEqHelperTerm
  {
    int helper_idx;
    unsigned char coef;
  };

  /** Build f×f failed-block coefficient matrix and invert once (same system as decode_glrc_ilp). */
  static bool glrc_ilp_build_inverse(int k, int r, int z, int n,
                                     const std::vector<int> &failed_block_ids,
                                     const std::vector<int> &selected_equation_indices,
                                     const unsigned char *encode_matrix,
                                     const std::vector<std::vector<int>> &groups,
                                     std::vector<unsigned char> &A_inv_out)
  {
    const int f = (int)failed_block_ids.size();
    std::vector<unsigned char> coef_row(n);
    std::vector<unsigned char> A(f * f);
    for (int ei = 0; ei < f; ei++)
    {
      build_block_coef_row(k, r, z, n, selected_equation_indices[ei], encode_matrix, groups, coef_row);
      for (int t = 0; t < f; t++)
        A[ei * f + t] = coef_row[failed_block_ids[t]];
    }
    A_inv_out.assign(f * f, 0);
    std::vector<unsigned char> A_work(A.begin(), A.end());
    return ECProject::gf_invert_matrix(A_work.data(), A_inv_out.data(), f) == 0;
  }

} // namespace

bool ECProject::glrc_ilp_decode_matrix_invertible(int k, int r, int z,
                                                  const std::vector<int> &failed_block_ids,
                                                  const std::vector<int> &selected_equation_indices)
{
  const int f = (int)failed_block_ids.size();
  if (f == 0 || (int)selected_equation_indices.size() != f)
    return false;

  const int n = k + r + z;
  std::vector<std::vector<int>> groups;
  glrc_build_placement_groups(k, r, z, groups);

  unsigned char *encode_matrix = new unsigned char[(k + r + z) * k];
  gen_glrc_matrix(encode_matrix, k, r, z);

  std::vector<unsigned char> A_inv;
  bool ok = glrc_ilp_build_inverse(k, r, z, n, failed_block_ids, selected_equation_indices,
                                   encode_matrix, groups, A_inv);

  delete[] encode_matrix;
  return ok;
}

bool ECProject::glrc_ilp_prepare_inverse(const int k, const int r, const int z,
                                         const std::vector<int> &failed_block_ids,
                                         const std::vector<int> &selected_equation_indices,
                                         std::vector<unsigned char> &A_inv_out)
{
  const int f = (int)failed_block_ids.size();
  if (f == 0 || (int)selected_equation_indices.size() != f)
    return false;

  const int n = k + r + z;
  std::vector<std::vector<int>> groups;
  glrc_build_placement_groups(k, r, z, groups);

  unsigned char *encode_matrix = new unsigned char[(k + r + z) * k];
  gen_glrc_matrix(encode_matrix, k, r, z);
  const bool ok = glrc_ilp_build_inverse(k, r, z, n, failed_block_ids, selected_equation_indices,
                                         encode_matrix, groups, A_inv_out);
  delete[] encode_matrix;
  return ok;
}

bool ECProject::decode_glrc_ilp_rhs_compact(const std::vector<unsigned char *> &rhs_ptrs,
                                            const std::vector<unsigned char> &A_inv,
                                            int failed_count, int range_len,
                                            std::vector<std::vector<unsigned char>> &recovered)
{
  const int f = failed_count;
  if (f <= 0 || range_len < 0 || (int)rhs_ptrs.size() != f || (int)A_inv.size() != f * f)
    return false;

  recovered.assign(f, std::vector<unsigned char>(static_cast<size_t>(range_len), 0));
  std::vector<unsigned char> col_b(f);
  for (int byte_off = 0; byte_off < range_len; byte_off++)
  {
    for (int ei = 0; ei < f; ei++)
      col_b[ei] = rhs_ptrs[ei][byte_off];
    for (int t = 0; t < f; t++)
    {
      unsigned char acc = 0;
      for (int j = 0; j < f; j++)
        acc ^= ECProject::gf_mul(A_inv[t * f + j], col_b[j]);
      recovered[t][byte_off] = acc;
    }
  }
  return true;
}

bool ECProject::glrc_ilp_prepare_helper_decode(const int k, const int r, const int z,
                                               const std::vector<int> &helper_block_ids,
                                               const std::vector<int> &failed_block_ids,
                                               const std::vector<int> &selected_equation_indices,
                                               std::vector<unsigned char> &A_inv_out,
                                               std::vector<std::vector<int>> &eq_helper_indices_out,
                                               std::vector<std::vector<unsigned char>> &eq_helper_coefs_out)
{
  const int n = k + r + z;
  const int f = (int)failed_block_ids.size();
  if (f == 0 || (int)selected_equation_indices.size() != f || helper_block_ids.empty())
    return false;

  std::vector<std::vector<int>> groups;
  glrc_build_placement_groups(k, r, z, groups);
  unsigned char *encode_matrix = new unsigned char[(k + r + z) * k];
  gen_glrc_matrix(encode_matrix, k, r, z);

  if (!glrc_ilp_build_inverse(k, r, z, n, failed_block_ids, selected_equation_indices,
                              encode_matrix, groups, A_inv_out))
  {
    delete[] encode_matrix;
    return false;
  }

  std::unordered_map<int, int> helper_pos;
  for (int i = 0; i < (int)helper_block_ids.size(); i++)
    helper_pos[helper_block_ids[i]] = i;
  std::unordered_set<int> failed_set(failed_block_ids.begin(), failed_block_ids.end());

  eq_helper_indices_out.assign(f, {});
  eq_helper_coefs_out.assign(f, {});
  std::vector<unsigned char> coef_row(n);
  for (int ei = 0; ei < f; ei++)
  {
    build_block_coef_row(k, r, z, n, selected_equation_indices[ei], encode_matrix, groups, coef_row);
    for (int b = 0; b < n; b++)
    {
      if (!coef_row[b] || failed_set.count(b))
        continue;
      auto it = helper_pos.find(b);
      if (it == helper_pos.end())
      {
        delete[] encode_matrix;
        return false;
      }
      eq_helper_indices_out[ei].push_back(it->second);
      eq_helper_coefs_out[ei].push_back(coef_row[b]);
    }
  }
  delete[] encode_matrix;
  return true;
}

bool ECProject::decode_glrc_ilp_helper_compact(unsigned char **helper_ptrs,
                                               const std::vector<unsigned char> &A_inv,
                                               const std::vector<std::vector<int>> &eq_helper_indices,
                                               const std::vector<std::vector<unsigned char>> &eq_helper_coefs,
                                               int failed_count, int helper_base_off, int range_off, int range_len,
                                               std::vector<std::vector<unsigned char>> &recovered)
{
  const int f = failed_count;
  if (f <= 0 || helper_base_off < 0 || range_off < helper_base_off || range_len < 0 ||
      (int)A_inv.size() != f * f ||
      (int)eq_helper_indices.size() != f || (int)eq_helper_coefs.size() != f)
    return false;

  recovered.assign(f, std::vector<unsigned char>(static_cast<size_t>(range_len), 0));
  if (range_len == 0)
    return true;

  const int helper_off = range_off - helper_base_off;
  std::vector<std::vector<unsigned char>> rhs(f, std::vector<unsigned char>(static_cast<size_t>(range_len), 0));

  for (int ei = 0; ei < f; ei++)
  {
    if (eq_helper_indices[ei].size() != eq_helper_coefs[ei].size())
      return false;
    const int term_count = static_cast<int>(eq_helper_indices[ei].size());
    if (term_count == 0)
      continue;

    std::vector<unsigned char *> srcs(term_count);
    for (int ti = 0; ti < term_count; ti++)
      srcs[ti] = helper_ptrs[eq_helper_indices[ei][ti]] + helper_off;

    std::vector<unsigned char> coefs = eq_helper_coefs[ei];
    std::vector<unsigned char> g_tbls(static_cast<size_t>(term_count) * 32);
    ec_init_tables(term_count, 1, coefs.data(), g_tbls.data());
    unsigned char *dst = rhs[ei].data();
    ec_encode_data_avx2(range_len, term_count, 1, g_tbls.data(), srcs.data(), &dst);
  }

  std::vector<unsigned char *> rhs_ptrs(f);
  std::vector<unsigned char *> recovered_ptrs(f);
  for (int i = 0; i < f; i++)
  {
    rhs_ptrs[i] = rhs[i].data();
    recovered_ptrs[i] = recovered[i].data();
  }
  std::vector<unsigned char> inv = A_inv;
  std::vector<unsigned char> g_tbls(static_cast<size_t>(f) * static_cast<size_t>(f) * 32);
  ec_init_tables(f, f, inv.data(), g_tbls.data());
  ec_encode_data_avx2(range_len, f, f, g_tbls.data(), rhs_ptrs.data(), recovered_ptrs.data());
  return true;
}

bool ECProject::decode_glrc_ilp_helper_compact_prepared(unsigned char **helper_ptrs,
                                                        const std::vector<std::vector<int>> &eq_helper_indices,
                                                        const std::vector<std::vector<unsigned char>> &eq_helper_g_tbls,
                                                        const std::vector<unsigned char> &inv_g_tbls,
                                                        int failed_count, int helper_base_off, int range_off,
                                                        int range_len,
                                                        std::vector<std::vector<unsigned char>> &rhs,
                                                        std::vector<std::vector<unsigned char>> &recovered)
{
  const int f = failed_count;
  if (f <= 0 || helper_base_off < 0 || range_off < helper_base_off || range_len < 0 ||
      (int)eq_helper_indices.size() != f || (int)eq_helper_g_tbls.size() != f ||
      (int)inv_g_tbls.size() != f * f * 32)
    return false;

  if ((int)rhs.size() != f)
    rhs.assign(f, std::vector<unsigned char>(static_cast<size_t>(range_len), 0));
  if ((int)recovered.size() != f)
    recovered.assign(f, std::vector<unsigned char>(static_cast<size_t>(range_len), 0));
  for (int i = 0; i < f; i++)
  {
    if ((int)rhs[i].size() != range_len)
      rhs[i].assign(static_cast<size_t>(range_len), 0);
    else
      std::fill(rhs[i].begin(), rhs[i].end(), 0);
    if ((int)recovered[i].size() != range_len)
      recovered[i].assign(static_cast<size_t>(range_len), 0);
  }
  if (range_len == 0)
    return true;

  const int helper_off = range_off - helper_base_off;
  for (int ei = 0; ei < f; ei++)
  {
    const int term_count = static_cast<int>(eq_helper_indices[ei].size());
    if (term_count == 0)
      continue;
    if ((int)eq_helper_g_tbls[ei].size() != term_count * 32)
      return false;

    std::vector<unsigned char *> srcs(term_count);
    for (int ti = 0; ti < term_count; ti++)
      srcs[ti] = helper_ptrs[eq_helper_indices[ei][ti]] + helper_off;
    unsigned char *dst = rhs[ei].data();
    ec_encode_data_avx2(range_len, term_count, 1,
                        const_cast<unsigned char *>(eq_helper_g_tbls[ei].data()), srcs.data(), &dst);
  }

  std::vector<unsigned char *> rhs_ptrs(f);
  std::vector<unsigned char *> recovered_ptrs(f);
  for (int i = 0; i < f; i++)
  {
    rhs_ptrs[i] = rhs[i].data();
    recovered_ptrs[i] = recovered[i].data();
  }
  ec_encode_data_avx2(range_len, f, f, const_cast<unsigned char *>(inv_g_tbls.data()), rhs_ptrs.data(),
                      recovered_ptrs.data());
  return true;
}

bool ECProject::decode_glrc_ilp(const int k, const int r, const int z, int block_size,
                                const std::vector<int> &helper_block_ids, unsigned char **helper_ptrs,
                                const std::vector<int> &failed_block_ids,
                                const std::vector<int> &selected_equation_indices,
                                std::vector<unsigned char *> &recovered_ptrs)
{
  const int n = k + r + z;
  const int f = (int)failed_block_ids.size();
  if (f == 0 || (int)selected_equation_indices.size() != f)
    return false;

  std::vector<std::vector<int>> groups;
  glrc_build_placement_groups(k, r, z, groups);

  unsigned char *encode_matrix = new unsigned char[(k + r + z) * k];
  gen_glrc_matrix(encode_matrix, k, r, z);

  std::unordered_map<int, int> helper_pos;
  for (int i = 0; i < (int)helper_block_ids.size(); i++)
    helper_pos[helper_block_ids[i]] = i;

  std::unordered_set<int> failed_set(failed_block_ids.begin(), failed_block_ids.end());

  std::vector<unsigned char> A_inv;
  if (!glrc_ilp_build_inverse(k, r, z, n, failed_block_ids, selected_equation_indices,
                              encode_matrix, groups, A_inv))
  {
    delete[] encode_matrix;
    return false;
  }

  std::vector<std::vector<GlrcIlpEqHelperTerm>> eq_helper_terms(f);
  std::vector<unsigned char> coef_row(n);
  for (int ei = 0; ei < f; ei++)
  {
    build_block_coef_row(k, r, z, n, selected_equation_indices[ei], encode_matrix, groups, coef_row);
    for (int b = 0; b < n; b++)
    {
      if (!coef_row[b] || failed_set.count(b))
        continue;
      auto it = helper_pos.find(b);
      if (it == helper_pos.end())
      {
        delete[] encode_matrix;
        return false;
      }
      eq_helper_terms[ei].push_back({it->second, coef_row[b]});
    }
  }
  delete[] encode_matrix;

  const int helper_count = static_cast<int>(helper_block_ids.size());
  if (helper_count <= 0)
    return false;

  // Fold the two scalar stages
  //
  //   rhs       = C_helper * helpers
  //   recovered = A_inv    * rhs
  //
  // into one direct decode matrix.  The previous implementation evaluated
  // both products byte-by-byte and performed billions of scalar gf_mul calls
  // for a 64 MiB block.  ISA-L expands the coefficients once and evaluates
  // all recovered blocks with its vectorized AVX2 kernel.
  std::vector<unsigned char> direct_matrix(
      static_cast<size_t>(f) * static_cast<size_t>(helper_count), 0);
  for (int target = 0; target < f; target++)
  {
    unsigned char *row =
        direct_matrix.data() + static_cast<size_t>(target) * static_cast<size_t>(helper_count);
    for (int equation = 0; equation < f; equation++)
    {
      const unsigned char inverse_coef = A_inv[target * f + equation];
      if (inverse_coef == 0)
        continue;
      for (const GlrcIlpEqHelperTerm &term : eq_helper_terms[equation])
        row[term.helper_idx] ^= ECProject::gf_mul(inverse_coef, term.coef);
    }
  }

  recovered_ptrs.resize(f);
  for (int i = 0; i < f; i++)
    recovered_ptrs[i] = new unsigned char[block_size];

  std::vector<unsigned char> g_tbls(
      static_cast<size_t>(helper_count) * static_cast<size_t>(f) * 32);
  ec_init_tables(helper_count, f, direct_matrix.data(), g_tbls.data());
  ec_encode_data_avx2(block_size, helper_count, f, g_tbls.data(),
                      helper_ptrs, recovered_ptrs.data());

  return true;
}

bool ECProject::decode_glrc_ilp_range(const int k, const int r, const int z, int block_size,
                                      const std::vector<int> &helper_block_ids,
                                      unsigned char **helper_ptrs,
                                      const std::vector<int> &failed_block_ids,
                                      const std::vector<int> &selected_equation_indices,
                                      int range_off, int range_len,
                                      std::vector<unsigned char *> &recovered_ptrs)
{
  if (range_off < 0 || range_len < 0 || range_off + range_len > block_size)
    return false;
  if (range_len == 0)
  {
    recovered_ptrs.resize(failed_block_ids.size());
    for (size_t i = 0; i < recovered_ptrs.size(); i++)
      recovered_ptrs[i] = new unsigned char[block_size];
    return true;
  }

  const int n = k + r + z;
  const int f = (int)failed_block_ids.size();
  if (f == 0 || (int)selected_equation_indices.size() != f)
    return false;

  std::vector<std::vector<int>> groups;
  glrc_build_placement_groups(k, r, z, groups);

  unsigned char *encode_matrix = new unsigned char[(k + r + z) * k];
  gen_glrc_matrix(encode_matrix, k, r, z);

  std::unordered_map<int, int> helper_pos;
  for (int i = 0; i < (int)helper_block_ids.size(); i++)
    helper_pos[helper_block_ids[i]] = i;

  std::unordered_set<int> failed_set(failed_block_ids.begin(), failed_block_ids.end());

  std::vector<unsigned char> A_inv;
  if (!glrc_ilp_build_inverse(k, r, z, n, failed_block_ids, selected_equation_indices,
                              encode_matrix, groups, A_inv))
  {
    delete[] encode_matrix;
    return false;
  }

  std::vector<std::vector<GlrcIlpEqHelperTerm>> eq_helper_terms(f);
  std::vector<unsigned char> coef_row(n);
  for (int ei = 0; ei < f; ei++)
  {
    build_block_coef_row(k, r, z, n, selected_equation_indices[ei], encode_matrix, groups, coef_row);
    for (int b = 0; b < n; b++)
    {
      if (!coef_row[b] || failed_set.count(b))
        continue;
      auto it = helper_pos.find(b);
      if (it == helper_pos.end())
      {
        delete[] encode_matrix;
        return false;
      }
      eq_helper_terms[ei].push_back({it->second, coef_row[b]});
    }
  }
  delete[] encode_matrix;

  recovered_ptrs.resize(f);
  for (int i = 0; i < f; i++)
    recovered_ptrs[i] = new unsigned char[block_size];

  std::vector<unsigned char> col_b(f);
  const int end_off = range_off + range_len;
  for (int byte_off = range_off; byte_off < end_off; byte_off++)
  {
    for (int ei = 0; ei < f; ei++)
    {
      unsigned char rhs = 0;
      for (const GlrcIlpEqHelperTerm &term : eq_helper_terms[ei])
        rhs ^= ECProject::gf_mul(term.coef, helper_ptrs[term.helper_idx][byte_off]);
      col_b[ei] = rhs;
    }
    for (int t = 0; t < f; t++)
    {
      unsigned char acc = 0;
      for (int j = 0; j < f; j++)
        acc ^= ECProject::gf_mul(A_inv[t * f + j], col_b[j]);
      recovered_ptrs[t][byte_off] = acc;
    }
  }

  return true;
}

bool ECProject::decode_glrc_ilp_rhs_range(const int k, const int r, const int z, int block_size,
                                          unsigned char **rhs_ptrs,
                                          const std::vector<int> &failed_block_ids,
                                          const std::vector<int> &selected_equation_indices,
                                          int range_off, int range_len,
                                          std::vector<unsigned char *> &recovered_ptrs)
{
  if (range_off < 0 || range_len < 0 || range_off + range_len > block_size)
    return false;
  const int n = k + r + z;
  const int f = (int)failed_block_ids.size();
  if (f == 0 || (int)selected_equation_indices.size() != f)
    return false;

  std::vector<std::vector<int>> groups;
  glrc_build_placement_groups(k, r, z, groups);

  unsigned char *encode_matrix = new unsigned char[(k + r + z) * k];
  gen_glrc_matrix(encode_matrix, k, r, z);

  std::vector<unsigned char> A_inv;
  if (!glrc_ilp_build_inverse(k, r, z, n, failed_block_ids, selected_equation_indices,
                              encode_matrix, groups, A_inv))
  {
    delete[] encode_matrix;
    return false;
  }
  delete[] encode_matrix;

  recovered_ptrs.resize(f);
  for (int i = 0; i < f; i++)
  {
    recovered_ptrs[i] = new unsigned char[block_size];
    std::memset(recovered_ptrs[i], 0, block_size);
  }

  std::vector<unsigned char> col_b(f);
  const int end_off = range_off + range_len;
  for (int byte_off = range_off; byte_off < end_off; byte_off++)
  {
    for (int ei = 0; ei < f; ei++)
      col_b[ei] = rhs_ptrs[ei][byte_off];
    for (int t = 0; t < f; t++)
    {
      unsigned char acc = 0;
      for (int j = 0; j < f; j++)
        acc ^= ECProject::gf_mul(A_inv[t * f + j], col_b[j]);
      recovered_ptrs[t][byte_off] = acc;
    }
  }
  return true;
}
