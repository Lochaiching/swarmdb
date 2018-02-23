#include "TEE.h"


#include <assert.h>


#include "sgx_tcrypto.h"



/* Global EID shared by multiple threads */
sgx_enclave_id_t global_eid = 0;



typedef uint8_t sgx_ec_key_128bit_t[SGX_CMAC_KEY_SIZE];

#ifndef SAMPLE_FEBITSIZE
    #define SAMPLE_FEBITSIZE                    256
#endif

#define SAMPLE_ECP_KEY_SIZE                     (SAMPLE_FEBITSIZE/8)

typedef struct sample_ec_dh_shared_t
{
    uint8_t s[SAMPLE_ECP_KEY_SIZE];
}sample_ec_dh_shared_t;

const char ID_U[] = "SGXRAENCLAVE"; // 8e50e33484683bcf17591e95d7d391807d80024c7e7b7e4960d9a377bcb72ea9
const char ID_V[] = "SGXRASERVER";

typedef struct _hash_buffer_t
{
    uint8_t counter[4];
    sample_ec_dh_shared_t shared_secret;
    uint8_t algorithm_id[4];
} hash_buffer_t;



// Derive two keys from shared key and key id.
bool derive_key()
{
    sgx_status_t sgx_ret = SGX_SUCCESS;
    sgx_sha_state_handle_t sha_context;
    sgx_sha256_hash_t key_material;

    sgx_ret = sgx_sha256_init(&sha_context);
    if (sgx_ret != SGX_SUCCESS)
    {
        return false;
    }
    printf("hashing: %d\n", sizeof(ID_U)); // sizeof(ID_U) = 13 but its a 12 byte str
    sgx_ret = sgx_sha256_update((uint8_t*)&ID_U, 12, sha_context);
    if (sgx_ret != SGX_SUCCESS)
    {
        sgx_sha256_close(sha_context);
        return false;
    }

    sgx_ret = sgx_sha256_get_hash(sha_context, &key_material);
    if (sgx_ret != SGX_SUCCESS)
    {
        sgx_sha256_close(sha_context);
        return false;
    }
    printf("sgx_ret: %d\n", sgx_ret);
    int j;
    for(j = 0; j < 32; j++) {
        printf("%02X", key_material[j]);
    }
    printf("\n", sgx_ret);

    sgx_ret = sgx_sha256_close(sha_context);
    assert(sizeof(sgx_ec_key_128bit_t)* 2 == sizeof(sgx_sha256_hash_t));
    return true;
}


int sgx_seal(void) {
    if (initialize_enclave(&global_eid, "enclave.token", "enclave.signed.so") < 0) {
        std::cout << "Fail to initialize enclave." << std::endl;
        return 1;
    }

    derive_key();

    const char* key = "a5718e79ae2fe43431820cba7315f48ac0a79e5305da6988c9f3358003784d85";
     const char* key2;

    sgx_status_t status;

    // Seal the random string
    size_t sealed_size = sizeof(sgx_sealed_data_t) + 64; //sizeof(key);
    uint8_t* sealed_data = (uint8_t*)malloc(sealed_size);

    sgx_status_t ecall_status;
    status = seal(global_eid, &ecall_status,
            (uint8_t*)&key, 64,
            (sgx_sealed_data_t*)sealed_data, sealed_size);

    printf("sealed_data : %s\n", &sealed_data);
    printf("sealed_size : %d\n", sealed_size);

    if (!is_ecall_successful(status, "Sealing failed :(", ecall_status)) {
        return 1;
    }

    // write
    const char* file_name = "seal.key";

    FILE* fp = fopen(file_name, "wb");
    if (fp == NULL) {
        printf("Warning: Failed to create/open (wb) file: \"%s\".\n", file_name);
        return 0;
    }

    size_t write_num = fwrite(sealed_data, 1, sealed_size, fp);
    if (write_num != sealed_size)
        printf("Warning: Failed to save sealed key to \"%s\".\n", file_name);
    fclose(fp);



    // read
    //const char* file_name = "seal.key";

    FILE* fp1 = fopen(file_name, "rb");
    if (fp1 == NULL) {
        printf("Warning: Failed to open (rb) file: \"%s\".\n", file_name);
        return 0;
    }
    uint8_t* sealed_data2 = (uint8_t*)malloc(sealed_size);
    printf("sealed_data : %s\n", &sealed_data2);
    printf("sealed_size : %d\n", sealed_size);

    size_t read_num = fread(sealed_data2, 1, sealed_size, fp1);
    if (read_num != sealed_size) {
        printf("Warning: Invalid sealed key read from \"%s\".\n", file_name);
        return 0;
    }



    printf("sealed_data : %s\n", &sealed_data2);
    printf("sealed_size : %d\n", sealed_size);







    status = unseal(global_eid, &ecall_status,
            (sgx_sealed_data_t*)sealed_data2, sealed_size,
            (uint8_t*)&key2, 64);

    if (!is_ecall_successful(status, "Unsealing failed :(", ecall_status)) {
        return 1;
    }

    printf("unsealed key: %s\n", key2);

    std::cout << "Seal round trip success! Receive back " << key2 << std::endl;

    return 0;
}
