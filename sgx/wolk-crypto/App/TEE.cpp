#include "TEE.h"

/* Global EID shared by multiple threads */
sgx_enclave_id_t global_eid = 0;

int sgx_seal(void) {
    if (initialize_enclave(&global_eid, "enclave.token", "enclave.signed.so") < 0) {
        std::cout << "Fail to initialize enclave." << std::endl;
        return 1;
    }


  /*
    const char* key = "a5718e79ae2fe43431820cba7315f48ac0a79e5305da6988c9f3358003784d85";
     const char* key2;

    sgx_status_t status;

    printf("key: %s\n", key);

    // Seal the random string
    size_t sealed_size = sizeof(sgx_sealed_data_t) + 64; //sizeof(key);
    uint8_t* sealed_data = (uint8_t*)malloc(sealed_size);

    sgx_status_t ecall_status;
    status = seal(global_eid, &ecall_status,
            (uint8_t*)&key, 64,
            (sgx_sealed_data_t*)sealed_data, sealed_size);



    */

    const char* plaintext = "SGXRAENCLAVE";
  //  uint8_t* plaintext = "SGXRAENCLAVE";
  //  uint8_t* hash[32] ;


   // uint8_t* hash = (uint8_t*)malloc(32);
    uint8_t hash[32] = "abc123";

    size_t hash_len = 32;
    sgx_status_t ecall_status;
    //wolkSHA256((uint8_t*)&plaintext, sizeof(plaintext)-1, (uint8_t*)&hash, hash_len);
    wolkSHA256(global_eid, &ecall_status, (uint8_t*)&plaintext, sizeof(plaintext)-1, hash, 32);


    printf("hash: ");
    int j;
    for(j = 0; j < 32; j++) {
        printf("%02X", hash[j]);
    }
    printf("\nEND\n");

/*
    const char ID_U[] = "SGXRAENCLAVE"; // 8e50e33484683bcf17591e95d7d391807d80024c7e7b7e4960d9a377bcb72ea9

    sgx_status_t sgx_ret = SGX_SUCCESS;
    sgx_sha_state_handle_t sha_context;
    sgx_sha256_hash_t key_material;

    sgx_ret = sgx_sha256_init(&sha_context);
    if (sgx_ret != SGX_SUCCESS)
    {
        return false;
    }

    sgx_ret = sgx_sha256_update((uint8_t*)&ID_U, sizeof(ID_U)-1, sha_context);
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

    char buffer[64] ;
    int j;
    int k;
    k=0;
    for(j = 0; j < 32; j++) {
    	k= k+2;
        sprintf(&buffer[k], "%02X", key_material[j]);
        printf("%02X", key_material[j]);
    }

    printf("\n");
    printf("%s", buffer);
    printf("\n");
    std::cout << buffer << std::endl;

    sgx_ret = sgx_sha256_close(sha_context);

    return true;

*/








/*
    const char* key = "a5718e79ae2fe43431820cba7315f48ac0a79e5305da6988c9f3358003784d85";
     const char* key2;

    sgx_status_t status;

    printf("key: %s\n", key);

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
*/
    return 0;
}
