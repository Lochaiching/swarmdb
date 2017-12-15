#include "TEE.h"

/* Global EID shared by multiple threads */
sgx_enclave_id_t global_eid = 0;

char* getSha256(char *str) {
    if (initialize_enclave(&global_eid, "enclave.token", "enclave.signed.so") < 0) {
        std::cout << "Fail to initialize enclave." << std::endl;
        exit;
    }

    printf("\nReceived string from Go: %s\n", str);
    //const char* src = "TestThisSGX";

    sgx_status_t ecall_status;

    uint8_t hash[32] = "\0"; // empty array 0000000000000000000000000000000000000000000000000000000000000000
    size_t hash_len = 32;

    sgxGetSha256(global_eid, &ecall_status, (uint8_t*)str, strlen(str), hash, 32);

    printf("hash: ");
    int j;
    for(j = 0; j < 32; j++) {
        printf("%02X", hash[j]);
    }

    char ret[64]= "\0";
    int k;
    for(k = 0; k < 32; k++) {
    	sprintf(&ret[k*2], "%02X", hash[k]);
    }
    printf("\nreturn: %s\n", ret);

    return ret;
}
