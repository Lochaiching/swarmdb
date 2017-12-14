#include "TEE.h"

/* Global EID shared by multiple threads */
sgx_enclave_id_t global_eid = 0;

int getSha256() {
    if (initialize_enclave(&global_eid, "enclave.token", "enclave.signed.so") < 0) {
        std::cout << "Fail to initialize enclave." << std::endl;
        return 1;
    }

    sgx_status_t ecall_status;

    const char* plaintext = "SGXRAENCLAVE";

    uint8_t hash[32] = "\0"; // empty array 0000000000000000000000000000000000000000000000000000000000000000
    size_t hash_len = 32;

    sgxGetSha256(global_eid, &ecall_status, (uint8_t*)plaintext, strlen(plaintext), hash, 32);

    printf("hash: ");
    int j;
    for(j = 0; j < 32; j++) {
        printf("%02X", hash[j]);
    }
    printf("\nEND\n");

    return 0;
}
