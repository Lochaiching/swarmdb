#include "TEE.h"

/* Global EID shared by multiple threads */
sgx_enclave_id_t global_eid = 0;

int sgx_seal(void) {
    if (initialize_enclave(&global_eid, "enclave.token", "enclave.signed.so") < 0) {
        std::cout << "Fail to initialize enclave." << std::endl;
        return 1;
    }
    int ptr = 95;
 //   const char* ptr = "a5718e79ae2fe43431820cba7315f48ac0a79e5305da6988c9f3358003784d85";

    sgx_status_t status;
    //sgx_status_t status = generate_random_number(global_eid, &ptr);
//    std::cout << status << std::endl;
//    if (status != SGX_SUCCESS) {
////        std::cout << "noob" << std::endl;
//    }
    printf("Random number: %d\n", ptr);
   // printf("Random number: %s\n", ptr);

    const char* file_name = "seal.key";


    // Seal the random number
    size_t sealed_size = sizeof(sgx_sealed_data_t) + sizeof(ptr);
    uint8_t* sealed_data = (uint8_t*)malloc(sealed_size);

    sgx_status_t ecall_status;


    status = seal(global_eid, &ecall_status,
            (uint8_t*)&ptr, sizeof(ptr),
            (sgx_sealed_data_t*)sealed_data, sealed_size);

    if (!is_ecall_successful(status, "Sealing failed :(", ecall_status)) {
        return 1;
    }
/////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    FILE* fp = fopen(file_name, "wb");
    if (fp == NULL) {
        printf("Warning: Failed to create/open (wb) file: \"%s\".\n", file_name);
        return 0;
    }

    size_t write_num = fwrite(sealed_data, 1, sealed_size, fp);
    if (write_num != sealed_size)
        printf("Warning: Failed to save sealed key to \"%s\".\n", file_name);
    fclose(fp);
/*
/////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    FILE* fp1 = fopen(file_name, "rb");
    if (fp1 == NULL) {
        printf("Warning: Failed to open (rb) file: \"%s\".\n", file_name);
        return 0;
    }
//    uint8_t* sealed_data = (uint8_t*)malloc(sealed_size);
//    printf("sealed_data : %s\n", &sealed_data);
//    printf("sealed_size : %d\n", sealed_size);

    size_t read_num = fread(sealed_data, 1, sealed_size, fp1);
    if (read_num != sealed_size) {
        printf("Warning: Invalid sealed key read from \"%s\".\n", file_name);
        return 0;
    }

    fclose(fp1);


/////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////



















    int unsealed;
    status = unseal(global_eid, &ecall_status,
            (sgx_sealed_data_t*)sealed_data, sealed_size,
            (uint8_t*)&unsealed, sizeof(unsealed));

    if (!is_ecall_successful(status, "Unsealing failed :(", ecall_status)) {
        return 1;
    }

    std::cout << "Seal round trip success! Receive back " << unsealed << std::endl;
*/
    return 0;
}

int sgx_unseal(void) {
    if (initialize_enclave(&global_eid, "enclave.token", "enclave.signed.so") < 0) {
        std::cout << "Fail to initialize enclave." << std::endl;
        return 1;
    }
   int ptr;
     //char* unsealed;

    sgx_status_t status;
 /*
    sgx_status_t status = generate_random_number(global_eid, &ptr);
    std::cout << status << std::endl;
    if (status != SGX_SUCCESS) {
        std::cout << "noob" << std::endl;
    }
    printf("Random number: %d\n", ptr);
*/
    const char* file_name = "seal.key";


    // Seal the random number
    size_t sealed_size = sizeof(sgx_sealed_data_t) + sizeof(ptr);
    uint8_t* sealed_data = (uint8_t*)malloc(sealed_size);

    sgx_status_t ecall_status;
/*

    status = seal(global_eid, &ecall_status,
            (uint8_t*)&ptr, sizeof(ptr),
            (sgx_sealed_data_t*)sealed_data, sealed_size);

    if (!is_ecall_successful(status, "Sealing failed :(", ecall_status)) {
        return 1;
    }
/////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    FILE* fp = fopen(file_name, "wb");
    if (fp == NULL) {
        printf("Warning: Failed to create/open (wb) file: \"%s\".\n", file_name);
        return 0;
    }

    size_t write_num = fwrite(sealed_data, 1, sealed_size, fp);
    if (write_num != sealed_size)
        printf("Warning: Failed to save sealed key to \"%s\".\n", file_name);
    fclose(fp);
*/
/////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    FILE* fp1 = fopen(file_name, "rb");
    if (fp1 == NULL) {
        printf("Warning: Failed to open (rb) file: \"%s\".\n", file_name);
        return 0;
    }
//    uint8_t* sealed_data = (uint8_t*)malloc(sealed_size);
//    printf("sealed_data : %s\n", &sealed_data);
//    printf("sealed_size : %d\n", sealed_size);

    size_t read_num = fread(sealed_data, 1, sealed_size, fp1);
    if (read_num != sealed_size) {
        printf("Warning: Invalid sealed key read from \"%s\".\n", file_name);
        return 0;
    }

    fclose(fp1);


/////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////



















    int unsealed;

    status = unseal(global_eid, &ecall_status,
            (sgx_sealed_data_t*)sealed_data, sealed_size,
            (uint8_t*)&unsealed, sizeof(unsealed));

    if (!is_ecall_successful(status, "Unsealing failed :(", ecall_status)) {
        return 1;
    }

    std::cout << "Seal round trip success! Receive back " << unsealed << std::endl;

    return 0;
}
