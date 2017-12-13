#include "sgx_trts.h"
#include "sgx_tseal.h"
#include "string.h"
#include "Enclave_t.h"

#include <stdio.h>

/**
 * @brief      Seals the plaintext given into the sgx_sealed_data_t structure
 *             given.
 *
 * @details    The plaintext can be any data. uint8_t is used to represent a
 *             byte. The sealed size can be determined by computing
 *             sizeof(sgx_sealed_data_t) + plaintext_len, since it is using
 *             AES-GCM which preserves length of plaintext. The size needs to be
 *             specified, otherwise SGX will assume the size to be just
 *             sizeof(sgx_sealed_data_t), not taking into account the sealed
 *             payload.
 *
 * @param      plaintext      The data to be sealed
 * @param[in]  plaintext_len  The plaintext length
 * @param      sealed_data    The pointer to the sealed data structure
 * @param[in]  sealed_size    The size of the sealed data structure supplied
 *
 * @return     Truthy if seal successful, falsy otherwise.
 */
sgx_status_t seal(uint8_t* plaintext, size_t plaintext_len, sgx_sealed_data_t* sealed_data, size_t sealed_size) {
    sgx_status_t status = sgx_seal_data(0, NULL, plaintext_len, plaintext, sealed_size, sealed_data);
    return status;
}

/**
 * @brief      Unseal the sealed_data given into c-string
 *
 * @details    The resulting plaintext is of type uint8_t to represent a byte.
 *             The sizes/length of pointers need to be specified, otherwise SGX
 *             will assume a count of 1 for all pointers.
 *
 * @param      sealed_data        The sealed data
 * @param[in]  sealed_size        The size of the sealed data
 * @param      plaintext          A pointer to buffer to store the plaintext
 * @param[in]  plaintext_max_len  The size of buffer prepared to store the
 *                                plaintext
 *
 * @return     Truthy if unseal successful, falsy otherwise.
 */
sgx_status_t unseal(sgx_sealed_data_t* sealed_data, size_t sealed_size, uint8_t* plaintext, uint32_t plaintext_len) {
    sgx_status_t status = sgx_unseal_data(sealed_data, NULL, NULL, (uint8_t*)plaintext, &plaintext_len);
    return status;
}




sgx_status_t wolkSHA256(uint8_t* plaintext, size_t plaintext_len, uint8_t* hash, size_t hash_len) {




    const char ID_U[] = "SGXRAENCLAVE"; // 8e50e33484683bcf17591e95d7d391807d80024c7e7b7e4960d9a377bcb72ea9

    sgx_status_t sgx_ret = SGX_SUCCESS;
    sgx_sha_state_handle_t sha_context;
    sgx_sha256_hash_t key_material;

    sgx_ret = sgx_sha256_init(&sha_context);
    if (sgx_ret != SGX_SUCCESS)
    {
        return sgx_ret;
    }

    //sgx_ret = sgx_sha256_update((uint8_t*)&ID_U, sizeof(ID_U)-1, sha_context);
    sgx_ret = sgx_sha256_update((uint8_t*)&ID_U, sizeof(ID_U)-1, sha_context);

    if (sgx_ret != SGX_SUCCESS)
    {
        sgx_sha256_close(sha_context);
        return sgx_ret;
    }

    sgx_ret = sgx_sha256_get_hash(sha_context, &key_material);
    if (sgx_ret != SGX_SUCCESS)
    {
        sgx_sha256_close(sha_context);
        return sgx_ret;
    }


    //char enclaveString[32] = "Internal enclave";
  //  uint8_t enclaveString[32]= "aaaaaaaaaa";
  //  hash = (uint8_t*)malloc(32);
   memcpy(hash, key_material, 32);






//    char buffer[64] ;
//    int j;
//    int k;
//    k=0;
//    for(j = 0; j < 32; j++) {
//    	k= k+2;
//        sprintf(&buffer[k], "%02X", key_material[j]);
//        printf("%02X", key_material[j]);
//    }

//    printf("\n");
//    printf("%s", buffer);
//    printf("\n");
//    std::cout << buffer << std::endl;

    sgx_ret = sgx_sha256_close(sha_context);

    return sgx_ret;
}
