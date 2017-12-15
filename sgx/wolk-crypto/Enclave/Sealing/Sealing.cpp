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

sgx_status_t sgxGetSha256(uint8_t* src, size_t src_len, uint8_t* hash, size_t hash_len) {

    sgx_status_t sgx_ret = SGX_SUCCESS;
    sgx_sha_state_handle_t sha_context;
    sgx_sha256_hash_t sgx_hash;

    sgx_ret = sgx_sha256_init(&sha_context);
    if (sgx_ret != SGX_SUCCESS)
    {
        return sgx_ret;
    }

    sgx_ret = sgx_sha256_update((uint8_t*)src, src_len, sha_context);
    if (sgx_ret != SGX_SUCCESS)
    {
        sgx_sha256_close(sha_context);
        return sgx_ret;
    }

    sgx_ret = sgx_sha256_get_hash(sha_context, &sgx_hash);
    if (sgx_ret != SGX_SUCCESS)
    {
        sgx_sha256_close(sha_context);
        return sgx_ret;
    }

    memcpy(hash, sgx_hash, 32);

    sgx_ret = sgx_sha256_close(sha_context);

    return sgx_ret;
}

sgx_status_t sgxEcc256CreateKeyPair(sgx_ec256_private_t* p_private, sgx_ec256_public_t* p_public) {

    sgx_status_t sgx_ret = SGX_SUCCESS;
    sgx_ecc_state_handle_t ecc_handle;

    sgx_ret = sgx_ecc256_open_context(&ecc_handle);
    if (sgx_ret != SGX_SUCCESS) {
        switch (sgx_ret) {
            case SGX_ERROR_OUT_OF_MEMORY:
                //ocall_print("SGX_ERROR_OUT_OF_MEMORY");
                break;
            case SGX_ERROR_UNEXPECTED:
                //ocall_print("SGX_ERROR_UNEXPECTED");
                break;
        }
    }

    // create private, public key pair
    sgx_ret = sgx_ecc256_create_key_pair(p_private, p_public, ecc_handle);
    if (sgx_ret != SGX_SUCCESS)
    {
        return sgx_ret;
    }

    /*
    swarm.wolk.com/sgx/go-with-intel-sgx/Enclave/Enclave.cpp

    ocall_print("ecc private key");
    ocall_uint8_t_print(p_private.r, SGX_ECP256_KEY_SIZE);

    ocall_print("ecc public key.gx");
    ocall_uint8_t_print(p_public.gx, SGX_ECP256_KEY_SIZE);
    ocall_print("ecc public key.gy");
    ocall_uint8_t_print(p_public.gy, SGX_ECP256_KEY_SIZE);
    */

    return sgx_ret;
}

















