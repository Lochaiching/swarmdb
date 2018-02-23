#ifndef ENCLAVE_U_H__
#define ENCLAVE_U_H__

#include <stdint.h>
#include <wchar.h>
#include <stddef.h>
#include <string.h>
#include "sgx_edger8r.h" /* for sgx_satus_t etc. */

#include "sgx_tseal.h"

#include <stdlib.h> /* for size_t */

#define SGX_CAST(type, item) ((type)(item))

#ifdef __cplusplus
extern "C" {
#endif

void SGX_UBRIDGE(SGX_NOCONVENTION, ocall_print, (const char* str));
void SGX_UBRIDGE(SGX_NOCONVENTION, ocall_uint8_t_print, (uint8_t* arr, size_t len));
void SGX_UBRIDGE(SGX_NOCONVENTION, ocall_uint32_t_print, (uint32_t* arr, size_t len));

sgx_status_t seal(sgx_enclave_id_t eid, sgx_status_t* retval, uint8_t* plaintext, size_t plaintext_len, sgx_sealed_data_t* sealed_data, size_t sealed_size);
sgx_status_t unseal(sgx_enclave_id_t eid, sgx_status_t* retval, sgx_sealed_data_t* sealed_data, size_t sealed_size, uint8_t* plaintext, uint32_t plaintext_len);
sgx_status_t sgxGetSha256(sgx_enclave_id_t eid, sgx_status_t* retval, uint8_t* src, size_t src_len, uint8_t* hash, size_t hash_len);
sgx_status_t sgxEcc256CreateKeyPair(sgx_enclave_id_t eid, sgx_status_t* retval, sgx_ec256_private_t* p_private, sgx_ec256_public_t* p_public);
sgx_status_t sgxEcdsaSign(sgx_enclave_id_t eid, sgx_status_t* retval, uint8_t* sample_data, size_t sample_data_len, sgx_ec256_private_t* p_private, sgx_ec256_signature_t* p_signature);

#ifdef __cplusplus
}
#endif /* __cplusplus */

#endif
