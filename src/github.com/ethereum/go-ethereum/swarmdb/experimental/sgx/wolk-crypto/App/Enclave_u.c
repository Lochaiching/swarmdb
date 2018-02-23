#include "Enclave_u.h"
#include <errno.h>

typedef struct ms_seal_t {
	sgx_status_t ms_retval;
	uint8_t* ms_plaintext;
	size_t ms_plaintext_len;
	sgx_sealed_data_t* ms_sealed_data;
	size_t ms_sealed_size;
} ms_seal_t;

typedef struct ms_unseal_t {
	sgx_status_t ms_retval;
	sgx_sealed_data_t* ms_sealed_data;
	size_t ms_sealed_size;
	uint8_t* ms_plaintext;
	uint32_t ms_plaintext_len;
} ms_unseal_t;

typedef struct ms_sgxGetSha256_t {
	sgx_status_t ms_retval;
	uint8_t* ms_src;
	size_t ms_src_len;
	uint8_t* ms_hash;
	size_t ms_hash_len;
} ms_sgxGetSha256_t;

typedef struct ms_sgxEcc256CreateKeyPair_t {
	sgx_status_t ms_retval;
	sgx_ec256_private_t* ms_p_private;
	sgx_ec256_public_t* ms_p_public;
} ms_sgxEcc256CreateKeyPair_t;

typedef struct ms_sgxEcdsaSign_t {
	sgx_status_t ms_retval;
	uint8_t* ms_sample_data;
	size_t ms_sample_data_len;
	sgx_ec256_private_t* ms_p_private;
	sgx_ec256_signature_t* ms_p_signature;
} ms_sgxEcdsaSign_t;

typedef struct ms_ocall_print_t {
	char* ms_str;
} ms_ocall_print_t;

typedef struct ms_ocall_uint8_t_print_t {
	uint8_t* ms_arr;
	size_t ms_len;
} ms_ocall_uint8_t_print_t;

typedef struct ms_ocall_uint32_t_print_t {
	uint32_t* ms_arr;
	size_t ms_len;
} ms_ocall_uint32_t_print_t;

static sgx_status_t SGX_CDECL Enclave_ocall_print(void* pms)
{
	ms_ocall_print_t* ms = SGX_CAST(ms_ocall_print_t*, pms);
	ocall_print((const char*)ms->ms_str);

	return SGX_SUCCESS;
}

static sgx_status_t SGX_CDECL Enclave_ocall_uint8_t_print(void* pms)
{
	ms_ocall_uint8_t_print_t* ms = SGX_CAST(ms_ocall_uint8_t_print_t*, pms);
	ocall_uint8_t_print(ms->ms_arr, ms->ms_len);

	return SGX_SUCCESS;
}

static sgx_status_t SGX_CDECL Enclave_ocall_uint32_t_print(void* pms)
{
	ms_ocall_uint32_t_print_t* ms = SGX_CAST(ms_ocall_uint32_t_print_t*, pms);
	ocall_uint32_t_print(ms->ms_arr, ms->ms_len);

	return SGX_SUCCESS;
}

static const struct {
	size_t nr_ocall;
	void * table[3];
} ocall_table_Enclave = {
	3,
	{
		(void*)Enclave_ocall_print,
		(void*)Enclave_ocall_uint8_t_print,
		(void*)Enclave_ocall_uint32_t_print,
	}
};
sgx_status_t seal(sgx_enclave_id_t eid, sgx_status_t* retval, uint8_t* plaintext, size_t plaintext_len, sgx_sealed_data_t* sealed_data, size_t sealed_size)
{
	sgx_status_t status;
	ms_seal_t ms;
	ms.ms_plaintext = plaintext;
	ms.ms_plaintext_len = plaintext_len;
	ms.ms_sealed_data = sealed_data;
	ms.ms_sealed_size = sealed_size;
	status = sgx_ecall(eid, 0, &ocall_table_Enclave, &ms);
	if (status == SGX_SUCCESS && retval) *retval = ms.ms_retval;
	return status;
}

sgx_status_t unseal(sgx_enclave_id_t eid, sgx_status_t* retval, sgx_sealed_data_t* sealed_data, size_t sealed_size, uint8_t* plaintext, uint32_t plaintext_len)
{
	sgx_status_t status;
	ms_unseal_t ms;
	ms.ms_sealed_data = sealed_data;
	ms.ms_sealed_size = sealed_size;
	ms.ms_plaintext = plaintext;
	ms.ms_plaintext_len = plaintext_len;
	status = sgx_ecall(eid, 1, &ocall_table_Enclave, &ms);
	if (status == SGX_SUCCESS && retval) *retval = ms.ms_retval;
	return status;
}

sgx_status_t sgxGetSha256(sgx_enclave_id_t eid, sgx_status_t* retval, uint8_t* src, size_t src_len, uint8_t* hash, size_t hash_len)
{
	sgx_status_t status;
	ms_sgxGetSha256_t ms;
	ms.ms_src = src;
	ms.ms_src_len = src_len;
	ms.ms_hash = hash;
	ms.ms_hash_len = hash_len;
	status = sgx_ecall(eid, 2, &ocall_table_Enclave, &ms);
	if (status == SGX_SUCCESS && retval) *retval = ms.ms_retval;
	return status;
}

sgx_status_t sgxEcc256CreateKeyPair(sgx_enclave_id_t eid, sgx_status_t* retval, sgx_ec256_private_t* p_private, sgx_ec256_public_t* p_public)
{
	sgx_status_t status;
	ms_sgxEcc256CreateKeyPair_t ms;
	ms.ms_p_private = p_private;
	ms.ms_p_public = p_public;
	status = sgx_ecall(eid, 3, &ocall_table_Enclave, &ms);
	if (status == SGX_SUCCESS && retval) *retval = ms.ms_retval;
	return status;
}

sgx_status_t sgxEcdsaSign(sgx_enclave_id_t eid, sgx_status_t* retval, uint8_t* sample_data, size_t sample_data_len, sgx_ec256_private_t* p_private, sgx_ec256_signature_t* p_signature)
{
	sgx_status_t status;
	ms_sgxEcdsaSign_t ms;
	ms.ms_sample_data = sample_data;
	ms.ms_sample_data_len = sample_data_len;
	ms.ms_p_private = p_private;
	ms.ms_p_signature = p_signature;
	status = sgx_ecall(eid, 4, &ocall_table_Enclave, &ms);
	if (status == SGX_SUCCESS && retval) *retval = ms.ms_retval;
	return status;
}

