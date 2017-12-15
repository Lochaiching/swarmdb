#ifndef _INTEL_SGX_H  
#define _INTEL_SGX_H  
  
#ifdef __cplusplus  

#include <stdio.h>
#include <iostream>
#include "Enclave_u.h"
#include "sgx_urts.h"
#include "sgx_utils/sgx_utils.h"

extern "C" {  

#endif

char* getSha256(char *str);
int ecc256CreateKeyPair(char* privateKey, char* publicKeyGX, char* publicKeyGY);

#ifdef __cplusplus

void ocall_print(const char* str);
void ocall_uint8_t_print(uint8_t *arr, size_t len);

}  
#endif  
  
#endif  

