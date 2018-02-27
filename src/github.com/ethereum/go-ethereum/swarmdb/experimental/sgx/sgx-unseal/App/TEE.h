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
int sgx_seal(void);
int sgx_unseal(void);
#ifdef __cplusplus  
}  
#endif  
  
#endif  

