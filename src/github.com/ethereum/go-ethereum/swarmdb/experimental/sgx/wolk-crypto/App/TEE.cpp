#include "TEE.h"
#include <openssl/ecdsa.h>

#include<string>
using namespace std;

/* Global EID shared by multiple threads */
sgx_enclave_id_t global_eid = 0;

#define SGX_ECP256_KEY_SIZE             32
#define SGX_NISTP_ECP256_KEY_SIZE       (SGX_ECP256_KEY_SIZE/sizeof(uint32_t))   // 32/4 = 8

void ocall_print(const char* str) {
	printf("%s\n", str);
}

void ocall_uint8_t_print(uint8_t *arr, size_t len) {
    for (int i = 0; i < len; i++) {
        printf("%02X", arr[i]);
    }
    printf("\n");
}

void ocall_uint32_t_print(uint32_t *arr, size_t len) {
    for (int i = 0; i < len; i++) {
        printf("%02X", arr[i]);
    }
    printf("\n");
}

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

int ecc256CreateKeyPair(char* privateKey, char* publicKeyGX, char* publicKeyGY) {
    if (initialize_enclave(&global_eid, "enclave.token", "enclave.signed.so") < 0) {
        std::cout << "Fail to initialize enclave." << std::endl;
        exit;
    }

    sgx_status_t ecall_status;

    sgx_ec256_private_t p_private;
    sgx_ec256_public_t p_public;

    sgxEcc256CreateKeyPair(global_eid, &ecall_status, &p_private, &p_public);

    ocall_print("ecc private key");
    ocall_uint8_t_print(p_private.r, SGX_ECP256_KEY_SIZE);
    ocall_print("ecc public key.gx");
    ocall_uint8_t_print(p_public.gx, SGX_ECP256_KEY_SIZE);
    ocall_print("ecc public key.gy");
    ocall_uint8_t_print(p_public.gy, SGX_ECP256_KEY_SIZE);

    int k;
    for(k = 0; k < 32; k++) {
    	sprintf(&privateKey[k*2], "%02X",p_private.r[k]);
    }
    // printf("\nreturn: %s\n", privateKey);

    //int k;
    for(k = 0; k < 32; k++) {
    	sprintf(&publicKeyGX[k*2], "%02X",p_public.gx[k]);
    }
    //printf("\nreturn: %s\n", publicKeyGX);

    //int k;
    for(k = 0; k < 32; k++) {
    	sprintf(&publicKeyGY[k*2], "%02X",p_public.gy[k]);
    }
    //printf("\nreturn: %s\n", publicKeyGY);


    uint8_t sig[64];
    string hex = "3bc843a917d6c19c487c1d0c660cdd61389ce2a7651ee3171bcc212ffddca164193f1f2e06f7ed8f9fbf2254232d99848a8102b552032b68a5507b4d81492f0f1b";
    int len = hex.length();
    //uint8_t r[SGX_ECP256_KEY_SIZE];
    int j=0;
    for(int i=0; i< len; i+=2)
    {
        string byte = hex.substr(i,2);
        sig[j] = (uint8_t) (int)strtol(byte.c_str(), NULL, 16);
        j=j+1;
    }


    // int8_t sig[64]= "\0";

        ECDSA_SIG* ec_sig = ECDSA_SIG_new();

        if (NULL == BN_bin2bn(sig, 32, (ec_sig->r))) {
          //dumpOpenSslErrors();
        	printf("FFFFFFFFFFFFFFFFFFAIL :%s\n");
         }

      //  printf("post r  :%s\n", BN_bn2hex(ec_sig->r));
        printf("post r  :%d%d%d\n", ec_sig->r);
		 //BN_bn2hex(ec_sig->r);
//        printf("post r  :%s\n", h1);
/*
        if (NULL == BN_bin2bn(sig + 32, 32, (ec_sig->s))) {
          //dumpOpenSslErrors();
         }
        printf("post s  :%s\n", BN_bn2hex(ec_sig->s));
*/




	return 0;
}

int ecdsaSign(char* privateKey) {return 0;
/*
    sgx_status_t ecall_status;

	sgx_ec256_private_t p_private;
    sgx_ec256_signature_t p_signature;

    int k;
    for(k = 0; k < 32; k++) {
    	sprintf(&p_private.r[k*2], "%02X",p_public.gx[k]);
    }




    p_private.r = (uint8_t*)&privateKey;

    uint8_t sample_data[8]
        = {0x12, 0x13, 0x3f, 0x00,
           0x9a, 0x02, 0x10, 0x53};

    size_t sample_data_len = sizeof(sample_data) / sizeof(sample_data[0]);

    sgx_status_t sgxEcdsaSign(global_eid, &ecall_status, uint8_t* sample_data, size_t sample_data_len, sgx_ec256_private_t* p_private, sgx_ec256_signature_t* p_signature)

    ocall_print("ecdsa signature x");
    ocall_uint32_t_print(p_signature.x, SGX_NISTP_ECP256_KEY_SIZE);
    ocall_print("ecdsa signature y");
    ocall_uint32_t_print(p_signature.y, SGX_NISTP_ECP256_KEY_SIZE);

	return 0;
	*/
}













