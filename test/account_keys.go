package main

import (
	"github.com/ethereum/go-ethereum/crypto"
	"github.com/ethereum/go-ethereum/accounts/keystore"
	"fmt"
    "io/ioutil"
    "golang.org/x/crypto/nacl/box"
    //"github.com/ethereum/go-ethereum/accounts"    
)

func main() { 
	  
//              ----------------- 1 -------------------	

    // Open the key file
    keyJson, readErr := ioutil.ReadFile("/var/www/vhosts/data/keystore/UTC--2017-10-13T23-15-16.214744640Z--dc8a520a69157a7087f0b575644b8e454f462159")
    if readErr != nil {
        fmt.Println("key json read error:")
        panic(readErr)
    }
 
//              ----------------- 2 -------------------	
   //keyJson := `{"address":"fd7e85a8465e96faadab5750e6efe364b3f197bd","crypto":{"cipher":"aes-128-ctr","ciphertext":"9d7dc4a809f4ec5ccad3f5ab6a69d927a9a932a41da1454b006ab0ff3d423b3e","cipherparams":{"iv":"3834d933e64256533e4c8b5bf44bb4b4"},"kdf":"scrypt","kdfparams":{"dklen":32,"n":262144,"p":1,"r":8,"salt":"9091c8a64cb69fc32e09b81325fc5bcf233854ed9ff585278b4de4f502bc9e61"},"mac":"c5f65dfbb83f0fe26dab104732dad06cba0c3c7ecdd3c6a37c6cc2f8b19d58b2"},"id":"33e7361e-057e-45b9-a41f-42eff9dae047","version":3}`
   //Private Key (unencrypted)   ->      16455fc56f66ec5d4100c64d225ea772f0e84de31482908ae218bb6cfb5633cb
//              ----------------- 3 -------------------	
   //keyJson := `{"address":"007a616728911ad2705693bc29697c4d6386ee77","crypto":{"cipher":"aes-128-ctr","ciphertext":"03529e9cec070e295d27422c33c97e942b8bc41b22bee1686ec4b9c2f950b3a1","cipherparams":{"iv":"7c671d0e86b048fec226834e5daa472e"},"mac":"5bd200022438762fa730171a9c36a8dbef30f76966ca5261d91f9fc8c09445da","kdf":"pbkdf2","kdfparams":{"c":262144,"dklen":32,"prf":"hmac-sha256","salt":"b8337c66df68c2d94dbc4649f5ce815e5413d749a5252f5408b39eea9dd18ead"}},"id":"284a9ea8-46a2-434e-9461-8a8aa06f8b83","version":3}`
 
/*
address:
007a616728911ad2705693bc29697c4d6386ee77

private key:
bf26687f3cecf2840ed507a093b63bcd3a3f3f333e8096be8422c98e7e45a37f

public key:
8d9b8f37188f79d05d544e8d9034cdaea49fa4ca69de1af3c4a9b101466e65eb5dd720717be05572c784afac3258962b4f95a32d6f14d273c5767141efef473b

keystore JSON:
{"address":"007a616728911ad2705693bc29697c4d6386ee77","crypto":{"cipher":"aes-128-ctr","ciphertext":"03529e9cec070e295d27422c33c97e942b8bc41b22bee1686ec4b9c2f950b3a1","cipherparams":{"iv":"7c671d0e86b048fec226834e5daa472e"},"mac":"5bd200022438762fa730171a9c36a8dbef30f76966ca5261d91f9fc8c09445da","kdf":"pbkdf2","kdfparams":{"c":262144,"dklen":32,"prf":"hmac-sha256","salt":"b8337c66df68c2d94dbc4649f5ce815e5413d749a5252f5408b39eea9dd18ead"}},"id":"284a9ea8-46a2-434e-9461-8a8aa06f8b83","version":3}    
*/ 

    // Get the private key
    keyWrapper, keyErr := keystore.DecryptKey([]byte(keyJson), "mdotm")   // mdotm     asf
    if keyErr != nil {
        fmt.Println("key decrypt error:")
        panic(keyErr)
    }
    
    /*  
        Id:         uuid.UUID(keyId),
		Address:    crypto.PubkeyToAddress(key.PublicKey),
		PrivateKey: key,
   */ 
   
	fmt.Printf("\n\n") 
	address := keyWrapper.Address
    fmt.Printf("Address:%v\n", address)   // Address:[0 122 97 103 40 145 26 210 112 86 147 188 41 105 124 77 99 134 238 119]     	   
    fmt.Printf("Address:%x\n\n", address) // Address:007a616728911ad2705693bc29697c4d6386ee77   
    
    sk := crypto.FromECDSA(keyWrapper.PrivateKey)
    fmt.Printf("sk:%v\n", sk)  // sk:[191 38 104 127 60 236 242 132 14 213 7 160 147 182 59 205 58 63 63 51 62 128 150 190 132 34 201 142 126 69 163 127] 
    fmt.Printf("sk:%#v\n", sk) // sk:[]byte{0xbf, 0x26, 0x68, 0x7f, 0x3c, 0xec, 0xf2, 0x84, 0xe, 0xd5, 0x7, 0xa0, 0x93, 0xb6, 0x3b, 0xcd, 0x3a, 0x3f, 0x3f, 0x33, 0x3e, 0x80, 0x96, 0xbe, 0x84, 0x22, 0xc9, 0x8e, 0x7e, 0x45, 0xa3, 0x7f}   
    fmt.Printf("sk:%x\n\n", sk)// sk:bf26687f3cecf2840ed507a093b63bcd3a3f3f333e8096be8422c98e7e45a37f
    
    pk :=   crypto.FromECDSAPub(&keyWrapper.PrivateKey.PublicKey)
    fmt.Printf("pk:%v\n", pk)  // pk:[4 141 155 143 55 24 143 121 208 93 84 78 141 144 52 205 174 164 159 164 202 105 222 26 243 196 169 177 1 70 110 101 235 93 215 32 113 123 224 85 114 199 132 175 172 50 88 150 43 79 149 163 45 111 20 210 115 197 118 113 65 239 239 71 59] 
    fmt.Printf("pk:%#v\n", pk) // pk:[]byte{0x4, 0x8d, 0x9b, 0x8f, 0x37, 0x18, 0x8f, 0x79, 0xd0, 0x5d, 0x54, 0x4e, 0x8d, 0x90, 0x34, 0xcd, 0xae, 0xa4, 0x9f, 0xa4, 0xca, 0x69, 0xde, 0x1a, 0xf3, 0xc4, 0xa9, 0xb1, 0x1, 0x46, 0x6e, 0x65, 0xeb, 0x5d, 0xd7, 0x20, 0x71, 0x7b, 0xe0, 0x55, 0x72, 0xc7, 0x84, 0xaf, 0xac, 0x32, 0x58, 0x96, 0x2b, 0x4f, 0x95, 0xa3, 0x2d, 0x6f, 0x14, 0xd2, 0x73, 0xc5, 0x76, 0x71, 0x41, 0xef, 0xef, 0x47, 0x3b}
    fmt.Printf("pk:%x\n", pk)// pk:048d9b8f37188f79d05d544e8d9034cdaea49fa4ca69de1af3c4a9b101466e65eb5dd720717be05572c784afac3258962b4f95a32d6f14d273c5767141efef473b    
    // fun call elliptic.Marshal   add  ret[0] = 4 // uncompressed point 
    // pk:[]byte{0x4, 0x8d, 0x9b,
    // need to remove the "ret[0] = 4" to get the pk
    pk = append(pk[:0], pk[1:]...)
    fmt.Printf("pk:%x\n\n", pk)// pk:8d9b8f37188f79d05d544e8d9034cdaea49fa4ca69de1af3c4a9b101466e65eb5dd720717be05572c784afac3258962b4f95a32d6f14d273c5767141efef473b


    secretkey := [32]byte{}
    for i := range sk {
          secretkey[i] = sk[i]
          if i == 31 {break}  
    }
    fmt.Printf("secretkey:%x\n\n", secretkey) // secretkey:bf26687f3cecf2840ed507a093b63bcd3a3f3f333e8096be8422c98e7e45a37f

    // crypto/nacl  box.Seal()  box.Open()   PublicKey is  type *[32]byte  so cut the account PublicKey from [64]byte to [32]byte
    //publickey := [64]byte{}
    publickey := [32]byte{}
    for i := range pk {
          publickey[i] = pk[i]
          if i == 31 {break} 
    }
    fmt.Printf("publickey:%x\n\n", publickey) // publickey:8d9b8f37188f79d05d544e8d9034cdaea49fa4ca69de1af3c4a9b101466e65eb

 
    senderPrivateKey := &secretkey
    senderPublicKey := &publickey

    recipientPrivateKey :=  senderPrivateKey
    recipientPublicKey  :=  senderPublicKey
 
    nonce := [24]byte {4, 0, 50, 203, 12, 81, 11, 49, 236, 255, 155, 11, 101, 6, 97, 233, 94, 169, 107, 4, 37, 57, 106, 151}
    //fmt.Printf("%v\n",nonce );

    msg := []byte("Alas, poor Yorick! I knew him, Horatio!")
    // This encrypts msg and appends the result to the nonce.
    encrypted := box.Seal(nonce[:], msg, &nonce, recipientPublicKey, senderPrivateKey)

    decryptNonce := nonce

    decrypted, ok := box.Open(nil, encrypted[24:], &decryptNonce, senderPublicKey, recipientPrivateKey)
    if !ok {
      panic("decryption error")
    }
    fmt.Println(string(decrypted))
	  
  
}


