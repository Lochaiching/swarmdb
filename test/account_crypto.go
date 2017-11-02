package main

import (
	"github.com/ethereum/go-ethereum/crypto"
	"github.com/ethereum/go-ethereum/accounts/keystore"
	"fmt"
    "io/ioutil"
    "golang.org/x/crypto/nacl/box"
    "github.com/ethereum/go-ethereum/accounts"    
)

func main() { 
	
	ks := keystore.NewKeyStore("/var/www/vhosts/data/keystore", keystore.StandardScryptN, keystore.StandardScryptP) 
	var ks_accounts []accounts.Account      //     type Account struct    in->   keystore/keystore.go
	ks_accounts = ks.Accounts()   
	acc_url := ks_accounts[0].URL   
	acc_url_string := fmt.Sprintf("%s", acc_url)
	filename := acc_url_string[11:]  // /var/www/vhosts/data/keystore/UTC--2017-10-13T23-15-16.214744640Z--dc8a520a69157a7087f0b575644b8e454f462159
            
    // Open the key file
    //keyJson, readErr := ioutil.ReadFile("/var/www/vhosts/data/keystore/UTC--2017-10-13T23-15-16.214744640Z--dc8a520a69157a7087f0b575644b8e454f462159")
    keyJson, readErr := ioutil.ReadFile(filename)    
    if readErr != nil {
        fmt.Println("key json read error:")
        panic(readErr)
    }

/*
address:
007a616728911ad2705693bc29697c4d6386ee77

private key:
bf26687f3cecf2840ed507a093b63bcd3a3f3f333e8096be8422c98e7e45a37f

public key:
8d9b8f37188f79d05d544e8d9034cdaea49fa4ca69de1af3c4a9b101466e65eb5dd720717be05572c784afac3258962b4f95a32d6f14d273c5767141efef473b

keystore JSON:
{"address":"007a616728911ad2705693bc29697c4d6386ee77","crypto":{"cipher":"aes-128-ctr","ciphertext":"03529e9cec070e295d27422c33c97e942b8bc41b22bee1686ec4b9c2f950b3a1","cipherparams":{"iv":"7c671d0e86b048fec226834e5daa472e"},"mac":"5bd200022438762fa730171a9c36a8dbef30f76966ca5261d91f9fc8c09445da","kdf":"pbkdf2","kdfparams":{"c":262144,"dklen":32,"prf":"hmac-sha256","salt":"b8337c66df68c2d94dbc4649f5ce815e5413d749a5252f5408b39eea9dd18ead"}},"id":"284a9ea8-46a2-434e-9461-8a8aa06f8b83","version":3}    

password for this JSON : asf
*/ 

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

	//address := keyWrapper.Address
    //fmt.Printf("Address:%x\n\n", address) // Address:dc8a520a69157a7087f0b575644b8e454f462159 
    
    sk := crypto.FromECDSA(keyWrapper.PrivateKey)
    
    pk :=   crypto.FromECDSAPub(&keyWrapper.PrivateKey.PublicKey)
    // fun call elliptic.Marshal   add  ret[0] = 4 // uncompressed point 
    // pk:[]byte{0x4, 0x8d, 0x9b,
    // need to remove the "ret[0] = 4" to get the pk
    pk = append(pk[:0], pk[1:]...)
    
    secretkey := [32]byte{}
    for i := range sk {
          secretkey[i] = sk[i]
          if i == 31 {break}  
    }
    
    // crypto/nacl  box.Seal()  box.Open()   PublicKey is  type *[32]byte  so cut the account PublicKey from [64]byte to [32]byte
    //publickey := [64]byte{}
    publickey := [32]byte{}
    for i := range pk {
          publickey[i] = pk[i]
          if i == 31 {break} 
    }
    
 
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


