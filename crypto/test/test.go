package main

import (
	"github.com/ethereum/go-ethereum/crypto"
	"github.com/ethereum/go-ethereum/accounts/keystore"
//	"ioutil"
	"fmt"
//	"keystore"
//  "github.com/ethereum/go-ethereum/common"






 
)

func main() {

   /*    // Open the key file
    keyJson, readErr := ioutil.ReadFile("/var/www/vhost/UTC--2017-10-24T23-46-18.584407711Z--fd7e85a8465e96faadab5750e6efe364b3f197bd")
    if readErr != nil {
        fmt.Println("key json read error:")
        panic(readErr)
    }
   */

   keyJson := `{"address":"fd7e85a8465e96faadab5750e6efe364b3f197bd","crypto":{"cipher":"aes-128-ctr","ciphertext":"9d7dc4a809f4ec5ccad3f5ab6a69d927a9a932a41da1454b006ab0ff3d423b3e","cipherparams":{"iv":"3834d933e64256533e4c8b5bf44bb4b4"},"kdf":"scrypt","kdfparams":{"dklen":32,"n":262144,"p":1,"r":8,"salt":"9091c8a64cb69fc32e09b81325fc5bcf233854ed9ff585278b4de4f502bc9e61"},"mac":"c5f65dfbb83f0fe26dab104732dad06cba0c3c7ecdd3c6a37c6cc2f8b19d58b2"},"id":"33e7361e-057e-45b9-a41f-42eff9dae047","version":3}`
   //Private Key (unencrypted)   ->      16455fc56f66ec5d4100c64d225ea772f0e84de31482908ae218bb6cfb5633cb

    // Get the private key
    keyWrapper, keyErr := keystore.DecryptKey([]byte(keyJson), "mdotm")
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
    fmt.Printf("key text=%#v\n\n", keyWrapper.PrivateKey)
    fmt.Printf("%#v\n\n", keyWrapper)
    fmt.Printf("%#v\n\n", keyWrapper.Id)    
    fmt.Printf("%#v\n\n", keyWrapper.Address)    
    fmt.Printf("%#v\n\n", keyWrapper.PrivateKey)    
    fmt.Printf("%#v\n\n", keyWrapper.PrivateKey.PublicKey) 
    
    pk := crypto.FromECDSA(keyWrapper.PrivateKey)
    fmt.Printf("%v\n\n", pk)  // [22 69 95 197 111 102 236 93 65 0 198 77 34 94 167 114 240 232 77 227 20 130 144 138 226 24 187 108 251 86 51 203]  
    fmt.Printf("%#v\n\n", pk) // []byte{0x16, 0x45, 0x5f, 0xc5, 0x6f, 0x66, 0xec, 0x5d, 0x41, 0x0, 0xc6, 0x4d, 0x22, 0x5e, 0xa7, 0x72, 0xf0, 0xe8, 0x4d, 0xe3, 0x14, 0x82, 0x90, 0x8a, 0xe2, 0x18, 0xbb, 0x6c, 0xfb, 0x56, 0x33, 0xcb}   
      
    Public_Key :=   crypto.FromECDSAPub(&keyWrapper.PrivateKey.PublicKey)
    fmt.Printf("%#v\n\n", Public_Key) 
    fmt.Printf("%v\n\n", Public_Key)  
  
       
     
    //p_k :=   crypto.ToECDSAPub([]byte {22, 69, 95, 197, 111, 102, 236, 93, 65, 0, 198, 77, 34, 94, 167, 114, 240, 232, 77, 227, 20, 130, 144, 138, 226, 24, 187, 108, 251, 86, 51, 203})
    p_k :=   crypto.ToECDSAPub([]byte {240, 59, 251, 116, 145, 52, 30, 76, 203, 237, 108, 95, 200, 16, 23, 228, 142, 155, 177, 199, 104, 251, 204, 162, 90, 121, 34, 77, 200, 214, 204, 50})
    fmt.Printf("%v\n\n", p_k) 
    fmt.Printf("%#v\n\n", p_k) 
  
 
   
  
}