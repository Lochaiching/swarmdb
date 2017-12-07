package main

/*
     #include "/root/downloads/nacl-20110221/build/wolk/include/amd64/crypto_box.h"
      
 
 
      
*/
import "C"

import "fmt"

 
func main() {

	fmt.Println("Hello\n");
	
//	string sk;
//	string pk;
 //   (crypto_box_PUBLICKEYBYTES )pk = C.crypto_box_keypair((crypto_box_SECRETKEYBYTES)&sk);
	n :=   C.crypto_box_PUBLICKEYBYTES
    pk0 := string(byteArray[:n])
    pk := C.CString(pk0); 
    
    n =  C.crypto_box_SECRETKEYBYTES;
    sk0 := string(byteArray[:n])
    sk := C.CString(sk0); 
    
    C.crypto_box_keypair(pk,sk);
	
	
	//fmt.Printf("rand: %d\n", n)
	
}





      
 