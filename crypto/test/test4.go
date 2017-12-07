package main

import (
 
	"fmt"
)  

var	senderPrivateKey  *[32]byte
var	senderPublicKey  *[32]byte

func main() {

  fmt.Println("hello")
  
 senderPrivateKey = &[32]byte {240, 59, 251, 116, 145, 52, 30, 76, 203, 237, 108, 95, 200, 16, 23, 228, 142, 155, 177, 199, 104, 251, 204, 162, 90, 121, 34, 77, 200, 214, 204, 50}
 senderPublicKey  = &[32]byte {159, 34, 74, 113, 185, 191, 95, 49, 125, 184, 92, 125, 15, 82, 209, 53, 25, 124, 115, 138, 46, 218, 156, 199, 210, 169, 145, 81, 199, 191, 134, 74}

  
  fmt.Printf("senderPrivateKey:%v\n\n", senderPrivateKey)
  fmt.Printf("senderPublicKey:%v\n\n", senderPublicKey)
  
  
  
  
}