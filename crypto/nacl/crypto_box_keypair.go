package main

 
import (
	 "fmt"
 "golang.org/x/crypto/nacl/box"
 //"io"
 //crypto_rand "crypto/rand" 
// "reflect"
)
 
func main() {

	fmt.Println("Hello\n");
	
//senderPublicKey, senderPrivateKey, err := box.GenerateKey(crypto_rand.Reader)
//if err != nil {
//    panic(err)
//}
//fmt.Println("senderP", reflect.TypeOf(senderPublicKey))
//a := *[32]byte(senderPublicKey)
//fmt.Println(a)
//a := fmt.Sprintf("%s\n",senderPrivateKey);
//fmt.Printf("%v\n",senderPublicKey ); 

//fmt.Printf("%v\n",senderPrivateKey );
//fmt.Printf("%v\n",senderPublicKey ); 

senderPrivateKey := &[32]byte {240, 59, 251, 116, 145, 52, 30, 76, 203, 237, 108, 95, 200, 16, 23, 228, 142, 155, 177, 199, 104, 251, 204, 162, 90, 121, 34, 77, 200, 214, 204, 50}
senderPublicKey  := &[32]byte {159, 34, 74, 113, 185, 191, 95, 49, 125, 184, 92, 125, 15, 82, 209, 53, 25, 124, 115, 138, 46, 218, 156, 199, 210, 169, 145, 81, 199, 191, 134, 74}

//fmt.Printf("%v\n",senderPrivateKey );
//fmt.Printf("%v\n",senderPublicKey ); 

//recipientPublicKey, recipientPrivateKey, err := box.GenerateKey(crypto_rand.Reader)
//if err != nil {
//    panic(err)
//}

recipientPrivateKey :=  senderPrivateKey
recipientPublicKey  :=  senderPublicKey
 
 

// You must use a different nonce for each message you encrypt with the
// same key. Since the nonce here is 192 bits long, a random value
// provides a sufficiently small probability of repeats.
//var nonce [24]byte
//if _, err := io.ReadFull(crypto_rand.Reader, nonce[:]); err != nil {
//    panic(err)
//}

 
nonce := [24]byte {4, 0, 50, 203, 12, 81, 11, 49, 236, 255, 155, 11, 101, 6, 97, 233, 94, 169, 107, 4, 37, 57, 106, 151}
//fmt.Printf("%v\n",nonce );

msg := []byte("Alas, poor Yorick! I knew him, Horatio!")
// This encrypts msg and appends the result to the nonce.
encrypted := box.Seal(nonce[:], msg, &nonce, recipientPublicKey, senderPrivateKey)

//fmt.Printf("%v\n",encrypted );


// The recipient can decrypt the message using their private key and the
// sender's public key. When you decrypt, you must use the same nonce you
// used to encrypt the message. One way to achieve this is to store the
// nonce alongside the encrypted message. Above, we stored the nonce in the
// first 24 bytes of the encrypted text.


//var decryptNonce [24]byte
//copy(decryptNonce[:], encrypted[:24])

decryptNonce := nonce

decrypted, ok := box.Open(nil, encrypted[24:], &decryptNonce, senderPublicKey, recipientPrivateKey)
if !ok {
    panic("decryption error")
}
fmt.Println(string(decrypted))
	
}





      
 