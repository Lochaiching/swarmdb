package main

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/md5"
	"crypto/rand"
	"fmt"
	"hash"
	"io"
	"math/big"
	"os"
)


/*
 Bruce case
 private key: 0f93c9085eebe10e1f9e10651ae05afb9e2d9f5e92b1e2fb4ccc1cc04d508a30
 public key: 0415d2ab9d14850688bbb6026c03aa92b0be18a810873797d1682902dab69347f98bb41d02783e1a8477cb81f8ee1d6b0b75d0ff14b178387e1d0966708d06bd8d
 Original message: sAFcbjKkwBOCtyNJFroPxWqn
 sha256 hashed message: 2d4d5c5eb832810ad65ea8d62b2c74ea623c4ffdf80ddf5790c1a36729625034
 Signature: 3045022100bad7f18f2e2dc435429d34b0d7659e63ab3e7c5be2b2ca0ab197b252faa2079f02205c62899da743685d2c4cbc949613b1161cb75a1ad560faf84f9a70e9aef7acfd
 Verify: Signature is GOOD
*/
func main() {

	pubkeyCurve := elliptic.P256() //see http://golang.org/pkg/crypto/elliptic/#P256

	privatekey := new(ecdsa.PrivateKey)
	privatekey, err := ecdsa.GenerateKey(pubkeyCurve, rand.Reader) // this generates a public & private key pair
	
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
	
	var pubkey ecdsa.PublicKey
	pubkey = privatekey.PublicKey

	// private key: 0f93c9085eebe10e1f9e10651ae05afb9e2d9f5e92b1e2fb4ccc1cc04d508a30
	// public key:  0415d2ab9d14850688bbb6026c03aa92b0be18a810873797d1682902dab69347f98bb41d02783e1a8477cb81f8ee1d6b0b75d0ff14b178387e1d0966708d06bd8d
	fmt.Println("Private Key :")
	fmt.Printf("%x \n", privatekey)

	fmt.Println("Public Key :")
	fmt.Printf("%x \n", pubkey)

	// Sign ecdsa style
	var h hash.Hash
	h = md5.New()
	r := big.NewInt(0)
	s := big.NewInt(0)

	// sha256 hashed message: 2d4d5c5eb832810ad65ea8d62b2c74ea623c4ffdf80ddf5790c1a36729625034
	io.WriteString(h, "This is a message to be signed and verified by ECDSA!")
	signhash := h.Sum(nil)

	r, s, serr := ecdsa.Sign(rand.Reader, privatekey, signhash)
	if serr != nil {
		fmt.Println(err)
		os.Exit(1)
	}

	signature := r.Bytes()
	signature = append(signature, s.Bytes()...)

	// Signature: 3045022100bad7f18f2e2dc435429d34b0d7659e63ab3e7c5be2b2ca0ab197b252faa2079f02205c62899da743685d2c4cbc949613b1161cb75a1ad560faf84f9a70e9aef7acfd
	fmt.Printf("Signature : %x\n", signature)

	// Verify
	verifystatus := ecdsa.Verify(&pubkey, signhash, r, s)
	fmt.Println(verifystatus) // should be true
}
