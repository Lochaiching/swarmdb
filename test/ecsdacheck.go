package main

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/md5"
	"crypto/rand"
	"crypto/x509"
	"fmt"
	"hash"
	"io"
	"io/ioutil"
	"math/big"
	"os"
	"encoding/pem"
)

func fromHex(s string) *big.Int {
	r, ok := new(big.Int).SetString(s, 16)
	if !ok {
		panic("bad hex")
	}
	return r
}

func EncodeKeys(privateKey *ecdsa.PrivateKey, publicKey *ecdsa.PublicKey) (string, string) {
	x509Encoded, _ := x509.MarshalECPrivateKey(privateKey)
	pemEncoded := pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: x509Encoded})

	x509EncodedPub, _ := x509.MarshalPKIXPublicKey(publicKey)
	pemEncodedPub := pem.EncodeToMemory(&pem.Block{Type: "PUBLIC KEY", Bytes: x509EncodedPub})
	
	return string(pemEncoded), string(pemEncodedPub)
}

func DecodeKeys(pemEncoded string, pemEncodedPub string) (*ecdsa.PrivateKey, *ecdsa.PublicKey) {
	block, _ := pem.Decode([]byte(pemEncoded))
	
	x509Encoded := block.Bytes
	privateKey, _ := x509.ParseECPrivateKey(x509Encoded)
	
	blockPub, _ := pem.Decode([]byte(pemEncodedPub))
	x509EncodedPub := blockPub.Bytes
	genericPublicKey, _ := x509.ParsePKIXPublicKey(x509EncodedPub)
	publicKey := genericPublicKey.(*ecdsa.PublicKey)
	
	return privateKey, publicKey
}

func GenKeys(privateFile string, publicFile string) (privateKey *ecdsa.PrivateKey, publicKey *ecdsa.PublicKey) {
	privateKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
	publicKey = &privateKey.PublicKey
	
	priv, pub := EncodeKeys(privateKey, publicKey)


	f, err := os.Create(privateFile)
	if err != nil {
	} 
	defer f.Close()
	f.WriteString(priv)


	f2, err := os.Create(publicFile)
	if err != nil {
	} 
	defer f2.Close()
	f2.WriteString(pub)
	return privateKey, publicKey
}

func LoadKeys(privateFile string, publicFile string) (privateKey *ecdsa.PrivateKey, publicKey *ecdsa.PublicKey) {

	b, err := ioutil.ReadFile(privateFile) 
	if err != nil {
		fmt.Print(err)
	}

	c, err2 := ioutil.ReadFile(publicFile) 
	if err2 != nil {
		fmt.Print(err2)
	}

	privateKey, publicKey = DecodeKeys(string(b), string(c))
	return privateKey, publicKey
}

func SignMessage(msg string, privateKey *ecdsa.PrivateKey, publicKey *ecdsa.PublicKey) (signhash []byte, r *big.Int, s *big.Int, err error) {
	// Sign ecdsa style
	var h hash.Hash
	h = md5.New()
	r = big.NewInt(0)
	s = big.NewInt(0)

	// sha256 hashed message: 2d4d5c5eb832810ad65ea8d62b2c74ea623c4ffdf80ddf5790c1a36729625034
	io.WriteString(h, msg)
	signhash = h.Sum(nil)
	r, s, err = ecdsa.Sign(rand.Reader, privateKey, signhash)
	if err != nil {
		return signhash, r, s, err
	}
	return signhash, r, s
}

func main() {
	privateFile := "f.prv"
	publicFile := "f.pub"
	// privateKey, publicKey := GenKeys(privateFile, publicFile)
	privateKey, publicKey := LoadKeys(privateFile, publicFile)

	msg := "12344321"
	signhash, r, s := SignMessage(msg, privateKey, publicKey)
	// signature := r.Bytes()
	// signature = append(signature, s.Bytes()...)
	// Signature: 3045022100bad7f18f2e2dc435429d34b0d7659e63ab3e7c5be2b2ca0ab197b252faa2079f02205c62899da743685d2c4cbc949613b1161cb75a1ad560faf84f9a70e9aef7acfd

	// Verify
	verifystatus := ecdsa.Verify(publicKey, signhash, r, s)
	fmt.Println(verifystatus) // should be true
}
