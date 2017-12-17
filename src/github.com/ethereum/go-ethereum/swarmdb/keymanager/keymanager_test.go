package keymanager_test

import (
	"fmt"
	"bytes"
	"math/big"
	"github.com/ethereum/go-ethereum/crypto"
	// "encoding/binary"
	"github.com/ethereum/go-ethereum/swarmdb/keymanager"
	"testing"
)

func TestSignMessage(t *testing.T) {
	km, err := keymanager.NewKeyManager()
	if err != nil {
		t.Fatal("Failure to open KeyManager", err)
	}
	msg := "sourabh"
	sig, h, err := km.SignMessage(msg)
	if err != nil {
		t.Fatal("sign err", err)
	} else {
		fmt.Printf("%x\n", sig)
	}
	
	r := new(big.Int).SetBytes(sig[:32])
	s := new(big.Int).SetBytes(sig[32:64])
	v := sig[64]

	a := crypto.ValidateSignatureValues(v, r, s, false)
	if a {
		fmt.Printf("SUCCESS\n")
	} else {
		t.Fatal("Failure to verify")
	}
	p, err := crypto.Ecrecover(h, sig)
	if err != nil {
		t.Fatal("Failure to ecrecover")
	}  else {
		fmt.Printf("pubkey: %x\n", p)
	}

}

func aTestEncryptDecrypt(t *testing.T) {
	km, err := keymanager.NewKeyManager()
	if err != nil {
		t.Fatal("Failure to open KeyManager", err)
	}

	r := []byte("sourabh")

	encData := km.EncryptData(r)
	decData := km.DecryptData(encData)
	a := bytes.Compare(decData, r) 

	fmt.Printf("Encrypted data is [%v][%x]", encData, encData)
	fmt.Printf("Decrypted data is [%v][%s] => %d", decData, decData, a)

}
