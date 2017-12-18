package keymanager_test

import (
	"bytes"
	"crypto/sha256"
	"fmt"
	// "github.com/ethereum/go-ethereum/crypto"
	// "encoding/binary"
	"github.com/ethereum/go-ethereum/swarmdb/keymanager"
	"testing"
)

const (
	TEST_MSG             = "sourabh"
	PATH                 = "/var/www/vhosts/sourabh/swarm.wolk.com/src/github.com/ethereum/go-ethereum/swarmdb/keymanager/"
	WOLKSWARMDB_ADDRESS  = "b6d1561697854dfa502140c8e2128f4ca4015b59"
	WOLKSWARMDB_PASSWORD = "h3r0c1ty!"
)

func TestSignVerifyMessage(t *testing.T) {
	km, err := keymanager.NewKeyManager(PATH, WOLKSWARMDB_ADDRESS, WOLKSWARMDB_PASSWORD)

	if err != nil {

		t.Fatal("Failure to open KeyManager", err)
	}
	msg := "swarmdb"

	h256 := sha256.New()
	h256.Write([]byte(msg))
	msg_hash := h256.Sum(nil)

	sig, err := km.SignMessage(msg_hash)
	if err != nil {
		t.Fatal("sign err", err)
	}

	verified, err := km.VerifyMessage(msg_hash, sig)
	if err != nil || !verified {
		t.Fatal("verify err", err)
	} else {
		fmt.Printf("Verified signature %x\n", sig)
	}
}

func failTestEncryptDecryptAES(t *testing.T) {
	km, err := keymanager.NewKeyManager(PATH, WOLKSWARMDB_ADDRESS, WOLKSWARMDB_PASSWORD)
	if err != nil {
		t.Fatal("Failure to open KeyManager", err)
	}

	msg := "0123456789abcdef"
	r := []byte(msg)

	encData, err2 := km.EncryptDataAES(r)
	if err2 != nil {
		t.Fatal(err2)
	}
	decData, err3 := km.DecryptDataAES(encData)
	if err3 != nil {
		t.Fatal(err3)
	}
	a := bytes.Compare(decData, r)
	if a != 0 {
		fmt.Printf("Encrypted data is [%v][%x]", encData, encData)
		fmt.Printf("Decrypted data is [%v][%s] => %d", decData, decData, a)
		t.Fatal("Failure to decrypt")
	} else {
		fmt.Printf("Success %s\n", msg)
	}

}

func TestEncryptDecrypt(t *testing.T) {
	km, err := keymanager.NewKeyManager(PATH, WOLKSWARMDB_ADDRESS, WOLKSWARMDB_PASSWORD)
	if err != nil {
		t.Fatal("Failure to open KeyManager", err)
	}

	msg := "0123456789abcdef"
	r := []byte(msg)

	encData := km.EncryptData(r)
	decData := km.DecryptData(encData)
	a := bytes.Compare(decData, r)
	if a != 0 {
		fmt.Printf("Encrypted data is [%v][%x]", encData, encData)
		fmt.Printf("Decrypted data is [%v][%s] => %d", decData, decData, a)
		t.Fatal("Failure to decrypt")
	} else {
		fmt.Printf("Success %s\n", msg)
	}

}
