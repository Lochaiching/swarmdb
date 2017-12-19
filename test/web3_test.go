package keymanager_test

import (
	// "bytes"
	"github.com/ethereum/go-ethereum/crypto"
	"fmt"
	// "encoding/hex"
	"strings"
	"testing"
	"github.com/btcsuite/btcd/btcec"
	//"crypto/elliptic"
)

/*
 This shows that we can get the same values as the Web3 example
 https://ethereum.stackexchange.com/questions/23701/can-i-web3-eth-sign-with-private-key
 > web3.eth.accounts.sign("Hello, world!", '0x0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef')
 { message: 'Hello, world!',
 messageHash: '0xb453bd4e271eed985cbab8231da609c4ce0a9cf1f763b6c1594e76315510e0f1',
v: '0x1b',
r: '0x3bc843a917d6c19c487c1d0c660cdd61389ce2a7651ee3171bcc212ffddca164',
s: '0x193f1f2e06f7ed8f9fbf2254232d99848a8102b552032b68a5507b4d81492f0f',
signature: '0x3bc843a917d6c19c487c1d0c660cdd61389ce2a7651ee3171bcc212ffddca164193f1f2e06f7ed8f9fbf2254232d99848a8102b552032b68a5507b4d81492f0f1b' }

*/
func TestWeb3SignMatch(t *testing.T) {
	data := []byte("Hello, world!")

	// this is how Ethereum signs messages https://github.com/ethereum/go-ethereum/wiki/Management-APIs#personal_sign
	msg := fmt.Sprintf("\x19Ethereum Signed Message:\n%d%s", len(data), data)
	msg_hash := crypto.Keccak256([]byte(msg))
	correct_msg_hash := "b453bd4e271eed985cbab8231da609c4ce0a9cf1f763b6c1594e76315510e0f1"
	out := fmt.Sprintf("%x", msg_hash)
	fmt.Printf("messageHash: %s\n", out) 
	if strings.Compare(out, correct_msg_hash) == 0 {
		fmt.Printf("CORRECT\n")
	} else {
		t.Fatalf("INCORRECT\n")
	}

	secretKeyRaw := "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef";
	secretKey, err := crypto.HexToECDSA(secretKeyRaw) 
	if err != nil {
		t.Fatal("Failure to get secretKey");
	} else {
		fmt.Printf("Key: %x\n", secretKey)
	}
	correct_signature := "3bc843a917d6c19c487c1d0c660cdd61389ce2a7651ee3171bcc212ffddca164193f1f2e06f7ed8f9fbf2254232d99848a8102b552032b68a5507b4d81492f0f1b"

	
	sig, err2 := crypto.Sign(msg_hash, secretKey)
	if err2 != nil {
		t.Fatalf("ERR2: cannot sign hash %v", err2)
	} else {
		// https://github.com/wolktoken/swarm.wolk.com/blob/886ba45e294fc40c9481e41041e1f88d8fe3a901/src/github.com/ethereum/go-ethereum/internal/ethapi/api.go#L374
		sig[64] += 27
		r := sig[0:32]
		s := sig[32:64]
		v := sig[64] + 27 
		fmt.Printf("v: 0x%x\n  r: 0x%x\n  s: 0x%x\n  signature: 0x%x\n", v, r, s, sig)
		if strings.Compare(fmt.Sprintf("%x", sig), correct_signature) == 0 {
			fmt.Printf("CORRECT\n");
		} else {
			t.Fatal("INCORRECT\n");
		}
	}

	// btcec.SignCompact 
	sig2, err3 := btcec.SignCompact(btcec.S256(), (*btcec.PrivateKey)(secretKey), msg_hash, false)
	v := sig2[0] // - 27
	copy(sig2, sig2[1:])
	sig2[64] = v
	if err3 != nil {
		t.Fatalf("ERR2: cannot btcec.SignCompact hash %v", err2)
	} else {
		fmt.Printf("btcec.SignCompact: 0x%x\n", sig2)
	}
}
