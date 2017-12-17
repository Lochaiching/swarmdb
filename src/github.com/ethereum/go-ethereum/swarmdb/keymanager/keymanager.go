package keymanager

import (
	"crypto/sha256"
	"fmt"
	"github.com/ethereum/go-ethereum/accounts"
	"github.com/ethereum/go-ethereum/accounts/keystore"
	"github.com/ethereum/go-ethereum/crypto"
	"golang.org/x/crypto/nacl/box"
//	"hash"
//	"io"
	"io/ioutil"
)

type KeyManager struct {
	keystore *keystore.Key
	pk       [32]byte
	sk       [32]byte
}

func NewKeyManager() (keymgr KeyManager, err error) {
	keystore, err := GetKeystore()
	if err != nil {
		return keymgr, err
	} else {
		keymgr.keystore = keystore
		sk, pk := ExtractPKSK(keymgr.keystore)
		copy(keymgr.sk[0:], sk)
		copy(keymgr.pk[0:], pk)
	}
	return keymgr, nil
}

func GetKeystore() (keyWrapper *keystore.Key, err error) {
	path := "/var/www/vhosts/sourabh/swarm.wolk.com/src/github.com/ethereum/go-ethereum/swarmdb/keymanager/"
	ks := keystore.NewKeyStore(path, keystore.StandardScryptN, keystore.StandardScryptP)
	var ks_accounts []accounts.Account //     type Account struct    in->   keystore/keystore.go
	ks_accounts = ks.Accounts()
	if len(ks_accounts) > 0 {
		acc_url := ks_accounts[0].URL
		acc_url_string := fmt.Sprintf("%s", acc_url)
		filename := acc_url_string[11:] // /var/www/vhosts/data/keystore/UTC--2017-10-13T23-15-16.214744640Z--dc8a520a69157a7087f0b575644b8e454f462159
		// Open the keystore file
		keyJson, readErr := ioutil.ReadFile(filename)
		if readErr != nil {
			return keyWrapper, readErr
		} else {
			keyWrapper, keyErr := keystore.DecryptKey([]byte(keyJson), "h3r0c1ty!")
			// fmt.Printf("KEYS: %v\n", keyWrapper)
			if keyErr != nil {
				return keyWrapper, keyErr
			} else {
				return keyWrapper, nil
			}
		}
	}
	return keyWrapper, fmt.Errorf("No accounts found")
}

func (self *KeyManager) SignMessage(msg string) (sig []byte, hashout []byte, err error) {
	privateKey := self.keystore.PrivateKey

	// The produced signature is in the [R || S || V] format where V is 0 or 1.
	h256 := sha256.New()
	h256.Write([]byte(msg))
	hashout = h256.Sum(nil)

	sig, err = crypto.Sign(hashout, privateKey)
	if err != nil {
		return sig, hashout, err
	}
	return sig, hashout, err
}

func ExtractPKSK(keyWrapper *keystore.Key) (acc_sk []byte, acc_pk []byte) {
	var sk [32]byte
	var pk [32]byte

	acc_sk = crypto.FromECDSA(keyWrapper.PrivateKey)
	acc_pk = crypto.FromECDSAPub(&keyWrapper.PrivateKey.PublicKey)
	acc_pk = append(pk[:0], pk[1:]...)
	// fun call elliptic.Marshal   add  ret[0] = 4 // uncompressed point
	// pk:[]byte{0x4, 0x8d, 0x9b,
	// need to remove the "ret[0] = 4" to get the pk

	//secretkey := [32]byte{}
	for i := range acc_sk {
		sk[i] = acc_sk[i]
		if i == 31 {
			break
		}
	}

	// crypto/nacl  box.Seal()  box.Open()   PublicKey is  type *[32]byte  so cut the account PublicKey from [64]byte to [32]byte
	//publickey := [64]byte{}
	//publickey := [32]byte{}
	for i := range acc_pk {
		pk[i] = acc_pk[i]
		if i == 31 {
			break
		}
	}
	return acc_sk, acc_pk
}

func (self *KeyManager) DecryptData(data []byte) []byte {
	var decryptNonce [24]byte
	copy(decryptNonce[:], data[:24])
	decrypted, ok := box.Open(nil, data[24:], &decryptNonce, &self.pk, &self.sk)
	if !ok {
		panic("decryption error")
	}
	return decrypted
}

func (self *KeyManager) EncryptData(data []byte) []byte {
	var nonce [24]byte
	// fix required
	nonce = [24]byte{4, 0, 50, 203, 12, 81, 11, 49, 236, 255, 155, 11, 101, 6, 97, 233, 94, 169, 107, 4, 37, 57, 106, 151}
	msg := data //[]byte("Alas, poor Yorick! I knew him, Horatio")
	encrypted := box.Seal(nonce[:], msg, &nonce, &self.pk, &self.sk)
	return encrypted
}

/*
func main() {
	privateFile := "f.prv"
	publicFile := "f.pub"
	// privateKey, publicKey := GenKeys(privateFile, publicFile)
	privateKey, publicKey := LoadKeys(privateFile, publicFile)
 }
*/
