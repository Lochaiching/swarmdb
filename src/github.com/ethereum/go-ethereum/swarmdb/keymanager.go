package swarmdb

import (
	"bytes"
	"fmt"
	// "github.com/ethereum/go-ethereum/accounts"
	"golang.org/x/crypto/nacl/box"

	// "crypto/aes"
	// "crypto/cipher"
	//"github.com/andreburgaud/crypt2go/ecb"
	//"github.com/andreburgaud/crypt2go/padding"
	"github.com/ethereum/go-ethereum/accounts/keystore"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/crypto"
	// "os"
)

type KeyManager struct {
	config   *SWARMDBConfig
	keystore *keystore.KeyStore
}

func NewKeyManager(c *SWARMDBConfig) (keymgr KeyManager, err error) {
	keymgr.config = c
	keymgr.keystore = keystore.NewKeyStore(c.ChunkDBPath, keystore.StandardScryptN, keystore.StandardScryptP)

	// for all users specified in the config file, set up their { sk, pk }  in the config
	wallets := keymgr.keystore.Wallets()
	for _, u := range c.Users {
		address := common.HexToAddress(u.Address)
		for _, w := range wallets {
			accounts := w.Accounts()
			for _, a := range accounts {
				if bytes.Compare(a.Address.Bytes(), address.Bytes()) == 0 {
					err := keymgr.keystore.Unlock(a, u.Passphrase)
					_, k, err := keymgr.keystore.WgetDecryptedKey(a, u.Passphrase)
					if err != nil {
						return keymgr, err
					} else {
						u.sk = crypto.FromECDSA(k.PrivateKey)
						u.pk = crypto.FromECDSAPub(&k.PrivateKey.PublicKey)

						copy(u.publicK[0:], u.pk[0:])
						copy(u.secretK[0:], u.sk[0:])
					}
				}
			}
		}
	}
	return keymgr, nil // fmt.Errorf("No keystore file found", ownerAddress)
}

// client libraries call this to sign messages (hashed to 32 bytes) with the config's PrivateKey
func (self *KeyManager) SignMessage(msg_hash []byte) (sig []byte, err error) {
	secretKey, err := crypto.HexToECDSA(self.config.PrivateKey)
	if err != nil {
		return sig, fmt.Errorf("Failure to get secretKey")
	} else {
		sig, err2 := crypto.Sign(msg_hash, secretKey)
		if err2 != nil {
			return sig, err2
		}
		return sig, nil
	}
}

// TCP server + HTTP use client response  to a challenge to determine which account the user is
func (self *KeyManager) VerifyMessage(msg_hash []byte, sig []byte) (u *SWARMDBUser, err error) {
	if sig[64] > 4 {
		sig[64] -= 27  // covers web3 (1b/1c) + go client (00/01)
	}
	pubKey, err := crypto.SigToPub(msg_hash, sig)
	if err != nil {
		return u, fmt.Errorf("invalid signature - cannot get public key")
	} else {
		address2 := crypto.PubkeyToAddress(*pubKey)
		for _, u0 := range self.config.Users {
			a := common.HexToAddress(u0.Address)
			fmt.Printf("%v \n", u0)
			if bytes.Compare(a.Bytes(), address2.Bytes()) == 0 {
				fmt.Printf("FOUND \n")
				return &u0, nil
			}
		}
		fmt.Printf("NOT FOUND \n")
		return u, fmt.Errorf("address not found: %x", address2.Bytes())
	}

}

func (self *KeyManager) DecryptData(u *SWARMDBUser, data []byte) []byte {
	var decryptNonce [24]byte
	copy(decryptNonce[:], data[:24])

	// decrypted, ok := box.Open(nil, data[24:], &decryptNonce, &self.publicK, &self.secretK)
	decrypted, ok := box.Open(nil, data[24:], &decryptNonce, &u.publicK, &u.secretK) // TODO
	if !ok {
		panic("decryption error")
	}
	return decrypted
}

func (self *KeyManager) EncryptData(u *SWARMDBUser, data []byte) []byte {
	var nonce [24]byte
	// fix required
	nonce = [24]byte{4, 0, 50, 203, 12, 81, 11, 49, 236, 255, 155, 11, 101, 6, 97, 233, 94, 169, 107, 4, 37, 57, 106, 151}
	msg := data //[]byte("Alas, poor Yorick! I knew him, Horatio")
	// encrypted := box.Seal(nonce[:], msg, &nonce, &self.publicK, &self.secretK)
	encrypted := box.Seal(nonce[:], msg, &nonce, &u.publicK, &u.secretK) // TODO
	return encrypted
}
