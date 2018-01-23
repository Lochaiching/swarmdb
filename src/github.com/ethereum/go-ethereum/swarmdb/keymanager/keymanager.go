package keymanager

import (
	"bytes"
	"fmt"
	"github.com/ethereum/go-ethereum/accounts"
	"golang.org/x/crypto/nacl/box"

	"crypto/aes"
	"crypto/cipher"
	//"github.com/andreburgaud/crypt2go/ecb"
	//"github.com/andreburgaud/crypt2go/padding"
	"github.com/ethereum/go-ethereum/accounts/keystore"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/crypto"
	// "os"
)

const (
	PATH                 = "/swarmdb/data/keystore"
	WOLKSWARMDB_ADDRESS  = "9982ad7bfbe62567287dafec879d20687e4b76f5" 
	WOLKSWARMDB_PASSWORD = "wolkwolkwolk"
	WOLKSWARMDB_TESTKEY  = "4b0d79af51456172dfcc064c1b4b8f45f363a80a434664366045165ba5217d53"
)

type KeyManager struct {
	OwnerAccount accounts.Account
	OwnerAddress common.Address
	keystore     *keystore.KeyStore
	passphrase   string
	pk           []byte
	sk           []byte
	publicK      [32]byte
	secretK      [32]byte
	aescipher    cipher.Block
}

func NewKeyManager(path string, ownerAddress string, passphrase string) (keymgr KeyManager, err error) {
	keymgr.keystore = keystore.NewKeyStore(path, keystore.StandardScryptN, keystore.StandardScryptP)
	keymgr.passphrase = passphrase
	keymgr.OwnerAddress = common.HexToAddress(ownerAddress)

	wallets := keymgr.keystore.Wallets()
	for _, w := range wallets {
		accounts := w.Accounts()
		for _, a := range accounts {
			if bytes.Compare(a.Address.Bytes(), keymgr.OwnerAddress.Bytes()) == 0 {
				err := keymgr.keystore.Unlock(a, keymgr.passphrase)
				_, k, err := keymgr.keystore.WgetDecryptedKey(a, passphrase)
				if err != nil {
					return keymgr, err
				} else {
					// fmt.Printf("SUCC: %v %v Address 0x%x\n", a, k, accounts[0].Address)
					keymgr.OwnerAccount = a
					keymgr.sk = crypto.FromECDSA(k.PrivateKey)
					keymgr.pk = crypto.FromECDSAPub(&k.PrivateKey.PublicKey)
					// fmt.Printf("NewKeyManager secretkey: %x\n",  keymgr.sk)
					copy(keymgr.publicK[0:], keymgr.pk[0:])
					copy(keymgr.secretK[0:], keymgr.sk[0:])
					aescipher, errcip := aes.NewCipher(keymgr.sk)
					if errcip != nil {
						return keymgr, err
					}
					keymgr.aescipher = aescipher
					return keymgr, nil
				}
			} else {
				// fmt.Printf("Keystore Not found %x\n%x\n", accounts[0].Address.Bytes(), keymgr.OwnerAddress.Bytes())
			}
		}
	}

	return keymgr, fmt.Errorf("No keystore file found %x", ownerAddress)
}

func (self *KeyManager) GetPublicKey() []byte {
	return self.pk // crypto.FromECDSAPub(signerKey)
}

// sign message with the Keymanager initiated
func (self *KeyManager) SignMessage(msg_hash []byte) (sig []byte, err error) {
	secretKeyRaw := fmt.Sprintf("%x", self.sk) 
	// secretKeyRaw = SWARMDB_TESTKEY
	secretKey, err := crypto.HexToECDSA(secretKeyRaw)

	if err != nil {
		return sig, fmt.Errorf("Failure to get secretKey");
	} else {
		// sig, err = self.keystore.SignHash(self.OwnerAccount, msg_hash)
		sig, err2 := crypto.Sign(msg_hash, secretKey)
		if err2 != nil {
			return sig, err2
		} 
		return sig, nil
	}
}


func (self *KeyManager) VerifyMessage(msg_hash []byte, sig []byte) (verified bool, err error) {
	pubKey, err := crypto.SigToPub(msg_hash, sig)
	if err != nil {
		return false, fmt.Errorf("invalid signature - cannot get public key")
	} else {
		address2 := crypto.PubkeyToAddress(*pubKey)
		wallets := self.keystore.Wallets()
		for _, w := range wallets {
			accounts := w.Accounts()
			for _, a := range accounts {
				if bytes.Compare(a.Address.Bytes(), address2.Bytes()) == 0 {
					// FOUND MATCH
					return true, nil
				}

			}
		}

	}
	return false, fmt.Errorf("signer mismatch: %x != %x", crypto.FromECDSAPub(pubKey), self.pk)

}

/*
func (self *KeyManager) DecryptDataAES(src []byte) (dst []byte, err error) {
	mode := ecb.NewECBEncrypter(self.aescipher)
	padder := padding.NewPkcs7Padding(mode.BlockSize())
	dst, err = padder.Pad(src) // padd last block of plaintext if block size less than block cipher size
	if err != nil {
		return dst, err
	}
	dst = make([]byte, len(dst))
	mode.CryptBlocks(dst, src)
	return dst, err
}

func (self *KeyManager) EncryptDataAES(src []byte) (dst []byte, err error) {
	mode := ecb.NewECBDecrypter(self.aescipher)
	dst = make([]byte, len(src))
	mode.CryptBlocks(dst, src)
	padder := padding.NewPkcs7Padding(mode.BlockSize())
	dst, err = padder.Unpad(dst) // unpad plaintext after decryption
	if err != nil {
		return dst, err
	}
	return dst, err
}
*/
func (self *KeyManager) DecryptData(data []byte) []byte {
	var decryptNonce [24]byte
	copy(decryptNonce[:], data[:24])

	decrypted, ok := box.Open(nil, data[24:], &decryptNonce, &self.publicK, &self.secretK)
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
	encrypted := box.Seal(nonce[:], msg, &nonce, &self.publicK, &self.secretK)
	return encrypted
}
