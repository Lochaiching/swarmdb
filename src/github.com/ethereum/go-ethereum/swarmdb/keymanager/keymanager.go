package keymanager

import (
	"bytes"
	"fmt"
	"github.com/ethereum/go-ethereum/accounts"
"golang.org/x/crypto/nacl/box"

	"github.com/ethereum/go-ethereum/accounts/keystore"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/crypto"
	"github.com/andreburgaud/crypt2go/ecb"
	"github.com/andreburgaud/crypt2go/padding"
	"crypto/aes"
	"crypto/cipher"
	// "os"
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
					copy(keymgr.publicK[0:], keymgr.pk[0:])
					copy(keymgr.secretK[0:], keymgr.sk[0:])

					aescipher, errcip := aes.NewCipher(keymgr.sk) 
					if errcip != nil {
						return keymgr, err
					}
					keymgr.aescipher = aescipher
				}
			} else {
				// fmt.Printf("Keystore Not found %x\n%x\n", accounts[0].Address.Bytes(), keymgr.OwnerAddress.Bytes())
				return keymgr, fmt.Errorf("No keystore file found %x", ownerAddress)
			}
		}
	}

	return keymgr, nil
}

func (self *KeyManager) GetPublicKey() []byte {
	return self.pk // crypto.FromECDSAPub(signerKey)
}

func (self *KeyManager) SignMessage(msg_hash []byte) (sig []byte, err error) {
	sig, err = self.keystore.SignHash(self.OwnerAccount, msg_hash)
	if err != nil {
		return sig, err
	}
	return sig, nil
}

func (self *KeyManager) VerifyMessage(msg_hash []byte, sig []byte) (verified bool, err error) {
	pubKey, err := crypto.SigToPub(msg_hash, sig)
	if err != nil {
		return false, err
	}
	if !bytes.Equal(crypto.FromECDSAPub(pubKey), self.pk) {
		return false, fmt.Errorf("signer mismatch: %x != %x", crypto.FromECDSAPub(pubKey), self.pk)
	}
	return true, nil

}

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

