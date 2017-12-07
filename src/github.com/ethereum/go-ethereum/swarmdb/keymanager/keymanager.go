package keymanager

import (
        "fmt"
	"golang.org/x/crypto/nacl/box"
        //"io"
        "io/ioutil"

        "github.com/ethereum/go-ethereum/accounts"
        "github.com/ethereum/go-ethereum/accounts/keystore"
        "github.com/ethereum/go-ethereum/crypto"
        "github.com/ethereum/go-ethereum/log"
)

type KeyManager struct {
	pk [32]byte
	sk [32]byte
}

func NewKeyManager(path string) (keymgr KeyManager, err error) {
	keymgr.sk, keymgr.pk = GetKeys()
	return keymgr, nil
}

func GetKeys() (sk [32]byte, pk [32]byte) {
	ks := keystore.NewKeyStore("/var/www/vhosts/data/keystore", keystore.StandardScryptN, keystore.StandardScryptP)
	var ks_accounts []accounts.Account //     type Account struct    in->   keystore/keystore.go
	ks_accounts = ks.Accounts()
	var keyJson []byte
	var readErr error
	if( len(ks_accounts) > 0 ) {
		acc_url := ks_accounts[0].URL
		acc_url_string := fmt.Sprintf("%s", acc_url)
		filename := acc_url_string[11:] // /var/www/vhosts/data/keystore/UTC--2017-10-13T23-15-16.214744640Z--dc8a520a69157a7087f0b575644b8e454f462159

		// Open the key file
		//keyJson, readErr = ioutil.ReadFile("/var/www/vhosts/data/keystore/UTC--2017-10-13T23-15-16.214744640Z--dc8a520a69157a7087f0b575644b8e454f462159")
		keyJson, readErr = ioutil.ReadFile(filename)
	}
	if readErr != nil || len(ks_accounts) == 0 {
		//s.logDebug("SWARM server.go ReadFile of keystore file error: %s ", readErr)
		log.Debug(fmt.Sprintf("[BZZ] HTTP: "+"SWARM server.go ReadFile of keystore file error: %s ", readErr))

		// if ReadFile fail use default keys
		sk = [32]byte{240, 59, 251, 116, 145, 52, 30, 76, 203, 237, 108, 95, 200, 16, 23, 228, 142, 155, 177, 199, 104, 251, 204, 162, 90, 121, 34, 77, 200, 214, 204, 50}
		pk = [32]byte{159, 34, 74, 113, 185, 191, 95, 49, 125, 184, 92, 125, 15, 82, 209, 53, 25, 124, 115, 138, 46, 218, 156, 199, 210, 169, 145, 81, 199, 191, 134, 74}
		return sk, pk
	}

	keyWrapper, keyErr := keystore.DecryptKey([]byte(keyJson), "mdotm")
	if keyErr != nil || len(ks_accounts) == 0 {
		//s.logDebug("SWARM server.go DecryptKey error: %s ", keyErr)
		log.Debug(fmt.Sprintf("[BZZ] HTTP: "+"SWARM server.go DecryptKey error: %s ", keyErr))

		// if we don't know the pass use default keys
		sk = [32]byte{240, 59, 251, 116, 145, 52, 30, 76, 203, 237, 108, 95, 200, 16, 23, 228, 142, 155, 177, 199, 104, 251, 204, 162, 90, 121, 34, 77, 200, 214, 204, 50}
		pk = [32]byte{159, 34, 74, 113, 185, 191, 95, 49, 125, 184, 92, 125, 15, 82, 209, 53, 25, 124, 115, 138, 46, 218, 156, 199, 210, 169, 145, 81, 199, 191, 134, 74}
		return sk, pk
	}

	acc_sk := crypto.FromECDSA(keyWrapper.PrivateKey)

	acc_pk := crypto.FromECDSAPub(&keyWrapper.PrivateKey.PublicKey)
	// fun call elliptic.Marshal   add  ret[0] = 4 // uncompressed point
	// pk:[]byte{0x4, 0x8d, 0x9b,
	// need to remove the "ret[0] = 4" to get the pk
	acc_pk = append(pk[:0], pk[1:]...)

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

	//	sk = [32]byte{240, 59, 251, 116, 145, 52, 30, 76, 203, 237, 108, 95, 200, 16, 23, 228, 142, 155, 177, 199, 104, 251, 204, 162, 90, 121, 34, 77, 200, 214, 204, 50}
	//	pk = [32]byte{159, 34, 74, 113, 185, 191, 95, 49, 125, 184, 92, 125, 15, 82, 209, 53, 25, 124, 115, 138, 46, 218, 156, 199, 210, 169, 145, 81, 199, 191, 134, 74}

	return sk, pk
}

func (self *KeyManager) DecryptData( data []byte ) []byte {
      	//sk = [32]byte{240, 59, 251, 116, 145, 52, 30, 76, 203, 237, 108, 95, 200, 16, 23, 228, 142, 155, 177, 199, 104, 251, 204, 162, 90, 121, 34, 77, 200, 214, 204, 50}
      	//pk = [32]byte{159, 34, 74, 113, 185, 191, 95, 49, 125, 184, 92, 125, 15, 82, 209, 53, 25, 124, 115, 138, 46, 218, 156, 199, 210, 169, 145, 81, 199, 191, 134, 74}
        var decryptNonce [24]byte
        //decryptNonce = [24]byte {4, 0, 50, 203, 12, 81, 11, 49, 236, 255, 155, 11, 101, 6, 97, 233, 94, 169, 107, 4, 37, 57, 106, 151}
        copy(decryptNonce[:], data[:24])
        decrypted, ok := box.Open(nil, data[24:], &decryptNonce, &self.pk, &self.sk)
        if !ok {
                panic("decryption error")
        }
        return decrypted
}
func (self *KeyManager) EncryptData( data []byte ) []byte {
      	//sk = [32]byte{240, 59, 251, 116, 145, 52, 30, 76, 203, 237, 108, 95, 200, 16, 23, 228, 142, 155, 177, 199, 104, 251, 204, 162, 90, 121, 34, 77, 200, 214, 204, 50}
      	//pk = [32]byte{159, 34, 74, 113, 185, 191, 95, 49, 125, 184, 92, 125, 15, 82, 209, 53, 25, 124, 115, 138, 46, 218, 156, 199, 210, 169, 145, 81, 199, 191, 134, 74}
        var nonce [24]byte
        nonce = [24]byte {4, 0, 50, 203, 12, 81, 11, 49, 236, 255, 155, 11, 101, 6, 97, 233, 94, 169, 107, 4, 37, 57, 106, 151}
        msg := data //[]byte("Alas, poor Yorick! I knew him, Horatio")
        encrypted := box.Seal(nonce[:], msg, &nonce, &self.pk, &self.sk)
        return encrypted
}
