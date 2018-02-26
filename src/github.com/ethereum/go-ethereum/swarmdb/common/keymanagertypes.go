package common

//crypto lib should also be 'common' - where should that go?
import (
	"github.com/wolkdb/swarmdb/crypto"
)

// client libraries call this to sign messages (hashed to 32 bytes) with the config's secret key
// original version:
/*
func (self *KeyManager) SignMessage(msg_hash []byte) (sig []byte, err error) {
	secretKey, err := crypto.HexToECDSA(self.config.PrivateKey)
	if err != nil {
		return sig, &SWARMDBError{message: fmt.Sprintf("[keymanager:SignMessage] HexToECDSA  %s", err.Error()), ErrorCode: 455, ErrorMessage: "Keymanager Unable to Sign Message"}
	} else {
		sig, err2 := crypto.Sign(msg_hash, secretKey)
		if err2 != nil {
			return sig, &SWARMDBError{message: fmt.Sprintf("[keymanager:SignMessage] Sign %s", err.Error()), ErrorCode: 455, ErrorMessage: "Keymanager Unable to Sign Message"}
		}
		return sig, nil
	}
}
 */

// but would need privateKey passed in, i.e.
func SignMessage(msg_hash []byte, privateKey string) (sig []byte, err error) {                  
        secretKey, err := crypto.HexToECDSA(privateKey)                                         
        if err != nil {                                                                         
                return sig, &SWARMDBError{message: fmt.Sprintf("[swarmdblib:SignMessage] HexToECDSA  %s", err.Error()), ErrorCode: 455, ErrorMessage: "Unable to Sign Message"}                
        } else {                                                                                
                sig, err2 := crypto.Sign(msg_hash, secretKey)                                   
                if err2 != nil {                                                                
                        return sig, &SWARMDBError{message: fmt.Sprintf("[swarmdblib:SignMessage] Sign %s", err.Error()), ErrorCode: 455, ErrorMessage: "Unable to Sign Message"}               
                }                                                                               
                return sig, nil                                                                 
        }                                                                                       
}  