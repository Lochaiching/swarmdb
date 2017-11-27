package swarmdb

import (
	/*
	   "encoding/hex"
	   "encoding/json"
	   "errors"
	   "io"
	   "io/ioutil"
	   "mime"
	   "mime/multipart"
	   "os"
	   "path"
	   "strconv"
	   "time"

	   "github.com/ethereum/go-ethereum/common"
	   "github.com/rs/cors"
	   "github.com/ethereum/go-ethereum/accounts/keystore"
	   "github.com/ethereum/go-ethereum/accounts"
	   "github.com/ethereum/go-ethereum/crypto"
	*/
	"crypto/sha256"
	"fmt"
	"net/http"
	"strings"
	//"sync"

	"github.com/ethereum/go-ethereum/log"
	api "github.com/ethereum/go-ethereum/swarm/api"
	"github.com/robertkrimen/otto"
	//"github.com/ethereum/go-ethereum/swarm/storage"
	//"github.com/robertkrimen/otto/repl"
	"github.com/xwb1989/sqlparser"
)

type Server struct {
	api *api.Api
	sk  [32]byte
	pk  [32]byte
}

// Request wraps http.Request and also includes the parsed bzz URI
type Request struct {
	http.Request

	uri *api.URI
}

func SWARMDB_createTable(tbl_name string, column string, primary bool, index string) (succ bool) {
	fmt.Printf("swarmdb.SWARMDB_createTable(%v, column: %v primary: %v index: %v)\n", tbl_name, column, primary, index)
	// RODNEY/MAYUMI: CONNECT TO dispatch.go -- create table descriptor (in LocalDB + ENS), ...
	return true
}

//func SWARMDB_query(sql string) (rec otto.Value) {
func SWARMDB_query(sql string) (jsonarray []string, err error) {
	// ALINA: FIGURE OUT HOW AN *** ARRAY ***  of JSON OBJECTS SHOULD BE RETURNED
	// fmt.Printf("sql is: %s\n", sql)
	stmt, err := sqlparser.Parse(sql)
	if err != nil {
		fmt.Printf("sqlparser.Parse err: %v\n", err)
		//return otto.Value{}
		return jsonarray, err
	}
	fmt.Printf("swarmdb.SWARMDB_query(%s, ", sqlparser.String(stmt.(*sqlparser.Select).From))

	for i, e := range stmt.(*sqlparser.Select).SelectExprs {
		fmt.Printf(" field %d: %+v", i, sqlparser.String(e)) // stmt.(*sqlparser.Select).SelectExprs)
	}
	fmt.Printf(")\n")
	//return otto.Value{}

	//pretending this is the solution to whatever the query puts out...
	jsonarray = append(jsonarray, `{ "email": "rodney@wolk.com", "name": "Rodney", "age": 38 }`)
	jsonarray = append(jsonarray, `{ "email": "alina@wolk.com", "name": "Alina", "age": 35 }`)
	return jsonarray, nil
}

func SWARMDB_add(tbl_name string, rec *otto.Object) (succ bool, err error) {
	// RODNEY/MAYUMI: CONNECT TO dispatch.go -- get table descriptor, get primary key's index type, ...
	fmt.Printf("swarmdb.SWARMDB_add(%s, ", tbl_name)
	for _, k := range rec.Keys() {
		v, _ := rec.Get(k)
		fmt.Printf(" key: %s value: %s", k, v)
	}
	fmt.Printf(")\n")
	return true, nil
}

func SWARMDB_get(tbl_name string, id string) (json string, err error) {
	// ALINA: FIGURE OUT HOW A JSON OBJECT SHOULD BE RETURNED

	// RODNEY/MAYUMI: CONNECT TO dispatch.go
	// get table descriptor, and based on the primary key's index, call dispatch.go
	fmt.Printf("swarmdb.SWARMDB_get(%s, %s)\n", tbl_name, id)

	index := GetColumnDesc("dummy", tbl_name, id)

	if index == "kademlia" {
		SwarmDbDownloadKademlia("0x728781E75735dc0962Df3a51d7Ef47E798A7107E", tbl_name, id)
	} else if index == "hash" {
		//SwarmDbDownloadKademlia( "dummy", tbl_name, id )
	} else if index == "btree" {
		//SwarmDbDownloadKademlia( "dummy", tbl_name, id )
	}

	//let's say this is the answer out of the swarmdb: (tbl_name: contacts, id: rodeny@wolk.com)
	json = `{ "email": "rodney@wolk.com", "name": "Rodney", "age": 38 }`

	//return rec
	return json, nil
}

func GetColumnDesc(owner string, table string, column string) (index string) {
	return "kademlia"
}

func BuildSwarmdbPrefix(owner string, table string, id string) string {
	//hashType := "SHA3"
	//hashType := SHA256"

	//Should add checks for valid type / length for building
	prepString := strings.ToLower(owner) + strings.ToLower(table) + strings.ToLower(id)
	h256 := sha256.New()
	h256.Write([]byte(prepString))
	prefix := fmt.Sprintf("%x", h256.Sum(nil))
	log.Debug(fmt.Sprintf("In BuildSwarmdbPrefix prepstring[%s] and prefix[%s] in Bytes [%v] with size [%v]", prepString, prefix, []byte(prefix), len([]byte(prefix))))
	return prefix
}

func SwarmDbUploadKademlia(owner string, table string, key string, content string) {
	/*
	   kvlen := int64(len(content))
	   dbwg := &sync.WaitGroup{}
	   rdb := strings.NewReader(content)

	   //Take the Hash returned for the stored 'Main' content and store it
	   raw_indexkey, err := s.api.StoreDB(rdb, kvlen, dbwg)
	   if err != nil {
	           //s.Error(w, r, err)
	           return
	   }
	   logDebug("Index content stored (kv=[%v]) for raw_indexkey.Log [%s] [%+v] (size of [%+v])", string(content), raw_indexkey.Log(), raw_indexkey, kvlen)
	*/
}

func SwarmDbDownloadKademlia(owner string, table string, id string) {
	/*
	           keylen := 64 ///////..........
	           dummy := bytes.Repeat([]byte("Z"), keylen)

	           contentPrefix := BuildSwarmdbPrefix(owner, table, id)
	           newkeybase := contentPrefix+string(dummy)
	           chunker := storage.NewTreeChunker(storage.NewChunkerParams())
	           rd := strings.NewReader(newkeybase)
	           key, err := chunker.Split(rd, int64(len(newkeybase)), nil, nil, nil, false)
	           log.Debug(fmt.Sprintf("In HandleGetDB prefix [%v] dummy %v newkeybase %v key %v", contentPrefix, dummy, newkeybase, key))

	   	fmt.Println("Key: ", key)
	           contentReader := swarmdbApi.Retrieve(key)
	           if _, err := contentReader.Size(nil); err != nil {
	                   log.Debug("key not found %s: %s", key, err)
	                   //http.NotFound(w, &r.Request)
	                   return
	           }
	           if err != nil {
	                   //s.Error(w, r, err)
	                   return
	           }

	           contentReaderSize,_ := contentReader.Size(nil)
	           contentBytes := make( []byte, contentReaderSize )
	           _,_ = contentReader.ReadAt( contentBytes, 0 )

	           encryptedContentBytes := bytes.TrimRight(contentBytes[577:],"\x00")
	           //encryptedContentBytes := contentBytes[len(contentPrefix):]
	           log.Debug(fmt.Sprintf("In HandledGetDB Retrieved 'mainhash' v[%v] s[%s] ", encryptedContentBytes, encryptedContentBytes))

	           //decrypted_reader := DecryptData(encryptedContentBytes)
	*/
}

/*
func DecryptData( data []byte ) []byte {
      	sk = [32]byte{240, 59, 251, 116, 145, 52, 30, 76, 203, 237, 108, 95, 200, 16, 23, 228, 142, 155, 177, 199, 104, 251, 204, 162, 90, 121, 34, 77, 200, 214, 204, 50}
      	pk = [32]byte{159, 34, 74, 113, 185, 191, 95, 49, 125, 184, 92, 125, 15, 82, 209, 53, 25, 124, 115, 138, 46, 218, 156, 199, 210, 169, 145, 81, 199, 191, 134, 74}

        var decryptNonce [24]byte
        //decryptNonce = [24]byte {4, 0, 50, 203, 12, 81, 11, 49, 236, 255, 155, 11, 101, 6, 97, 233, 94, 169, 107, 4, 37, 57, 106, 151}
        copy(decryptNonce[:], data[:24])
        decrypted, ok := box.Open(nil, data[24:], &decryptNonce, &pk, &sk)
        if !ok {
                panic("decryption error")
        }
        return decrypted
}

func EncryptData( data []byte ) []byte {
      	sk = [32]byte{240, 59, 251, 116, 145, 52, 30, 76, 203, 237, 108, 95, 200, 16, 23, 228, 142, 155, 177, 199, 104, 251, 204, 162, 90, 121, 34, 77, 200, 214, 204, 50}
      	pk = [32]byte{159, 34, 74, 113, 185, 191, 95, 49, 125, 184, 92, 125, 15, 82, 209, 53, 25, 124, 115, 138, 46, 218, 156, 199, 210, 169, 145, 81, 199, 191, 134, 74}

        var nonce [24]byte
        nonce = [24]byte {4, 0, 50, 203, 12, 81, 11, 49, 236, 255, 155, 11, 101, 6, 97, 233, 94, 169, 107, 4, 37, 57, 106, 151}
        msg := data //[]byte("Alas, poor Yorick! I knew him, Horatio")
        encrypted := box.Seal(nonce[:], msg, &nonce, &pk, &sk)
        return encrypted
}
*/
func logDebug(format string, v ...interface{}) {
	log.Debug(fmt.Sprintf("[SWARMDB] HTTP: "+format, v...))
}
