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
	//"bytes"
	"fmt"
	"net/http"
	"strings"
	//"sync"

	"github.com/ethereum/go-ethereum/log"
	api "github.com/ethereum/go-ethereum/swarm/api"
	//"github.com/robertkrimen/otto"
	//"github.com/ethereum/go-ethereum/swarm/storage"
	//"github.com/robertkrimen/otto/repl"
	"encoding/json"
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
	switch index {
	case "kademlia":
		//fill in
	case "hash":
		//fill in
	case "btree":
		//fill in
	}

	return true
}

//data should be a pointer not actual structure
func SWARMDB_query(sql string) (data []string, err error) {

	//parse sql
	stmt, err := sqlparser.Parse(sql)
	if err != nil {
		fmt.Printf("sqlparser.Parse err: %v\n", err)
		return data, err
	}
	node := stmt.(*sqlparser.Select)
	//for i, e := range node.SelectExprs {
	//	fmt.Printf("FIELD %d: %+v\n", i, sqlparser.String(e)) // stmt.(*sqlparser.Select).SelectExprs)
	//}
	//really should only have 1 table
	//for i, e := range node.From {
	//	fmt.Printf("FROM %d: %s \n", i, sqlparser.String(e))
	//}
	if node.From == nil {
		return data, fmt.Errorf("no table specified")
	}

	//get table
	//data, primarykeycol = GET(node.From[0])  //not sure a primary key col is a feature we'll have
	//pretending this is the solution to whatever the query puts out... (i.e. the whole contacts table)
	var dataget []string
	dataget = append(dataget, `{ "email": "rodney@wolk.com", "name": "Rodney", "age": 38 }`)
	dataget = append(dataget, `{ "email": "alina@wolk.com", "name": "Alina", "age": 35 }`)
	dataget = append(dataget, `{ "email": "who@wolk.com", "name": "Who", "age": 38 }`)
	//pretending it also turns out a primary key col:
	//primarykeycol := "email"

	if node.Where != nil {
		fmt.Printf("WHERE: %s \n", sqlparser.String(node.Where))
		readable(node.Where.Expr)

		switch n := node.Where.Expr.(type) {
		case *sqlparser.OrExpr:
			left := strings.Split(sqlparser.String(n.Left), "=")
			right := strings.Split(sqlparser.String(n.Right), "=")
			for _, record := range dataget {
				rmap := make(map[string]interface{})
				if err := json.Unmarshal([]byte(record), &rmap); err != nil {
					return data, err
				}
				if (rmap[left[0]] == left[1]) || rmap[right[0]] == right[1] {
					data = append(data, record)
				}

			}
		case *sqlparser.AndExpr:
			left := strings.Split(sqlparser.String(n.Left), "=")
			right := strings.Split(sqlparser.String(n.Right), "=")
			for _, record := range dataget {
				rmap := make(map[string]interface{})
				if err := json.Unmarshal([]byte(record), &rmap); err != nil {
					return data, err
				}
				if (rmap[left[0]] == left[1]) && rmap[right[0]] == right[1] {
					data = append(data, record)
				}
			}
		case *sqlparser.IsExpr:

		}
	}

	//fmt.Printf("HAVING: %s \n", sqlparser.String(node.Having))

	//fmt.Printf("GROUPBY: %s \n", sqlparser.String(node.GroupBy))

	//fmt.Printf("ORDER BY: %s \n", sqlparser.String(node.OrderBy))

	//fmt.Printf(")\n")

	return data, nil
}

func SWARMDB_add(tablename string, record map[string]interface{}) (success bool, err error) {

	fmt.Printf("swarmdb.SWARMDB_add(%s, %+v)\n", tablename, record)
	for key, _ := range record {
		switch key {
		case "email":
			// fill in
		case "name":
			// fill in
		case "age":
			// fill in

		}
	}
	return true, nil
}

func SWARMDB_get(tbl_name string, id string) (jsonrecord string, err error) {

	// RODNEY/MAYUMI: CONNECT TO dispatch.go
	// get table descriptor, and based on the primary key's index, call dispatch.go
	fmt.Printf("swarmdb.SWARMDB_get(%s, %s)\n", tbl_name, id)

	index := GetColumnDesc("dummy", tbl_name, id)

	record := make(map[string]interface{})
	switch index {
	case "kademlia":
		record, err = SwarmDbDownloadKademlia("0x728781E75735dc0962Df3a51d7Ef47E798A7107E", tbl_name, id)
	case "hash":
		//record, err = SwarmDbDownloadKademlia( "dummy", tbl_name, id )

	case "btree":
		//record, err = SwarmDbDownloadKademlia( "dummy", tbl_name, id )
	}
	jr, _ := json.Marshal(record)
	jsonrecord = string(jr)
	if err != nil {
		return jsonrecord, err
	}
	//let's say this is the answer out of the swarmdb: (tbl_name: contacts, id: rodeny@wolk.com)
	jsonrecord = `{ "email": "rodney@wolk.com", "name": "Rodney", "age": 38 }`

	//return rec
	return jsonrecord, nil
}

func GetColumnDesc(owner string, table string, column string) (index string) {
	return "kademlia"
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

func SwarmDbDownloadKademlia(owner string, table string, id string) (record map[string]interface{}, err error) {
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
	           contentReader := api.Retrieve(key) //swarmdbapi.
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
	return record, err

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

//func andOperation(data ) {
//}

func readable(node sqlparser.Expr) string {
	switch node := node.(type) {
	case *sqlparser.OrExpr:
		return fmt.Sprintf("(%s or %s)", readable(node.Left), readable(node.Right))
	case *sqlparser.AndExpr:
		//fmt.Printf("got to AND expr (%s and %s)", readable(node.Left), readable(node.Right))
		return fmt.Sprintf("got to AND expr (%s and %s)", readable(node.Left), readable(node.Right))
	case *sqlparser.BinaryExpr:
		return fmt.Sprintf("(%s %s %s)", readable(node.Left), node.Operator, readable(node.Right))
	case *sqlparser.IsExpr:
		return fmt.Sprintf("(%s %s)", readable(node.Expr), node.Operator)
	default:
		return sqlparser.String(node)
	}
}
