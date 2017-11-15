package swarmdb 

import (
/*
        "bytes"
        "crypto/sha256"
        "encoding/hex"
        "encoding/json"
        "errors"
        "golang.org/x/crypto/nacl/box"
        "io"
        "io/ioutil"
        "mime"
        "mime/multipart"
        "os"
        "path"
        "strconv"
        "time"

        "github.com/ethereum/go-ethereum/common"
        "github.com/ethereum/go-ethereum/swarm/storage"
        "github.com/rs/cors"
        "github.com/ethereum/go-ethereum/accounts/keystore"
        "github.com/ethereum/go-ethereum/accounts"
        "github.com/ethereum/go-ethereum/crypto"
*/
	"fmt"
	"github.com/ethereum/go-ethereum/log"
        "net/http"
        "strings"
        "sync"

	"github.com/robertkrimen/otto"
        api "github.com/ethereum/go-ethereum/swarm/api"
	//"github.com/robertkrimen/otto/repl"
	//"github.com/xwb1989/sqlparser"
)

type Server struct {
        api *api.Api
        sk [32]byte
        pk [32]byte
}

// Request wraps http.Request and also includes the parsed bzz URI
type Request struct {
        http.Request

        uri *api.URI
}

func SWARMDB_add(tbl_name string, rec *otto.Object) (succ bool) {
	// RODNEY/MAYUMI: CONNECT TO dispatch.go -- get table descriptor, get primary key's index type, ...
	fmt.Printf("swarmdb.SWARMDB_add(%s, ", tbl_name)
	for _, k := range rec.Keys() {
		v, _ := rec.Get(k)
		fmt.Printf(" key: %s value: %s", k, v)
	}
	fmt.Printf(")\n");
	return true
}

func (s *Server) SwarmDbUploadKademlia(owner string, table string, key string, content string) {
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
}

func logDebug(format string, v ...interface{}) {
        log.Debug(fmt.Sprintf("[SWARMDB] HTTP: "+format, v...))
}
