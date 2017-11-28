package api

import (
	"bytes"
	//	"encoding/binary"
	"fmt"
	//	"github.com/ethereum/go-ethereum/common"
	//	"github.com/ethereum/go-ethereum/crypto/sha3"
	"github.com/ethereum/go-ethereum/log"
	"github.com/ethereum/go-ethereum/swarm/storage"
	//	"io"
	//	"reflect"
	//	"strconv"
	"crypto/sha256"
	"strings"
	"sync"
)

type KademliaDB struct {
	api       *Api
	mutex     sync.Mutex
	owner     []byte
	tableName []byte
	column    []byte
}

func NewKademliaDB(api *Api) (*KademliaDB, error) {
	kd := new(KademliaDB)
	kd.api = api
	return kd, nil
}

func (self *KademliaDB) Open(tableName []byte, column []byte) (err error) {
	self.tableName = tableName
	self.column = column

	return nil
}

func (self *KademliaDB) Insert(k, v []byte) error {
	res, _ := self.Get(k)
	if res != nil {
		err := fmt.Errorf("%s is already in Database", string(k))
		return err
	}
	err := self.Put(k, v)
	return err
}

func (self *KademliaDB) Put(k, v []byte) error {
	//log.Debug(fmt.Sprintf("In Kademlia r.uri(%v) r.uri.Path(%v) r.uri.Addr(%v)", r.uri, r.uri.Path, r.uri.Addr))

	postData := string(v)
	log.Debug("In KademliaDB postData PRESTORE (%v) ", postData)
	postDataLen := int64(len(postData))
	dbwg := &sync.WaitGroup{}
	rdb := strings.NewReader(postData)

	//Take the Hash returned for the stored 'Main' content and store it
	raw_indexkey, err := self.api.StoreDB(rdb, postDataLen, dbwg)
	if err != nil {
		//s.Error(w, r, err)
		return nil
	}
	log.Debug("Index content stored (postData=[%v]) for raw_indexkey.Log [%s] [%+v] (size of [%+v])", string(postData), raw_indexkey.Log(), raw_indexkey, postDataLen)
	log.Debug(fmt.Sprintf("KademliaDB Add ", self))
	return nil
}

func (self *KademliaDB) Get(k []byte) ([]byte, error) {
	keylen := 64 ///////..........
	dummy := bytes.Repeat([]byte("Z"), keylen)

	owner := strings.ToLower(string(self.owner))
	table := strings.ToLower(string(self.tableName))
	id := strings.ToLower(string(k))
	contentPrefix := BuildSwarmdbPrefix(owner, table, id)
	//column = strings.ToLower(path_parts[2])

	newkeybase := contentPrefix + string(dummy)
	chunker := storage.NewTreeChunker(storage.NewChunkerParams())
	rd := strings.NewReader(newkeybase)
	key, _ := chunker.Split(rd, int64(len(newkeybase)), nil, nil, nil, false)
	log.Debug(fmt.Sprintf("In KademliaDB prefix [%v] dummy %v newkeybase %v key %v", contentPrefix, dummy, newkeybase, k))

	contentReader := self.api.Retrieve(key)
	//reader := self.api.dpa.Retrieve(self.NodeHash)
	if _, err := contentReader.Size(nil); err != nil {
		log.Debug("key not found %s: %s", key, err)
		return nil, fmt.Errorf("key not found: %s", err)
	}

	contentReaderSize, _ := contentReader.Size(nil)
	contentBytes := make([]byte, contentReaderSize)
	_, _ = contentReader.ReadAt(contentBytes, 0)

	encryptedContentBytes := bytes.TrimRight(contentBytes[577:], "\x00")
	log.Debug(fmt.Sprintf("In HandledGetDB Retrieved 'mainhash' v[%v] s[%s] ", encryptedContentBytes, encryptedContentBytes))

	response_reader := bytes.NewReader(encryptedContentBytes)
	/* Current Plan is for Decryption to happen at SwarmDBManager Layer (so commenting out) */
	/*
	        decryptedContentBytes := s.DecryptData(encryptedContentBytes)
	        decrypted_reader := bytes.NewReader(decryptedContentBytes)
	        log.Debug(fmt.Sprintf("In HandledGetDB got back the 'reader' v[%v] s[%s] ", decrypted_reader, decrypted_reader))
		response_reader := decrypted_reader
	*/

	queryResponse := make([]byte, 4096)             //TODO: match to sizes in metadata content
	_, _ = response_reader.ReadAt(queryResponse, 0) //TODO: match to sizes in metadata content

	//queryResponseReader := bytes.NewReader(queryResponse)
	return queryResponse, nil
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

func (self *KademliaDB) Delete(k []byte) error {
	err := self.Put(k, nil)
	return err
}

func (self *KademliaDB) Close() {
	return
}

/*
func (self *Node) Update(updatekey []byte, updatevalue []byte) (newnode *Node, err error) {
	res, _ := self.Get(updatekey)
	if res != nil {
		err = fmt.Errorf("couldn't find the key for updating")
		return
	}
	return self, err
}
func convertToByte(a Val) []byte {
	log.Trace(fmt.Sprintf("convertToByte type: %v '%v'", a, reflect.TypeOf(a)))
	if va, ok := a.([]byte); ok {
		log.Trace(fmt.Sprintf("convertToByte []byte: %v '%v' %s", a, va, string(va)))
		return []byte(va)
	}
	if va, ok := a.(storage.Key); ok {
		log.Trace(fmt.Sprintf("convertToByte storage.Key: %v '%v' %s", a, va, string(va)))
		return []byte(va)
	} else if va, ok := a.(string); ok {
		return []byte(va)
	}
	return nil
}
*/
