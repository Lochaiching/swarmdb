package swarmdb

import (
	//	"encoding/binary"
	"fmt"
	//	"github.com/ethereum/go-ethereum/common"
	//	"github.com/ethereum/go-ethereum/crypto/sha3"
	"github.com/ethereum/go-ethereum/log"
	"github.com/ethereum/go-ethereum/swarm/api"
	//"github.com/ethereum/go-ethereum/swarm/storage"
	//	"io"
	//	"reflect"
	"crypto/sha256"
	"strconv"
	//"strings"
	"sync"
	"time"
)

const (
	chunkSize = 4096
)

type KademliaDB struct {
	api       *api.Api
	mutex     sync.Mutex
	owner     []byte
	tableName []byte
	column    []byte
}

func NewKademliaDB(api *api.Api) (*KademliaDB, error) {
	kd := new(KademliaDB)
	kd.api = api
	return kd, nil
}

func (self *KademliaDB) Open(owner []byte, tableName []byte, column []byte) (bool, error) {
	self.owner = owner
	self.tableName = tableName
	self.column = column

	return true, nil
}

func (self *KademliaDB) BuildSdata(key []byte, value []byte) []byte {
	buyAt := []byte("4096000000000000") //Need to research how to grab
	timestamp := []byte(strconv.FormatInt(time.Now().Unix(), 10))
	blockNumber := []byte("100")                         //How does this get retrieved? passed in?
	wlksig := []byte("6909ea88ced9c594e5212a1292fcf73c") //md5("wolk4all")

	var metadataBody []byte
	metadataBody = make([]byte, 140)
	copy(metadataBody[0:41], self.owner)
	copy(metadataBody[42:59], buyAt)
	copy(metadataBody[60:91], blockNumber)
	copy(metadataBody[92:107], timestamp)
	copy(metadataBody[108:139], wlksig)
	log.Debug("Metadata is [%+v]", metadataBody)

	contentPrefix := BuildSwarmdbPrefix(self.owner, self.tableName, key)
	/*
		encryptedBodycontent := s.EncryptData(bodycontent)
		testDecrypt := s.DecryptData(encryptedBodycontent)
		s.logDebug("Initial BodyContent is [%s][%+v]", bodycontent, bodycontent)
		s.logDebug("Decrypted test is [%s][%+v]", testDecrypt, testDecrypt)
		s.logDebug("Encrypted is [%+v]", encryptedBodycontent)
	*/
	var mergedBodycontent []byte
	mergedBodycontent = make([]byte, chunkSize)
	copy(mergedBodycontent[:], metadataBody)
	copy(mergedBodycontent[512:576], contentPrefix)
	copy(mergedBodycontent[577:], value) // expected to be the encrypted body content

	log.Debug("ContentPrefix: [%+v]", string(contentPrefix))
	//log.Debug("Content: [%+v][%+v]", bodycontent, encryptedBodycontent)
	log.Debug("Merged Body Content: [%v]", mergedBodycontent)
	return (mergedBodycontent)
}

func (self *KademliaDB) Put(k []byte, v []byte) (bool, error) {
	hashVal := v[512:576]
	//Need to put EXPECTED in there instead of 'v'
	raw_indexkey := self.api.StoreKDBChunk(hashVal, v)
	log.Debug(fmt.Sprintf("In KademliaDB rawkey [%v] ", raw_indexkey))
	return true, nil
}

func (self *KademliaDB) Get(k []byte) ([]byte, bool, error) {
	chunkKey := self.GenerateChunkKey(k)
	//column = strings.ToLower(path_parts[2])
	contentReader, err := self.api.RetrieveKDBChunk(chunkKey)
	if err != nil {
		log.Debug("key not found %s: %s", chunkKey, err)
		return nil, false, fmt.Errorf("key not found: %s", err)
	}
	return contentReader, true, nil
}

func (self *KademliaDB) GenerateChunkKey(k []byte) []byte {
	owner := self.owner
	table := self.tableName
	id := k
	contentPrefix := BuildSwarmdbPrefix(owner, table, id)
	log.Debug(fmt.Sprintf("\nIn GenerateChunkKey prefix Owner: [%s] Table: [%s] ID: [%s] == [%v](%s)", owner, table, id, contentPrefix, contentPrefix))
	return contentPrefix
}

func BuildSwarmdbPrefix(owner []byte, table []byte, id []byte) []byte {
	//hashType := "SHA3"
	//hashType := SHA256"

	//Should add checks for valid type / length for building
	prepLen := len(owner) + len(table) + len(id)
	prepBytes := make([]byte, prepLen)
	copy(prepBytes[0:], owner)
	copy(prepBytes[len(owner):], table)
	copy(prepBytes[len(owner)+len(table):], id)
	h256 := sha256.New()
	h256.Write([]byte(prepBytes))
	prefix := h256.Sum(nil)
	//fmt.Printf("\nIn BuildSwarmdbPrefix prepBytes[%s] and prefix[%x] in Bytes [%v] with size [%v]", prepBytes, prefix, []byte(prefix), len([]byte(prefix)))
	log.Debug(fmt.Sprintf("\nIn BuildSwarmdbPrefix prepstring[%s] and prefix[%s] in Bytes [%v] with size [%v]", prepBytes, prefix, []byte(prefix), len([]byte(prefix))))
	return (prefix)
}

func (self *KademliaDB) Close() (bool, error) {
	return true, nil
}

func (self *KademliaDB) FlushBuffer() (bool, error) {
	return true, nil
}

func (self *KademliaDB) StartBuffer() (bool, error) {
	return true, nil
}

func (self *KademliaDB) Print() {
	return
}

/*
func (self *KademliaDB) Delete(k []byte) (bool, error) {
	_, err := self.Put(k, nil)
	if err != nil {
		return false, err
	}
	return true, err
}

func (self *KademliaDB) Insert(k, v []byte) (bool, error) {
	res, _, _ := self.Get(k)
	if res != nil {
		err := fmt.Errorf("%s is already in Database", string(k))
		return false, err
	}
	_, err := self.Put(k, v)
	if err != nil {
		return false, nil
	}
	return true, err
}

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
