package swarmdb

import (
	"bytes"
	//	"encoding/binary"
	"fmt"
	//	"github.com/ethereum/go-ethereum/common"
	//	"github.com/ethereum/go-ethereum/crypto/sha3"
	"github.com/ethereum/go-ethereum/log"
	"github.com/ethereum/go-ethereum/swarm/api"
	"github.com/ethereum/go-ethereum/swarm/storage"
	//	"io"
	//	"reflect"
	"crypto/sha256"
	"strconv"
	"strings"
	"sync"
	"time"
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
	blockNumber := []byte("100") //How does this get retrieved? passed in?

	var metadataBody []byte
	metadataBody = make([]byte, 108)
	copy(metadataBody[0:41], self.owner)
	copy(metadataBody[42:59], buyAt)
	copy(metadataBody[60:91], blockNumber)
	copy(metadataBody[92:107], timestamp)
	log.Debug("Metadata is [%+v]", metadataBody)

	contentPrefix := BuildSwarmdbPrefix(string(self.owner), string(self.tableName), string(key))
	/*
		encryptedBodycontent := s.EncryptData(bodycontent)
		testDecrypt := s.DecryptData(encryptedBodycontent)
		s.logDebug("Initial BodyContent is [%s][%+v]", bodycontent, bodycontent)
		s.logDebug("Decrypted test is [%s][%+v]", testDecrypt, testDecrypt)
		s.logDebug("Encrypted is [%+v]", encryptedBodycontent)
	*/
	var mergedBodycontent []byte
	mergedBodycontent = make([]byte, 4088)
	copy(mergedBodycontent[:], metadataBody)
	copy(mergedBodycontent[512:576], contentPrefix)
	copy(mergedBodycontent[577:], value) // expected to be the encrypted body content

	log.Debug("ContentPrefix: [%+v]", string(contentPrefix))
	//log.Debug("Content: [%+v][%+v]", bodycontent, encryptedBodycontent)
	log.Debug("Merged Body Content: [%v]", mergedBodycontent)
	return (mergedBodycontent)
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

func (self *KademliaDB) Put(k, v []byte) (bool, error) {
	postData := string(v)
	postDataLen := int64(len(postData))
	dbwg := &sync.WaitGroup{}
	rdb := strings.NewReader(postData)

	//Take the Hash returned for the stored 'Main' content and store it
	raw_indexkey, err := self.api.StoreDB(rdb, postDataLen, dbwg)
	log.Debug(fmt.Sprintf("In KademliaDB rawkey [%v] ", raw_indexkey))
	if err != nil {
		//s.Error(w, r, err)
		return false, err
	}
	return true, nil
}

func (self *KademliaDB) Get(k []byte) ([]byte, bool, error) {
	chunkKey := self.GenerateChunkKey(k)

	//column = strings.ToLower(path_parts[2])
	contentReader := self.api.Retrieve(chunkKey)
	if _, err := contentReader.Size(nil); err != nil {
		log.Debug("key not found %s: %s", chunkKey, err)
		return nil, false, fmt.Errorf("key not found: %s", err)
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
	return queryResponse, true, nil
}

func (self *KademliaDB) GenerateChunkKey(k []byte) storage.Key {
	owner := strings.ToLower(string(self.owner))
	table := strings.ToLower(string(self.tableName))
	id := strings.ToLower(string(k))
	contentPrefix := BuildSwarmdbPrefix(owner, table, id)
	keylen := 64 ///////..........
	dummy := bytes.Repeat([]byte("Z"), keylen)
	keybase := contentPrefix + string(dummy)
	chunker := storage.NewTreeChunker(storage.NewChunkerParams())
	rd := strings.NewReader(keybase)
	key, _ := chunker.Split(rd, int64(len(keybase)), nil, nil, nil, false)
	log.Debug(fmt.Sprintf("In KademliaDB prefix [%v] dummy [%v] Keybase [%s] key [%s] from [%s]", contentPrefix, dummy, keybase, key, k))
	fmt.Printf("In KademliaDB prefix [%v] dummy [%v] Keybase [%s] key [%s][%+v] from [%s]", contentPrefix, dummy, keybase, key, key, k)
	return key
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

func (self *KademliaDB) Delete(k []byte) (bool, error) {
	_, err := self.Put(k, nil)
	if err != nil {
		return false, err
	}
	return true, err
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
