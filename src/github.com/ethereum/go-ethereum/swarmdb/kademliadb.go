package swarmdb

import (
	"bytes"
	"crypto/sha256"
	"fmt"
	"github.com/ethereum/go-ethereum/crypto"
	"github.com/ethereum/go-ethereum/log"
	//	"strconv"
	//	"time"
)

const (
	chunkSize = 4096
)

func NewKademliaDB(dbChunkstore *DBChunkstore) (*KademliaDB, error) {
	kd := new(KademliaDB)
	kd.dbChunkstore = dbChunkstore
	kd.nodeType = []byte("K")
	return kd, nil
}

func (self *KademliaDB) Open(owner []byte, tableName []byte, column []byte, encrypted int) (bool, error) {
	self.owner = owner
	self.tableName = tableName
	self.column = column
	self.encrypted = encrypted

	return true, nil
}

func (self *KademliaDB) buildSdata(key []byte, value []byte) []byte {
	contentPrefix := BuildSwarmdbPrefix(self.owner, self.tableName, key)
	config, errConfig := LoadSWARMDBConfig(SWARMDBCONF_FILE)
	if errConfig != nil {
		fmt.Printf("Failure to open Config", errConfig)
		//TODO: Should we be passing in 'u' instead?
	}
	km, _ := NewKeyManager(&config)
	//TODO: KeyManagerCreateError

	var metadataBody []byte
	metadataBody = make([]byte, 156)
	copy(metadataBody[0:40], self.owner)
	copy(metadataBody[40:41], self.nodeType)
	copy(metadataBody[41:42], IntToByte(self.encrypted))
	copy(metadataBody[42:43], IntToByte(self.autoRenew))
	copy(metadataBody[43:51], IntToByte(self.minReplication))
	copy(metadataBody[51:59], IntToByte(self.maxReplication))

	unencryptedMetadata := metadataBody[0:59]
	msg := fmt.Sprintf("\x19Ethereum Signed Message:\n%d%s", len(unencryptedMetadata), unencryptedMetadata)
	msg_hash := crypto.Keccak256([]byte(msg))
	copy(metadataBody[59:91], msg_hash)

	sdataSig, _ := km.SignMessage(msg_hash)
	//TODO: SignMessageError

	copy(metadataBody[91:156], sdataSig)
	log.Debug("Metadata is [%+v]", metadataBody)

	var mergedBodycontent []byte
	mergedBodycontent = make([]byte, chunkSize)
	copy(mergedBodycontent[:], metadataBody)
	copy(mergedBodycontent[512:544], contentPrefix)
	copy(mergedBodycontent[577:], value) // expected to be the encrypted body content

	log.Debug("Merged Body Content: [%v]", mergedBodycontent)
	return (mergedBodycontent)
}

func (self *KademliaDB) Put(u *SWARMDBUser, k []byte, v []byte) ([]byte, error) {
	self.autoRenew = u.AutoRenew
	self.minReplication = u.MinReplication
	self.maxReplication = u.MaxReplication
	sdata := self.buildSdata(k, v)
	hashVal := sdata[512:544] // 32 bytes
	err := self.dbChunkstore.StoreKChunk(u, hashVal, sdata, self.encrypted)
	//TODO: PutError
	if err != nil {
		swErr := &SWARMDBError{ message: `Error putting data` + err.Error() }
		log.Error(swErr.Error())
		return hashVal, swErr
	}
	return hashVal, nil
}

func (self *KademliaDB) GetByKey(u *SWARMDBUser, k []byte) ([]byte, error) {
	chunkKey := self.GenerateChunkKey(k)
	content, err := self.Get(u, chunkKey)
	if err != nil {
		log.Debug("key not found %s: %s", chunkKey, err)
		return nil, fmt.Errorf("key not found: %s", err)
	}
	return content, nil
}

func (self *KademliaDB) Get(u *SWARMDBUser, h []byte) ([]byte, error) {
	contentReader, err := self.dbChunkstore.RetrieveKChunk(u, h)
	if bytes.Trim(contentReader, "\x00") == nil {
		return nil, nil
	}
	if err != nil {
		log.Debug("key not found %s: %s", h, err)
		return nil, fmt.Errorf("key not found: %s", err)
	}
	return contentReader, nil
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
//TODO: Implement Delete
func (self *KademliaDB) Delete(k []byte) (bool, error) {
	_, err := self.Put(k, nil)
	if err != nil {
		return false, err
	}
	return true, err
}

//TODO: Define difference between Insert and Put
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

//TODO: Define difference between Update and Put
func (self *Node) Update(updatekey []byte, updatevalue []byte) (newnode *Node, err error) {
	res, _ := self.Get(updatekey)
	if res != nil {
		err = fmt.Errorf("couldn't find the key for updating")
		return
	}
	return self, err
}
*/
