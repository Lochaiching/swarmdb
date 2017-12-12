package common

import (
	"crypto/sha256"
	"encoding/binary"
	//storage "github.com/ethereum/go-ethereum/swarmdb/storage"
	"database/sql"
	"fmt"
	ethcommon "github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/swarmdb/keymanager"
	"math"
	"math/big"
	"sync"
	"time"
)

type DBChunkstore struct {
	db *sql.DB
	km *keymanager.KeyManager

	//file directory
	filepath string
	statpath string

	//persisted fields
	nodeid string
	farmer ethcommon.Address
	claims map[string]*big.Int

	//persisted stats
	chunkR int64
	chunkW int64
	chunkS int64

	//temp fields
	chunkRL int64
	chunkWL int64
	chunkSL int64

	launchDT time.Time
	lwriteDT time.Time
	logDT    time.Time
}

type ENSSimulation struct {
	filepath string
	db       *sql.DB
}

type KademliaDB struct {
	swarmdb   *SwarmDB
	mutex     sync.Mutex
	owner     []byte
	tableName []byte
	column    []byte
}

type SwarmDB struct {
	tables       map[string]map[string]*Table
	dbchunkstore *DBChunkstore // Sqlite3 based
	ens          ENSSimulation
	kaddb        *KademliaDB
}

type IndexInfo struct {
	indexname string
	indextype string
	roothash  []byte
	dbaccess  Database
	active    int
	primary   int
	keytype   int
}

type Table struct {
	swarmdb   SwarmDB
	tablename string
	ownerID   string
	roothash  []byte
	indexes   map[string]*IndexInfo
	primary   string
	counter   int //// not supported yet.
}

type DBChunkstorage interface {
	RetrieveDBChunk(key []byte) (val []byte, err error)
	StoreDBChunk(val []byte) (key []byte, err error)
}

type Database interface {
	GetRootHash() ([]byte, error)

	// Insert: adds key-value pair (value is an entire recrod)
	// ok - returns true if new key added
	// Possible Errors: KeySizeError, ValueSizeError, DuplicateKeyError, NetworkError, BufferOverflowError
	Insert(key []byte, value []byte) (bool, error)

	// Put -- inserts/updates key-value pair (value is an entire record)
	// ok - returns true if new key added
	// Possible Errors: KeySizeError, ValueSizeError, NetworkError, BufferOverflowError
	Put(key []byte, value []byte) (bool, error)

	// Get - gets value of key (value is an entire record)
	// ok - returns true if key found, false if not found
	// Possible errors: KeySizeError, NetworkError
	Get(key []byte) ([]byte, bool, error)

	// Delete - deletes key
	// ok - returns true if key found, false if not found
	// Possible errors: KeySizeError, NetworkError, BufferOverflowError
	Delete(key []byte) (bool, error)

	// Start/Flush - any buffered updates will be flushed to SWARM on FlushBuffer
	// ok - returns true if buffer started / flushed
	// Possible errors: NoBufferError, NetworkError
	StartBuffer() (bool, error)
	FlushBuffer() (bool, error)

	// Close - if buffering, then will flush buffer
	// ok - returns true if operation successful
	// Possible errors: NetworkError
	Close() (bool, error)

	// prints what is in memory
	Print()
}

type OrderedDatabase interface {
	Database

	// Seek -- moves cursor to key k
	// ok - returns true if key found, false if not found
	// Possible errors: KeySizeError, NetworkError
	Seek(k []byte /*K*/) (e OrderedDatabaseCursor, ok bool, err error)
}

type OrderedDatabaseCursor interface {
	Next() (k []byte /*K*/, v []byte /*V*/, err error)
	Prev() (k []byte /*K*/, v []byte /*V*/, err error)
}

type KeyType int

const (
	KT_INTEGER = 1
	KT_STRING  = 2
	KT_FLOAT   = 3
	KT_BLOB    = 4
)

func KeyToString(keytype KeyType, k []byte) (out string) {
	switch keytype {
	case KT_BLOB:
		return fmt.Sprintf("%v", k)
	case KT_STRING:
		return fmt.Sprintf("%s", string(k))
	case KT_INTEGER:
		a := binary.BigEndian.Uint64(k)
		return fmt.Sprintf("%d [%x]", a, k)
	case KT_FLOAT:
		bits := binary.BigEndian.Uint64(k)
		f := math.Float64frombits(bits)
		return fmt.Sprintf("%f", f)
	}
	return "unknown key type"
}

func ValueToString(v []byte) (out string) {
	if IsHash(v) {
		return fmt.Sprintf("%x", v)
	} else {
		return fmt.Sprintf("%v", string(v))
	}
}

func EmptyBytes(hashid []byte) (valid bool) {
	valid = true
	for i := 0; i < len(hashid); i++ {
		if hashid[i] != 0 {
			return false
		}
	}
	return valid
}
func IsHash(hashid []byte) (valid bool) {
	cnt := 0
	for i := 0; i < len(hashid); i++ {
		if hashid[i] == 0 {
			cnt++
		}
	}
	if cnt > 3 {
		return false
	} else {
		return true
	}
}

func IntToByte(i int) (k []byte) {
	k = make([]byte, 8)
	binary.BigEndian.PutUint64(k, uint64(i))
	return k
}

func FloatToByte(f float64) (k []byte) {
	bits := math.Float64bits(f)
	k = make([]byte, 8)
	binary.BigEndian.PutUint64(k, bits)
	return k
}

func SHA256(inp string) (k []byte) {
	h := sha256.New()
	h.Write([]byte(inp))
	k = h.Sum(nil)
	return k
}

type RequestOption struct {
	RequestType  string        `json:"requesttype"` //"OpenConnection, Insert, Get, Put, etc"
	Owner        string        `json:"owner,omitempty"`
	Table        string        `json:"table,omitempty"` //"contacts"
	Index        string        `json:"index,omitempty"`
	Key          string        `json:"key,omitempty"`   //value of the key, like "rodney@wolk.com"
	Value        string        `json:"value,omitempty"` //value of val, usually the whole json record
	TableOptions []TableOption `json:"tableoptions",omitempty"`
}

type TableOption struct {
	TreeType string `json:"treetype,omitempty"` //BT or HD
	Index    string `json:"index,omitempty"` //Column Name
	KeyType  int    `json:"keytype,omitempty"` //INTEGER, STRING, etc ..
	Primary  int    `json:"primary,omitempty" //1 - Primary 0 - not Primary`
}

type TableNotExistError struct {
}

func (t *TableNotExistError) Error() string {
	return fmt.Sprintf("Table does not exist")
}

type KeyNotFoundError struct {
}

func (t *KeyNotFoundError) Error() string {
	return fmt.Sprintf("Key not found")
}

type KeySizeError struct {
}

func (t *KeySizeError) Error() string {
	return fmt.Sprintf("Key size too large")
}

type ValueSizeError struct {
}

func (t *ValueSizeError) Error() string {
	return fmt.Sprintf("Value size too large")
}

type DuplicateKeyError struct {
}

func (t *DuplicateKeyError) Error() string {
	return fmt.Sprintf("Duplicate key error")
}

type NetworkError struct {
}

func (t *NetworkError) Error() string {
	return fmt.Sprintf("Network error")
}

type NoBufferError struct {
}

func (t *NoBufferError) Error() string {
	return fmt.Sprintf("No buffer error")
}

type BufferOverflowError struct {
}

func (t *BufferOverflowError) Error() string {
	return fmt.Sprintf("Buffer overflow error")
}

type RequestFormatError struct {
}

func (t *RequestFormatError) Error() string {
	return fmt.Sprintf("Request format error")
}

type NoColumnError struct {
}

func (t *NoColumnError) Error() string {
	return fmt.Sprintf("No column --- in the table")
}

func (self SwarmDB) RetrieveDBChunk(key []byte) (val []byte, err error) {
	val, err = self.dbchunkstore.RetrieveChunk(key)
	return val, err
}

func (self SwarmDB) StoreDBChunk(val []byte) (key []byte, err error) {
	key, err = self.dbchunkstore.StoreChunk(val)
	return key, err
}
