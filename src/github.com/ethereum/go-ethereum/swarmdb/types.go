package swarmdb

import (
	"bufio"
	"crypto/sha256"
	"database/sql"
	"encoding/binary"
	"encoding/json"
	"fmt"
	ethcommon "github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/swarmdb/log"
	"math"
	"math/big"
	"net"
	"strconv"
	"sync"
	"time"
)

//for passing request data from client to server if the request needs Table data
type Column struct {
	ColumnName string     `json:"columnname,omitempty"` // e.g. "accountID"
	IndexType  IndexType  `json:"indextype,omitempty"`  // IT_BTREE
	ColumnType ColumnType `json:"columntype,omitempty"`
	Primary    int        `json:"primary,omitempty"`
}

//for passing request data from client to server
type RequestOption struct {
	RequestType string  `json:"requesttype"` //"OpenConnection, Insert, Get, Put, etc"
	TableOwner  string  `json:"tableowner,omitempty"`
	Table       string  `json:"table,omitempty"` //"contacts"
	Encrypted   int     `json:"encrypted,omitempty"`
	Bid         float64 `json:"bid,omitempty"`
	Replication int     `json:"replication,omitempty"`
	Key         string  `json:"key,omitempty"` //value of the key, like "rodney@wolk.com"
	// Value       string   `json:"value,omitempty"` //value of val, usually the whole json record
	Rows     []Row    `json:"rows,omitempty"` //value of val, usually the whole json record
	Columns  []Column `json:"columns,omitempty"`
	RawQuery string   `json:"rawquery,omitempty"` //"Select name, age from contacts where email = 'blah'"
}

type SWARMDBConnection struct {
	connection net.Conn
	keymanager KeyManager
	ownerID    string
	reader     *bufio.Reader
	writer     *bufio.Writer
}

type SWARMDBTable struct {
	dbc       *SWARMDBConnection
	tableName string
}

type SWARMDBRow struct {
	cells map[string]string `json:"cells,omitempty"`
}

type NetstatFile struct {
	NodeID        string
	WalletAddress string
	Ticket        map[string]string
	ChunkStat     map[string]string
	ByteStat      map[string]string
	CStat         map[string]*big.Int `json:"-"`
	BStat         map[string]*big.Int `json:"-"`
	Claim         map[string]*big.Int `json:"-"`
	LaunchDT      *time.Time
	LReadDT       *time.Time
	LWriteDT      *time.Time
	LogDT         *time.Time
}

type DBChunkstore struct {
	db       *sql.DB
	km       *KeyManager
	farmer   ethcommon.Address
	netstat  *NetstatFile
	filepath string
	statpath string
}

type ENSSimulation struct {
	filepath string
	db       *sql.DB
}

type IncomingInfo struct {
	Data    string
	Address string
}

type KademliaDB struct {
	dbChunkstore   *DBChunkstore
	mutex          sync.Mutex
	owner          []byte
	tableName      []byte
	column         []byte
	nodeType       []byte
	updateCount    int
	encrypted      int
	autoRenew      int
	minReplication int
	maxReplication int
}

type SwarmDB struct {
	Logger       *swarmdblog.Logger
	tables       map[string]*Table
	dbchunkstore *DBChunkstore // Sqlite3 based
	ens          ENSSimulation
	kaddb        *KademliaDB
}

//for sql parsing
type QueryOption struct {
	Type           string //"Select" or "Insert" or "Update" probably should be an enum
	Table          string
	TableOwner     string
	RequestColumns []Column
	Inserts        []Row
	Update         map[string]interface{} //'SET' portion: map[columnName]value
	Where          Where
	Ascending      int //1 true, 0 false (descending)
}

//for sql parsing
type Where struct {
	Left     string
	Right    string //all values are strings in query parsing
	Operator string //sqlparser.ComparisonExpr.Operator; sqlparser.BinaryExpr.Operator; sqlparser.IsExpr.Operator; sqlparser.AndExpr.Operator, sqlparser.OrExpr.Operator
}

type ColumnInfo struct {
	columnName string
	indexType  IndexType
	roothash   []byte
	dbaccess   Database
	primary    uint8
	columnType ColumnType
}

type Table struct {
	buffered          bool
	swarmdb           *SwarmDB
	tableName         string
	ownerID           string
	roothash          []byte
	columns           map[string]*ColumnInfo
	primaryColumnName string
	encrypted         int
}

type Row struct {
	//primaryKeyValue interface{}
	Cells map[string]interface{}
}

type DBChunkstorage interface {
	RetrieveDBChunk(u *SWARMDBUser, key []byte) (val []byte, err error)
	StoreDBChunk(u *SWARMDBUser, val []byte, encrypted int) (key []byte, err error)
	PrintDBChunk(columnType ColumnType, hashid []byte, c []byte)
}

type Database interface {
	GetRootHash() ([]byte, error)

	// Insert: adds key-value pair (value is an entire recrod)
	// ok - returns true if new key added
	// Possible Errors: KeySizeError, ValueSizeError, DuplicateKeyError, NetworkError, BufferOverflowError
	Insert(u *SWARMDBUser, key []byte, value []byte) (bool, error)

	// Put -- inserts/updates key-value pair (value is an entire record)
	// ok - returns true if new key added
	// Possible Errors: KeySizeError, ValueSizeError, NetworkError, BufferOverflowError
	Put(u *SWARMDBUser, key []byte, value []byte) (bool, error)

	// Get - gets value of key (value is an entire record)
	// ok - returns true if key found, false if not found
	// Possible errors: KeySizeError, NetworkError
	Get(u *SWARMDBUser, key []byte) ([]byte, bool, error)

	// Delete - deletes key
	// ok - returns true if key found, false if not found
	// Possible errors: KeySizeError, NetworkError, BufferOverflowError
	Delete(u *SWARMDBUser, key []byte) (bool, error)

	// Start/Flush - any buffered updates will be flushed to SWARM on FlushBuffer
	// ok - returns true if buffer started / flushed
	// Possible errors: NoBufferError, NetworkError
	StartBuffer(u *SWARMDBUser) (bool, error)
	FlushBuffer(u *SWARMDBUser) (bool, error)

	// Close - if buffering, then will flush buffer
	// ok - returns true if operation successful
	// Possible errors: NetworkError
	Close(u *SWARMDBUser) (bool, error)

	// prints what is in memory
	Print(u *SWARMDBUser)
}

type OrderedDatabase interface {
	Database

	// Seek -- moves cursor to key k
	// ok - returns true if key found, false if not found
	// Possible errors: KeySizeError, NetworkError
	Seek(u *SWARMDBUser, k []byte /*K*/) (e OrderedDatabaseCursor, ok bool, err error)
	SeekFirst(u *SWARMDBUser) (e OrderedDatabaseCursor, err error)
	SeekLast(u *SWARMDBUser) (e OrderedDatabaseCursor, err error)
}

type OrderedDatabaseCursor interface {
	Next(*SWARMDBUser) (k []byte /*K*/, v []byte /*V*/, err error)
	Prev(*SWARMDBUser) (k []byte /*K*/, v []byte /*V*/, err error)
}

type ColumnType uint8

const (
	CT_INTEGER = 1
	CT_STRING  = 2
	CT_FLOAT   = 3
	CT_BLOB    = 4
)

type IndexType uint8

const (
	IT_NONE        = 0
	IT_HASHTREE    = 1
	IT_BPLUSTREE   = 2
	IT_FULLTEXT    = 3
	IT_FRACTALTREE = 4
)

// SwarmDB Configuration for a node kept here
const (
	SWARMDBCONF_FILE = "/swarmdb/swarmdb.conf"
)

type SWARMDBConfig struct {
	ListenAddrTCP string `json:"listenAddrTCP,omitempty"` // IP for TCP server
	PortTCP       int    `json:"portTCP,omitempty"`       // port for TCP server

	ListenAddrHTTP string `json:"listenAddrHTTP,omitempty"` // IP for HTTP server
	PortHTTP       int    `json:"portHTTP,omitempty"`       // port for HTTP server

	Address    string `json:"address,omitempty"`    // the address that earns, must be in keystore directory
	PrivateKey string `json:"privateKey,omitempty"` // to access child chain

	ChunkDBPath    string        `json:"chunkDBPath,omitempty"`    // the directory of the SQLite3 chunk databases
	Authentication int           `json:"authentication,omitempty"` // 0 - authentication is not required, 1 - required 2 - only users data stored
	UsersKeyPath   string        `json:"usersKeysPath,omitempty"`  // directory containing the keystore of Ethereum wallets
	Users          []SWARMDBUser `json:"users,omitempty"`          // array of users with permissions

	Currency            string  `json:"currency,omitempty"`            //
	TargetCostStorage   float64 `json:"targetCostStorage,omitempty"`   //
	TargetCostBandwidth float64 `json:"targetCostBandwidth,omitempty"` //
}

type SWARMDBUser struct {
	Address        string `json:"address,omitempty"`        //value of val, usually the whole json record
	Passphrase     string `json:"passphrase,omitempty"`     // password to unlock key in keystore directory
	MinReplication int    `json:"minReplication,omitempty"` // should this be in config
	MaxReplication int    `json:"maxReplication,omitempty"` // should this be in config
	AutoRenew      int    `json:"autoRenew,omitempty"`      // should this be in config
	pk             []byte
	sk             []byte
	publicK        [32]byte
	secretK        [32]byte
}

//for comparing rows in two different sets of data
func checkDuplicateRow(row1 Row, row2 Row) bool {

	//if row1.primaryKeyValue == row2.primaryKeyValue {
	//	return true
	//}

	for k1, r1 := range row1.Cells {
		if _, ok := row2.Cells[k1]; !ok {
			return true
		}
		if r1 != row2.Cells[k1] {
			return true
		}
	}
	for k2, r2 := range row2.Cells {
		if _, ok := row1.Cells[k2]; !ok {
			return true
		}
		if r2 != row1.Cells[k2] {
			return true
		}
	}

	return false
}

//gets data (Row.Cells) out of a slice of Rows, and rtns as one json.
func rowDataToJson(rows []Row) (string, error) {
	var resMap map[string]interface{}
	for _, row := range rows {
		for key, val := range row.Cells {
			if _, ok := resMap[key]; !ok {
				resMap[key] = val
			}
		}
	}
	resBytes, err := json.Marshal(resMap)
	if err != nil {
		return "", err
	}
	return string(resBytes), nil
}

//gets only the specified Columns (column name and value) out of a single Row, returns as a Row with only the relevant data
func filterRowByColumns(row *Row, columns []Column) (filteredRow Row) {
	//filteredRow.primaryKeyValue = row.primaryKeyValue
	filteredRow.Cells = make(map[string]interface{})
	for _, col := range columns {
		if _, ok := row.Cells[col.ColumnName]; ok {
			filteredRow.Cells[col.ColumnName] = row.Cells[col.ColumnName]
		}
	}
	return filteredRow
}

//used in client.go for user input
func convertStringToIndexType(in string) (out IndexType, err error) {
	switch in {
	case "hashtree":
		return IT_HASHTREE, nil
	case "bplustree":
		return IT_BPLUSTREE, nil
	case "fulltext":
		return IT_FULLTEXT, nil
	case "fractaltree":
		return IT_FRACTALTREE, nil
	}
	return out, fmt.Errorf("index %s not found", in) //KeyNotFoundError?
}

//used in client.go for user input
func convertStringToColumnType(in string) (out ColumnType, err error) {
	switch in {
	case "int":
		return CT_INTEGER, nil
	case "string":
		return CT_STRING, nil
	case "float":
		return CT_FLOAT, nil
	case "blob":
		return CT_BLOB, nil
	}
	return out, fmt.Errorf("columntype %s not found", in) //KeyNotFoundError?
}

func convertStringToKey(columnType ColumnType, key string) (k []byte) {
	k = make([]byte, 32)
	switch columnType {
	case CT_INTEGER:
		// convert using atoi to int
		i, _ := strconv.Atoi(key)
		k8 := IntToByte(i) // 8 byte
		copy(k, k8)        // 32 byte
	case CT_STRING:
		copy(k, []byte(key))
	case CT_FLOAT:
		f, _ := strconv.ParseFloat(key, 64)
		k8 := FloatToByte(f) // 8 byte
		copy(k, k8)          // 32 byte
	case CT_BLOB:
		// TODO: do this correctly with JSON treatment of binary
		copy(k, []byte(key))
	}
	return k
}

func KeyToString(columnType ColumnType, k []byte) (out string) {
	switch columnType {
	case CT_BLOB:
		return fmt.Sprintf("%v", k)
	case CT_STRING:
		return fmt.Sprintf("%s", string(k))
	case CT_INTEGER:
		a := binary.BigEndian.Uint64(k)
		return fmt.Sprintf("%d [%x]", a, k)
	case CT_FLOAT:
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

func BytesToFloat(b []byte) (f float64) {
	bits := binary.BigEndian.Uint64(b)
	f = math.Float64frombits(bits)
	return f
}

func BytesToInt64(b []byte) (i int64) {
	i = int64(binary.BigEndian.Uint64(b))
	return i
}

func SHA256(inp string) (k []byte) {
	h := sha256.New()
	h.Write([]byte(inp))
	k = h.Sum(nil)
	return k
}

type TableNotExistError struct {
	tableName string
	ownerID   string
}

func (t *TableNotExistError) Error() string {
	return fmt.Sprintf("Table [%s] with Owner [%s] does not exist", t.tableName, t.ownerID)
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
	tableOwner string
	tableName  string
	columnName string
}

func (t *NoColumnError) Error() string {
	return fmt.Sprintf("No column [%s] in the table [%s] owned by [%s]", t.tableName, t.columnName, t.tableOwner)
}
