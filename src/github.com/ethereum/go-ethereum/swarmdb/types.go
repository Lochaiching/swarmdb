// Copyright (c) 2018 Wolk Inc.  All rights reserved.

// The SWARMDB library is free software: you can redistribute it and/or modify
// it under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// The SWARMDB library is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU Lesser General Public License for more details.
//
// You should have received a copy of the GNU Lesser General Public License
// along with the go-ethereum library. If not, see <http://www.gnu.org/licenses/>.

package swarmdb

import (
	"bufio"
	"crypto/sha256"
	"database/sql"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"github.com/ethereum/go-ethereum/accounts/abi/bind"
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
	RequestType string      `json:"requesttype"` //"OpenConnection, Insert, Get, Put, etc"
	TableOwner  string      `json:"tableowner,omitempty"`
	Table       string      `json:"table,omitempty"` //"contacts"
	Encrypted   int         `json:"encrypted,omitempty"`
	Key         interface{} `json:"key,omitempty"` //value of the key, like "rodney@wolk.com"
	//TODO: Key should be a byte array or interface
	// Value       string   `json:"value,omitempty"` //value of val, usually the whole json record
	Rows     []Row    `json:"rows,omitempty"` //value of val, usually the whole json record
	Columns  []Column `json:"columns,omitempty"`
	RawQuery string   `json:"rawquery,omitempty"` //"Select name, age from contacts where email = 'blah'"
}

//shouldn't Data be an interface{}?
type SWARMDBResponse struct {
	ErrorCode        int    `json:"errorcode,omitempty"`
	ErrorMessage     string `json:"errormessage,omitempty"`
	Data             []Row  `json:"data,omitempty"`
	AffectedRowCount int    `json:"affectedrowcount,omitempty"`
	MatchedRowCount  int    `json:"matchedrowcount,omitempty"`
}

type SWARMDBConnection struct {
	connection net.Conn
	keymanager KeyManager
	ownerID    string //owner of the connection opened
	reader     *bufio.Reader
	writer     *bufio.Writer
}

type SWARMDBTable struct {
	dbc       *SWARMDBConnection
	tableName string
	encrypted int //means all transactions on the table are encrypted
	//replication int
	tableOwner string //owner of the table being accessed
}

//type SWARMDBRow struct {
//	cells map[string]string `json:"cells,omitempty"`
//}

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

type ENSSimple struct {
	auth *bind.TransactOpts
	sens *Simplestens
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

func NewRow() (r Row) {
	r.Cells = make(map[string]interface{})
	return r
}

func (r Row) Set(columnName string, val interface{}) {
	r.Cells[columnName] = val
}

type DBChunkstorage interface {
	RetrieveDBChunk(u *SWARMDBUser, key []byte) (val []byte, err error)
	StoreDBChunk(u *SWARMDBUser, val []byte, encrypted int) (key []byte, err error)
	PrintDBChunk(columnType ColumnType, hashid []byte, c []byte)
}

type Database interface {
	GetRootHash() []byte

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
	IT_NONE      = 0
	IT_HASHTREE  = 1
	IT_BPLUSTREE = 2
	IT_FULLTEXT  = 3
)

const (
	RT_CREATE_DATABASE   = "CreateDatabase"
	RT_DESCRIBE_DATABASE = "DescribeDatabase"
	RT_DROP_DATABASE     = "SelectDatabase"

	RT_CREATE_TABLE   = "CreateTable"
	RT_DESCRIBE_TABLE = "DescribeTable"
	RT_DROP_TABLES    = "DropTable"

	RT_PUT    = "Put"
	RT_GET    = "Get"
	RT_DELETE = "Delete"
	RT_QUERY  = "Query"
)

// SwarmDB Configuration Defaults
const (
	SWARMDBCONF_FILE                  = "/usr/local/swarmdb/etc/swarmdb.conf"
	SWARMDBCONF_DEFAULT_PASSPHRASE    = "wolk"
	SWARMDBCONF_CHUNKDB_PATH          = "/usr/local/swarmdb/data"
	SWARMDBCONF_KEYSTORE_PATH         = "/usr/local/swarmdb/data/keystore"
	SWARMDBCONF_ENSDOMAIN             = "ens.wolk.com"
	SWARMDBCONF_LISTENADDR            = "0.0.0.0"
	SWARMDBCONF_PORTTCP               = 2001
	SWARMDBCONF_PORTHTTP              = 8501
	SWARMDBCONF_PORTENS               = 8545
	SWARMDBCONF_CURRENCY              = "WLK"
	SWARMDBCONF_TARGET_COST_STORAGE   = 2.71828
	SWARMDBCONF_TARGET_COST_BANDWIDTH = 3.14159
)

type SWARMDBConfig struct {
	ListenAddrTCP string `json:"listenAddrTCP,omitempty"` // IP for TCP server
	PortTCP       int    `json:"portTCP,omitempty"`       // port for TCP server

	ListenAddrHTTP string `json:"listenAddrHTTP,omitempty"` // IP for HTTP server
	PortHTTP       int    `json:"portHTTP,omitempty"`       // port for HTTP server

	Address    string `json:"address,omitempty"`    // the address that earns, must be in keystore directory
	PrivateKey string `json:"privateKey,omitempty"` // to access child chain

	ChunkDBPath    string        `json:"chunkDBPath,omitempty"`    // the directory of the SQLite3 chunk databases (SWARMDBCONF_CHUNKDB_PATH)
	KeystorePath   string        `json:"usersKeysPath,omitempty"`  // directory containing the keystore of Ethereum wallets (SWARMDBCONF_KEYSTORE_PATH)
	Authentication int           `json:"authentication,omitempty"` // 0 - authentication is not required, 1 - required 2 - only users data stored
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
//only 1 cell in the row has to be different in order for the rows to be different
func isDuplicateRow(row1 Row, row2 Row) bool {

	//if row1.primaryKeyValue == row2.primaryKeyValue {
	//	return true
	//}

	for k1, r1 := range row1.Cells {
		if _, ok := row2.Cells[k1]; !ok {
			return false
		}
		if r1 != row2.Cells[k1] {
			return false
		}
	}

	for k2, r2 := range row2.Cells {
		if _, ok := row1.Cells[k2]; !ok {
			return false
		}
		if r2 != row1.Cells[k2] {
			return false
		}
	}

	return true
}

//gets data (Row.Cells) out of a slice of Rows, and rtns as one json.
func rowDataToJson(rows []Row) (string, error) {
	var resRows []map[string]interface{}
	for _, row := range rows {
		resRows = append(resRows, row.Cells)
	}
	resBytes, err := json.Marshal(resRows)
	if err != nil {
		return "", err
	}
	return string(resBytes), nil
}

//json input string should be []map[string]interface{} format
func JsonDataToRow(in string) (rows []Row, err error) {

	var jsonRows []map[string]interface{}
	if err = json.Unmarshal([]byte(in), &jsonRows); err != nil {
		return rows, err
	}
	for _, jRow := range jsonRows {
		row := NewRow()
		row.Cells = jRow
		rows = append(rows, row)
	}
	return rows, nil
}

func stringToColumnType(in string, columnType ColumnType) (out interface{}, err error) {
	switch columnType {
	case CT_INTEGER:
		out, err = strconv.Atoi(in)
	case CT_STRING:
		out = in
	case CT_FLOAT:
		out, err = strconv.ParseFloat(in, 64)
	//case: CT_BLOB:
	//?
	default:
		err = &SWARMDBError{message: "[types|stringToColumnType] columnType not found", ErrorCode: 434, ErrorMessage: fmt.Sprintf("ColumnType [%s] not SUPPORTED. Value [%s] rejected", columnType, in)}
	}
	return out, err
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

func CheckColumnType(colType ColumnType) bool {
	/*
		var ct uint8
		switch colType.(type) {
		case int:
			ct = uint8(colType.(int))
		case uint8:
			ct = colType.(uint8)
		case float64:
			ct = uint8(colType.(float64))
		case string:
			cttemp, _ := strconv.ParseUint(colType.(string), 10, 8)
			ct = uint8(cttemp)
		case ColumnType:
			ct = colType.(ColumnType)
		default:
			fmt.Printf("CheckColumnType not a type I can work with\n")
			return false
		}
	*/
	ct := colType
	if ct == CT_INTEGER || ct == CT_STRING || ct == CT_FLOAT { //|| ct == CT_BLOB {
		return true
	}
	return false
}

func CheckIndexType(it IndexType) bool {
	if it == IT_HASHTREE || it == IT_BPLUSTREE { //|| it == IT_FULLTEXT || it == IT_FRACTALTREE || it == IT_NONE {
		return true
	}
	return false
}

/*
//used in cli for user input
func ConvertStringToIndexType(in string) (out IndexType, err error) {
	switch in {
	case "hashtree":
		return IT_HASHTREE, nil
	case "IT_HASHTREE":
		return IT_HASHTREE, nil
	case "bplustree":
		return IT_BPLUSTREE, nil
	case "IT_BPLUSTREE":
		return IT_BPLUSTREE, nil
	case "fulltext":
		return IT_FULLTEXT, nil
	case "IT_FULLTEXT":
		return IT_FULLTEXT, nil
	case "fractaltree":
		return IT_FRACTALTREE, nil
	case "IT_FRACTALTREE":
		return IT_FRACTALTREE, nil
	case "":
		return out, fmt.Errorf("no index found")
	}
	return out, fmt.Errorf("index %s not found", in) //KeyNotFoundError?
}

//used in cli for user input
func ConvertStringToColumnType(in string) (out ColumnType, err error) {
	switch in {
	case "int":
		return CT_INTEGER, nil
	case "CT_INTEGER":
		return CT_INTEGER, nil
	case "string":
		return CT_STRING, nil
	case "CT_STRING":
		return CT_STRING, nil
	case "float":
		return CT_FLOAT, nil
	case "CT_FLOAT":
		return CT_FLOAT, nil
	case "blob":
		return CT_BLOB, nil
	case "CT_BLOB":
		return CT_BLOB, nil
	case "":
		return out, fmt.Errorf("no column type found")
	}
	return out, fmt.Errorf("columntype %s not found", in) //KeyNotFoundError?
}
*/

func StringToKey(columnType ColumnType, key string) (k []byte) {

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

type SWARMDBError struct {
	message      string
	ErrorCode    int
	ErrorMessage string
}

func (e *SWARMDBError) Error() string {
	return e.message
}

func (e *SWARMDBError) SetError(m string) {
	e.message = m
}

//for client output
//TODO: take the e.message out ... just for debugging at the moment
func (e *SWARMDBError) Print() string {
	return fmt.Sprintf("Error (%d): %s [%s]\n", e.ErrorCode, e.ErrorMessage, e.message)
}

func GenerateSWARMDBError(err error, msg string) (swErr error) {
	if wolkErr, ok := err.(*SWARMDBError); ok {
		return &SWARMDBError{message: msg, ErrorCode: wolkErr.ErrorCode, ErrorMessage: wolkErr.ErrorMessage}
	} else {
		return &SWARMDBError{message: msg}
	}
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
