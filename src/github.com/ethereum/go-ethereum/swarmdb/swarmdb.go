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
	"bytes"
	"encoding/json"
	"fmt"
	"github.com/ethereum/go-ethereum/crypto"
        "github.com/ethereum/go-ethereum/crypto/sha3"
	"github.com/ethereum/go-ethereum/log"
	swarmdblog "github.com/ethereum/go-ethereum/swarmdb/log"
	"github.com/ethereum/go-ethereum/swarm/storage"
	"path/filepath"
	"strings"
)

const (
	OK_RESPONSE = "ok"
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
	RequestType string `json:"requesttype"` //"OpenConnection, Insert, Get, Put, etc"
	Owner       string `json:"owner,omitempty"`
	Database    string `json:"database,omitempty"`

	Table     string      `json:"table,omitempty"` //"contacts"
	Encrypted int         `json:"encrypted,omitempty"`
	Key       interface{} `json:"key,omitempty"` //value of the key, like "rodney@wolk.com"
	//TODO: Key should be a byte array or interface
	// Value       string   `json:"value,omitempty"` //value of val, usually the whole json record
	Rows     []Row    `json:"rows,omitempty"` //value of val, usually the whole json record
	Columns  []Column `json:"columns,omitempty"`
	RawQuery string   `json:"query,omitempty"` //"Select name, age from contacts where email = 'blah'"

}

//shouldn't Data be an interface{}?
type SWARMDBResponse struct {
	Error            *SWARMDBError `json:"error,omitempty"`
	ErrorCode        int           `json:"errorcode,omitempty"`
	ErrorMessage     string        `json:"errormessage,omitempty"`
	Data             []Row         `json:"data,omitempty"`
	AffectedRowCount int           `json:"affectedrowcount,omitempty"`
	MatchedRowCount  int           `json:"matchedrowcount,omitempty"`
}

func (resp *SWARMDBResponse) String() string {
	return resp.Stringify()
}

func (resp *SWARMDBResponse) Stringify() string {
	/*
	   wolkErr, ok := resp.Error.(*swarmdb.SWARMDBError)
	   if !ok {
	           return (`{ "errorcode":-1, "errormessage":"UNKNOWN ERROR"}`) //TODO: Make Default Error Handling
	   }
	   if wolkErr.ErrorCode == 0 { //FYI: default empty int is 0. maybe should be a pointer.  //TODO this is a hack with what errors are being returned right now
	           //fmt.Printf("wolkErr.ErrorCode doesn't exist\n")
	           respObj.ErrorCode = 474
	           respObj.ErrorMessage = resp.Error.Error()
	   } else {
	           respObj.ErrorCode = wolkErr.ErrorCode
	           respObj.ErrorMessage = wolkErr.ErrorMessage
	   }
	*/
	jbyte, jErr := json.Marshal(resp)
	if jErr != nil {
		//fmt.Printf("Error: [%s] [%+v]", jErr.Error(), resp)
		return `{ "errorcode":474, "errormessage":"ERROR Encountered Generating Response"}` //TODO: Make Default Error Handling
	}
	jstr := string(jbyte)
	return jstr
}

type SwarmDB struct {
	tables       map[string]*Table
	dbchunkstore *DBChunkstore // Sqlite3 based
	ens          ENSSimple
	Config       *SWARMDBConfig
	SwarmStore   storage.ChunkStore
	Logger       *swarmdblog.Logger
	SwapDB       *SwapDB
	Netstats     *Netstats
}

//for sql parsing
type QueryOption struct {
	Type           string //"Select" or "Insert" or "Update" probably should be an enum
	Owner          string
	Database       string
	Table          string
	Encrypted      int
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

func hashcolumn(k []byte) [32]byte {
        return sha3.Sum256(k)
}

type ColumnType string
type IndexType string
type RequestType string

const (
	CT_INTEGER = "INTEGER"
	CT_STRING  = "STRING"
	CT_FLOAT   = "FLOAT"
	CT_BLOB    = "BLOB"

	IT_NONE      = "NONE"
	IT_HASHTREE  = "HASH"
	IT_BPLUSTREE = "BPLUS"
	IT_FULLTEXT  = "FULLTEXT"

	RT_CREATE_DATABASE = "CreateDatabase"
	RT_LIST_DATABASES  = "ListDatabases"
	RT_DROP_DATABASE   = "DropDatabase"

	RT_CREATE_TABLE   = "CreateTable"
	RT_DESCRIBE_TABLE = "DescribeTable"
	RT_LIST_TABLES    = "ListTables"
	RT_DROP_TABLE     = "DropTable"
	RT_CLOSE_TABLE    = "CloseTable"

	RT_START_BUFFER = "StartBuffer"
	RT_FLUSH_BUFFER = "FlushBuffer"

	RT_PUT    = "Put"
	RT_GET    = "Get"
	RT_DELETE = "Delete"
	RT_QUERY  = "Query"
	RT_SCAN   = "Scan"

	DATABASE_NAME_LENGTH_MAX = 31
	TABLE_NAME_LENGTH_MAX    = 32
	DATABASES_PER_USER_MAX   = 30
	COLUMNS_PER_TABLE_MAX    = 30

	KNODE_START_ENCRYPTION = 320
	KNODE_START_CHUNKKEY   = 96
	KNODE_END_CHUNKKEY     = 128
)

func NewSwarmDB(config *SWARMDBConfig, cloud storage.ChunkStore) (swdb *SwarmDB, err error) {
	sd := new(SwarmDB)
	log.Debug(fmt.Sprintf("NewSwarmDB config = %v", config))
	sd.Config = config
	sd.tables = make(map[string]*Table)
	sd.SwarmStore = cloud
	log.Debug(fmt.Sprintf("NewSwarmDB cloud = %v", cloud))
	chunkdbFileName := "chunk.db"
	dbChunkStoreFullPath := filepath.Join(config.ChunkDBPath, chunkdbFileName)
	dbchunkstore, err := NewDBChunkStore(dbChunkStoreFullPath, cloud)
	if err != nil {
               return swdb, GenerateSWARMDBError(err, `[swarmdb:NewSwarmDB] NewDBChunkStore `+err.Error())
	} else {
		sd.dbchunkstore = dbchunkstore
	}

	//default /tmp/ens.db
	ensdbFileName := "ens.db"
	ensdbFullPath := filepath.Join(config.ChunkDBPath, ensdbFileName)
	ens, err := NewENSSimple(ensdbFullPath, config)
	if err != nil {
		return swdb, GenerateSWARMDBError(err, `[swarmdb:NewSwarmDB] NewENSSimulation `+err.Error())
	} else {
		sd.ens = ens
	}

	sd.Logger = swarmdblog.NewLogger()
	sd.Netstats = NewNetstats(config)

	return sd, nil
}

// DBChunkStore  API
func (self *SwarmDB) PrintDBChunk(columnType ColumnType, hashid []byte, c []byte) {
	self.dbchunkstore.PrintDBChunk(columnType, hashid, c)
}

func (self *SwarmDB) RetrieveDBChunk(u *SWARMDBUser, key []byte) (val []byte, err error) {
	val, err = self.dbchunkstore.RetrieveChunk(u, key)
	return val, err
}

func (self *SwarmDB) StoreDB(key []byte, val []byte, options *storage.CloudOption) (err error){
	return self.dbchunkstore.StoreDB(key, val, options)
}

func (self *SwarmDB) StoreDBChunk(u *SWARMDBUser, val []byte, encrypted int) (key []byte, err error) {
	key, err = self.dbchunkstore.StoreChunk(u, val, encrypted)
	return key, err
}

/*
func (self *SwarmDB) StoreKDBChunk(key []byte, val []byte) (err error) {
	return self.dbchunkstore.StoreKChunk(u, key, val, 0)
}
*/

func (self *SwarmDB) RetrieveDB(key []byte) (val []byte, options *storage.CloudOption, err error){
	return self.dbchunkstore.RetrieveDB(key)
}

// ENSSimulation  API
func (self *SwarmDB) GetRootHash(tblKey []byte /* GetTableKeyValue */) (roothash []byte, err error) {
	log.Debug(fmt.Sprintf("[swarmdb GetRootHash] Getting Root Hash for (%s)[%x] ", tblKey, tblKey))
	hashc := hashcolumn(tblKey)
	s := hashc[:]
	res, status, err := self.ens.GotRootHashFromLDB(s)
	if err != nil{
		res, err = self.ens.GetRootHash(s)
	}
	log.Debug(fmt.Sprintf("swarmdb GetRootHash index = %x hashed index = %x status = %d", tblKey, s, status)) 
	return res, err
}

func (self *SwarmDB) GetRootHashFromLDB(tblKey []byte /* GetTableKeyValue */) (roothash []byte, status uint, err error) {
        log.Debug(fmt.Sprintf("[swarmdb GetRootHashFromLDB] Getting Root Hash for (%s)[%x] ", tblKey, tblKey))
        hashc := hashcolumn(tblKey)
        s := hashc[:]
        res, status, err := self.ens.GotRootHashFromLDB(s)
        log.Debug(fmt.Sprintf("swarmdb GetRootHashFromLDB index = %x hashed index = %x status = %d", tblKey, s, status))
        return res, status, err
}

func (self *SwarmDB) StoreRootHash(fullTableName []byte /* GetTableKey Value */, roothash []byte) (err error) {
	hashc := hashcolumn(fullTableName)
	s := hashc[:]
	log.Debug(fmt.Sprintf("swarmdb StoreRootHash index = %x value = %x hashed index = %x", fullTableName, roothash, s)) 
	return self.ens.StoreRootHashToLDB(s, roothash, 1)
}

func (self *SwarmDB) StoreRootHashWithStatus(fullTableName []byte /* GetTableKey Value */, roothash []byte, status uint) (err error) {
        hashc := hashcolumn(fullTableName)
        s := hashc[:]
        log.Debug(fmt.Sprintf("swarmdb StoreRootHash index = %x value = %x hashed index = %x", fullTableName, roothash, s))
	if status == 3{
        	self.ens.StoreRootHashToLDB(s, roothash, 0)
		return self.ens.StoreRootHash(s, roothash)
	}
	if status == 2{
        	self.ens.StoreRootHashToLDB(s, roothash, status)
		return self.ens.StoreRootHash(s, roothash)
	}
        return self.ens.StoreRootHashToLDB(s, roothash, status)
}

// parse sql and return rows in bulk (order by, group by, etc.)
func (self *SwarmDB) QuerySelect(u *SWARMDBUser, query *QueryOption) (rows []Row, err error) {
	table, err := self.GetTable(u, query.Owner, query.Database, query.Table)
	if err != nil {
		return rows, GenerateSWARMDBError(err, `[swarmdb:QuerySelect] GetTable `+err.Error())
	}

	//var rawRows []Row
	log.Debug(fmt.Sprintf("QueryOwner is: [%s]\n", query.Owner))
	colRows, err := self.Scan(u, query.Owner, query.Database, query.Table, table.primaryColumnName, query.Ascending)
	if err != nil {
		return rows, GenerateSWARMDBError(err, `[swarmdb:QuerySelect] Scan `+err.Error())
	}
	//fmt.Printf("\nColRows = [%+v]", colRows)

	//apply WHERE
	whereRows, err := table.applyWhere(colRows, query.Where)
	if err != nil {
		return rows, GenerateSWARMDBError(err, `[swarmdb:QuerySelect] applyWhere `+err.Error())
	}
	log.Debug(fmt.Sprintf("QuerySelect applied where rows: %+v and number of rows returned = %d", whereRows, len(whereRows)))

	//filter for requested columns
	for _, row := range whereRows {
		// fmt.Printf("QS b4 filterRowByColumns row: %+v\n", row)
		fRow := filterRowByColumns(row, query.RequestColumns)
		// fmt.Printf("QS after filterRowByColumns row: %+v\n", fRow)
		if len(fRow) > 0 {
			rows = append(rows, fRow)
		}
	}
	// fmt.Printf("\nNumber of FINAL rows returned : %d", len(rows))

	//TODO: Put it in order for Ascending/GroupBy
	// fmt.Printf("\nQS returning: %+v\n", rows)
	return rows, nil
}

// Insert is for adding new data to the table
// example: 'INSERT INTO tablename (col1, col2) VALUES (val1, val2)
func (self *SwarmDB) QueryInsert(u *SWARMDBUser, query *QueryOption) (affectedRows int, err error) {

	table, err := self.GetTable(u, query.Owner, query.Database, query.Table)
	if err != nil {
		return 0, GenerateSWARMDBError(err, `[swarmdb:QueryInsert] GetTable `+err.Error())
	}
	affectedRows = 0
	for _, row := range query.Inserts {
		// check if primary column exists in Row
		if _, ok := row[table.primaryColumnName]; !ok {
			return affectedRows, &SWARMDBError{message: fmt.Sprintf("[swarmdb:QueryInsert] Insert row %+v needs primary column '%s' value", row, table.primaryColumnName), ErrorCode: 446, ErrorMessage: fmt.Sprintf("Insert Query Missing Primary Key [%]", table.primaryColumnName)}
		}
		// check if Row already exists
		convertedKey, err := convertJSONValueToKey(table.columns[table.primaryColumnName].columnType, row[table.primaryColumnName])
		if err != nil {
			return affectedRows, GenerateSWARMDBError(err, fmt.Sprintf("[swarmdb:QueryInsert] convertJSONValueToKey - %s", err.Error()))
		}
		_, ok, err := table.Get(u, convertedKey)
		//log.Debug(fmt.Sprintf("Row already exists | [%s] | [%+v] | [%d]", existingByteRow, existingByteRow, len(existingByteRow)))
		if ok {
			return affectedRows, &SWARMDBError{message: fmt.Sprintf("[swarmdb:QueryInsert] Insert row key %s already exists | Error: %s", row[table.primaryColumnName], err), ErrorCode: 434, ErrorMessage: fmt.Sprintf("Record with key [%s] already exists.  If you wish to modify, please use UPDATE SQL statement or PUT", bytes.Trim(convertedKey, "\x00"))}
		}
		if err != nil {
			//TODO: why is this uncommented?
			//return &SWARMDBError{message: fmt.Sprintf("[swarmdb:QueryInsert] Error: %s", err.Error()), ErrorCode: 434, ErrorMessage: fmt.Sprintf("Record with key [%s] already exists.  If you wish to modify, please use UPDATE SQL statement or PUT", bytes.Trim(convertedKey, "\x00")}
		}
		// put the new Row in
		err = table.Put(u, row)
		if err != nil {
			return affectedRows, GenerateSWARMDBError(err, fmt.Sprintf("[swarmdb:QueryInsert] Put %s", err.Error()))
		}
		affectedRows = affectedRows + 1
	}
	return affectedRows, nil
}

// Update is for modifying existing data in the table (can use a Where clause)
// example: 'UPDATE tablename SET col1=value1, col2=value2 WHERE col3 > 0'
func (self *SwarmDB) QueryUpdate(u *SWARMDBUser, query *QueryOption) (affectedRows int, err error) {
	table, err := self.GetTable(u, query.Owner, query.Database, query.Table)
	if err != nil {
		return 0, GenerateSWARMDBError(err, fmt.Sprintf("[swarmdb:QueryUpdate] GetTable %s", err.Error()))
	}

	// get all rows with Scan, using primary key column
	rawRows, err := self.Scan(u, query.Owner, query.Database, query.Table, table.primaryColumnName, query.Ascending)
	if err != nil {
		return 0, GenerateSWARMDBError(err, fmt.Sprintf("[swarmdb:QueryUpdate] Scan %s", err.Error()))
	}

	// check to see if Update cols are in pulled set
	for colname, _ := range query.Update {
		if _, ok := table.columns[colname]; !ok {
			return 0, &SWARMDBError{message: fmt.Sprintf("[swarmdb:QueryUpdate] Update SET column name %s is not in table", colname), ErrorCode: 445, ErrorMessage: fmt.Sprintf("Attempting to update a column [%s] which is not in table [%s]", colname, table.tableName)}
		}
	}

	// apply WHERE clause
	filteredRows, err := table.applyWhere(rawRows, query.Where)
	if err != nil {
		return 0, GenerateSWARMDBError(err, fmt.Sprintf("[swarmdb:QueryUpdate] applyWhere %s", err.Error()))
	}

	// set the appropriate columns in filtered set
	for i, row := range filteredRows {
		for colname, value := range query.Update {
			if _, ok := row[colname]; !ok {
				//return &SWARMDBError{message: fmt.Sprintf("[swarmdb:QueryUpdate] Update SET column name %s is not in filtered rows", colname), ErrorCode: , ErrorMessage:""}
				//TODO: need to actually add this cell if it's an update query and the columnname is actually "valid"
				continue
			}
			filteredRows[i][colname] = value
		}
	}

	// put the changed rows back into the table
	affectedRows = 0
	for _, row := range filteredRows {
		if len(row) > 0 {
			err := table.Put(u, row)
			if err != nil {
				return affectedRows, GenerateSWARMDBError(err, fmt.Sprintf("[swarmdb:QueryUpdate] Put %s", err.Error()))
			}
			affectedRows = affectedRows + 1
		}
	}
	return affectedRows, nil
}

//Delete is for deleting data rows (can use a Where clause, not just a key)
//example: 'DELETE FROM tablename WHERE col1 = value1'
func (self *SwarmDB) QueryDelete(u *SWARMDBUser, query *QueryOption) (affectedRows int, err error) {
	table, err := self.GetTable(u, query.Owner, query.Database, query.Table)
	if err != nil {
		return 0, GenerateSWARMDBError(err, fmt.Sprintf("[swarmdb:QueryDelete] GetTable %s", err.Error()))
	}

	//get all rows with Scan, using Where's specified col
	rawRows, err := self.Scan(u, query.Owner, query.Database, query.Table, query.Where.Left, query.Ascending)
	if err != nil {
		return 0, GenerateSWARMDBError(err, fmt.Sprintf("[swarmdb:QueryDelete] Scan %s", err.Error()))
	}

	//apply WHERE clause
	filteredRows, err := table.applyWhere(rawRows, query.Where)
	if err != nil {
		return 0, GenerateSWARMDBError(err, fmt.Sprintf("[swarmdb:QueryDelete] applyWhere %s", err.Error()))
	}

	//delete the selected rows
	for _, row := range filteredRows {
		if p, okp := row[table.primaryColumnName]; okp {
			ok, err := table.Delete(u, p)
			if err != nil {
				return affectedRows, GenerateSWARMDBError(err, fmt.Sprintf("[swarmdb:QueryDelete] Delete %s", err.Error()))
			}
			if !ok {
				// TODO: if !ok, what should happen? return appropriate response -- number of records affected
			} else {
				affectedRows = affectedRows + 1
			}
		}
	}
	return affectedRows, nil
}

func (self *SwarmDB) Query(u *SWARMDBUser, query *QueryOption) (rows []Row, affectedRows int, err error) {
	switch query.Type {
	case "Select":
		rows, err = self.QuerySelect(u, query)
		if err != nil {
			return rows, len(rows), GenerateSWARMDBError(err, fmt.Sprintf("[swarmdb:Query] QuerySelect %s", err.Error()))
		}
		return rows, len(rows), nil
	case "Insert":
		affectedRows, err = self.QueryInsert(u, query)
		if err != nil {
			return rows, affectedRows, GenerateSWARMDBError(err, fmt.Sprintf("[swarmdb:Query] QueryInsert %s", err.Error()))
		}
		return rows, affectedRows, nil
	case "Update":
		affectedRows, err = self.QueryUpdate(u, query)
		if err != nil {
			return rows, affectedRows, GenerateSWARMDBError(err, fmt.Sprintf("[swarmdb:Query] QueryUpdate %s", err.Error()))
		}
		return rows, affectedRows, nil
	case "Delete":
		affectedRows, err = self.QueryDelete(u, query)
		if err != nil {
			return rows, affectedRows, GenerateSWARMDBError(err, fmt.Sprintf("[swarmdb:Query] QueryDelete %s", err.Error()))
		}
		return rows, affectedRows, nil
	}
	return rows, 0, nil
}

func (self *SwarmDB) Scan(u *SWARMDBUser, owner string, database string, tableName string, columnName string, ascending int) (rows []Row, err error) {
	tblKey := self.GetTableKey(owner, database, tableName)
	tbl, ok := self.tables[tblKey]
	if !ok {
		//TODO: how would this ever happen?
		return rows, &SWARMDBError{message: fmt.Sprintf("[swarmdb:Scan] No such table to scan [%s:%s] - [%s]", owner, database, tblKey), ErrorCode: 403, ErrorMessage: fmt.Sprintf("Table Does Not Exist:  Table: [%s] Database [%s] Owner: [%s]", tableName, database, owner)}
	}
	rows, err = tbl.Scan(u, columnName, ascending)
	if err != nil {
		return rows, GenerateSWARMDBError(err, fmt.Sprintf("[swarmdb:Scan] Error doing table scan: [%s] %s", columnName, err.Error()))
	}
	rows, err = tbl.assignRowColumnTypes(rows)
	if err != nil {
		return rows, GenerateSWARMDBError(err, fmt.Sprintf("[swarmdb:Scan] Error assigning column types to row values"))
	}
	// fmt.Printf("swarmdb Scan finished ok: %+v\n", rows)
	return rows, nil
}

func (self *SwarmDB) GetTable(u *SWARMDBUser, owner string, database string, tableName string) (tbl *Table, err error) {
	if len(owner) == 0 {
		return tbl, &SWARMDBError{message: fmt.Sprintf("[swarmdb:GetTable] owner missing "), ErrorCode: 430, ErrorMessage: "Owner Missing"}
	}
	if len(database) == 0 {
		return tbl, &SWARMDBError{message: fmt.Sprintf("[swarmdb:GetTable] database missing "), ErrorCode: 500, ErrorMessage: "Database Missing"}
	}
	if len(tableName) == 0 {
		return tbl, &SWARMDBError{message: fmt.Sprintf("[swarmdb:GetTable] tablename missing "), ErrorCode: 426, ErrorMessage: "Table Name Missing"}
	}
	tblKey := self.GetTableKey(owner, database, tableName)
	// fmt.Printf("\nGetting Table [%s] with the Owner [%s] from TABLES [%v]", tableName, owner, self.tables)
	if tbl, ok := self.tables[tblKey]; ok {
		log.Debug(fmt.Sprintf("Table[%v] with Owner [%s] Database %s found in tables, it is: %+v\n", tblKey, owner, database, tbl))
		// fmt.Printf("\nprimary column name GetTable: %+v -> columns: %+v\n", tbl.columns, tbl.primaryColumnName)
		return tbl, nil
	} else {
		tbl = self.NewTable(owner, database, tableName)
		err = tbl.OpenTable(u)
		if err != nil {
			return tbl, GenerateSWARMDBError(err, fmt.Sprintf("[swarmdb:GetTable] OpenTable %s", err.Error()))
		}
		self.RegisterTable(owner, database, tableName, tbl)
		return tbl, nil
	}
}

// TODO: when there are errors, the error must be parsable make user friendly developer errors that can be trapped by Node.js, Go library, JS CLI
func (self *SwarmDB) SelectHandler(u *SWARMDBUser, data string) (resp SWARMDBResponse, err error) {

	log.Debug(fmt.Sprintf("SelectHandler Input: %s\n", data))
	d, err := parseData(data)
	if err != nil {
		return resp, GenerateSWARMDBError(err, fmt.Sprintf("[swarmdb:SelectHandler] parseData %s", err.Error()))
	}

	switch d.RequestType {
	case RT_CREATE_DATABASE:
		err = self.CreateDatabase(u, d.Owner, d.Database, d.Encrypted)
		if err != nil {
			return resp, GenerateSWARMDBError(err, fmt.Sprintf("[swarmdb:SelectHandler] CreateDatabase %s", err.Error()))
		}

		return SWARMDBResponse{AffectedRowCount: 1}, nil

	case RT_DROP_DATABASE:
		ok, err := self.DropDatabase(u, d.Owner, d.Database)
		if err != nil {
			return resp, GenerateSWARMDBError(err, fmt.Sprintf("[swarmdb:SelectHandler] DropDatabase %s", err.Error()))
		}
		if ok {
			return SWARMDBResponse{AffectedRowCount: 1}, nil
		} else {
			return SWARMDBResponse{AffectedRowCount: 0}, nil
		}

	case RT_LIST_DATABASES:
		databases, err := self.ListDatabases(u, d.Owner)
		if err != nil {
			return resp, GenerateSWARMDBError(err, fmt.Sprintf("[swarmdb:SelectHandler] ListDatabases %s", err.Error()))
		}
		resp.Data = databases
		resp.MatchedRowCount = len(databases)
		return resp, nil

	case RT_CREATE_TABLE:
		if len(d.Table) == 0 || len(d.Columns) == 0 {
			return resp, &SWARMDBError{message: fmt.Sprintf("[swarmdb:SelectHandler] empty table and column"), ErrorCode: 417, ErrorMessage: "Invalid [CreateTable] Request: Missing Table and/or Columns"}
		}
		//TODO: Upon further review, could make a NewTable and then call this from tbl. ---
		_, err := self.CreateTable(u, d.Owner, d.Database, d.Table, d.Columns)
		if err != nil {
			return resp, GenerateSWARMDBError(err, fmt.Sprintf("[swarmdb:SelectHandler] CreateTable %s", err.Error()))
		}
		return SWARMDBResponse{AffectedRowCount: 1}, nil

	case RT_DROP_TABLE:
		ok, err := self.DropTable(u, d.Owner, d.Database, d.Table)
		if err != nil {
			return resp, GenerateSWARMDBError(err, fmt.Sprintf("[swarmdb:SelectHandler] DropTable %s", err.Error()))
		}
		if ok {
			return SWARMDBResponse{AffectedRowCount: 1}, nil
		} else {
			return SWARMDBResponse{AffectedRowCount: 0}, nil
		}

	case RT_SCAN:
		tbl, err := self.GetTable(u, d.Owner, d.Database, d.Table)
		if err != nil {
			return resp, GenerateSWARMDBError(err, fmt.Sprintf("[swarmdb:SelectHandler] GetTable %s", err.Error()))
		}
		rawRows, err := self.Scan(u, d.Owner, d.Database, d.Table, tbl.primaryColumnName, 1)
		if err != nil {
			return resp, GenerateSWARMDBError(err, fmt.Sprintf("[swarmdb:SelectHandler] GetTable %s", err.Error()))
		}
		resp.Data = rawRows
		resp.AffectedRowCount = len(resp.Data)
		return resp, nil

	case RT_DESCRIBE_TABLE:
		tbl, err := self.GetTable(u, d.Owner, d.Database, d.Table)
		if err != nil {
			return resp, GenerateSWARMDBError(err, fmt.Sprintf("[swarmdb:SelectHandler] GetTable %s", err.Error()))
		}
		tblcols, err := tbl.DescribeTable()
		if err != nil {
			return resp, GenerateSWARMDBError(err, fmt.Sprintf("[swarmdb:SelectHandler] DescribeTable %s", err.Error()))
		}
		for _, colInfo := range tblcols {
			r := NewRow()
			r["ColumnName"] = colInfo.ColumnName
			r["IndexType"] = colInfo.IndexType
			r["Primary"] = colInfo.Primary
			r["ColumnType"] = colInfo.ColumnType
			resp.Data = append(resp.Data, r)
		}
		return resp, nil
	case RT_CLOSE_TABLE:
		tbl, err := self.GetTable(u, d.Owner, d.Database, d.Table)
		if err != nil{
			return resp, GenerateSWARMDBError(err, fmt.Sprintf("[swarmdb:SelectHandler] CloseTable %s", err.Error()))
		}
		err = tbl.Close(u)
		if err != nil{
			return resp, GenerateSWARMDBError(err, fmt.Sprintf("[swarmdb:SelectHandler] CloseTable %s", err.Error()))
		}
		return resp, nil

	case RT_LIST_TABLES:
		tableNames, err := self.ListTables(u, d.Owner, d.Database)
		if err != nil {
			return resp, GenerateSWARMDBError(err, fmt.Sprintf("[swarmdb:SelectHandler] ListDatabases %s", err.Error()))
		}
		resp.Data = tableNames
		resp.MatchedRowCount = len(tableNames)
		log.Debug(fmt.Sprintf("returning resp %+v tablenames %+v Mrc %+v", resp, tableNames, len(tableNames)))
		return resp, nil
	case RT_PUT:
		tbl, err := self.GetTable(u, d.Owner, d.Database, d.Table)
		if err != nil {
			return resp, GenerateSWARMDBError(err, fmt.Sprintf("[swarmdb:SelectHandler] GetTable %s", err.Error()))
		}
		tblInfo, err := tbl.DescribeTable()
		if err != nil {
			return resp, GenerateSWARMDBError(err, fmt.Sprintf("[swarmdb:SelectHandler] DescribeTable %s", err.Error()))
		}
		d.Rows, err = tbl.assignRowColumnTypes(d.Rows)
		if err != nil {
			return resp, GenerateSWARMDBError(err, fmt.Sprintf("[swarmdb:SelectHandler] assignRowColumnTypes %s", err.Error()))
		}

		//error checking for primary column, and valid columns
		for _, row := range d.Rows {
			log.Debug(fmt.Sprintf("checking row %v\n", row))
			if _, ok := row[tbl.primaryColumnName]; !ok {
				return resp, &SWARMDBError{message: fmt.Sprintf("[swarmdb:SelectHandler] Put row %+v needs primary column '%s' value", row, tbl.primaryColumnName), ErrorCode: 428, ErrorMessage: "Row missing primary key"}
			}
			for columnName, _ := range row {
				if _, ok := tblInfo[columnName]; !ok {
					return resp, &SWARMDBError{message: fmt.Sprintf("[swarmdb:SelectHandler] Put row %+v has unknown column %s", row, columnName), ErrorCode: 429, ErrorMessage: fmt.Sprintf("Row contains unknown column [%s]", columnName)}
				}
			}
			// check to see if row already exists in table (no overwriting, TODO: check if that is right??)
			/* TODO: we want to have PUT blindly update.  INSERT will fail on duplicate and need to confirm what to do if multiple rows attempted to be inserted and just some are dupes
			primaryColumnType := tbl.columns[tbl.primaryColumnName].columnType
			convertedKey, err := convertJSONValueToKey(primaryColumnType, row[tbl.primaryColumnName])
			if err != nil {
				return resp, GenerateSWARMDBError( err, fmt.Sprintf("[swarmdb:SelectHandler] convertJSONValueToKey %s", err.Error()) )
			}
			validBytes, err := tbl.Get(u, convertedKey)
			if err == nil {
				validRow, err2 := tbl.byteArrayToRow(validBytes)
				if err2 != nil {
					return resp, GenerateSWARMDBError( err2, fmt.Sprintf("[swarmdb:SelectHandler] byteArrayToRow %s", err2.Error()) )
				}
				return resp GenerateSWARMDBError( err, fmt.Sprintf("[swarmdb:SelectHandler] Row with that primary key already exists: %+v", validRow) )
			} else {
				fmt.Printf("good, row wasn't found\n")
			}
			*/
		}

		//put the rows in
		successfulRows := 0
		for _, row := range d.Rows {
			err = tbl.Put(u, row)
			if err != nil {
				return resp, GenerateSWARMDBError(err, fmt.Sprintf("[swarmdb:SelectHandler] Put %s", err.Error()))
			}
			successfulRows++
		}
		return SWARMDBResponse{AffectedRowCount: successfulRows}, nil

	case RT_GET:
		tbl, err := self.GetTable(u, d.Owner, d.Database, d.Table)
		if err != nil {
			return resp, GenerateSWARMDBError(err, fmt.Sprintf("[swarmdb:SelectHandler] GetTable %s", err.Error()))
		}
		if isNil(d.Key) {
			return resp, &SWARMDBError{message: fmt.Sprintf("[swarmdb:SelectHandler] Get - Missing Key"), ErrorCode: 433, ErrorMessage: "GET Request Missing Key"}
		}
		primaryColumnType := tbl.columns[tbl.primaryColumnName].columnType
		convertedKey, err := convertJSONValueToKey(primaryColumnType, d.Key)
		if err != nil {
			return resp, GenerateSWARMDBError(err, fmt.Sprintf("[swarmdb:SelectHandler] convertJSONValueToKey %s", err.Error()))
		}
		byteRow, ok, err := tbl.Get(u, convertedKey)
		if err != nil {
			return resp, GenerateSWARMDBError(err, fmt.Sprintf("[swarmdb:SelectHandler] Get %s", err.Error()))
		}

		if ok {
			validRow, err2 := tbl.byteArrayToRow(byteRow)
			if err2 != nil {
				return resp, GenerateSWARMDBError(err2, fmt.Sprintf("[swarmdb:SelectHandler] byteArrayToRow %s", err2.Error()))
			}
			resp.Data = append(resp.Data, validRow)
			resp.MatchedRowCount = 1
		}
		return resp, nil

	case RT_DELETE:
		tbl, err := self.GetTable(u, d.Owner, d.Database, d.Table)
		if err != nil {
			return resp, GenerateSWARMDBError(err, fmt.Sprintf("[swarmdb:SelectHandler] GetTable %s", err.Error()))
		}
		if isNil(d.Key) {
			return resp, &SWARMDBError{message: fmt.Sprintf("[swarmdb:SelectHandler] Delete is Missing Key"), ErrorCode: 448, ErrorMessage: "Delete Statement missing KEY"}
		}
		ok, err := tbl.Delete(u, d.Key)
		if err != nil {
			return resp, GenerateSWARMDBError(err, fmt.Sprintf("[swarmdb:SelectHandler] Delete %s", err.Error()))
		}
		if ok {
			return SWARMDBResponse{AffectedRowCount: 1}, nil
		}
		return SWARMDBResponse{AffectedRowCount: 0}, nil

	case RT_START_BUFFER:
		tbl, err := self.GetTable(u, d.Owner, d.Database, d.Table)
		if err != nil {
			return resp, GenerateSWARMDBError(err, fmt.Sprintf("[swarmdb:SelectHandler] GetTable %s", err.Error()))
		}
		err = tbl.StartBuffer(u)
		if err != nil {
			return resp, GenerateSWARMDBError(err, fmt.Sprintf("[swarmdb:SelectHandler] StartBuffer %s", err.Error()))
		}
		//TODO: update to use real "count"
		return SWARMDBResponse{AffectedRowCount: 1}, nil

	case RT_FLUSH_BUFFER:
		tbl, err := self.GetTable(u, d.Owner, d.Database, d.Table)
		if err != nil {
			return resp, GenerateSWARMDBError(err, fmt.Sprintf("[swarmdb:SelectHandler] GetTable %s", err.Error()))
		}
		err = tbl.FlushBuffer(u)
		if err != nil {
			return resp, GenerateSWARMDBError(err, fmt.Sprintf("[swarmdb:SelectHandler] FlushBuffer %s", err.Error()))
		}
		err = tbl.UpdateRootHash()
		if err != nil {
			return resp, GenerateSWARMDBError(err, fmt.Sprintf("[swarmdb:SelectHandler] FlushBuffer %s", err.Error()))
		}
		//TODO: update to use real "count"
		return SWARMDBResponse{AffectedRowCount: 1}, nil

	case RT_QUERY:
		if len(d.RawQuery) == 0 {
			return resp, &SWARMDBError{message: fmt.Sprintf("[swarmdb:SelectHandler] RawQuery is blank"), ErrorCode: 425, ErrorMessage: "Invalid Query Request. Missing Rawquery"}
		}
		query, err := ParseQuery(d.RawQuery)
		query.Encrypted = d.Encrypted
		if err != nil {
			return resp, GenerateSWARMDBError(err, fmt.Sprintf("[swarmdb:SelectHandler] ParseQuery [%s] %s", d.RawQuery, err.Error()))
		}
		query.Owner = d.Owner
		query.Database = d.Database
		if len(d.Table) == 0 {
			//TODO: check if empty even after query.Table check
			d.Table = query.Table //since table is specified in the query we do not have get it as a separate input
		}
		tbl, err := self.GetTable(u, d.Owner, d.Database, d.Table)
		if err != nil {
			return resp, GenerateSWARMDBError(err, fmt.Sprintf("[swarmdb:SelectHandler] GetTable %s", err.Error()))
		}
		tblInfo, err := tbl.DescribeTable()
		if err != nil {
			return resp, GenerateSWARMDBError(err, fmt.Sprintf("[swarmdb:SelectHandler] DescribeTable %s", err.Error()))
		}

		//checking validity of columns
		for _, reqCol := range query.RequestColumns {
			if _, ok := tblInfo[reqCol.ColumnName]; !ok {
				return resp, &SWARMDBError{message: fmt.Sprintf("[swarmdb:SelectHandler] Requested col [%s] does not exist in table [%+v]", reqCol.ColumnName, tblInfo), ErrorCode: 404, ErrorMessage: fmt.Sprintf("Column Does Not Exist in table definition: [%s]", reqCol.ColumnName)}
			}
		}

		//checking the Where clause
		if query.Type == "Select" && len(query.Where.Left) > 0 {
			if _, ok := tblInfo[query.Where.Left]; !ok {
				return resp, &SWARMDBError{message: fmt.Sprintf("[swarmdb:SelectHandler] Query col [%s] does not exist in table", query.Where.Left), ErrorCode: 432, ErrorMessage: fmt.Sprintf("WHERE Clause contains invalid column [%s]", query.Where.Left)}
			}

			//checking if the query is just a primary key Get
			if query.Where.Left == tbl.primaryColumnName && query.Where.Operator == "=" {
				// fmt.Printf("Calling Get from Query\n")
				convertedKey, err := convertJSONValueToKey(tbl.columns[tbl.primaryColumnName].columnType, query.Where.Right)
				if err != nil {
					return resp, GenerateSWARMDBError(err, fmt.Sprintf("[swarmdb:SelectHandler] convertJSONValueToKey %s", err.Error()))
				}

				byteRow, ok, err := tbl.Get(u, convertedKey)
				if err != nil {
					return resp, GenerateSWARMDBError(err, fmt.Sprintf("[swarmdb:SelectHandler] Get %s", err.Error()))
				}
				if ok {
					row, err := tbl.byteArrayToRow(byteRow)
					// fmt.Printf("Response row from Get: %s (%v)\n", row, row)
					if err != nil {
						return resp, GenerateSWARMDBError(err, fmt.Sprintf("[swarmdb:SelectHandler] byteArrayToRow %s", err.Error()))
					}

					filteredRow := filterRowByColumns(row, query.RequestColumns)
					// fmt.Printf("\nResponse filteredrow from Get: %s (%v)", filteredRow, filteredRow)
					resp.Data = append(resp.Data, filteredRow)
				}
				return resp, nil
			}
		}

		// process the query
		qRows, affectedRows, err := self.Query(u, &query)
		if err != nil {
			return resp, GenerateSWARMDBError(err, fmt.Sprintf("[swarmdb:SelectHandler] Query [%+v] %s", query, err.Error()))
		}
		return SWARMDBResponse{AffectedRowCount: affectedRows, Data: qRows}, nil

	} //end switch

	return resp, &SWARMDBError{message: fmt.Sprintf("[swarmdb:SelectHandler] RequestType invalid: [%s]", d.RequestType), ErrorCode: 418, ErrorMessage: "Request Invalid"}

}

func parseData(data string) (*RequestOption, error) {
	udata := new(RequestOption)
	if err := json.Unmarshal([]byte(data), udata); err != nil {
		return nil, &SWARMDBError{message: fmt.Sprintf("[swarmdb:parseData] Unmarshal %s", err.Error()), ErrorCode: 432, ErrorMessage: "Unable to Parse Request"}
	}
	return udata, nil
}

func (self *SwarmDB) NewTable(owner string, database string, tableName string) *Table {
	t := new(Table)
	t.swarmdb = self
	t.Owner = owner
	t.Database = database
	t.tableName = tableName
	t.columns = make(map[string]*ColumnInfo)

	return t
}

func (self *SwarmDB) RegisterTable(owner string, database string, tableName string, t *Table) {
	// register the Table in SwarmDB
	tblKey := self.GetTableKey(owner, database, tableName)
	self.tables[tblKey] = t
}

// creating a database results in a new entry, e.g. "videos" in the owners ENS e.g. "wolktoken.eth" stored in a single chunk
// e.g.  key 1: wolktoken.eth (up to 64 chars)
//       key 2: videos     => 32 byte hash, pointing to tables of "video'
func (self *SwarmDB) CreateDatabase(u *SWARMDBUser, owner string, database string, encrypted int) (err error) {
	// this is the 32 byte version of the database name
	if len(database) > DATABASE_NAME_LENGTH_MAX {
		return &SWARMDBError{message: "[swarmdb:CreateDatabase] Database exists already", ErrorCode: 500, ErrorMessage: "Database Name too long (max is 32 chars)"}
	}

	ownerHash := crypto.Keccak256([]byte(owner))
	newDBName := make([]byte, DATABASE_NAME_LENGTH_MAX) //TODO: confirm use of constant ok -- making consistent with other DB names
	copy(newDBName[0:], database)

	// look up what databases the owner has already
	ownerDatabaseChunkID, err := self.GetRootHash(ownerHash)
	if err != nil {
		return GenerateSWARMDBError(err, fmt.Sprintf("[swarmdb:CreateDatabase] GetRootHash %s", err))
	}

	buf := make([]byte, CHUNK_SIZE)
	log.Debug(fmt.Sprintf("[swarmdb:CreateDatabase] Getting Root Hash using ownerHash [%x] and got [%x]", ownerHash, ownerDatabaseChunkID))

	if EmptyBytes(ownerDatabaseChunkID) {
		// put the 32-byte ownerHash in the first 32 bytes
		log.Debug(fmt.Sprintf("Creating new %s - %x\n", owner, ownerHash))
		copy(buf[0:32], []byte(ownerHash))
	} else {
		buf, err = self.RetrieveDBChunk(u, ownerDatabaseChunkID)
		if err != nil {
			return GenerateSWARMDBError(err, fmt.Sprintf("[swarmdb:CreateDatabase] RetrieveDBChunk %s", err))
		}

		// the first 32 bytes of the buf should match
		if bytes.Compare(buf[0:32], ownerHash[0:32]) != 0 {
			return &SWARMDBError{message: fmt.Sprintf("[swarmdb:CreateDatabase] Invalid owner %x != %x", ownerHash, buf[0:32]), ErrorCode: 450, ErrorMessage: fmt.Sprintf("Owner [%s] is invalid", owner)}
			//TODO: understand how/when this would occur
		}

		// check if there is already a database entry
		for i := 64; i < CHUNK_SIZE; i += 64 {
			if bytes.Equal(buf[i:(i+DATABASE_NAME_LENGTH_MAX)], newDBName) {
				return &SWARMDBError{message: "[swarmdb:CreateDatabase] Database exists already", ErrorCode: 500, ErrorMessage: "Database Exists Already"}
			}
		}
	}

	for i := 64; i < CHUNK_SIZE; i += 64 {
		// find the first 000 byte entry
		if EmptyBytes(buf[i:(i + 64)]) {
			// make a new database chunk, with the first 32 bytes of the chunk being the database name (the next keys will be the tables)
			bufDB := make([]byte, CHUNK_SIZE)
			copy(bufDB[0:DATABASE_NAME_LENGTH_MAX], newDBName[0:DATABASE_NAME_LENGTH_MAX])

			newDBHash, err := self.StoreDBChunk(u, bufDB, encrypted)
			if err != nil {
				return GenerateSWARMDBError(err, fmt.Sprintf("[swarmdb:CreateDatabase] StoreDBChunk %s", err.Error()))
			}

			// save the owner chunk, with the name + new DB hash
			copy(buf[i:(i+DATABASE_NAME_LENGTH_MAX)], newDBName[0:DATABASE_NAME_LENGTH_MAX])
			log.Debug(fmt.Sprintf("Saving Database with encrypted bit of %d at possition: %d", encrypted, i+DATABASE_NAME_LENGTH_MAX))
			if encrypted > 0 {
				buf[i+DATABASE_NAME_LENGTH_MAX] = 1
			} else {
				buf[i+DATABASE_NAME_LENGTH_MAX] = 0
			}
			copy(buf[(i+32):(i+64)], newDBHash[0:32])
			log.Debug(fmt.Sprintf("Buffer has encrypted bit of %d ", buf[i+DATABASE_NAME_LENGTH_MAX]))

			ownerDatabaseChunkID, err = self.StoreDBChunk(u, buf, 0) // this could be a function of the top level domain .pri/.eth
			if err != nil {
				return GenerateSWARMDBError(err, fmt.Sprintf("[swarmdb:CreateDatabase] StoreDBChunk %s", err.Error()))
			}

			err = self.StoreRootHashWithStatus(ownerHash, ownerDatabaseChunkID, 2)
			if err != nil {
				return GenerateSWARMDBError(err, fmt.Sprintf("[swarmdb:CreateDatabase] StoreRootHash %s", err.Error()))
			}
			return nil
		}
	}
	return &SWARMDBError{message: fmt.Sprintf("[swarmdb:CreateDatabase] Database could not be created -- exceeded allocation"), ErrorCode: 451, ErrorMessage: fmt.Sprintf("Database could not be created -- exceeded allocation of %d", DATABASE_NAME_LENGTH_MAX)}
}

func (self *SwarmDB) ListDatabases(u *SWARMDBUser, owner string) (ret []Row, err error) {
	ownerHash := crypto.Keccak256([]byte(owner))
	// look up what databases the owner has
	ownerDatabaseChunkID, err := self.GetRootHash(ownerHash)
	if err != nil {
		return ret, GenerateSWARMDBError(err, fmt.Sprintf("[swarmdb:ListDatabases] GetRootHash %s", err))
	}

	buf := make([]byte, CHUNK_SIZE)
	if EmptyBytes(ownerDatabaseChunkID) {

	} else {
		buf, err = self.RetrieveDBChunk(u, ownerDatabaseChunkID)
		if err != nil {
			return ret, GenerateSWARMDBError(err, fmt.Sprintf("[swarmdb:ListDatabases] RetrieveDBChunk %s", err))
		}

		// the first 32 bytes of the buf should match
		if bytes.Compare(buf[0:32], ownerHash[0:32]) != 0 {
			return ret, &SWARMDBError{message: fmt.Sprintf("[swarmdb:ListDatabases] Invalid owner %x != %x", ownerHash, buf[0:32]), ErrorCode: 450, ErrorMessage: "Invalid Owner Specified"}
		}

		// check if there is already a database entry
		for i := 64; i < CHUNK_SIZE; i += 64 {
			if EmptyBytes(buf[i:(i + DATABASE_NAME_LENGTH_MAX)]) {
			} else {
				r := NewRow()
				db := string(bytes.Trim(buf[i:(i+DATABASE_NAME_LENGTH_MAX)], "\x00"))
				log.Debug(fmt.Sprintf("DB: %s | %v BUF %s | %v ", db, db, buf[i:(i+32)], buf[i:(i+32)]))
				//rowstring := fmt.Sprintf("{\"database\":\"%s\"}", db)
				r["database"] = db
				ret = append(ret, r)
			}
		}
	}

	return ret, nil
}

// dropping a database removes the ENS entry
func (self *SwarmDB) DropDatabase(u *SWARMDBUser, owner string, database string) (ok bool, err error) {
	if len(database) > DATABASE_NAME_LENGTH_MAX {
		return false, &SWARMDBError{message: "[swarmdb:CreateDatabase] Database exists already", ErrorCode: 500, ErrorMessage: "Database Name too long (max is 32 chars)"}
	}

	// this is the 32 byte version of the database name
	ownerHash := crypto.Keccak256([]byte(owner))
	dropDBName := make([]byte, DATABASE_NAME_LENGTH_MAX)
	copy(dropDBName[0:], database)

	// look up what databases the owner has already
	ownerDatabaseChunkID, err := self.GetRootHash(ownerHash)
	if err != nil {
		return false, &SWARMDBError{message: fmt.Sprintf("[swarmdb:DropDatabase] GetRootHash %s", err)}
	}

	buf := make([]byte, CHUNK_SIZE)
	if EmptyBytes(ownerDatabaseChunkID) {
		return false, &SWARMDBError{message: fmt.Sprintf("[swarmdb:DropDatabase] No database %s", err)}
	} else {
		buf, err = self.RetrieveDBChunk(u, ownerDatabaseChunkID)
		if err != nil {
			return false, &SWARMDBError{message: fmt.Sprintf("[swarmdb:DropDatabase] RetrieveDBChunk %s", err)}
		}

		// the first 32 bytes of the buf should match
		if bytes.Compare(buf[0:32], ownerHash[0:32]) != 0 {
			return false, &SWARMDBError{message: fmt.Sprintf("[swarmdb:DropDatabase] Invalid owner %x != %x", ownerHash, buf[0:32])}
		}

		// check for the database entry
		for i := 64; i < CHUNK_SIZE; i += 64 {
			if bytes.Compare(buf[i:(i+DATABASE_NAME_LENGTH_MAX)], dropDBName) == 0 {
				// found it, zero out the database
				copy(buf[i:(i+64)], make([]byte, 64))
				ownerDatabaseChunkID, err = self.StoreDBChunk(u, buf, 0) // TODO: .eth disc
				if err != nil {
					return false, GenerateSWARMDBError(err, fmt.Sprintf("[swarmdb:DropDatabase] StoreDBChunk %s", err.Error()))
				}
				err = self.StoreRootHashWithStatus(ownerHash, ownerDatabaseChunkID, 2)
				if err != nil {
					return false, GenerateSWARMDBError(err, fmt.Sprintf("[swarmdb:DropDatabase] StoreRootHash %s", err.Error()))
				}
				return true, nil
			}
		}
	}
	return false, nil // &SWARMDBError{message: fmt.Sprintf("[swarmdb:DropDatabase] Database could not be found")}
}

func (self *SwarmDB) DropTable(u *SWARMDBUser, owner string, database string, tableName string) (ok bool, err error) {
	if len(tableName) > TABLE_NAME_LENGTH_MAX {
		return false, &SWARMDBError{message: "[swarmdb:DropTable] Tablename length", ErrorCode: 500, ErrorMessage: "Table Name too long (max is 32 chars)"}
	}

	// this is the 32 byte version of the database name
	ownerHash := crypto.Keccak256([]byte(owner))
	dbName := make([]byte, DATABASE_NAME_LENGTH_MAX)
	copy(dbName[0:], database)

	dropTableName := make([]byte, TABLE_NAME_LENGTH_MAX)
	copy(dropTableName[0:], tableName)

	// look up what databases the owner has already
	ownerDatabaseChunkID, err := self.GetRootHash(ownerHash)
	if err != nil {
		return false, &SWARMDBError{message: fmt.Sprintf("[swarmdb:DropTable] GetRootHash %s", err)}
	}

	buf := make([]byte, CHUNK_SIZE)
	if EmptyBytes(ownerDatabaseChunkID) {
		return false, &SWARMDBError{message: fmt.Sprintf("[swarmdb:DropTable] No owner found %s", err)}
	} else {
		buf, err = self.RetrieveDBChunk(u, ownerDatabaseChunkID)
		if err != nil {
			return false, &SWARMDBError{message: fmt.Sprintf("[swarmdb:DropTable] RetrieveDBChunk %s", err)}
		}

		// the first 32 bytes of the buf should match
		if bytes.Compare(buf[0:32], ownerHash[0:32]) != 0 {
			return false, &SWARMDBError{message: fmt.Sprintf("[swarmdb:DropTable] Invalid owner %x != %x", ownerHash, buf[0:32])}
		}

		// check for the database entry
		for i := 64; i < CHUNK_SIZE; i += 64 {
			if bytes.Compare(buf[i:(i+DATABASE_NAME_LENGTH_MAX)], dbName) == 0 {
				// found it - read the encryption level
				encrypted := 0
				if buf[i+DATABASE_NAME_LENGTH_MAX] > 0 {
					encrypted = 1
				}

				databaseHash := make([]byte, 32)
				copy(databaseHash[:], buf[(i+32):(i+64)])

				// bufDB has the tables!
				bufDB := make([]byte, CHUNK_SIZE)
				bufDB, err = self.RetrieveDBChunk(u, databaseHash)
				if err != nil {
					return false, &SWARMDBError{message: fmt.Sprintf("[swarmdb:DropTable] RetrieveDBChunk %s", err)}
				}

				// nuke the table name in bufDB and write the updated bufDB
				for j := 64; j < CHUNK_SIZE; j += 64 {
					if bytes.Compare(bufDB[j:(j+TABLE_NAME_LENGTH_MAX)], dropTableName) == 0 {
						blankN := make([]byte, TABLE_NAME_LENGTH_MAX)
						copy(bufDB[j:(j+TABLE_NAME_LENGTH_MAX)], blankN[0:TABLE_NAME_LENGTH_MAX])
						databaseHash, err := self.StoreDBChunk(u, bufDB, encrypted)
						if err != nil {
							return false, &SWARMDBError{message: fmt.Sprintf("[swarmdb:DropTable] StoreDBChunk %s", err)}
						}
						// update the database hash in the owner's databases
						copy(buf[(i+32):(i+64)], databaseHash[0:32])
						ownerDatabaseChunkID, err = self.StoreDBChunk(u, buf, 0) // TODO: review
						if err != nil {
							return false, &SWARMDBError{message: fmt.Sprintf("[swarmdb:DropTable] StoreDBChunk %s", err)}
						}

						err = self.StoreRootHashWithStatus(ownerHash, ownerDatabaseChunkID, 2)
						if err != nil {
							return false, GenerateSWARMDBError(err, fmt.Sprintf("[swarmdb:DropTable] StoreRootHash %s", err.Error()))
						}
						return true, nil
					}
				}
				return false, nil

			}
		}
	}
	return false, &SWARMDBError{message: fmt.Sprintf("[swarmdb:DropDatabase] Database could not be found")}

}

func (self *SwarmDB) ListTables(u *SWARMDBUser, owner string, database string) (tableNames []Row, err error) {
	// this is the 32 byte version of the database name
	ownerHash := crypto.Keccak256([]byte(owner))
	dbName := make([]byte, DATABASE_NAME_LENGTH_MAX)
	copy(dbName[0:], database)

	// look up what databases the owner has already
	ownerDatabaseChunkID, err := self.GetRootHash(ownerHash)
	if err != nil {
		return tableNames, &SWARMDBError{message: fmt.Sprintf("[swarmdb:ListTables] GetRootHash %s", err)}
	}

	buf := make([]byte, CHUNK_SIZE)
	if EmptyBytes(ownerDatabaseChunkID) {
		return tableNames, &SWARMDBError{message: fmt.Sprintf("[swarmdb:ListTables] Requested owner [%s] not found", owner), ErrorCode: 477, ErrorMessage: fmt.Sprintf("Requested owner [%s] not found", owner)}
	} else {
		buf, err = self.RetrieveDBChunk(u, ownerDatabaseChunkID)
		if err != nil {
			return tableNames, &SWARMDBError{message: fmt.Sprintf("[swarmdb:ListTables] RetrieveDBChunk %s", err)}
		}

		// the first 32 bytes of the buf should match
		if bytes.Compare(buf[0:32], ownerHash[0:32]) != 0 {
			return tableNames, &SWARMDBError{message: fmt.Sprintf("[swarmdb:ListTables] Invalid owner %x != %x", ownerHash, buf[0:32])}
		}

		// check for the database entry
		for i := 64; i < CHUNK_SIZE; i += 64 {
			if bytes.Compare(buf[i:(i+DATABASE_NAME_LENGTH_MAX)], dbName) == 0 {
				// found it - read the encryption level
				databaseHash := make([]byte, 32)
				copy(databaseHash[:], buf[(i+32):(i+64)])

				// bufDB has the tables!
				bufDB := make([]byte, CHUNK_SIZE)
				bufDB, err = self.RetrieveDBChunk(u, databaseHash)
				if err != nil {
					return tableNames, &SWARMDBError{message: fmt.Sprintf("[swarmdb:ListTables] RetrieveDBChunk %s", err)}
				}

				for j := 64; j < CHUNK_SIZE; j += 64 {
					if EmptyBytes(bufDB[j:(j + TABLE_NAME_LENGTH_MAX)]) {
					} else {
						r := NewRow()
						r["table"] = string(bytes.Trim(bufDB[j:(j+TABLE_NAME_LENGTH_MAX)], "\x00"))
						tableNames = append(tableNames, r)
					}
				}
				return tableNames, nil
			}
		}
	}
	return tableNames, &SWARMDBError{message: fmt.Sprintf("[swarmdb:ListTables] Did not find database %s", database), ErrorCode: 476, ErrorMessage: fmt.Sprintf("Database [%s] Not Found", database)}
}

// TODO: Review adding owner string, database string input parameters where the goal is to get database.owner/table/key type HTTP urls like:
//       https://swarm.wolk.com/wolkinc.eth => GET: ListDatabases
//       https://swarm.wolk.com/videos.wolkinc.eth => GET; ListTables
//       https://swarm.wolk.com/videos.wolkinc.eth/user => GET: DescribeTable
//       https://swarm.wolk.com/videos.wolkinc.eth/user/sourabhniyogi => GET: Get
// TODO: check for the existence in the owner-database combination before creating.
// TODO: need to make sure the types of the columns are correct
func (self *SwarmDB) CreateTable(u *SWARMDBUser, owner string, database string, tableName string, columns []Column) (tbl *Table, err error) {
	columnsMax := COLUMNS_PER_TABLE_MAX
	primaryColumnName := ""
	if len(columns) > columnsMax {
		return tbl, &SWARMDBError{message: fmt.Sprintf("[swarmdb:CreateTable] Max Allowed Columns for a table is %s and you submit %s", columnsMax, len(columns)), ErrorCode: 409, ErrorMessage: fmt.Sprintf("Max Allowed Columns exceeded - [%d] supplied, max is [MaxNumColumns]", len(columns), columnsMax)}
	}

	if len(tableName) > TABLE_NAME_LENGTH_MAX {
		return tbl, &SWARMDBError{message: fmt.Sprintf("[swarmdb:CreateTable] Maximum length of table name exceeded (max %d chars)", TABLE_NAME_LENGTH_MAX), ErrorCode: 500, ErrorMessage: fmt.Sprintf("Max table name length exceeded")}
	}

	//error checking
	for _, columninfo := range columns {
		if columninfo.Primary > 0 {
			if len(primaryColumnName) > 0 {
				return tbl, &SWARMDBError{message: "[swarmdb:CreateTable] More than one primary column", ErrorCode: 406, ErrorMessage: "Multiple Primary keys specified in Create Table"}
			}
			primaryColumnName = columninfo.ColumnName
		}
		if !CheckColumnType(columninfo.ColumnType) {
			return tbl, &SWARMDBError{message: fmt.Sprintf("[swarmdb:CreateTable] bad columntype"), ErrorCode: 407, ErrorMessage: "Invalid ColumnType: [columnType]"}
		}
		if !CheckIndexType(columninfo.IndexType) {
			return tbl, &SWARMDBError{message: fmt.Sprintf("[swarmdb:CreateTable] bad indextype"), ErrorCode: 408, ErrorMessage: "Invalid IndexType: [indexType]"}
		}
	}
	if len(primaryColumnName) == 0 {
		return tbl, &SWARMDBError{message: fmt.Sprintf("[swarmdb:CreateTable] no primary column indicated"), ErrorCode: 405, ErrorMessage: "No Primary Key specified in Create Table"}
	}

	// creating a database results in a new entry, e.g. "videos" in the owners ENS e.g. "wolktoken.eth" stored in a single chunk
	// e.g.  key 1: wolktoken.eth (up to 64 chars)
	//       key 2: videos     => 32 byte hash, pointing to tables of "video'
	ownerHash := crypto.Keccak256([]byte(owner))
	databaseName := make([]byte, DATABASE_NAME_LENGTH_MAX)
	databaseHash := make([]byte, 32)
	copy(databaseName[0:], database)

	// look up what databases the owner has already
	ownerDatabaseChunkID, err := self.GetRootHash(ownerHash)
	if err != nil {
		return tbl, GenerateSWARMDBError(err, fmt.Sprintf("[swarmdb:GetDatabase] GetRootHash %s", err))
	}
	log.Debug(fmt.Sprintf("[swarmdb:CreateTable] GetRootHash using ownerHash (%x) for DBChunkID => (%x)", ownerHash, ownerDatabaseChunkID))
	var buf []byte
	var bufDB []byte
	dbi := 0
	encrypted := 0
	if EmptyBytes(ownerDatabaseChunkID) {
		return tbl, &SWARMDBError{message: fmt.Sprintf("[swarmdb:GetDatabase] No database", err), ErrorCode: 443, ErrorMessage: "Database Specified Not Found"}
	} else {
		found := false
		// buf holds a list of the owner's databases
		buf, err = self.RetrieveDBChunk(u, ownerDatabaseChunkID)
		if err != nil {
			return tbl, GenerateSWARMDBError(err, fmt.Sprintf("[swarmdb:GetDatabase] RetrieveDBChunk %s", err))
		}

		// the first 32 bytes of the buf should match the ownerHash
		if bytes.Compare(buf[0:32], ownerHash[0:32]) != 0 {
			return tbl, &SWARMDBError{message: fmt.Sprintf("[swarmdb:GetDatabase] Invalid owner %x != %x", ownerHash, buf[0:32]), ErrorCode: 450, ErrorMessage: "Invalid Owner Specified"}
		}

		// look for the database
		for i := 64; i < CHUNK_SIZE; i += 64 {
			if (bytes.Compare(buf[i:(i+DATABASE_NAME_LENGTH_MAX)], databaseName) == 0) && (found == false) {
				log.Debug(fmt.Sprintf("Found Database [%s] and it's encrypted bit is: [%+v]", databaseName, buf[i+DATABASE_NAME_LENGTH_MAX]))
				if buf[i+DATABASE_NAME_LENGTH_MAX] > 0 {
					encrypted = 1
				}
				// database is found, so we have the databaseHash now
				dbi = i
				databaseHash = make([]byte, 32)
				copy(databaseHash[:], buf[(i+32):(i+64)])
				// bufDB has the tables
				log.Debug(fmt.Sprintf("Pulled bufDB using [%x]", databaseHash))
				bufDB, err = self.RetrieveDBChunk(u, databaseHash)
				if err != nil {
					return tbl, GenerateSWARMDBError(err, fmt.Sprintf("[swarmdb:GetDatabase] RetrieveDBChunk %s", err))
				}
				found = true
				break //TODO: think this should be ok?
			}
		}
		if !found {
			return tbl, &SWARMDBError{message: fmt.Sprintf("[swarmdb:GetDatabase] Database could not be found"), ErrorCode: 443, ErrorMessage: "Database Specified Not Found"}
			//TODO: ErrorCode/Msg
		}
	}

	// add table to bufDB
	found := false
	for i := 64; i < CHUNK_SIZE; i += 64 {
		if EmptyBytes(bufDB[i:(i + 32)]) {
			if found == true {
			} else {
				// update the table name in bufDB and write the chunk
				tblN := make([]byte, 32)
				copy(tblN[0:32], tableName)
				copy(bufDB[i:(i+32)], tblN[0:32])
				log.Debug(fmt.Sprintf("Copying tableName [%s] to bufDB [%s]", tblN[0:32], bufDB[i:(i+32)]))
				newdatabaseHash, err := self.StoreDBChunk(u, bufDB, encrypted)
				if err != nil {
					return tbl, GenerateSWARMDBError(err, fmt.Sprintf("[swarmdb:CreateTable] StoreDBChunk %s", err))
				}

				// update the database hash in the owner's databases
				copy(buf[(dbi+32):(dbi+64)], newdatabaseHash[0:32])
				ownerDatabaseChunkID, err = self.StoreDBChunk(u, buf, 0) // TODO

				if err != nil {
					return tbl, GenerateSWARMDBError(err, fmt.Sprintf("[swarmdb:CreateTable] StoreDBChunk %s", err))
				}
				log.Debug(fmt.Sprintf("[swarmdb:CreateTable] Storing Hash of (%x) and ChunkID: [%s]", ownerHash, ownerDatabaseChunkID))
				err = self.StoreRootHashWithStatus(ownerHash, ownerDatabaseChunkID, 2)
				if err != nil {
					return tbl, GenerateSWARMDBError(err, fmt.Sprintf("[swarmdb:CreateTable] StoreRootHash %s", err.Error()))
				}
				debugbufDB, _ := self.RetrieveDBChunk(u, newdatabaseHash)
				log.Debug(fmt.Sprintf("debugbufDB[%d:%d] => [%s] using [%x]", i, i+32, debugbufDB[i:(i+32)], newdatabaseHash))
				found = true
				break //TODO: This ok?
			}
		} else {
			tbl0 := string(bytes.Trim(bufDB[i:(i+32)], "\x00"))
			log.Debug(fmt.Sprintf("Comparing tableName [%s](%+v) to tbl0 [%s](%+v)", tableName, tableName, tbl0, tbl0))
			if strings.Compare(tableName, tbl0) == 0 {
				return tbl, &SWARMDBError{message: fmt.Sprintf("[swarmdb:CreateTable] table exists already"), ErrorCode: 500, ErrorMessage: "Table exists already"}
			}
		}
	}

	// ok now make the table!
	log.Debug(fmt.Sprintf("Creating Table [%s] - Owner [%s] Database [%s]\n", tableName, owner, database))
	tbl = self.NewTable(owner, database, tableName)
	tbl.encrypted = encrypted
	for i, columninfo := range columns {
		copy(buf[2048+i*64:], columninfo.ColumnName)
		b := make([]byte, 1)
		b[0] = byte(columninfo.Primary)
		copy(buf[2048+i*64+26:], b)

		intColumnInfo, _ := ColumnTypeToInt(columninfo.ColumnType)
		//TODO: check this
		b[0] = byte(intColumnInfo)
		copy(buf[2048+i*64+28:], b)

		intIndexType := IndexTypeToInt(columninfo.IndexType)
		b[0] = byte(intIndexType)
		copy(buf[2048+i*64+30:], b) // columninfo.IndexType
		// fmt.Printf(" column: %v\n", columninfo)
	}

	//Could (Should?) be less bytes, but leaving space in case more is to be there
	copy(buf[4000:4024], IntToByte(tbl.encrypted))

	log.Debug(fmt.Sprintf("Storing Table with encrypted bit set to %d [%v]", tbl.encrypted, buf[4000:4024]))
	swarmhash, err := self.StoreDBChunk(u, buf, tbl.encrypted)
	if err != nil {
		return tbl, GenerateSWARMDBError(err, fmt.Sprintf("[swarmdb:CreateTable] StoreDBChunk %s", err.Error()))
	}
	tbl.primaryColumnName = primaryColumnName
	tbl.roothash = swarmhash

	tblKey := self.GetTableKey(tbl.Owner, tbl.Database, tbl.tableName)

	log.Debug(fmt.Sprintf("**** CreateTable (owner [%s] database [%s] tableName: [%s]) Primary: [%s] tblKey: [%s] Roothash:[%x]\n", tbl.Owner, tbl.Database, tbl.tableName, tbl.primaryColumnName, tblKey, swarmhash))
	err = self.StoreRootHashWithStatus([]byte(tblKey), []byte(swarmhash), 2)
	if err != nil {
		return tbl, GenerateSWARMDBError(err, fmt.Sprintf("[swarmdb:CreateTable] StoreRootHash %s", err.Error()))
	}
	err = tbl.OpenTable(u)
	if err != nil {
		return tbl, GenerateSWARMDBError(err, fmt.Sprintf("[swarmdb:CreateTable] OpenTable %s", err.Error()))
	}
	self.RegisterTable(owner, database, tableName, tbl)
	return tbl, nil
}

func (self *SwarmDB) GetTableKey(owner string, database string, tableName string) (key string) {
	return fmt.Sprintf("%s|%s|%s", owner, database, tableName)
}

func (self *SwarmDB) Close(){
/*
	for _, table := range self.tables{
		table.Close()
	}
*/
}
