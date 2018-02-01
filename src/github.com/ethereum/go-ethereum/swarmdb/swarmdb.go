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
	"github.com/ethereum/go-ethereum/log"
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
	Error            SWARMDBError `json:"error,omitempty"`
	ErrorCode        int          `json:"errorcode,omitempty"`
	ErrorMessage     string       `json:"errormessage,omitempty"`
	Data             []Row        `json:"data,omitempty"`
	AffectedRowCount int          `json:"affectedrowcount,omitempty"`
	MatchedRowCount  int          `json:"matchedrowcount,omitempty"`
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
	ens          ENSSimulation
	kaddb        *KademliaDB
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

type RequestType string

const (
	RT_CREATE_DATABASE   = "CreateDatabase"
	RT_DESCRIBE_DATABASE = "DescribeDatabase"
	RT_LIST_DATABASES    = "ListDatabases"
	RT_DROP_DATABASE     = "SelectDatabase"

	RT_CREATE_TABLE   = "CreateTable"
	RT_DESCRIBE_TABLE = "DescribeTable"
	RT_LIST_TABLES    = "ListTables"
	RT_DROP_TABLE     = "DropTable"

	RT_START_BUFFER = "StartBuffer"
	RT_FLUSH_BUFFER = "FlushBuffer"

	RT_PUT    = "Put"
	RT_GET    = "Get"
	RT_DELETE = "Delete"
	RT_QUERY  = "Query"
)

const (
	DATABASE_NAME_LENGTH_MAX = 31
	DATABASES_PER_USER_MAX   = 30
	COLUMNS_PER_TABLE_MAX    = 30
)

func NewSwarmDB(ensPath string, chunkDBPath string) (swdb *SwarmDB, err error) {
	sd := new(SwarmDB)
	sd.tables = make(map[string]*Table)
	chunkdbFileName := "chunk.db"
	dbChunkStoreFullPath := filepath.Join(chunkDBPath, chunkdbFileName)
	dbchunkstore, err := NewDBChunkStore(dbChunkStoreFullPath)
	if err != nil {
		return swdb, GenerateSWARMDBError(err, `[swarmdb:NewSwarmDB] NewDBChunkStore `+err.Error())
	} else {
		sd.dbchunkstore = dbchunkstore
	}

	//default /tmp/ens.db
	ensdbFileName := "ens.db"
	ensdbFullPath := filepath.Join(ensPath, ensdbFileName)
	ens, errENS := NewENSSimulation(ensdbFullPath)
	if errENS != nil {
		return swdb, GenerateSWARMDBError(errENS, `[swarmdb:NewSwarmDB] NewENSSimulation `+errENS.Error())
	} else {
		sd.ens = ens
	}

	kaddb, err := NewKademliaDB(dbchunkstore)
	if err != nil {
		return swdb, GenerateSWARMDBError(err, `[swarmdb:NewSwarmDB] NewKademliaDB `+err.Error())
	} else {
		sd.kaddb = kaddb
	}

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

func (self *SwarmDB) StoreDBChunk(u *SWARMDBUser, val []byte, encrypted int) (key []byte, err error) {
	key, err = self.dbchunkstore.StoreChunk(u, val, encrypted)
	return key, err
}

// ENSSimulation  API
func (self *SwarmDB) GetRootHash(u *SWARMDBUser, tblKey []byte /* GetTableKeyValue */) (roothash []byte, err error) {
	log.Debug(fmt.Sprintf("[GetRootHash] Getting Root Hash for (%s)[%x] ", tblKey, tblKey))
	return self.ens.GetRootHash(u, tblKey)
}

func (self *SwarmDB) StoreRootHash(u *SWARMDBUser, fullTableName []byte /* GetTableKey Value */, roothash []byte) (err error) {
	return self.ens.StoreRootHash(u, fullTableName, roothash)
}

// parse sql and return rows in bulk (order by, group by, etc.)
func (self *SwarmDB) QuerySelect(u *SWARMDBUser, query *QueryOption) (rows []Row, err error) {
	table, err := self.GetTable(u, query.Owner, query.Database, query.Table)
	if err != nil {
		return rows, GenerateSWARMDBError(err, `[swarmdb:QuerySelect] GetTable `+err.Error())
	}

	//var rawRows []Row
	log.Debug("QueryOwner is: [%s]\n", query.Owner)
	colRows, err := self.Scan(u, query.Owner, query.Database, query.Table, table.primaryColumnName, query.Ascending)
	if err != nil {
		return rows, GenerateSWARMDBError(err, `[swarmdb:QuerySelect] Scan `+err.Error())
	}
	// fmt.Printf("\nColRows = [%+v]", colRows)

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
func (self *SwarmDB) QueryInsert(u *SWARMDBUser, query *QueryOption) (err error) {

	table, err := self.GetTable(u, query.Owner, query.Database, query.Table)
	if err != nil {
		return GenerateSWARMDBError(err, `[swarmdb:QueryInsert] GetTable `+err.Error())
	}
	for _, row := range query.Inserts {
		// check if primary column exists in Row
		if _, ok := row[table.primaryColumnName]; !ok {
			return &SWARMDBError{message: fmt.Sprintf("[swarmdb:QueryInsert] Insert row %+v needs primary column '%s' value", row, table.primaryColumnName), ErrorCode: 446, ErrorMessage: fmt.Sprintf("Insert Query Missing Primary Key [%]", table.primaryColumnName)}
		}
		// check if Row already exists
		convertedKey, err := convertJSONValueToKey(table.columns[table.primaryColumnName].columnType, row[table.primaryColumnName])
		if err != nil {
			return GenerateSWARMDBError(err, fmt.Sprintf("[swarmdb:QueryInsert] convertJSONValueToKey - %s", err.Error()))
		}
		existingByteRow, err := table.Get(u, convertedKey)
		if len(existingByteRow) > 0 || err == nil {
			existingRow, errB := table.byteArrayToRow(existingByteRow)
			if errB != nil {
				return GenerateSWARMDBError(errB, fmt.Sprintf("[swarmdb:QueryInsert] byteArrayToRow - %s", errB.Error()))
			}
			return &SWARMDBError{message: fmt.Sprintf("[swarmdb:QueryInsert] Insert row key %s already exists: %+v", row[table.primaryColumnName], existingRow), ErrorCode: 434, ErrorMessage: fmt.Sprintf("Record with key [%s] already exists.  If you wish to modify, please use UPDATE SQL statement or PUT", convertedKey)}
		}
		// put the new Row in
		err = table.Put(u, row)
		if err != nil {
			return GenerateSWARMDBError(err, fmt.Sprintf("[swarmdb:QueryInsert] Put %s", err.Error()))
		}
	}
	return nil
}

// Update is for modifying existing data in the table (can use a Where clause)
// example: 'UPDATE tablename SET col1=value1, col2=value2 WHERE col3 > 0'
func (self *SwarmDB) QueryUpdate(u *SWARMDBUser, query *QueryOption) (err error) {
	table, err := self.GetTable(u, query.Owner, query.Database, query.Table)
	if err != nil {
		return GenerateSWARMDBError(err, fmt.Sprintf("[swarmdb:QueryUpdate] GetTable %s", err.Error()))
	}

	// get all rows with Scan, using primary key column
	rawRows, err := self.Scan(u, query.Owner, query.Database, query.Table, table.primaryColumnName, query.Ascending)
	if err != nil {
		return GenerateSWARMDBError(err, fmt.Sprintf("[swarmdb:QueryUpdate] Scan %s", err.Error()))
	}

	// check to see if Update cols are in pulled set
	for colname, _ := range query.Update {
		if _, ok := table.columns[colname]; !ok {
			return &SWARMDBError{message: fmt.Sprintf("[swarmdb:QueryUpdate] Update SET column name %s is not in table", colname), ErrorCode: 445, ErrorMessage: fmt.Sprintf("Attempting to update a column [%s] which is not in table [%s]", colname, table.tableName)}
		}
	}

	// apply WHERE clause
	filteredRows, err := table.applyWhere(rawRows, query.Where)
	if err != nil {
		return GenerateSWARMDBError(err, fmt.Sprintf("[swarmdb:QueryUpdate] applyWhere %s", err.Error()))
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
	for _, row := range filteredRows {
		err := table.Put(u, row)
		if err != nil {
			return GenerateSWARMDBError(err, fmt.Sprintf("[swarmdb:QueryUpdate] Put %s", err.Error()))
		}
	}
	return nil
}

//Delete is for deleting data rows (can use a Where clause, not just a key)
//example: 'DELETE FROM tablename WHERE col1 = value1'
func (self *SwarmDB) QueryDelete(u *SWARMDBUser, query *QueryOption) (err error) {

	table, err := self.GetTable(u, query.Owner, query.Database, query.Table)
	if err != nil {
		return GenerateSWARMDBError(err, fmt.Sprintf("[swarmdb:QueryDelete] GetTable %s", err.Error()))
	}

	//get all rows with Scan, using Where's specified col
	rawRows, err := self.Scan(u, query.Owner, query.Database, query.Table, query.Where.Left, query.Ascending)
	if err != nil {
		return GenerateSWARMDBError(err, fmt.Sprintf("[swarmdb:QueryDelete] Scan %s", err.Error()))
	}

	//apply WHERE clause
	filteredRows, err := table.applyWhere(rawRows, query.Where)
	if err != nil {
		return GenerateSWARMDBError(err, fmt.Sprintf("[swarmdb:QueryDelete] applyWhere %s", err.Error()))
	}

	//delete the selected rows
	for _, row := range filteredRows {
		ok, err := table.Delete(u, row[table.primaryColumnName].(string))
		if err != nil {
			return GenerateSWARMDBError(err, fmt.Sprintf("[swarmdb:QueryDelete] Delete %s", err.Error()))
		}
		if !ok {
			// TODO: if !ok, what should happen? return appropriate response -- number of records affected
		}
	}
	return &SWARMDBError{message: "DELETE Quereies Not currently supported", ErrorCode: 401, ErrorMessage: "SQL Parsing error: [DELETE queries not currently supported]"}
	//TODO:
}

func (self *SwarmDB) Query(u *SWARMDBUser, query *QueryOption) (rows []Row, err error) {
	switch query.Type {
	case "Select":
		rows, err := self.QuerySelect(u, query)
		if err != nil {
			return rows, GenerateSWARMDBError(err, fmt.Sprintf("[swarmdb:Query] QuerySelect %s", err.Error()))
		}
		/* TODO: Not an Error to have nothing come back
		if len(rows) == 0 {
			return rows, GenerateSWARMDBError(err, fmt.Sprintf("[swarmdb:Query] select query came back empty"))
		}
		*/
		return rows, nil
	case "Insert":
		err = self.QueryInsert(u, query)
		if err != nil {
			return rows, GenerateSWARMDBError(err, fmt.Sprintf("[swarmdb:Query] QueryInsert %s", err.Error()))
		}
		return rows, nil
	case "Update":
		err = self.QueryUpdate(u, query)
		if err != nil {
			return rows, GenerateSWARMDBError(err, fmt.Sprintf("[swarmdb:Query] QueryUpdate %s", err.Error()))
		}
		return rows, nil
	case "Delete":
		err = self.QueryDelete(u, query)
		if err != nil {
			return rows, GenerateSWARMDBError(err, fmt.Sprintf("[swarmdb:Query] QueryDelete %s", err.Error()))
		}
		return rows, nil
	}
	return rows, nil
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

	var tblKey string
	var tbl *Table
	var tblInfo map[string]Column
	if d.RequestType != "CreateTable" && d.RequestType != "CreateDatabase" && d.RequestType != "DropDatabase" && d.RequestType != "ListDatabases" && d.RequestType != "DescribeDatabase" && d.RequestType != "ListTables" && d.RequestType != RT_QUERY {
		tblKey = self.GetTableKey(d.Owner, d.Database, d.Table)
		tbl, err = self.GetTable(u, d.Owner, d.Database, d.Table)
		log.Debug(fmt.Sprintf("[swarmdb:SelectHandler] GetTable returned table: [%+v] for tablekey: [%s]\n", tbl, tblKey))
		if err != nil {
			return resp, GenerateSWARMDBError(err, fmt.Sprintf("[swarmdb:GetTable] OpenTable %s", err.Error()))
		}
		tblInfo, err = tbl.DescribeTable()
		if err != nil {
			return resp, GenerateSWARMDBError(err, fmt.Sprintf("[swarmdb:SelectHandler] DescribeTable %s", err.Error()))
		}
	}

	switch d.RequestType {
	case RT_CREATE_DATABASE:
		err = self.CreateDatabase(u, d.Owner, d.Database, d.Encrypted)
		if err != nil {
			return resp, GenerateSWARMDBError(err, fmt.Sprintf("[swarmdb:SelectHandler] CreateDatabase %s", err.Error()))
		}

		return SWARMDBResponse{AffectedRowCount: 1}, nil
	case RT_DROP_DATABASE:
		err = self.DropDatabase(u, d.Owner, d.Database)
		if err != nil {
			return resp, GenerateSWARMDBError(err, fmt.Sprintf("[swarmdb:SelectHandler] DropDatabase %s", err.Error()))
			//TODO: SWARMDBResponse{Error: GenerateSWARMDBError(err, fmt.Sprintf("[swarmdb:SelectHandler] DropDatabase %s", err.Error()))}
		}
		return SWARMDBResponse{AffectedRowCount: 1}, nil
	case RT_LIST_DATABASES:
		databases, err := self.ListDatabases(u, d.Owner)
		resp.Data = databases
		if err != nil {
			return resp, GenerateSWARMDBError(err, fmt.Sprintf("[swarmdb:SelectHandler] ListDatabases %s", err.Error()))
		}
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
		err = self.DropTable(u, d.Owner, d.Database)
		if err != nil {
			return resp, GenerateSWARMDBError(err, fmt.Sprintf("[swarmdb:SelectHandler] DropTable %s", err.Error()))
		}
		return SWARMDBResponse{AffectedRowCount: 1}, nil
	case RT_DESCRIBE_TABLE:
		tblcols, err := self.tables[tblKey].DescribeTable()
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
		/*
		    tblinfo, err := json.Marshal(tblcols)
		   		if err != nil {
		   			return resp, GenerateSWARMDBError(err, fmt.Sprintf("[swarmdb:SelectHandler] Marshal %s", err.Error()))
		   		}
		*/
		return resp, nil
	case RT_LIST_TABLES:
		ret, err := self.ListTables(u, d.Owner, d.Database)
		if err != nil {
			return resp, GenerateSWARMDBError(err, fmt.Sprintf("[swarmdb:SelectHandler] DescribeDatabase %s", err.Error()))
		}
		resp.Data = ret
		return resp, nil
	case RT_PUT:
		d.Rows, err = tbl.assignRowColumnTypes(d.Rows)
		//fmt.Printf("\nPut DATA: [%+v]\n", d)
		if err != nil {
			return resp, GenerateSWARMDBError(err, fmt.Sprintf("[swarmdb:SelectHandler] assignRowColumnTypes %s", err.Error()))
		}

		//error checking for primary column, and valid columns
		for _, row := range d.Rows {
			log.Debug("checking row %v\n", row)
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
		if isNil(d.Key) {
			return resp, &SWARMDBError{message: fmt.Sprintf("[swarmdb:SelectHandler] Get - Missing Key"), ErrorCode: 433, ErrorMessage: "GET Request Missing Key"}
		}
		primaryColumnType := tbl.columns[tbl.primaryColumnName].columnType
		convertedKey, err := convertJSONValueToKey(primaryColumnType, d.Key)
		if err != nil {
			return resp, GenerateSWARMDBError(err, fmt.Sprintf("[swarmdb:SelectHandler] convertJSONValueToKey %s", err.Error()))
		}
		byteRow, err := tbl.Get(u, convertedKey)
		if err != nil {
			//fmt.Printf(fmt.Sprintf("[swarmdb:SelectHandler] Get %s", err.Error()))
			return resp, GenerateSWARMDBError(err, fmt.Sprintf("[swarmdb:SelectHandler] Get %s", err.Error()))
		}
		validRow, err2 := tbl.byteArrayToRow(byteRow)
		if err2 != nil {
			return resp, GenerateSWARMDBError(err2, fmt.Sprintf("[swarmdb:SelectHandler] byteArrayToRow %s", err2.Error()))
		}
		resp.Data = append(resp.Data, validRow)
		return resp, nil
	case RT_DELETE:
		if isNil(d.Key) {
			return resp, &SWARMDBError{message: fmt.Sprintf("[swarmdb:SelectHandler] Delete is Missing Key"), ErrorCode: 448, ErrorMessage: "Delete Statement missing KEY"}
		}
		_, err = tbl.Delete(u, d.Key)
		if err != nil {
			return resp, GenerateSWARMDBError(err, fmt.Sprintf("[swarmdb:SelectHandler] Delete %s", err.Error()))
		}
		return SWARMDBResponse{AffectedRowCount: 1}, nil
	case RT_START_BUFFER:
		err = tbl.StartBuffer(u)
		if err != nil {
			return resp, GenerateSWARMDBError(err, fmt.Sprintf("[swarmdb:SelectHandler] StartBuffer %s", err.Error()))
		}
		//TODO: update to use real "count"
		return SWARMDBResponse{AffectedRowCount: 1}, nil
	case RT_FLUSH_BUFFER:
		err = tbl.FlushBuffer(u)
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
		query.Owner = d.Owner       //probably should check the owner against the tableinfo owner here
		query.Database = d.Database //probably should check the owner against the tableinfo owner here
		fmt.Printf("d.Table: %v, query.Table: %v\n", d.Table, query.Table)
		if len(d.Table) == 0 {
			fmt.Printf("Getting Table from Query rather than data obj\n")
			//TODO: check if empty even after query.Table check
			d.Table = query.Table //since table is specified in the query we do not have get it as a separate input
		}
		fmt.Printf("right before GetTable, u: %v, d.Owner: %v, d.Table: %v \n", u, d.Owner, d.Table)
		tblKey = self.GetTableKey(d.Owner, d.Database, d.Table)
		tbl, err = self.GetTable(u, d.Owner, d.Database, d.Table)
		log.Debug(fmt.Sprintf("[swarmdb:SelectHandler] GetTable returned table: [%+v] for tablekey: [%s]\n", tbl, tblKey))
		if err != nil {
			return resp, GenerateSWARMDBError(err, fmt.Sprintf("[swarmdb:GetTable] OpenTable %s", err.Error()))
		}
		tblInfo, err = tbl.DescribeTable()
		if err != nil {
			return resp, GenerateSWARMDBError(err, fmt.Sprintf("[swarmdb:SelectHandler] DescribeTable %s", err.Error()))
		}

		//fmt.Printf("Table info gotten: [%+v]\n", tblInfo)
		// fmt.Printf("QueryOption is: [%+v]\n", query)

		//checking validity of columns
		for _, reqCol := range query.RequestColumns {
			if _, ok := tblInfo[reqCol.ColumnName]; !ok {
				return resp, &SWARMDBError{message: fmt.Sprintf("[swarmdb:SelectHandler] Requested col [%s] does not exist in table [%+v]", reqCol.ColumnName, tblInfo), ErrorCode: 404, ErrorMessage: fmt.Sprintf("Column Does Not Exist in table definition: [%s]", reqCol.ColumnName)}
			}
		}

		//checking the Where clause
		if len(query.Where.Left) > 0 {
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

				byteRow, err := tbl.Get(u, convertedKey)
				if err != nil {
					return resp, GenerateSWARMDBError(err, fmt.Sprintf("[swarmdb:SelectHandler] Get %s", err.Error()))
				}

				row, err := tbl.byteArrayToRow(byteRow)
				// fmt.Printf("Response row from Get: %s (%v)\n", row, row)
				if err != nil {
					return resp, GenerateSWARMDBError(err, fmt.Sprintf("[swarmdb:SelectHandler] byteArrayToRow %s", err.Error()))
				}

				filteredRow := filterRowByColumns(row, query.RequestColumns)
				// fmt.Printf("\nResponse filteredrow from Get: %s (%v)", filteredRow, filteredRow)
				resp.Data = append(resp.Data, filteredRow)
				return resp, nil
			}
		}

		// process the query
		qRows, err := self.Query(u, &query)
		// fmt.Printf("\nQRows: [%+v]", qRows)
		if err != nil {
			return resp, GenerateSWARMDBError(err, fmt.Sprintf("[swarmdb:SelectHandler] Query [%+v] %s", query, err.Error()))
		}
		return SWARMDBResponse{AffectedRowCount: len(qRows), Data: qRows}, nil
	}
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
	ownerDatabaseChunkID, err := self.ens.GetRootHash(u, ownerHash)
	if err != nil {
		return GenerateSWARMDBError(err, fmt.Sprintf("[swarmdb:CreateDatabase] GetRootHash %s", err))
	}
	log.Debug(fmt.Sprintf("[swarmdb:CreateDatabase] Getting Root Hash using ownerHash [%x] and got [%x]", ownerHash, ownerDatabaseChunkID))
	buf := make([]byte, 4096)
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
		for i := 64; i < 4096; i += 64 {
			log.Debug(fmt.Sprintf("Comparing buf[%d:%d] => %s (%+v) to newDBName => %s (%+v)", i, i+DATABASE_NAME_LENGTH_MAX, buf[i:(i+DATABASE_NAME_LENGTH_MAX)], buf[i:(i+DATABASE_NAME_LENGTH_MAX)], newDBName, newDBName))
			if bytes.Equal(buf[i:(i+DATABASE_NAME_LENGTH_MAX)], newDBName) {
				return &SWARMDBError{message: "[swarmdb:CreateDatabase] Database exists already", ErrorCode: 500, ErrorMessage: "Database Exists Already"}
			}
		}
	}

	for i := 64; i < 4096; i += 64 {
		// find the first 000 byte entry
		if EmptyBytes(buf[i:(i + 64)]) {
			fmt.Printf("Byte: %d\n", i)
			// make a new database chunk, with the first 32 bytes of the chunk being the database name (the next keys will be the tables)
			bufDB := make([]byte, 4096)
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

			ownerDatabaseChunkID, err = self.StoreDBChunk(u, buf, encrypted)
			if err != nil {
				return GenerateSWARMDBError(err, fmt.Sprintf("[swarmdb:CreateDatabase] StoreDBChunk %s", err.Error()))
			}

			err = self.StoreRootHash(u, ownerHash, ownerDatabaseChunkID)
			if err != nil {
				return GenerateSWARMDBError(err, fmt.Sprintf("[swarmdb:CreateDatabase] StoreRootHash %s", err.Error()))
			}
			return nil
		}
	}
	return &SWARMDBError{message: fmt.Sprintf("[swarmdb:CreateDatabase] Database could not be created -- exceeded allocation"), ErrorCode: 451, ErrorMessage: fmt.Sprintf("Database could not be created -- exceeded allocation of %d", DATABASE_NAME_LENGTH_MAX)}
}

// dropping a database removes the ENS entry
func (self *SwarmDB) DropDatabase(u *SWARMDBUser, owner string, database string) (err error) {
	return nil
}

func (self *SwarmDB) DescribeDatabase(u *SWARMDBUser, owner string, database string) (ret string, err error) {
	return OK_RESPONSE, nil
}

func (self *SwarmDB) ListDatabases(u *SWARMDBUser, owner string) (ret []Row, err error) {
	ownerHash := crypto.Keccak256([]byte(owner))
	// look up what databases the owner has
	ownerDatabaseChunkID, err := self.ens.GetRootHash(u, ownerHash)
	if err != nil {
		return ret, GenerateSWARMDBError(err, fmt.Sprintf("[swarmdb:ListDatabases] GetRootHash %s", err))
	}
	log.Debug(fmt.Sprintf("list databases: %x -> %x\n", ownerHash, ownerDatabaseChunkID))

	buf := make([]byte, 4096)
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
		for i := 64; i < 4096; i += 64 {
			if EmptyBytes(buf[i:(i + 32)]) {
			} else {
				r := NewRow()
				db := string(bytes.Trim(buf[i:(i+32)], "\x00"))
				//rowstring := fmt.Sprintf("{\"database\":\"%s\"}", db)
				r["database"] = db
				ret = append(ret, r)
			}
		}
	}

	return ret, nil
}

func (self *SwarmDB) DropTable(u *SWARMDBUser, owner string, database string) (err error) {
	return nil
}

func (self *SwarmDB) ListTables(u *SWARMDBUser, owner string, database string) (ret []Row, err error) {
	return ret, nil
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

	//error checking
	for _, columninfo := range columns {
		if columninfo.Primary > 0 {
			if len(primaryColumnName) > 0 {
				return tbl, &SWARMDBError{message: fmt.Sprintf("[swarmdb:CreateTable] More than one primary column"), ErrorCode: 406, ErrorMessage: "Multiple Primary keys specified in Create Table"}
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
	ownerDatabaseChunkID, err := self.ens.GetRootHash(u, ownerHash)
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
		for i := 64; i < 4096; i += 64 {
			log.Debug(fmt.Sprintf("Comparing buf[ %d : %d ] looking for [%s] -- currentbuff is [%s]", i, i+DATABASE_NAME_LENGTH_MAX, databaseName, buf[i:i+DATABASE_NAME_LENGTH_MAX]))
			//No plus 1 b/c databaseName actually uses DATABASE_NAME_LENGTH_MAX for size
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
	for i := 64; i < 4096; i += 64 {
		log.Debug(fmt.Sprintf("looking at %d %d", i, i+32))
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
				ownerDatabaseChunkID, err = self.StoreDBChunk(u, buf, encrypted)
				if err != nil {
					return tbl, GenerateSWARMDBError(err, fmt.Sprintf("[swarmdb:CreateTable] StoreDBChunk %s", err))
				}
				log.Debug(fmt.Sprintf("[swarmdb:CreateTable] Storing Hash of (%x) and ChunkID: [%s]", ownerHash, ownerDatabaseChunkID))
				err = self.StoreRootHash(u, ownerHash, ownerDatabaseChunkID)
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
	fmt.Printf("Creating Table [%s] - Owner [%s] Database [%s]\n", tableName, owner, database)
	tbl = self.NewTable(owner, database, tableName)
	tbl.encrypted = encrypted
	for i, columninfo := range columns {
		copy(buf[2048+i*64:], columninfo.ColumnName)
		b := make([]byte, 1)
		b[0] = byte(columninfo.Primary)
		copy(buf[2048+i*64+26:], b)

		b[0] = byte(columninfo.ColumnType)
		copy(buf[2048+i*64+28:], b)

		b[0] = byte(columninfo.IndexType)
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
	err = self.StoreRootHash(u, []byte(tblKey), []byte(swarmhash))
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
