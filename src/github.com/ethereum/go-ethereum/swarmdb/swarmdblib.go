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

// SWARMDB Go client
package swarmdb

import (
	"bufio"
	"encoding/json"
	"fmt"
	"net"
	//"os"
	"strconv"
	"strings"
	// "github.com/ethereum/go-ethereum/swarmdb/keymanager"
	//"time"
)

type SWARMDBConnection struct {
	connection      net.Conn
	keymanager      KeyManager
	reader          *bufio.Reader
	writer          *bufio.Writer
	connectionOwner string //owner of the connection; right now the same as "owner" in singleton mode
	Owner           string //owner of the databases/tables
	Databases       map[string]*SWARMDBDatabase
}

type SWARMDBDatabase struct {
	DBConnection *SWARMDBConnection
	Name         string
	Encrypted    int //means all transactions on the tables in this db will be encrypted or not
	Tables       map[string]*SWARMDBTable
}

type SWARMDBTable struct {
	DBDatabase *SWARMDBDatabase
	Name       string
}

var CONN_HOST = "127.0.0.1" //default, but reads from config. TODO: default this to whatever config defaults to
var CONN_PORT = int(2001)   //default, but reads from config. TODO: default this to whatever config defaults to
var CONN_TYPE = "tcp"

// opens a TCP connection to ip port
func NewSWARMDBConnection(ip string, port int) (dbc SWARMDBConnection, err error) {

	//load up configs, set configs
	config, err := LoadSWARMDBConfig(SWARMDBCONF_FILE)
	if err != nil {
		return dbc, &SWARMDBError{message: fmt.Sprintf("[swarmdblib:NewSWARMDBConnection] LoadSWARMDBConfig %s", err.Error())}
	}
	dbc.keymanager, err = NewKeyManager(config)
	if err != nil {
		return dbc, &SWARMDBError{message: fmt.Sprintf("[swarmdblib:NewSWARMDBConnection] NewKeyManager %s", err.Error())}
	}
	if len(ip) > 0 {
		CONN_HOST = ip
	} else if len(config.ListenAddrTCP) > 0 {
		CONN_HOST = config.ListenAddrTCP
	}
	if port > 0 {
		CONN_PORT = config.PortTCP
	} else if config.PortTCP > 0 {
		CONN_PORT = config.PortTCP
	}

	//dial the tcp connection
	conn, err := net.Dial(CONN_TYPE, CONN_HOST+":"+strconv.Itoa(CONN_PORT))
	if err != nil {
		return dbc, &SWARMDBError{message: fmt.Sprintf("[swarmdblib:NewSWARMDBConnection] Dial %s", err.Error())}
	}

	//set the SWARDBConnection params
	dbc.connection = conn
	reader := bufio.NewReader(conn)
	writer := bufio.NewWriter(conn)
	dbc.reader = reader
	dbc.writer = writer
	dbc.connectionOwner = config.Address //TODO: this is ok for singleton node or default owner
	dbc.Owner = config.Address
	dbc.Databases = make(map[string]*SWARMDBDatabase)

	//server access challenge and verification
	challenge, err := dbc.reader.ReadString('\n') // read a random length string from the server
	if err != nil {
		return dbc, &SWARMDBError{message: fmt.Sprintf("[swarmdblib:Open] ReadString %s", err.Error())}
	}
	challenge = strings.Trim(challenge, "\n")
	// challenge_bytes, _ := hex.DecodeString(challenge)
	challenge_bytes := SignHash([]byte(challenge)) // sign the message Web3 style
	sig, err := dbc.keymanager.SignMessage(challenge_bytes)
	if err != nil {
		return dbc, err
	}
	response := fmt.Sprintf("%x", sig) // response should be hex string like this: "6b1c7b37285181ef74fb1946968c675c09f7967a3e69888ee37c42df14a043ac2413d19f96760143ee8e8d58e6b0bda4911f642912d2b81e1f2834814fcfdad700"
	//fmt.Printf("challenge:[%v] response:[%v]\n", challenge, response)
	//fmt.Printf("Challenge: [%x] Sig:[%x]\n", challenge_bytes, sig)
	dbc.writer.WriteString(response + "\n")
	dbc.writer.Flush()

	return dbc, nil
}

func (dbc *SWARMDBConnection) OpenDatabase(name string, encrypted int) (db *SWARMDBDatabase, err error) {

	if _, ok := dbc.Databases[name]; ok {
		return dbc.Databases[name], nil //database already exists, open it
	}

	databases, err := dbc.ListDatabases()
	if err != nil {
		return db, &SWARMDBError{message: fmt.Sprintf("[swarmdblib:OpenDatabase] ListDatabases %s", err.Error())}
	}
	if !findName(databases, name) {
		return db, &SWARMDBError{message: fmt.Sprintf("[swarmdblib:OpenDatabase] Database does not exist")}
	}

	db = new(SWARMDBDatabase)
	db.DBConnection = dbc
	db.Name = name
	db.Encrypted = encrypted
	db.Tables = make(map[string]*SWARMDBTable)
	dbc.Databases[db.Name] = db

	return db, nil
}

func (db *SWARMDBDatabase) OpenTable(name string) (tbl *SWARMDBTable, err error) {

	if _, ok := db.Tables[name]; ok {
		return db.Tables[name], nil //table already exists, open it
	}

	tables, err := db.ListTables()
	if err != nil {
		return tbl, &SWARMDBError{message: fmt.Sprintf("[swarmdblib:OpenTable] ListTables %s", err.Error())}
	}
	if !findName(tables, name) {
		return tbl, &SWARMDBError{message: fmt.Sprintf("[swarmdblib:OpenTable] Table does not exist")}
	}

	tbl = new(SWARMDBTable)
	tbl.Name = name
	tbl.DBDatabase = db
	db.Tables[tbl.Name] = tbl

	return tbl, nil
}

func (dbc *SWARMDBConnection) CreateDatabase(name string, encrypted int) (db *SWARMDBDatabase, err error) {

	//check to see if we have opened it before
	if _, ok := dbc.Databases[name]; ok {
		return dbc.Databases[name], &SWARMDBError{message: fmt.Sprintf("[swarmdblib:CreateDatabase] Database exists")}
	}

	//create request
	var req RequestOption
	req.RequestType = RT_CREATE_DATABASE
	req.Owner = dbc.Owner
	req.Database = name
	req.Encrypted = encrypted
	_, err = dbc.ProcessRequestResponseCommand(req) //send to server
	if err != nil {
		return db, &SWARMDBError{message: fmt.Sprintf("[swarmdblib:CreateDatabase] ProcessRequestResponseCommand %s", err.Error())}
	}

	//add to databases
	db = new(SWARMDBDatabase)
	db.DBConnection = dbc
	db.Name = name
	db.Encrypted = encrypted
	db.Tables = make(map[string]*SWARMDBTable)
	dbc.Databases[db.Name] = db

	return db, nil
}

func (dbc *SWARMDBConnection) DropDatabase(name string) (err error) {
	var req RequestOption
	req.RequestType = RT_DROP_DATABASE
	req.Owner = dbc.Owner
	req.Database = name
	_, err = dbc.ProcessRequestResponseCommand(req)
	if err != nil {
		return &SWARMDBError{message: fmt.Sprintf("[swarmdblib:DropDatabase] ProcessRequestResponseCommand %s", err.Error())}
	}

	delete(dbc.Databases, name)

	return nil
}

func (db *SWARMDBDatabase) CreateTable(name string, columns []Column) (tbl *SWARMDBTable, err error) {

	//check to see if we have opened it before
	if _, ok := db.Tables[name]; ok {
		return db.Tables[name], &SWARMDBError{message: fmt.Sprintf("[swarmdblib:CreateTable] Table exists")}
	}

	// create request
	var req RequestOption
	req.RequestType = RT_CREATE_TABLE
	req.Owner = db.DBConnection.Owner
	req.Database = db.Name
	req.Table = name
	req.Columns = columns
	_, err = db.DBConnection.ProcessRequestResponseCommand(req) //send it to the server
	if err != nil {
		return tbl, &SWARMDBError{message: fmt.Sprintf("[swarmdblib:CreateTable] ProcessRequestResponseCommand %s", err.Error())}
	}

	//add the new table
	tbl = new(SWARMDBTable)
	tbl.Name = name
	tbl.DBDatabase = db
	db.Tables[tbl.Name] = tbl

	return tbl, nil
}

func (db *SWARMDBDatabase) DropTable(name string) (err error) {
	var req RequestOption
	req.RequestType = RT_DROP_TABLE
	req.Owner = db.DBConnection.Owner
	req.Database = db.Name
	req.Table = name
	_, err = db.DBConnection.ProcessRequestResponseCommand(req) //send it to the server
	if err != nil {
		return &SWARMDBError{message: fmt.Sprintf("[swarmdblib:DropTable] ProcessRequestResponseCommand %s", err.Error())}
	}

	delete(db.Tables, name)

	return nil
}

func (dbc *SWARMDBConnection) ListDatabases() (databases []Row, err error) {
	//fmt.Printf("got into ListDatabases\n")
	var req RequestOption
	req.RequestType = RT_LIST_DATABASES
	req.Owner = dbc.Owner
	//fmt.Printf("sending this req: %+v\n", req)
	databases, err = dbc.ProcessRequestResponseCommand(req)
	//if err == nil {
	//	fmt.Printf("what's the err? %+v\n", err)
	//}
	if err != nil {
		//fmt.Printf("why get caught in this error? %+v\n", err)
		return databases, &SWARMDBError{message: fmt.Sprintf("[swarmdblib:ListDatabases] ProcessRequestResponseCommand %s", err.Error())}
	}
	//fmt.Printf("got back from Process into List and that's good\n")

	return databases, nil
}

func (db *SWARMDBDatabase) ListTables() (tables []Row, err error) {
	var req RequestOption
	req.RequestType = RT_LIST_TABLES
	req.Owner = db.DBConnection.Owner
	req.Database = db.Name
	tables, err = db.DBConnection.ProcessRequestResponseCommand(req)
	if err != nil {
		return tables, &SWARMDBError{message: fmt.Sprintf("[swarmdblib:ListTables] ProcessRequestResponseCommand %s", err.Error())}
	}
	return tables, nil
}

func (tbl *SWARMDBTable) DescribeTable() (description []Row, err error) {
	var req RequestOption
	req.RequestType = RT_DESCRIBE_TABLE
	req.Owner = tbl.DBDatabase.DBConnection.Owner
	req.Database = tbl.DBDatabase.Name
	req.Table = tbl.Name
	description, err = tbl.DBDatabase.DBConnection.ProcessRequestResponseCommand(req)
	if err != nil {
		return description, &SWARMDBError{message: fmt.Sprintf("[swarmdblib:DescribeTable] ProcessRequestResponseCommand %s", err.Error())}
	}
	return description, nil

}

//allows to write multiple rows ([]Row) or single row (Row)
func (tbl *SWARMDBTable) Put(row interface{}) error {

	var r RequestOption
	r.RequestType = RT_PUT
	r.Owner = tbl.DBDatabase.DBConnection.Owner
	r.Database = tbl.DBDatabase.Name
	r.Table = tbl.Name
	r.Encrypted = tbl.DBDatabase.Encrypted

	switch row.(type) {
	case Row:
		r.Rows = append(r.Rows, row.(Row))
	case []Row:
		r.Rows = row.([]Row)
	default:
		return &SWARMDBError{message: fmt.Sprintf("[swarmdblib:Put] row must be Row or []Row")}
	}
	_, err := tbl.DBDatabase.DBConnection.ProcessRequestResponseCommand(r)
	return err
}

func (dbc *SWARMDBConnection) ProcessRequestResponseCommand(request RequestOption) (response []Row, err error) {

	message, err := json.Marshal(request)
	if err != nil {
		return response, &SWARMDBError{message: fmt.Sprintf("[swarmdblib:ProcessRequestResponseCommand] Marshal %s", err.Error())}
	}
	str := string(message) + "\n"
	dbc.writer.WriteString(str)
	dbc.writer.Flush()
	connResponse, err := dbc.reader.ReadString('\n')
	//fmt.Printf("connResponse is ... %s\n", connResponse)
	if err != nil {
		//fmt.Printf("err is not nil\n")
		return response, &SWARMDBError{message: fmt.Sprintf("[swarmdblib:ProcessRequestResponseCommand] ReadString %s", err.Error())}
	}
	//fmt.Printf("making swarmdb response\n")
	var sResponse SWARMDBResponse
	if err = json.Unmarshal([]byte(connResponse), &sResponse); err != nil {
		//fmt.Printf("some unmarshal err\n")
		return response, &SWARMDBError{ErrorCode: 400, ErrorMessage: `[swarmdblib:ProcessRequestResponseCommand] Bad JSON Supplied: [` + connResponse + `]`}
	}
	//fmt.Printf("got to the end of Process\n")
	//fmt.Printf("data: %+v\n, err: %+v\n", sResponse.Data, sResponse.Error)
	if sResponse.Error == nil {
		//fmt.Printf("yes! it's nil!!\n")
		return sResponse.Data, nil
	}
	return sResponse.Data, sResponse.Error
}

func (tbl *SWARMDBTable) Get(key string) (rows []Row, err error) {

	var r RequestOption
	r.RequestType = RT_GET
	r.Owner = tbl.DBDatabase.DBConnection.Owner
	r.Database = tbl.DBDatabase.Name
	r.Table = tbl.Name
	r.Encrypted = tbl.DBDatabase.Encrypted
	r.Key = key
	rows, err = tbl.DBDatabase.DBConnection.ProcessRequestResponseCommand(r)
	return rows, err
}

func (tbl *SWARMDBTable) Delete(key string) error {

	var r RequestOption
	r.RequestType = RT_DELETE
	r.Owner = tbl.DBDatabase.DBConnection.Owner
	r.Database = tbl.DBDatabase.Name
	r.Table = tbl.Name
	r.Encrypted = tbl.DBDatabase.Encrypted
	r.Key = key
	_, err := tbl.DBDatabase.DBConnection.ProcessRequestResponseCommand(r)
	return err
}

func (t *SWARMDBTable) Scan(columnName string) (err error) {
	// TODO: Implement this!
	return nil
}

func (tbl *SWARMDBTable) Query(query string) (response []Row, err error) {

	var r RequestOption
	r.RequestType = RT_QUERY
	r.Owner = tbl.DBDatabase.DBConnection.Owner
	r.Database = tbl.DBDatabase.Name
	r.Table = tbl.Name
	r.Encrypted = tbl.DBDatabase.Encrypted
	r.RawQuery = query
	rows, err := tbl.DBDatabase.DBConnection.ProcessRequestResponseCommand(r)
	return rows, err
}

func findName(data []Row, nameToFind string) bool {
	found := false
	for _, row := range data {
		for _, name := range row {
			if name == nameToFind {
				found = true
				break
			}
		}
		if found == true {
			break
		}
	}
	return found
}

func (t *SWARMDBTable) Close() {
	//TODO: implement this -- need to FlushBuffer
}

//TODO: need closes for database and table too? do we even need this close? - more important in non-singleton mode
func (dbc *SWARMDBConnection) Close(tbl *SWARMDBTable) (err error) {
	//TODO: need something like this? to close the 'Open' connection? - something with tbl.Close()
	return nil
}

//TODO: make this work
/*
func NewGoClient() {
	dbc, err := NewSWARMDBConnection()
	if err != nil {
		fmt.Printf("%s\n", err)

	}

	//var ens ENSSimulation
	tableName := "test"

	tbl, err2 := dbc.Open(tableName, dbc.owner, dbc.database)
	if err2 != nil {
	}
	var columns []Column
	columns = make([]Column, 1)
	columns[0].ColumnName = "email"
	columns[0].Primary = 1              // What if this is inconsistent?
	columns[0].IndexType = IT_BPLUSTREE //  What if this is inconsistent?
	columns[0].ColumnType = CT_STRING
	tbl, err3 := dbc.CreateTable(dbc.owner, dbc.database, tableName, columns, 0) //, ens)
	if err3 != nil {
		fmt.Printf("ERR CREATE TABLE %v\n", err3)
	} else {
		// fmt.Printf("SUCCESS CREATE TABLE\n")
	}
	nrows := 5

	for i := 0; i < nrows; i++ {
		//row := NewRow()
		//row.Set("email", fmt.Sprintf("test%03d@wolk.com", i))
		//row.Set("age", fmt.Sprintf("%d", i))
		var row Row
		row = make(map[string]interface{})
		row["email"] = `"test%03d@wolk.com"`
		row["age"] = i
		_, err := tbl.Put(row)
		if err != nil {
			fmt.Printf("ERROR PUT %s %v\n", err, row)
		} else {
			// fmt.Printf("SUCCESS PUT %v %s\n", row, resp)
		}
	}

	for i := 0; i < nrows; i++ {
		key := fmt.Sprintf("test%03d@wolk.com", i)
		row, err := tbl.Get(key)
		if err != nil {
			fmt.Printf("ERROR GET %s key: %v\n", err, row)
		} else {
			// fmt.Printf("SUCCESS GET %v\n", row)
		}
		time.Sleep(100 * time.Millisecond)
	}
}
*/
