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
	connectionOwner string         //owner of the connection; right now the same as "owner" in singleton mode
	Owner           string         //owner of the databases/tables
	Databases       map[string]int //string is database name
}

type SWARMDBDatabase struct {
	DBConnection *SWARMDBConnection
	Tables       map[string]int
	Name         string
	Encrypted    int //means all transactions on the tables in this db will be encrypted or not
}

type SWARMDBTable struct {
	DBDatabase *SWARMDBDatabase
	Name       string
}

//TODO: should this go client live somewhere else, not with the server swarmdb package?

//TODO: flags for host/port info
var CONN_HOST = "127.0.0.1" //default, but reads from config. TODO: default this to whatever config defaults to
var CONN_PORT = int(2001)   //default, but reads from config. TODO: default this to whatever config defaults to
var CONN_TYPE = "tcp"

// opens a TCP connection to ip port
// usage: dbc, err := NewSWARMDBConnection()
func NewSWARMDBConnection(ip string, port int) (dbc SWARMDBConnection, err error) {

	config, err := LoadSWARMDBConfig(SWARMDBCONF_FILE)
	if err != nil {
		return dbc, &SWARMDBError{message: fmt.Sprintf("[swarmdblib:NewSWARMDBConnection] LoadSWARMDBConfig %s", err.Error())}
	}
	dbc.keymanager, err = NewKeyManager(&config)
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
	//fmt.Printf("connection host %v and port %v\n", CONN_HOST, CONN_PORT)
	conn, err := net.Dial(CONN_TYPE, CONN_HOST+":"+strconv.Itoa(CONN_PORT))
	if err != nil {
		return dbc, &SWARMDBError{message: fmt.Sprintf("[swarmdblib:NewSWARMDBConnection] Dial %s", err.Error())}
	}
	dbc.connection = conn

	reader := bufio.NewReader(conn)
	writer := bufio.NewWriter(conn)

	dbc.reader = reader
	dbc.writer = writer
	dbc.connectionOwner = config.Address //TODO: this is ok for singleton node or default owner
	dbc.Owner = config.Address
	dbc.Databases = make(map[string]int)

	//CHALLENGE
	challenge, err := dbc.reader.ReadString('\n') // read a random length string from the server
	if err != nil {
		return dbc, &SWARMDBError{message: fmt.Sprintf("[swarmdblib:Open] ReadString %s", err.Error())}
	}
	challenge = strings.Trim(challenge, "\n")
	// challenge_bytes, _ := hex.DecodeString(challenge)

	// sign the message Web3 style
	challenge_bytes := SignHash([]byte(challenge))

	sig, err := dbc.keymanager.SignMessage(challenge_bytes)
	if err != nil {
		return dbc, err
	}

	// response should be hex string like this: "6b1c7b37285181ef74fb1946968c675c09f7967a3e69888ee37c42df14a043ac2413d19f96760143ee8e8d58e6b0bda4911f642912d2b81e1f2834814fcfdad700"
	response := fmt.Sprintf("%x", sig)
	//fmt.Printf("challenge:[%v] response:[%v]\n", challenge, response)
	//fmt.Printf("Challenge: [%x] Sig:[%x]\n", challenge_bytes, sig)
	dbc.writer.WriteString(response + "\n")
	dbc.writer.Flush()

	return dbc, nil
}

func (dbc *SWARMDBConnection) OpenDatabase(name string, encrypted int) (db *SWARMDBDatabase, err error) {
	//challenge?
	db = new(SWARMDBDatabase)
	db.DBConnection = dbc
	db.Name = name
	db.Encrypted = encrypted
	if len(db.Tables) == 0 {
		db.Tables = make(map[string]int)
	}

	if _, ok := dbc.Databases[db.Name]; ok {
		//database already opened err, is this still ok?
		return db, &SWARMDBError{message: fmt.Sprintf("[swarmdblib:OpenDatabase] db already open")}
	}
	dbc.Databases[db.Name] = 1

	return db, nil
}

func (db *SWARMDBDatabase) OpenTable(name string) (tbl *SWARMDBTable, err error) {
	//challenge?
	tbl = new(SWARMDBTable)
	tbl.Name = name
	tbl.DBDatabase = db
	if _, ok := db.Tables[tbl.Name]; ok {
		//table already opened err, is this still ok?
		return tbl, &SWARMDBError{message: fmt.Sprintf("[swarmdblib:OpenTable] Table already open")}
	}
	db.Tables[tbl.Name] = 1
	return tbl, nil
}

//TODO: fix this when Rodney gives wolkdb.go a standard response back
func processConnectionResponse(in string) (out string, err error) {

	if !strings.Contains(strings.ToLower(in), "errorcode") { //Getting around the fact that a valid response is not always a SWARMDBResponse right now
		//fmt.Printf("checking for the word errorcode\n")
		return in, nil
	}

	//hack b/c SWARMDBResponse.Data is a []Row.
	type tempResponse struct {
		ErrorCode        int    `json:"errorcode,omitempty"`
		ErrorMessage     string `json:"errormessage,omitempty"`
		Data             string `json:"data,omitempty"`
		AffectedRowCount int    `json:"affectedrowcount,omitempty"`
		MatchedRowCount  int    `json:"matchedrowcount,omitempty"`
	}

	var response tempResponse
	err = json.Unmarshal([]byte(in), &response)
	if err != nil {
		return "", &SWARMDBError{ErrorCode: 400, ErrorMessage: `Bad JSON Supplied: [` + in + `]`, message: "[processConnectionResponse]"}
	}
	//fmt.Printf("so now response is: %+v\n", response)
	var swdbErr SWARMDBError
	swdbErr.ErrorMessage = response.ErrorMessage
	swdbErr.ErrorCode = response.ErrorCode
	//fmt.Printf("made swarmdberror: %v\n", swdbErr)
	out = response.Data

	return out, &swdbErr
}

func (dbc *SWARMDBConnection) CreateDatabase(name string, encrypted int) (db *SWARMDBDatabase, err error) {

	//open database
	db, err = dbc.OpenDatabase(name, encrypted)
	if err != nil {
		return db, err
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

	return db, nil

}

//note: can CreateTable without Opening first.
func (db *SWARMDBDatabase) CreateTable(name string, columns []Column) (tbl *SWARMDBTable, err error) {

	//open the table
	tbl, err = db.OpenTable(name)
	if err != nil {
		return tbl, err
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

	return tbl, nil
}

//allows to write multiple rows ([]Row) or single row (Row)
func (tbl *SWARMDBTable) Put(row interface{}) (response string, err error) {

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
		return "", &SWARMDBError{message: fmt.Sprintf("[swarmdblib:Put] row must be Row or []Row")}
	}
	return tbl.DBDatabase.DBConnection.ProcessRequestResponseCommand(r)
}

/*
func (dbc *SWARMDBConnection) ProcessRequestResponseRow(request RequestOption) (row *Row, err error) {
	response, err := dbc.ProcessRequestResponseCommand(request)
	if err != nil {
		return row, &SWARMDBError{message: fmt.Sprintf("[swarmdblib:ProcessRequestResponseRow] ProcessRequestResponseCommand %s", err.Error())}
	}
	if len(response) > 0 {
		// TODO: turn row_string into row HERE
	}
	return row, nil

}
*/

// TODO: make sure this returns the right string, in correct formatting
func (dbc *SWARMDBConnection) ProcessRequestResponseCommand(request RequestOption) (response string, err error) {

	//fmt.Printf("process request response cmd: %+v\n", request)
	message, err := json.Marshal(request)
	if err != nil {
		return response, &SWARMDBError{message: fmt.Sprintf("[swarmdblib:ProcessRequestResponseCommand] Marshal %s", err.Error())}
	}
	str := string(message) + "\n"
	//fmt.Printf("Req: %v", str)
	dbc.writer.WriteString(str)
	dbc.writer.Flush()
	response, err = dbc.reader.ReadString('\n')
	if err != nil {
		//fmt.Printf("Readstring err: %v\n", err)
		return response, &SWARMDBError{message: fmt.Sprintf("[swarmdblib:ProcessRequestResponseCommand] ReadString %s", err.Error())}
	}
	//fmt.Printf("original response from readstring: %s\n", response)
	responseData, swdbErr := processConnectionResponse(response)
	return responseData, swdbErr
}

//func (t *SWARMDBTable) Get(key string) (row *Row, err error) {
func (tbl *SWARMDBTable) Get(key string) (response string, err error) {

	var r RequestOption
	r.RequestType = RT_GET
	r.Owner = tbl.DBDatabase.DBConnection.Owner
	r.Database = tbl.DBDatabase.Name
	r.Table = tbl.Name
	r.Encrypted = tbl.DBDatabase.Encrypted
	r.Key = key
	return tbl.DBDatabase.DBConnection.ProcessRequestResponseCommand(r)
}

func (tbl *SWARMDBTable) Delete(key string) (response string, err error) {

	var r RequestOption
	r.RequestType = RT_DELETE
	r.Owner = tbl.DBDatabase.DBConnection.Owner
	r.Database = tbl.DBDatabase.Name
	r.Table = tbl.Name
	r.Encrypted = tbl.DBDatabase.Encrypted
	r.Key = key
	return tbl.DBDatabase.DBConnection.ProcessRequestResponseCommand(r)
}

func (t *SWARMDBTable) Scan(rowfunc func(r Row) bool) (err error) {
	// TODO: Implement this!
	return nil
}

func (tbl *SWARMDBTable) Query(query string) (string, error) {

	var r RequestOption
	r.RequestType = RT_QUERY
	r.Owner = tbl.DBDatabase.DBConnection.Owner
	r.Database = tbl.DBDatabase.Name
	r.Table = tbl.Name
	r.Encrypted = tbl.DBDatabase.Encrypted
	r.RawQuery = query
	return tbl.DBDatabase.DBConnection.ProcessRequestResponseCommand(r)
}

func (t *SWARMDBTable) Close() {
	//TODO: implement this -- need to FlushBuffer
}

//TODO: need closes for database and table too? do we even need this close? - more important in non-singleton mode
func (dbc *SWARMDBConnection) Close(tbl *SWARMDBTable) (err error) {
	//TODO: need something like this? to close the 'Open' connection? - something with tbl.Close()
	return nil
}

//Exposure functions:

// expose the owner of the swarmdb connection
/*
func (dbc *SWARMDBConnection) GetOwner() string {
	return dbc.owner
}

func (dbc *SWARMDBConnection) SetOwner(owner string) {
	dbc.owner = owner
}

// expose the database of the swarmdb connection
func (db *SWARMDBDatabase) GetDatabaseName() string {
	return db.name
}

func (db *SWARMDBDatabase) GetTables() []string {
	var names []string
	for _, table := range db.tables {
		names = append(names, table)
	}
	return names
 }
*/

//TODO: make this work
/*
func NewGoClient() {
	dbc, err := NewSWARMDBConnection()
	if err != nil {
		fmt.Printf("%s\n", err)
		os.Exit(0)
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
		row.Cells = make(map[string]interface{})
		row.Cells["email"] = `"test%03d@wolk.com"`
		row.Cells["age"] = i
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
