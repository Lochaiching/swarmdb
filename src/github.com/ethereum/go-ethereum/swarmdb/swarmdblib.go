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
	"os"
	"strconv"
	"strings"
	// "github.com/ethereum/go-ethereum/swarmdb/keymanager"
	"time"
)

//TODO: should this go client live somewhere else, not with the server swarmdb package?

//TODO: flags for host/port info

var CONN_HOST = "127.0.0.1" //default, but reads from config
var CONN_PORT = int(2001)   //default, but reads from config
var CONN_TYPE = "tcp"

var TEST_NOCONNECTION = false

func NewGoClient() {
	dbc, err := NewSWARMDBConnection()
	if err != nil {
		fmt.Printf("%s\n", err)
		os.Exit(0)
	}

	//var ens ENSSimulation
	tableName := "test"

	tbl, err2 := dbc.Open(tableName, dbc.ownerID, 1)
	if err2 != nil {
	}
	var columns []Column
	columns = make([]Column, 1)
	columns[0].ColumnName = "email"
	columns[0].Primary = 1              // What if this is inconsistent?
	columns[0].IndexType = IT_BPLUSTREE //  What if this is inconsistent?
	columns[0].ColumnType = CT_STRING
	tbl, err3 := dbc.CreateTable(dbc.ownerID, 1, tableName, columns) //, ens)
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

// opens a TCP connection to ip port
// usage: dbc, err := NewSWARMDBConnection()
func NewSWARMDBConnection() (dbc SWARMDBConnection, err error) {

	config, err := LoadSWARMDBConfig(SWARMDBCONF_FILE)
	if err != nil {
		return dbc, &SWARMDBError{message: fmt.Sprintf("[swarmdblib:NewSWARMDBConnection] LoadSWARMDBConfig %s", err.Error())}
	}
	dbc.keymanager, err = NewKeyManager(&config)
	if err != nil {
		return dbc, &SWARMDBError{message: fmt.Sprintf("[swarmdblib:NewSWARMDBConnection] NewKeyManager %s", err.Error())}
	}
	if len(config.ListenAddrTCP) > 0 {
		CONN_HOST = config.ListenAddrTCP
	}
	if config.PortTCP > 0 {
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
	return dbc, nil
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

func (dbc *SWARMDBConnection) Close(tbl *SWARMDBTable) (err error) {
	//TODO: need something like this? to close the 'Open' connection? - something with tbl.Close()
	return nil
}

func (dbc *SWARMDBConnection) Open(tableName string, tableOwner string, encrypted int) (tbl *SWARMDBTable, err error) {

	// read a random length string from the server
	challenge, err := dbc.reader.ReadString('\n')
	if err != nil {
		return tbl, &SWARMDBError{message: fmt.Sprintf("[swarmdblib:Open] ReadString %s", err.Error())}
	}
	challenge = strings.Trim(challenge, "\n")
	// challenge_bytes, _ := hex.DecodeString(challenge)

	// sign the message Web3 style
	challenge_bytes := SignHash([]byte(challenge))

	sig, err := dbc.keymanager.SignMessage(challenge_bytes)
	if err != nil {
		return tbl, err
	}

	// response should be hex string like this: "6b1c7b37285181ef74fb1946968c675c09f7967a3e69888ee37c42df14a043ac2413d19f96760143ee8e8d58e6b0bda4911f642912d2b81e1f2834814fcfdad700"
	response := fmt.Sprintf("%x", sig)
	//fmt.Printf("challenge:[%v] response:[%v]\n", challenge, response)
	//fmt.Printf("Challenge: [%x] Sig:[%x]\n", challenge_bytes, sig)
	dbc.writer.WriteString(response + "\n")
	dbc.writer.Flush()

	tbl = new(SWARMDBTable)
	tbl.tableName = tableName
	tbl.dbc = dbc
	tbl.encrypted = encrypted
	tbl.tableOwner = tableOwner
	return tbl, nil
}

// expose the ownerID of the swarmdb connection
func (dbc *SWARMDBConnection) GetOwnerID() string {
	return dbc.ownerID
}

func (dbc *SWARMDBConnection) CreateTable(tableOwner string, encrypted int, tableName string, columns []Column) (tbl *SWARMDBTable, err error) {
	// TODO: ens = ENSSimulation / GetTable lookup to verify if table exists already

	// create request
	var req RequestOption
	req.RequestType = RT_CREATE_TABLE
	req.TableOwner = tableOwner //dbc.ownerID is the owner of the session, not always the table
	req.Table = tableName
	req.Encrypted = encrypted
	req.Columns = columns
	_, err = dbc.ProcessRequestResponseCommand(req)
	if err != nil {
		return tbl, &SWARMDBError{message: fmt.Sprintf("[swarmdblib:CreateTable] ProcessRequestResponseCommand %s", err.Error())}
	}

	// send to server
	tbl = new(SWARMDBTable)
	tbl.tableName = tableName
	tbl.dbc = dbc
	tbl.encrypted = encrypted
	tbl.tableOwner = tableOwner
	//tbl.replication = replication
	return tbl, nil

}

//allows to write multiple rows ([]Row) or single row (Row)
func (t *SWARMDBTable) Put(row interface{}) (response string, err error) {

	var r RequestOption
	r.RequestType = RT_PUT
	r.TableOwner = t.tableOwner
	r.Table = t.tableName
	r.Encrypted = t.encrypted

	switch row.(type) {
	case Row:
		r.Rows = append(r.Rows, row.(Row))
	case []Row:
		r.Rows = row.([]Row)
	default:
		return "", &SWARMDBError{message: fmt.Sprintf("[swarmdblib:Put] row must be Row or []Row")}
	}
	return t.dbc.ProcessRequestResponseCommand(r)
}

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
func (t *SWARMDBTable) Get(key string) (response string, err error) {

	var r RequestOption
	r.RequestType = RT_GET
	r.TableOwner = t.tableOwner
	r.Table = t.tableName
	r.Encrypted = t.encrypted
	r.Key = key
	return t.dbc.ProcessRequestResponseCommand(r)
}

func (t *SWARMDBTable) Delete(key string) (response string, err error) {

	var r RequestOption
	r.RequestType = RT_DELETE
	r.TableOwner = t.tableOwner
	r.Table = t.tableName
	r.Encrypted = t.encrypted
	r.Key = key
	return t.dbc.ProcessRequestResponseCommand(r)
}

func (t *SWARMDBTable) Scan(rowfunc func(r Row) bool) (err error) {
	// TODO: Implement this!
	// create request
	// send to server
	return nil
}

//func (t *SWARMDBTable) Query(sql string, f func(r Row) bool) (err error) {
func (t *SWARMDBTable) Query(query string) (string, error) {
	var r RequestOption
	r.RequestType = RT_QUERY
	r.TableOwner = t.tableOwner
	r.Table = t.tableName
	r.Encrypted = t.encrypted
	r.RawQuery = query
	return t.dbc.ProcessRequestResponseCommand(r)
}

func (t *SWARMDBTable) Close() {
	//TODO: implement this -- need to FlushBuffer
	// create request
	// send to server
}
