// SWARMDB Go client
package swarmdb

import (
	"bufio"
	"encoding/json"
	"fmt"
	"github.com/ethereum/go-ethereum/crypto"
	"net"
	"os"
	"strings"
	// "github.com/ethereum/go-ethereum/swarmdb/keymanager"
	"time"
)

//TODO: flags for host/port info
const (
	CONN_HOST = "127.0.0.1"
	CONN_PORT = "2000"
	CONN_TYPE = "tcp"
)

var TEST_NOCONNECTION = false

func NewGoClient() {
	dbc, err := NewSWARMDBConnection()
	if err != nil {
		fmt.Printf("%s\n", err)
		os.Exit(0)
	}

	//var ens ENSSimulation
	tableName := "test"

	tbl, err2 := dbc.Open(tableName, 1, 0)
	if err2 != nil {
	}
	var columns []Column
	columns = make([]Column, 1)
	columns[0].ColumnName = "email"
	columns[0].Primary = 1              // What if this is inconsistent?
	columns[0].IndexType = IT_BPLUSTREE //  What if this is inconsistent?
	columns[0].ColumnType = CT_STRING
	tbl, err3 := dbc.CreateTable(dbc.ownerID, 1, 0, 0.01, tableName, columns) //, ens)
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
		_, err := tbl.Put(0.01, row)
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
		return dbc, err
	}
	dbc.keymanager, err = NewKeyManager(&config)
	if err != nil {
		return dbc, err
	}
	conn, err := net.Dial(CONN_TYPE, CONN_HOST+":"+CONN_PORT)
	if err != nil {
		return dbc, err
	}
	dbc.connection = conn
	fmt.Printf("Opened connection: reading string...")
	reader := bufio.NewReader(conn)
	writer := bufio.NewWriter(conn)

	dbc.reader = reader
	dbc.writer = writer
	return dbc, nil
}

//TODO: need something like this? to close the 'Open' connection?
func (dbc *SWARMDBConnection) Close(tbl *SWARMDBTable) (err error) {
	//something with tbl.Close()
	return nil
}

func (dbc *SWARMDBConnection) Open(tableName string, encrypted int, replication int) (tbl *SWARMDBTable, err error) {

	// read a random length string from the server
	challenge, err := dbc.reader.ReadString('\n')
	if err != nil {
		return tbl, err
	}
	challenge = strings.Trim(challenge, "\n")
	// challenge_bytes, _ := hex.DecodeString(challenge)

	// sign the message Web3 style
	msg := fmt.Sprintf("\x19Ethereum Signed Message:\n%d%s", len(challenge), challenge)
	challenge_bytes := crypto.Keccak256([]byte(msg))

	sig, err := dbc.keymanager.SignMessage(challenge_bytes)
	if err != nil {
		return tbl, err
	} else {
		fmt.Printf("Challenge: [%x] Sig:[%x]\n", challenge_bytes, sig)
	}
	// response = "6b1c7b37285181ef74fb1946968c675c09f7967a3e69888ee37c42df14a043ac2413d19f96760143ee8e8d58e6b0bda4911f642912d2b81e1f2834814fcfdad700"
	response := fmt.Sprintf("%x", sig)
	fmt.Printf("challenge:[%v] response:[%v]\n", challenge, response)
	dbc.writer.WriteString(response + "\n")
	dbc.writer.Flush()

	tbl = new(SWARMDBTable)
	tbl.tableName = tableName
	tbl.dbc = dbc
	tbl.encrypted = encrypted
	tbl.replication = replication
	return tbl, nil
}

//expose the ownerID of the swarmdb connection
func (dbc *SWARMDBConnection) GetOwnerID() string {
	return dbc.ownerID
}

func (dbc *SWARMDBConnection) CreateTable(tableOwner string, encrypted int, replication int, bid float64, tableName string, columns []Column) (tbl *SWARMDBTable, err error) {
	//TODO: ens = ENSSimulation to verify if table exists already?
	//TODO: GetTable lookup to verify if table exists already

	// create request
	var req RequestOption
	req.RequestType = "CreateTable"
	req.TableOwner = tableOwner //dbc.ownerID is the owner of the session, not always the table
	req.Table = tableName
	req.Encrypted = encrypted
	req.Columns = columns
	_, err = dbc.ProcessRequestResponseCommand(req)
	if err != nil {
		return tbl, err
	}

	// send to server
	tbl = new(SWARMDBTable)
	tbl.tableName = tableName
	tbl.dbc = dbc
	tbl.encrypted = encrypted
	tbl.replication = replication
	return tbl, nil

}

//allows to write multiple rows ([]Row) or single row (Row)
//if writing multiple rows, bid applies for mass write (is this ok?)
func (t *SWARMDBTable) Put(bid float64, row interface{}) (response string, err error) {

	var r RequestOption
	r.RequestType = "Put"
	r.TableOwner = t.dbc.ownerID
	r.Table = t.tableName
	r.Encrypted = t.encrypted

	switch row.(type) {
	case Row:
		r.Rows = append(r.Rows, row.(Row))
	case []Row:
		r.Rows = row.([]Row)
	default:
		return "", fmt.Errorf("row must be Row or []Row")
	}

	return t.dbc.ProcessRequestResponseCommand(r)
}

/*
//TODO: Insert. Also not sure how Insert differs from Put. This is b/c Insert is not fleshed out in swarmdb.go yet.
func (t *SWARMDBTable) Insert(bid float64, rows []Row) (response string, err error) {

	var r RequestOption
	var reqOptRow Row
	r.RequestType = "Insert"
	r.TableOwner = t.dbc.ownerID
	r.Table = t.tableName
	r.Encrypted = t.encrypted
	switch row.(type) {
	case Row:
		r.Rows = append(r.Rows, row)
	case []Row:
		r.Rows = row
	default:
		return "", err.Error("row must be Row or []Row")
	}

	return t.dbc.ProcessRequestResponseCommand(r)
}
*/

func (dbc *SWARMDBConnection) ProcessRequestResponseRow(request RequestOption) (row *Row, err error) {
	response, err := dbc.ProcessRequestResponseCommand(request)
	if err != nil {
		return row, err
	}
	if len(response) > 0 {
		// TODO: turn row_string into row HERE
	}
	return row, nil

}

//TODO: make sure this returns the right string, in correct formatting
func (dbc *SWARMDBConnection) ProcessRequestResponseCommand(request RequestOption) (response string, err error) {

	message, err := json.Marshal(request)
	if err != nil {
		return response, err
	}
	str := string(message) + "\n"
	dbc.writer.WriteString(str)
	dbc.writer.Flush()
	fmt.Printf("Req: %s", str)
	response, err2 := dbc.reader.ReadString('\n')
	if err2 != nil {
		fmt.Printf("err: \n")
		return response, err
	}
	fmt.Printf("Res: %s\n", response)
	return response, nil

	fmt.Printf("\nprocess request response cmd: %+v\n", request)
	return "", nil
}

//TODO: finish
func (t *SWARMDBTable) Get(key string) (row *Row, err error) {
	// create request
	var r RequestOption
	r.RequestType = "Get"
	r.TableOwner = t.dbc.ownerID
	r.Table = t.tableName
	r.Key = key
	return t.dbc.ProcessRequestResponseRow(r)
}

//TODO: finish
func (t *SWARMDBTable) Delete(key string) (response string, err error) {
	// send to server
	var r RequestOption
	r.RequestType = "Delete"
	r.TableOwner = t.dbc.ownerID
	r.Table = t.tableName
	r.Key = key
	return t.dbc.ProcessRequestResponseCommand(r)
}

//TODO:
func (t *SWARMDBTable) Scan(rowfunc func(r Row) bool) (err error) {
	// create request
	// send to server
	return nil
}

//TODO: finish
func (t *SWARMDBTable) Query(sql string, f func(r Row) bool) (err error) {
	// create request
	var r RequestOption
	r.RequestType = "Query"
	r.TableOwner = t.dbc.ownerID
	r.Table = t.tableName
	r.RawQuery = sql
	return nil
}

//TODO: what goes here?
func (t *SWARMDBTable) Close() {
	// create request
	// send to server
}
