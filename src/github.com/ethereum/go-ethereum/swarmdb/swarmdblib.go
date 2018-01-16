// SWARMDB Go client
package swarmdb

import (
	"fmt"
	"github.com/ethereum/go-ethereum/crypto"
	"net"
	"os"
	"strings"
	// "encoding/hex"
	// "encoding/gob"
	"bufio"
	"encoding/json"
	// "github.com/ethereum/go-ethereum/swarmdb/keymanager"
	"time"
)

const (
	CONN_HOST = "127.0.0.1"
	CONN_PORT = "2000"
	CONN_TYPE = "tcp"
)

func NewGoClient() {
	dbc, err := NewSWARMDBConnection()
	if err != nil {
		fmt.Printf("%s\n", err)
		os.Exit(0)
	}
	var ens ENSSimulation
	tableName := "test"

	tbl, err2 := dbc.Open(tableName)
	if err2 != nil {
	}
	var columns []Column
	columns = make([]Column, 1)
	columns[0].ColumnName = "email"
	columns[0].Primary = 1              // What if this is inconsistent?
	columns[0].IndexType = IT_BPLUSTREE //  What if this is inconsistent?
	columns[0].ColumnType = CT_STRING
	tbl, err3 := dbc.CreateTable(tableName, columns, ens)
	if err3 != nil {
		fmt.Printf("ERR CREATE TABLE %v\n", err3)
	} else {
		// fmt.Printf("SUCCESS CREATE TABLE\n")
	}
	nrows := 5
/*
	for i := 0; i < nrows; i++ {
		row := NewRow()
		row.Set("email", fmt.Sprintf("test%03d@wolk.com", i))
		row.Set("age", fmt.Sprintf("%d", i))
		_, err := tbl.Put(row)
		if err != nil {
			fmt.Printf("ERROR PUT %s %v\n", err, row)
		} else {
			// fmt.Printf("SUCCESS PUT %v %s\n", row, resp)
		}
	}
*/
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

func NewSWARMDBConnection() (dbc SWARMDBConnection, err error) {
	// open a TCP connection to ip port
	// dbc = new(SWARMDBConnection)
	config, configerr := LoadSWARMDBConfig(SWARMDBCONF_FILE)
	if configerr != nil {
		return dbc, err
	}
	km, kmerr := NewKeyManager(&config)
	if err != nil {
		return dbc, kmerr
	} else {
		dbc.keymanager = km
	}

	conn, err := net.Dial(CONN_TYPE, CONN_HOST+":"+CONN_PORT)
	if err != nil {
		return dbc, err
	} else {
		dbc.connection = conn
	}
	fmt.Printf("Opened connection: reading string...")
	reader := bufio.NewReader(conn)
	writer := bufio.NewWriter(conn)

	dbc.reader = reader
	dbc.writer = writer
	return dbc, nil
}

func (dbc *SWARMDBConnection) Open(tableName string) (tbl *SWARMDBTable, err error) {
	// read a random length string from the server
	challenge, _ := dbc.reader.ReadString('\n')
	challenge = strings.Trim(challenge, "\n")
	// challenge_bytes, _ := hex.DecodeString(challenge)

	// sign the message Web3 style
	msg := fmt.Sprintf("\x19Ethereum Signed Message:\n%d%s", len(challenge), challenge)
	challenge_bytes := crypto.Keccak256([]byte(msg))

	sig, err := dbc.keymanager.SignMessage(challenge_bytes)
	if err != nil {
		fmt.Printf("Err %s\n", err)
	} else {
		// fmt.Printf("Challenge: [%x] Sig:[%x]\n", challenge_bytes, sig)
	}
	// response = "6b1c7b37285181ef74fb1946968c675c09f7967a3e69888ee37c42df14a043ac2413d19f96760143ee8e8d58e6b0bda4911f642912d2b81e1f2834814fcfdad700"
	response := fmt.Sprintf("%x", sig)
	fmt.Printf("challenge:[%v] response:[%v]\n", challenge, response)
	dbc.writer.WriteString(response + "\n")
	dbc.writer.Flush()

	tbl = new(SWARMDBTable)
	tbl.tableName = tableName
	tbl.dbc = dbc
	return tbl, nil
}

func (dbc *SWARMDBConnection) CreateTable(tableName string, columns []Column, ens ENSSimulation) (tbl *SWARMDBTable, err error) {
	// create request
	var r RequestOption
	r.RequestType = "CreateTable"
	r.TableOwner = dbc.ownerID
	r.Table = tableName
	r.Columns = columns

	_, err = dbc.ProcessRequestResponseCommand(r)
	if err != nil {
		return tbl, err
	} else {
		// send to server
		tbl = new(SWARMDBTable)
		tbl.tableName = tableName
		tbl.dbc = dbc
		return tbl, nil
	}
}

func (t *SWARMDBTable) Put(row *Row) (response string, err error) {
	// create request
	var r RequestOption
	var reqOptRow Row
	r.RequestType = "Put"
	r.TableOwner = t.dbc.ownerID
	r.Table = t.tableName
	reqOptRow.Cells = row.Cells
	r.Rows = append(r.Rows, reqOptRow)

	// send to server
	return t.dbc.ProcessRequestResponseCommand(r)
}

func (t *SWARMDBTable) Insert(row *Row) (response string, err error) {
	// create request
	var r RequestOption
	var reqOptRow Row
	r.RequestType = "Insert"
	r.TableOwner = t.dbc.ownerID
	r.Table = t.tableName
	reqOptRow.Cells = row.Cells
	r.Rows = append(r.Rows, reqOptRow)
	// send to server
	return t.dbc.ProcessRequestResponseCommand(r)
}

func (dbc *SWARMDBConnection) ProcessRequestResponseRow(r RequestOption) (row *Row, err error) {
	resp, err := dbc.ProcessRequestResponseCommand(r)
	if err != nil {
		return row, err
	} else {
		if len(resp) > 0 {
			// TODO: turn row_string into row HERE
		}
		return row, nil
	}
}

func (dbc *SWARMDBConnection) ProcessRequestResponseCommand(r RequestOption) (response string, err error) {
	message, err := json.Marshal(r)
	if err != nil {
		return response, err
	} else {
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
	}
}

func (t *SWARMDBTable) Get(key string) (row *Row, err error) {
	// create request
	var r RequestOption
	r.RequestType = "Get"
	r.TableOwner = t.dbc.ownerID
	r.Table = t.tableName
	r.Key = key
	return t.dbc.ProcessRequestResponseRow(r)
}

func (t *SWARMDBTable) Delete(key string) (response string, err error) {
	// send to server
	var r RequestOption
	r.RequestType = "Delete"
	r.TableOwner = t.dbc.ownerID
	r.Table = t.tableName
	r.Key = key
	return t.dbc.ProcessRequestResponseCommand(r)
}

func (t *SWARMDBTable) Scan(rowfunc func(r Row) bool) (err error) {
	// create request
	// send to server
	return nil
}

func (t *SWARMDBTable) Query(sql string, f func(r Row) bool) (err error) {
	// create request
	var r RequestOption
	r.RequestType = "Query"
	r.TableOwner = t.dbc.ownerID
	r.Table = t.tableName
	r.RawQuery = sql
	return nil
}

func (t *SWARMDBTable) Close() {
	// create request
	// send to server
}
/*
func NewRow() (r *Row) {
	r = new(Row)
	r.Cells = make(map[string]interface{})
	return r
}

func (r *Row) Set(columnName string, val string) (err error) {
	r.Cells[columnName] = val
	return nil
}
*/
