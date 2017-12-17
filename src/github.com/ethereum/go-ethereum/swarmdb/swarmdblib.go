// SWARMDB Go client
package swarmdb

import (
	"net"
	"fmt"
	// "encoding/gob"
	// "encoding/json"
	"time"
	"bufio"
	// "github.com/ethereum/go-ethereum/swarmdb"
	// "github.com/ethereum/go-ethereum/swarmdb/keymanager"
)


func OpenConnection(ip string, port int) (conn *SWARMDBConnection, err error) {
	// open a TCP connection to ip port
	conn = new(SWARMDBConnection)
	conn.ownerID = "0xf6b55acbbc49f4524aa48d19281a9a77c54de10f"
	// connect to this socket
	
	connstr := fmt.Sprintf("%s:%d", ip, port)
	c, connerr := net.DialTimeout("tcp", connstr, 500*time.Millisecond)
	if connerr != nil {
		fmt.Print("Error: ", connerr)
		return conn, connerr
	}
	conn.connection = c
	conn.nrw = bufio.NewReadWriter(bufio.NewReader(c), bufio.NewWriter(c))
	// enc := gob.NewEncoder(conn.nrw)
	message, _ := conn.nrw.ReadString('\n')
	fmt.Print("Challenge:[" + message + "]")
	// generate challenge message 
	return conn, err 
}

func (c *SWARMDBConnection) Open(tableName string) (tbl *SWARMDBTable, err error) {
	// create request 
	var r RequestOption
	r.RequestType = "OpenTable"
	r.Owner = c.ownerID
	r.Table = tableName

	// send to server
	tbl = new(SWARMDBTable)
	tbl.tableName = tableName
	tbl.conn = c
	return tbl, nil
}

func (c *SWARMDBConnection) CreateTable(tableName string, columns []Column, ens ENSSimulation) (tbl *SWARMDBTable, err error) {
	// create request 
	var r RequestOption
	r.RequestType = "CreateTable"
	r.Owner = c.ownerID
	r.Table = tableName
	r.Columns = columns

	// send to server
	tbl = new(SWARMDBTable)
	tbl.tableName = tableName
	tbl.conn = c
	return tbl, nil
}

func (t *SWARMDBTable) Put(row SWARMDBRow) (err error) {
	// create request 
	var r RequestOption
	r.RequestType = "Put"
	r.Owner = t.conn.ownerID
	r.Table = t.tableName
	r.Row = row // json.Marshal(r)
	// send to server
	return nil
}

func (t *SWARMDBTable) Insert(row SWARMDBRow) (err error) {
	// create request 
	var r RequestOption
	r.RequestType = "Insert"
	r.Owner = t.conn.ownerID
	r.Table = t.tableName
	r.Key = "key"
	// send to server
	return nil
}


func (t *SWARMDBTable) Get(key string) (row SWARMDBRow, err error) {
	// create request 
	var r RequestOption
	r.RequestType = "Get"
	r.Owner = t.conn.ownerID
	r.Table = t.tableName
	r.Key = key
	
	return row, nil
}


func (t *SWARMDBTable) Delete(key string) (err error) {
	// send to server
	var r RequestOption
	r.RequestType = "Delete"
	r.Owner = t.conn.ownerID
	r.Table = t.tableName
	r.Key = key
	return nil
}

func (t *SWARMDBTable) Scan(rowfunc func(r SWARMDBRow) bool) (err error) {
	// create request 
	// send to server
	return nil
}

func (t *SWARMDBTable) Query(sql string, f func (r SWARMDBRow) bool) (err error) {
	// create request 
	var r RequestOption
	r.RequestType = "Query"
	r.Owner = t.conn.ownerID
	r.Table = t.tableName
	r.RawQuery = sql
	return nil
}

func (t *SWARMDBTable) Close() {
	// create request 
	// send to server
}

func NewRow() (r SWARMDBRow) {
	// r = new(SWARMDBRow)
	r.cells = make(map[string]string)
	return r
}

func (r *SWARMDBRow) Set(columnName string, val string) (err error) {
	r.cells[columnName] = val
	return nil
}


