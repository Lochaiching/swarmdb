package swarmdb_test

import (
	"testing"
	"fmt"
	swarmdb "github.com/ethereum/go-ethereum/swarmdb"
	)

const (
	TEST_TABLE = "testtable"
)

func TestConn(t *testing.T) {
	_, err := swarmdb.OpenConnection("127.0.0.1", 2000)
	if err != nil {
		t.Fatal(err)
	}

}

func aTestAll(t *testing.T) {
	conn, err := swarmdb.OpenConnection("127.0.0.1", 2000)
	if err != nil {
		t.Fatal(err)
	}

	var columns []swarmdb.Column
	var c swarmdb.Column
	columns = append(columns, c)
	ens, _ := swarmdb.NewENSSimulation("/tmp/ens.db")
	tbl, _ := conn.CreateTable(TEST_TABLE, columns, ens) 

	r := swarmdb.NewRow()
	r.Set("email", "rodney@wolk.com")
	r.Set("age", "38")
	r.Set("gender", "M")
	err = tbl.Put(r) 
	if ( err != nil ) {
	}
	r.Set("email", "minnie@gmail.com")
	r.Set("age", "3")
	r.Set("gender", "F")
	err = tbl.Insert(r) 

	key := "minnie@gmail.com"
	r, err = tbl.Get(key)

	key = "minnie@gmail.com"
	err = tbl.Delete(key)

	key = "minnie@gmail.com"
	r, err = tbl.Get(key)

	tbl.Scan(func(r swarmdb.SWARMDBRow) bool {
		fmt.Printf("%v", r)
		return true
	})

	sql := "select * from contacts"
	tbl.Query(sql, func(r swarmdb.SWARMDBRow) bool {
		fmt.Printf("%v", r)
		return true
	})
}

func bTestPut(t *testing.T) {
	// create request 
	// send to server
}

func bTestInsert(t *testing.T) {
	// create request 
	// send to server
}

func bTestGet(t *testing.T) {
	// create request 
	// send to server
}

func bTestDelete(t *testing.T) {
	// create request 
	// send to server
}

func bTestScan(t *testing.T) {
	// create request 
	// send to server
}

func bTestQuerySelect(t *testing.T) {
	// create request 
	// send to server
}

