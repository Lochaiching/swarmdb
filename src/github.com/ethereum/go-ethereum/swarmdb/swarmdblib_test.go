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

package swarmdb_test

import (
	"fmt"
	swarmdb "github.com/ethereum/go-ethereum/swarmdb"
	"testing"
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
	if err != nil {
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

	tbl.Scan(func(r swarmdb.Row) bool {
		fmt.Printf("%v", r)
		return true
	})

	sql := "select * from contacts"
	tbl.Query(sql, func(r swarmdb.Row) bool {
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
