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
	"encoding/json"
	"fmt"
	"os"
	"strings"
	sdb "swarmdb"
	"testing"
	"time"
)

const (
	TEST_ENS_DIR = "/tmp"
)

type testTableConfig struct {
	tableName         string
	primaryColumnName string
	indexType         sdb.IndexType
	columnType        sdb.ColumnType
	sampleValue1      interface{}
	sampleValue2      interface{}
	sampleValue3      interface{}
	sampleValue4      interface{}
	sampleValue1str   string
	sampleValue2str   string
	sampleValue3str   string
	sampleValue4str   string
}

var config *sdb.SWARMDBConfig
var swarmdb *sdb.SwarmDB
var u *sdb.SWARMDBUser

func TestMain(m *testing.M) {
	config, _ = sdb.LoadSWARMDBConfig(sdb.SWARMDBCONF_FILE)
	var err error
	swarmdb, err = sdb.NewSwarmDB(config)
	if err != nil {
		fmt.Printf("could not create SWARMDB %s", err)
		os.Exit(0)
	}
	sdb.NewKeyManager(config)
	u = config.GetSWARMDBUser()

	code := m.Run()
	// do somethng in shutdown
	os.Exit(code)
}

func make_name(prefix string) (nm string) {
	return fmt.Sprintf("%s%d", prefix, int32(time.Now().Unix()))
}

func TestCoreTables(t *testing.T) {

	owner := make_name("owner.eth")
	owner2 := make_name("altowner.eth")
	database := make_name("db")
	database2 := make_name("altdb")
	encrypted := int(1)
	encrypted2 := int(1)

	// create database
	var tReq *sdb.RequestOption
	tReq = new(sdb.RequestOption)
	tReq.RequestType = sdb.RT_CREATE_DATABASE
	tReq.Owner = owner
	tReq.Database = database
	tReq.Encrypted = encrypted

	mReq, _ := json.Marshal(tReq)
	fmt.Printf("Input: %s\n", mReq)
	res, err := swarmdb.SelectHandler(u, string(mReq))
	if err != nil {
		t.Fatalf("[swarmdb_test:TestCoreTables] CREATE DATABASE: %s", err)
	}
	fmt.Printf("Output: %s\n\n", res.Stringify())

	// create database again with the exact name ==> should fail
	fmt.Printf("Input: %s\n", mReq)
	res, err = swarmdb.SelectHandler(u, string(mReq))
	if err != nil {
		fmt.Printf("Output: %s\n\n", err)
	} else {
		fmt.Printf("Output: %s\n\n", res.Stringify())
		t.Fatalf("[swarmdb_test:TestCoreTables] CREATE DATABASE again succeeded")
	}

	// create another database

	tReq = new(sdb.RequestOption)
	tReq.RequestType = sdb.RT_CREATE_DATABASE
	tReq.Owner = owner
	tReq.Database = database2
	tReq.Encrypted = encrypted2

	mReq, _ = json.Marshal(tReq)
	fmt.Printf("Input: %s\n", mReq)
	res, err = swarmdb.SelectHandler(u, string(mReq))
	if err != nil {
		t.Fatalf("[swarmdb_test:TestCoreTables] CREATE DATABASE: %s", err)
	}
	fmt.Printf("Output: %s\n\n", res.Stringify())

	// list databases ==> should have 2 databases
	tReq = new(sdb.RequestOption)
	tReq.RequestType = sdb.RT_LIST_DATABASES
	tReq.Owner = owner
	tReq.Database = database
	//tReq.Encrypted = encrypted

	mReq, _ = json.Marshal(tReq)
	fmt.Printf("Input: %s\n", mReq)
	res, err = swarmdb.SelectHandler(u, string(mReq))
	if err != nil {
		t.Fatalf("[swarmdb_test:TestCoreTables] LIST DATABASES: %s", err)
	}
	if len(res.Data) != 2 {
		t.Fatalf("[swarmdb_test:TestCoreTables] incorrect number of databases: %d", len(res.Data))
	}
	fmt.Printf("Output: %s \n\n", res.Stringify())

	// list databases ==> should have 0 databases
	tReq = new(sdb.RequestOption)
	tReq.RequestType = sdb.RT_LIST_DATABASES
	tReq.Owner = owner2
	tReq.Database = database
	mReq, _ = json.Marshal(tReq)
	fmt.Printf("Input: %s\n", mReq)
	res, err = swarmdb.SelectHandler(u, string(mReq))
	if err != nil {
		t.Fatalf("[swarmdb_test:TestCoreTables] LIST DATABASES: %s", err)
	}
	if len(res.Data) != 0 {
		t.Fatalf("[swarmdb_test:TestCoreTables] incorrect number of databases: %d", len(res.Data))
	}
	fmt.Printf("Output: %s \n\n", res.Stringify())

	// list tables
	tReq.RequestType = sdb.RT_LIST_TABLES
	tReq.Owner = owner
	tReq.Database = database
	mReq, _ = json.Marshal(tReq)
	fmt.Printf("Input: %s\n", mReq)
	res, err = swarmdb.SelectHandler(u, string(mReq))
	if err != nil {
		t.Fatalf("[swarmdb_test:TestCoreTables] LIST TABLES: %s", err)
	}
	if len(res.Data) > 0 {
		t.Fatalf("[swarmdb_test:TestCoreTables] incorrect number of tables: %d", len(res.Data))
	}
	fmt.Printf("Output: %s\n\n", res.Stringify())

	tabletest := make([]testTableConfig, 6)
	tabletest[0].tableName = make_name("teststrb")
	tabletest[0].primaryColumnName = "stb"
	tabletest[0].columnType = sdb.CT_STRING
	tabletest[0].indexType = sdb.IT_BPLUSTREE
	tabletest[0].sampleValue1 = "gamma"
	tabletest[0].sampleValue2 = "alpha"
	tabletest[0].sampleValue3 = "beta"
	tabletest[0].sampleValue1str = fmt.Sprintf("%s", tabletest[0].sampleValue1)
	tabletest[0].sampleValue2str = fmt.Sprintf("%s", tabletest[0].sampleValue2)
	tabletest[0].sampleValue3str = fmt.Sprintf("%s", tabletest[0].sampleValue3)

	tabletest[1].tableName = make_name("teststrh")
	tabletest[1].primaryColumnName = "sth"
	tabletest[1].columnType = sdb.CT_STRING
	tabletest[1].indexType = sdb.IT_HASHTREE
	tabletest[1].sampleValue1 = "gamma"
	tabletest[1].sampleValue2 = "alpha"
	tabletest[1].sampleValue3 = "beta"
	tabletest[1].sampleValue1str = fmt.Sprintf("%s", tabletest[1].sampleValue1)
	tabletest[1].sampleValue2str = fmt.Sprintf("%s", tabletest[1].sampleValue2)
	tabletest[1].sampleValue3str = fmt.Sprintf("%s", tabletest[1].sampleValue3)

	tabletest[2].tableName = make_name("testintb")
	tabletest[2].primaryColumnName = "inb"
	tabletest[2].columnType = sdb.CT_INTEGER
	tabletest[2].indexType = sdb.IT_BPLUSTREE
	tabletest[2].sampleValue1 = 3
	tabletest[2].sampleValue2 = 1
	tabletest[2].sampleValue3 = 2
	tabletest[2].sampleValue1str = fmt.Sprintf("%d", tabletest[2].sampleValue1)
	tabletest[2].sampleValue2str = fmt.Sprintf("%d", tabletest[2].sampleValue2)
	tabletest[2].sampleValue3str = fmt.Sprintf("%d", tabletest[2].sampleValue3)

	tabletest[3].tableName = make_name("testinth")
	tabletest[3].primaryColumnName = "inh"
	tabletest[3].columnType = sdb.CT_INTEGER
	tabletest[3].indexType = sdb.IT_HASHTREE
	tabletest[3].sampleValue1 = 3
	tabletest[3].sampleValue2 = 1
	tabletest[3].sampleValue3 = 2
	tabletest[3].sampleValue1str = fmt.Sprintf("%d", tabletest[3].sampleValue1)
	tabletest[3].sampleValue2str = fmt.Sprintf("%d", tabletest[3].sampleValue2)
	tabletest[3].sampleValue3str = fmt.Sprintf("%d", tabletest[3].sampleValue3)

	tabletest[4].tableName = make_name("testfltb")
	tabletest[4].primaryColumnName = "flb"
	tabletest[4].columnType = sdb.CT_FLOAT
	tabletest[4].indexType = sdb.IT_BPLUSTREE
	tabletest[4].sampleValue1 = 3.14
	tabletest[4].sampleValue2 = 1.66
	tabletest[4].sampleValue3 = 2.71
	tabletest[4].sampleValue1str = fmt.Sprintf("%f", tabletest[4].sampleValue1)
	tabletest[4].sampleValue2str = fmt.Sprintf("%f", tabletest[4].sampleValue2)
	tabletest[4].sampleValue3str = fmt.Sprintf("%f", tabletest[4].sampleValue3)

	tabletest[5].tableName = make_name("testflth")
	tabletest[5].primaryColumnName = "flh"
	tabletest[5].columnType = sdb.CT_FLOAT
	tabletest[5].indexType = sdb.IT_HASHTREE
	tabletest[5].sampleValue1 = 3.14
	tabletest[5].sampleValue2 = 1.66
	tabletest[5].sampleValue3 = 2.71
	tabletest[5].sampleValue1str = fmt.Sprintf("%f", tabletest[5].sampleValue1)
	tabletest[5].sampleValue2str = fmt.Sprintf("%f", tabletest[5].sampleValue2)
	tabletest[5].sampleValue3str = fmt.Sprintf("%f", tabletest[5].sampleValue3)

	tableCountExpected := 0
	for _, tbl := range tabletest {
		tableName := tbl.tableName

		// CREATE TABLE
		var testColumn []sdb.Column
		testColumn = make([]sdb.Column, 3)
		testColumn[0].ColumnName = tbl.primaryColumnName
		testColumn[0].Primary = 1 // TODO: test when (a) more than one primary (b) no primary specified
		testColumn[0].IndexType = tbl.indexType
		testColumn[0].ColumnType = tbl.columnType

		testColumn[1].ColumnName = "name"
		testColumn[1].Primary = 0
		testColumn[1].IndexType = sdb.IT_BPLUSTREE
		testColumn[1].ColumnType = sdb.CT_STRING // TODO: test what happens when value of incorrect type supplied for column

		testColumn[2].ColumnName = "age"
		testColumn[2].Primary = 0
		testColumn[2].IndexType = sdb.IT_BPLUSTREE
		testColumn[2].ColumnType = sdb.CT_INTEGER

		tReq = new(sdb.RequestOption)
		tReq.RequestType = sdb.RT_CREATE_TABLE
		tReq.Owner = owner
		tReq.Database = database
		tReq.Table = tableName
		tReq.Columns = testColumn
		mReq, _ = json.Marshal(tReq)
		fmt.Printf("Input: %s\n", mReq)
		res, err = swarmdb.SelectHandler(u, string(mReq))
		if err != nil {
			t.Fatalf("[swarmdb_test:TestCoreTables] CreateTable: %s", err)
		}
		fmt.Printf("Output: %s\n\n", res.Stringify())

		// CREATE TABLE second time ==> FAIL
		fmt.Printf("Input: %s\n", mReq)
		res, err = swarmdb.SelectHandler(u, string(mReq))
		if err != nil {
			fmt.Printf("Output: %s\n\n", err)
		} else {
			fmt.Printf("Output: %s\n\n", res)
			t.Fatalf("[swarmdb_test:TestCoreTables] CreateTable2: %s", err)
		}

		// DESCRIBE TABLE
		tReq = new(sdb.RequestOption)
		tReq.RequestType = sdb.RT_DESCRIBE_TABLE
		tReq.Owner = owner
		tReq.Database = database
		tReq.Table = tableName
		mReq, _ = json.Marshal(tReq)
		fmt.Printf("Input: %s\n", mReq)
		res, err = swarmdb.SelectHandler(u, string(mReq))
		if err != nil {
			t.Fatalf("[swarmdb_test:TestCoreTables] DescribeTable: %s", err)
		}
		fmt.Printf("Output: %s\n\n", res.Stringify())
		if len(res.Data) != 3 {
			t.Fatalf("[swarmdb_test:TestCoreTables] DescribeTable: incorrect data %s")
		}

		// PUT(sampleValue1)
		testKey := tbl.sampleValue1
		tReq = new(sdb.RequestOption)
		tReq.RequestType = sdb.RT_PUT
		tReq.Owner = owner
		tReq.Database = database
		tReq.Table = tableName
		tReq.Key = testKey
		rowObj := make(sdb.Row)
		rowObj[tbl.primaryColumnName] = testKey
		rowObj["name"] = "Rodney"
		rowObj["age"] = int(37)

		tReq.Rows = append(tReq.Rows, rowObj)
		mReq, _ = json.Marshal(tReq)
		fmt.Printf("Input: %s\n", mReq)
		res, err := swarmdb.SelectHandler(u, string(mReq))
		if err != nil {
			t.Fatalf("[swarmdb_test:TestCoreTables] Put %s", err.Error())
		}
		fmt.Printf("Output: %s\n\n", res.Stringify())
		if res.AffectedRowCount != 1 {
			t.Fatalf("Put affectedRowCount NOT OK")
		}

		// GET(sampleValue1)
		tReq = new(sdb.RequestOption)
		tReq.RequestType = sdb.RT_GET
		tReq.Owner = owner
		tReq.Database = database
		tReq.Table = tableName
		tReq.Key = testKey
		mReq, _ = json.Marshal(tReq)
		fmt.Printf("Input: %s\n", mReq)
		res, err = swarmdb.SelectHandler(u, string(mReq))
		if err != nil {
			t.Fatalf("[swarmdb_test:TestCoreTables] Get %s", err.Error())
		}
		fmt.Printf("Output: %s\n\n", res.Stringify())
		if len(res.Data) == 1 {
			d := res.Data[0]
			if d["age"] != 37 {
				t.Fatalf("MISMATCH: [%v]\n", d["age"])
			} else if strings.Compare(d["name"].(string), "Rodney") != 0 {
				t.Fatalf("MISMATCH: [%v]\n", d["name"])
			} else {
				if tbl.columnType == sdb.CT_STRING {
					if strings.Compare(d[tbl.primaryColumnName].(string), testKey.(string)) != 0 {
						t.Fatalf("MISMATCH on %s: [%v] != [%v]\n", tbl.primaryColumnName, d[tbl.primaryColumnName], testKey)
					}
				} else if tbl.columnType == sdb.CT_INTEGER {
					if d[tbl.primaryColumnName].(int) != testKey.(int) {
						t.Fatalf("MISMATCH on %s: [%d] != [%d]\n", tbl.primaryColumnName, d[tbl.primaryColumnName], testKey)
					}
				} else if tbl.columnType == sdb.CT_FLOAT {
					if d[tbl.primaryColumnName].(float64) != testKey.(float64) {
						t.Fatalf("MISMATCH on %s: [%f] != [%f]\n", tbl.primaryColumnName, d[tbl.primaryColumnName], testKey)
					}
				}
			}
		} else {
			t.Fatalf("Missing row!")
		}
		// GET(samplevalue2) should return ok = false, but not error
		tReq = new(sdb.RequestOption)
		tReq.RequestType = sdb.RT_GET
		tReq.Owner = owner
		tReq.Database = database
		tReq.Table = tableName
		tReq.Key = tbl.sampleValue2
		mReq, _ = json.Marshal(tReq)
		fmt.Printf("Input: %s\n", mReq)
		res, err = swarmdb.SelectHandler(u, string(mReq))
		if err != nil {
			t.Fatalf("[swarmdb_test:TestCoreTables] Get %s", err.Error())
		}
		fmt.Printf("Output: %s\n\n", res.Stringify())
		if len(res.Data) > 0 {
			t.Fatalf("[swarmdb_test:TestCoreTables] Get(samplevalue2) should not be returning data")
		}

		// DELETE(samplevalue2) should return ok = false, but not error
		tReq = new(sdb.RequestOption)
		tReq.RequestType = sdb.RT_DELETE
		tReq.Owner = owner
		tReq.Database = database
		tReq.Table = tableName
		tReq.Key = tbl.sampleValue2
		mReq, _ = json.Marshal(tReq)
		fmt.Printf("Input: %s\n", mReq)
		res, err = swarmdb.SelectHandler(u, string(mReq))
		if err != nil {
			t.Fatalf("[swarmdb_test:TestCoreTables] Delete %s", err.Error())
		}
		if res.AffectedRowCount > 0 {
			t.Fatalf("[swarmdb_test:TestCoreTables] Delete has incorrect affectedRowCount %d", res.AffectedRowCount)
		}
		fmt.Printf("Output: %s\n\n", res.Stringify())

		// INSERT(sampleValue2) QUERY
		tReq = new(sdb.RequestOption)
		queryInsert := fmt.Sprintf("insert into %s (%s, name, age) values ('%s', 'randomname', '99')", tableName, tbl.primaryColumnName, tbl.sampleValue2str)
		tReq.RequestType = sdb.RT_QUERY
		tReq.Owner = owner
		tReq.Database = database
		tReq.Table = tableName
		tReq.RawQuery = queryInsert
		mReq, _ = json.Marshal(tReq)
		fmt.Printf("Input: %s\n", mReq)
		res, err = swarmdb.SelectHandler(u, string(mReq))
		if err != nil {
			t.Fatalf("[swarmdb_test:TestCoreTables] Insert %s", err.Error())
		}
		fmt.Printf("Output: %s\n\n", res.Stringify())
		if res.AffectedRowCount != 1 {
			t.Fatalf("[swarmdb_test:TestCoreTables] Insert affected row count has incorrect affectedRowCount %d", res.AffectedRowCount)
		}

		// SELECT(sampleValue2) ==> 1
		tReq = new(sdb.RequestOption)
		querySelect := fmt.Sprintf("select %s, name, age from %s where %s = '%s'", tbl.primaryColumnName, tableName, tbl.primaryColumnName, tbl.sampleValue2str)
		tReq.RequestType = sdb.RT_QUERY
		tReq.Owner = owner
		tReq.Database = database
		tReq.Table = tableName
		tReq.RawQuery = querySelect
		mReq, _ = json.Marshal(tReq)
		fmt.Printf("Input: %s\n", mReq)
		res, err = swarmdb.SelectHandler(u, string(mReq))
		if err != nil {
			t.Fatalf("[swarmdb_test:TestCoreTables] Select(samplevalue2) %s", err.Error())
		}
		fmt.Printf("Output: %s\n\n", res.Stringify())
		if len(res.Data) != 1 {
			t.Fatalf("[swarmdb_test:TestCoreTables] Select(samplevalue2) incorrect # of rows in output %d", len(res.Data))
		}

		// SELECT AND ==> 2 rows
		tReq = new(sdb.RequestOption)
		querySelect = fmt.Sprintf("select %s, name, age from %s where %s = \"%s\" AND age = 99", tbl.primaryColumnName, tableName, tbl.primaryColumnName, tbl.sampleValue2str)
		tReq.RequestType = sdb.RT_QUERY
		tReq.Owner = owner
		tReq.Database = database
		tReq.Table = tableName
		tReq.RawQuery = querySelect
		mReq, _ = json.Marshal(tReq)
		fmt.Printf("Input: %s\n", mReq)
		res, err = swarmdb.SelectHandler(u, string(mReq))
		if err != nil {
			// t.Fatalf("[swarmdb_test:TestCoreTables] Select(*) %s", err.Error())
			// TODO: FIX THIS -- [swarmdb_test:TestCoreTables] Select(*) [swarmdb:SelectHandler] Query col [(stb = 'alpha')] does not exist in table
		}
		fmt.Printf("Output: %s\n\n", res.Stringify())
		if len(res.Data) != 1 {
			// t.Fatalf("[swarmdb_test:TestCoreTables] Select AND has incorrect # of rows in output %d (should be 1)", len(res.Data))
		}

		// SELECT OR ==> 2 rows
		tReq = new(sdb.RequestOption)
		querySelect = fmt.Sprintf("select %s, name, age from %s where %s = '%s' OR %s = '%s'", tbl.primaryColumnName, tableName, tbl.primaryColumnName, tbl.sampleValue1str, tbl.primaryColumnName, tbl.sampleValue2str)
		tReq.RequestType = sdb.RT_QUERY
		tReq.Owner = owner
		tReq.Database = database
		tReq.Table = tableName
		tReq.RawQuery = querySelect
		mReq, _ = json.Marshal(tReq)
		fmt.Printf("Input: %s\n", mReq)
		res, err = swarmdb.SelectHandler(u, string(mReq))
		if err != nil {
			// t.Fatalf("[swarmdb_test:TestCoreTables] Select(*) %s", err.Error())
			// TODO: FIX THIS -- Select(*) [swarmdb:SelectHandler] Query col [(stb = 'gamma')] does not exist in table
		}
		fmt.Printf("Output: %s\n\n", res.Stringify())
		if len(res.Data) != 2 {
			// t.Fatalf("[swarmdb_test:TestCoreTables] Select OR has incorrect # of rows in output %d (should be 1)", len(res.Data))
		}

		// SCAN ==> 2 Rows
		tReq = new(sdb.RequestOption)
		tReq.RequestType = sdb.RT_SCAN
		tReq.Owner = owner
		tReq.Database = database
		tReq.Table = tableName
		mReq, _ = json.Marshal(tReq)
		fmt.Printf("Input: %s\n", mReq)
		res, err = swarmdb.SelectHandler(u, string(mReq))
		if err != nil {
			t.Fatalf("[swarmdb_test:TestCoreTables] Scan %s", err.Error())
		}
		if res.AffectedRowCount != 2 {
			t.Fatalf("[swarmdb_test:TestCoreTables] Scan should be returning 2 rows, got %d", res.AffectedRowCount)
		}
		fmt.Printf("Output: %s\n\n", res.Stringify())

		// SELECT(sampleValue3) ==> 0 rows
		tReq = new(sdb.RequestOption)
		querySelect = fmt.Sprintf("select %s, name, age from %s where %s = '%s'", tbl.primaryColumnName, tableName, tbl.primaryColumnName, tbl.sampleValue3str)
		tReq.RequestType = sdb.RT_QUERY
		tReq.Owner = owner
		tReq.Database = database
		tReq.Table = tableName
		tReq.RawQuery = querySelect
		mReq, _ = json.Marshal(tReq)
		fmt.Printf("Input: %s\n", mReq)
		res, err = swarmdb.SelectHandler(u, string(mReq))
		if err != nil {
			t.Fatalf("[swarmdb_test:TestCoreTables] Select(samplevalue3) %s", err.Error())
		}
		fmt.Printf("Output: %s\n\n", res.Stringify())
		if len(res.Data) > 0 {
			t.Fatalf("[swarmdb_test:TestCoreTables] Select(samplevalue3) has incorrect # of rows in output %d (should be 0)", len(res.Data))
		}

		// Update(sampleValue2) ==> 1 row affected
		tReq = new(sdb.RequestOption)
		queryUpdate := fmt.Sprintf("update %s set age = 38 where %s = '%s'", tableName, tbl.primaryColumnName, tbl.sampleValue2str)
		tReq.RequestType = sdb.RT_QUERY
		tReq.Owner = owner
		tReq.Database = database
		tReq.Table = tableName
		tReq.RawQuery = queryUpdate
		mReq, _ = json.Marshal(tReq)
		fmt.Printf("Input: %s\n", mReq)
		res, err = swarmdb.SelectHandler(u, string(mReq))
		if err != nil {
			t.Fatalf("[swarmdb_test:TestCoreTables] Update(samplevalue2) %s", err.Error())
		}
		fmt.Printf("Output: %s\n\n", res.Stringify())
		if res.AffectedRowCount != 1 {
			t.Fatalf("[swarmdb_test:TestCoreTables] Update(samplevalue2) has incorrect # of rows %d affected (should be 1)", res.AffectedRowCount)
		}

		// GET(samplevalue2) should have age 38
		tReq = new(sdb.RequestOption)
		tReq.RequestType = sdb.RT_GET
		tReq.Owner = owner
		tReq.Database = database
		tReq.Table = tableName
		tReq.Key = tbl.sampleValue2
		mReq, _ = json.Marshal(tReq)
		fmt.Printf("Input: %s\n", mReq)
		res, err = swarmdb.SelectHandler(u, string(mReq))
		if err != nil {
			t.Fatalf("[swarmdb_test:TestCoreTables] Get %s", err.Error())
		}
		fmt.Printf("Output: %s\n\n", res.Stringify())
		if len(res.Data) > 0 {
			row := res.Data[0]
			if row["age"] != 38 {
				// TODO: [swarmdb_test:TestCoreTables] Get(samplevalue2) should be 38
				// t.Fatalf("[swarmdb_test:TestCoreTables] Get(samplevalue2) should be 38")
			}
		} else {
			t.Fatalf("[swarmdb_test:TestCoreTables] Get(samplevalue2) should be returning data")
		}

		// Delete(sampleValue2) ==> 1 row affected
		tReq = new(sdb.RequestOption)
		queryDelete := fmt.Sprintf("delete from %s where %s = '%s'", tableName, tbl.primaryColumnName, tbl.sampleValue2str)
		tReq.RequestType = sdb.RT_QUERY
		tReq.Owner = owner
		tReq.Database = database
		tReq.Table = tableName
		tReq.RawQuery = queryDelete
		mReq, _ = json.Marshal(tReq)
		fmt.Printf("Input: %s\n", mReq)
		res, err = swarmdb.SelectHandler(u, string(mReq))
		if err != nil {
			t.Fatalf("[swarmdb_test:TestCoreTables] Delete(samplevalue2)y %s", err.Error())
		}
		fmt.Printf("Output: %s\n\n", res.Stringify())
		if res.AffectedRowCount != 1 {
			t.Fatalf("[swarmdb_test:TestCoreTables] Delete(samplevalue2) has incorrect # of rows %d affected (should be 1)", res.AffectedRowCount)
		}

		// GET(samplevalue2) should have no data
		tReq = new(sdb.RequestOption)
		tReq.RequestType = sdb.RT_GET
		tReq.Owner = owner
		tReq.Database = database
		tReq.Table = tableName
		tReq.Key = tbl.sampleValue2
		mReq, _ = json.Marshal(tReq)
		fmt.Printf("Input: %s\n", mReq)
		res, err = swarmdb.SelectHandler(u, string(mReq))
		if err != nil {
			t.Fatalf("[swarmdb_test:TestCoreTables] Get %s", err.Error())
		}
		fmt.Printf("Output: %s\n\n", res.Stringify())
		if len(res.Data) > 0 {
			t.Fatalf("[swarmdb_test:TestCoreTables] Get(samplevalue2) should be returning data")
		}

		// DELETE(samplevalue1) => 1
		tReq = new(sdb.RequestOption)
		tReq.RequestType = sdb.RT_DELETE
		tReq.Owner = owner
		tReq.Database = database
		tReq.Table = tableName
		tReq.Key = tbl.sampleValue1
		mReq, _ = json.Marshal(tReq)
		fmt.Printf("Input: %s\n", mReq)
		res, err = swarmdb.SelectHandler(u, string(mReq))
		if err != nil {
			t.Fatalf("[swarmdb_test:TestCoreTables] Delete(samplevalue1) %s", err.Error())
		}
		if res.AffectedRowCount > 0 {
		} else {
			t.Fatalf("[swarmdb_test:TestCoreTables] Delete has incorrect affectedRowCount %d", res.AffectedRowCount)
		}
		fmt.Printf("Output: %s\n\n", res.Stringify())

		// SCAN ==> 0 Rows
		tReq = new(sdb.RequestOption)
		tReq.RequestType = sdb.RT_SCAN
		tReq.Owner = owner
		tReq.Database = database
		tReq.Table = tableName
		mReq, _ = json.Marshal(tReq)
		fmt.Printf("Input: %s\n", mReq)
		res, err = swarmdb.SelectHandler(u, string(mReq))
		if err != nil {
			t.Fatalf("[swarmdb_test:TestCoreTables] Scan %s", err.Error())
		}
		if res.AffectedRowCount > 0 {
			t.Fatalf("[swarmdb_test:TestCoreTables] Scan should be returning 0 rows", res.AffectedRowCount)
		}
		fmt.Printf("Output: %s\n\n", res.Stringify())

		tableCountExpected = tableCountExpected + 1
	}

	// list tables, then drop 1 for each for the tables
	for _, tbl := range tabletest {
		// list tables should have 6 tables, then 5, .. then 4, ... until just 1
		tReq = new(sdb.RequestOption)
		tReq.RequestType = sdb.RT_LIST_TABLES
		tReq.Owner = owner
		tReq.Database = database
		tReq.Table = ""
		mReq, _ = json.Marshal(tReq)
		fmt.Printf("Input: %s\n", mReq)
		res, err = swarmdb.SelectHandler(u, string(mReq))
		if err != nil {
			t.Fatalf("SCAN error: %s", err)
		}
		fmt.Printf("Output: %s\n\n", res.Stringify())
		if len(res.Data) != tableCountExpected {
			t.Fatalf("[swarmdb_test:TestCoreTables] List Tables count error -- expected %d, but got %d", tableCountExpected, len(res.Data))
		}

		// drop table
		tReq.RequestType = sdb.RT_DROP_TABLE
		tReq.Owner = owner
		tReq.Database = database
		tReq.Table = tbl.tableName
		mReq, _ = json.Marshal(tReq)
		fmt.Printf("Input: %s\n", mReq)
		res, err = swarmdb.SelectHandler(u, string(mReq))
		if err != nil {
			t.Fatal(err)
		}
		if res.AffectedRowCount != 1 {
			t.Fatalf("[swarmdb_test:TestCoreTables] DROP TABLE  has incorrect affectedRowCount %d", res.AffectedRowCount)
		}
		fmt.Printf("Output: %s\n\n", res.Stringify())
		tableCountExpected = tableCountExpected - 1
	}

	// drop table
	tReq.RequestType = sdb.RT_DROP_TABLE
	tReq.Owner = owner
	tReq.Database = database
	tReq.Table = "random"
	mReq, _ = json.Marshal(tReq)
	fmt.Printf("Input: %s\n", mReq)
	res, err = swarmdb.SelectHandler(u, string(mReq))
	if err != nil {
		t.Fatal(err)
	}
	if res.AffectedRowCount > 0 {
		t.Fatalf("[swarmdb_test:TestCoreTables] DROP TABLE has incorrect affectedRowCount %d", res.AffectedRowCount)
	}
	fmt.Printf("Output: %s\n\n", res.Stringify())

	// drop database "random"
	tReq.RequestType = sdb.RT_DROP_DATABASE
	tReq.Owner = owner
	tReq.Database = "random"
	mReq, _ = json.Marshal(tReq)
	fmt.Printf("Input: %s\n", mReq)
	res, err = swarmdb.SelectHandler(u, string(mReq))
	if err != nil {
		t.Fatal(err)
	}
	if res.AffectedRowCount > 0 {
		t.Fatalf("[swarmdb_test:TestCoreTables] DROP DATABASE has incorrect affectedRowCount %d", res.AffectedRowCount)
	}
	fmt.Printf("Output: %s\n\n", res.Stringify())

	// drop database
	tReq = new(sdb.RequestOption)
	tReq.RequestType = sdb.RT_DROP_DATABASE
	tReq.Owner = owner
	tReq.Database = database
	mReq, _ = json.Marshal(tReq)
	fmt.Printf("Input: %s\n", mReq)
	res, err = swarmdb.SelectHandler(u, string(mReq))
	if err != nil {
		t.Fatal(err)
	}
	fmt.Printf("Output: %s\n\n", res.Stringify())
	if res.AffectedRowCount != 1 {
		t.Fatalf("[swarmdb_test:TestCoreTables] DROP DATABASE has incorrect affectedRowCount %d", res.AffectedRowCount)
	}

	// list databases
	tReq = new(sdb.RequestOption)
	tReq.RequestType = sdb.RT_LIST_DATABASES
	tReq.Owner = owner
	mReq, _ = json.Marshal(tReq)
	fmt.Printf("Input: %s\n", mReq)
	res, err = swarmdb.SelectHandler(u, string(mReq))
	if err != nil {
		t.Fatalf("error marshaling tReq 2: %s", err)
	}
	fmt.Printf("Output: %s\n\n", res.Stringify())
	if len(res.Data) > 1 {
		t.Fatalf("[swarmdb_test:TestCoreTables] LIST DATABASES has incorrect outputs")
	}
	if res.AffectedRowCount > 0 {
		t.Fatalf("[swarmdb_test:TestCoreTables] LIST DATABASES has incorrect affectedRowCount %d", res.AffectedRowCount)
	}
}

func checktype(columnType sdb.ColumnType, v interface{}) (ok bool) {
	switch columnType {
	case sdb.CT_INTEGER:
		switch v.(type) {
		case int, uint:
			return true
		default:
			return false
		}
	case sdb.CT_STRING:
		switch v.(type) {
		case string:
			return true
		default:
			return false
		}
	case sdb.CT_FLOAT:
		switch v.(type) {
		case float32, float64:
			return true
		default:
			return false
		}
	default:
		return false
	}
	return false
}

func TestTypeCoercion(t *testing.T) {

	owner := make_name("owner.eth")
	database := make_name("dbTestTypeCoercion")
	encrypted := int(1)

	// create database
	var tReq *sdb.RequestOption
	tReq = new(sdb.RequestOption)
	tReq.RequestType = sdb.RT_CREATE_DATABASE
	tReq.Owner = owner
	tReq.Database = database
	tReq.Encrypted = encrypted

	mReq, _ := json.Marshal(tReq)
	fmt.Printf("Input: %s\n", mReq)
	res, err := swarmdb.SelectHandler(u, string(mReq))
	if err != nil {
		t.Fatalf("[swarmdb_test:TestTypeCoercion] CREATE DATABASE: %s |  %s", tReq.Database, err)
	}
	fmt.Printf("Output: %s\n\n", res.Stringify())

	tabletest := make([]testTableConfig, 4)

	// (1) create table with "inb" integer (primary key) and "age" integer (secondary key)
	tabletest[0].tableName = make_name("testintb")
	tabletest[0].primaryColumnName = "inb"
	tabletest[0].columnType = sdb.CT_INTEGER
	tabletest[0].indexType = sdb.IT_BPLUSTREE
	tabletest[0].sampleValue1 = 88
	tabletest[0].sampleValue2 = 9
	tabletest[0].sampleValue3 = 13
	tabletest[0].sampleValue4 = 47
	tabletest[0].sampleValue1str = fmt.Sprintf("%d", tabletest[0].sampleValue1)
	tabletest[0].sampleValue2str = fmt.Sprintf("%d", tabletest[0].sampleValue2)
	tabletest[0].sampleValue3str = fmt.Sprintf("%d", tabletest[0].sampleValue3)
	tabletest[0].sampleValue4str = fmt.Sprintf("%d", tabletest[0].sampleValue4)

	// (2) same as above, with hashdb
	tabletest[1].tableName = make_name("testinth")
	tabletest[1].primaryColumnName = "inh"
	tabletest[1].columnType = sdb.CT_INTEGER
	tabletest[1].indexType = sdb.IT_HASHTREE
	tabletest[1].sampleValue1 = 77
	tabletest[1].sampleValue2 = 5
	tabletest[1].sampleValue3 = 19
	tabletest[1].sampleValue4 = 100
	tabletest[1].sampleValue1str = fmt.Sprintf("%d", tabletest[1].sampleValue1)
	tabletest[1].sampleValue2str = fmt.Sprintf("%d", tabletest[1].sampleValue2)
	tabletest[1].sampleValue3str = fmt.Sprintf("%d", tabletest[1].sampleValue3)
	tabletest[1].sampleValue4str = fmt.Sprintf("%d", tabletest[1].sampleValue4)

	// (3) create table with "flb" float and "age" float with samplevalue1str and samplevalue2str
	tabletest[2].tableName = make_name("testfltb")
	tabletest[2].primaryColumnName = "flb"
	tabletest[2].columnType = sdb.CT_FLOAT
	tabletest[2].indexType = sdb.IT_BPLUSTREE
	tabletest[2].sampleValue1 = float64(3.14)
	tabletest[2].sampleValue2 = float64(1.66)
	tabletest[2].sampleValue3 = float64(2.71)
	tabletest[2].sampleValue4 = float64(4.87)
	tabletest[2].sampleValue1str = fmt.Sprintf("%f", tabletest[2].sampleValue1)
	tabletest[2].sampleValue2str = fmt.Sprintf("%f", tabletest[2].sampleValue2)
	tabletest[2].sampleValue3str = fmt.Sprintf("%f", tabletest[2].sampleValue3)
	tabletest[2].sampleValue4str = fmt.Sprintf("%f", tabletest[2].sampleValue4)

	// (4) same tests with hashdb
	tabletest[3].tableName = make_name("testflth")
	tabletest[3].primaryColumnName = "flh"
	tabletest[3].columnType = sdb.CT_FLOAT
	tabletest[3].indexType = sdb.IT_HASHTREE
	tabletest[3].sampleValue1 = float64(12.34)
	tabletest[3].sampleValue2 = float64(666.66)
	tabletest[3].sampleValue3 = float64(43.21)
	tabletest[3].sampleValue4 = float64(77.88)
	tabletest[3].sampleValue1str = fmt.Sprintf("%f", tabletest[3].sampleValue1)
	tabletest[3].sampleValue2str = fmt.Sprintf("%f", tabletest[3].sampleValue2)
	tabletest[3].sampleValue3str = fmt.Sprintf("%f", tabletest[3].sampleValue3)
	tabletest[3].sampleValue4str = fmt.Sprintf("%f", tabletest[3].sampleValue4)

	for _, tbl := range tabletest {
		tableName := tbl.tableName

		// CREATE TABLE
		var testColumn []sdb.Column
		testColumn = make([]sdb.Column, 3)
		testColumn[0].ColumnName = tbl.primaryColumnName
		testColumn[0].Primary = 1 // TODO: test when (a) more than one primary (b) no primary specified
		testColumn[0].IndexType = tbl.indexType
		testColumn[0].ColumnType = tbl.columnType

		testColumn[1].ColumnName = "age"
		testColumn[1].Primary = 0
		testColumn[1].IndexType = tbl.indexType
		testColumn[1].ColumnType = tbl.columnType

		testColumn[2].ColumnName = "name"
		testColumn[2].Primary = 0
		testColumn[2].IndexType = sdb.IT_BPLUSTREE
		testColumn[2].ColumnType = sdb.CT_STRING // TODO: test what happens when value of incorrect type supplied for column

		tReq = new(sdb.RequestOption)
		tReq.RequestType = sdb.RT_CREATE_TABLE
		tReq.Owner = owner
		tReq.Database = database
		tReq.Table = tableName
		tReq.Columns = testColumn
		mReq, _ = json.Marshal(tReq)
		fmt.Printf("Input: %s\n", mReq)
		res, err = swarmdb.SelectHandler(u, string(mReq))
		if err != nil {
			t.Fatalf("[swarmdb_test:TestTypeCoercion] CreateTable: %s", err)
		}
		fmt.Printf("Output: %s\n\n", res.Stringify())

		// PUT(samplevalue1) - with quotes
		dReq := fmt.Sprintf("{\"requesttype\":\"Put\",\"owner\":\"%s\",\"database\":\"%s\",\"table\":\"%s\",\"key\":\"%s\",\"rows\":[{\"%s\":\"%s\",\"age\":\"%s\",\"name\":\"Rodney\"}]}", owner, database, tableName, tbl.sampleValue1str, tbl.primaryColumnName, tbl.sampleValue1str, tbl.sampleValue2str)
		fmt.Printf("Input: %s\n", dReq)
		res, err := swarmdb.SelectHandler(u, dReq)
		if err != nil {
			t.Fatalf("[swarmdb_test:TestTypeCoercion] Put %s", err.Error())
		}
		fmt.Printf("Output: %s\n\n", res.Stringify())
		if res.AffectedRowCount != 1 {
			t.Fatalf("Put affectedRowCount NOT OK")
		}

		// GET(sampleValue1)
		tReq = new(sdb.RequestOption)
		tReq.RequestType = sdb.RT_GET
		tReq.Owner = owner
		tReq.Database = database
		tReq.Table = tableName
		tReq.Key = tbl.sampleValue1
		mReq, _ = json.Marshal(tReq)
		fmt.Printf("Input: %s\n", mReq)
		res, err = swarmdb.SelectHandler(u, string(mReq))
		if err != nil {
			t.Fatalf("[swarmdb_test:TestTypeCoercion] Get %s", err.Error())
		}
		fmt.Printf("Output: %s\n\n", res.Stringify())
		if len(res.Data) > 0 {
			row := res.Data[0]
			if row[tbl.primaryColumnName] != tbl.sampleValue1 {
				t.Fatalf("Mismatch: result[primaryColumnName] %v is not expected value %v", row[tbl.primaryColumnName], tbl.sampleValue1)
			}
			if row["age"] != tbl.sampleValue2 {
				t.Fatalf("Mismatch: result[age] %v is not expected value %v", row["age"], tbl.sampleValue2)
			}

			if checktype(tbl.columnType, row[tbl.primaryColumnName]) {
			} else {
				t.Fatalf("Mismatch: type mismatch on primaryColumn [%v]", row[tbl.primaryColumnName])
			}
			if checktype(tbl.columnType, row["age"]) {
			} else {
				t.Fatalf("Mismatch: type mismatch on age [%v]", row["age"])
			}
		} else {
			t.Fatalf("[swarmdb_test:TestTypeCoercion] No data for key")
		}

		// PUT(samplevalue2) - no quotes
		dReq = fmt.Sprintf("{\"requesttype\":\"Put\",\"owner\":\"%s\",\"database\":\"%s\",\"table\":\"%s\",\"key\":\"%s\",\"rows\":[{\"%s\":%s,\"age\":%s,\"name\":\"Rodney\"}]}", owner, database, tableName, tbl.sampleValue2str, tbl.primaryColumnName, tbl.sampleValue2str, tbl.sampleValue3str)
		fmt.Printf("Input: %s\n", dReq)
		res, err = swarmdb.SelectHandler(u, dReq)
		if err != nil {
			t.Fatalf("[swarmdb_test:TestTypeCoercion] Put %s", err.Error())
		}
		fmt.Printf("Output: %s\n\n", res.Stringify())
		if res.AffectedRowCount != 1 {
			t.Fatalf("Put affectedRowCount NOT OK")
		}

		// GET(sampleValue2)
		tReq = new(sdb.RequestOption)
		tReq.RequestType = sdb.RT_GET
		tReq.Owner = owner
		tReq.Database = database
		tReq.Table = tableName
		tReq.Key = tbl.sampleValue2
		mReq, _ = json.Marshal(tReq)
		fmt.Printf("Input: %s\n", mReq)
		res, err = swarmdb.SelectHandler(u, string(mReq))
		if err != nil {
			t.Fatalf("[swarmdb_test:TestTypeCoercion] Get %s", err.Error())
		}
		fmt.Printf("Output: %s\n\n", res.Stringify())
		if len(res.Data) > 0 {
			row := res.Data[0]
			if row[tbl.primaryColumnName] != tbl.sampleValue2 {
				t.Fatalf("Mismatch: result[primaryColumnName] %v is not expected value %v", row[tbl.primaryColumnName], tbl.sampleValue2)
			}
			if row["age"] != tbl.sampleValue3 {
				t.Fatalf("Mismatch: result[age] %v is not expected value %v", row["age"], tbl.sampleValue3)
			}
			if checktype(tbl.columnType, row[tbl.primaryColumnName]) {
			} else {
				t.Fatalf("Mismatch: type mismatch on primaryColumn [%v]", row[tbl.primaryColumnName])
			}
			if checktype(tbl.columnType, row["age"]) {
			} else {
				t.Fatalf("Mismatch: type mismatch on age [%v]", row["age"])
			}

		} else {
			t.Fatalf("[swarmdb_test:TestTypeCoercion] No data for key")
		}

		// INSERT(samplevalue3) -- with quotes
		sql := fmt.Sprintf("insert into %s (%s, age, name) values (\"%s\", \"%s\", \"Sourabh\")", tableName, tbl.primaryColumnName, tbl.sampleValue3str, tbl.sampleValue4str)
		tReq = new(sdb.RequestOption)
		tReq.RequestType = sdb.RT_QUERY
		tReq.Owner = owner
		tReq.Database = database
		tReq.Table = tableName
		tReq.RawQuery = sql
		mReq, _ = json.Marshal(tReq)
		fmt.Printf("Input: %s\n", mReq)
		res, err = swarmdb.SelectHandler(u, string(mReq))
		if err != nil {
			t.Fatalf("[swarmdb_test:TestTypeCoercion] Insert %s", err.Error())
		}
		fmt.Printf("Output: %s\n\n", res.Stringify())
		if res.AffectedRowCount != 1 {
			t.Fatalf("[swarmdb_test:TestTypeCoercion] Insert affected row count has incorrect affectedRowCount %d", res.AffectedRowCount)
		}

		// SELECT(samplevalue3)
		sql = fmt.Sprintf("select %s, age, name from %s where %s = \"%s\"", tbl.primaryColumnName, tableName, tbl.primaryColumnName, tbl.sampleValue3str)
		tReq = new(sdb.RequestOption)
		tReq.RequestType = sdb.RT_QUERY
		tReq.Owner = owner
		tReq.Database = database
		tReq.Table = tableName
		tReq.RawQuery = sql
		mReq, _ = json.Marshal(tReq)
		fmt.Printf("Input: %s\n", mReq)
		res, err = swarmdb.SelectHandler(u, string(mReq))
		if err != nil {
			t.Fatalf("[swarmdb_test:TestTypeCoercion] Select %s", err.Error())
		}
		fmt.Printf("Output: %s\n\n", res.Stringify())
		if len(res.Data) == 1 {
			row := res.Data[0]
			if row[tbl.primaryColumnName] != tbl.sampleValue3 {
				t.Fatalf("Mismatch: result[primaryColumnName] %v is not expected value %v", row[tbl.primaryColumnName], tbl.sampleValue3)
			}
			if row["age"] != tbl.sampleValue4 {
				t.Fatalf("Mismatch: result[age] %v is not expected value %v", row["age"], tbl.sampleValue4)
			}

			if checktype(tbl.columnType, row[tbl.primaryColumnName]) {
			} else {
				t.Fatalf("Mismatch: type mismatch on primaryColumn [%v]", row[tbl.primaryColumnName])
			}
			if checktype(tbl.columnType, row["age"]) {
			} else {
				t.Fatalf("Mismatch: type mismatch on age [%v]", row["age"])
			}
		} else {
			t.Fatalf("[swarmdb_test:TestTypeCoercion] Select affected row count has incorrect affectedRowCount %d [# Rows: %d]", res.AffectedRowCount, len(res.Data))
		}

		// INSERT(samplevalue4) -- without quotes
		sql = fmt.Sprintf("insert into %s (%s, age, name) values (%s, %s, \"Sourabh\")", tableName, tbl.primaryColumnName, tbl.sampleValue4str, tbl.sampleValue1str)
		tReq = new(sdb.RequestOption)
		tReq.RequestType = sdb.RT_QUERY
		tReq.Owner = owner
		tReq.Database = database
		tReq.Table = tableName
		tReq.RawQuery = sql
		mReq, _ = json.Marshal(tReq)
		fmt.Printf("Input: %s\n", mReq)
		res, err = swarmdb.SelectHandler(u, string(mReq))
		if err != nil {
			t.Fatalf("[swarmdb_test:TestTypeCoercion] Insert %s", err.Error())
		}
		fmt.Printf("Output: %s\n\n", res.Stringify())
		if res.AffectedRowCount != 1 {
			t.Fatalf("[swarmdb_test:TestTypeCoercion] Insert affected row count has incorrect affectedRowCount %d", res.AffectedRowCount)
		}

		// SELECT(samplevalue4)
		sql = fmt.Sprintf("select %s, age, name from %s where %s = \"%s\"", tbl.primaryColumnName, tableName, tbl.primaryColumnName, tbl.sampleValue4str)
		tReq = new(sdb.RequestOption)
		tReq.RequestType = sdb.RT_QUERY
		tReq.Owner = owner
		tReq.Database = database
		tReq.Table = tableName
		tReq.RawQuery = sql
		mReq, _ = json.Marshal(tReq)
		fmt.Printf("Input: %s\n", mReq)
		res, err = swarmdb.SelectHandler(u, string(mReq))
		if err != nil {
			t.Fatalf("[swarmdb_test:TestTypeCoercion] Insert %s", err.Error())
		}
		fmt.Printf("Output: %s\n\n", res.Stringify())
		if len(res.Data) == 1 {
			row := res.Data[0]
			if row[tbl.primaryColumnName] != tbl.sampleValue4 {
				t.Fatalf("Mismatch: result[primaryColumnName] %v is not expected value %v", row[tbl.primaryColumnName], tbl.sampleValue4)
			}
			if row["age"] != tbl.sampleValue1 {
				t.Fatalf("Mismatch: result[age] %v is not expected value %v", row["age"], tbl.sampleValue1)
			}

			if checktype(tbl.columnType, row[tbl.primaryColumnName]) {
			} else {
				t.Fatalf("Mismatch: type mismatch on primaryColumn [%v]", row[tbl.primaryColumnName])
			}
			if checktype(tbl.columnType, row["age"]) {
			} else {
				t.Fatalf("Mismatch: type mismatch on age [%v]", row["age"])
			}
		} else {
			t.Fatalf("[swarmdb_test:TestTypeCoercion] Select affected row count has incorrect affectedRowCount %d [# Rows: %d]", res.AffectedRowCount, len(res.Data))
		}
	}
}

func TestSmallOps(t *testing.T) {
	owner := make_name("smallops.eth")
	database := make_name("smalldb")
	encrypted := int(1)

	// create database
	var tReq *sdb.RequestOption
	tReq = new(sdb.RequestOption)
	tReq.RequestType = sdb.RT_CREATE_DATABASE
	tReq.Owner = owner
	tReq.Database = database
	tReq.Encrypted = encrypted

	mReq, _ := json.Marshal(tReq)
	fmt.Printf("Input: %s\n", mReq)
	res, err := swarmdb.SelectHandler(u, string(mReq))
	if err != nil {
		t.Fatalf("[swarmdb_test:TestSmallOps] CREATE DATABASE: %s", err)
	}
	fmt.Printf("Output: %s\n\n", res.Stringify())

	tabletest := make([]testTableConfig, 3)
	tabletest[0].tableName = make_name("testintb")
	tabletest[0].primaryColumnName = "inb"
	tabletest[0].columnType = sdb.CT_INTEGER
	tabletest[0].indexType = sdb.IT_BPLUSTREE
	tabletest[0].sampleValue1str = "55"
	tabletest[0].sampleValue2str = "50"
	tabletest[0].sampleValue3str = "45"

	tabletest[1].tableName = make_name("teststrb")
	tabletest[1].primaryColumnName = "stb"
	tabletest[1].columnType = sdb.CT_STRING
	tabletest[1].indexType = sdb.IT_BPLUSTREE
	tabletest[1].sampleValue1str = "key055"
	tabletest[1].sampleValue2str = "key050"
	tabletest[1].sampleValue3str = "key045"

	tabletest[2].tableName = make_name("testfltb")
	tabletest[2].primaryColumnName = "flb"
	tabletest[2].columnType = sdb.CT_FLOAT
	tabletest[2].indexType = sdb.IT_BPLUSTREE
	tabletest[2].sampleValue1str = "55.1"
	tabletest[2].sampleValue2str = "50.1"
	tabletest[2].sampleValue3str = "45.1"

	for _, tbl := range tabletest {
		tableName := tbl.tableName

		// CREATE TABLE
		var testColumn []sdb.Column
		testColumn = make([]sdb.Column, 3)
		testColumn[0].ColumnName = tbl.primaryColumnName
		testColumn[0].Primary = 1 // TODO: test when (a) more than one primary (b) no primary specified
		testColumn[0].IndexType = tbl.indexType
		testColumn[0].ColumnType = tbl.columnType
		testColumn[1].ColumnName = "name"
		testColumn[1].Primary = 0
		testColumn[1].IndexType = sdb.IT_BPLUSTREE
		testColumn[1].ColumnType = sdb.CT_STRING // TODO: test what happens when value of incorrect type supplied for column

		testColumn[2].ColumnName = "age"
		testColumn[2].Primary = 0
		testColumn[2].IndexType = sdb.IT_BPLUSTREE
		testColumn[2].ColumnType = sdb.CT_INTEGER

		tReq = new(sdb.RequestOption)
		tReq.RequestType = sdb.RT_CREATE_TABLE
		tReq.Owner = owner
		tReq.Database = database
		tReq.Table = tableName
		tReq.Columns = testColumn
		mReq, _ = json.Marshal(tReq)
		fmt.Printf("Input: %s\n", mReq)
		res, err = swarmdb.SelectHandler(u, string(mReq))
		if err != nil {
			t.Fatalf("[swarmdb_test:TestSmallOps] CreateTable: %s", err)
		}
		fmt.Printf("Output: %s\n\n", res.Stringify())

		// PUT(sampleValue1)
		for i := 0; i < 100; i++ {

			tReq = new(sdb.RequestOption)
			tReq.RequestType = sdb.RT_PUT
			tReq.Owner = owner
			tReq.Database = database
			tReq.Table = tableName
			rowObj := make(sdb.Row)
			switch tbl.columnType {
			case sdb.CT_INTEGER:
				tReq.Key = i
				rowObj[tbl.primaryColumnName] = i
				rowObj["name"] = fmt.Sprintf("name%3d", i)
				rowObj["age"] = 37 + i
			case sdb.CT_FLOAT:
				tReq.Key = float64(i) + .1
				rowObj[tbl.primaryColumnName] = tReq.Key
				rowObj["name"] = fmt.Sprintf("name%3d", i)
				rowObj["age"] = 13 + i
			case sdb.CT_STRING:
				tReq.Key = fmt.Sprintf("key%03d", i)
				rowObj[tbl.primaryColumnName] = tReq.Key
				rowObj["name"] = fmt.Sprintf("name%03d", i)
				rowObj["age"] = 40 + i
			}

			tReq.Rows = append(tReq.Rows, rowObj)
			mReq, _ = json.Marshal(tReq)
			res, err := swarmdb.SelectHandler(u, string(mReq))
			if err != nil {
				t.Fatalf("[swarmdb_test:TestSmallOps] Put %s", err.Error())
			}
			if res.AffectedRowCount != 1 {
				fmt.Printf("Input: %s\n", mReq)
				fmt.Printf("Output: %s\n\n", res.Stringify())
				t.Fatalf("Put affectedRowCount NOT OK")
			} else {
				fmt.Printf(".")
			}
		}
		fmt.Printf("Put operations done\n")

		// SCAN ==> 100 Rows
		tReq = new(sdb.RequestOption)
		tReq.RequestType = sdb.RT_SCAN
		tReq.Owner = owner
		tReq.Database = database
		tReq.Table = tableName
		mReq, _ = json.Marshal(tReq)
		fmt.Printf("Input: %s\n", mReq)
		res, err = swarmdb.SelectHandler(u, string(mReq))
		if err != nil {
			t.Fatalf("[swarmdb_test:TestSmallOps] Scan %s", err.Error())
		}
		if res.AffectedRowCount != 100 {
			t.Fatalf("[swarmdb_test:TestSmallOps] Scan should be returning 100 rows, got %d", res.AffectedRowCount)
		}
		fmt.Printf("Output: %s\n\n", res.Stringify())

		var expectedRows map[sdb.ColumnType]int
		expectedRows = make(map[sdb.ColumnType]int)
		for j := 0; j < 5; j++ {
			sql := ""
			switch j {
			case 0:
				sql = fmt.Sprintf("select %s, name, age from %s where %s >= '%s'", tbl.primaryColumnName, tableName, tbl.primaryColumnName, tbl.sampleValue1str)
				expectedRows[sdb.CT_INTEGER] = 45
				expectedRows[sdb.CT_FLOAT] = 45
				expectedRows[sdb.CT_STRING] = 45

			case 1:
				sql = fmt.Sprintf("select %s, name, age from %s where %s > '%s'", tbl.primaryColumnName, tableName, tbl.primaryColumnName, tbl.sampleValue1str)
				expectedRows[sdb.CT_INTEGER] = 44
				expectedRows[sdb.CT_FLOAT] = 44
				expectedRows[sdb.CT_STRING] = 44
			case 2:
				sql = fmt.Sprintf("select %s, name, age from %s where %s = '%s'", tbl.primaryColumnName, tableName, tbl.primaryColumnName, tbl.sampleValue2str)
				expectedRows[sdb.CT_INTEGER] = 1
				expectedRows[sdb.CT_FLOAT] = 1
				expectedRows[sdb.CT_STRING] = 1
			case 3:
				sql = fmt.Sprintf("select %s, name, age from %s where %s < '%s'", tbl.primaryColumnName, tableName, tbl.primaryColumnName, tbl.sampleValue3str)
				expectedRows[sdb.CT_INTEGER] = 45
				expectedRows[sdb.CT_FLOAT] = 45
				expectedRows[sdb.CT_STRING] = 45
			case 4:
				sql = fmt.Sprintf("select %s, name, age from %s where %s <= '%s'", tbl.primaryColumnName, tableName, tbl.primaryColumnName, tbl.sampleValue3str)
				expectedRows[sdb.CT_INTEGER] = 46
				expectedRows[sdb.CT_FLOAT] = 46
				expectedRows[sdb.CT_STRING] = 46
			}
			expectedAffectedRows := expectedRows[tbl.columnType]
			tReq = new(sdb.RequestOption)
			tReq.RequestType = sdb.RT_QUERY
			tReq.Owner = owner
			tReq.Database = database
			tReq.Table = tableName
			tReq.RawQuery = sql
			mReq, _ = json.Marshal(tReq)
			fmt.Printf("Input: %s\n", mReq)
			res, err = swarmdb.SelectHandler(u, string(mReq))
			if err != nil {
				t.Fatalf("[swarmdb_test:TestSmallOps] Select [%s] %s", sql, err.Error())
			}
			if expectedAffectedRows != len(res.Data) {
				fmt.Printf("Output: %s\tEXPECTED %d\tGOT %d\nRows = %s\n\n", tableName, expectedAffectedRows, len(res.Data), res.Stringify())
				t.Fatalf("[swarmdb_test:TestSmallOps] Select [%s] %s", sql, err.Error())
			} else {
				fmt.Printf("Output: %s\tEXPECTED %d\tGOT %d\n", tableName, expectedAffectedRows, len(res.Data))
			}
		}
	}
}
