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
	"strings"
	"swarmdb"
	"testing"
	"time"
)

const (
	TEST_OWNER           = "wolktoken.eth"
	TEST_DATABASE        = "pets"
	TEST_TABLE           = "dogs"
	TEST_PKEY_INT        = "accountID"
	TEST_PKEY_STRING     = "email"
	TEST_PKEY_FLOAT      = "ts"
	TEST_SKEY_INT        = "age"
	TEST_SKEY_STRING     = "gender"
	TEST_SKEY_FLOAT      = "weight"
	TEST_TABLE_INDEXTYPE = swarmdb.IT_BPLUSTREE
	TEST_ENS_DIR         = "/tmp"
)

type testTableConfig struct {
	tableName  string
	primaryColumnName string
	indexType  swarmdb.IndexType
	columnType swarmdb.ColumnType
	sampleValue1 interface{}
	sampleValue2 interface{}
	sampleValue3 interface{}
	sampleValue1str string 
	sampleValue2str string 
	sampleValue3str string 
}

func make_name(prefix string) (nm string) {
	return fmt.Sprintf("%s%d", prefix, int32(time.Now().Unix()))
}

func getUser() (u *swarmdb.SWARMDBUser) {
	config, err := swarmdb.LoadSWARMDBConfig(swarmdb.SWARMDBCONF_FILE)
	if err != nil {
		panic("No config error")
	}

	swarmdb.NewKeyManager(&config)
	user := config.GetSWARMDBUser()
	return user
}


func TestCoreTables(t *testing.T) {
	u := getUser()
	owner := make_name("owner")
	database := make_name("db")

	config, _ := swarmdb.LoadSWARMDBConfig(swarmdb.SWARMDBCONF_FILE)
	ensdbPath := TEST_ENS_DIR
	swdb, _ := swarmdb.NewSwarmDB(ensdbPath, config.ChunkDBPath)

	// create database
	var tReq *swarmdb.RequestOption
	tReq = new(swarmdb.RequestOption)
	tReq.RequestType = swarmdb.RT_CREATE_DATABASE
	tReq.Owner = owner
	tReq.Database = database

	mReq, _ := json.Marshal(tReq)
	fmt.Printf("Input: %s\n", mReq)
	res, err := swdb.SelectHandler(u, string(mReq))
	if err != nil {
		t.Fatalf("[swarmdb_test:TestCoreTables] CREATE DATABASE: %s", err)
	}
	fmt.Printf("Output: %s\n\n", res.Stringify())

	// create database again with the exact name ==> should fail
	fmt.Printf("Input: %s\n", mReq)
	res, err = swdb.SelectHandler(u, string(mReq))
	if err != nil {
		fmt.Printf("Output: %s\n\n", err)
	} else {
		fmt.Printf("Output: %s\n\n", res.Stringify())
		t.Fatalf("[swarmdb_test:TestCoreTables] CREATE DATABASE again succeeded")
	}

	// list databases
	tReq.RequestType = swarmdb.RT_LIST_DATABASES
	tReq.Owner = owner
	tReq.Database = database
	mReq, _ = json.Marshal(tReq)
	fmt.Printf("Input: %s\n", mReq)
	res, err = swdb.SelectHandler(u, string(mReq))
	if err != nil {
		t.Fatalf("[swarmdb_test:TestCoreTables] LIST DATABASES: %s", err)
	}
	if len(res.Data) != 1 {
		t.Fatalf("[swarmdb_test:TestCoreTables] incorrect number of databases: %d", len(res.Data))
	}
	fmt.Printf("Output: %s \n\n", res.Stringify())

	// list tables
	tReq.RequestType = swarmdb.RT_LIST_TABLES
	tReq.Owner = owner
	tReq.Database = database
	mReq, _ = json.Marshal(tReq)
	fmt.Printf("Input: %s\n", mReq)
	res, err = swdb.SelectHandler(u, string(mReq))
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
	tabletest[0].columnType = swarmdb.CT_STRING
	tabletest[0].indexType = swarmdb.IT_BPLUSTREE
	tabletest[0].sampleValue1 = "gamma"
	tabletest[0].sampleValue2 = "alpha"
	tabletest[0].sampleValue3 = "beta"
	tabletest[0].sampleValue1str = fmt.Sprintf("%s", tabletest[0].sampleValue1)
	tabletest[0].sampleValue2str = fmt.Sprintf("%s", tabletest[0].sampleValue2)
	tabletest[0].sampleValue2str = fmt.Sprintf("%s", tabletest[0].sampleValue3)

	tabletest[1].tableName = make_name("teststrh")
	tabletest[1].primaryColumnName = "sth"
	tabletest[1].columnType = swarmdb.CT_STRING
	tabletest[1].indexType = swarmdb.IT_HASHTREE
	tabletest[1].sampleValue1 = "gamma"
	tabletest[1].sampleValue2 = "alpha"
	tabletest[1].sampleValue3 = "beta"
	tabletest[1].sampleValue1str = fmt.Sprintf("%s", tabletest[1].sampleValue1)
	tabletest[1].sampleValue2str = fmt.Sprintf("%s", tabletest[1].sampleValue2)
	tabletest[1].sampleValue2str = fmt.Sprintf("%s", tabletest[1].sampleValue3)

	tabletest[2].tableName = make_name("testintb")
	tabletest[2].primaryColumnName = "inb"
	tabletest[2].columnType = swarmdb.CT_INTEGER
	tabletest[2].indexType = swarmdb.IT_BPLUSTREE
	tabletest[2].sampleValue1 = 3
	tabletest[2].sampleValue2 = 1
	tabletest[2].sampleValue3 = 2
	tabletest[2].sampleValue1str = fmt.Sprintf("%d", tabletest[2].sampleValue1)
	tabletest[2].sampleValue2str = fmt.Sprintf("%d", tabletest[2].sampleValue2)
	tabletest[2].sampleValue2str = fmt.Sprintf("%d", tabletest[2].sampleValue3)

	tabletest[3].tableName = make_name("testinth")
	tabletest[3].primaryColumnName = "inh"
	tabletest[3].columnType = swarmdb.CT_INTEGER
	tabletest[3].indexType = swarmdb.IT_HASHTREE
	tabletest[3].sampleValue1 = 3
	tabletest[3].sampleValue2 = 1
	tabletest[3].sampleValue3 = 2
	tabletest[3].sampleValue1str = fmt.Sprintf("%d", tabletest[3].sampleValue1)
	tabletest[3].sampleValue2str = fmt.Sprintf("%d", tabletest[3].sampleValue2)
	tabletest[3].sampleValue2str = fmt.Sprintf("%d", tabletest[3].sampleValue3)


	tabletest[4].tableName = make_name("testfltb")
	tabletest[4].primaryColumnName = "flb"
	tabletest[4].columnType = swarmdb.CT_FLOAT
	tabletest[4].indexType = swarmdb.IT_BPLUSTREE
	tabletest[4].sampleValue1 = 3.14
	tabletest[4].sampleValue2 = 1.66
	tabletest[4].sampleValue3 = 2.71
	tabletest[4].sampleValue1str = fmt.Sprintf("%f", tabletest[4].sampleValue1)
	tabletest[4].sampleValue2str = fmt.Sprintf("%f", tabletest[4].sampleValue2)
	tabletest[4].sampleValue2str = fmt.Sprintf("%f", tabletest[4].sampleValue3)

	tabletest[5].tableName = make_name("testflth")
	tabletest[5].primaryColumnName = "flh"
	tabletest[5].columnType = swarmdb.CT_FLOAT
	tabletest[5].indexType = swarmdb.IT_HASHTREE
	tabletest[5].sampleValue1 = 3.14
	tabletest[5].sampleValue2 = 1.66
	tabletest[5].sampleValue3 = 2.71
	tabletest[5].sampleValue1str = fmt.Sprintf("%f", tabletest[5].sampleValue1)
	tabletest[5].sampleValue2str = fmt.Sprintf("%f", tabletest[5].sampleValue2)
	tabletest[5].sampleValue2str = fmt.Sprintf("%f", tabletest[5].sampleValue3)

	tableCountExpected := 0
	for _, tbl := range tabletest {
		tableName := tbl.tableName

		// CREATE TABLE
		var testColumn []swarmdb.Column
		testColumn = make([]swarmdb.Column, 3)
		testColumn[0].ColumnName = tbl.primaryColumnName
		testColumn[0].Primary = 1                      // TODO: test when (a) more than one primary (b) no primary specified
		testColumn[0].IndexType = tbl.indexType
		testColumn[0].ColumnType = tbl.columnType
		
		testColumn[1].ColumnName = "name"
		testColumn[1].Primary = 0                      
		testColumn[1].IndexType = swarmdb.IT_BPLUSTREE 
		testColumn[1].ColumnType = swarmdb.CT_STRING   // TODO: test what happens when value of incorrect type supplied for column
		
		testColumn[2].ColumnName = "age"
		testColumn[2].Primary = 0                      
		testColumn[2].IndexType = swarmdb.IT_BPLUSTREE 
		testColumn[2].ColumnType = swarmdb.CT_INTEGER
		
		tReq = new(swarmdb.RequestOption)
		tReq.RequestType = swarmdb.RT_CREATE_TABLE
		tReq.Owner = owner
		tReq.Database = database
		tReq.Table = tableName
		tReq.Columns = testColumn
		
		mReq, _ = json.Marshal(tReq)
		fmt.Printf("Input: %s\n", mReq)
		res, err = swdb.SelectHandler(u, string(mReq))
		if err != nil {
			t.Fatalf("[swarmdb_test:TestCoreTables] CreateTable: %s", err)
		}
		fmt.Printf("Output: %s\n\n", res.Stringify())

		// CREATE TABLE second time ==> FAIL
		fmt.Printf("Input: %s\n", mReq)
		res, err = swdb.SelectHandler(u, string(mReq))
		if err != nil {
			fmt.Printf("Output: %s\n\n", err)
		} else {
			fmt.Printf("Output: %s\n\n", res)
			t.Fatalf("[swarmdb_test:TestCoreTables] CreateTable2: %s", err)
		}

		// PUT(sampleValue1)
		testKey := tbl.sampleValue1
		tReq = new(swarmdb.RequestOption)
		tReq.RequestType = swarmdb.RT_PUT
		tReq.Owner = owner
		tReq.Database = database
		tReq.Table = tableName
		tReq.Key = testKey
		rowObj := make(swarmdb.Row) 
		rowObj[tbl.primaryColumnName] = testKey
		rowObj["name"] = "Rodney"
		rowObj["age"] = int(37)
		
		tReq.Rows = append(tReq.Rows, rowObj)
		mReq, _ = json.Marshal(tReq)
		fmt.Printf("Input: %s\n", mReq)
		res, err := swdb.SelectHandler(u, string(mReq))
		if err != nil {
			t.Fatalf("[swarmdb_test:TestCoreTables] Put %s", err.Error())
		}
		fmt.Printf("Output: %s\n\n", res.Stringify())
		if res.AffectedRowCount != 1 {
			t.Fatalf("Put affectedRowCount NOT OK")
		}
		
		// GET(sampleValue1)
		tReq = new(swarmdb.RequestOption)
		tReq.RequestType = swarmdb.RT_GET
		tReq.Owner = owner
		tReq.Database = database
		tReq.Table = tableName
		tReq.Key = testKey
		mReq, _ = json.Marshal(tReq)
		fmt.Printf("Input: %s\n", mReq)
		res, err = swdb.SelectHandler(u, string(mReq))
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
				if tbl.columnType == swarmdb.CT_STRING {
					if strings.Compare(d[tbl.primaryColumnName].(string), testKey.(string)) != 0 {
						t.Fatalf("MISMATCH on %s: [%v] != [%v]\n", tbl.primaryColumnName, d[tbl.primaryColumnName], testKey)
					}
				} else if tbl.columnType == swarmdb.CT_INTEGER {
					if d[tbl.primaryColumnName].(int) != testKey.(int) {
						t.Fatalf("MISMATCH on %s: [%d] != [%d]\n", tbl.primaryColumnName, d[tbl.primaryColumnName], testKey)
					}
				} else if tbl.columnType == swarmdb.CT_FLOAT {
					if d[tbl.primaryColumnName].(float64) != testKey.(float64) {
						t.Fatalf("MISMATCH on %s: [%f] != [%f]\n", tbl.primaryColumnName, d[tbl.primaryColumnName], testKey)
					}
				}
			}
		}
		
		// GET(samplevalue2) should return ok = false, but not error
		tReq = new(swarmdb.RequestOption)
		tReq.RequestType = swarmdb.RT_GET
		tReq.Owner = owner
		tReq.Database = database
		tReq.Table = tableName
		tReq.Key = tbl.sampleValue2
		mReq, _ = json.Marshal(tReq)
		fmt.Printf("Input: %s\n", mReq)
		res, err = swdb.SelectHandler(u, string(mReq))
		if err != nil {
			t.Fatalf("[swarmdb_test:TestCoreTables] Get %s", err.Error())
		}
		fmt.Printf("Output: %s\n\n", res.Stringify())
		if len(res.Data) > 0 {
			t.Fatalf("[swarmdb_test:TestCoreTables] Get(samplevalue2) should not be returning data")
		}
		
		// DELETE(samplevalue2) should return ok = false, but not error
		tReq = new(swarmdb.RequestOption)
		tReq.RequestType = swarmdb.RT_DELETE
		tReq.Owner = owner
		tReq.Database = database
		tReq.Table = tableName
		tReq.Key = tbl.sampleValue2
		mReq, _ = json.Marshal(tReq)
		fmt.Printf("Input: %s\n", mReq)
		res, err = swdb.SelectHandler(u, string(mReq))
		if err != nil {
			t.Fatalf("[swarmdb_test:TestCoreTables] Delete %s", err.Error())
		}
		if res.AffectedRowCount > 0 {
			t.Fatalf("[swarmdb_test:TestCoreTables] Delete has incorrect affectedRowCount %d", res.AffectedRowCount)
		}
		fmt.Printf("Output: %s\n\n", res.Stringify())
			
		// INSERT(sampleValue2) QUERY
		tReq = new(swarmdb.RequestOption)
		queryInsert := fmt.Sprintf("insert into %s (%s, name, age) values ('%s', 'randomname', '99')", tableName, tbl.primaryColumnName, tbl.sampleValue2str)
		tReq.RequestType = swarmdb.RT_QUERY
		tReq.Owner = owner
		tReq.Database = database
		tReq.Table = tableName
		tReq.RawQuery = queryInsert
		mReq, _ = json.Marshal(tReq)
		fmt.Printf("Input: %s\n", mReq)
		res, err = swdb.SelectHandler(u, string(mReq))
		if err != nil {
			t.Fatalf("[swarmdb_test:TestCoreTables] Insert %s", err.Error())
		}
		fmt.Printf("Output: %s\n\n", res.Stringify())
		if res.AffectedRowCount != 1 {
			t.Fatalf("[swarmdb_test:TestCoreTables] Insert affected row count has incorrect affectedRowCount %d", res.AffectedRowCount)
		}
		
		// SELECT(sampleValue2) ==> 1
		tReq = new(swarmdb.RequestOption)
		querySelect := fmt.Sprintf("select %s, name, age from %s where %s = '%s'", tbl.primaryColumnName, tableName, tbl.primaryColumnName, tbl.sampleValue2str)
		tReq.RequestType = swarmdb.RT_QUERY
		tReq.Owner = owner
		tReq.Database = database
		tReq.Table = tableName
		tReq.RawQuery = querySelect
		mReq, _ = json.Marshal(tReq)
		fmt.Printf("Input: %s\n", mReq)
		res, err = swdb.SelectHandler(u, string(mReq))
		if err != nil {
			t.Fatalf("[swarmdb_test:TestCoreTables] Select(samplevalue2) %s", err.Error())
		}
		fmt.Printf("Output: %s\n\n", res.Stringify())
		if len(res.Data) != 1 {
			t.Fatalf("[swarmdb_test:TestCoreTables] Select(samplevalue2) incorrect # of rows in output %d", len(res.Data))
		}

		// SELECT AND ==> 2 rows
		tReq = new(swarmdb.RequestOption)
		querySelect = fmt.Sprintf("select %s, name, age from %s where %s = \"%s\" AND age = 99", tbl.primaryColumnName, tableName, tbl.primaryColumnName, tbl.sampleValue2str)
		tReq.RequestType = swarmdb.RT_QUERY
		tReq.Owner = owner
		tReq.Database = database
		tReq.Table = tableName
		tReq.RawQuery = querySelect
		mReq, _ = json.Marshal(tReq)
		fmt.Printf("Input: %s\n", mReq)
		res, err = swdb.SelectHandler(u, string(mReq))
		if err != nil {
			// t.Fatalf("[swarmdb_test:TestCoreTables] Select(*) %s", err.Error())
			// TODO: FIX THIS -- [swarmdb_test:TestCoreTables] Select(*) [swarmdb:SelectHandler] Query col [(stb = 'alpha')] does not exist in table
		}
		fmt.Printf("Output: %s\n\n", res.Stringify())
		if len(res.Data) != 1 {
			// t.Fatalf("[swarmdb_test:TestCoreTables] Select AND has incorrect # of rows in output %d (should be 1)", len(res.Data))
		}

		// SELECT OR ==> 2 rows
		tReq = new(swarmdb.RequestOption)
		querySelect = fmt.Sprintf("select %s, name, age from %s where %s = '%s' OR %s = '%s'", tbl.primaryColumnName, tableName, tbl.primaryColumnName, tbl.sampleValue1str, tbl.primaryColumnName, tbl.sampleValue2str)
		tReq.RequestType = swarmdb.RT_QUERY
		tReq.Owner = owner
		tReq.Database = database
		tReq.Table = tableName
		tReq.RawQuery = querySelect
		mReq, _ = json.Marshal(tReq)
		fmt.Printf("Input: %s\n", mReq)
		res, err = swdb.SelectHandler(u, string(mReq))
		if err != nil {
			// t.Fatalf("[swarmdb_test:TestCoreTables] Select(*) %s", err.Error())
			// TODO: FIX THIS -- Select(*) [swarmdb:SelectHandler] Query col [(stb = 'gamma')] does not exist in table
		}
		fmt.Printf("Output: %s\n\n", res.Stringify())
		if len(res.Data) != 2 {
			// t.Fatalf("[swarmdb_test:TestCoreTables] Select OR has incorrect # of rows in output %d (should be 1)", len(res.Data))
		}

		// SELECT(sampleValue3) ==> 0 rows
		tReq = new(swarmdb.RequestOption)
		querySelect = fmt.Sprintf("select %s, name, age from %s where %s = '%s'", tbl.primaryColumnName, tableName, tbl.primaryColumnName, tbl.sampleValue3str)
		tReq.RequestType = swarmdb.RT_QUERY
		tReq.Owner = owner
		tReq.Database = database
		tReq.Table = tableName
		tReq.RawQuery = querySelect
		mReq, _ = json.Marshal(tReq)
		fmt.Printf("Input: %s\n", mReq)
		res, err = swdb.SelectHandler(u, string(mReq))
		if err != nil {
			t.Fatalf("[swarmdb_test:TestCoreTables] Select(samplevalue3) %s", err.Error())
		}
		fmt.Printf("Output: %s\n\n", res.Stringify())
		if len(res.Data) > 0 {
			t.Fatalf("[swarmdb_test:TestCoreTables] Select(samplevalue3) has incorrect # of rows in output %d (should be 0)", len(res.Data))
		}
		tableCountExpected = tableCountExpected + 1
	}

	// TODO: list tables, then drop 1 for each for the tables
	for _, tbl := range tabletest {
		// list tables should have 6 tables, then 5, .. then 4, ... until just 1
		tReq = new(swarmdb.RequestOption)
		tReq.RequestType = swarmdb.RT_LIST_TABLES
		tReq.Owner = owner
		tReq.Database = database
		tReq.Table = ""
		mReq, _ = json.Marshal(tReq)
		fmt.Printf("Input: %s\n", mReq)
		res, err = swdb.SelectHandler(u, string(mReq))
		if err != nil {
			t.Fatalf("SCAN error: %s", err)
		}
		fmt.Printf("Output: %s\n\n", res.Stringify())
		if len(res.Data) != tableCountExpected {
			t.Fatalf("[swarmdb_test:TestCoreTables] List Tables count error -- expected %d, but got %d", tableCountExpected, len(res.Data))
		}

		// drop table
		tReq.RequestType = swarmdb.RT_DROP_TABLE
		tReq.Owner = owner
		tReq.Database = database
		tReq.Table = tbl.tableName
		mReq, _ = json.Marshal(tReq)
		fmt.Printf("Input: %s\n", mReq)
		res, err = swdb.SelectHandler(u, string(mReq))
		if err != nil {
			t.Fatal(err)
		}
		if res.AffectedRowCount != 1 {
			t.Fatalf("[swarmdb_test:TestCoreTables] Insert affected row count has incorrect affectedRowCount %d", res.AffectedRowCount)
		}
		fmt.Printf("Output: %s\n\n", res.Stringify())
		tableCountExpected = tableCountExpected - 1
	}

	// drop database
	tReq = new(swarmdb.RequestOption)
	tReq.RequestType = swarmdb.RT_DROP_DATABASE
	tReq.Owner = owner
	tReq.Database = database
	mReq, _ = json.Marshal(tReq)
	fmt.Printf("Input: %s\n", mReq)
	res, err = swdb.SelectHandler(u, string(mReq))
	if err != nil {
		t.Fatal(err)
	}
	fmt.Printf("Output: %s\n\n", res.Stringify())
	if res.AffectedRowCount != 1 {
		t.Fatalf("[swarmdb_test:TestCoreTables] DROP DATABASE has incorrect affectedRowCount %d", res.AffectedRowCount)
	}

	// list databases
	tReq = new(swarmdb.RequestOption)
	tReq.RequestType = swarmdb.RT_LIST_DATABASES
	tReq.Owner = owner
	mReq, _ = json.Marshal(tReq)
	fmt.Printf("Input: %s\n", mReq)
	res, err = swdb.SelectHandler(u, string(mReq))
	if err != nil {
		t.Fatalf("error marshaling tReq 2: %s", err)
	}
	fmt.Printf("Output: %s\n\n", res.Stringify())
	if len(res.Data) > 0 {	
		t.Fatalf("[swarmdb_test:TestCoreTables] LIST DATABASES has incorrect outputs")
	}
	if res.AffectedRowCount > 0 {
		t.Fatalf("[swarmdb_test:TestCoreTables] LIST DATABASES has incorrect affectedRowCount %d", res.AffectedRowCount)
	}
}

func zTestGetTableFail(t *testing.T) {
	/*config, _ := swarmdb.LoadSWARMDBConfig(swarmdb.SWARMDBCONF_FILE)
	ensdbPath := TEST_ENS_DIR
	swdb, _ := swarmdb.NewSwarmDB(ensdbPath, config.ChunkDBPath)
	owner := "BadOwner"
	database := "BadDatabase"
	tableName := "BadTable"
	u := getUser()
*/
	// TODO: test what happens when there is:
	// (1) a bad owner with LIST DATABASES
	// (2) a valid owner, but bad database with LIST TABLES
	// (3) a valid owner and database, but invalid tableName with DESCRIBE TABLE

	// TODO: test what happens when there is a valid owner/database/tableName encrypted, and someone else does 
	// (1) a LIST DATABASES of the owner
	// (2) a LIST TABLES of the database
	// (3) 
}

func aTestPrimaryMedium(t *testing.T) {
	t.SkipNow()

	
	// TODO: insert 100 row inserts for {integer, string, float}
	// SELECT a random row

	// TODO: select the first 100 rows
	// sql := fmt.Sprintf("select * from %s where %s < 10", TEST_TABLE, TEST_SKEY_FLOAT)
	// check for 100 rows in output

	// TODO: scan all

	// repeat this 10 times

	// TODO: now delete 100 rows
}


func bTestSecondaryMedium(t *testing.T) {
}

