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
	"math/rand"
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

func getSWARMDBTable(u *swarmdb.SWARMDBUser, tableName string, primaryKeyName string, primaryIndexType swarmdb.IndexType, primaryColumnType swarmdb.ColumnType, create bool) (tbl *swarmdb.Table) {
	config, _ := swarmdb.LoadSWARMDBConfig(swarmdb.SWARMDBCONF_FILE)
	ensdbPath := TEST_ENS_DIR
	swarmdbObj, err := swarmdb.NewSwarmDB(ensdbPath, config.ChunkDBPath)
	if err != nil {
		panic("Could not create NewSWARMDB")
	}

	owner := TEST_OWNER
	database := TEST_DATABASE

	// CreateTable
	if create {
		var option []swarmdb.Column
		o := swarmdb.Column{ColumnName: primaryKeyName, Primary: 1, IndexType: primaryIndexType, ColumnType: primaryColumnType}
		option = append(option, o)
		tbl, _ = swarmdbObj.CreateTable(u, owner, database, tableName, option)

		// OpenTable
		err = tbl.OpenTable(u)
		if err != nil {
			fmt.Print("OPENTABLE ERR %v\n", err)
		}
		return tbl
	} else {
		tbl = swarmdbObj.NewTable(owner, database, tableName)
		err = tbl.OpenTable(u)
		if err != nil {
			panic("Could not open table")
		}
		return tbl
	}
}

func getSWARMDBTableSecondary(u *swarmdb.SWARMDBUser, tableName string, primaryKeyName string, primaryIndexType swarmdb.IndexType, primaryColumnType swarmdb.ColumnType,
	secondaryKeyName string, secondaryIndexType swarmdb.IndexType, secondaryColumnType swarmdb.ColumnType,
	create bool) (swarmdbObj *swarmdb.SwarmDB, err error) {

	config, _ := swarmdb.LoadSWARMDBConfig(swarmdb.SWARMDBCONF_FILE)
	ensdbPath := TEST_ENS_DIR
	swarmdbObj, _ = swarmdb.NewSwarmDB(ensdbPath, config.ChunkDBPath)

	owner := TEST_OWNER
	database := TEST_DATABASE

	// CreateTable
	var swErr swarmdb.SWARMDBError
	if create {
		var option []swarmdb.Column
		o := swarmdb.Column{ColumnName: primaryKeyName, Primary: 1, IndexType: primaryIndexType, ColumnType: primaryColumnType}
		option = append(option, o)

		s := swarmdb.Column{ColumnName: secondaryKeyName, Primary: 0, IndexType: secondaryIndexType, ColumnType: secondaryColumnType}
		option = append(option, s)
		tbl, errTblCreate := swarmdbObj.CreateTable(u, owner, database, tableName, option)
		if errTblCreate != nil {
			swErr.SetError("Error: [%s] " + errTblCreate.Error())
		}

		// OpenTable
		err := tbl.OpenTable(u)
		if err != nil {
			fmt.Printf("OPENTABLE ERR %v\n", err)
			swErr.SetError("OPENTTABLE Error: [%s] " + err.Error())
		}

		putstr := `{"email":"rodney@wolk.com", "age": 38, "gender": "M", "weight": 172.5}`
		var putjson map[string]interface{}
		_ = json.Unmarshal([]byte(putstr), putjson)
		tbl.Put(u, putjson)

		putstr = `{"email":"sourabh@wolk.com", "age": 45, "gender": "M", "weight": 210.5}`
		_ = json.Unmarshal([]byte(putstr), putjson)
		tbl.Put(u, putjson)

		// Put
		for i := 1; i < 10; i++ {
			g := "F"
			w := float64(i) + .314159
			putstr = fmt.Sprintf(`{"%s":"test%03d@wolk.com", "age": %d, "gender": "%s", "weight": %f}`,
				TEST_PKEY_STRING, i*2, i%5+21, g, w)
			_ = json.Unmarshal([]byte(putstr), putjson)
			tbl.Put(u, putjson)

			g = "M"
			w = float64(i) + float64(0.414159)
			putstr = fmt.Sprintf(`{"%s":"test%03d@wolk.com", "age": %d, "gender": "%s", "weight": %f}`,
				TEST_PKEY_STRING, i*2+1, i%5+21, g, w)
			_ = json.Unmarshal([]byte(putstr), putjson)
			tbl.Put(u, putjson)

		}
	} else {
		tbl, _ := swarmdbObj.GetTable(u, owner, database, tableName)
		err := tbl.OpenTable(u)
		if err != nil {
			fmt.Printf("OPENTABLE ERR %v\n", err)
		}
	}
	return swarmdbObj, nil
}

func TestCreateListDropDatabase(t *testing.T) {
	u := getUser()
	owner := make_name("owner")
	database := make_name("db")

	config, _ := swarmdb.LoadSWARMDBConfig(swarmdb.SWARMDBCONF_FILE)
	ensdbPath := TEST_ENS_DIR
	swdb, _ := swarmdb.NewSwarmDB(ensdbPath, config.ChunkDBPath)

	// create database
	var testReqOption swarmdb.RequestOption
	testReqOption.RequestType = swarmdb.RT_CREATE_DATABASE
	testReqOption.Owner = owner
	testReqOption.Database = database

	marshalTestReqOption, err := json.Marshal(testReqOption)
	fmt.Printf("Input: %s\n", marshalTestReqOption)
	if err != nil {
		t.Fatalf("error marshaling testReqOption: %s", err)

	}
	res, err := swdb.SelectHandler(u, string(marshalTestReqOption))
	fmt.Printf("Output: %s\n\n", res.Stringify())

	// list databases
	testReqOption.RequestType = swarmdb.RT_LIST_DATABASES
	testReqOption.Owner = owner
	testReqOption.Database = database
	marshalTestReqOption, err = json.Marshal(testReqOption)
	fmt.Printf("Input: %s\n", marshalTestReqOption)
	if err != nil {
		t.Fatalf("error marshaling testReqOption 1: %s", err)
	}
	res, err = swdb.SelectHandler(u, string(marshalTestReqOption))
	if err != nil {
		t.Fatalf("error marshaling testReqOption 2: %s", err)
	}
	fmt.Printf("Output: %s\n\n", res.Stringify())

	// list tables
	testReqOption.RequestType = swarmdb.RT_LIST_TABLES
	testReqOption.Owner = owner
	testReqOption.Database = database

	marshalTestReqOption, err = json.Marshal(testReqOption)
	fmt.Printf("Input: %s\n", marshalTestReqOption)
	if err != nil {
		t.Fatalf("error marshaling testReqOption: %s", err)

	}
	res, err = swdb.SelectHandler(u, string(marshalTestReqOption))
	if err != nil {
		t.Fatalf("error marshaling testReqOption 2: %s", err)
	}
	fmt.Printf("Output: %s\n\n", res.Stringify())

	tableName := make_name("test")

	var testColumn []swarmdb.Column
	testColumn = make([]swarmdb.Column, 3)
	testColumn[0].ColumnName = "email"
	testColumn[0].Primary = 1                      // What if this is inconsistent?
	testColumn[0].IndexType = swarmdb.IT_BPLUSTREE //  What if this is inconsistent?
	testColumn[0].ColumnType = swarmdb.CT_STRING

	testColumn[1].ColumnName = "name"
	testColumn[1].Primary = 0                      // What if this is inconsistent?
	testColumn[1].IndexType = swarmdb.IT_BPLUSTREE //  What if this is inconsistent?
	testColumn[1].ColumnType = swarmdb.CT_STRING

	testColumn[2].ColumnName = "age"
	testColumn[2].Primary = 0                      // What if this is inconsistent?
	testColumn[2].IndexType = swarmdb.IT_BPLUSTREE //  What if this is inconsistent?
	testColumn[2].ColumnType = swarmdb.CT_INTEGER

	testReqOption.RequestType = swarmdb.RT_CREATE_TABLE
	testReqOption.Owner = owner
	testReqOption.Database = database
	testReqOption.Table = tableName
	testReqOption.Columns = testColumn

	marshalTestReqOption, err = json.Marshal(testReqOption)
	fmt.Printf("Input: %s\n", marshalTestReqOption)
	if err != nil {
		t.Fatalf("error marshaling testReqOption: %s", err)

	}
	res, err = swdb.SelectHandler(u, string(marshalTestReqOption))
	if err != nil {
		t.Fatalf("error marshaling testReqOption 2: %s", err)
	}
	fmt.Printf("Output: %s\n\n", res.Stringify())

	// list tables
	testReqOption.RequestType = swarmdb.RT_LIST_TABLES
	testReqOption.Owner = owner
	testReqOption.Database = database

	marshalTestReqOption, err = json.Marshal(testReqOption)
	fmt.Printf("Input: %s\n", marshalTestReqOption)
	if err != nil {
		t.Fatalf("error marshaling testReqOption: %s", err)

	}
	res, err = swdb.SelectHandler(u, string(marshalTestReqOption))
	if err != nil {
		t.Fatalf("error marshaling testReqOption 2: %s", err)
	}
	fmt.Printf("Output: %s\n\n", res.Stringify())

	testKey := "rodneytest1@wolk.com"

	testReqOption.RequestType = swarmdb.RT_PUT
	testReqOption.Owner = owner
	testReqOption.Database = database
	testReqOption.Table = tableName
	testReqOption.Key = testKey
	rowObj := make(swarmdb.Row) // map[string]interface{}
	rowObj["name"] = "Rodney"
	rowObj["age"] = int(37)
	rowObj["email"] = testKey

	testReqOption.Rows = append(testReqOption.Rows, rowObj)
	marshalTestReqOption, err = json.Marshal(testReqOption)
	fmt.Printf("Input: %s\n", marshalTestReqOption)
	if err != nil {
		t.Fatalf("error marshaling testReqOption: %s", err)
	}

	resp, errS := swdb.SelectHandler(u, string(marshalTestReqOption))
	if errS != nil {
		t.Fatalf("[swarmdb_test:TestPut] SelectHandler %s", errS.Error())
	}
	fmt.Printf("Output: %s\n\n", resp.Stringify())
	if resp.AffectedRowCount != 1 {
		t.Fatal("NOT OK")
	} else {
		fmt.Printf("PASS\n")
	}

	testReqOption.RequestType = swarmdb.RT_GET
	testReqOption.Owner = owner
	testReqOption.Database = database
	testReqOption.Table = tableName

	testReqOption.Key = testKey

	marshalTestReqOption, err = json.Marshal(testReqOption)
	fmt.Printf("Input: %s\n", marshalTestReqOption)
	if err != nil {
		t.Fatalf("error marshaling testReqOption: %s", err)
	}

	resp, err = swdb.SelectHandler(u, string(marshalTestReqOption))
	if err != nil {
		t.Fatal(err)
	}
	fmt.Printf("Output: %s\n\n", resp.Stringify())

	/*rowObj = make(map[string]interface{})
	err = json.Unmarshal([]byte(resp), &rowObj)
	if err != nil {
		t.Fatal("error parsing response")
	} else {
		if strings.Compare(fmt.Sprintf("%v", rowObj["age"]), "37") != 0 {
			fmt.Printf("MISMATCH: [%v]\n", rowObj["age"])
		} else if strings.Compare(rowObj["name"].(string), "Rodney") != 0 {
			fmt.Printf("MISMATCH: [%v]\n", rowObj["name"])
		} else if strings.Compare(rowObj["email"].(string), testKey) != 0 {
			fmt.Printf("MISMATCH email: [%v]\n", rowObj["email"])
		} else {
			fmt.Printf("PASS\n")
		}
	}*/

	// drop table
	testReqOption.RequestType = swarmdb.RT_DROP_TABLE
	testReqOption.Owner = owner
	testReqOption.Database = database
	testReqOption.Table = tableName
	marshalTestReqOption, err = json.Marshal(testReqOption)
	fmt.Printf("Input: %s\n", marshalTestReqOption)
	if err != nil {
		t.Fatalf("error marshaling testReqOption: %s", err)

	}
	res, err = swdb.SelectHandler(u, string(marshalTestReqOption))
	if err != nil {
		t.Fatal(err)
	}
	fmt.Printf("Output: %s\n\n", res.Stringify())

	// list tables
	testReqOption.RequestType = swarmdb.RT_LIST_TABLES
	testReqOption.Owner = owner
	testReqOption.Database = database

	marshalTestReqOption, err = json.Marshal(testReqOption)
	if err != nil {
		t.Fatalf("error marshaling testReqOption: %s", err)

	}
	fmt.Printf("Input: %s\n", marshalTestReqOption)
	res, err = swdb.SelectHandler(u, string(marshalTestReqOption))
	if err != nil {
		t.Fatalf("error marshaling testReqOption 2: %s", err)
	}
	fmt.Printf("Output: %s\n\n", res.Stringify())

	// drop database
	testReqOption.RequestType = swarmdb.RT_DROP_DATABASE
	testReqOption.Owner = owner
	testReqOption.Database = database
	marshalTestReqOption, err = json.Marshal(testReqOption)

	if err != nil {
		t.Fatalf("error marshaling testReqOption: %s", err)
	}
	fmt.Printf("Input: %s\n", marshalTestReqOption)
	resp, err = swdb.SelectHandler(u, string(marshalTestReqOption))
	if err != nil {
		t.Fatal(err)
	}
	fmt.Printf("Output: %s\n\n", resp.Stringify())

	// list databases
	testReqOption.RequestType = swarmdb.RT_LIST_DATABASES
	testReqOption.Owner = owner
	testReqOption.Database = database
	marshalTestReqOption, err = json.Marshal(testReqOption)
	if err != nil {
		t.Fatalf("error marshaling testReqOption 1: %s", err)
	}
	fmt.Printf("Input: %s\n", marshalTestReqOption)
	res, err = swdb.SelectHandler(u, string(marshalTestReqOption))
	if err != nil {
		t.Fatalf("error marshaling testReqOption 2: %s", err)
	}
	fmt.Printf("Output: %s\n\n", res.Stringify())
}

func zTestGetTableFail(t *testing.T) {
	config, _ := swarmdb.LoadSWARMDBConfig(swarmdb.SWARMDBCONF_FILE)
	ensdbPath := TEST_ENS_DIR
	swdb, _ := swarmdb.NewSwarmDB(ensdbPath, config.ChunkDBPath)
	owner := "BadOwner"
	database := "BadDatabase"
	tableName := "BadTable"
	u := getUser()
	_, err := swdb.GetTable(u, owner, database, tableName)
	if err == nil {
		t.Fatalf("TestGetTableFail: FAILED")
	} else {
		fmt.Printf("PASS\n")
	}
}

// primary key is integer
func zTestPutInteger(t *testing.T) {
	fmt.Printf("---- TestPutInteger: generate 20 ints and enumerate them\n")
	u := getUser()

	owner := make_name("owner")
	database := make_name("db")

	config, _ := swarmdb.LoadSWARMDBConfig(swarmdb.SWARMDBCONF_FILE)
	ensdbPath := TEST_ENS_DIR
	swdb, _ := swarmdb.NewSwarmDB(ensdbPath, config.ChunkDBPath)

	// set up table
	tableName := make_name("testputinteger")
	var testColumn []swarmdb.Column
	testColumn = make([]swarmdb.Column, 2)
	testColumn[0].ColumnName = TEST_PKEY_INT
	testColumn[0].Primary = 1                      // What if this is inconsistent?
	testColumn[0].IndexType = swarmdb.IT_BPLUSTREE //  What if this is inconsistent?
	testColumn[0].ColumnType = swarmdb.CT_INTEGER

	testColumn[1].ColumnName = "email"
	testColumn[1].Primary = 0                      // What if this is inconsistent?
	testColumn[1].IndexType = swarmdb.IT_BPLUSTREE //  What if this is inconsistent?
	testColumn[1].ColumnType = swarmdb.CT_STRING

	var testReqOption swarmdb.RequestOption
	testReqOption.RequestType = swarmdb.RT_CREATE_TABLE
	testReqOption.Owner = owner
	testReqOption.Database = database
	testReqOption.Table = tableName
	testReqOption.Columns = testColumn

	// create table
	marshalTestReqOption, err := json.Marshal(testReqOption)
	fmt.Printf("Input: %s", marshalTestReqOption)
	if err != nil {
		t.Fatalf("error marshaling testReqOption: %s", err)

	} else {
		resp, err := swdb.SelectHandler(u, string(marshalTestReqOption))
		if err != nil {
			t.Fatalf("[swarm_test:TestPutInteger] SelectHandler %s", err.Error())
		}
		fmt.Printf("Output: %s\n", resp.Stringify())
	}

	// write 20 values into B-tree (only kept in memory)
	vals := rand.Perm(20)
	for _, i := range vals {
		rowObj := make(map[string]interface{})
		rowObj[TEST_PKEY_INT] = i
		rowObj["email"] = fmt.Sprintf("test%03d@wolk.com", i)

		var testReqOption swarmdb.RequestOption
		testReqOption.RequestType = swarmdb.RT_PUT
		testReqOption.Owner = owner
		testReqOption.Database = database
		testReqOption.Table = tableName
		testReqOption.Rows = append(testReqOption.Rows, rowObj)
		marshalTestReqOption, err := json.Marshal(testReqOption)
		fmt.Printf("Input: %s\n", marshalTestReqOption)
		resp, err := swdb.SelectHandler(u, string(marshalTestReqOption))
		if err != nil {
			t.Fatalf("[swarm_test:TestPutInteger] SelectHandler %s", err.Error())
		} else {
			fmt.Printf("Output: %s\n", resp.Stringify())
		}
	}

	// Get(8)
	var testReqOptionGet swarmdb.RequestOption
	testReqOptionGet.RequestType = swarmdb.RT_GET
	testReqOptionGet.Owner = owner
	testReqOptionGet.Database = database
	testReqOptionGet.Table = tableName
	testReqOptionGet.Key = 8

	marshalTestReqOption, err1 := json.Marshal(testReqOptionGet)
	if err1 != nil {
		t.Fatalf("[swarmdb_test:TestPutInteger] Marshal %s", err1.Error())
	} else {
		fmt.Printf("Input: %s\n", marshalTestReqOption)
	}

	resp, err2 := swdb.SelectHandler(u, string(marshalTestReqOption))
	if err2 != nil {
		t.Fatalf("[swarmdb_test:TestPutInteger] SelectHandler %s", err2.Error())
	} else {
		fmt.Printf("Output: %s\n", resp.Stringify())
	}
	/*
		rowObj := make(map[string]interface{})
		err = json.Unmarshal([]byte(resp), &rowObj)
		if err != nil {
			t.Fatalf("[swarmdb_test:TestPutInteger] Unmarshal %s", err.Error())
		} else {
			if strings.Compare(rowObj["email"].(string), "test008@wolk.com") != 0 {
				fmt.Printf("MISMATCH email: [%v]\n", rowObj["email"])
			} else {
				fmt.Printf("PASS\n")
			}
		}
	*/
}

func aTestSetGetInt(t *testing.T) {
	t.SkipNow()
	/*
		const N = 4
		u := getUser()

		for _, x := range []int{0, -1, 0x555555, 0xaaaaaa, 0x333333, 0xcccccc, 0x314159} {
			r := getSWARMDBTable(u, TEST_TABLE, TEST_PKEY_INT, TEST_TABLE_INDEXTYPE, swarmdb.CT_INTEGER, true)

			a := make([]int, N)
			for i := range a {
				a[i] = (i ^ x) << 1
			}

			for _, k := range a {
				val := fmt.Sprintf(`{"%s":"%d", "value":"%d"}`, TEST_PKEY_INT, k, k^x)
				fmt.Printf("%s\n", val)
				var putjson map[string]interface{}
				_ = json.Unmarshal([]byte(val), putjson)
				r.Put(u, putjson)
			}

			s := getSWARMDBTable(u, TEST_TABLE, TEST_PKEY_INT, TEST_TABLE_INDEXTYPE, swarmdb.CT_INTEGER, false)
			for i, k := range a {
				key := fmt.Sprintf("%d", k) // swarmdb.IntToByte(k)
				val := fmt.Sprintf(`{"%s":"%d", "value":"%d"}`, TEST_PKEY_INT, k, k^x)
				v, err := s.Get(u, key)
				if err != nil || strings.Compare(val, string(v)) != 0 {
					t.Fatal(i, val, v)
				} else {
					fmt.Printf("Get(%s) => %s\n", key, val)
				}

				k |= 1
				key = fmt.Sprintf("%d", k) // swarmdb.IntToByte(k)
				v, err = s.Get(u, key)
				if len(v) > 0 {
					t.Fatal(i, k)
				}
			}

			r2 := getSWARMDBTable(u, TEST_TABLE, TEST_PKEY_INT, TEST_TABLE_INDEXTYPE, swarmdb.CT_INTEGER, false)
			for _, k := range a {
				val := fmt.Sprintf(`{"%s":"%d", "value":"%d"}`, TEST_PKEY_INT, k, k^x+1)
				var putjson map[string]interface{}
				_ = json.Unmarshal([]byte(val), putjson)
				r2.Put(u, putjson)
			}

			s2 := getSWARMDBTable(u, TEST_TABLE, TEST_PKEY_INT, TEST_TABLE_INDEXTYPE, swarmdb.CT_INTEGER, false)
			for i, k := range a {
				key := fmt.Sprintf("%d", k)
				val := fmt.Sprintf(`{"%s":"%d", "value":"%d"}`, TEST_PKEY_INT, k, k^x+1)
				v, err := s2.Get(u, key) //
				if err != nil || strings.Compare(string(v), val) != 0 {
					t.Fatal(i, v, val)
				} else {
					fmt.Printf("Get(%s) => %s\n", key, val)
				}
			}
		}
	*/
}

func aTestTable(t *testing.T) {
	t.SkipNow()
	/*
		u := getUser()
		tbl := getSWARMDBTable(u, TEST_TABLE, TEST_PKEY_STRING, TEST_TABLE_INDEXTYPE, swarmdb.CT_STRING, true)

		putstr := `{"email":"rodney@wolk.com", "age": 38, "gender": "M", "weight": 172.5}`
		var putjson map[string]interface{}
		_ = json.Unmarshal([]byte(putstr), putjson)
		tbl.Put(u, putjson)

		putstr = `{"email":"sourabh@wolk.com", "age": 45, "gender": "M", "weight": 210.5}`
		_ = json.Unmarshal([]byte(putstr), putjson)
		tbl.Put(u, putjson)

		// Put
		for i := 1; i < 100; i++ {
			g := "F"
			w := float64(i) + .314159
			putstr = fmt.Sprintf(`{"%s":"test%03d@wolk.com", "age": %d, "gender": "%s", "weight": %f}`,
				TEST_PKEY_STRING, i, i, g, w)
			_ = json.Unmarshal([]byte(putstr), putjson)
			tbl.Put(u, putjson)

			g = "M"
			w = float64(i) + float64(0.414159)
			putstr = fmt.Sprintf(`{"%s":"test%03d@wolk.com", "age": %d, "gender": "%s", "weight": %f}`,
				TEST_PKEY_STRING, i, i, g, w)
			_ = json.Unmarshal([]byte(putstr), putjson)
			tbl.Put(u, putjson)

		}

		tbl2 := getSWARMDBTable(u, TEST_TABLE, TEST_PKEY_STRING, TEST_TABLE_INDEXTYPE, swarmdb.CT_STRING, false)
		// Get
		res, err := tbl2.Get(u, "rodney@wolk.com")
		fmt.Printf("Get %s %v \n", string(res), err)

		// Get
		fres, ferr := tbl2.Get(u, "test010@wolk.com")
		fmt.Printf("Get %s %v \n", string(fres), ferr)
		//t.CloseTable()
	*/

}

func bTestTableSecondaryInt(t *testing.T) {
	u := getUser()
	swarmdb, _ := getSWARMDBTableSecondary(u, TEST_TABLE, TEST_PKEY_STRING, TEST_TABLE_INDEXTYPE, swarmdb.CT_STRING,
		TEST_SKEY_INT, TEST_TABLE_INDEXTYPE, swarmdb.CT_INTEGER, true)
	owner := make_name("owner")
	database := make_name("db")
	tableName := make_name("tbl")

	rows, err := swarmdb.Scan(u, owner, database, tableName, "age", 1)
	if err != nil {
		t.Fatal(err)
	}
	for i, r := range rows {
		fmt.Printf("%v:%v\n", i, r)
	}

	// select * from table where age < 30
	/*	sql := fmt.Sprintf("select * from %s where %s < 30", TEST_TABLE, TEST_SKEY_INT)
		 rows, err := swarmdb.QuerySelect(u, sql)
			if err != nil {
			} else {
				for i, row := range rows {
					fmt.Printf("%d:%v\n", i, row)
				}
			} */
}

func bTestTableSecondaryFloat(t *testing.T) {
	t.SkipNow()
	owner := make_name("owner")
	database := make_name("db")

	u := getUser()
	swdb, _ := getSWARMDBTableSecondary(u, TEST_TABLE, TEST_PKEY_STRING, TEST_TABLE_INDEXTYPE, swarmdb.CT_STRING,
		TEST_SKEY_FLOAT, TEST_TABLE_INDEXTYPE, swarmdb.CT_FLOAT, true)
	// select * from table where age < 30
	sql := fmt.Sprintf("select * from %s where %s < 10", TEST_TABLE, TEST_SKEY_FLOAT)

	query, err := swarmdb.ParseQuery(sql)
	if err != nil {
		t.Fatal(err)
	}
	query.Owner = owner
	query.Database = database

	rows, err := swdb.QuerySelect(u, &query)
	if err != nil {
		t.Fatal(err)
	} else {
		for i, row := range rows {
			fmt.Printf("%d:%v\n", i, row)
		}
	}
}

func bTestTableSecondaryString(t *testing.T) {
	t.SkipNow()
	owner := make_name("owner")
	database := make_name("db")

	u := getUser()
	swdb, _ := getSWARMDBTableSecondary(u, TEST_TABLE, TEST_PKEY_STRING, TEST_TABLE_INDEXTYPE, swarmdb.CT_STRING,
		TEST_SKEY_STRING, TEST_TABLE_INDEXTYPE, swarmdb.CT_STRING, true)
	sql := fmt.Sprintf("select * from %s where %s < 10", TEST_TABLE, TEST_SKEY_STRING)

	query, err := swarmdb.ParseQuery(sql)
	if err != nil {
		t.Fatal(err)
	}
	query.Owner = owner
	query.Database = database

	rows, err := swdb.QuerySelect(u, &query)
	if err != nil {
		t.Fatal(err)
	} else {
		for i, row := range rows {
			fmt.Printf("%d:%v\n", i, row)
		}
	}
}

func aTestDelete0(t *testing.T) {

	u := getUser()
	r := getSWARMDBTable(u, TEST_TABLE, TEST_PKEY_INT, TEST_TABLE_INDEXTYPE, swarmdb.CT_INTEGER, true)

	key0 := "0"
	key1 := "1"

	val0 := fmt.Sprintf(`{"accountID":"%s","val":"%s"}`, key0, key0)
	val1 := fmt.Sprintf(`{"accountID":"%s","val":"%s"}`, key1, key1)
	if ok, _ := r.Delete(u, key0); ok {
		t.Fatal(ok)
	}

	var putjson map[string]interface{}

	if ok, _ := r.Delete(u, key1); ok {
		t.Fatal(ok)
	}

	if ok, _ := r.Delete(u, key0); !ok {
		t.Fatal(ok)
	}

	if ok, _ := r.Delete(u, key0); ok {
		t.Fatal(ok)
	}

	_ = json.Unmarshal([]byte(val0), putjson)
	r.Put(u, putjson)
	_ = json.Unmarshal([]byte(val1), putjson)
	r.Put(u, putjson)

	if ok, _ := r.Delete(u, key1); !ok {
		t.Fatal(ok)
	}

	if ok, _ := r.Delete(u, key1); ok {
		t.Fatal(ok)
	}

	if ok, _ := r.Delete(u, key0); !ok {
		t.Fatal(ok)
	}

	if ok, _ := r.Delete(u, key0); ok {
		t.Fatal(ok)
	}

	_ = json.Unmarshal([]byte(val0), putjson)
	r.Put(u, putjson)
	_ = json.Unmarshal([]byte(val1), putjson)
	r.Put(u, putjson)

	if ok, _ := r.Delete(u, key0); !ok {
		t.Fatal(ok)
	}

	if ok, _ := r.Delete(u, key0); ok {
		t.Fatal(ok)
	}

	if ok, _ := r.Delete(u, key1); !ok {
		t.Fatal(ok)
	}

	if ok, _ := r.Delete(u, key1); ok {
		t.Fatal(ok)
	}
}

func aTestDelete1(t *testing.T) {
	u := getUser()
	const N = 130
	for _, x := range []int{0, -1, 0x555555, 0xaaaaaa, 0x333333, 0xcccccc, 0x314159} {
		r := getSWARMDBTable(u, TEST_TABLE, TEST_PKEY_INT, TEST_TABLE_INDEXTYPE, swarmdb.CT_INTEGER, true)
		a := make([]int, N)
		for i := range a {
			a[i] = (i ^ x) << 1
		}
		for _, k := range a {
			v := fmt.Sprintf(`{"%s":"%d","val":"value%d"}`, TEST_PKEY_INT, k, k)
			var putjson map[string]interface{}
			_ = json.Unmarshal([]byte(v), putjson)
			r.Put(u, putjson)

		}

		s := getSWARMDBTable(u, TEST_TABLE, TEST_PKEY_INT, TEST_TABLE_INDEXTYPE, swarmdb.CT_INTEGER, false)
		for i, k := range a {
			key := fmt.Sprintf("%d", k)
			fmt.Printf("attempt delete [%s]\n", key)
			ok, _ := s.Delete(u, key)
			if !ok {
				fmt.Printf("**** YIPES: [%s]\n", key)
				t.Fatal(i, x, k)
			}
		}
	}
}

func aTestDelete2(t *testing.T) {
	const N = 100
	u := getUser()

	for _, x := range []int{0, -1, 0x555555, 0xaaaaaa, 0x333333, 0xcccccc, 0x314159} {
		r := getSWARMDBTable(u, TEST_TABLE, TEST_PKEY_INT, TEST_TABLE_INDEXTYPE, swarmdb.CT_INTEGER, true)
		a := make([]int, N)
		rng := swarmdb.Rng()
		for i := range a {
			a[i] = (rng.Next() ^ x) << 1
		}
		var putjson map[string]interface{}
		for _, k := range a {

			v := fmt.Sprintf(`{"%s":"%d","val":"value%d"`, TEST_PKEY_INT, k, k)
			_ = json.Unmarshal([]byte(v), putjson)
			r.Put(u, putjson)

		}
		for i, k := range a {
			key := fmt.Sprintf("%d", k)
			ok, _ := r.Delete(u, key)
			if !ok {
				t.Fatal(i, x, k)
			}
		}
	}
}
