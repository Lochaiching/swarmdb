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
	swarmdb "github.com/ethereum/go-ethereum/swarmdb"
	"os"
	"testing"
	// "bytes"
	"github.com/cznic/mathutil"
	"math"
	"math/rand"
	"strings"
)

const (
	TEST_OWNER           = "owner1"
	TEST_TABLE           = "secondary"
	TEST_PKEY_INT        = "accountID"
	TEST_PKEY_STRING     = "email"
	TEST_PKEY_FLOAT      = "ts"
	TEST_SKEY_INT        = "age"
	TEST_SKEY_STRING     = "gender"
	TEST_SKEY_FLOAT      = "weight"
	TEST_TABLE_INDEXTYPE = swarmdb.IT_BPLUSTREE
	TEST_ENCRYPTED       = 1
)

func getUser() (u *swarmdb.SWARMDBUser) {
	config, err := swarmdb.LoadSWARMDBConfig(swarmdb.SWARMDBCONF_FILE)
	if err != nil {
		fmt.Printf("No config error: ", err)
		os.Exit(0)
	}

	swarmdb.NewKeyManager(&config)
	user := config.GetSWARMDBUser()
	return user
}

func getSWARMDBTable(u *swarmdb.SWARMDBUser, tableName string, primaryKeyName string, primaryIndexType swarmdb.IndexType, primaryColumnType swarmdb.ColumnType, create bool) (tbl *swarmdb.Table) {
	config, _ := swarmdb.LoadSWARMDBConfig(swarmdb.SWARMDBCONF_FILE)
	ensdbPath := "/tmp"
	swarmdbObj := swarmdb.NewSwarmDB(ensdbPath, config.ChunkDBPath)

	// Commenting: (Rodney) -- CreateTable called from swarmdbObj and inside of that it calls NewTable
	// tbl = swarmdbObj.NewTable(ownerID, tableName)

	// CreateTable
	if create {
		var option []swarmdb.Column
		o := swarmdb.Column{ColumnName: primaryKeyName, Primary: 1, IndexType: primaryIndexType, ColumnType: primaryColumnType}
		option = append(option, o)
		tbl, _ = swarmdbObj.CreateTable(u, tableName, option, TEST_ENCRYPTED)
	}

	// OpenTable
	err := tbl.OpenTable(u)
	if err != nil {
		fmt.Print("OPENTABLE ERR %v\n", err)
	}
	return tbl
}

func getSWARMDBTableSecondary(u *swarmdb.SWARMDBUser, tableName string, primaryKeyName string, primaryIndexType swarmdb.IndexType, primaryColumnType swarmdb.ColumnType,
	secondaryKeyName string, secondaryIndexType swarmdb.IndexType, secondaryColumnType swarmdb.ColumnType,
	create bool) (swarmdbObj *swarmdb.SwarmDB) {

	config, _ := swarmdb.LoadSWARMDBConfig(swarmdb.SWARMDBCONF_FILE)
	ensdbPath := "/tmp"
	swarmdbObj = swarmdb.NewSwarmDB(ensdbPath, config.ChunkDBPath)

	// Commenting: (Rodney) -- CreateTable called from swarmdbObj and inside of that it calls NewTable
	//u := getUser()

	// CreateTable
	if create {
		var option []swarmdb.Column
		o := swarmdb.Column{ColumnName: primaryKeyName, Primary: 1, IndexType: primaryIndexType, ColumnType: primaryColumnType}
		option = append(option, o)

		s := swarmdb.Column{ColumnName: secondaryKeyName, Primary: 0, IndexType: secondaryIndexType, ColumnType: secondaryColumnType}
		option = append(option, s)
		tbl, _ := swarmdbObj.CreateTable(u, tableName, option, TEST_ENCRYPTED)

		// OpenTable
		err := tbl.OpenTable(u)
		if err != nil {
			fmt.Printf("OPENTABLE ERR %v\n", err)
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
		tbl, _ := swarmdbObj.GetTable(u, u.Address, tableName)
		err := tbl.OpenTable(u)
		if err != nil {
			fmt.Printf("OPENTABLE ERR %v\n", err)
		}
	}
	return swarmdbObj
}

func TestSetGetInt(t *testing.T) {
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

func TestTable(t *testing.T) {
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

func TestTableSecondaryInt(t *testing.T) {
	u := getUser()
	swarmdb := getSWARMDBTableSecondary(u, TEST_TABLE, TEST_PKEY_STRING, TEST_TABLE_INDEXTYPE, swarmdb.CT_STRING,
		TEST_SKEY_INT, TEST_TABLE_INDEXTYPE, swarmdb.CT_INTEGER, true)

	rows, err := swarmdb.Scan(u, TEST_OWNER, TEST_TABLE, "age", 1)
	if err != nil {
		t.Fatal(err)
	}
	for i, r := range rows {
		fmt.Printf("%v:%v\n", i, r)
	}

	//	os.Exit(0)
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

func TestTableSecondaryFloat(t *testing.T) {
	t.SkipNow()
	u := getUser()
	swdb := getSWARMDBTableSecondary(u, TEST_TABLE, TEST_PKEY_STRING, TEST_TABLE_INDEXTYPE, swarmdb.CT_STRING,
		TEST_SKEY_FLOAT, TEST_TABLE_INDEXTYPE, swarmdb.CT_FLOAT, true)
	// select * from table where age < 30
	sql := fmt.Sprintf("select * from %s where %s < 10", TEST_TABLE, TEST_SKEY_FLOAT)

	query, err := swarmdb.ParseQuery(sql)
	if err != nil {
		t.Fatal(err)
	}
	query.TableOwner = TEST_OWNER

	rows, err := swdb.QuerySelect(u, &query)
	if err != nil {
		t.Fatal(err)
	} else {
		for i, row := range rows {
			fmt.Printf("%d:%v\n", i, row)
		}
	}
}

func TestTableSecondaryString(t *testing.T) {
	t.SkipNow()
	u := getUser()
	swdb := getSWARMDBTableSecondary(u, TEST_TABLE, TEST_PKEY_STRING, TEST_TABLE_INDEXTYPE, swarmdb.CT_STRING,
		TEST_SKEY_STRING, TEST_TABLE_INDEXTYPE, swarmdb.CT_STRING, true)
	sql := fmt.Sprintf("select * from %s where %s < 10", TEST_TABLE, TEST_SKEY_STRING)

	query, err := swarmdb.ParseQuery(sql)
	if err != nil {
		t.Fatal(err)
	}
	query.TableOwner = TEST_OWNER

	rows, err := swdb.QuerySelect(u, &query)
	if err != nil {
		t.Fatal(err)
	} else {
		for i, row := range rows {
			fmt.Printf("%d:%v\n", i, row)
		}
	}
}

func rng() *mathutil.FC32 {
	x, err := mathutil.NewFC32(math.MinInt32/4, math.MaxInt32/4, false)
	if err != nil {
		panic(err)
	}
	return x
}

// primary key is integer "accountID"
func aTestPutInteger(t *testing.T) {
	fmt.Printf("---- TestPutInteger: generate 20 ints and enumerate them\n")
	u := getUser()

	r := getSWARMDBTable(u, TEST_TABLE, TEST_PKEY_INT, TEST_TABLE_INDEXTYPE, swarmdb.CT_INTEGER, true)

	// write 20 values into B-tree (only kept in memory)
	r.StartBuffer(u)
	vals := rand.Perm(20)
	var putjson map[string]interface{}
	for _, i := range vals {
		v := fmt.Sprintf(`{"%s":"%d", "email":"test%03d@wolk.com"}`, TEST_PKEY_INT, i, i)
		_ = json.Unmarshal([]byte(v), putjson)
		r.Put(u, putjson)
	}
	r.FlushBuffer(u)

	s := getSWARMDBTable(u, TEST_TABLE, TEST_PKEY_INT, TEST_TABLE_INDEXTYPE, swarmdb.CT_INTEGER, false)

	g, err := s.Get(u, []byte("8"))
	if err != nil {
		t.Fatal(g, err)
	} else {
		fmt.Printf("Get(8): [%s]\n", string(g))
	}
	h, err2 := s.Get(u, []byte("1"))
	if err2 != nil {
		t.Fatal(h, err2)
	}
	fmt.Printf("Get(1): [%s]\n", string(h))
	// s.Print()
}

func aTestPutString(t *testing.T) {
	fmt.Printf("---- TestPutString: generate 20 strings and enumerate them\n")
	u := getUser()

	r := getSWARMDBTable(u, TEST_TABLE, TEST_PKEY_STRING, TEST_TABLE_INDEXTYPE, swarmdb.CT_STRING, true)

	r.StartBuffer(u)
	vals := rand.Perm(20)
	var putjson map[string]interface{}
	// write 20 values into B-tree (only kept in memory)
	for _, i := range vals {
		v := fmt.Sprintf(`{"%s":"t%06x@wolk.com", "val":"valueof%06x"}`, TEST_PKEY_STRING, i, i)
		_ = json.Unmarshal([]byte(v), putjson)
		r.Put(u, putjson)

	}
	// this writes B+tree to SWARM
	r.FlushBuffer(u)

	s := getSWARMDBTable(u, TEST_TABLE, TEST_PKEY_STRING, TEST_TABLE_INDEXTYPE, swarmdb.CT_STRING, false)
	k := "t000008@wolk.com"
	g, _ := s.Get(u, []byte(k))
	fmt.Printf("Get(%s): %v\n", k, string(g))

	k1 := "t000001@wolk.com"
	h, _ := s.Get(u, []byte(k1))
	fmt.Printf("Get(%s): %v\n", k1, string(h))

}

func aTestPutFloat(t *testing.T) {
	fmt.Printf("---- TestPutFloat: generate 20 floats and enumerate them\n")
	u := getUser()

	r := getSWARMDBTable(u, TEST_TABLE, TEST_PKEY_FLOAT, TEST_TABLE_INDEXTYPE, swarmdb.CT_FLOAT, true)

	r.StartBuffer(u)
	vals := rand.Perm(20)
	var putjson map[string]interface{}
	// write 20 values into B-tree (only kept in memory)
	for _, i := range vals {
		f := float64(i) + .3141519
		v := fmt.Sprintf(`{"%s":"%f", "val":"valueof%06x"}`, TEST_PKEY_FLOAT, f, i)
		_ = json.Unmarshal([]byte(v), putjson)
		r.Put(u, putjson)

	}
	// this writes B+tree to SWARM
	r.FlushBuffer(u)

	s := getSWARMDBTable(u, TEST_TABLE, TEST_PKEY_STRING, TEST_TABLE_INDEXTYPE, swarmdb.CT_STRING, false)
	i := 4
	f := float64(i) + .3141519
	k := fmt.Sprintf("%f", f)
	g, _ := s.Get(u, []byte(k))
	fmt.Printf("Get(%s): %v\n", k, string(g))

	i = 6
	f = float64(i) + .3141519
	k = fmt.Sprintf("%f", f)
	h, _ := s.Get(u, []byte(k))
	fmt.Printf("Get(%s): %v\n", k, string(h))
}

func aTestSetGetString(t *testing.T) {
	u := getUser()
	r := getSWARMDBTable(u, TEST_TABLE, TEST_PKEY_STRING, TEST_TABLE_INDEXTYPE, swarmdb.CT_FLOAT, true)

	// put
	key := "88"
	val := fmt.Sprintf(`{"%s":"%s", "val":"valueof%06x"}`, TEST_PKEY_STRING, key, key)
	var putjson map[string]interface{}
	_ = json.Unmarshal([]byte(val), putjson)
	r.Put(u, putjson)

	// check put with get
	g, err := r.Get(u, []byte(key))
	if err != nil || strings.Compare(string(g), val) != 0 {
		t.Fatal(g, val)
	} else {
		fmt.Printf("Get(%s) => %s\n", key, val)
	}

	// r2 put
	r2 := getSWARMDBTable(u, TEST_TABLE, TEST_PKEY_STRING, TEST_TABLE_INDEXTYPE, swarmdb.CT_FLOAT, false)
	val2 := fmt.Sprintf(`{"%s":"%s", "val":"newvalueof%06x"}`, TEST_PKEY_STRING, key, key)
	_ = json.Unmarshal([]byte(val2), putjson)
	r.Put(u, putjson)

	// check put with get
	g2, err := r2.Get(u, []byte(key))
	if err != nil || strings.Compare(string(g2), val2) != 0 {
		t.Fatal(g2, val2)
	} else {
		fmt.Printf("Get(%s) => %s\n", key, val2)
	}

	// r3 put
	r3 := getSWARMDBTable(u, TEST_TABLE, TEST_PKEY_STRING, TEST_TABLE_INDEXTYPE, swarmdb.CT_FLOAT, false)
	val3 := fmt.Sprintf(`{"%s":"%s", "val":"valueof%06x"}`, TEST_PKEY_STRING, key, key)
	_ = json.Unmarshal([]byte(val3), putjson)
	r.Put(u, putjson)

	// check put with get
	g3, err := r3.Get(u, []byte(key))
	if err != nil || strings.Compare(string(g3), val3) != 0 {
		t.Fatal(g3, val3)
	} else {
		fmt.Printf("Get(%s) => %s\n", key, val3)
	}
	fmt.Printf("PASS\n")
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
		rng := rng()
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

func TestCreateTable(t *testing.T) {
	//t.SkipNow()
	u := getUser()

	config, _ := swarmdb.LoadSWARMDBConfig(swarmdb.SWARMDBCONF_FILE)
	ensdbPath := "/tmp"
	swdb := swarmdb.NewSwarmDB(ensdbPath, config.ChunkDBPath)

	var testColumn []swarmdb.Column
	testColumn = make([]swarmdb.Column, 3)
	testColumn[0].ColumnName = "email"
	testColumn[0].Primary = 1                      // What if this is inconsistent?
	testColumn[0].IndexType = swarmdb.IT_BPLUSTREE //  What if this is inconsistent?
	testColumn[0].ColumnType = swarmdb.CT_STRING

	testColumn[1].ColumnName = "yob"
	testColumn[1].Primary = 0                      // What if this is inconsistent?
	testColumn[1].IndexType = swarmdb.IT_BPLUSTREE //  What if this is inconsistent?
	testColumn[1].ColumnType = swarmdb.CT_INTEGER

	testColumn[2].ColumnName = "location"
	testColumn[2].Primary = 0                      // What if this is inconsistent?
	testColumn[2].IndexType = swarmdb.IT_BPLUSTREE //  What if this is inconsistent?
	testColumn[2].ColumnType = swarmdb.CT_STRING

	var testReqOption swarmdb.RequestOption

	testReqOption.RequestType = "CreateTable"
	testReqOption.TableOwner = "0xf6b55acbbc49f4524aa48d19281a9a77c54de10f"
	testReqOption.Table = "contacts"
	//testReqOption.Bid = 7.07
	//testReqOption.Replication = 3
	testReqOption.Encrypted = 1
	testReqOption.Columns = testColumn

	marshalTestReqOption, err := json.Marshal(testReqOption)
	fmt.Printf("JSON --> %s", marshalTestReqOption)
	if err != nil {
		t.Fatalf("error marshaling testReqOption: %s", err)

	}
	swdb.SelectHandler(u, string(marshalTestReqOption))
}

func TestOpenTable(t *testing.T) {
	u := getUser()
	config, _ := swarmdb.LoadSWARMDBConfig(swarmdb.SWARMDBCONF_FILE)
	ensdbPath := "/tmp"
	swdb := swarmdb.NewSwarmDB(ensdbPath, config.ChunkDBPath)
	var testReqOption swarmdb.RequestOption

	testReqOption.RequestType = "OpenTable"
	testReqOption.TableOwner = "0xf6b55acbbc49f4524aa48d19281a9a77c54de10f"
	testReqOption.Table = "contacts"

	marshalTestReqOption, err := json.Marshal(testReqOption)
	fmt.Printf("\nJSON --> %s", marshalTestReqOption)
	if err != nil {
		t.Fatalf("error marshaling testReqOption: %s", err)
	}
	swdb.SelectHandler(u, string(marshalTestReqOption))
}

func TestGetTableFail(t *testing.T) {
	config, _ := swarmdb.LoadSWARMDBConfig(swarmdb.SWARMDBCONF_FILE)
	ensdbPath := "/tmp"
	swdb := swarmdb.NewSwarmDB(ensdbPath, config.ChunkDBPath)
	ownerID := "BadOwner"
	tableName := "BadTable"
	u := getUser()
	_, err := swdb.GetTable(u, ownerID, tableName)
	if err == nil {
		t.Fatalf("TestGetTableFail: FAILED")
	}
	if err.Error() != `Table [`+tableName+`] with Owner [`+ownerID+`] does not exist` {
		t.Fatalf("TestGetTableFail: FAILED")
	}
}

func OpenTable(swdb *swarmdb.SwarmDB, owner string, table string) {
	u := getUser()
	var testReqOption swarmdb.RequestOption

	testReqOption.RequestType = "OpenTable"
	testReqOption.TableOwner = "0xf6b55acbbc49f4524aa48d19281a9a77c54de10f"
	testReqOption.Table = "contacts"

	marshalTestReqOption, err := json.Marshal(testReqOption)
	if err != nil {
		fmt.Printf("error marshaling testReqOption: %s", err)
	}
	swdb.SelectHandler(u, string(marshalTestReqOption))
}

func TestPut(t *testing.T) {
	u := getUser()
	config, _ := swarmdb.LoadSWARMDBConfig(swarmdb.SWARMDBCONF_FILE)
	ensdbPath := "/tmp"
	swdb := swarmdb.NewSwarmDB(ensdbPath, config.ChunkDBPath)

	var testReqOption swarmdb.RequestOption
	testReqOption.RequestType = "Put"
	testReqOption.TableOwner = "0xf6b55acbbc49f4524aa48d19281a9a77c54de10f"
	testReqOption.Table = "contacts"
	testReqOption.Key = "rodneytest1@wolk.com"
	//testReqOption.Row = make(map[string]interface{})
	row := `{"name": "Rodney", "age": 37, "email": "rodneytest1@wolk.com"}`
	rowObj := make(map[string]interface{})
	err := json.Unmarshal([]byte(row), &rowObj)
	if err != nil {
		t.Fatalf("json unmarshal err... %s", err)
	}
	/*
		for k, v := range rowObj {
			switch v.(type) {
			case float64:
				testReqOption.Row[k] = fmt.Sprintf("%f", v)

			default:
				testReqOption.Row[k] = v.(string)
			}
		 }*/
	newRow := swarmdb.Row{Cells: rowObj}
	testReqOption.Rows = append(testReqOption.Rows, newRow)
	marshalTestReqOption, err := json.Marshal(testReqOption)
	fmt.Printf("\nJSON --> %s", marshalTestReqOption)
	if err != nil {
		t.Fatalf("error marshaling testReqOption: %s", err)
	}
	OpenTable(swdb, testReqOption.TableOwner, testReqOption.Table)
	swdb.SelectHandler(u, string(marshalTestReqOption))
}

func TestGet(t *testing.T) {

	u := getUser()
	config, _ := swarmdb.LoadSWARMDBConfig(swarmdb.SWARMDBCONF_FILE)
	ensdbPath := "/tmp"
	swdb := swarmdb.NewSwarmDB(ensdbPath, config.ChunkDBPath)

	var testReqOption swarmdb.RequestOption
	testReqOption.RequestType = "Get"
	testReqOption.TableOwner = "0xf6b55acbbc49f4524aa48d19281a9a77c54de10f"
	testReqOption.Table = "contacts"
	testReqOption.Key = "rodneytest1@wolk.com"

	marshalTestReqOption, err := json.Marshal(testReqOption)
	fmt.Printf("\nJSON --> %s", marshalTestReqOption)
	if err != nil {
		t.Fatalf("error marshaling testReqOption: %s", err)
	}
	OpenTable(swdb, testReqOption.TableOwner, testReqOption.Table)

	resp, err := swdb.SelectHandler(u, string(marshalTestReqOption))
	if err != nil {
		t.Fatal(err)
	}
	fmt.Printf("\nResponse of TestGet is [%s]", resp)

}

func TestPutGet(t *testing.T) {
	u := getUser()
	config, _ := swarmdb.LoadSWARMDBConfig(swarmdb.SWARMDBCONF_FILE)
	ensdbPath := "/tmp"
	swdb := swarmdb.NewSwarmDB(ensdbPath, config.ChunkDBPath)

	var testReqOption swarmdb.RequestOption
	testReqOption.RequestType = "Put"
	testReqOption.TableOwner = "0xf6b55acbbc49f4524aa48d19281a9a77c54de10f"
	testReqOption.Table = "contacts"
	testReqOption.Key = "alinatest@wolk.com"
	//testReqOption.Row = make(map[string]interface{})
	row := `{"name": "ZAlina", "age": 35, "email": "alinatest@wolk.com"}`

	rowObj := make(map[string]interface{})
	_ = json.Unmarshal([]byte(row), &rowObj)
	/*
		for k, v := range rowObj {
			switch v.(type) {
			case float64:
				testReqOption.Row[k] = fmt.Sprintf("%f", v)

			default:
				testReqOption.Row[k] = v.(string)
			}
		 }*/
	newRow := swarmdb.Row{Cells: rowObj}
	testReqOption.Rows = append(testReqOption.Rows, newRow)
	marshalTestReqOption, err := json.Marshal(testReqOption)
	fmt.Printf("\nJSON --> %s", marshalTestReqOption)
	if err != nil {
		t.Fatalf("error marshaling testReqOption: %s", err)
	}
	OpenTable(swdb, testReqOption.TableOwner, testReqOption.Table)

	resp, err := swdb.SelectHandler(u, string(marshalTestReqOption))
	if err != nil {
		t.Fatal(err)
	} else {
		fmt.Printf("\nResponse of TestPutGet is [%s]", resp)
	}

	var testReqOptionGet swarmdb.RequestOption
	testReqOptionGet.RequestType = "Get"
	testReqOptionGet.TableOwner = "0xf6b55acbbc49f4524aa48d19281a9a77c54de10f"
	testReqOptionGet.Table = "contacts"
	testReqOptionGet.Key = "rodneytest1@wolk.com"
	testReqOptionGet.Key = "alinatest@wolk.com"

	marshalTestReqOption, err = json.Marshal(testReqOptionGet)
	fmt.Printf("\nJSON --> %s", marshalTestReqOption)
	if err != nil {
		t.Fatalf("error marshaling testReqOption: %s", err)
	}
	OpenTable(swdb, testReqOptionGet.TableOwner, testReqOptionGet.Table)

	resp, err2 := swdb.SelectHandler(u, string(marshalTestReqOption))
	if err2 != nil {
		t.Fatal(err)
	}

	fmt.Printf("\nResponse of TestGet is [%s]", resp)
}
