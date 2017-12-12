package common_test

import (
	"fmt"
	common "github.com/ethereum/go-ethereum/swarmdb"
	"testing"
	//"os"
	// "bytes"
	"strings"
	"github.com/cznic/mathutil"
	"math"
	"math/rand"
)

const (
TEST_OWNER = "owner1"
TEST_TABLE = "testtable"
TEST_PKEY_INT = "accountID"
TEST_PKEY_STRING = "email"
TEST_PKEY_FLOAT = "ts"
TEST_TABLE_TREETYPE = common.TT_BPLUSTREE
)

func getSWARMDBTable(ownerID string, tableName string, primaryKey string, primaryTreeType common.TreeType, primaryKeyType common.KeyType, create bool) (tbl *common.Table) {
	swarmdb := common.NewSwarmDB()

	tbl = swarmdb.NewTable(ownerID, tableName)

	// CreateTable
	if create {
		var option []common.TableOption
		o := common.TableOption{Index: primaryKey, Primary: 1, TreeType: primaryTreeType, KeyType: primaryKeyType}
		option = append(option, o)
		tbl.CreateTable(option)
	}

	// OpenTable
	err := tbl.OpenTable()
	if ( err != nil ) {
		fmt.Print("OPENTABLE ERR %v\n", err)
	}
	return tbl
}

func TestTable(t *testing.T) {
	tbl := getSWARMDBTable(TEST_OWNER, TEST_TABLE, TEST_PKEY_STRING, TEST_TABLE_TREETYPE, common.KT_STRING, true)

	putstr := `{"email":"rodney@wolk.com", "age": 38, "gender": "M", "weight": 172.5}`
	tbl.Put(putstr)

	putstr = `{"email":"sourabh@wolk.com", "age": 45, "gender": "M", "weight": 210.5}`
	tbl.Put(putstr)
	// Put
	for i := 1; i < 100; i++ {
		g := "F"
		w := float64(i) + .314159
		putstr = fmt.Sprintf(`{"%s":"test%03d@wolk.com", "age": %d, "gender": "%s", "weight": %f}`,
			TEST_PKEY_STRING, i, i, g, w)

		g = "M"
		w = float64(i) + float64(0.414159)
		putstr = fmt.Sprintf(`{"%s":"test%03d@wolk.com", "age": %d, "gender": "%s", "weight": %f}`,
			TEST_PKEY_STRING, i, i, g, w)
		tbl.Put(putstr)
	}

	tbl2 := getSWARMDBTable(TEST_OWNER, TEST_TABLE, TEST_PKEY_STRING, TEST_TABLE_TREETYPE, common.KT_STRING, false)
	// Get
	res, err := tbl2.Get("rodney@wolk.com")
	fmt.Printf("Get %s %v \n", string(res), err)

	// Get
	fres, ferr := tbl2.Get("test010@wolk.com")
	fmt.Printf("Get %s %v \n", string(fres), ferr)
	//t.CloseTable()

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
	r := getSWARMDBTable(TEST_OWNER, TEST_TABLE, TEST_PKEY_INT, TEST_TABLE_TREETYPE, common.KT_INTEGER, true)

	// write 20 values into B-tree (only kept in memory)
	r.StartBuffer()
	vals := rand.Perm(20)
	for _, i := range vals {
		v := fmt.Sprintf(`{"%s":%d, "email":"test%03d@wolk.com"}`, TEST_PKEY_INT, i, i)
		r.Put(v)
	}
	r.FlushBuffer()

	s := getSWARMDBTable(TEST_OWNER, TEST_TABLE, TEST_PKEY_INT, TEST_TABLE_TREETYPE, common.KT_INTEGER, false)

	g, err := s.Get("8")
	if err != nil {
		t.Fatal(g, err)
	} else {
		fmt.Printf("Get(8): [%s]\n", string(g))
	}
	h, err2 := s.Get("1")
	if err2 != nil {
		t.Fatal(h, err2)
	}
	fmt.Printf("Get(1): [%s]\n", string(h))
	// s.Print()

}

func aTestPutString(t *testing.T) {
	fmt.Printf("---- TestPutString: generate 20 strings and enumerate them\n")


	r := getSWARMDBTable(TEST_OWNER, TEST_TABLE, TEST_PKEY_STRING, TEST_TABLE_TREETYPE, common.KT_STRING, true)

	r.StartBuffer()
	vals := rand.Perm(20)
	// write 20 values into B-tree (only kept in memory)
	for _, i := range vals {
		v := fmt.Sprintf(`{"%s":"%s", "val":"valueof%06x"}`, TEST_PKEY_STRING, i, i)
		r.Put(v)
	}
	// this writes B+tree to SWARM
	r.FlushBuffer()
	

	
	s := getSWARMDBTable(TEST_OWNER, TEST_TABLE, TEST_PKEY_STRING, TEST_TABLE_TREETYPE, common.KT_STRING, false)
	g, _ := s.Get("000008")
	fmt.Printf("Get(000008): %v\n", string(g))

	h, _ := s.Get("000001")
	fmt.Printf("Get(000001): %v\n", string(h))

}

func aTestPutFloat(t *testing.T) {
	fmt.Printf("---- TestPutFloat: generate 20 floats and enumerate them\n")

	r := getSWARMDBTable(TEST_OWNER, TEST_TABLE, TEST_PKEY_FLOAT, TEST_TABLE_TREETYPE, common.KT_FLOAT, true)

	r.StartBuffer()
	vals := rand.Perm(20)
	// write 20 values into B-tree (only kept in memory)
	for _, i := range vals {
		f := float64(i) + .3141519
		v := fmt.Sprintf(`{"%s":"%f", "val":"valueof%06x"}`, TEST_PKEY_FLOAT, f, i)
		// fmt.Printf("Insert %d %v %v\n", i, common.KeyToString(common.KT_FLOAT, k), string(v))
		r.Put(v)
	}
	// this writes B+tree to SWARM
	r.FlushBuffer()
}

func aTestSetGetString(t *testing.T) {

	r := getSWARMDBTable(TEST_OWNER, TEST_TABLE, TEST_PKEY_STRING, TEST_TABLE_TREETYPE, common.KT_FLOAT, true)

	// put
	key := "88"
	val := fmt.Sprintf(`{"%s":"%s", "val":"valueof%06x"}`, TEST_PKEY_STRING, key, key)
	r.Put(val)

	// check put with get
	g, err := r.Get(key)
	if err != nil || strings.Compare(string(g), val) != 0 {
		t.Fatal(g, val)
	}

	// r2 put
	r2 := getSWARMDBTable(TEST_OWNER, TEST_TABLE, TEST_PKEY_STRING, TEST_TABLE_TREETYPE, common.KT_FLOAT, false)
	val2 := fmt.Sprintf(`{"%s":"%s", "val":"newvalueof%06x"}`, TEST_PKEY_STRING, key, key)
	r2.Put(val2)

	// check put with get
	g2, err := r2.Get(key)
	if err != nil || strings.Compare(string(g2), val2) != 0 {
		t.Fatal(g2)
	}

	// r3 put
	r3 := getSWARMDBTable(TEST_OWNER, TEST_TABLE, TEST_PKEY_STRING, TEST_TABLE_TREETYPE, common.KT_FLOAT, false)
	key2 := "420"
	val3 := fmt.Sprintf(`{"%s":"%s", "val":"valueof%06x"}`, TEST_PKEY_STRING, key2, key2)
	r3.Put(val3)

	// check put with get
	g3, err := r3.Get(key2)
	if err != nil || strings.Compare(string(g3), val3) != 0 {
		t.Fatal(g3, val3)
	}
	fmt.Printf("PASS\n")
}

func aTestSetGetInt(t *testing.T) {
	const N = 4

	for _, x := range []int{0, -1, 0x555555, 0xaaaaaa, 0x333333, 0xcccccc, 0x314159} {
		r := getSWARMDBTable(TEST_OWNER, TEST_TABLE, TEST_PKEY_INT, TEST_TABLE_TREETYPE, common.KT_INTEGER, true)

		a := make([]int, N)
		for i := range a {
			a[i] = (i ^ x) << 1
		}
		fmt.Printf("%v\n", a)
		for _, k := range a {
			val := fmt.Sprintf(`{"%s":"%d", "value":"%d"}`, TEST_PKEY_INT, k, k^x)
			r.Put(val)
		}

		s := getSWARMDBTable(TEST_OWNER, TEST_TABLE, TEST_PKEY_INT, TEST_TABLE_TREETYPE, common.KT_INTEGER, false)
		for i, k := range a {
			key := fmt.Sprintf("%d", k) // common.IntToByte(k)
			val := fmt.Sprintf(`{"%s":"%d", "value":"%d"}`, TEST_PKEY_INT, k, k^x)
			v, err := r.Get(key)
			if err != nil || strings.Compare(val, string(v)) != 0 {
				t.Fatal(i, val, v)
			}

			k |= 1
			key = fmt.Sprintf("%d", k) // common.IntToByte(k)
			v, err = s.Get(key)
			if len(v) > 0 {
				t.Fatal(i, k)
			}
		}

		r2 := getSWARMDBTable(TEST_OWNER, TEST_TABLE, TEST_PKEY_INT, TEST_TABLE_TREETYPE, common.KT_INTEGER, false)
		for _, k := range a {
			val := fmt.Sprintf(`{"%s":"%d", "value":"%d"}`, TEST_PKEY_INT, k, k^x+1)
			r2.Put(val)
		}

		s2 := getSWARMDBTable(TEST_OWNER, TEST_TABLE, TEST_PKEY_INT, TEST_TABLE_TREETYPE, common.KT_INTEGER, false)
		for i, k := range a {
			key := fmt.Sprintf("%d", k) 
			val := fmt.Sprintf(`{"%s":"%d", "value":"%d"}`, TEST_PKEY_INT, k, k^x+1)
			v, err := s2.Get(key) // 
			if err != nil || strings.Compare(string(v), val) != 0 {
				t.Fatal(i, v, val)
			}

			k |= 1
			key = fmt.Sprintf("%d", k)
			_, err = r.Get(key)
			if len(v) > 0 || err != nil {
				t.Fatal(i, k)
			}
		}
	}
}

func aTestDelete0(t *testing.T) {

	r := getSWARMDBTable(TEST_OWNER, TEST_TABLE, TEST_PKEY_INT, TEST_TABLE_TREETYPE, common.KT_INTEGER, true)

	key0 := "0"
	key1 := "1"

	val0 := fmt.Sprintf(`{"accountID":"%s","val":"%s"}`, key0, key0)
	val1 := fmt.Sprintf(`{"accountID":"%s","val":"%s"}`, key1, key1)
	if ok, _ := r.Delete(key0); ok {
		t.Fatal(ok)
	}

	r.Put(val0)
	if ok, _ := r.Delete(key1); ok {
		t.Fatal(ok)
	}

	if ok, _ := r.Delete(key0); !ok {
		t.Fatal(ok)
	}

	if ok, _ := r.Delete(key0); ok {
		t.Fatal(ok)
	}

	r.Put(val0)
	r.Put(val1)
	if ok, _ := r.Delete(key1); !ok {
		t.Fatal(ok)
	}

	if ok, _ := r.Delete(key1); ok {
		t.Fatal(ok)
	}

	if ok, _ := r.Delete(key0); !ok {
		t.Fatal(ok)
	}

	if ok, _ := r.Delete(key0); ok {
		t.Fatal(ok)
	}

	r.Put(val0)
	r.Put(val1)
	if ok, _ := r.Delete(key0); !ok {
		t.Fatal(ok)
	}

	if ok, _ := r.Delete(key0); ok {
		t.Fatal(ok)
	}

	if ok, _ := r.Delete(key1); !ok {
		t.Fatal(ok)
	}

	if ok, _ := r.Delete(key1); ok {
		t.Fatal(ok)
	}
}

func aTestDelete1(t *testing.T) {

	const N = 130
	for _, x := range []int{0, -1, 0x555555, 0xaaaaaa, 0x333333, 0xcccccc, 0x314159} {
		r := getSWARMDBTable(TEST_OWNER, TEST_TABLE, TEST_PKEY_INT, TEST_TABLE_TREETYPE, common.KT_INTEGER, true)
		a := make([]int, N)
		for i := range a {
			a[i] = (i ^ x) << 1
		}
		for _, k := range a {
			v := fmt.Sprintf(`{"%s":"%d","val":"value%d"`, TEST_PKEY_INT, k, k)
			r.Put(v)
		}

		s := getSWARMDBTable(TEST_OWNER, TEST_TABLE, TEST_PKEY_INT, TEST_TABLE_TREETYPE, common.KT_INTEGER, true)
		for i, k := range a {
			key := fmt.Sprintf("%d", k)
			ok, _ := s.Delete(key)
			if !ok {
				fmt.Printf("YIPE%s\n", k)
				t.Fatal(i, x, k)
			}
		}
	}
}

func aTestDelete2(t *testing.T) {
	const N = 100

	for _, x := range []int{0, -1, 0x555555, 0xaaaaaa, 0x333333, 0xcccccc, 0x314159} {
		r := getSWARMDBTable(TEST_OWNER, TEST_TABLE, TEST_PKEY_INT, TEST_TABLE_TREETYPE, common.KT_INTEGER, true)
		a := make([]int, N)
		rng := rng()
		for i := range a {
			a[i] = (rng.Next() ^ x) << 1
		}
		for _, k := range a {
			
			v := fmt.Sprintf(`{"%s":"%d","val":"value%d"`, TEST_PKEY_INT, k, k)
			r.Put(v)
		}
		for i, k := range a {
			key := fmt.Sprintf("%d", k)
			ok, _ := r.Delete(key)
			if !ok {
				t.Fatal(i, x, k)
			}
		}
	}
}
