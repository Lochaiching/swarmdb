package swarmdb

import (
	// "encoding/binary"
	"bytes"
	// "strings"
	"fmt"
	"github.com/cznic/mathutil"
	"github.com/ethereum/go-ethereum/swarm/api"
	"github.com/ethereum/go-ethereum/swarmdb/common"
	"github.com/ethereum/go-ethereum/swarmdb/tree"
	"math"
	"math/rand"
	"testing"
)

func rng() *mathutil.FC32 {
	x, err := mathutil.NewFC32(math.MinInt32/4, math.MaxInt32/4, false)
	if err != nil {
		panic(err)
	}
	return x
}

func getAPI(t *testing.T) (a *api.Api) {
	return api.NewApi(nil, nil)
}

func TestPutInteger(t *testing.T) {

	fmt.Printf("---- TestPutInteger: generate 20 ints and enumerate them\n")
	hashid := make([]byte, 32)
	r := swarmdb.NewBPlusTreeDB(getAPI(t), hashid, common.KT_INTEGER)

	// write 20 values into B-tree (only kept in memory)
	r.StartBuffer()
	vals := rand.Perm(20)
	for _, i := range vals {
		k := common.IntToByte(i)
		v := []byte(fmt.Sprintf("valueof%06x", i))
		// fmt.Printf("Insert %d %v %v\n", i, string(k), string(v))
		r.Put(k, v)
	}
	// flush B+tree in memory to SWARM
	r.FlushBuffer()
	// r.Print()

	hashid, _ = r.GetRootHash()
	s := swarmdb.NewBPlusTreeDB(getAPI(t), hashid, common.KT_INTEGER)

	g, ok, err := s.Get(common.IntToByte(8))
	if !ok || err != nil {
		t.Fatal(g, err)
	} else {
		fmt.Printf("Get(8): [%s]\n", string(g))
	}
	h, ok2, err2 := s.Get(common.IntToByte(1))
	if !ok2 || err2 != nil {
		t.Fatal(h, err2)
	}
	fmt.Printf("Get(1): [%s]\n", string(h))
	// s.Print()

	// ENUMERATOR
	res, _, _ := r.Seek([]byte("000004"))
	records := 0
	for k, v, err := res.Next(); err == nil; k, v, err = res.Next() {
		fmt.Printf(" *int*> %d: K: %s V: %v\n", records, common.KeyToString(common.KT_INTEGER, k), string(v))
		records++
	}
	fmt.Printf("---- TestPutInteger DONE (%d records)\n", records)
}

func aTestPutString(t *testing.T) {
	fmt.Printf("---- TestPutString: generate 20 strings and enumerate them\n")

	hashid := make([]byte, 32)
	r := swarmdb.NewBPlusTreeDB(getAPI(t), hashid, common.KT_STRING)

	r.StartBuffer()
	vals := rand.Perm(20)
	// write 20 values into B-tree (only kept in memory)
	for _, i := range vals {
		k := []byte(fmt.Sprintf("%06x", i))
		v := []byte(fmt.Sprintf("valueof%06x", i))
		// fmt.Printf("Insert %d %v %v\n", i, string(k), string(v))
		r.Put(k, v)
	}
	// this writes B+tree to SWARM
	r.FlushBuffer()
	// r.Print()

	hashid, _ = r.GetRootHash()
	s := swarmdb.NewBPlusTreeDB(getAPI(t), hashid, common.KT_STRING)
	g, _, _ := s.Get([]byte("000008"))
	fmt.Printf("Get(000008): %v\n", string(g))

	h, _, _ := s.Get([]byte("000001"))
	fmt.Printf("Get(000001): %v\n", string(h))
	// s.Print()

	// ENUMERATOR
	res, _, _ := r.Seek([]byte("000004"))
	records := 0
	for k, v, err := res.Next(); err == nil; k, v, err = res.Next() {
		fmt.Printf(" *string*> %d K: %s V: %v\n", records, common.KeyToString(common.KT_STRING, k), string(v))
		records++
	}
	fmt.Printf("---- TestPutString DONE (%d records)\n", records)
}

func aTestPutFloat(t *testing.T) {
	fmt.Printf("---- TestPutFloat: generate 20 floats and enumerate them\n")

	hashid := make([]byte, 32)
	r := swarmdb.NewBPlusTreeDB(getAPI(t), hashid, common.KT_FLOAT)

	r.StartBuffer()
	vals := rand.Perm(20)
	// write 20 values into B-tree (only kept in memory)
	for _, i := range vals {
		k := common.FloatToByte(float64(i) + .314159)
		v := []byte(fmt.Sprintf("valueof%06x", i))
		// fmt.Printf("Insert %d %v %v\n", i, common.KeyToString(common.KT_FLOAT, k), string(v))
		r.Put(k, v)
	}
	// this writes B+tree to SWARM
	r.FlushBuffer()
	// r.Print()

	hashid, _ = r.GetRootHash()
	s := swarmdb.NewBPlusTreeDB(getAPI(t), hashid, common.KT_FLOAT)
	// ENUMERATOR
	res, _, _ := s.Seek(common.FloatToByte(3.14159))
	records := 0
	for k, v, err := res.Next(); err == nil; k, v, err = res.Next() {
		fmt.Printf(" *float*> %d: K: %s V: %v\n", records, common.KeyToString(common.KT_FLOAT, k), string(v))
		records++
	}
}

func aTestSetGetString(t *testing.T) {
	hashid := make([]byte, 32)
	r := swarmdb.NewBPlusTreeDB(getAPI(t), hashid, common.KT_STRING)

	// put
	key := []byte("42")
	val := common.SHA256("314")
	r.Put(key, val)

	// check put with get
	g, ok, err := r.Get(key)
	if !ok || err != nil {
		t.Fatal(ok)
	}
	if bytes.Compare(g, val) != 0 {
		t.Fatal(g, val)
	}
	//r.Print()
	hashid, _ = r.GetRootHash()

	// r2 put
	r2 := swarmdb.NewBPlusTreeDB(getAPI(t), hashid, common.KT_STRING)
	val2 := common.SHA256("278")
	r2.Put(key, val2)
	//r2.Print()

	// check put with get
	g2, ok, err := r2.Get(key)
	if !ok || err != nil {
		t.Fatal(ok)
	}
	if bytes.Compare(g2, val2) != 0 {
		t.Fatal(g2, val2)
	}
	hashid, _ = r2.GetRootHash()

	// r3 put
	r3 := swarmdb.NewBPlusTreeDB(getAPI(t), hashid, common.KT_STRING)
	key2 := []byte("420")
	val3 := common.SHA256("bbb")
	r3.Put(key2, val3)

	// check put with get
	g3, ok, err := r3.Get(key2)
	//r3.Print()
	if !ok || err != nil {
		t.Fatal(ok)
	}
	if bytes.Compare(g3, val3) != 0 {
		t.Fatal(g3, val3)
	}
	fmt.Printf("PASS\n")

}

func aTestSetGetInt(t *testing.T) {
	const N = 4
	hashid := make([]byte, 32)
	for _, x := range []int{0, -1, 0x555555, 0xaaaaaa, 0x333333, 0xcccccc, 0x314159} {
		r := swarmdb.NewBPlusTreeDB(getAPI(t), hashid, common.KT_INTEGER)

		a := make([]int, N)
		for i := range a {
			a[i] = (i ^ x) << 1
		}
		fmt.Printf("%v\n", a)
		for _, k := range a {
			r.Put(common.IntToByte(k), common.SHA256(fmt.Sprintf("%v", k^x)))
		}

		for i, k := range a {
			v, ok, err := r.Get(common.IntToByte(k))
			if !ok || err != nil {
				t.Fatal(i, k, v, ok)
			}

			val := common.SHA256(fmt.Sprintf("%v", k^x))
			if bytes.Compare([]byte(val), v) != 0 {
				t.Fatal(i, val, v)
			}

			k |= 1

			_, ok, _ = r.Get(common.IntToByte(k))
			if ok {
				t.Fatal(i, k)
			}

		}

		for _, k := range a {
			r.Put(common.IntToByte(k), common.SHA256(fmt.Sprintf("%v", k^x+42)))
		}

		for i, k := range a {
			v, ok, err := r.Get(common.IntToByte(k))
			if !ok || err != nil {
				t.Fatal(i, k, v, ok)
			}

			val := common.SHA256(fmt.Sprintf("%v", k^x+42))
			if bytes.Compare([]byte(val), v) != 0 {
				t.Fatal(i, v, val)
			}

			k |= 1
			_, ok, _ = r.Get(common.IntToByte(k))
			if ok {
				t.Fatal(i, k)
			}
		}

	}
}

func aTestDelete0(t *testing.T) {
	hashid := make([]byte, 32)
	r := swarmdb.NewBPlusTreeDB(getAPI(t), hashid, common.KT_INTEGER)

	key0 := common.IntToByte(0)
	key1 := common.IntToByte(1)

	val0 := common.SHA256("0")
	val1 := common.SHA256("1")

	if ok, _ := r.Delete(key0); ok {
		t.Fatal(ok)
	}

	r.Put(key0, val0)
	if ok, _ := r.Delete(key1); ok {
		t.Fatal(ok)
	}

	if ok, _ := r.Delete(key0); !ok {
		t.Fatal(ok)
	}

	if ok, _ := r.Delete(key0); ok {
		t.Fatal(ok)
	}

	r.Put(key0, val0)
	r.Put(key1, val1)
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

	r.Put(key0, val0)
	r.Put(key1, val1)
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
	hashid := make([]byte, 32)
	const N = 130
	for _, x := range []int{0, -1, 0x555555, 0xaaaaaa, 0x333333, 0xcccccc, 0x314159} {
		r := swarmdb.NewBPlusTreeDB(getAPI(t), hashid, common.KT_INTEGER)
		a := make([]int, N)
		for i := range a {
			a[i] = (i ^ x) << 1
		}
		for _, k := range a {
			r.Put(common.IntToByte(k), common.SHA256("0"))
		}

		for i, k := range a {
			ok, _ := r.Delete(common.IntToByte(k))
			if !ok {
				fmt.Printf("YIPE%s\n", k)
				t.Fatal(i, x, k)
			}
		}
	}
}

func aTestDelete2(t *testing.T) {
	const N = 100
	hashid := make([]byte, 32)
	for _, x := range []int{0, -1, 0x555555, 0xaaaaaa, 0x333333, 0xcccccc, 0x314159} {
		r := swarmdb.NewBPlusTreeDB(getAPI(t), hashid, common.KT_INTEGER)
		a := make([]int, N)
		rng := rng()
		for i := range a {
			a[i] = (rng.Next() ^ x) << 1
		}
		for _, k := range a {
			r.Put(common.IntToByte(k), common.SHA256("0"))
		}
		for i, k := range a {
			ok, _ := r.Delete(common.IntToByte(k))
			if !ok {
				t.Fatal(i, x, k)
			}
		}
	}
}
