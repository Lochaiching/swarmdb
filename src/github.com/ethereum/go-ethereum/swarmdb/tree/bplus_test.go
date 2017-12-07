package swarmdb

import (
	"encoding/binary"
	"fmt"
	"github.com/cznic/mathutil"
	"github.com/ethereum/go-ethereum/swarm/api"
	"github.com/ethereum/go-ethereum/swarmdb/tree"
	"math"
	"testing"
	"github.com/ethereum/go-ethereum/swarmdb/common"
	"math/rand"
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

	fmt.Printf("---- TestPutInteger\n")
	hashid := make([]byte, 32)
	r := swarmdb.NewBPlusTreeDB(getAPI(t), hashid, common.KT_INTEGER)

	r.StartBuffer()
	vals := rand.Perm(20)
	// write 20 values into B-tree (only kept in memory)
	for _, i := range vals {
		k := IntToByte(i)
		v := []byte(fmt.Sprintf("valueof%06x", i))
		fmt.Printf("Insert %d %v %v\n", i, string(k), string(v))
		r.Put(k, v)
	}
	// this writes B+tree to SWARM
	r.FlushBuffer()

	// r.Close()
	r.Print()
	fmt.Printf("Put Test DONE\n----\n")

	hashid = r.GetHashID()
	s := swarmdb.NewBPlusTreeDB(getAPI(t), hashid, common.KT_INTEGER)
	fmt.Printf("getting...\n")

	g, _, _ := s.Get([]byte("000008"))
	fmt.Printf("GET1: %v\n", string(g))

	h, _, _ := s.Get([]byte("000001"))
	fmt.Printf("GET3: %v\n", string(h))

	s.Print()

	// ENUMERATOR
	res, _, _ := r.Seek([]byte("000004"))
	for k, v, err := res.Next(); err == nil; k, v, err = res.Next() {
		fmt.Printf("---> K: %s V: %v\n", common.KeyToString(common.KT_INTEGER, k), string(v))
	}
}


func TestPutString(t *testing.T) {
	fmt.Printf("---- TestPutString\n")

	hashid := make([]byte, 32)
	r := swarmdb.NewBPlusTreeDB(getAPI(t), hashid, common.KT_STRING)

	r.StartBuffer()
	vals := rand.Perm(20)
	// write 20 values into B-tree (only kept in memory)
	for _, i := range vals {
		k := []byte(fmt.Sprintf("%06x", i))
		v := []byte(fmt.Sprintf("valueof%06x", i))
		fmt.Printf("Insert %d %v %v\n", i, string(k), string(v))
		r.Put(k, v)
	}
	// this writes B+tree to SWARM
	r.FlushBuffer()

	// r.Close()
	r.Print()
	fmt.Printf("Put Test DONE\n----\n")

	hashid = r.GetHashID()

	s := swarmdb.NewBPlusTreeDB(getAPI(t), hashid, common.KT_STRING)
	fmt.Printf("getting...\n")

	g, _, _ := s.Get([]byte("000008"))
	fmt.Printf("GET1: %v\n", string(g))

	h, _, _ := s.Get([]byte("000001"))
	fmt.Printf("GET3: %v\n", string(h))

	s.Print()

	// ENUMERATOR
	res, _, _ := r.Seek([]byte("000004"))
	for k, v, err := res.Next(); err == nil; k, v, err = res.Next() {
		fmt.Printf("---> K: %s V: %v\n", common.KeyToString(common.KT_STRING, k), string(v))
	}
}

func IntToByte(i int) (k []byte){
	k = make([]byte, 8)
	binary.BigEndian.PutUint64(k, uint64(i))
	return k
}

func FloatToByte(f float64) (k []byte) {
	bits := math.Float64bits(f)
	k = make([]byte, 8)
	binary.BigEndian.PutUint64(k, bits)
	return k
}

func TestPutFloat(t *testing.T) {
	fmt.Printf("---- TestPutFloat\n")

	hashid := make([]byte, 32)
	r := swarmdb.NewBPlusTreeDB(getAPI(t), hashid, common.KT_FLOAT)

	r.StartBuffer()
	vals := rand.Perm(20)
	// write 20 values into B-tree (only kept in memory)
	for _, i := range vals {
		k := FloatToByte(float64(i) + .314159)
		v := []byte(fmt.Sprintf("valueof%06x", i))
		fmt.Printf("Insert %d %v %v\n", i, common.KeyToString(common.KT_FLOAT, k), string(v))
		r.Put(k, v)
	}
	// this writes B+tree to SWARM
	r.FlushBuffer()

	// r.Close()
	r.Print()
	fmt.Printf("Put Test DONE\n----\n")

	hashid = r.GetHashID()

	// ENUMERATOR
	s := swarmdb.NewBPlusTreeDB(getAPI(t), hashid, common.KT_FLOAT)
	
	res, _, _ := s.Seek(FloatToByte(3.14159))
	for k, v, err := res.Next(); err == nil; k, v, err = res.Next() {
		fmt.Printf("---> K: %s V: %v\n", common.KeyToString(common.KT_FLOAT, k), string(v))
	}
}


/*
func TestSetGet0(t *testing.T) {
	r := swarmdb.NewBPlusTreeDB(getAPI(t))
	set := r.Put
	key := []byte("42")
	val := []byte("314")
	set(key, val)

	g, ok, err := r.Get(key)
	if !ok || err != nil {
		t.Fatal(ok)
	}

	if bytes.Compare(g, val) != 0 {
		t.Fatal(g, val)
	}

	val2 := []byte("278")
	set(key, val2)
	r.Print()

	fmt.Printf("-----R2\n");
	r2 := swarmdb.NewBPlusTreeDB(getAPI(t))
	r2.Print()
	g2, ok, err := r2.Get(key)
	if ! ok || err != nil {
		fmt.Printf("ok %v err %v\n", ok, err)
		t.Fatal(ok)
	} else {
		fmt.Printf("PASS GET1\n")
	}
	os.Exit(0);

	if bytes.Compare(g2, val2) != 0 {
		t.Fatal(g2, val2)
	}

	key2 := []byte("420")
	val3 := []byte("bbb")
	set(key2, val3)
	r3 := swarmdb.NewBPlusTreeDB(getAPI(t))
	g3, ok, err := r3.Get(key2)
	if !ok || err != nil {
		t.Fatal(ok)
	}

	if bytes.Compare(g3, val3) != 0 {
		t.Fatal(g3, val3)
	} else {
		fmt.Printf("PASS GET2\n")
	}
	r.Close()
}


func TestSetGet1(t *testing.T) {
	const N = 4
	for _, x := range []int{0, -1, 0x555555, 0xaaaaaa, 0x333333, 0xcccccc, 0x314159} {
		r := swarmdb.NewBPlusTreeDB(getAPI(t))
		set := r.Put
		a := make([]int, N)
		for i := range a {
			a[i] = (i ^ x) << 1
		}
		fmt.Printf("%v\n", a)
		for _, k := range a {
			key := fmt.Sprintf("%v", k)
			val := fmt.Sprintf("%v", k^x)
			set([]byte(key), []byte(val))
		}

		for i, k := range a {
			key := fmt.Sprintf("%v", k)
			v, ok, err := r.Get([]byte(key))
			if ! ok || err!= nil {
				t.Fatal(i, key, v, ok)
			}

			val := fmt.Sprintf("%v", k^x)
			if bytes.Compare([]byte(val), v) != 0 {
				t.Fatal(i, val, v)
			}

			k |= 1
			key2 := fmt.Sprintf("%v", k)
			_, ok, _ = r.Get([]byte(key2))
			if ok {
				t.Fatal(i, k)
			}

		}

		for _, k := range a {
			key := fmt.Sprintf("%v", k)
			val := fmt.Sprintf("%v", k^x+42)

			r.Put([]byte(key), []byte(val))
		}

		for i, k := range a {
			key := fmt.Sprintf("%v", k)
			val := fmt.Sprintf("%v", k^x+42)
			v, ok, err := r.Get([]byte(key))
			if !ok || err != nil {
				t.Fatal(i, k, v, ok)
			}

			if bytes.Compare([]byte(val), v) != 0 {
				t.Fatal(i, v, val)
			}

			k |= 1
			key2 := fmt.Sprintf("%v", k)
			_, ok, _ = r.Get([]byte(key2))
			if ok {
				t.Fatal(i, k)
			}
		}
		r.Close()
	}
}
*/

/*
func TestSetGet2(t *testing.T) {
	const N = 40000
	for _, x := range []int{0, -1, 0x555555, 0xaaaaaa, 0x333333, 0xcccccc, 0x314159} {
		rng := rng()
		r := swarmdb.NewBPlusTreeDB(getAPI(t))

		set := r.Put
		a := make([]int, N)
		for i := range a {
			a[i] = (rng.Next() ^ x) << 1
		}
		for _, k := range a {
			key := fmt.Sprintf("%v", k)
			val := fmt.Sprintf("%v", k^x)
			set([]byte(key), []byte(val))
		}

		for i, k := range a {
			key := fmt.Sprintf("%v", k)
			v, ok, err := r.Get([]byte(key))
			if !ok || err!= nil {
				t.Fatal(i, k, v, ok)
			}
			val := fmt.Sprintf("%v", k^x)
			if bytes.Compare([]byte(val), v) != 0 {
				t.Fatal(i, val, v)
			}

			k |= 1
			key2 := fmt.Sprintf("%v", k)
 			_, ok, _ = r.Get([]byte(key2))
			if ok {
				t.Fatal(i, k)
			}
		}

		for _, k := range a {
			key := fmt.Sprintf("%v", k)
			val := fmt.Sprintf("%v", (k^x)+42)
			r.Put([]byte(key), []byte(val))
		}

		for i, k := range a {
			key := fmt.Sprintf("%v", k)
			v, ok, err := r.Get([]byte(key))
			if !ok || err != nil {
				t.Fatal(i, k, v, ok)
			}

			val := fmt.Sprintf("%v", k^x+42)
			if bytes.Compare(v, []byte(val)) != 0 {
				t.Fatal(i, val, v)
			}

			k |= 1
			key2 := fmt.Sprintf("%v", k)
			_, ok, _ = r.Get([]byte(key2))
			if ok {
				t.Fatal(i, k)
			}
		}
	}
}

func TestSetGet3(t *testing.T) {
	r := swarmdb.NewBPlusTreeDB(getAPI(t))
	set := r.Put
	var i int
	for i = 0; ; i++ {
		key := fmt.Sprintf("%v", i)
		val := fmt.Sprintf("%v", -i)
		set([]byte(key), []byte(val))
		if _, ok := r.r.(*x); ok {
			break
		}
	}
	for j := 0; j <= i; j++ {
		key := fmt.Sprintf("%v", j)
		val := fmt.Sprintf("%v", j)
		set([]byte(key), []byte(val))
	}

	for j := 0; j <= i; j++ {
		key := fmt.Sprintf("%v", j)
		val := fmt.Sprintf("%v", j)
		v, ok, err := r.Get(j)
		if !ok || err != nil{
			t.Fatal(j)
 }
		if bytes.Compare(val, v) {
			t.Fatal(val, v)
		}
	}
}

// works
func TestDelete0(t *testing.T) {
	r := swarmdb.NewBPlusTreeDB(getAPI(t))

	key0 := []byte(fmt.Sprintf("%d", 0))
	key1 := []byte(fmt.Sprintf("%d", 1))

	if ok, _ := r.Delete(key0); ok {
		t.Fatal(ok)
	}

	r.Put(key0, key0)
	if ok, _ := r.Delete(key1); ok {
		t.Fatal(ok)
	}

	if ok, _ := r.Delete(key0); !ok  {
		t.Fatal(ok)
	}

	if ok, _ := r.Delete(key0); ok {
		t.Fatal(ok)
	}

	r.Put(key0, key0)
	r.Put(key1, key1)
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

	r.Put(key0, key0)
	r.Put(key1, key1)
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

func TestDelete1(t *testing.T) {
	const N = 130000
	for _, x := range []int{0, -1, 0x555555, 0xaaaaaa, 0x333333, 0xcccccc, 0x314159} {
		r := swarmdb.NewBPlusTreeDB(getAPI(t))
		set := r.Put
		a := make([]int, N)
		for i := range a {
			a[i] = (i ^ x) << 1
		}
		for _, k := range a {
			key := fmt.Sprintf("%v", k)
			val := fmt.Sprintf("%v", 0)
			// fmt.Printf("%s|%s\n", key, val)
			set([]byte(key), []byte(val))
		}

		for i, k := range a {
			key := fmt.Sprintf("%v", k)
			ok, _ := r.Delete([]byte(key))
			// fmt.Printf("DEL%s\n", key)
			if !ok {
				fmt.Printf("YIPE%s\n", key)
				t.Fatal(i, x, k)
			}
		}
		r.Close();
	}
	fmt.Printf("DONE\n");
}

func TestDelete2(t *testing.T) {
	const N = 100000
	for _, x := range []int{0, -1, 0x555555, 0xaaaaaa, 0x333333, 0xcccccc, 0x314159} {
		r := swarmdb.NewBPlusTreeDB(getAPI(t))

		set := r.Put
		a := make([]int, N)
		rng := rng()
		for i := range a {
			a[i] = (rng.Next() ^ x) << 1
		}
		for _, k := range a {
			key := fmt.Sprintf("%d", k);
			val := fmt.Sprintf("%d", 0);
			set([]byte(key), []byte(val))
		}

		for i, k := range a {
			key := fmt.Sprintf("%d", k);
			ok, _ := r.Delete([]byte(key))
			if !ok {
				t.Fatal(i, x, k)
			}
		}
	}
}
*/
