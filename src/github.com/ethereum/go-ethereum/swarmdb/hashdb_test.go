package swarmdb_test

import (
	"bytes"
	"fmt"
	"github.com/cznic/mathutil"
	"github.com/ethereum/go-ethereum/swarmdb"
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

func getSwarmDB(t *testing.T) swarmdb.SwarmDB {
	swarmdb := swarmdb.NewSwarmDB()
	return *swarmdb
}

func TestPutInteger(t *testing.T) {

	fmt.Printf("---- TestPutInteger: generate 20 ints and enumerate them\n")
	hashid := make([]byte, 32)
	r, _ := swarmdb.NewHashDB(nil, getSwarmDB(t), swarmdb.CT_INTEGER)

	// write 20 values into B-tree (only kept in memory)
	r.StartBuffer()
	vals := rand.Perm(20)
	for _, i := range vals {
		k := swarmdb.IntToByte(i)
		v := []byte(fmt.Sprintf("valueof%06x", i))
		// fmt.Printf("Insert %d %v %v\n", i, string(k), string(v))
		r.Put(k, v)
	}
	// flush B+tree in memory to SWARM
	r.FlushBuffer()
	r.Print()

	hashid, _ = r.GetRootHash()
	s, _ := swarmdb.NewHashDB(hashid, getSwarmDB(t), swarmdb.CT_INTEGER)

	g, ok, err := s.Get(swarmdb.IntToByte(10))
	if !ok || err != nil {
		t.Fatal(g, err)
	} else {
		fmt.Printf("Get(10): [%s]\n", string(g))
	}
	h, ok2, err2 := s.Get(swarmdb.IntToByte(1))
	if !ok2 || err2 != nil {
		t.Fatal(h, err2)
	}
	fmt.Printf("Get(1): [%s]\n", string(h))
	// s.Print()

        // ENUMERATOR
        if false {
                res, _ := s.SeekFirst()
                records := 0
                for k, v, err := res.Next(); err == nil; k, v, err = res.Next() {
                        fmt.Printf(" *int*> %d: K: %s V: %v\n", records, swarmdb.KeyToString(swarmdb.CT_INTEGER, k), string(v))
                        records++
                }
                fmt.Printf("---- TestPutInteger Next (%d records)\n", records)
        }

        // ENUMERATOR
        if true {
                res, _ := s.SeekLast()
                records := 0
                for k, v, err := res.Prev(); ; k, v, err = res.Prev() {
                        fmt.Printf(" *int*> %d: K: %s V: %v\n", records, swarmdb.KeyToString(swarmdb.CT_INTEGER, k), string(v))
                        records++
			if err != nil {
				fmt.Println("err = ", err)
				break
			}
                }
                fmt.Printf("---- TestPutInteger Prev (%d records)\n", records)
        }
}

func TestPutString(t *testing.T) {
	fmt.Printf("---- TestPutString: generate 20 strings and enumerate them\n")

	hashid := make([]byte, 32)
	r, _ := swarmdb.NewHashDB(nil, getSwarmDB(t), swarmdb.CT_STRING)

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
	s, _ := swarmdb.NewHashDB(hashid, getSwarmDB(t), swarmdb.CT_STRING)
	g, _, _ := s.Get([]byte("000008"))
	fmt.Printf("Get(000008): %v\n", string(g))

	h, _, _ := s.Get([]byte("000001"))
	fmt.Printf("Get(000001): %v\n", string(h))
	// s.Print()

        // ENUMERATOR
        res, _, _ := r.Seek([]byte("000004"))
        records := 0
        for k, v, err := res.Next(); err == nil; k, v, err = res.Next() {
                fmt.Printf(" *string*> %d K: %s V: %v\n", records, swarmdb.KeyToString(swarmdb.CT_STRING, k), string(v))
                records++
        }
        fmt.Printf("---- TestPutString DONE (%d records)\n", records)

}

func TestPutFloat(t *testing.T) {
	fmt.Printf("---- TestPutFloat: generate 20 floats and enumerate them\n")

	r, _ := swarmdb.NewHashDB(nil, getSwarmDB(t), swarmdb.CT_FLOAT)

	r.StartBuffer()
	vals := rand.Perm(20)
	// write 20 values into B-tree (only kept in memory)
	for _, i := range vals {
		k := swarmdb.FloatToByte(float64(i) + .314159)
		v := []byte(fmt.Sprintf("valueof%06x", i))
		// fmt.Printf("Insert %d %v %v\n", i, swarmdb.KeyToString(swarmdb.KT_FLOAT, k), string(v))
		r.Put(k, v)
	}
	// this writes B+tree to SWARM
	r.FlushBuffer()
	h, _, _ := r.Get(swarmdb.FloatToByte(0.314159))
	fmt.Printf("Get(0.314159): %v\n", string(h))
	// r.Print()
       // ENUMERATOR
        hashid, _ := r.GetRootHash()
        s, _ := swarmdb.NewHashDB(hashid, getSwarmDB(t),  swarmdb.CT_FLOAT)
        res, _, err := s.Seek(swarmdb.FloatToByte(0.314159))
        if res == nil || err != nil {
                t.Fatal(err)
		return
	}
	
        records := 0
        for k, v, err := res.Next(); err == nil; k, v, err = res.Next() {
                fmt.Printf(" *float*> %d: K: %s V: %v\n", records, swarmdb.KeyToString(swarmdb.CT_FLOAT, k), string(v))
                records++
        }
}

func TestSetGetString(t *testing.T) {
	hashid := make([]byte, 32)
	r, _ := swarmdb.NewHashDB(nil, getSwarmDB(t), swarmdb.CT_STRING)

	// put
	key := []byte("42")
	val := swarmdb.SHA256("314")
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
	r2, _ := swarmdb.NewHashDB(hashid, getSwarmDB(t), swarmdb.CT_STRING)
	val2 := swarmdb.SHA256("278")
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
	r3, _ := swarmdb.NewHashDB(hashid, getSwarmDB(t), swarmdb.CT_STRING)
	key2 := []byte("420")
	val3 := swarmdb.SHA256("bbb")
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

func TestSetGetInt(t *testing.T) {
	const N = 4
	for _, x := range []int{0, -1, 0x555555, 0xaaaaaa, 0x333333, 0xcccccc, 0x314159} {
		r, _ := swarmdb.NewHashDB(nil, getSwarmDB(t), swarmdb.CT_INTEGER)

		a := make([]int, N)
		for i := range a {
			a[i] = (i ^ x) << 1
		}
		fmt.Printf("%v\n", a)
		for _, k := range a {
			r.Put(swarmdb.IntToByte(k), swarmdb.SHA256(fmt.Sprintf("%v", k^x)))
		}

		for i, k := range a {
			v, ok, err := r.Get(swarmdb.IntToByte(k))
			if !ok || err != nil {
				t.Fatal(i, k, v, ok)
			}

			val := swarmdb.SHA256(fmt.Sprintf("%v", k^x))
			if bytes.Compare([]byte(val), v) != 0 {
				t.Fatal(i, val, v)
			}

			k |= 1

			_, ok, _ = r.Get(swarmdb.IntToByte(k))
			if ok {
				t.Fatal(i, k)
			}

		}

		for _, k := range a {
			r.Put(swarmdb.IntToByte(k), swarmdb.SHA256(fmt.Sprintf("%v", k^x+42)))
		}

		for i, k := range a {
			v, ok, err := r.Get(swarmdb.IntToByte(k))
			if !ok || err != nil {
				t.Fatal(i, k, v, ok)
			}

			val := swarmdb.SHA256(fmt.Sprintf("%v", k^x+42))
			if bytes.Compare([]byte(val), v) != 0 {
				t.Fatal(i, v, val)
			}

			k |= 1
			_, ok, _ = r.Get(swarmdb.IntToByte(k))
			if ok {
				t.Fatal(i, k)
			}
		}

	}
}

func TestDelete0(t *testing.T) {
	r, _ := swarmdb.NewHashDB(nil, getSwarmDB(t), swarmdb.CT_INTEGER)

	key0 := swarmdb.IntToByte(0)
	key1 := swarmdb.IntToByte(1)

	val0 := swarmdb.SHA256("0")
	val1 := swarmdb.SHA256("1")

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

func TestDelete1(t *testing.T) {
	const N = 130
	for _, x := range []int{0, -1, 0x555555, 0xaaaaaa, 0x333333, 0xcccccc, 0x314159} {
		r, _ := swarmdb.NewHashDB(nil, getSwarmDB(t), swarmdb.CT_INTEGER)
		a := make([]int, N)
		for i := range a {
			a[i] = (i ^ x) << 1
		}
		for _, k := range a {
			r.Put(swarmdb.IntToByte(k), swarmdb.SHA256("0"))
		}

		for i, k := range a {
			ok, _ := r.Delete(swarmdb.IntToByte(k))
			if !ok {
				fmt.Printf("YIPE%s\n", k)
				t.Fatal(i, x, k)
			}
		}
	}
}

func TestDelete2(t *testing.T) {
	const N = 100
	for _, x := range []int{0, -1, 0x555555, 0xaaaaaa, 0x333333, 0xcccccc, 0x314159} {
		r, _ := swarmdb.NewHashDB(nil, getSwarmDB(t), swarmdb.CT_INTEGER)
		a := make([]int, N)
		rng := rng()
		for i := range a {
			a[i] = (rng.Next() ^ x) << 1
		}
		for _, k := range a {
			r.Put(swarmdb.IntToByte(k), swarmdb.SHA256("0"))
		}
		for i, k := range a {
			ok, _ := r.Delete(swarmdb.IntToByte(k))
			if !ok {
				t.Fatal(i, x, k)
			}
		}
	}
}
