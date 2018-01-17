// Copyright 2018 Wolk Inc. - SWARMDB Working Group
// This file is part of a SWARMDB fork of the go-ethereum library 

// The SWARMDB library is free software: you can redistribute it and/or modify
// it under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// The SWARM ethereum library is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU Lesser General Public License for more details.
//
// You should have received a copy of the GNU Lesser General Public License
// along with the go-ethereum library. If not, see <http://www.gnu.org/licenses/>.

package swarmdb_test

import (
	"bytes"
	"fmt"
	"github.com/cznic/mathutil"
	"github.com/ethereum/go-ethereum/swarmdb"
	"math"
	"math/rand"
	"testing"
	//"os"
)

func rng() *mathutil.FC32 {
	x, err := mathutil.NewFC32(math.MinInt32/4, math.MaxInt32/4, false)
	if err != nil {
		panic(err)
	}
	return x
}

func getSwarmDB(t *testing.T) (a swarmdb.SwarmDB) {
	config, _ := swarmdb.LoadSWARMDBConfig(swarmdb.SWARMDBCONF_FILE)
	ensdbPath := "/tmp"
	swarmdb := swarmdb.NewSwarmDB(ensdbPath, config.ChunkDBPath)
	return *swarmdb
}

func TestPutInteger(t *testing.T) {
	config, _ := swarmdb.LoadSWARMDBConfig(swarmdb.SWARMDBCONF_FILE)
	swarmdb.NewKeyManager(&config)
	u := config.GetSWARMDBUser()

	fmt.Printf("---- TestPutInteger: generate 20 ints and enumerate them\n")
	hashid := make([]byte, 32)
	r := swarmdb.NewBPlusTreeDB(u, getSwarmDB(t), hashid, swarmdb.CT_INTEGER, false, swarmdb.CT_STRING)

	// write 20 values into B-tree (only kept in memory)
	r.StartBuffer(u)
	vals := rand.Perm(20)
	for _, i := range vals {
		k := swarmdb.IntToByte(i)
		v := []byte(fmt.Sprintf("valueof%06x", i))
		// fmt.Printf("Insert %d %v %v\n", i, string(k), string(v))
		r.Put(u, k, v)
	}
	// flush B+tree in memory to SWARM
	r.FlushBuffer(u)

	hashid, _ = r.GetRootHash()
	s := swarmdb.NewBPlusTreeDB(u, getSwarmDB(t), hashid, swarmdb.CT_INTEGER, false, swarmdb.CT_STRING)

	g, ok, err := s.Get(u, swarmdb.IntToByte(8))
	if !ok || err != nil {
		t.Fatal(g, err)
	} else {
		fmt.Printf("Get(8): [%s]\n", string(g))
	}
	h, ok2, err2 := s.Get(u, swarmdb.IntToByte(1))
	if !ok2 || err2 != nil {
		t.Fatal(h, err2)
	}
	fmt.Printf("Get(1): [%s]\n", string(h))
	// s.Print()

	g, ok2, err2 = s.Get(u, swarmdb.IntToByte(12))
	g, ok2, err2 = s.Get(u, swarmdb.IntToByte(16))
	// s.Print()

	// ENUMERATOR
	if false {
		res, _ := s.SeekFirst(u)
		records := 0
		for k, v, err := res.Next(u); err == nil; k, v, err = res.Next(u) {
			fmt.Printf(" *int*> %d: K: %s V: %v\n", records, swarmdb.KeyToString(swarmdb.CT_INTEGER, k), string(v))
			records++
		}
		fmt.Printf("---- TestPutInteger Next (%d records)\n", records)
	}

	// ENUMERATOR
	if true {
		res, _ := s.SeekLast(u)
		records := 0
		for k, v, err := res.Prev(u); err == nil; k, v, err = res.Prev(u) {
			fmt.Printf(" *int*> %d: K: %s V: %v\n", records, swarmdb.KeyToString(swarmdb.CT_INTEGER, k), string(v))
			records++
		}
		fmt.Printf("---- TestPutInteger Prev (%d records)\n", records)
	}
}

func TestPutString(t *testing.T) {
	fmt.Printf("---- TestPutString: generate 20 strings and enumerate them\n")
	config, _ := swarmdb.LoadSWARMDBConfig(swarmdb.SWARMDBCONF_FILE)
	swarmdb.NewKeyManager(&config)
	u := config.GetSWARMDBUser()

	hashid := make([]byte, 32)
	r := swarmdb.NewBPlusTreeDB(u, getSwarmDB(t), hashid, swarmdb.CT_STRING, false, swarmdb.CT_STRING)

	r.StartBuffer(u)
	vals := rand.Perm(20)
	// write 20 values into B-tree (only kept in memory)
	for _, i := range vals {
		k := []byte(fmt.Sprintf("%06x", i))
		v := []byte(fmt.Sprintf("valueof%06x", i))
		// fmt.Printf("Insert %d %v %v\n", i, string(k), string(v))
		r.Put(u, k, v)
	}
	// this writes B+tree to SWARM
	r.FlushBuffer(u)
	// r.Print()

	hashid, _ = r.GetRootHash()
	s := swarmdb.NewBPlusTreeDB(u, getSwarmDB(t), hashid, swarmdb.CT_STRING, false, swarmdb.CT_STRING)
	g, _, _ := s.Get(u, []byte("000008"))
	fmt.Printf("Get(000008): %v\n", string(g))

	h, _, _ := s.Get(u, []byte("000001"))
	fmt.Printf("Get(000001): %v\n", string(h))
	// s.Print()

	// ENUMERATOR
	res, _, _ := r.Seek(u, []byte("000004"))
	records := 0
	for k, v, err := res.Next(u); err == nil; k, v, err = res.Next(u) {
		fmt.Printf(" *string*> %d K: %s V: %v\n", records, swarmdb.KeyToString(swarmdb.CT_STRING, k), string(v))
		records++
	}
	fmt.Printf("---- TestPutString DONE (%d records)\n", records)
}

func TestPutFloat(t *testing.T) {
	fmt.Printf("---- TestPutFloat: generate 20 floats and enumerate them\n")
	config, _ := swarmdb.LoadSWARMDBConfig(swarmdb.SWARMDBCONF_FILE)
	swarmdb.NewKeyManager(&config)
	u := config.GetSWARMDBUser()

	hashid := make([]byte, 32)
	r := swarmdb.NewBPlusTreeDB(u, getSwarmDB(t), hashid, swarmdb.CT_FLOAT, false, swarmdb.CT_STRING)

	r.StartBuffer(u)
	vals := rand.Perm(20)
	// write 20 values into B-tree (only kept in memory)
	for _, i := range vals {
		k := swarmdb.FloatToByte(float64(i) + .314159)
		v := []byte(fmt.Sprintf("valueof%06x", i))
		// fmt.Printf("Insert %d %v %v\n", i, swarmdb.KeyToString(swarmdb.CT_FLOAT, k), string(v))
		r.Put(u, k, v)
	}
	// this writes B+tree to SWARM
	r.FlushBuffer(u)
	// r.Print()

	hashid, _ = r.GetRootHash()
	s := swarmdb.NewBPlusTreeDB(u, getSwarmDB(t), hashid, swarmdb.CT_FLOAT, false, swarmdb.CT_STRING)
	// ENUMERATOR
	res, _, _ := s.Seek(u, swarmdb.FloatToByte(3.14159))
	records := 0
	for k, v, err := res.Next(u); err == nil; k, v, err = res.Next(u) {
		fmt.Printf(" *float*> %d: K: %s V: %v\n", records, swarmdb.KeyToString(swarmdb.CT_FLOAT, k), string(v))
		records++
	}
}

func TestSetGetString(t *testing.T) {
	config, _ := swarmdb.LoadSWARMDBConfig(swarmdb.SWARMDBCONF_FILE)
	swarmdb.NewKeyManager(&config)
	u := config.GetSWARMDBUser()

	hashid := make([]byte, 32)
	r := swarmdb.NewBPlusTreeDB(u, getSwarmDB(t), hashid, swarmdb.CT_STRING, false, swarmdb.CT_STRING)

	// put
	key := []byte("42")
	val := swarmdb.SHA256("314")
	r.Put(u, key, val)

	// check put with get
	g, ok, err := r.Get(u, key)
	if !ok || err != nil {
		t.Fatal(ok)
	}
	if bytes.Compare(g, val) != 0 {
		t.Fatal(g, val)
	}
	//r.Print()
	hashid, _ = r.GetRootHash()

	// r2 put
	r2 := swarmdb.NewBPlusTreeDB(u, getSwarmDB(t), hashid, swarmdb.CT_STRING, false, swarmdb.CT_STRING)
	val2 := swarmdb.SHA256("278")
	r2.Put(u, key, val2)
	//r2.Print()

	// check put with get
	g2, ok, err := r2.Get(u, key)
	if !ok || err != nil {
		t.Fatal(ok)
	}
	if bytes.Compare(g2, val2) != 0 {
		t.Fatal(g2, val2)
	}
	hashid, _ = r2.GetRootHash()

	// r3 put
	r3 := swarmdb.NewBPlusTreeDB(u, getSwarmDB(t), hashid, swarmdb.CT_STRING, false, swarmdb.CT_STRING)
	key2 := []byte("420")
	val3 := swarmdb.SHA256("bbb")
	r3.Put(u, key2, val3)

	// check put with get
	g3, ok, err := r3.Get(u, key2)
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
	config, _ := swarmdb.LoadSWARMDBConfig(swarmdb.SWARMDBCONF_FILE)
	swarmdb.NewKeyManager(&config)
	u := config.GetSWARMDBUser()

	const N = 4
	hashid := make([]byte, 32)
	for _, x := range []int{0, -1, 0x555555, 0xaaaaaa, 0x333333, 0xcccccc, 0x314159} {
		r := swarmdb.NewBPlusTreeDB(u, getSwarmDB(t), hashid, swarmdb.CT_INTEGER, false, swarmdb.CT_STRING)

		a := make([]int, N)
		for i := range a {
			a[i] = (i ^ x) << 1
		}
		fmt.Printf("%v\n", a)
		for _, k := range a {
			r.Put(u, swarmdb.IntToByte(k), swarmdb.SHA256(fmt.Sprintf("%v", k^x)))
		}

		for i, k := range a {
			v, ok, err := r.Get(u, swarmdb.IntToByte(k))
			if !ok || err != nil {
				t.Fatal(i, k, v, ok)
			}

			val := swarmdb.SHA256(fmt.Sprintf("%v", k^x))
			if bytes.Compare([]byte(val), v) != 0 {
				t.Fatal(i, val, v)
			}

			k |= 1

			_, ok, _ = r.Get(u, swarmdb.IntToByte(k))
			if ok {
				t.Fatal(i, k)
			}

		}

		for _, k := range a {
			r.Put(u, swarmdb.IntToByte(k), swarmdb.SHA256(fmt.Sprintf("%v", k^x+42)))
		}

		for i, k := range a {
			v, ok, err := r.Get(u, swarmdb.IntToByte(k))
			if !ok || err != nil {
				t.Fatal(i, k, v, ok)
			}

			val := swarmdb.SHA256(fmt.Sprintf("%v", k^x+42))
			if bytes.Compare([]byte(val), v) != 0 {
				t.Fatal(i, v, val)
			}

			k |= 1
			_, ok, _ = r.Get(u, swarmdb.IntToByte(k))
			if ok {
				t.Fatal(i, k)
			}
		}

	}
}

func TestDelete0(t *testing.T) {
	// TODO: make this test work!
	t.SkipNow();

	config, _ := swarmdb.LoadSWARMDBConfig(swarmdb.SWARMDBCONF_FILE)
	swarmdb.NewKeyManager(&config)
	u := config.GetSWARMDBUser()

	hashid := make([]byte, 32)
	r := swarmdb.NewBPlusTreeDB(u, getSwarmDB(t), hashid, swarmdb.CT_INTEGER, false, swarmdb.CT_STRING)

	key0 := swarmdb.IntToByte(0)
	key1 := swarmdb.IntToByte(1)

	val0 := swarmdb.SHA256("0")
	val1 := swarmdb.SHA256("1")

	if ok, _ := r.Delete(u, key0); ok {
		t.Fatal(ok)
	}

	r.Put(u, key0, val0)
	if ok, _ := r.Delete(u, key1); ok {
		t.Fatal(ok)
	}

	if ok, _ := r.Delete(u, key0); !ok {
		t.Fatal(ok)
	}

	if ok, _ := r.Delete(u, key0); ok {
		t.Fatal(ok)
	}

	r.Put(u, key0, val0)
	r.Put(u, key1, val1)
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

	r.Put(u, key0, val0)
	r.Put(u, key1, val1)
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

func TestDelete1(t *testing.T) {
	// TODO: make this test work!
	t.SkipNow();

	config, _ := swarmdb.LoadSWARMDBConfig(swarmdb.SWARMDBCONF_FILE)
	swarmdb.NewKeyManager(&config)
	u := config.GetSWARMDBUser()

	hashid := make([]byte, 32)
	const N = 130
	for _, x := range []int{0, -1, 0x555555, 0xaaaaaa, 0x333333, 0xcccccc, 0x314159} {
		r := swarmdb.NewBPlusTreeDB(u, getSwarmDB(t), hashid, swarmdb.CT_INTEGER, false, swarmdb.CT_STRING)
		a := make([]int, N)
		for i := range a {
			a[i] = (i ^ x) << 1
		}
		for _, k := range a {
			r.Put(u, swarmdb.IntToByte(k), swarmdb.SHA256("0"))
		}

		for i, k := range a {
			ok, _ := r.Delete(u, swarmdb.IntToByte(k))
			if !ok {
				fmt.Printf("YIPE%s\n", k)
				t.Fatal(i, x, k)
			}
		}
	}
}

func TestDelete2(t *testing.T) {
	// TODO: make this test work!
	t.SkipNow();

	config, _ := swarmdb.LoadSWARMDBConfig(swarmdb.SWARMDBCONF_FILE)
	swarmdb.NewKeyManager(&config)
	u := config.GetSWARMDBUser()

	const N = 100
	hashid := make([]byte, 32)
	for _, x := range []int{0, -1, 0x555555, 0xaaaaaa, 0x333333, 0xcccccc, 0x314159} {
		r := swarmdb.NewBPlusTreeDB(u, getSwarmDB(t), hashid, swarmdb.CT_INTEGER, false, swarmdb.CT_STRING)
		a := make([]int, N)
		rng := rng()
		for i := range a {
			a[i] = (rng.Next() ^ x) << 1
		}
		for _, k := range a {
			r.Put(u, swarmdb.IntToByte(k), swarmdb.SHA256("0"))
		}
		for i, k := range a {
			ok, _ := r.Delete(u, swarmdb.IntToByte(k))
			if !ok {
				t.Fatal(i, x, k)
			}
		}
	}
}
