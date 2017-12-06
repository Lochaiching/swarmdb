// not working 
// I was testing (yaron)
// can be deleted



// Copyright 2016 The go-ethereum Authors
// This file is part of the go-ethereum library.
//
// The go-ethereum library is free software: you can redistribute it and/or modify
// it under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// The go-ethereum library is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU Lesser General Public License for more details.
//
// You should have received a copy of the GNU Lesser General Public License
// along with the go-ethereum library. If not, see <http://www.gnu.org/licenses/>.

package main

import (
	"bytes"
	"io/ioutil"
	//"testing"
    "fmt"
	"github.com/ethereum/go-ethereum/common"
	st "github.com/ethereum/go-ethereum/swarm/storage"
)


const defaultHash = "SHA3" //SHA256 here does not work
const defaultDbCapacity = 5000000
const defaultRadius = 0 //some note about not yet used?

func initDbStore() *st.DbStore {
	dir, err := ioutil.TempDir("", "bzz-storage-test")
	if err != nil {
		//t.Fatal(err)
		fmt.Printf("TempDir err:%s\n\n", err)
	}
	m, err := st.NewDbStore(dir, st.MakeHashFunc(defaultHash), defaultDbCapacity, defaultRadius)
	if err != nil {
		//t.Fatal("can't create store:", err)
		fmt.Printf("can't create store:%s\n\n", err)
	}
	return m
}
/*
func testDbStore(l int64, branches int64, t *testing.T) {
	m := initDbStore(t)
	defer m.Close()
	st.testStore(m, l, branches, t)
}

func TestDbStore128_0x1000000(t *testing.T) {
	testDbStore(0x1000000, 128, t)
}

func TestDbStore128_10000_(t *testing.T) {
	testDbStore(10000, 128, t)
}

func TestDbStore128_1000_(t *testing.T) {
	testDbStore(1000, 128, t)
}

func TestDbStore128_100_(t *testing.T) {
	testDbStore(100, 128, t)
}

func TestDbStore2_100_(t *testing.T) {
	testDbStore(100, 2, t)
}


func main() {
	t *testing.T 
	testDbStore(100, 2, t)
}

func TestDbStoreNotFound(t *testing.T) {
	m := initDbStore(t)
	defer m.Close()
	_, err := m.Get(ZeroKey)
	if err != st.notFound {
		t.Errorf("Expected notFound, got %v", err)
	}
}
*/
func main() {
//	t *testing.T
	m := initDbStore()
	defer m.Close()
	keys := []st.Key{
		st.Key(common.Hex2Bytes("0000000000000000000000000000000000000000000000000000000000000000")),
		st.Key(common.Hex2Bytes("4000000000000000000000000000000000000000000000000000000000000000")),
		st.Key(common.Hex2Bytes("5000000000000000000000000000000000000000000000000000000000000000")),
		st.Key(common.Hex2Bytes("3000000000000000000000000000000000000000000000000000000000000000")),
		st.Key(common.Hex2Bytes("2000000000000000000000000000000000000000000000000000000000000000")),
		st.Key(common.Hex2Bytes("1000000000000000000000000000000000000000000000000000000000000000")),
	}
	for _, key := range keys {
		m.Put(st.NewChunk(key, nil))
	}
	it, err := m.NewSyncIterator(st.DbSyncState{
		Start: st.Key(common.Hex2Bytes("1000000000000000000000000000000000000000000000000000000000000000")),
		Stop:  st.Key(common.Hex2Bytes("4000000000000000000000000000000000000000000000000000000000000000")),
		First: 2,
		Last:  4,
	})
	if err != nil {
		//t.Fatalf("unexpected error creating NewSyncIterator")
		fmt.Printf("unexpected error creating NewSyncIterator:%s\n\n", err)
	}

	var chunk st.Key
	var res []st.Key
	for {
		chunk = it.Next()
		if chunk == nil {
			break
		}
		res = append(res, chunk)
	}
	if len(res) != 1 {
		//t.Fatalf("Expected 1 chunk, got %v: %v", len(res), res)
		fmt.Printf("Expected 1 chunk, got %v: %v\n\n", len(res), res)
	}
	if !bytes.Equal(res[0][:], keys[3]) {
		//t.Fatalf("Expected %v chunk, got %v", keys[3], res[0])
		fmt.Printf("Expected %v chunk, got %v\n\n", keys[3], res[0])
	}

	if err != nil {
		//t.Fatalf("unexpected error creating NewSyncIterator")
		fmt.Printf("unexpected error creating NewSyncIterator:%s\n\n", err)
	}

	it, err = m.NewSyncIterator(st.DbSyncState{
		Start: st.Key(common.Hex2Bytes("1000000000000000000000000000000000000000000000000000000000000000")),
		Stop:  st.Key(common.Hex2Bytes("5000000000000000000000000000000000000000000000000000000000000000")),
		First: 2,
		Last:  4,
	})

	res = nil
	for {
		chunk = it.Next()
		if chunk == nil {
			break
		}
		res = append(res, chunk)
	}
	if len(res) != 2 {
		//t.Fatalf("Expected 2 chunk, got %v: %v", len(res), res)
		fmt.Printf("Expected 2 chunk, got %v: %v\n\n", len(res), res)
	}
	if !bytes.Equal(res[0][:], keys[3]) {
		//t.Fatalf("Expected %v chunk, got %v", keys[3], res[0])
		fmt.Printf("Expected %v chunk, got %v\n\n", keys[3], res[0])
	}
	if !bytes.Equal(res[1][:], keys[2]) {
		//t.Fatalf("Expected %v chunk, got %v", keys[2], res[1])
		fmt.Printf("Expected %v chunk, got %v\n\n", keys[2], res[1])
	}

	if err != nil {
		//t.Fatalf("unexpected error creating NewSyncIterator")
	}

	it, _ = m.NewSyncIterator(st.DbSyncState{
		Start: st.Key(common.Hex2Bytes("1000000000000000000000000000000000000000000000000000000000000000")),
		Stop:  st.Key(common.Hex2Bytes("4000000000000000000000000000000000000000000000000000000000000000")),
		First: 2,
		Last:  5,
	})
	res = nil
	for {
		chunk = it.Next()
		if chunk == nil {
			break
		}
		res = append(res, chunk)
	}
	if len(res) != 2 {
		//t.Fatalf("Expected 2 chunk, got %v", len(res))
		fmt.Printf("Expected 2 chunk, got %v\n\n", len(res))
	}
	if !bytes.Equal(res[0][:], keys[4]) {
		//t.Fatalf("Expected %v chunk, got %v", keys[4], res[0])
		fmt.Printf("Expected %v chunk, got %v\n\n", keys[4], res[0])
	}
	if !bytes.Equal(res[1][:], keys[3]) {
		//t.Fatalf("Expected %v chunk, got %v", keys[3], res[1])
		fmt.Printf("Expected %v chunk, got %v\n\n", keys[3], res[1])
	}

	it, _ = m.NewSyncIterator(st.DbSyncState{
		Start: st.Key(common.Hex2Bytes("2000000000000000000000000000000000000000000000000000000000000000")),
		Stop:  st.Key(common.Hex2Bytes("4000000000000000000000000000000000000000000000000000000000000000")),
		First: 2,
		Last:  5,
	})
	res = nil
	for {
		chunk = it.Next()
		if chunk == nil {
			break
		}
		res = append(res, chunk)
	}
	if len(res) != 1 {
		//t.Fatalf("Expected 1 chunk, got %v", len(res))
	}
	if !bytes.Equal(res[0][:], keys[3]) {
		//t.Fatalf("Expected %v chunk, got %v", keys[3], res[0])
	}
}
