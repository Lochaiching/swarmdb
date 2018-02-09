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
	"bytes"
	"crypto/sha256"
	"fmt"
	"github.com/syndtr/goleveldb/leveldb"
	"swarmdb"
	"testing"
)

// chunk storage in SQLite3 with encryption
func BenchmarkStoreSQLiteSimple1(b *testing.B) {
	config, _ := swarmdb.LoadSWARMDBConfig(swarmdb.SWARMDBCONF_FILE)
	swarmdb.NewKeyManager(&config)
	u := config.GetSWARMDBUser()

	store, err := swarmdb.NewDBChunkStore("chunks.db")
	if err != nil {
		b.Fatal("Failure to open NewDBChunkStore")
	}

	v := make([]byte, 4096)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		r := []byte(fmt.Sprintf("%d", i))
		copy(v, r)
		_, err := store.StoreChunkSimple(u, v, 1)
		if err != nil {
		} else {
			// fmt.Printf("%d %x\n", i, k)
		}
	}
}

// chunk storage in SQLite3 without encryption
func BenchmarkStoreSQLiteSimple0(b *testing.B) {
	config, _ := swarmdb.LoadSWARMDBConfig(swarmdb.SWARMDBCONF_FILE)
	swarmdb.NewKeyManager(&config)
	u := config.GetSWARMDBUser()

	store, err := swarmdb.NewDBChunkStore("chunks.db")
	if err != nil {
		b.Fatal("Failure to open NewDBChunkStore")
	}

	v := make([]byte, 4096)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		r := []byte(fmt.Sprintf("%d", i))
		copy(v, r)
		_, err := store.StoreChunkSimple(u, v, 0)
		if err != nil {
		} else {
			// fmt.Printf("%d %x\n", i, k)
		}
	}
}

// chunk storage in SQLite3 with encryption
func BenchmarkStoreSQLite1(b *testing.B) {
	config, _ := swarmdb.LoadSWARMDBConfig(swarmdb.SWARMDBCONF_FILE)
	swarmdb.NewKeyManager(&config)
	u := config.GetSWARMDBUser()

	store, err := swarmdb.NewDBChunkStore("chunks.db")
	if err != nil {
		b.Fatal("Failure to open NewDBChunkStore")
	}

	v := make([]byte, 4096)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		r := []byte(fmt.Sprintf("%d", i))
		copy(v, r)
		_, err := store.StoreChunk(u, v, 1)
		if err != nil {
		} else {
			// fmt.Printf("%d %x\n", i, k)
		}
	}
}

// chunk storage in SQLite3 without encryption
func BenchmarkStoreSQLite0(b *testing.B) {
	config, _ := swarmdb.LoadSWARMDBConfig(swarmdb.SWARMDBCONF_FILE)
	swarmdb.NewKeyManager(&config)
	u := config.GetSWARMDBUser()

	store, err := swarmdb.NewDBChunkStore("chunks.db")
	if err != nil {
		b.Fatal("Failure to open NewDBChunkStore")
	}

	v := make([]byte, 4096)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		r := []byte(fmt.Sprintf("%d", i))
		copy(v, r)
		_, err := store.StoreChunk(u, v, 0)
		if err != nil {
		} else {
			// fmt.Printf("%d %x\n", i, k)
		}
	}
}

// chunk storage in leveldb with encryption
func BenchmarkStoreLevelDB1(b *testing.B) {
	config, _ := swarmdb.LoadSWARMDBConfig(swarmdb.SWARMDBCONF_FILE)
	swarmdb.NewKeyManager(&config)
	u := config.GetSWARMDBUser()

	store, err := swarmdb.NewDBChunkStore("chunks.db")
	if err != nil {
		b.Fatal("Failure to open NewDBChunkStore")
	}

	db, err := leveldb.OpenFile("/tmp", nil)
	defer db.Close()

	v := make([]byte, 4096)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		r := []byte(fmt.Sprintf("%d", i))
		copy(v, r)
		h := sha256.New()
		h.Write([]byte(v))
		chunkVal := store.GetKeyManager().EncryptData(u, v)
		key := h.Sum(nil)
		err = db.Put(key, chunkVal, nil)
		if err != nil {
		} else {
			// fmt.Printf("%d %x\n", i, k)
		}
	}
}

// chunk storage in leveldb without encryption
func BenchmarkStoreLevelDB0(b *testing.B) {
	db, err := leveldb.OpenFile("/tmp", nil)
	defer db.Close()

	v := make([]byte, 4096)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		r := []byte(fmt.Sprintf("%d", i))
		copy(v, r)
		h := sha256.New()
		h.Write([]byte(v))
		key := h.Sum(nil)
		err = db.Put(key, v, nil)
		if err != nil {
		} else {
			// fmt.Printf("%d %x\n", i, k)
		}
	}
}

// chunk storage in files with encryption
func BenchmarkStoreFile1(b *testing.B) {
	config, _ := swarmdb.LoadSWARMDBConfig(swarmdb.SWARMDBCONF_FILE)
	swarmdb.NewKeyManager(&config)
	u := config.GetSWARMDBUser()

	store, err := swarmdb.NewDBChunkStore("chunks.db")
	if err != nil {
		b.Fatal("Failure to open NewDBChunkStore")
	}

	v := make([]byte, 4096)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		r := []byte(fmt.Sprintf("%d", i))
		copy(v, r)
		_, err := store.StoreChunkFile(u, v, 1)
		if err != nil {
		} else {
			// fmt.Printf("%d %x\n", i, k)
		}
	}
}

// chunk storage in files without encryption
func BenchmarkStoreFile0(b *testing.B) {
	config, _ := swarmdb.LoadSWARMDBConfig(swarmdb.SWARMDBCONF_FILE)
	swarmdb.NewKeyManager(&config)
	u := config.GetSWARMDBUser()

	store, err := swarmdb.NewDBChunkStore("chunks.db")
	if err != nil {
		b.Fatal("Failure to open NewDBChunkStore")
	}

	v := make([]byte, 4096)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		r := []byte(fmt.Sprintf("%d", i))
		copy(v, r)
		_, err := store.StoreChunkFile(u, v, 0)
		if err != nil {
		} else {
			// fmt.Printf("%d %x\n", i, k)
		}
	}
}

// chunk computation with encryption
func BenchmarkStoreDummy1(b *testing.B) {
	config, _ := swarmdb.LoadSWARMDBConfig(swarmdb.SWARMDBCONF_FILE)
	swarmdb.NewKeyManager(&config)
	u := config.GetSWARMDBUser()

	store, err := swarmdb.NewDBChunkStore("chunks.db")
	if err != nil {
		b.Fatal("Failure to open NewDBChunkStore")
	}

	v := make([]byte, 4096)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		r := []byte(fmt.Sprintf("%d", i))
		copy(v, r)
		_, err := store.StoreChunkDummy(u, v, 1)
		if err != nil {
		} else {
			// fmt.Printf("%d %x\n", i, k)
		}
	}
}

// chunk computation without encryption (just hashing)
func BenchmarkStoreDummy0(b *testing.B) {
	config, _ := swarmdb.LoadSWARMDBConfig(swarmdb.SWARMDBCONF_FILE)
	swarmdb.NewKeyManager(&config)
	u := config.GetSWARMDBUser()

	store, err := swarmdb.NewDBChunkStore("chunks.db")
	if err != nil {
		b.Fatal("Failure to open NewDBChunkStore")
	}

	v := make([]byte, 4096)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		r := []byte(fmt.Sprintf("%d", i))
		copy(v, r)
		_, err := store.StoreChunkDummy(u, v, 0)
		if err != nil {
		} else {
			// fmt.Printf("%d %x\n", i, k)
		}
	}
}

func TestDBChunkStore(t *testing.T) {
	config, _ := swarmdb.LoadSWARMDBConfig(swarmdb.SWARMDBCONF_FILE)
	swarmdb.NewKeyManager(&config)
	u := config.GetSWARMDBUser()

	store, err := swarmdb.NewDBChunkStore("chunks.db")
	if err != nil {
		t.Fatal("Failure to open NewDBChunkStore")
	}

	r := []byte("randombytes23412341")
	v := make([]byte, 4096)
	copy(v, r)

	// StoreChunk
	k, err := store.StoreChunk(u, r, 1)
	if err == nil {
		t.Fatal("Failure to generate StoreChunk Err", k, v)
	} else {
		fmt.Printf("SUCCESS in StoreChunk Err (input only has %d bytes)\n", len(r))
	}

	k, err1 := store.StoreChunk(u, v, 1)
	if err1 != nil {
		t.Fatal("Failure to StoreChunk", k, v, err1)
	} else {
		fmt.Printf("SUCCESS in StoreChunk:  %x => %v\n", string(k), string(v))
	}
	// RetrieveChunk
	val, err := store.RetrieveChunk(u, k)
	if err != nil {
		t.Fatal("Failure to RetrieveChunk: Failure to retrieve", k, v, val)
	}
	if bytes.Compare(val, v) != 0 {
		t.Fatal("Failure to RetrieveChunk: Incorrect match", k, v, val)
	} else {
		fmt.Printf("SUCCESS in RetrieveChunk:  %x => %v\n", string(k), string(v))
	}

}
