package swarmdb

import (
	"fmt"
	"github.com/ethereum/go-ethereum/swarmdb/packages"
	// "bytes"
	// "sync"
	// "math/rand"
	"github.com/ethereum/go-ethereum/swarm/api"
	"github.com/ethereum/go-ethereum/swarm/storage"
	"testing"
)

const (
	OWNER = "0x34c7fc051eae78f8c37b82387a50a5458b8f7018"
	TABLENAME = "testtable"
	COLUMNNAME = "id"
	DATA_DIR = "/tmp/joy"
)
/*

func internalTable(tableName string, r OrderedDatabase) {
	// open table [only gets the root node]
	vals := rand.Perm(20)
	// write 20 values into B-tree (only kept in memory)
	r.StartBuffer()
	for _, i := range vals {
		k := []byte(fmt.Sprintf("%06x", i))
		v := []byte(fmt.Sprintf("valueof%06x", i))
		fmt.Printf("Insert %d %v %v\n", i, string(k), string(v))
		r.Put(k, v)
	}
	// this writes B+tree to SWARM
	r.FlushBuffer() // tableName
	r.Print()

	r.StartBuffer()
	r.Put([]byte("000004"), []byte("Sammy2"))
	r.Put([]byte("000009"), []byte("Happy2"))
	r.Put([]byte("00000e"), []byte("Leroy2"))
	g, _, _ := r.Get([]byte("00000d"))
	fmt.Printf("GET: %v\n", g)
	r.FlushBuffer()
	r.Print()

	// ENUMERATOR
	res, _, _ := r.Seek([]byte("000004"))
	for k, v, err := res.Next(); err == nil; k, v, err = res.Next() {
		fmt.Printf(" K: %v V: %v\n", string(k), string(v))
	}
}
*/
func TestPut(t *testing.T) {
	fmt.Printf("Put Test START\n");
	dpa, err := storage.NewLocalDPA(DATA_DIR)
	if err != nil {
		t.Fatal("no dpa")
	}
	dpa.Start() // missing
	api := api.NewApi(dpa, nil)
	r := swarmdb.NewBPlusTreeDB(api)

	r.Open([]byte(OWNER), []byte(TABLENAME), []byte(COLUMNNAME))
	r.StartBuffer()
	r.Put([]byte("000004"), []byte("Minnie"))
	r.Put([]byte("000003"), []byte("Sammy"))
	r.Put([]byte("000002"), []byte("Bertie"))
	r.Put([]byte("000001"), []byte("Happy"))
	r.Print()
	r.FlushBuffer() // tableName
	r.Close()
	fmt.Printf("Put Test DONE\n----\n");
}
/*
func TestOpen(t *testing.T) {
	fmt.Printf("Open Test Start\n");
	r := BPlusTree()
	r.Open([]byte(OWNER), []byte(TABLENAME), []byte(COLUMNNAME))
	r.Print()
	fmt.Printf("Open Test DONE\n----\n");
}
*/


