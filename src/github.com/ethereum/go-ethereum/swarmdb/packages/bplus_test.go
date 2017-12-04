package swarmdb

import (
	"fmt"
	"github.com/ethereum/go-ethereum/swarmdb/packages"
	// "bytes"
	// "sync"
	// "math/rand"
	// "github.com/ethereum/go-ethereum/swarm/api"
	//"github.com/ethereum/go-ethereum/swarm/storage"
	"testing"
)

const (
	OWNER = "0x34c7fc051eae78f8c37b82387a50a5458b8f7018"
	TABLENAME = "testtable"
	COLUMNNAME = "id"
	DATA_DIR = "/tmp/joy"
)

func aTestPut(t *testing.T) {
	fmt.Printf("Put Test START\n");
	dpa, err := storage.NewLocalDPA(DATA_DIR)
	if err != nil {
		t.Fatal("no dpa")
	}
	dpa.Start() // missing
	api := api.NewApi(dpa, nil)
	r := swarmdb.NewBPlusTreeDB(api)

	r.Open([]byte(OWNER), []byte(TABLENAME), []byte(COLUMNNAME))
	//r.StartBuffer()
	r.Put([]byte("000001"), []byte("rawJson"))
	r.Print()
	g, _, _ := r.Get([]byte("000001"))
	fmt.Printf("g: %s \n----\n", g);
	r.Close()
	//r.FlushBuffer() // tableName
} 
/*
func TestGet(t *testing.T) {
	fmt.Printf("Get Test START\n");
	dpa, err := storage.NewLocalDPA(DATA_DIR)
	if err != nil {
		t.Fatal("no dpa")
	}
	dpa.Start() // missing
	api := api.NewApi(dpa, nil)
	r := swarmdb.NewBPlusTreeDB(api)

	r.Open([]byte(OWNER), []byte(TABLENAME), []byte(COLUMNNAME))
	g, _, _ := r.Get([]byte("000001"))
	fmt.Printf("g: %s \n----\n", g);
	r.Close()
	fmt.Printf("Get Test DONE\n----\n");
}
*/

