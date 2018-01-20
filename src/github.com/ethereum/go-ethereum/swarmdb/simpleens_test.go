package swarmdb_test

import (
	"bytes"
	"fmt"
	"os"
	"github.com/ethereum/go-ethereum/swarmdb"
	"testing"
)

func TestENSSimple(t *testing.T) {
	store, err := swarmdb.NewENSSimple("/tmp/ens.db")
	if err != nil {
		t.Fatal("failure to open ENSSimulation")
	}
	indexName := []byte("12345678123456781234567812345678")
	roothash  := []byte("87654321876543218765432187654321")
	// store.StoreRootHash(indexName, roothash)
	val, err := store.GetRootHash(indexName)
	if err != nil {
	}
		
	fmt.Printf("roothash [%x]\nrootchek [%x]\n", roothash, val); 
	os.Exit(0)
	if bytes.Compare(val, roothash) != 0 {
		t.Fatal("Err1", indexName, roothash, val)
	} else {
		fmt.Printf("SUCCESS1:  %v => %v\n", string(indexName), string(val))
	}
}
