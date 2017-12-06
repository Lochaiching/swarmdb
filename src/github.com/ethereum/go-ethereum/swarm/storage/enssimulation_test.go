package storage

import (
	"fmt"
	"bytes"
	"testing" 
	"github.com/ethereum/go-ethereum/swarm/storage"
)

func TestENSSimulation(t *testing.T) {
	store, err := storage.NewENSSimulation("/tmp/ens.db")
	if err != nil {
		t.Fatal("failure to open ENSSimulation")
	}
	indexName := []byte("contact")
	roothash := []byte("contactroothash")
	store.StoreIndexRootHash(indexName, roothash)

	val, err := store.GetIndexRootHash(indexName)
	if err != nil {
	} 
	if bytes.Compare(val, roothash) != 0 {
		t.Fatal("Err1", indexName, roothash, val)
	} else {
		fmt.Printf("SUCCESS1:  %v => %v\n", string(indexName), string(val))
	}
}
