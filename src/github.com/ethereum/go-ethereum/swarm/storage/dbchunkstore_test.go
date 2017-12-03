package storage

import (
	"fmt"
	"bytes"
	"testing" 
	"github.com/ethereum/go-ethereum/swarm/storage"
)

func TestDBChunkStore(t *testing.T) {
	store, err := storage.NewDBChunkStore("chunks.db")
	if err != nil {
		t.Fatal("Failure to open NewDBChunkStore")
	}
	v := []byte("randombytes")
	k, err := store.StoreChunk(v)
	if err != nil {
		t.Fatal("Failure to StoreChunk", k, v)
	} 
	val, err := store.RetrieveChunk(k)
	if err != nil {
		t.Fatal("Failure to RetrieveChunk: Failure to retrieve", k, v, val)
	} 
	if bytes.Compare(val, v) != 0 {
		t.Fatal("Failure to RetrieveChunk: Incorrect match", k, v, val)
	} else {
		fmt.Printf("SUCCESS in RetrieveChunk:  %x => %v\n", string(k), string(v))
	}
}
