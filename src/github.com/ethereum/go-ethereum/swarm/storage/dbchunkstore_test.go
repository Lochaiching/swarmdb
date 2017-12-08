package storage

import (
	"bytes"
	"fmt"
	"github.com/ethereum/go-ethereum/swarm/storage"
	"testing"
)

func TestDBChunkStore(t *testing.T) {
	store, err := storage.NewDBChunkStore("chunks.db")
	if err != nil {
		t.Fatal("Failure to open NewDBChunkStore")
	}

	r := []byte("randombytes23412341")
	v := make([]byte, 4096)
	copy(v, r)

	// StoreChunk
	k, err := store.StoreChunk(r)
	if err == nil {
		t.Fatal("Failure to generate StoreChunk Err", k, v)
	} else {
		fmt.Printf("SUCCESS in StoreChunk Err (input only has %d bytes)\n", len(r))
	}

	k, err1 := store.StoreChunk(v)
	if err1 != nil {
		t.Fatal("Failure to StoreChunk", k, v)
	} else {
		fmt.Printf("SUCCESS in StoreChunk:  %x => %v\n", string(k), string(v))
	}
	// RetrieveChunk
	val, err := store.RetrieveChunk(k)
	if err != nil {
		t.Fatal("Failure to RetrieveChunk: Failure to retrieve", k, v, val)
	}
	if bytes.Compare(val, v) != 0 {
		t.Fatal("Failure to RetrieveChunk: Incorrect match", k, v, val)
	} else {
		fmt.Printf("SUCCESS in RetrieveChunk:  %x => %v\n", string(k), string(v))
	}

	// StoreKChunk
	err2 := store.StoreKChunk(k, v)
	if err2 != nil {
		t.Fatal("Failure to StoreKChunk", k, v)
	} else {
		fmt.Printf("SUCCESS in StoreKChunk:  %x => %v\n", string(k), string(v))
	}

	err3 := store.StoreKChunk(k, r)
	if err3 == nil {
		t.Fatal("Failure to generate StoreKChunk Err", k, r)
	} else {
		fmt.Printf("SUCCESS in StoreKChunk Err (input only has %d bytes)\n", len(r))
	}
}
