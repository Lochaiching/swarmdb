package swarmdb_test

import (
	"bytes"
	"fmt"
	"github.com/ethereum/go-ethereum/swarmdb"
	"testing"
)

func TestDBChunkStore(t *testing.T) {
	config, _ := swarmdb.LoadSWARMDBConfig(swarmdb.SWARMDBCONF_FILE)
	swarmdb.NewKeyManager(&config)
	u := config.GetSWARMDBUser()

	store, err := swarmdb.NewDBChunkStore("/tmp/chunks.db")
	if err != nil {
		t.Fatal("Failure to open NewDBChunkStore")
	}

	r := []byte("randombytes23412341")
	v := make([]byte, 4096)
	copy(v, r)

	// encrypted := int(1)

	// StoreChunk
	k, err := store.StoreChunk(u, r, 1)
	if err == nil {
		t.Fatal("Failure to generate StoreChunk Err", k, v)
	} else {
		fmt.Printf("SUCCESS in StoreChunk Err (input only has %d bytes)\n", len(r))
	}

	k, err1 := store.StoreChunk(u, v, 1)
	if err1 != nil {
		t.Fatal("Failure to StoreChunk", k, v)
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

	// StoreKChunk
	/*
			Need to simulate building sdata for KChunk to test appropriately
		kdb := swarmdb.NewKademliaDB(store)
		kChunk := kdb.BuildSData(v)
		fmt.Printf("StoreKChunk storing [%s]", v)
		err2 := store.StoreKChunk(k, v, encrypted)
		if err2 != nil {
			t.Fatal("Failure to StoreKChunk ->", k, v, encrypted)
		} else {
			fmt.Printf("SUCCESS in StoreKChunk:  %x => %v\n", string(k), string(v))
		}

		// RetrieveKChunk
		//	fmt.Printf("\nBEFORE RetrieveKChunk:  %x => %v\n", string(k), string(v))
		valK, errK := store.RetrieveKChunk(k)
		//	fmt.Printf("\nAFTER RetrieveKChunk:  %x => %v\n", string(k), string(v))
		if errK != nil {
			t.Fatal("Failure to RetrieveChunk: Failure to retrieve", k, v, valK)
		}
		if bytes.Compare(valK, v) != 0 {
			fmt.Printf("Failure to RetrieveChunk: Incorrect match k[%s] v[%s], valK[%s]", k, v, valK)
			t.Fatal("Failure to RetrieveChunk: Incorrect match", k, v, valK)
		} else {
			fmt.Printf("SUCCESS in RetrieveChunk:  %x => %v\n", string(k), string(v))
		}

		err3 := store.StoreKChunk(k, r, encrypted)
		if err3 == nil {
			t.Fatal("Failure to generate StoreKChunk Err", k, r)
		} else {
			fmt.Printf("SUCCESS in StoreKChunk Err (input only has %d bytes)\n", len(r))
		}
	*/
}
