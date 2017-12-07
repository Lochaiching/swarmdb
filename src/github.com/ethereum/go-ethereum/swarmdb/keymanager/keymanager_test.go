package keymanager 

import (
	"fmt"
	"testing" 
	"github.com/ethereum/go-ethereum/swarmdb/keymanager"
)

func TestKeyManager(t *testing.T) {
	//km, err := NewKeyManager("chunks.db")
	km, err := keymanager.NewKeyManager("chunks.db")
	if err != nil {
		t.Fatal("Failure to open NewDBChunkStore")
	}

	r := []byte("randombytes23412341")

	encData := km.EncryptData( r )
 		fmt.Printf("Encrypted data is [%v][%s]", encData, encData)
	decData := km.DecryptData( encData )
 		fmt.Printf("Decrypted data is [%v][%s]", decData, decData)
/*
	k, err1 := store.StoreChunk(v)
	if err1 != nil {
		t.Fatal("Failure to StoreChunk", k, v)
	} else {
 		fmt.Printf("SUCCESS in StoreChunk:  %x => %v\n", string(k), string(v))
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
*/

}
