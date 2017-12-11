package common

import (
	"fmt"
	//"bytes"
	common "github.com/ethereum/go-ethereum/swarmdb"
	"testing"
)

var (
	testDBPath = "chunks.db"
	chunkTotal = 1000
)

func TestDBChunkStore(t *testing.T) {

	//General Connection
	store, err := common.NewDBChunkStore(testDBPath)
	if err != nil {
		t.Fatal("[FAILURE] to open DBChunkStore\n")
	} else {
		fmt.Printf("[SUCCESS] open DBChunkStore\n")
	}

	t.Run("Write", func(t *testing.T) {
		//Simulate chunk writes w/ n chunkTotal
		for j := 0; j < chunkTotal; j++ {
			simdata := make([]byte, 4096)
			tmp := fmt.Sprintf("%s%d", "randombytes", j)
			copy(simdata, tmp)
			simh, err := store.StoreChunk(simdata)
			if err != nil {
				t.Fatal("[FAILURE] writting record #%v [%x] => %v\n", j, simh, string(simdata[:]))
			} else if j%50 == 0 {
				fmt.Printf("Generating record [%x] => %v ... ", simh, string(simdata[:]))
				fmt.Printf("[SUCCESS] writing #%v chunk to %v\n", j, testDBPath)
			}
		}
	})

	t.Run("Scan", func(t *testing.T) {
		err := store.ScanAll()
		if err != nil {
			t.Fatal("[FAILURE] ScanAll Error\n")
		} else {
			fmt.Printf("[SUCCESS] ScanAll Operation\n")
		}
	})

	t.Run("Stat", func(t *testing.T) {
		res, err := store.GetChunkStat()
		if err != nil {
			t.Fatal("[FAILURE] netStat Retrieval Error\n")
		} else {
			fmt.Printf("[SUCCESS] netStat optput: %s\n", res)
		}
	})

	t.Run("Save", func(t *testing.T) {
		err := store.Save()
		if err != nil {
			t.Fatal("[FAILURE] unable to generate netStat json\n")
		} else {
			fmt.Printf("[SUCCESS] netStat stored in persisted files\n")
		}
	})

}

func TestLoadDBChunkStore(t *testing.T) {

	//Opening existing DB
	store, err := common.LoadDBChunkStore(testDBPath)
	if err != nil {
		t.Fatal("[FAILURE] to open existing DBChunkStore\n")
	} else {
		fmt.Printf("[SUCCESS] open exsisting DBChunkStore\n")
	}

	t.Run("EWrite", func(t *testing.T) {
		//Simulate chunk writes w/ n chunkTotal
		for j := 0; j < chunkTotal; j++ {
			simdata := make([]byte, 4096)
			tmp := fmt.Sprintf("%s%d", "randombytes", j)
			copy(simdata, tmp)
			simh, err := store.StoreChunk(simdata)
			if err != nil {
				t.Fatal("[FAILURE] writting record #%v [%x] => %v\n", j, simh, string(simdata[:]))
			} else if j%50 == 0 {
				fmt.Printf("Generating record [%x] => %v ... ", simh, string(simdata[:]))
				fmt.Printf("[SUCCESS] writing #%v chunk to %v\n", j, testDBPath)
			}
		}
	})

	t.Run("EScan", func(t *testing.T) {
		err := store.ScanAll()
		if err != nil {
			t.Fatal("[FAILURE] ScanAll Error\n")
		} else {
			fmt.Printf("[SUCCESS] ScanAll Operation\n")
		}
	})

	t.Run("EStat", func(t *testing.T) {
		res, err := store.GetChunkStat()
		if err != nil {
			t.Fatal("[FAILURE] netStat Retrieval Error\n")
		} else {
			fmt.Printf("[SUCCESS] netStat optput: %s\n", res)
		}
	})

	t.Run("ESave", func(t *testing.T) {
		err := store.Save()
		if err != nil {
			t.Fatal("[FAILURE] unable to generate netStat json\n")
		} else {
			fmt.Printf("[SUCCESS] netStat stored in persisted files\n")
		}
	})
}
