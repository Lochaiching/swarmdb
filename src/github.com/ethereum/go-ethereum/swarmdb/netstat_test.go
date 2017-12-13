package swarmdb_test

import (
	"fmt"
	//"bytes"
	"testing"
)

var (
	testDBPath = "chunks.db"
	chunkTotal = 2000
)

func TestDBChunkStore(t *testing.T) {

	//General Connection
	store, err := swarmdb.NewDBChunkStore(testDBPath)
	if err != nil {
		t.Fatal("[FAILURE] to open DBChunkStore\n")
	} else {
		fmt.Printf("[SUCCESS] open DBChunkStore\n")
	}

	t.Run("Write=0", func(t *testing.T) {
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
		_ = store.Save()
	})

	t.Run("Scan=0", func(t *testing.T) {
		err := store.ScanAll()
		if err != nil {
			t.Fatal("[FAILURE] ScanAll Error\n")
		} else {
			fmt.Printf("[SUCCESS] ScanAll Operation\n")
		}
		_ = store.Save()
	})

	t.Run("Stat=0", func(t *testing.T) {
		res, err := store.GetChunkStat()
		if err != nil {
			t.Fatal("[FAILURE] netStat Retrieval Error\n")
		} else {
			fmt.Printf("[SUCCESS] netStat optput: %s\n", res)
		}
	})

	t.Run("Save=0", func(t *testing.T) {
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
	store, err := swarmdb.LoadDBChunkStore(testDBPath)
	if err != nil {
		t.Fatal("[FAILURE] to open existing DBChunkStore\n")
	} else {
		fmt.Printf("[SUCCESS] open exsisting DBChunkStore\n")
	}

	t.Run("EWrite=1", func(t *testing.T) {
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
		_ = store.Save()
	})

	t.Run("EScan=1", func(t *testing.T) {
		err := store.ScanAll()
		if err != nil {
			t.Fatal("[FAILURE] ScanAll Error\n")
		} else {
			fmt.Printf("[SUCCESS] ScanAll Operation\n")
		}
		_ = store.Save()
	})

	t.Run("EStat=1", func(t *testing.T) {
		res, err := store.GetChunkStat()
		if err != nil {
			t.Fatal("[FAILURE] netStat Retrieval Error\n")
		} else {
			fmt.Printf("[SUCCESS] netStat optput: %s\n", res)
		}
	})

	t.Run("EClaim=1", func(t *testing.T) {
		err := store.ClaimAll()
		if err != nil {
			t.Fatal("[FAILURE] netStat Retrieval Error\n")
		} else {
			fmt.Printf("[SUCCESS] netStat optput\n")
		}
		_ = store.Save()
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
