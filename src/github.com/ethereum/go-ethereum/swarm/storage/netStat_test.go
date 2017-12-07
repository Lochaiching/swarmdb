package storage

import (
	"fmt"
	//"bytes"
	"testing" 
	"github.com/ethereum/go-ethereum/swarm/storage"
)

var (
    testDBPath = "chunks.db"
    chunkTotal = 1000    
)


func TestDBChunkStore(t *testing.T) {

    //General Connection
    store, err := storage.NewDBChunkStore(testDBPath)
    if err != nil {
        t.Fatal("[FAILURE] to open DBChunkStore\n")
    }else{
        fmt.Printf("[SUCCESS] open DBChunkStore\n")
    }

    t.Run("Write", func(t *testing.T){
        //Simulate chunk writes w/ n chunkTotal
        for j := 0; j <= chunkTotal; j++ {
            simdata := make([]byte, 4096)
            tmp := fmt.Sprintf("%s%d", "randombytes",j)
            copy(simdata,tmp)
            simh, err := store.StoreChunk(simdata)
            if err != nil {
                t.Fatal("[FAILURE] writting record #%v [%x] => %v\n",j, simh, string(simdata[:]) )
            }else if j % 10 == 0 {
                fmt.Printf("Generating record #%v [%x] => %v ... \n",j, simh, string(simdata[:]) )
            }
            fmt.Printf("[SUCCESS] writing #%v chunk to %v\n", j, testDBPath)
        }
    })

    t.Run("Scan", func(t *testing.T) {
        err := store.ScanAll()
        if err != nil {
            t.Fatal("[FAILURE] ScanAll Error\n")
        }else {
            fmt.Printf("[SUCCESS] ScanAll Operation\n")
        }
    })

}


