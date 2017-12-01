package main

import (
    "fmt"
    "bytes"
    "io"
    "sync"
    "github.com/ethereum/go-ethereum/swarm/api"
    "github.com/ethereum/go-ethereum/swarm/storage"
)

func main() {
    datadir := "/tmp/testdpa"
    dpa, err := storage.NewLocalDPA(datadir)
    dpa.Start() // missing
    api := api.NewApi(dpa, nil)
    if err != nil {
    } else {
        sdata := make([]byte, 4096)
        copy(sdata[0:], []byte("testdpa"))
        rd := bytes.NewReader(sdata)
        wg := &sync.WaitGroup{}
        dhash, _ := api.Store(rd, int64(len(sdata)), wg)
        fmt.Printf("Issued Store: %v\n", dhash)
        wg.Wait()
        fmt.Printf("WG Done: %v\n", dhash)

        reader := api.Retrieve(dhash)
        fmt.Printf("Retrieve: %v\n", dhash)
        buf := make([]byte, 4096)
        offset, err := reader.Read(buf)
        fmt.Printf("Read done - %v\n", string(buf))
        if err != nil && err != io.EOF{
            fmt.Printf("Retrieve ERR: %v'", err)
        } else {
            fmt.Printf("Retrieve: %v offset:%d buf:'%v'", dhash, offset, buf)
        }
    }
    dpa.Stop()
}
