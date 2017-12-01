package main

import (
    "fmt"
    "bytes"
    "io"
    "strconv"
    "sync"
    "github.com/ethereum/go-ethereum/swarm/api"
    "github.com/ethereum/go-ethereum/swarm/storage"
)

func main() {
    keymap := make(map[string][]byte)
    datadir := "/tmp/testdpa"
    dpa, err := storage.NewLocalDPA(datadir)
    dpa.Start() // missing
    api := api.NewApi(dpa, nil)
    for i := 0; i < 100000; i++{
        if err != nil {
        } else {
            sdata := make([]byte, 4096)
            str := "testdata" + strconv.Itoa(i)
            copy(sdata[0:], []byte(str))
            rd := bytes.NewReader(sdata)
            wg := &sync.WaitGroup{}
            dhash, _ := api.Store(rd, int64(len(sdata)), wg)
            keymap[str] = dhash
            if i % 100 == 0{
                fmt.Printf("Issued Store: %d %v\n", i, dhash)
            }
            wg.Wait()
//            fmt.Printf("WG Done: %v\n", dhash)

        }
    }
    for i := 1; i <= 100000; i = i*10{
            a := i-1
            str := "testdata" + strconv.Itoa(a)
            reader := api.Retrieve(keymap[str])
            fmt.Printf("Retrieve: %v\n", keymap[str])
            buf := make([]byte, 4096)
            offset, err := reader.Read(buf)
            fmt.Printf("Read done - %v\n", string(buf))
            if err != nil && err != io.EOF{
                fmt.Printf("Retrieve ERR: %d %v'", i, err)
            } else {
                fmt.Printf("Retrieve: %s %v offset:%d buf:'%v'", str, keymap[str], offset, buf)
            }
    }
    dpa.Stop()
}
