// not working 
// I was testing (yaron)
// can be deleted



package main

import (
    //"bytes"
    //"path/filepath"
    //"reflect"
    "fmt"
    //"sync"
    st "github.com/ethereum/go-ethereum/swarm/storage"
    //db1 "github.com/ethereum/go-ethereum/swarm/storage/dbstore"
    "github.com/ethereum/go-ethereum/common"
)

func main(){
//  ldb, err := storage.NewLDBDatabase(filepath.Join("/var/www/vhosts/data/swarm/bzz-81cb99136b046306671d80daaf8631fe1a94f4dd", "chunks"))
//  ldb, err := storage.NewLDBDatabase(filepath.Join("/tmp", "testdpa"))
path := "/tmp/testdpa"
hash := st.MakeHashFunc("SHA3")
ldb, err := st.NewDbStore(path,hash,10000000, 0)
fmt.Println(ldb)
fmt.Println(err)
fmt.Println(ldb)


type Key []byte
	 
	keys := []Key{
		Key(common.Hex2Bytes("0000000000000000000000000000000000000000000000000000000000000000")),
		Key(common.Hex2Bytes("4000000000000000000000000000000000000000000000000000000000000000")),
		Key(common.Hex2Bytes("5000000000000000000000000000000000000000000000000000000000000000")),
		Key(common.Hex2Bytes("3000000000000000000000000000000000000000000000000000000000000000")),
		Key(common.Hex2Bytes("2000000000000000000000000000000000000000000000000000000000000000")),
		Key(common.Hex2Bytes("1000000000000000000000000000000000000000000000000000000000000000")),
	}
	for _, key := range keys {
		ldb.Put(st.NewChunk(key, nil))
	}
	it, err := ldb.NewSyncIterator(st.DbSyncState{
		Start: Key(common.Hex2Bytes("1000000000000000000000000000000000000000000000000000000000000000")),
		Stop:  Key(common.Hex2Bytes("4000000000000000000000000000000000000000000000000000000000000000")),
		First: 2,
		Last:  4,
	})
	if err != nil {
		//t.Fatalf("unexpected error creating NewSyncIterator")
	}

	var chunk st.Key
	var res []Key
	for {
		chunk = it.Next()
		if chunk == nil {
			break
		}
		res = append(res, chunk)
	}









/*

    //iter := ldb.NewIterator()

        iter, err := ldb.NewSyncIterator(st.DbSyncState{
                        Start: st.Key(common.Hex2Bytes("1000000000000000000000000000000000000000000000000000000000000000")),  // types.go    type Key []byte
                        Stop:  st.Key(common.Hex2Bytes("4000000000000000000000000000000000000000000000000000000000000000")),
                        First: 2,
                        Last:  4,
                })
        if err != nil {
                //      t.Fatalf("unexpected error creating NewSyncIterator")
                }

    //iter := ldb.NewSyncIterator()


	res = nil
	for {
		chunk = it.Next()
		if chunk == nil {
			break
		}
		res = append(res, chunk)
	}


 
    for iter.Next(){
        ikey := iter.Key()
        ivalue := iter.Value()
        //fmt.Println(reflect.TypeOf(ikey))
        //fmt.Println(reflect.TypeOf(ivalue))
        //skey := string(ikey)
        //svalue := string(ivalue)
        //fmt.Println(reflect.TypeOf(skey))
        //fmt.Println(reflect.TypeOf(svalue))
        fmt.Printf("[KEY] %x %v : %v\n", ikey, string(ivalue), ivalue)
    }
    
    */
}