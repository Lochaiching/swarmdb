package main

import (
    //"bytes"
    "path/filepath"
    "reflect"
    "fmt"
    "github.com/ethereum/go-ethereum/swarm/storage"
	"github.com/ethereum/go-ethereum/common"
)

func main(){
	
    ldb, err := storage.NewLDBDatabase(filepath.Join("/var/www/vhosts/data/swarm/bzz-f02e8134d956daba24262882d11f4c8ebe60d05c", "chunks"))
    //fmt.Println(ldb)
    if (err != nil) {
        fmt.Println(err)
    }

    iter := ldb.NewIterator()

    for iter.Next(){
        ikey := iter.Key()
        ivalue := iter.Value()
        fmt.Println(reflect.TypeOf(ikey))
        fmt.Println(reflect.TypeOf(ivalue))
        //skey := string(ikey)
        //svalue := string(ivalue)
        //fmt.Println(reflect.TypeOf(skey))
        //fmt.Println(reflect.TypeOf(svalue))
        //fmt.Printf("key = %v  value = %v\n", ikey, ivalue)                
                
        a := ""  
        b := ""
        
		a = common.Bytes2Hex(ikey)
		b = fmt.Sprintf("%s", ivalue)
		
	    /*	
		if len(ivalue) > 8{
			b = fmt.Sprintf("%s", ivalue[8:])
		}
	   */	
	
		fmt.Printf("converted key = %s value = %s \n", a, b)
        //ldb.Delete(ikey)
    }
}

/* ************************* for this to work I had to kill swarm after       swarm up     and   curl .....

[root@puppeth-centos7-11091000-3428 chunks]# curl -X POST http://104.198.198.45:8500/bzzr: -d 'pepsi'

[root@puppeth-centos7-11091000-3428 chunks]# curl http://104.198.198.45:8500/bzzr:/02edd318c7560a108369e4d043472f28678010379ce3cd077d5b421bdaa155a9
pepsi


[root@puppeth-centos7-11091000-3428 test]# go run readldb.go
[]uint8
[]uint8
converted key = 0002edd318c7560a108369e4d043472f28678010379ce3cd077d5b421bdaa155a9 value = 
[]uint8
[]uint8
converted key = 010000000000000000 value = pepsi
[]uint8
[]uint8
converted key = 02 value =
[]uint8
[]uint8
converted key = 03 value =
[]uint8
[]uint8
converted key = 04 value =

*/



