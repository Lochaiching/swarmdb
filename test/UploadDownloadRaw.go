package main

import (
	"bytes"
	"io/ioutil"
	cl "github.com/ethereum/go-ethereum/swarm/api/client"
    "fmt"	
)

// UploadDownloadRaw test uploading and downloading raw data to swarm

/*
[root@www5009 api]# ./UploadDownloadRaw
data: foo123
hash: ba683cff2f9c43dce23b85371be4653e98454099417f119211358932695375d7
gotData: foo123
[root@www5009 api]# curl http://50.225.47.159:8500/bzzr:/ba683cff2f9c43dce23b85371be4653e98454099417f119211358932695375d7
foo123
*/
func main() {	

	client := cl.NewClient("http://50.225.47.159:8500")

	// upload some raw data
	data := []byte("foo123")
	dataStr := string(data)
	fmt.Printf("data: %v\n", dataStr)
	hash, err := client.UploadRaw(bytes.NewReader(data), int64(len(data)))   //+ "/bzzr:/"
	if err != nil {
		//t.Fatal(err)
		fmt.Printf("err UploadRaw: %v\n", err)
		return
	}
    
    fmt.Printf("hash: %v\n", hash)
    
	// check we can download the same data
	res, err := client.DownloadRaw(hash)   //+ "/bzzr:/"
	if err != nil {
		//t.Fatal(err)
		fmt.Printf("err DownloadRaw: %v\n", err)
		return
	}
	defer res.Close()
	gotData, err := ioutil.ReadAll(res)
	if err != nil {
		//t.Fatal(err)
		fmt.Printf("err ReadAll: %v\n", err)
		return
	}
	
	gotDataStr := string(gotData)
	fmt.Printf("gotData: %v\n", gotDataStr)
	
	if !bytes.Equal(gotData, data) {
		//t.Fatalf("expected downloaded data to be %q, got %q", data, gotData)
		fmt.Printf("expected downloaded data to be %v, got %v\n", data, gotData)
		return
	}
}
