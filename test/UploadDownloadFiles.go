package main

import (
	"bytes"
	"io/ioutil"
    cl "github.com/ethereum/go-ethereum/swarm/api/client"
	"github.com/ethereum/go-ethereum/swarm/api"
	"fmt"
)

// UploadDownloadFiles test uploading and downloading files to swarm

/*

[root@www5009 api]# ./UploadDownloadFiles
rootHash: f4bee0173feaaef7ce664bfd9b3f379ec5a6f682fcdab3734a93ac660edbf721
checkDownload: some-data
newHash: a9a1ee90aa044eb1ef20e12f1504fd03c25937c0246029f29ed186eea3b136fc
checkDownload: some-data
checkDownload: some-other-data
newHash: bd996bfdd0a01f27f567c2e2acb4001ab7c0e0c7bb2667084863e887e7bb5ae6
checkDownload: some-other-data
checkDownload: some-other-data
 
[root@www5009 api]# curl http://50.225.47.159:8500/bzz:/f4bee0173feaaef7ce664bfd9b3f379ec5a6f682fcdab3734a93ac660edbf721
some-data

[root@www5009 api]# curl http://50.225.47.159:8500/bzz:/a9a1ee90aa044eb1ef20e12f1504fd03c25937c0246029f29ed186eea3b136fc
some-data
[root@www5009 api]# curl http://50.225.47.159:8500/bzz:/a9a1ee90aa044eb1ef20e12f1504fd03c25937c0246029f29ed186eea3b136fc/some/other/path
some-other-data
[

[root@www5009 api]# curl http://50.225.47.159:8500/bzz:/bd996bfdd0a01f27f567c2e2acb4001ab7c0e0c7bb2667084863e887e7bb5ae6
some-other-data
[root@www5009 api]# curl http://50.225.47.159:8500/bzz:/bd996bfdd0a01f27f567c2e2acb4001ab7c0e0c7bb2667084863e887e7bb5ae6/some/other/path
some-other-data


*/

func main() {
	
	client := cl.NewClient("http://50.225.47.159:8500")
	
	upload := func(manifest, path string, data []byte) string {
		file := &cl.File{
			ReadCloser: ioutil.NopCloser(bytes.NewReader(data)),
			ManifestEntry: api.ManifestEntry{
				Path:        path,
				ContentType: "text/plain",
				Size:        int64(len(data)),
			},
		}
		hash, err := client.Upload(file, manifest)
		if err != nil {
			//t.Fatal(err)
			fmt.Printf("err Upload: %v\n", err)
		}
		return hash
	}
	checkDownload := func(manifest, path string, expected []byte) {
		file, err := client.Download(manifest, path)
		if err != nil {
			//t.Fatal(err)
			fmt.Printf("err Download: %v\n", err)
		}
		defer file.Close()
		if file.Size != int64(len(expected)) {
			//t.Fatalf("expected downloaded file to be %d bytes, got %d", len(expected), file.Size)
			fmt.Printf("expected downloaded file to be %d bytes, got %d", len(expected), file.Size)
		}
		if file.ContentType != file.ContentType {
			//t.Fatalf("expected downloaded file to have type %q, got %q", file.ContentType, file.ContentType)
			fmt.Printf("expected downloaded file to have type %q, got %q", file.ContentType, file.ContentType)
		}
		data, err := ioutil.ReadAll(file)
		if err != nil {
			//t.Fatal(err)
			fmt.Printf("err ReadAll: %v\n", err)
		}
		dataStr := string(data)
	    fmt.Printf("checkDownload: %v\n", dataStr)
		
		if !bytes.Equal(data, expected) {
			//t.Fatalf("expected downloaded data to be %q, got %q", expected, data)
			fmt.Printf("expected downloaded data to be %q, got %q", expected, data)
		}
	}

	// upload a file to the root of a manifest
	rootData := []byte("some-data")
	rootHash := upload("", "", rootData)
    fmt.Printf("rootHash: %v\n", rootHash)

	// check we can download the root file
	checkDownload(rootHash, "", rootData)

	// upload another file to the same manifest
	otherData := []byte("some-other-data")
	newHash := upload(rootHash, "some/other/path", otherData)
	fmt.Printf("newHash: %v\n", newHash)

	// check we can download both files from the new manifest
	checkDownload(newHash, "", rootData)
	checkDownload(newHash, "some/other/path", otherData)

	// replace the root file with different data
	newHash = upload(newHash, "", otherData)
	fmt.Printf("newHash: %v\n", newHash)	

	// check both files have the other data
	checkDownload(newHash, "", otherData)
	checkDownload(newHash, "some/other/path", otherData)
}
