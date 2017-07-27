package main

import (
	"bytes"
	"io/ioutil"
	"github.com/ethereum/go-ethereum/swarm/api"	
	cl "github.com/ethereum/go-ethereum/swarm/api/client"
    "fmt"
)


/*


[root@www5009 test]# ./MultipartUpload
File: file1.txt   Data: some-data
File: file2.txt   Data: some-data
File: dir1/file3.txt   Data: some-data
File: dir1/file4.txt   Data: some-data
File: dir2/file5.txt   Data: some-data
File: dir2/dir3/file6.txt   Data: some-data
File: dir2/dir4/file7.txt   Data: some-data
File: dir2/dir4/file8.txt   Data: some-data


*/


var testDirFiles = []string{
	"file1.txt",
	"file2.txt",
	"dir1/file3.txt",
	"dir1/file4.txt",
	"dir2/file5.txt",
	"dir2/dir3/file6.txt",
	"dir2/dir4/file7.txt",
	"dir2/dir4/file8.txt",
}

// MultipartUpload tests uploading files to swarm using a multipart
// upload
func main() {

	client := cl.NewClient("http://50.225.47.159:8500")
	
	// define an uploader which uploads testDirFiles with some data
	data := []byte("some-data")
	uploader := cl.UploaderFunc(func(upload cl.UploadFn) error {
		for _, name := range testDirFiles {
			file := &cl.File{
				ReadCloser: ioutil.NopCloser(bytes.NewReader(data)),
				ManifestEntry: api.ManifestEntry{
					Path:        name,
					ContentType: "text/plain",
					Size:        int64(len(data)),
				},
			}
			if err := upload(file); err != nil {
				return err
			}
		}
		return nil
	})

	// upload the files as a multipart upload
	//client := cl.NewClient("http://50.225.47.159:8500")
	hash, err := client.MultipartUpload("", uploader)
	if err != nil {
		//t.Fatal(err)
		fmt.Printf("err MultipartUpload: %v\n", err)
		
	}

	// check we can download the individual files
	checkDownloadFile := func(path string) {
		file, err := client.Download(hash, path)
		if err != nil {
			//t.Fatal(err)
			fmt.Printf("err Download: %v\n", err)
		}
		defer file.Close()
		gotData, err := ioutil.ReadAll(file)
		if err != nil {
			//t.Fatal(err)
			fmt.Printf("err ReadAll: %v\n", err)
		}
		
		dataStr := string(gotData)
	    fmt.Printf("File: %v   Data: %v\n", path, dataStr)		
		
		if !bytes.Equal(gotData, data) {
			//t.Fatalf("expected data to be %q, got %q", data, gotData)
			fmt.Printf("expected data to be %q, got %q", data, gotData)
		}
	}
	for _, file := range testDirFiles {
		checkDownloadFile(file)
	}
}