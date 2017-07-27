package main

/*

[root@www5009 test]# ./UploadDownloadDirectory
defaultPath: /tmp/swarm-client-test199386341/file1.txt
hash: 905c88ed43385ab4cbad687dab9824095123ac005596ab0c76fa30647ec31606
File: file1.txt   Data: file1.txt
File: file2.txt   Data: file2.txt
File: dir1/file3.txt   Data: dir1/file3.txt
File: dir1/file4.txt   Data: dir1/file4.txt
File: dir2/file5.txt   Data: dir2/file5.txt
File: dir2/dir3/file6.txt   Data: dir2/dir3/file6.txt
File: dir2/dir4/file7.txt   Data: dir2/dir4/file7.txt
File: dir2/dir4/file8.txt   Data: dir2/dir4/file8.txt
File:    Data: file1.txt
File: file1.txt   Data: file1.txt
File: file2.txt   Data: file2.txt
File: dir1/file3.txt   Data: dir1/file3.txt
File: dir1/file4.txt   Data: dir1/file4.txt
File: dir2/file5.txt   Data: dir2/file5.txt
File: dir2/dir3/file6.txt   Data: dir2/dir3/file6.txt
File: dir2/dir4/file7.txt   Data: dir2/dir4/file7.txt
File: dir2/dir4/file8.txt   Data: dir2/dir4/file8.txt
[root@www5009 test]#
[root@www5009 test]# curl http://50.225.47.159:8500/bzzr:/905c88ed43385ab4cbad687dab9824095123ac005596ab0c76fa30647ec31606
{"entries":[{"hash":"5199a430036e26f2e2cc50d7958d686e625a2f4081da8cfe5b47d2af9169391a","path":"dir","contentType":"application/bzz-manifest+json","mod_time":"0001-01-01T00:00:00Z"},{"hash":"6259774eab32f76eedc255684bea32d167910ba41e746f87b997b5ad7fbe36b4","path":"file","contentType":"application/bzz-manifest+json","mod_time":"0001-01-01T00:00:00Z"},{"hash":"a2aef141984ac63d5616f29eff237d64072244a9923cae4123244b8669f25acd","contentType":"text/plain; charset=utf-8","mode":420,"size":9,"mod_time":"2017-07-27T15:35:39-07:00"}]}

{
	"entries": [{
		"hash": "5199a430036e26f2e2cc50d7958d686e625a2f4081da8cfe5b47d2af9169391a",
		"path": "dir",
		"contentType": "application/bzz-manifest+json",
		"mod_time": "0001-01-01T00:00:00Z"
	}, {
		"hash": "6259774eab32f76eedc255684bea32d167910ba41e746f87b997b5ad7fbe36b4",
		"path": "file",
		"contentType": "application/bzz-manifest+json",
		"mod_time": "0001-01-01T00:00:00Z"
	}, {
		"hash": "a2aef141984ac63d5616f29eff237d64072244a9923cae4123244b8669f25acd",
		"contentType": "text/plain; charset=utf-8",
		"mode": 420,
		"size": 9,
		"mod_time": "2017-07-27T15:35:39-07:00"
	}]
}

[root@www5009 test]# curl http://50.225.47.159:8500/bzz:/905c88ed43385ab4cbad687dab9824095123ac005596ab0c76fa30647ec31606
file1.txt

[root@www5009 test]#  curl http://50.225.47.159:8500/bzzr:/5199a430036e26f2e2cc50d7958d686e625a2f4081da8cfe5b47d2af9169391a
{"entries":[{"hash":"2f7be0bb48e1953938d02303d02df5b2f3a6d99045e3a69c9a011fbfdc78bb27","path":"1/file","contentType":"application/bzz-manifest+json","mod_time":"0001-01-01T00:00:00Z"},{"hash":"a3fde20048bc56a92c2d478495e82b3029dd2bd841e9e5e8932b09c316b9e5b5","path":"2/","contentType":"application/bzz-manifest+json","mod_time":"0001-01-01T00:00:00Z"}]}

{
	"entries": [{
		"hash": "2f7be0bb48e1953938d02303d02df5b2f3a6d99045e3a69c9a011fbfdc78bb27",
		"path": "1/file",
		"contentType": "application/bzz-manifest+json",
		"mod_time": "0001-01-01T00:00:00Z"
	}, {
		"hash": "a3fde20048bc56a92c2d478495e82b3029dd2bd841e9e5e8932b09c316b9e5b5",
		"path": "2/",
		"contentType": "application/bzz-manifest+json",
		"mod_time": "0001-01-01T00:00:00Z"
	}]
}

[root@www5009 test]#  curl http://50.225.47.159:8500/bzzr:/2f7be0bb48e1953938d02303d02df5b2f3a6d99045e3a69c9a011fbfdc78bb27
{"entries":[{"hash":"80fb06e6f79bfa888d204c1e9b0d8d950da09f57b4c0d2ce202783021cfd3179","path":"3.txt","contentType":"text/plain; charset=utf-8","mode":420,"size":14,"mod_time":"2017-07-27T15:35:39-07:00"},{"hash":"46d2b42329397329230493a1cf9a7405e8ea3fbdde288cdb3b546e9e8338e5af","path":"4.txt","contentType":"text/plain; charset=utf-8","mode":420,"size":14,"mod_time":"2017-07-27T15:35:39-07:00"}]}

{
	"entries": [{
		"hash": "80fb06e6f79bfa888d204c1e9b0d8d950da09f57b4c0d2ce202783021cfd3179",
		"path": "3.txt",
		"contentType": "text/plain; charset=utf-8",
		"mode": 420,
		"size": 14,
		"mod_time": "2017-07-27T15:35:39-07:00"
	}, {
		"hash": "46d2b42329397329230493a1cf9a7405e8ea3fbdde288cdb3b546e9e8338e5af",
		"path": "4.txt",
		"contentType": "text/plain; charset=utf-8",
		"mode": 420,
		"size": 14,
		"mod_time": "2017-07-27T15:35:39-07:00"
	}]
}

[root@www5009 test]# curl http://50.225.47.159:8500/bzzr:/80fb06e6f79bfa888d204c1e9b0d8d950da09f57b4c0d2ce202783021cfd3179
dir1/file3.txt

[root@www5009 test]# curl http://50.225.47.159:8500/bzz:/905c88ed43385ab4cbad687dab9824095123ac005596ab0c76fa30647ec31606/file1.txt
file1.txt

*/

import (
	"bytes"
	"io/ioutil"
	"os"
	"path/filepath"
	cl "github.com/ethereum/go-ethereum/swarm/api/client"
    "fmt"
)

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

func newTestDirectory() string {
	dir, err := ioutil.TempDir("", "swarm-client-test")
	if err != nil {
		//t.Fatal(err)
		fmt.Printf("err TempDir: %v\n", err)
	}

	for _, file := range testDirFiles {
		path := filepath.Join(dir, file)
		if err := os.MkdirAll(filepath.Dir(path), 0755); err != nil {
			os.RemoveAll(dir)
			//t.Fatalf("error creating dir for %s: %s", path, err)
			fmt.Printf("error creating dir for %s: %s", path, err)
		}
		if err := ioutil.WriteFile(path, []byte(file), 0644); err != nil {
			os.RemoveAll(dir)
			//t.Fatalf("error writing file %s: %s", path, err)
			fmt.Printf("error writing file %s: %s", path, err)
		}
	}

	return dir
}

// UploadDownloadDirectory tests uploading and downloading a
// directory of files to a swarm manifest
func main() {

	dir := newTestDirectory()
	//defer os.RemoveAll(dir)

	// upload the directory
	client := cl.NewClient("http://50.225.47.159:8500")
	defaultPath := filepath.Join(dir, testDirFiles[0])
	fmt.Printf("defaultPath: %v\n", defaultPath)
	hash, err := client.UploadDirectory(dir, defaultPath, "")
	if err != nil {
		//t.Fatalf("error uploading directory: %s", err)
		fmt.Printf("error uploading directory: %s", err)
	}
    fmt.Printf("hash: %v\n", hash)
    
	// check we can download the individual files
	checkDownloadFile := func(path string, expected []byte) {
		file, err := client.Download(hash, path)
		if err != nil {
			//t.Fatal(err)
			fmt.Printf("err Download: %v\n", err)
		}
		defer file.Close()
		data, err := ioutil.ReadAll(file)
		if err != nil {
			//t.Fatal(err)
			fmt.Printf("err ReadAll: %v\n", err)
		}
		
		dataStr := string(data)
	    fmt.Printf("File: %v   Data: %v\n", path, dataStr)
	
		if !bytes.Equal(data, expected) {
			//t.Fatalf("expected data to be %q, got %q", expected, data)
			fmt.Printf("expected data to be %q, got %q", expected, data)
		}
	}
	for _, file := range testDirFiles {
		checkDownloadFile(file, []byte(file))
	}

	// check we can download the default path
	checkDownloadFile("", []byte(testDirFiles[0]))

	// check we can download the directory
	tmp, err := ioutil.TempDir("", "swarm-client-test-download")
	if err != nil {
		//t.Fatal(err)
		fmt.Printf("err TempDir: %v\n", err)
	}
	//defer os.RemoveAll(tmp)
	if err := client.DownloadDirectory(hash, "", tmp); err != nil {
		//t.Fatal(err)
		fmt.Printf("err DownloadDirectory: %v\n", err)
	}
	for _, file := range testDirFiles {
		data, err := ioutil.ReadFile(filepath.Join(tmp, file))
		if err != nil {
			//t.Fatal(err)
			fmt.Printf("err ReadFile: %v\n", err)
		}
		
		dataStr := string(data)
	    fmt.Printf("File: %v   Data: %v\n", file, dataStr)
	    
		if !bytes.Equal(data, []byte(file)) {
			//t.Fatalf("expected data to be %q, got %q", file, data)
			fmt.Printf("expected data to be %q, got %q", file, data)
		}
	}
}
