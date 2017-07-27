package main

import (
	"io/ioutil"
	"os"	
	"path/filepath"	
	"reflect"
	"sort"
	cl "github.com/ethereum/go-ethereum/swarm/api/client"
    "fmt"
)

/*

[root@www5009 test]# ./FileList
ls prefix: dir1/file3.txt paths: [dir1/file3.txt]
ls prefix: dir1/file34 paths: []
ls prefix: dir2/dir4/ paths: [dir2/dir4/file7.txt dir2/dir4/file8.txt]
ls prefix: dir2/dir4/file78 paths: []
ls prefix: file paths: [file1.txt file2.txt]
ls prefix: file1 paths: [file1.txt]
ls prefix: file12 paths: []
ls prefix: dir1/file paths: [dir1/file3.txt dir1/file4.txt]
ls prefix:  paths: [dir1/ dir2/ file1.txt file2.txt]
ls prefix: dir1 paths: [dir1/]
ls prefix: dir2/ paths: [dir2/dir3/ dir2/dir4/ dir2/file5.txt]
ls prefix: dir2/dir4/file paths: [dir2/dir4/file7.txt dir2/dir4/file8.txt]
ls prefix: dir2/dir4/file7.txt paths: [dir2/dir4/file7.txt]
ls prefix: file2.txt paths: [file2.txt]
ls prefix: dir1/ paths: [dir1/file3.txt dir1/file4.txt]
ls prefix: dir2/file paths: [dir2/file5.txt]
ls prefix: dir2/dir paths: [dir2/dir3/ dir2/dir4/]
ls prefix: dir paths: [dir1/ dir2/]
ls prefix: dir2/dir3/ paths: [dir2/dir3/file6.txt]



[root@www5009 test]# curl http://50.225.47.159:8500/bzz:/905c88ed43385ab4cbad687dab9824095123ac005596ab0c76fa30647ec31606/?list=true
{"common_prefixes":["dir1/","dir2/"],"entries":[{"hash":"a2aef141984ac63d5616f29eff237d64072244a9923cae4123244b8669f25acd","path":"file1.txt","contentType":"text/plain; charset=utf-8","mode":420,"size":9,"mod_time":"2017-07-27T15:35:39-07:00"},{"hash":"750049c9f344810f1abb5b555fb1dfbde488edcf20cc27d34e1550189508159e","path":"file2.txt","contentType":"text/plain; charset=utf-8","mode":420,"size":9,"mod_time":"2017-07-27T15:35:39-07:00"},{"hash":"a2aef141984ac63d5616f29eff237d64072244a9923cae4123244b8669f25acd","path":"/","contentType":"text/plain; charset=utf-8","mode":420,"size":9,"mod_time":"2017-07-27T15:35:39-07:00"}]}

{
	"common_prefixes": ["dir1/", "dir2/"],
	"entries": [{
		"hash": "a2aef141984ac63d5616f29eff237d64072244a9923cae4123244b8669f25acd",
		"path": "file1.txt",
		"contentType": "text/plain; charset=utf-8",
		"mode": 420,
		"size": 9,
		"mod_time": "2017-07-27T15:35:39-07:00"
	}, {
		"hash": "750049c9f344810f1abb5b555fb1dfbde488edcf20cc27d34e1550189508159e",
		"path": "file2.txt",
		"contentType": "text/plain; charset=utf-8",
		"mode": 420,
		"size": 9,
		"mod_time": "2017-07-27T15:35:39-07:00"
	}, {
		"hash": "a2aef141984ac63d5616f29eff237d64072244a9923cae4123244b8669f25acd",
		"path": "/",
		"contentType": "text/plain; charset=utf-8",
		"mode": 420,
		"size": 9,
		"mod_time": "2017-07-27T15:35:39-07:00"
	}]
}


[root@www5009 test]# curl http://50.225.47.159:8500/bzz:/905c88ed43385ab4cbad687dab9824095123ac005596ab0c76fa30647ec31606/dir1/?list=true
{"entries":[{"hash":"80fb06e6f79bfa888d204c1e9b0d8d950da09f57b4c0d2ce202783021cfd3179","path":"dir1/file3.txt","contentType":"text/plain; charset=utf-8","mode":420,"size":14,"mod_time":"2017-07-27T15:35:39-07:00"},{"hash":"46d2b42329397329230493a1cf9a7405e8ea3fbdde288cdb3b546e9e8338e5af","path":"dir1/file4.txt","contentType":"text/plain; charset=utf-8","mode":420,"size":14,"mod_time":"2017-07-27T15:35:39-07:00"}]}

{
	"entries": [{
		"hash": "80fb06e6f79bfa888d204c1e9b0d8d950da09f57b4c0d2ce202783021cfd3179",
		"path": "dir1/file3.txt",
		"contentType": "text/plain; charset=utf-8",
		"mode": 420,
		"size": 14,
		"mod_time": "2017-07-27T15:35:39-07:00"
	}, {
		"hash": "46d2b42329397329230493a1cf9a7405e8ea3fbdde288cdb3b546e9e8338e5af",
		"path": "dir1/file4.txt",
		"contentType": "text/plain; charset=utf-8",
		"mode": 420,
		"size": 14,
		"mod_time": "2017-07-27T15:35:39-07:00"
	}]
}

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

// FileList tests listing files in a swarm manifest
func main() {

	dir := newTestDirectory()
	//defer os.RemoveAll(dir)

	client := cl.NewClient("http://50.225.47.159:8500")
	hash, err := client.UploadDirectory(dir, "", "")
	if err != nil {
		//t.Fatalf("error uploading directory: %s", err)
		fmt.Printf("error uploading directory: %s", err)
	}

	ls := func(prefix string) []string {
		list, err := client.List(hash, prefix)
		if err != nil {
			//t.Fatal(err)
			fmt.Printf("err List: %v\n", err)
		}
		paths := make([]string, 0, len(list.CommonPrefixes)+len(list.Entries))
		for _, prefix := range list.CommonPrefixes {
			paths = append(paths, prefix)
		}
		for _, entry := range list.Entries {
			paths = append(paths, entry.Path)
		}
		sort.Strings(paths)
		return paths
	}

	tests := map[string][]string{
		"":                    []string{"dir1/", "dir2/", "file1.txt", "file2.txt"},
		"file":                []string{"file1.txt", "file2.txt"},
		"file1":               []string{"file1.txt"},
		"file2.txt":           []string{"file2.txt"},
		"file12":              []string{},
		"dir":                 []string{"dir1/", "dir2/"},
		"dir1":                []string{"dir1/"},
		"dir1/":               []string{"dir1/file3.txt", "dir1/file4.txt"},
		"dir1/file":           []string{"dir1/file3.txt", "dir1/file4.txt"},
		"dir1/file3.txt":      []string{"dir1/file3.txt"},
		"dir1/file34":         []string{},
		"dir2/":               []string{"dir2/dir3/", "dir2/dir4/", "dir2/file5.txt"},
		"dir2/file":           []string{"dir2/file5.txt"},
		"dir2/dir":            []string{"dir2/dir3/", "dir2/dir4/"},
		"dir2/dir3/":          []string{"dir2/dir3/file6.txt"},
		"dir2/dir4/":          []string{"dir2/dir4/file7.txt", "dir2/dir4/file8.txt"},
		"dir2/dir4/file":      []string{"dir2/dir4/file7.txt", "dir2/dir4/file8.txt"},
		"dir2/dir4/file7.txt": []string{"dir2/dir4/file7.txt"},
		"dir2/dir4/file78":    []string{},
	}
	for prefix, expected := range tests {
		actual := ls(prefix)
		fmt.Printf("ls prefix: %v paths: %v\n", prefix, actual)
		if !reflect.DeepEqual(actual, expected) {
			//t.Fatalf("expected prefix %q to return %v, got %v", prefix, expected, actual)
			fmt.Printf("expected prefix %q to return %v, got %v", prefix, expected, actual)
		}
	}
}
