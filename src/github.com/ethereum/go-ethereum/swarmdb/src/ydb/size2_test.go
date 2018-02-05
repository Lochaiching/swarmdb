package ydb_test

import (
	//"github.com/ethereum/go-ethereum/swarmdb/ydb"
	"ydb"
	"testing"
)

/*
 package size

 import "testing"
*/
/*
type Test struct {
    in  int
    out string
}
*/
var tests2 = []Test{
    {0, "zero"},
    {101, "huge"},
}

func TestSize2(t *testing.T) {
    for i, test := range tests2 {
        size := ydb.Size(test.in)
        if size != test.out {
            t.Errorf("#%d: Size(%d)=%s; want %s", i, test.in, size, test.out)
        }
    }
}


/*

**** very slow need to build all   ->   package swarmdb

[yaron@www6002 swarmdb]$ go test size_test.go
--- FAIL: TestSize (0.00s)
        size_test.go:32: #1: Size(987)=huge; want small
FAIL
FAIL    command-line-arguments  0.065s
[yaron@www6002 swarmdb]$

*/



