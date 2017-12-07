// Copyright 2016 The go-ethereum Authors
// This file is part of the go-ethereum library.
//
// The go-ethereum library is free software: you can redistribute it and/or modify
// it under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// The go-ethereum library is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU Lesser General Public License for more details.
//
// You should have received a copy of the GNU Lesser General Public License
// along with the go-ethereum library. If not, see <http://www.gnu.org/licenses/>.

package swarmdb

import (
	//"errors"
	"fmt"
	//"io"
	"io/ioutil"
	"os"
	"strconv"
	"testing"

//	"github.com/ethereum/go-ethereum/common"
//	"github.com/ethereum/go-ethereum/log"
	"github.com/ethereum/go-ethereum/swarm/storage"
	"github.com/ethereum/go-ethereum/swarm/api"
)

func testHashDB(t *testing.T, f func(*HashDB)) {
	datadir, err := ioutil.TempDir("", "hashdbtest")
	if err != nil {
		t.Fatalf("unable to create temp dir: %v", err)
	}
	os.RemoveAll(datadir)
	defer os.RemoveAll(datadir)
	dpa, err := storage.NewLocalDPA(datadir)
	if err != nil {
		return
	}
	if err != nil{
		fmt.Println("ldb error ", err)
	}
	api := api.NewApi(dpa, nil)
	dpa.Start()
	hashdb, err := NewHashDB(nil,  api)
	if err != nil{
		fmt.Println("hashdb open error")
	}
	f(hashdb)
	dpa.Stop()
}

func TestHashDBPut(t *testing.T){
        testHashDB(t, func(hashdb *HashDB){
		var keylist []string
		for i := 0; i < 100 ; i++{
                	//key := "hello"
			key := "key"+strconv.Itoa(i)
			//value := "world"
			value := "value"+strconv.Itoa(i)
			keylist = append(keylist, key)
			fmt.Println("TestHashDBPut key = ", key, " value = ", value)
                	//b, err := hashdb.Put([]byte(key), []byte(value))
                	_, err := hashdb.Put([]byte(key), []byte(value))
                	if err != nil {
                        	t.Fatalf("error: %v", err)
                	}
/*
			if b{
				fmt.Println("true")
			}else{
				fmt.Println("false")
			}
*/
		}
		for i := 1 ; i < 100 ; i = i * 10{
                	resp := testHashDBGet(t, hashdb, []byte(keylist[i]))
			fmt.Println(string(resp))
		}
        })
}

func testHashDBGet(t *testing.T, hashdb *HashDB, key []byte) []byte{
	fmt.Printf("testHashDBGut key = %s\n", key)
	res, _, _ := hashdb.Get(key)
	fmt.Println("result for  ", string(key), "is ",  string(res))
	return res
}

func TestPutString(t *testing.T) {
        fmt.Printf("---- TestPutString\n")

	testHashDB(t, func(hashdb *HashDB){

        	hashdb.StartBuffer()
        	vals := rand.Perm(20)
        	// write 20 values into B-tree (only kept in memory)
        	for _, i := range vals {
                	k := []byte(fmt.Sprintf("%06x", i))
                v := []byte(fmt.Sprintf("valueof%06x", i))
                fmt.Printf("Insert %d %v %v\n", i, string(k), string(v))
                r.Put(k, v)
        }
        // this writes B+tree to SWARM
        r.FlushBuffer()

        // r.Close()
        r.Print()
        fmt.Printf("Put Test DONE\n----\n")

        hashid = r.GetHashID()

        s := swarmdb.NewBPlusTreeDB(getAPI(t), hashid, common.KT_STRING)
        fmt.Printf("getting...\n")

        g, _, _ := s.Get([]byte("000008"))
        fmt.Printf("GET1: %v\n", string(g))

        h, _, _ := s.Get([]byte("000001"))
        fmt.Printf("GET3: %v\n", string(h))

        s.Print()

        // ENUMERATOR
        res, _, _ := r.Seek([]byte("000004"))
        for k, v, err := res.Next(); err == nil; k, v, err = res.Next() {
                fmt.Printf("---> K: %s V: %v\n", common.KeyToString(common.KT_STRING, k), string(v))
        }
}

