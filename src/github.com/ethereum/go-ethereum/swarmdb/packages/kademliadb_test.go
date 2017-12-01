// Copyright 2017 The go-ethereum Authors
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
	"bytes"
	"fmt"
	//"strings"
	"sync"
	"testing"

	"github.com/ethereum/go-ethereum/swarm/api"
	"github.com/ethereum/go-ethereum/swarm/storage"
	swarmdb "github.com/ethereum/go-ethereum/swarmdb/packages"
)

type testCase struct {
	Owner     []byte
	TableName []byte
	Column    []byte
	Value     []byte
	Key       []byte
}

func TestKademlia(t *testing.T) {
	datadir := "/tmp/joy"
	dpa, err := storage.NewLocalDPA(datadir)
	if err != nil {
		t.Fatal(err)
	}

	api := api.NewApi(dpa, nil)
	kdb, err := swarmdb.NewKademliaDB(api)
	if err != nil {
		t.Fatal("Failed creating Kademlia")
	}
	dpa.Start() // missing

	var tc testCase
	tc.Value = []byte(`[{"yob":1980,"location":"San Mateo"}]`)
	tc.Key = []byte(`rodney@wolk.com`)
	tc.Owner = []byte(`0x728781e75735dc0962df3a51d7ef47e798a7107e`)
	tc.TableName = []byte(`email`)
	//Need to revisit this concept for KDB - may not be necessary (column)
	tc.Column = []byte(`yob`)
	kdb.Open(tc.Owner, tc.TableName, tc.Column)
	sdata := kdb.BuildSdata(tc.Key, tc.Value)
	fmt.Printf("SDATA: (%s) [%+v]", sdata, sdata)
	expectedKey := kdb.GenerateChunkKey( tc.Key )
	fmt.Printf("Expected Key: (%s) [%+v]", expectedKey, expectedKey)

	ok, err := kdb.Put(tc.Key, sdata)
	if err != nil {
		t.Fatal(err)
	}
	if !ok {
		t.Fatal("The expected HASH for --> ", tc.Value, " <- is [", tc.Key, "] SAVED")
	}
	fmt.Printf("WG Done: %v\n", tc.Key)

	reader := api.GetDPA().Retrieve(expectedKey)
	fmt.Printf("Retrieve: %v\n", expectedKey)
	buf := make([]byte, 4096)
	offset, err := reader.Read(buf)
	fmt.Printf("Read done - %v\n", string(buf))
	if err != nil {
		fmt.Printf("Retrieve ERR: %v'", err)
	} else {
		fmt.Printf("Retrieve: %v offset:%d buf:'%v'", expectedKey, offset, buf)
	}
	
	/*
	*/
	sdataDebug := make([]byte, 4096)
	copy(sdataDebug[0:], []byte("Hello World"))
	rd := bytes.NewReader(sdataDebug)
	wg := &sync.WaitGroup{}
	keyDebug, _ := api.GetDPA().Store(rd, int64(len(sdataDebug)), wg, nil)
	fmt.Printf("KEY: [%s][%s]\n\n",keyDebug, sdataDebug)
	dataDebug := api.GetDPA().Retrieve(keyDebug)
        if _, err = dataDebug.Size(nil); err != nil {
                //log.Debug("key not found %s: %s", keyDebug, err)
        }
        contentReaderSize, _ := dataDebug.Size(nil)
        contentBytes := make([]byte, contentReaderSize)
        _, _ = dataDebug.ReadAt(contentBytes, 0)

	fmt.Print("data: [%s][%v]\n\n",string(contentBytes), contentBytes)
	dpa.Stop()
	/*
		tests := []test{
			{
				k:       "",
				v:       "",
				expectResponse: "true",
			},
			{
				k:       "",
				v:       "",
				expectResponse: "true",
			},
		}
		for _, x := range tests {
			if false {
					t.Fatalf("expected %s to error", x.k)
				continue
			}
			if false {
				t.Fatalf("error parsing %s: %s", x.k, x.v)
			}
			if false {
				t.Fatalf("expected %s to return %#v, got ", x.k, x.v)
			}
			if false {
				t.Fatalf("expected %s raw to be %t, got ", x.k, x.expectResponse)
			}
			if false {
				t.Fatalf("expected %s immutable to be %t, got ", x.k, x.expectResponse)
			}
		}
	*/
}

//fmt.Println("Kademlia Test")
