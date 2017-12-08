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
	//"sync"
	"testing"

	"github.com/ethereum/go-ethereum/swarm/api"
	//"github.com/ethereum/go-ethereum/swarm/storage"
	swarmdb "github.com/ethereum/go-ethereum/swarmdb/leaf"
)

type testCase struct {
	Owner       []byte
	TableName   []byte
	Column      []byte
	Value       []byte
	Key         []byte
	ExpectedKey []byte
}

func TestKademliaChunkKeyGeneration(t *testing.T) {
	fmt.Printf("\nStarting TestKademliaChunkKeyGeneration\n")
	var tc testCase
	tc.Value = []byte(`[{"yob":1980,"location":"San Mateo/Chicago"}]`)
	tc.Key = []byte(`rodney1@wolk.com`)
	tc.Owner = []byte(`0x728781e75735dc0962df3a51d7ef47e798a7107e`)
	tc.TableName = []byte(`email`)
	tc.Column = []byte(`yob`)

	api := api.NewApi(nil, nil)
	kdb, err := swarmdb.NewKademliaDB(api)
	if err != nil {
		t.Fatal("Failed creating Kademlia")
	}

	kdb.Open(tc.Owner, tc.TableName, tc.Column)
	generatedKey := kdb.GenerateChunkKey(tc.Key)
	expectedKey := []byte{232, 13, 189, 249, 19, 48, 66, 109, 189, 89, 16, 49, 191, 59, 245, 251, 210, 223, 121, 151, 165, 252, 232, 245, 156, 183, 4, 176, 14, 37, 155, 30}
	if bytes.Compare(generatedKey, expectedKey) != 0 {
		t.Fatal("The generatedKey is incorrect [", generatedKey, "] doesn't match expected Key of [", expectedKey, "]")
	}
}

func TestKademliaPutGet(t *testing.T) {
	fmt.Printf("\nStarting TestKademlia\n")
	var tc testCase
	tc.Value = []byte(`[{"yob":1980,"location":"San Mateo/Chicago"}]`)
	tc.Key = []byte(`rodney1@wolk.com`)
	tc.Owner = []byte(`0x728781e75735dc0962df3a51d7ef47e798a7107e`)
	tc.TableName = []byte(`email`)
	tc.Column = []byte(`yob`)

	api := api.NewApi(nil, nil)
	kdb, err := swarmdb.NewKademliaDB(api)
	if err != nil {
		t.Fatal("Failed creating Kademlia")
	}

	kdb.Open(tc.Owner, tc.TableName, tc.Column)
	_, err = kdb.Put(tc.Key, tc.Value)
	if err != nil {
		t.Fatal(err)
	}
	val, _, _ := kdb.Get(tc.Key)
	if bytes.Compare(val, tc.Value) != 0 {
		t.Fatal("The value retrieved is incorrect [", val, "] and doesn't match expected value of [", tc.Value, "]")
	}
}
