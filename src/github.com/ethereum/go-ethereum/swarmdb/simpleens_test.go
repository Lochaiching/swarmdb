// Copyright (c) 2018 Wolk Inc.  All rights reserved.

// The SWARMDB library is free software: you can redistribute it and/or modify
// it under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// The SWARMDB library is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU Lesser General Public License for more details.
//
// You should have received a copy of the GNU Lesser General Public License
// along with the go-ethereum library. If not, see <http://www.gnu.org/licenses/>.
package swarmdb_test

import (
	"bytes"
	"fmt"
	"swarmdb"
	"testing"
)

func TestENSSimple(t *testing.T) {
	store, err := swarmdb.NewENSSimple("/tmp/ens.db")
	if err != nil {
		t.Fatal("failure to open ENSSimulation")
	}
	indexName := []byte("12345678123456781234567812345678")
	roothash := []byte("87654321876543218765432187654321")
	// store.StoreRootHash(indexName, roothash)
	val, err := store.GetRootHash(indexName)
	if err != nil {
	}

	fmt.Printf("roothash [%x]\nrootchek [%x]\n", roothash, val)

	if bytes.Compare(val, roothash) != 0 {
		t.Fatal("Err1", indexName, roothash, val)
	} else {
		fmt.Printf("SUCCESS1:  %v => %v\n", string(indexName), string(val))
	}
}