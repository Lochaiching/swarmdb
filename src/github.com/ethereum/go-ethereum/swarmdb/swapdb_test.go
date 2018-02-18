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
	"fmt"
	"github.com/ethereum/go-ethereum/common"
	"swarmdb"
	"testing"
)

func TestSwapDB(t *testing.T) {
	config, _ := swarmdb.LoadSWARMDBConfig(swarmdb.SWARMDBCONF_FILE)
	km, _ := swarmdb.NewKeyManager(&config)
	u := config.GetSWARMDBUser()

	swapdb, err := swarmdb.NewSwapDB("swap.db")
	if err != nil {
		t.Fatal("Failure to open NewSwapDB")
	}

	// Test Issue
	var beneficiary common.Address
	var amount int

	ch, err := swapdb.Issue(&km, u, beneficiary, amount)
	if err != nil {
		t.Fatalf("[swapdb_test:TestSwapDB] Issue %s", err.Error())
	} else {
		fmt.Printf("Issued check %v\n", ch)
	}

}
