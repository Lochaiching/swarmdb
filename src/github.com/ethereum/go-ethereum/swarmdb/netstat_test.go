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
	"encoding/json"
	"fmt"
	"math/rand"
	"swarmdb"
	"swarmdb/ash"
	"testing"
    "swarmdb/ash"
)

var (
	testDBPath = "chunks.db"
	chunkTotal = 2000
)

func TestNetstatsDBChunkStore(t *testing.T) {

	//General Connection
	config, _ := swarmdb.LoadSWARMDBConfig(swarmdb.SWARMDBCONF_FILE)
	swarmdb.NewKeyManager(&config)
	u := config.GetSWARMDBUser()

	store, err := swarmdb.NewDBChunkStore(testDBPath)
	if err != nil {
		t.Fatal("[FAILURE] to open DBChunkStore\n")
	} else {
		fmt.Printf("[SUCCESS] open DBChunkStore\n")
	}

	t.Run("Write=0", func(t *testing.T) {
		//Simulate chunk writes w/ n chunkTotal
		for j := 0; j < chunkTotal; j++ {
			simdata := make([]byte, 4096)
			tmp := fmt.Sprintf("%s%d", "randombytes", j)
			copy(simdata, tmp)
			enc := rand.Intn(2)
			simh, err := store.StoreChunk(u, simdata, enc)
			if err != nil {
				t.Fatal("[FAILURE] writting record #%v [%x] => %v\n", j, simh, string(simdata[:]))
			} else if j%50 == 0 {
				fmt.Printf("Generating record [%x] => %v ... ", simh, string(simdata[:]))
				fmt.Printf("[SUCCESS] writing #%v chunk | Encryption: %v\n", j, enc)
			}
		}
		_ = store.Save()
	})

	t.Run("Scan=0", func(t *testing.T) {
		err := store.ScanAll()
		if err != nil {
			t.Fatal("[FAILURE] ScanAll Error\n")
		} else {
			fmt.Printf("[SUCCESS] ScanAll Operation\n")
		}
		_ = store.Save()
	})

	t.Run("Stat=0", func(t *testing.T) {
		res, err := store.GetChunkStat()
		if err != nil {
			t.Fatal("[FAILURE] netStat Retrieval Error\n")
		} else {
			fmt.Printf("[SUCCESS] netStat optput: %s\n", res)
		}
	})

	t.Run("Save=0", func(t *testing.T) {
		err := store.Save()
		if err != nil {
			t.Fatal("[FAILURE] unable to generate netStat json\n")
		} else {
			fmt.Printf("[SUCCESS] netStat stored in persisted files\n")
		}
	})

}

func TestLoadDBChunkStore(t *testing.T) {

	//Opening existing DB
	config, _ := swarmdb.LoadSWARMDBConfig(swarmdb.SWARMDBCONF_FILE)
	swarmdb.NewKeyManager(&config)
	u := config.GetSWARMDBUser()

	store, err := swarmdb.LoadDBChunkStore(testDBPath)
	if err != nil {
		t.Fatal("[FAILURE] to open DBChunkStore\n")
	} else {
		fmt.Printf("[SUCCESS] open DBChunkStore\n")
	}

	t.Run("EWrite=1", func(t *testing.T) {
		//Simulate chunk writes w/ n chunkTotal
		for j := 0; j < chunkTotal; j++ {
			simdata := make([]byte, 4096)
			tmp := fmt.Sprintf("%s%d", "randombytes", j)
			copy(simdata, tmp)
			enc := rand.Intn(2)
			simh, err := store.StoreChunk(u, simdata, enc)
			if err != nil {
				t.Fatal("[FAILURE] writting record #%v [%x] => %v %s\n", j, simh, string(simdata[:]), err)
			} else if j%50 == 0 {
				fmt.Printf("Generating record [%x] => %v ... ", simh, string(simdata[:]))
				fmt.Printf("[SUCCESS] writing #%v chunk | Encryption: %v\n", j, enc)
			}
		}
		_ = store.Flush()
	})

	t.Run("EScan=1", func(t *testing.T) {
		err := store.ScanAll()
		if err != nil {
			t.Fatal("[FAILURE] ScanAll Error\n")
		} else {
			fmt.Printf("[SUCCESS] ScanAll Operation\n")
		}
		_ = store.Flush()
	})

	t.Run("EFarmLog=1", func(t *testing.T) {
		err := store.GenerateFarmerLog()
		if err != nil {
			t.Fatal("[FAILURE] Farmer log Error\n")
		} else {
			fmt.Printf("[SUCCESS] Farmer Operation completed\n")
		}
		_ = store.Flush()
	})

	t.Run("EStat=1", func(t *testing.T) {
		res, err := store.GetChunkStat()
		if err != nil {
			t.Fatal("[FAILURE] netStat Retrieval Error\n")
		} else {
			fmt.Printf("[SUCCESS] netStat optput: %s\n", res)
		}
	})

	t.Run("ESave", func(t *testing.T) {
		err := store.Save()
		if err != nil {
			t.Fatal("[FAILURE] unable to generate netStat json\n")
		} else {
			fmt.Printf("[SUCCESS] netStat stored in persisted files\n")
		}
	})

	err = store.Save()
	if err != nil {
		t.Fatal("[FAILURE] to persist netstat\n")
	} else {
		fmt.Printf("[SUCCESS] persist netstat to local\n")
	}

	t.Run("EASH=1", func(t *testing.T) {
		//Simulate chunk writes w/ n chunkTotal
		for j := 0; j < chunkTotal; j++ {
			simdata := make([]byte, 4096)
			tmp := fmt.Sprintf("%s%d", "randombytes", j)
			copy(simdata, tmp)
			enc := rand.Intn(2)
			simh, err := store.StoreChunk(u, simdata, enc)
			if err != nil {
				t.Fatal("[FAILURE] writting record #%v [%x] => %v %s\n", j, simh, string(simdata[:]), err)
			} else if j%50 == 0 {
				fmt.Printf("Generating record [%x] => %v ... ", simh, string(simdata[:]))
				fmt.Printf("[SUCCESS] writing #%v chunk | Encryption: %v\n", j, enc)
			}

			secret := make([]byte, 32)
			rand.Read(secret)
			proofRequired := rand.Intn(2) != 0
			auditIndex := rand.Intn(128)

			response, err := store.RetrieveAsh(simh, secret, proofRequired, int8(auditIndex))
			if err != nil {
				t.Fatal("[FAILURE] Generating record [%x] %s\n", simh, err.Error())
			} else if j%50 == 0 {
				if proofRequired {
					ok, mr, err := ash.CheckProof(response.Proof.Root, response.Proof.Path, response.Proof.Index)
					if err == nil {
						fmt.Printf("Proof Verified: %t | Root: %x\n", ok, mr)
					} else {
						t.Fatal(err.Error())
					}
				}
				output, _ := json.Marshal(response)
				fmt.Printf("ProofRequired: %T | Index: %d | Seed: [%x]\n", proofRequired, auditIndex, secret)
				fmt.Printf("Generating record [%x]\n %v\n\n", simh, string(output))
			}
		}
		_ = store.Flush()
	})

}
