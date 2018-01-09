package swarmdb_test

import (
	"encoding/json"
	"fmt"
	swarmdb "github.com/ethereum/go-ethereum/swarmdb"
	"strings"
	"testing"
)

func TestConfig(t *testing.T) {
	config := swarmdb.GenerateSampleSWARMDBConfig("4b0d79af51456172dfcc064c1b4b8f45f363a80a434664366045165ba5217d53", "9982ad7bfbe62567287dafec879d20687e4b76f5", "wolkwolkwolk")
	err := swarmdb.SaveSWARMDBConfig(config, swarmdb.SWARMDBCONF_FILE)
	if err != nil {

	}

	config2, err1 := swarmdb.LoadSWARMDBConfig(swarmdb.SWARMDBCONF_FILE)
	if err1 != nil {
	}
	targ := `{"listenAddrTCP":"127.0.0.1","portTCP":2000,"listenAddrHTTP":"127.0.0.1","portHTTP":8500,"address":"9982ad7bfbe62567287dafec879d20687e4b76f5","privateKey":"4b0d79af51456172dfcc064c1b4b8f45f363a80a434664366045165ba5217d53","chunkDBPath":"/swarmdb/data/keystore","authentication":1,"usersKeysPath":"/swarmdb/data/keystore","users":[{"address":"9982ad7bfbe62567287dafec879d20687e4b76f5","passphrase":"wolkwolkwolk","minReplication":3,"maxReplication":5,"autoRenew":1}],"currency":"WLK","targetCostStorage":2.71828,"targetCostBandwidth":3.14159}`

	cout, _ := json.Marshal(config2)
	if strings.Compare(string(cout), targ) == 0 {
		fmt.Printf("PASS Config: %s\n", cout)
	} else {
		t.Fatal("Mismatched output", string(cout), targ)
	}
}
