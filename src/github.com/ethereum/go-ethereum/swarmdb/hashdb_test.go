package common_test

import (
	//"errors"
	"fmt"
	"testing"
	"math/rand"

	common "github.com/ethereum/go-ethereum/swarmdb"
)

func getSwarmDB(t *testing.T) (a common.SwarmDB) {
	swarmdb := common.NewSwarmDB()
	return *swarmdb
}

func TestPutInteger(t *testing.T) {

	fmt.Printf("---- TestPutInteger: generate 20 ints and enumerate them\n")
	hashid := make([]byte, 32)
	r, err := common.NewHashDB(hashid, getSwarmDB(t))
	if err != nil {
		t.Fatal(err)
	}
	// write 20 values into B-tree (only kept in memory)
	r.StartBuffer()
	vals := rand.Perm(20)
	for _, i := range vals {
		k := common.IntToByte(i)
		v := []byte(fmt.Sprintf("valueof%06x", i))
		// fmt.Printf("Insert %d %v %v\n", i, string(k), string(v))
		r.Put(k, v)
	}
	// flush B+tree in memory to SWARM
	r.FlushBuffer()
	// r.Print()

	hashid, _ = r.GetRootHash()
	s := common.NewBPlusTreeDB(getSwarmDB(t), hashid, common.KT_INTEGER)

	g, ok, err := s.Get(common.IntToByte(8))
	if !ok || err != nil {
		t.Fatal(g, err)
	} else {
		fmt.Printf("Get(8): [%s]\n", string(g))
	}
	h, ok2, err2 := s.Get(common.IntToByte(1))
	if !ok2 || err2 != nil {
		t.Fatal(h, err2)
	}
	fmt.Printf("Get(1): [%s]\n", string(h))
	
}



