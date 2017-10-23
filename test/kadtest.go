package main

import (
	"github.com/ethereum/go-ethereum/swarm/network/kademlia"
	"fmt"
	"math/rand"
	"time"
	"testing/quick"
	"reflect"

)

var (
	quickrand           = rand.New(rand.NewSource(time.Now().Unix()))
	quickcfgFindClosest = &quick.Config{MaxCount: 50, Rand: quickrand}
	quickcfgBootStrap   = &quick.Config{MaxCount: 100, Rand: quickrand}
)

type testNode struct {
	addr kademlia.Address
}
func gen(typ interface{}, rand *rand.Rand) interface{} {
	v, ok := quick.Value(reflect.TypeOf(typ), rand)
	if !ok {
		panic(fmt.Sprintf("couldn't generate random value of type %T", typ))
	}
	return v.Interface()
}
func (n *testNode) String() string {
	return fmt.Sprintf("%x", n.addr[:])
}

func (n *testNode) Addr() kademlia.Address {
	return n.addr
}

func (n *testNode) Drop() {
}

func (n *testNode) Url() string {
	return ""
}

func (n *testNode) LastActive() time.Time {
	return time.Now()
}


func TestSaveLoad() {
	r := rand.New(rand.NewSource(time.Now().UnixNano()))
	addresses := gen([]kademlia.Address{}, r).([]kademlia.Address)
	self := kademlia.RandomAddress()
	params := kademlia.NewKadParams()
	params.MaxProx = 8
	params.BucketSize = 4
	kad := kademlia.New(self, params)

	var err error

	for _, a := range addresses {
fmt.Println("a = ",a)
		err = kad.On(&testNode{addr: a}, nil)
		if err != nil && err.Error() != "bucket full" {
			fmt.Printf("backend not accepting node: %v", err)
		}
	fmt.Println(kad.String())
	}
	nodes := kad.FindClosest(self, 100)
	fmt.Println(kad.String())
    for _, node := range nodes {
		fmt.Println("node = ", node.Addr())
    }

}


func main(){
	TestSaveLoad()
}
