package main

import (
	"ash"
	"fmt"
	"io/ioutil"
)

var (
	debug      = false
	testmode   = 1
	list       []ash.Content
	sampleFile = "data2"
	seed       = "secret"
	testList   = []string{"a", "b", "c", "d", "e", "f", "g", "h"}
)

func main() {

	if testmode == 2 {
		samplechunk := make([]byte, 4096)
		data, _ := ioutil.ReadFile(sampleFile)
		copy(samplechunk[:], data)
		fmt.Printf("%s\n%v\n", samplechunk, data)
		segments := ash.PrepareASH(samplechunk, seed)
		//fmt.Printf("Segments: %v", segments)

		for j, seg := range segments {
			if debug {
				//s := fmt.Sprintf("%s", seg)
				fmt.Printf("[seg %v]%v(%s)\n", j, seg, seg)
			}
			list = append(list, ash.Content{S: fmt.Sprintf("%s", seg), B: seg})
		}

	} else {
		//Build list of Content to build tree
		raws := testList
		for j, rs := range raws {
			rawseg := make([]byte, 32)
			copy(rawseg[:], rs)
			if debug {
				fmt.Printf("[seg %v]%v(%v)\n", j, rs, rawseg)
			}
			list = append(list, ash.Content{S: rs, B: rawseg})
		}
	}

	fmt.Printf("Segment: %s\n", list)
	//Create a new Merkle Tree from the list of Content
	t, _ := ash.NewTree(list)

	//Get the Merkle Root of the tree
	mr := t.MerkleRoot()
	fmt.Printf("Merkle Root: %x\n", mr)

	//Verify the entire tree (hashes for each node) is valid
	vt := t.VerifyTree()
	fmt.Printf("Verify Tree: %v\n", vt)

	//Verify a specific content in in the tree
	vp := t.GetProof(t.MerkleRoot(), list[0])
	fmt.Printf("Get Proof: %v\n", vp)

	//Generate merkle proof for a given element
	vc, proof := t.VerifyContent(t.MerkleRoot(), list[0])
	fmt.Printf("Merkle Proof: [%v] [%x] [%t]\n", list[0], proof, vc)

	//Verify Merkle proof
	chash := ash.Computehash(list[0].B)
	_ = ash.CheckProof(mr, chash, proof)

}
