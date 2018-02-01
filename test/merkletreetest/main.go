package main

import (
	"merkletree"
	"bytes"
	"fmt"
	"github.com/ethereum/go-ethereum/crypto"
	"io/ioutil"
)

var (
	debug      = false
	testmode   = 2
	list       []ash.Content
	sampleFile = "data2"
	testList   = []string{"a", "b", "c", "d", "e", "f", "g", "h"}
)

//Segment implements the Content interface provided by merkletree and represents the content stored in the tree.
type Fragment struct {
	s string
	b []byte
}

//CalculateHash hashes the values of a Segment
func (f Fragment) CalculateHash() []byte {
	r := bytes.TrimRight(f.b, "\x00")
	res := crypto.Keccak256(r)
	//fmt.Printf("keccak256: %s %v => %x\n", t.x, r, res)
	return res
}

//Equals tests for equality of two Contents
func (f Fragment) Equals(other ash.Content) bool {
    if bytes.Compare(f.b, other.(Fragment).b) == 0 {
        return true
    }else{
        return false
    }
}

func main() {

	if testmode == 2 {
		samplechunk := make([]byte, 4096)
		data, _ := ioutil.ReadFile(sampleFile)
		copy(samplechunk[:], data)
		fmt.Printf("%s\n%v\n", samplechunk, data)

		segments := ash.PrepareASH(samplechunk, "secret")
		//fmt.Printf("Segments: %v", segments)

		for j, seg := range segments {
			if debug {
				//s := fmt.Sprintf("%s", seg)
				fmt.Printf("[seg %v]%v(%s)\n", j, seg, seg)
			}
			list = append(list, Fragment{s: fmt.Sprintf("%s", seg), b: seg})
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
			list = append(list, Fragment{s: rs, b: rawseg})

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
	chash := list[0].CalculateHash()
	_ = ash.CheckProof(mr, chash, proof)

	//String representation
	fmt.Printf("%s", t.String())
	//fmt.Println(t)

}
