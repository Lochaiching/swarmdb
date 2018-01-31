package main

import (
	"encoding/binary"
	"fmt"
	"merkletree"
	"bytes"
	//"github.com/cbergoon/merkletree"
	"github.com/ethereum/go-ethereum/crypto"
	"io/ioutil"
)

var (
	debug      = false
	testmode   = 2
	list       []merkletree.Content
	sampleFile = "data2"
	testList   = []string{"a", "b","c","d","e","f","g","h"}
)

//TestContent implements the Content interface provided by merkletree and represents the content stored in the tree.
type TestContent struct {
	x string
	b []byte
}

//CalculateHash hashes the values of a TestContent
func (t TestContent) CalculateHash() []byte {
	r := bytes.TrimRight(t.b, "\x00")
	res := crypto.Keccak256(r)
	//fmt.Printf("keccak256: %s %v => %x\n", t.x, r, res)
	return res
}

func (t TestContent) Returnbyte() []byte {
    return t.b
}

//Chop a 4096 byte chunk into 128 32byte chunk
func chunksplit(chunk []byte) (segments [][]byte) {
	curr := 0
	for curr < 4096 {
		prev := curr
		curr += 32
		rawseg := make([]byte, 32)
		copy(rawseg[:], chunk[prev:curr])
		//seg := bytes.TrimRight(rawseg, "\x00")
		//fmt.Printf("Segemgt[%v:%v] | %v (%s)\n", prev, curr, rawseg, rawseg)
		segments = append(segments, rawseg)
	}
	return segments
}

//Equals tests for equality of two Contents
func (t TestContent) Equals(other merkletree.Content) bool {
	return t.x == other.(TestContent).x
}

//Compute segment index j
func getIndex(seedsecret string) (index uint8) {
	seedhash := crypto.Keccak256([]byte(seedsecret))
	_ = binary.Read(bytes.NewReader(seedhash[31:]), binary.BigEndian, &index)
	fmt.Printf("%v | Index: %s\n", seedhash, index)
	return index
}

//Replace jth segment with h(content+seed)
func prepareASH(chunk []byte, seed string) (segments [][]byte) {
	j := getIndex(seed)
	segments = chunksplit(chunk)
	segments[j] = crypto.Keccak256(append(segments[j], []byte(seed)...))
	return segments
}

func main() {

	if testmode == 2 {
		//Test Chunk
		samplechunk := make([]byte, 4096)
		data, _ := ioutil.ReadFile(sampleFile)
		copy(samplechunk[:], data)
		fmt.Printf("%s\n%v\n", samplechunk, data)

		//segments := chunksplit(samplechunk)
		segments := prepareASH(samplechunk, "secret")
		//fmt.Printf("Segments: %v", segments)

		for j, seg := range segments {
			if debug {
				//s := fmt.Sprintf("%s", seg)
				fmt.Printf("[seg %v]%v(%s)\n", j, seg, seg)
			}
			list = append(list, TestContent{x: fmt.Sprintf("%s", seg), b: seg})
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
			list = append(list, TestContent{x: rs, b: rawseg})

		}
	}

	fmt.Printf("Segment: %s\n", list)
	//Create a new Merkle Tree from the list of Content
	t, _ := merkletree.NewTree(list)

	//Get the Merkle Root of the tree
	mr := t.MerkleRoot()
	fmt.Printf("Merkle Root: %x\n",mr)

	//Verify the entire tree (hashes for each node) is valid
	vt := t.VerifyTree()
	fmt.Printf("Verify Tree: %v\n", vt)

	//Verify a specific content in in the tree
	vp := t.GetProof(t.MerkleRoot(), list[0])
	fmt.Printf("Get Proof: %v\n", vp)

    //Generate merkle proof for a given element
    vc, proof := t.VerifyContent(t.MerkleRoot(), list[0])
    fmt.Printf("Merkle Proof: [%v] [%x] [%t]\n", list[0], proof,vc)

    //Verify Merkle proof
    chash := list[0].CalculateHash()
    _ = merkletree.CheckProof(mr, chash, proof)

	//String representation
	fmt.Printf("%s", t.String())
	//fmt.Println(t)

}
