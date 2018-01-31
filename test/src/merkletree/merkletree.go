// ASH
// Extension of github.com/cbergoon/merkletree

package merkletree

import (
	"bytes"
	"errors"
	"fmt"
	"github.com/ethereum/go-ethereum/crypto"
)

//Content represents the data that is stored and verified by the tree. A type that
//implements this interface can be used as an item in the tree.
type Content interface {
	CalculateHash() []byte
	Equals(other Content) bool
	Returnbyte() []byte
}

//MerkleTree is the container for the tree. It holds a pointer to the root of the tree,
//a list of pointers to the leaf nodes, and the merkle root.
type MerkleTree struct {
	Root       *Node
	merkleRoot []byte
	Leafs      []*Node
}

//Node represents a node, root, or leaf in the tree. It stores pointers to its immediate
//relationships, a hash, the content stored if it is a leaf, and other metadata.
type Node struct {
	Parent *Node
	Sister *Node
	Left   *Node
	Right  *Node
	leaf   bool
	dup    bool
	Hash   []byte
	C      Content
}

//verifyNode walks down the tree until hitting a leaf, calculating the hash at each level
//and returning the resulting hash of Node n.
func (n *Node) verifyNode() []byte {
	if n.leaf {
		return n.C.CalculateHash()
	}
	lhash := n.Left.verifyNode()
	rhash := n.Right.verifyNode()
	lhash = bytes.TrimRight(lhash, "\x00")
	rhash = bytes.TrimRight(rhash, "\x00")

	lr := append(lhash, rhash...)
	lrhash := crypto.Keccak256(lr)
	fmt.Printf("L+R: %s => %x\n", bsplit(lr), lrhash)
	return lrhash

}

//calculateNodeHash is a helper function that calculates the hash of the node.
func (n *Node) calculateNodeHash() []byte {
	if n.leaf {
		return n.C.CalculateHash()
	}
	lrhash := n.calculateLRHash()
	return lrhash
}

// calculateLRHash is a helper function that calculates the hash given left and right node
func (n *Node) calculateLRHash() []byte {
	lhash := bytes.TrimRight(n.Left.Hash, "\x00")
	rhash := bytes.TrimRight(n.Right.Hash, "\x00")
	lr := append(lhash, rhash...)
	lrhash := crypto.Keccak256(lr)
	fmt.Printf("L+R: %s => %x\n", bsplit(lr), lrhash)
	return lrhash
}

//NewTree creates a new Merkle Tree using the content cs.
func NewTree(cs []Content) (*MerkleTree, error) {
	root, leafs, err := buildWithContent(cs)
	if err != nil {
		return nil, err
	}
	t := &MerkleTree{
		Root:       root,
		merkleRoot: root.Hash,
		Leafs:      leafs,
	}
	return t, nil
}

//buildWithContent is a helper function that for a given set of Contents, generates a
//corresponding tree and returns the root node, a list of leaf nodes, and a possible error.
//Returns an error if cs contains no Contents.
func buildWithContent(cs []Content) (*Node, []*Node, error) {
	if len(cs) == 0 {
		return nil, nil, errors.New("Error: cannot construct tree with no content.")
	}
	var leafs []*Node
	for _, c := range cs {
		leafs = append(leafs, &Node{
			Hash: c.CalculateHash(),
			C:    c,
			leaf: true,
		})
	}
	if len(leafs)%2 == 1 {
		leafs = append(leafs, leafs[len(leafs)-1])
		leafs[len(leafs)-1].dup = true
	}
	root := buildIntermediate(leafs)
	return root, leafs, nil
}


//buildIntermediate is a helper function that for a given list of leaf nodes, constructs
//the intermediate and root levels of the tree. Returns the resulting root node of the tree.
func buildIntermediate(nl []*Node) *Node {
	var nodes []*Node
	for i := 0; i < len(nl); i += 2 {
		chash := append(nl[i].Hash, nl[i+1].Hash...)
		h := crypto.Keccak256(chash)
		n := &Node{
			Left:  nl[i],
			Right: nl[i+1],
			Hash:  h,
		}
		nodes = append(nodes, n)
		nl[i].Parent = n
		nl[i+1].Parent = n
        //TODO: set sister nodes
		if len(nl) == 2 {
			return n
		}
	}
	return buildIntermediate(nodes)
}

//MerkleRoot returns the unverified Merkle Root (hash of the root node) of the tree.
func (m *MerkleTree) MerkleRoot() []byte {
	return m.merkleRoot
}

//RebuildTree is a helper function that will rebuild the tree reusing only the content that
//it holds in the leaves.
func (m *MerkleTree) RebuildTree() error {
	var cs []Content
	for _, c := range m.Leafs {
		cs = append(cs, c.C)
	}
	root, leafs, err := buildWithContent(cs)
	if err != nil {
		return err
	}
	m.Root = root
	m.Leafs = leafs
	m.merkleRoot = root.Hash
	return nil
}

//RebuildTreeWith replaces the content of the tree and does a complete rebuild; while the root of
//the tree will be replaced the MerkleTree completely survives this operation. Returns an error if the
//list of content cs contains no entries.
func (m *MerkleTree) RebuildTreeWith(cs []Content) error {
	root, leafs, err := buildWithContent(cs)
	if err != nil {
		return err
	}
	m.Root = root
	m.Leafs = leafs
	m.merkleRoot = root.Hash
	return nil
}

//VerifyTree verify tree validates the hashes at each level of the tree and returns true if the
//resulting hash at the root of the tree matches the resulting root hash; returns false otherwise.
func (m *MerkleTree) VerifyTree() bool {
	calculatedMerkleRoot := m.Root.verifyNode()
	if bytes.Compare(m.merkleRoot, calculatedMerkleRoot) == 0 {
		return true
	}
	return false
}

func bsplit(rawhash []byte) string {
	var segments [][]byte
	n := 0
	for n < len(rawhash)/32 {
		segments = append(segments, rawhash[n*32:(n+1)*32])
		n++
	}
	s := fmt.Sprintf("%x", segments)
	return s
}

//VerifyContent indicates whether a given content is in the tree and the hashes are valid for that content.
//Returns true if the expected Merkle Root is equivalent to the Merkle root calculated on the critical path
//for a given content. Returns true if valid and false otherwise.
func (m *MerkleTree) VerifyContent(expectedMerkleRoot []byte, content Content) (res bool, proof []byte) {
	fmt.Printf("Verifying: %s\n", content)
	var mkproof []byte
	for _, l := range m.Leafs {
		if l.C.Equals(content) {
			currentSelf := l
			currentSister := l.Hash
			currentParent := l.Parent
			for currentParent != nil {

				if bytes.Compare(currentSelf.Hash, currentParent.Left.Hash) == 0 {
					currentSister = currentParent.Right.Hash
				} else {
					currentSister = currentParent.Left.Hash
				}
				mkproof = append(mkproof, currentSister...)
				fmt.Printf("Self:%x | Sister:%x | Parent: %x\n", currentSelf.Hash, currentSister, currentParent.Hash)

				if currentParent.Left.leaf && currentParent.Right.leaf {
					currentParentLR := append(currentParent.Left.calculateNodeHash(), currentParent.Right.calculateNodeHash()...)
					currentParentHash := crypto.Keccak256(currentParentLR)
					if bytes.Compare(currentParentHash, currentParent.Hash) != 0 {
						fmt.Printf("[Mismatch0] [ParentLRHash:%v] => CurrentParent Hash: %v\n", currentParentHash, currentParent.Hash)
						return false, nil
					} else {
						fmt.Printf("[Match0] [currentParentLR:%s] => CurrentParent Hash: %x\n", bsplit(currentParentLR), currentParentHash)
					}

					currentSelf = currentParent
					currentParent = currentParent.Parent
					fmt.Printf("New Parent:%x\n", currentParent.Hash)
				} else {
					currentParentLR := append(currentParent.Left.calculateNodeHash(), currentParent.Right.calculateNodeHash()...)
					currentParentHash := crypto.Keccak256(currentParentLR)
					if bytes.Compare(currentParentHash, currentParent.Hash) != 0 {
						fmt.Printf("[Mismatch1] [ParentLRHash:%v] => CurrentParent Hash: %v\n", currentParentHash, currentParent.Hash)
						return false, nil
					} else {
						fmt.Printf("[Match1] [currentParentLR:%s] => CurrentParent Hash: %x\n", bsplit(currentParentLR), currentParentHash)
					}
					currentSelf = currentParent
					currentParent = currentParent.Parent
				}
			}
			fmt.Printf("Proof:%x\n", mkproof)
			return true, mkproof
		}
	}
	return false, nil
}

func (m *MerkleTree) GetProof(expectedMerkleRoot []byte, content Content) bool {
	fmt.Printf("Proof: %s\n", content)
	var mkproof []byte

	for _, l := range m.Leafs {
		if l.C.Equals(content) {
			currentSelf := l
			currentSister := l.Hash
			currentParent := l.Parent
			for currentParent != nil {
				if bytes.Compare(currentSelf.Hash, currentParent.Left.Hash) == 0 {
					currentSister = currentParent.Right.Hash
				} else {
					currentSister = currentParent.Left.Hash
				}
				mkproof = append(mkproof, currentSister...)

				fmt.Printf("Self:%x | Sister:%x | Parent: %x\n", currentSelf.Hash, currentSister, currentParent.Hash)
				currentSelf = currentParent
				currentParent = currentParent.Parent
			}
			fmt.Printf("Proof:%x\n", mkproof)
			return true
		}
	}
	return false
}


func CheckProof(expectedMerkleRoot []byte, content []byte, mkproof []byte) bool {
	merkleroot := append(content[:0], content...)
	merklepath := merkleroot
	depth := 0
	for depth < len(mkproof)/32 {
		start := depth * 32
		end := start + 32
		merkleroot = crypto.Keccak256(append(merkleroot, mkproof[start:end]...))
		merklepath = append(merklepath, merkleroot...)
		depth++
	}
	if bytes.Compare(expectedMerkleRoot, merkleroot) != 0 {
		fmt.Printf("[CheckProof][FALSE] Expected: [%x] | Actual: [%x] | MRPath: {%v} | Proof {%v}\n", expectedMerkleRoot, merkleroot, bsplit(merklepath), bsplit(mkproof))
		return false
	} else {
		fmt.Printf("[CheckProof][TRUE] MRPath: {%v} | Proof {%v}\n", bsplit(merklepath), bsplit(mkproof))
		return true
	}
}

func (n *Node) getString() string {
	x := ""
	var lnode []byte
	var rnode []byte

	if n.Parent == nil {
		x = fmt.Sprintf("Merkle Root: %x | Self: %x | Left:%x | Right: %x | leaf: %t | dup: %t | Content: %v\n", n.Hash, &n, &n.Left, &n.Right, n.leaf, n.dup, n.C)
	} else if !n.leaf {
		x = fmt.Sprintf("Intermediate Node: %x| Left:%x | Right: %x | leaf: %t | dup: %t | Content: %v\n", n.Hash, &n.Left, &n.Right, n.leaf, n.dup, n.C)
	} else {
		x = fmt.Sprintf("Leaf Node: %x | Self:%x | Left:%x | Right: %x | leaf: %t | dup: %t | Content: %v\n", n.Hash, &n, lnode, rnode, n.leaf, n.dup, n.C)
	}
	return x
}

//String returns a string representation of the tree. Only leaf nodes are included
//in the output.
func (m *MerkleTree) String() string {
	s := ""
	s += m.Root.getString()
	for _, l := range m.Leafs {
		x := fmt.Sprintf("Parent:%v | Sister: %v| Left:%v | Right: %v | leaf: %t | dup: %t | Hash: %x | Content: %v\n", &l.Parent, &l.Sister, l.Left, l.Right, l.leaf, l.dup, l.Hash, l.C)
		s += x
	}
	return s
}
