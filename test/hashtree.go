package main

import(
	"bytes"
	"fmt"
	"github.com/ethereum/go-ethereum/crypto/sha3"
	"strconv"
)

const binnum = 64

type Val interface{}

type Node struct{
	Key []byte
	Next	bool
	Bin		[]*Node
	Value	Val
	Level	int
	Root	bool
}

func keyhash(k []byte) [32]byte{
	return sha3.Sum256(k)
}

func hashbin(k [32]byte, level int) int{
	x := 0x3F
	bytepos := level*6/8
	bitpos := level*6%8
	fmt.Println("bytepos", bytepos, "bitpos", bitpos)
	var fb int
	if bitpos <= 2{
		fb = int(k[bytepos]) >> uint(2-bitpos)
	}else{
		fb = int(k[bytepos]) << uint(bitpos-2) 
		fb = fb + (int(k[bytepos+1])>> uint(8 - (6 - (8-bitpos))))
		fmt.Println("bitpos >2 ", bytepos, bitpos, fb, uint(8 - (6 - (8-bitpos))))
	}	
	fb = fb&x
	fmt.Printf("fb = %d %x\n", fb, fb)
	return fb
}

func NewNode(k []byte, val Val) *Node{
	var nodelist = make([]*Node, binnum)
	var node = &Node{
		Key: k,
		Next:false,
		Bin: nodelist,	
		Value:	val,	
		Level:	0,
		Root:	false,
	}
	return node
}

func NewRootNode(k []byte, val Val) *Node{
	return newRootNode(k, val, 0)
}

func newRootNode(k []byte, val Val, l int) *Node{
    var nodelist = make([]*Node, binnum)
	var n = &Node{
        Key: k,
        Next: false,
        Bin: nil,
        Value:  val,
        Level:  l+1,
        Root:   false,
    }

	kh := keyhash(k)
	
	var bnum int
	bnum = hashbin(kh, l)
	
	nodelist[bnum] = n
    var rootnode = &Node{
        Key: nil,
        Next: true,
        Bin: nodelist,
        Value:  nil,
        Level:  l,
        Root:   true,
    }
	return rootnode
}

func (self *Node)Add(k []byte, v Val){
	self.add(NewNode(k, v))
	return
}

func (self *Node)add(addnode *Node) (newnode *Node){
	kh := keyhash(addnode.Key)
	bin := hashbin(kh, self.Level)
	fmt.Println("add ", string(addnode.Key), bin)
	
	if self.Next || self.Root{
		if self.Bin[bin] != nil{
			self.Bin[bin] = self.Bin[bin].add(addnode)
		}else{
			addnode.Level = self.Level+1 
			self.Bin[bin] = addnode
		}
	}else{
		if bytes.Compare(self.Key, addnode.Key) == 0{
			return self
		}
		n := newRootNode(self.Key, self.Value, self.Level)
		fmt.Printf("add split %s level %d bin %d\n", addnode.Key, addnode.Level, bin)
		n.Next = true
		n.add(addnode)
		return n
	}
	return self
}
	
func (self *Node)Get(k []byte) Val{
	kh := keyhash(k)
	bin := hashbin(kh, self.Level)

	fmt.Println("Get ", kh, bin, "key = ",string(self.Key))
	if self.Bin[bin] == nil{
		return nil
	}
	
	if self.Bin[bin].Next{
		fmt.Println("Get Next ", k, bin, self.Bin[bin].Key)
		return self.Bin[bin].Get(k)
	}else{
		fmt.Println("Get find ", k, self.Value)
		return self.Bin[bin].Value
	} 
	return nil
}

func (self *Node)Print(){
	fmt.Printf("Key = %s\nValue = %s\nNext = %v\nLevel = %d\nRoot = %v\n", self.Key, self.Value, self.Next, self.Level, self.Root)
	for i, b := range self.Bin{
		fmt.Printf("%d %v\n", i, b)
	}
}


func main(){
	root := NewRootNode([]byte("aaa"), "data")
	fmt.Println(root)
	root.Add([]byte("bbb"), "data2")
	fmt.Println(root)
	fmt.Println(root.Get([]byte("bbb")))
	buf := "teststr"
	data := "testdata"
	for i :=0; i <1000; i++{
		str := buf + strconv.Itoa(i)
		dstr := data + strconv.Itoa(i)
fmt.Println("*******",i,"*********", str)
		root.Add([]byte(str), []byte(dstr))
fmt.Println("=======",i,"=========", str)
	}
	fmt.Printf("%s\n",root.Get([]byte("teststr500")))
	fmt.Printf("%s\n",root.Get([]byte("teststr129")))
	fmt.Printf("%s\n",root.Get([]byte("teststr50")))
}


