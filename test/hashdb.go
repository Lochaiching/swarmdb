package main

import(
	"bytes"
	"fmt"
	"github.com/ethereum/go-ethereum/crypto/sha3"
	"github.com/ethereum/go-ethereum/swarm/storage"
//	"reflect"
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
	Version	int
	NodeKey	[]byte	//for disk/(net?)DB
}

var db *storage.LDBDatabase

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
		Version: 0,
		NodeKey: nil,
	}
	return node
}

func NewRootNode(k []byte, val Val) *Node{
	return newRootNode(k, val, 0, 0, []byte("0:0"))
}

func newRootNode(k []byte, val Val, l int, version int, NodeKey []byte) *Node{
    var nodelist = make([]*Node, binnum)
	kh := keyhash(k)
	var bnum int
	bnum = hashbin(kh, l)
	newnodekey := string(NodeKey)+"|"+strconv.Itoa(bnum)+":"+strconv.Itoa(version)
	var n = &Node{
        Key: k,
        Next: false,
        Bin: nil,
        Value:  val,
        Level:  l+1,
        Root:   false,
		Version: version,
		NodeKey: []byte(newnodekey),
    }
	
	nodelist[bnum] = n
    var rootnode = &Node{
        Key: nil,
        Next: true,
        Bin: nodelist,
        Value:  nil,
        Level:  l,
        Root:   true,
		Version: version,
		NodeKey: NodeKey,
    }
	return rootnode
}

func (self *Node)Add(k []byte, v Val){
	self.Version++
	self.NodeKey = []byte("0:"+strconv.Itoa(self.Version))
	self.add(NewNode(k, v), self.Version, self.NodeKey)
	return
}

func (self *Node)add(addnode *Node, version int, nodekey []byte) (newnode *Node){
	kh := keyhash(addnode.Key)
	bin := hashbin(kh, self.Level)
	fmt.Println("add ", string(addnode.Key), bin, self.Version, string(self.NodeKey))
	self.NodeKey = nodekey
	
	if self.Next || self.Root{
		if self.Bin[bin] != nil{
			newnodekey := string(self.NodeKey)+"|"+strconv.Itoa(bin)+":"+strconv.Itoa(version)
			self.Bin[bin] = self.Bin[bin].add(addnode, version, []byte(newnodekey))
		}else{
			addnode.Level = self.Level+1 
			addnode.NodeKey = []byte(string(self.NodeKey)+"|"+strconv.Itoa(bin)+":"+strconv.Itoa(version))
			self.Bin[bin] = addnode
			nk := string(addnode.Key)+":"+strconv.Itoa(addnode.Version)
			db.Put([]byte(nk), convertToByte(addnode.Value))
		}
	}else{
		if bytes.Compare(self.Key, addnode.Key) == 0{
			return self
		}
		n := newRootNode(self.Key, self.Value, self.Level, version, self.NodeKey)
		fmt.Printf("add split %s level %d bin %d\n", addnode.Key, addnode.Level, bin)
		n.Next = true
		n.add(addnode, version, self.NodeKey)
		return n
	}
	var svalue string
	for i, b := range self.Bin{
		if b != nil{
			svalue = svalue+"|"+strconv.Itoa(i)+":"+strconv.Itoa(b.Version)
		}
	}
	db.Put([]byte(self.NodeKey), []byte(svalue))
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
		fmt.Println("Get find ", k, self.Bin[bin].Value, string(self.Bin[bin].NodeKey))
		if compareVal(k, self.Bin[bin].Key) == 0{
			return self.Bin[bin].Value
		}
	} 
	return nil
}

func compareVal(a, b Val) int{
	if va, ok := a.([]byte); ok{
		if vb, ok := b.([]byte); ok{
			return bytes.Compare(va,vb)
		}
	}
	return 100
}

func convertToByte(a Val)[]byte{
	if va, ok := a.([]byte); ok{
		return []byte(va)
	} else if va, ok := a.(string); ok{
		return []byte(va)
	}
	return nil
}

func (self *Node)Update(updatekey []byte, updatevalue []byte)(newnode *Node, err error){
    kh := keyhash(updatekey)
    bin := hashbin(kh, self.Level)

    fmt.Println("Update ", kh, bin, "key = ",string(self.Key))
    if self.Bin[bin] == nil{
        return self, nil
    }

    if self.Bin[bin].Next{
        fmt.Println("Update Next ", updatekey, bin, self.Bin[bin].Key)
        return self.Bin[bin].Update(updatekey, updatevalue)
    }else{
        fmt.Println("Update find ", updatekey, self.Value)
		self.Bin[bin].Value = updatevalue
    	return self, nil
        //return self.Bin[bin].Value
    }
	err = fmt.Errorf("couldn't find the key for updating")
   	return self, err
}

func (self *Node)Delete(k []byte)(newnode *Node){
    kh := keyhash(k)
    bin := hashbin(kh, self.Level)

    fmt.Println("Delete ", kh, bin, "key = ",string(self.Key))
    if self.Bin[bin] == nil{
        return nil
    }

    if self.Bin[bin].Next{
        fmt.Println("Delete Next ", k, bin, self.Bin[bin].Key)
        self.Bin[bin] = self.Bin[bin].Delete(k)
        bincount := 0
        pos := -1
        for i, b := range self.Bin{
            if b != nil  {
                bincount++
                pos = i
            }
        }
        if bincount == 1 && self.Bin[pos].Next == false{
            return self.Bin[pos]
        }
    }else{
        fmt.Println("Delete find ", k, self.Value)
		self.Bin[bin] = nil
		
		bincount := 0
		pos := -1
		for i, b := range self.Bin{
			if b != nil {
				bincount++
				pos = i
			}
		}
		if bincount == 1{
			return self.Bin[pos]
		}
    }
	return self
}

func (self *Node)Print(){
	fmt.Printf("Key = %s\nValue = %s\nNext = %v\nLevel = %d\nRoot = %v\nNodeKey = %s\nVersion = %d\n", self.Key, self.Value, self.Next, self.Level, self.Root, self.NodeKey, self.Version)
/*
	for i, b := range self.Bin{
		fmt.Printf("%d %v\n", i, b)
	}
*/
}

func (self *Node)PrintNodes(){
	fmt.Printf("Key = %s\nValue = %s\nNext = %v\nLevel = %d\nRoot = %v\nNodeKey = %s\nVersion = %d\n", self.Key, self.Value, self.Next, self.Level, self.Root, self.NodeKey, self.Version)
	for i, b := range self.Bin{
		if b != nil{
			fmt.Println("Node ", i, "==========")
			b.PrintNodes()
		}
/*
		if b != nil{
			b.Print()
		}
*/
	}
}

func (self *Node)PrintNodeKeys(){
	fmt.Printf("%s\n", self.NodeKey)
    for _, b := range self.Bin{
        if b != nil{
            b.PrintNodeKeys()
        }
	}
}


func main(){
	db, _ = storage.NewLDBDatabase("/var/www/vhosts/mayumi/testdb")
	root := NewRootNode([]byte("aaa"), "data")
	fmt.Println(root)
	root.Add([]byte("bbb"), "data2")
	fmt.Println(root)
	fmt.Println(root.Get([]byte("bbb")))
	buf := "teststr"
	data := "testdata"
	for i :=0; i <250; i++{
		str := buf + strconv.Itoa(i)
		dstr := data + strconv.Itoa(i)
fmt.Println("*******",i,"*********", str)
		root.Add([]byte(str), []byte(dstr))
fmt.Println("=======",i,"=========", str)
	}
	root.PrintNodes()
/*
	fmt.Printf("%s\n",root.Get([]byte("teststr5")))
	root.Update([]byte("teststr50"), []byte("newteststr50"))
	    for i :=45; i <55; i++{
        str := buf + strconv.Itoa(i)
		fmt.Printf("*****************%d %s\n",i, root.Get([]byte(str)))
	}
*/


	root.PrintNodeKeys()

	fmt.Println("LastKnownTD ", db.LastKnownTD())

	db.Put([]byte("RootNode"), root.NodeKey)
	itr := db.NewIterator()
	fmt.Println(itr)
	for itr.Next(){
		fmt.Println(itr)
		fmt.Printf("%s\t%s\n",itr.Key(), itr.Value())
	}
}


