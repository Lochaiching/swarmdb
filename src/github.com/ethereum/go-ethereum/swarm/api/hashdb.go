package api

import(
	"encoding/binary"
	"bytes"
	"fmt"
	"github.com/ethereum/go-ethereum/crypto/sha3"
	"strconv"
	"github.com/ethereum/go-ethereum/log"
	"sync"
)

const binnum = 64

type Val interface{}

type hashNode Node

type Node struct{
	Key []byte
	Next	bool
	Bin	 []*Node
	Value   Val
	Level   int
	Root	bool
	Version int
	NodeKey []byte  //for disk/(net?)DB. Currently, it's bin data but it will be the hash
	NodeHash	[]byte  //for disk/(net?)DB. Currently, it's bin data but it will be the hash
	Loaded	bool
}


func keyhash(k []byte) [32]byte{
	return sha3.Sum256(k)
}	   

func hashbin(k [32]byte, level int) int{
	x := 0x3F
	bytepos := level*6/8
	bitpos := level*6%8
	var fb int
	if bitpos <= 2{
		fb = int(k[bytepos]) >> uint(2-bitpos)
	}else{
		fb = int(k[bytepos]) << uint(bitpos-2)
		fb = fb + (int(k[bytepos+1])>> uint(8 - (6 - (8-bitpos))))
	}
	fb = fb&x
	return fb
}

func NewNode(k []byte, val Val) *Node{
	var nodelist = make([]*Node, binnum)
	var node = &Node{
		Key: k,
		Next:false,
		Bin: nodelist,
		Value:  val,
		Level:  0,
		Root:   false,
		Version: 0,
		NodeKey: nil,
		NodeHash: nil,
		Loaded: true,
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
	newnodekey := string(NodeKey)+"|"+strconv.Itoa(bnum)
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
/*
func (self *Api)HashDBAdd(k []byte, v Val, wg *sync.WaitGroup){
	self.hashdbroot.Add(k, v, self, wg)
}
*/

func (self *Node)Add(k []byte, v Val, api *Api, wg *sync.WaitGroup){
	log.Debug(fmt.Sprintf("HashDB Add ", self))
	self.Version++
	self.NodeKey = []byte("0")
	self.add(NewNode(k, v), self.Version, self.NodeKey, api, wg)
	return
}

func (self *Node)add(addnode *Node, version int, nodekey []byte, api *Api, wg *sync.WaitGroup) (newnode *Node){
	kh := keyhash(addnode.Key)
	bin := hashbin(kh, self.Level)
	log.Debug(fmt.Sprintf("add ", string(addnode.Key), bin, self.Version, string(self.NodeKey)))
	self.NodeKey = nodekey

	if self.Next || self.Root{
		if self.Bin[bin] != nil{
			newnodekey := string(self.NodeKey)+"|"+strconv.Itoa(bin)
			self.Bin[bin] = self.Bin[bin].add(addnode, version, []byte(newnodekey), api, wg)
			var str string
			for i, b := range self.Bin{
				if b != nil{
					if b.Key != nil {
						str = str+"|"+strconv.Itoa(i)+":"+string(b.Key)
					}else{
						str = str+"|"+strconv.Itoa(i)
					}
				}
			}
		}else{
			addnode.Level = self.Level+1
			addnode.NodeKey = []byte(string(self.NodeKey)+"|"+strconv.Itoa(bin))
			sdata := make([]byte, 32*4)
			copy(sdata[64:], convertToByte(addnode.Value))
			copy(sdata[96:], addnode.Key)
			fmt.Println("sdata = ", sdata)
			rd := bytes.NewReader(convertToByte(sdata))
			dhash, _ := api.dpa.Store(rd, int64(len(convertToByte(sdata))), wg, nil)
			addnode.NodeHash = dhash
			self.Bin[bin] = addnode
		}
	}else{
		if bytes.Compare(self.Key, addnode.Key) == 0{
			return self
		}
		n := newRootNode(self.Key, self.Value, self.Level, version, self.NodeKey)
		n.Next = true
		n.Root = self.Root
		n.add(addnode, version, self.NodeKey, api, wg)
		n.NodeHash = self.storeBinToNetwork(api, wg)
		api.ldb.Put([]byte(n.NodeKey), n.NodeHash)
		if n.Root {
			api.ldb.Put([]byte("RootNode"), n.NodeHash)
			fmt.Println("store rootnode ", self.NodeHash)
		}
		return n
	}
	var svalue string
	for i, b := range self.Bin{
		if b != nil{
			svalue = svalue+"|"+strconv.Itoa(i)
		}
	}
	self.NodeHash = self.storeBinToNetwork(api, wg)
	if self.Root {
		api.ldb.Put([]byte("RootNode"), self.NodeHash)
	}
	return self
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
		log.Trace(fmt.Sprintf("convertToByte: %v '%v' %s", a, va, string(va)))
		return []byte(va)
	} else if va, ok := a.(string); ok{
		return []byte(va)
	}
	return nil
}
 
func (self *Node)storeBinToNetwork(api *Api, wg *sync.WaitGroup) []byte{
	storedata := make([]byte, 66*64)

	if self.Next{
		binary.LittleEndian.PutUint64(storedata[0:8], uint64(1))
	}else{
		binary.LittleEndian.PutUint64(storedata[0:8], uint64(0))
	}
	binary.LittleEndian.PutUint64(storedata[9:32], uint64(self.Level))
	//fmt.Println(storedata)

	for i, bin := range self.Bin{
		//copy(storedata[64+i*32:], bin.NodeHash[0:32])
		if bin != nil{
			fmt.Println(string(bin.NodeKey))
			copy(storedata[64+i*32:], bin.NodeHash)
			fmt.Printf("storing bin hash %v %s %d \n", bin.NodeHash, bin.NodeHash, len(bin.NodeHash))
			h := fmt.Sprintf("%s", bin.NodeHash)
			fmt.Printf("storing bin hash2 %v %s %d \n", h, h, len(h))
		}
	}
	/////////
	//hash := getHash(storedata)
	rd := bytes.NewReader(storedata)
	//chunker := storage.NewTreeChunker(storage.NewChunkerParams())
	//hash, _ := chunker.Split(rd, int64(len(storedata)), nil, nil, nil)
	//rd = bytes.NewReader(storedata)
	//adhash, _ := client.UploadRaw(rd, int64(len(storedata)))
	adhash, _ := api.dpa.Store(rd, int64(len(storedata)), wg, nil)
	return adhash
	//return hash
}

func (self *Node)Get(k []byte, api *Api) Val{
	kh := keyhash(k)
	bin := hashbin(kh, self.Level)
   	log.Trace(fmt.Sprintf("hashdb Node Get: %d '%v %v'", bin, k, kh))

	if self.Loaded == false{
		reader := api.dpa.Retrieve(self.NodeHash)
		buf := make([]byte, 4096)
		offset, err := reader.Read(buf)	
		log.Trace(fmt.Sprintf("hashdb Node Get: %d '%v %v'", offset, buf, err))
		lf :=  int64(binary.LittleEndian.Uint64(buf[0:8]))
		if lf == 1{
			for i := 0; i < 64; i++{
				binnode := NewNode(nil, nil)
				binnode.NodeHash = make([]byte, 32)
				binnode.NodeHash = buf[64+32*i:64+32*(i+1)]
				binnode.Loaded = false
				self.Bin[i] = binnode
			}
			self.Next = true
		}else{
			self.Key = buf[94:]
			self.Value = buf[64:94]
			self.Next = false
		}
		self.Loaded = true
	}

	if self.Bin[bin] == nil{
		return nil
	}
	if self.Bin[bin].Loaded == false {
		self.Bin[bin].load(api)
	}
	if self.Bin[bin].Next {
		return self.Bin[bin].Get(k, api)
	}else{
		if compareVal(k, self.Bin[bin].Key) == 0{
			return self.Bin[bin].Value
		}
	}
	return nil
}

func (self *Node)load(api *Api){
		reader := api.dpa.Retrieve(self.NodeHash)
		buf := make([]byte, 4096)
		offset, err := reader.Read(buf)
		lf :=  int64(binary.LittleEndian.Uint64(buf[0:8]))
		log.Trace(fmt.Sprintf("hashdb Node Get: %d '%v %v'", offset, buf, err))
		if lf == 1{
			for i := 0; i < 64; i++{
				binnode := NewNode(nil, nil)
				binnode.NodeHash = make([]byte, 32)
				binnode.NodeHash = buf[64+32*i:64+32*(i+1)]
				binnode.Loaded = false
				self.Bin[i] = binnode
			}   
			self.Next = true
		}else{
			self.Key = buf[94:]
			self.Value = buf[64:94]
			self.Next = false
		}   
		self.Loaded = true
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


