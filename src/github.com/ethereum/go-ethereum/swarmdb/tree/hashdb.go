package swarmdb

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"github.com/ethereum/go-ethereum/common"
	sdbcommon "github.com/ethereum/go-ethereum/swarmdb/common"
	"github.com/ethereum/go-ethereum/crypto/sha3"
	"github.com/ethereum/go-ethereum/log"
	"github.com/ethereum/go-ethereum/swarm/storage"
	"github.com/ethereum/go-ethereum/swarm/api"
	"io"
	"reflect"
	"strconv"
	"strings"
	"sync"
)

const binnum = 64

type Val interface{}

type hashNode Node

type HashDB struct {
	rootnode *Node
	api      *api.Api
	mutex    sync.Mutex
}

type Node struct {
	Key      []byte
	Next     bool
	Bin      []*Node
	Value    Val
	Level    int
	Root     bool
	Version  int
	NodeKey  []byte //for disk/(net?)DB. Currently, it's bin data but it will be the hash
	NodeHash []byte //for disk/(net?)DB. Currently, it's bin data but it will be the hash
	Loaded   bool
}
func (self *HashDB) GetRootHash() ([]byte, error) {
	return self.rootnode.NodeHash, nil
}

func NewHashDB(rootnode []byte, api *api.Api) (*HashDB, error) {
fmt.Println("In NewHashDB")
	hd := new(HashDB)
	n := NewNode(nil, nil)
	n.Root = true
	if rootnode == nil{
	}else{
		n.NodeHash = rootnode
	}
	hd.rootnode = n
	hd.api = api
fmt.Println("NewHashDB = ", hd)
	return hd, nil
}

func keyhash(k []byte) [32]byte {
	return sha3.Sum256(k)
}

func hashbin(k [32]byte, level int) int {
	x := 0x3F
	bytepos := level * 6 / 8
	bitpos := level * 6 % 8
	var fb int
	if bitpos <= 2 {
		fb = int(k[bytepos]) >> uint(2-bitpos)
	} else {
		fb = int(k[bytepos]) << uint(bitpos-2)
		fb = fb + (int(k[bytepos+1]) >> uint(8-(6-(8-bitpos))))
	}
	fb = fb & x
	return fb
}

func NewNode(k []byte, val Val) *Node {
	var nodelist = make([]*Node, binnum)
	var node = &Node{
		Key:      k,
		Next:     false,
		Bin:      nodelist,
		Value:    val,
		Level:    0,
		Root:     false,
		Version:  0,
		NodeKey:  nil,
		NodeHash: nil,
		Loaded:   true,
	}
	return node
}

func NewRootNode(k []byte, val Val) *Node {
	return newRootNode(k, val, 0, 0, []byte("0:0"))
}

func newRootNode(k []byte, val Val, l int, version int, NodeKey []byte) *Node {
	var nodelist = make([]*Node, binnum)
	kh := keyhash(k)
	var bnum int
	bnum = hashbin(kh, l)
	newnodekey := string(NodeKey) + "|" + strconv.Itoa(bnum)
	var n = &Node{
		Key:     k,
		Next:    false,
		Bin:     nil,
		Value:   val,
		Level:   l + 1,
		Root:    false,
		Version: version,
		NodeKey: []byte(newnodekey),
	}

	nodelist[bnum] = n
	var rootnode = &Node{
		Key:     nil,
		Next:    true,
		Bin:     nodelist,
		Value:   nil,
		Level:   l,
		Root:    true,
		Version: version,
		NodeKey: NodeKey,
	}
	return rootnode
}

func (self *HashDB)Open(owner, tablename, columnname []byte)(bool, error){
	return true, nil
}

func (self *HashDB)Put(k, v[]byte) (bool, error){
	self.rootnode.Add(k, v, self.api)
	return true, nil
}

func (self *HashDB) GetRootNode() []byte {
	return self.rootnode.NodeHash
}

func (self *Node) Add(k []byte, v Val, api *api.Api) {
	log.Debug(fmt.Sprintf("HashDB Add ", self))
	self.Version++
	self.NodeKey = []byte("0")
	self.add(NewNode(k, v), self.Version, self.NodeKey, api)
	return
}

func (self *Node) add(addnode *Node, version int, nodekey []byte, api *api.Api) (newnode *Node) {
	kh := keyhash(addnode.Key)
	bin := hashbin(kh, self.Level)
	log.Debug(fmt.Sprintf("hashdb add ", string(addnode.Key), bin, self.Version, string(self.NodeKey)))
	self.NodeKey = nodekey

	log.Debug(fmt.Sprintf("hashdb add Next %v %v %v", self.Next, self.Root, self.Loaded))
	if self.Loaded == false {
		self.load(api)
		self.Loaded = true
	}
	log.Debug(fmt.Sprintf("hashdb add Next!! %v %v %v %v", self.Next, self.Root, self.Loaded, self.Bin[bin]))

	if self.Next || self.Root {
		if self.Bin[bin] != nil {
			log.Debug(fmt.Sprintf("hashdb add bin not nil %d %v", bin, self.Bin[bin].NodeHash))
			newnodekey := string(self.NodeKey) + "|" + strconv.Itoa(bin)
			if self.Bin[bin].Loaded == false {
				self.Bin[bin].load(api)
			}
			self.Bin[bin] = self.Bin[bin].add(addnode, version, []byte(newnodekey), api)
			var str string
			for i, b := range self.Bin {
				if b != nil {
					if b.Key != nil {
						str = str + "|" + strconv.Itoa(i) + ":" + string(b.Key)
					} else {
						str = str + "|" + strconv.Itoa(i)
					}
				}
			}
		} else {
			log.Debug(fmt.Sprintf("hashdb add bin nil %d", bin))
			addnode.Level = self.Level + 1
			addnode.NodeKey = []byte(string(self.NodeKey) + "|" + strconv.Itoa(bin))
			sdata := make([]byte, 64*4)
			a := convertToByte(addnode.Value)
			copy(sdata[64:], convertToByte(addnode.Value))
			log.Debug(fmt.Sprintf("hashdb add bin leaf Value %v %s %s %v a = %s a = %v", sdata, addnode.Key, addnode.Value, addnode.Value, a, a))
			copy(sdata[96:], addnode.Key)
			log.Debug(fmt.Sprintf("hashdb add bin leaf Key %v %s %s %v", sdata, addnode.Key, addnode.Value, addnode.Key))
			//rd := bytes.NewReader(sdata)
			//wg := &sync.WaitGroup{}
			//dhash, _ := api.Store(rd, int64(len(sdata)), wg, nil)
			//dhash, _ := api.Store(rd, int64(len(sdata)), wg)
			dhash, _ := api.StoreDBChunk(sdata)
			//wg.Wait()
			addnode.NodeHash = dhash
			log.Debug(fmt.Sprintf("hashdb add bin leaf %d %v", bin, dhash))
			self.Bin[bin] = addnode
		}
	} else {
		log.Debug(fmt.Sprintf("hashdb add node not next %d '%s' '%v' '%s' '%v' %v", bin, self.Key, self.Key, addnode.Key, addnode.Key, strings.Compare(string(self.Key), string(addnode.Key))))
		if strings.Compare(string(self.Key), string(addnode.Key)) == 0 {
			return self
		}
		if len(self.Key) == 0 {
			sdata := make([]byte, 64*4)
			a := convertToByte(addnode.Value)
			copy(sdata[64:], convertToByte(addnode.Value))
			log.Debug(fmt.Sprintf("hashdb add bin leaf Value %v %s %s %v a = %s a = %v", sdata, addnode.Key, addnode.Value, addnode.Value, a, a))
			copy(sdata[96:], addnode.Key)
			log.Debug(fmt.Sprintf("hashdb add bin leaf Key %v %s %s %v", sdata, addnode.Key, addnode.Value, addnode.Key))
			//rd := bytes.NewReader(sdata)
			//wg := &sync.WaitGroup{}
			//dhash, _ := api.Store(rd, int64(len(sdata)), wg)
			dhash, _ := api.StoreDBChunk(sdata)
			//wg.Done()
			addnode.NodeHash = dhash
			addnode.Next = false
			addnode.Loaded = true
			self = addnode
			return self
		}
		n := newRootNode(self.Key, self.Value, self.Level, version, self.NodeKey)
		n.Next = true
		n.Root = self.Root
		n.add(addnode, version, self.NodeKey, api)
		n.NodeHash = self.storeBinToNetwork(api)
		//api.ldb.Put([]byte(n.NodeKey), n.NodeHash)
/*
		if n.Root {
			api.ldb.Put([]byte("RootNode"), n.NodeHash)
			fmt.Println("store rootnode ", self.NodeHash)
		}
*/
		n.Loaded = true
		return n
	}
	var svalue string
	for i, b := range self.Bin {
		if b != nil {
			svalue = svalue + "|" + strconv.Itoa(i)
		}
	}
	self.NodeHash = self.storeBinToNetwork(api)
/*
	if self.Root {
		api.ldb.Put([]byte("RootNode"), self.NodeHash)
	}
*/
	self.Loaded = true
	return self
}

func compareVal(a, b Val) int {
	if va, ok := a.([]byte); ok {
		if vb, ok := b.([]byte); ok {
			return bytes.Compare(va, vb)
		}
	}
	return 100
}

func convertToByte(a Val) []byte {
	log.Trace(fmt.Sprintf("convertToByte type: %v '%v'", a, reflect.TypeOf(a)))
	if va, ok := a.([]byte); ok {
		log.Trace(fmt.Sprintf("convertToByte []byte: %v '%v' %s", a, va, string(va)))
		return []byte(va)
	}
	if va, ok := a.(storage.Key); ok {
		log.Trace(fmt.Sprintf("convertToByte storage.Key: %v '%v' %s", a, va, string(va)))
		return []byte(va)
	} else if va, ok := a.(string); ok {
		return []byte(va)
	}
	return nil
}

func (self *Node) storeBinToNetwork(api *api.Api) []byte {
	storedata := make([]byte, 66*64)

	if self.Next || self.Root {
		binary.LittleEndian.PutUint64(storedata[0:8], uint64(1))
	} else {
		binary.LittleEndian.PutUint64(storedata[0:8], uint64(0))
	}
	binary.LittleEndian.PutUint64(storedata[9:32], uint64(self.Level))
	//fmt.Println(storedata)

	for i, bin := range self.Bin {
		//copy(storedata[64+i*32:], bin.NodeHash[0:32])
		if bin != nil {
			//fmt.Println(string(bin.NodeKey))
			copy(storedata[64+i*32:], bin.NodeHash)
			//fmt.Printf("storing bin hash %v %s %d \n", bin.NodeHash, bin.NodeHash, len(bin.NodeHash))
			//h := fmt.Sprintf("%s", bin.NodeHash)
			//fmt.Printf("storing bin hash2 %v %s %d \n", h, h, len(h))
		}
	}
	//rd := bytes.NewReader(storedata)
	//wg := &sync.WaitGroup{}
	//adhash, _ := api.Store(rd, int64(len(storedata)), wg, nil)
	adhash, _ := api.StoreDBChunk(storedata)
	//wg.Wait()
	return adhash
}

func (self *HashDB)Get(k []byte)([]byte, bool, error){
	ret := self.rootnode.Get(k, self.api) 
	b := true
	if ret == nil{
		b = false
		var err *sdbcommon.KeyNotFoundError
		return nil, b, err
	}
	value := convertToByte(ret)
	return value, b, nil
}

func (self *Node) Get(k []byte, api *api.Api) Val {
	kh := keyhash(k)
	bin := hashbin(kh, self.Level)
	log.Trace(fmt.Sprintf("hashdb Node Get: %d '%v %v'", bin, k, kh))

	if self.Loaded == false {
		log.Trace(fmt.Sprintf("hashdb Node Get NodeHash: %v", self.NodeHash))
		self.load(api)
		self.Loaded = true
	}

	if self.Bin[bin] == nil {
		log.Trace(fmt.Sprintf("hashdb Node Get bin nil: %v'", bin))
		return nil
	}
	if self.Bin[bin].Loaded == false {
		log.Trace(fmt.Sprintf("hashdb Node Get loaded false: %v' %d", bin, self.Bin[bin].NodeHash))
		self.Bin[bin].load(api)
	}
	if self.Bin[bin].Next {
		log.Trace(fmt.Sprintf("hashdb Node Get next: %v'", bin))
		return self.Bin[bin].Get(k, api)
	} else {
		log.Trace(fmt.Sprintf("hashdb Node Get fin: %v %s %v %v'", k, k, self.Bin[bin].Value, self.Bin[bin].Value))
		if compareVal(k, self.Bin[bin].Key) == 0 {
			return self.Bin[bin].Value
		}
	}
	return nil
}

func (self *Node) load(api *api.Api) {
	log.Trace(fmt.Sprintf("hashdb Node Get load: %v %s", self.NodeHash, common.Bytes2Hex(self.NodeHash)))
/*
	//reader := api.Retrieve(self.NodeHash)
	buf := make([]byte, 4096)
	offset, err := reader.Read(buf)
*/
	buf, err := api.RetrieveDBChunk(self.NodeHash)
	lf := int64(binary.LittleEndian.Uint64(buf[0:8]))
	//log.Trace(fmt.Sprintf("hashdb Node Get load: %d '%v %v'", offset, buf, err))
	if err != nil && err != io.EOF {
		log.Trace(fmt.Sprintf("hashdb load Node Get err: %d  %v'", lf, err))
		self.Loaded = false
		self.Next = false
		return
	}
	emptybyte := make([]byte, 32)
	if lf == 1 {
		log.Trace(fmt.Sprintf("hashdb load Node Get bins: %d  %v'", lf, self.NodeHash))
		for i := 0; i < 64; i++ {
			binnode := NewNode(nil, nil)
			binnode.NodeHash = make([]byte, 32)
			binnode.NodeHash = buf[64+32*i : 64+32*(i+1)]
			binnode.Loaded = false
			if binnode.NodeHash == nil || bytes.Compare(binnode.NodeHash, emptybyte) == 0 {
				log.Trace(fmt.Sprintf("hashdb Node Get load nil: %d '%v'", i, binnode.NodeHash))
				self.Bin[i] = nil
			} else {
				log.Trace(fmt.Sprintf("hashdb Node Get load true: %d '%v'", i, binnode.NodeHash))
				self.Bin[i] = binnode
			}
		}
		self.Next = true
	} else {
		log.Trace(fmt.Sprintf("hashdb load Node Get leaf: %d  %v'", lf, self.NodeHash))
		var pos int

		eb := make([]byte, 1)
		log.Trace(fmt.Sprintf("hashdb Node Get load index: %d", bytes.Index(buf, eb)))
		for pos = 96; pos < len(buf); pos++ {
			if buf[pos] == 0 {
				break
			}
		}
		log.Trace(fmt.Sprintf("hashdb Node Get load index: %d pos = %d", bytes.Index(buf[96:], eb), pos))
		self.Key = buf[96:pos]
		self.Value = buf[64:96]
		self.Next = false
		log.Trace(fmt.Sprintf("hashdb Node Get load leaf: %s '%s'", self.Key, self.Value))
	}
	self.Loaded = true
	log.Trace(fmt.Sprintf("hashdb Node Get load self: %v'", self))
}

func (self *HashDB)Insert(k, v []byte) (bool, error){
	res, b, _ := self.Get(k)
	if res != nil || b{
		err := fmt.Errorf("%s is already in Database", string(k))
		return false, err
	}
	_, err := self.Put(k, v)
	return true, err
}

func (self *HashDB) Delete(k []byte) (bool, error) {
	self.rootnode.Delete(k)
	return true, nil
}

func (self *Node) Delete(k []byte) (newnode *Node) {
	/*
		if self.Get(k) == nil{
			return self
		}
	*/
	kh := keyhash(k)
	bin := hashbin(kh, self.Level)

	fmt.Println("Delete ", kh, bin, "key = ", string(self.Key))
	if self.Bin[bin] == nil {
		return nil
	}

	if self.Bin[bin].Next {
		fmt.Println("Delete Next ", k, bin, self.Bin[bin].Key)
		self.Bin[bin] = self.Bin[bin].Delete(k)
		bincount := 0
		pos := -1
		for i, b := range self.Bin {
			if b != nil {
				bincount++
				pos = i
			}
		}
		if bincount == 1 && self.Bin[pos].Next == false {
			return self.Bin[pos]
		}
	} else {
		fmt.Println("Delete find ", k, self.Value)
		self.Bin[bin] = nil

		bincount := 0
		pos := -1
		for i, b := range self.Bin {
			if b != nil {
				bincount++
				pos = i
			}
		}
		if bincount == 1 {
			return self.Bin[pos]
		}
	}
	return self
}

func (self *Node) Update(updatekey []byte, updatevalue []byte) (newnode *Node, err error) {
	kh := keyhash(updatekey)
	bin := hashbin(kh, self.Level)

	fmt.Println("Update ", kh, bin, "key = ", string(self.Key))
	if self.Bin[bin] == nil {
		return self, nil
	}

	if self.Bin[bin].Next {
		fmt.Println("Update Next ", updatekey, bin, self.Bin[bin].Key)
		return self.Bin[bin].Update(updatekey, updatevalue)
	} else {
		fmt.Println("Update find ", updatekey, self.Value)
		self.Bin[bin].Value = updatevalue
		return self, nil
		//return self.Bin[bin].Value
	}
	err = fmt.Errorf("couldn't find the key for updating")
	return self, err
}

func (self *HashDB) Close()(bool, error) {
	return true, nil
}

func (self *HashDB)StartBuffer()(bool, error){
	return true, nil
}

func(self *HashDB)FlushBuffer()(bool, error){
	return true, nil
}

func (self *HashDB)Print(){
	return
}
