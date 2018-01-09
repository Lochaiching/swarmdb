package swarmdb

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"github.com/ethereum/go-ethereum/crypto/sha3"
	"github.com/ethereum/go-ethereum/log"
	"github.com/ethereum/go-ethereum/swarm/storage"
	"io"
	//"reflect"
	"strconv"
	"strings"
	"sync"
)

const binnum = 64
const STACK_SIZE = 100

type Val interface{}

type HashDB struct {
	rootnode   *Node
	swarmdb    *SwarmDB
	buffered   bool
	columnType ColumnType
	mutex      sync.Mutex
}

type Node struct {
	Key        []byte
	Value      Val
	Next       bool
	Bin        []*Node
	Level      int
	Root       bool
	Version    int
	NodeKey    []byte //for disk/(net?)DB. Currently, it's bin data but it will be the hash
	NodeHash   []byte //for disk/(net?)DB. Currently, it's bin data but it will be the hash
	Loaded     bool
	Stored     bool
	columnType ColumnType
	counter    int
}

type HashdbCursor struct {
	hashdb *HashDB
	level  int
	bin    *stack_t
	node   *Node
}

func (self *HashDB) GetRootHash() ([]byte, error) {
	return self.rootnode.NodeHash, nil
}

func NewHashDB(u *SWARMDBUser, rootnode []byte, swarmdb SwarmDB, columntype ColumnType) (*HashDB, error) {
	hd := new(HashDB)
	n := NewNode(nil, nil)
	n.Root = true
	if rootnode == nil {
		//fmt.Printf("rootnode is nill!?")
	} else {
		n.NodeHash = rootnode
		n.load(u, &swarmdb, columntype)
	}
	hd.rootnode = n
	hd.swarmdb = &swarmdb
	hd.buffered = false
	hd.columnType = columntype
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
		Loaded:   false,
		Stored:   true,
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

func (self *HashDB) Open(owner, tablename, columnname []byte) (bool, error) {
	return true, nil
}

func (self *HashDB) Put(u *SWARMDBUser, k []byte, v []byte) (bool, error) {
	self.rootnode.Add(u, k, v, self.swarmdb, self.columnType)
	return true, nil
}

func (self *HashDB) GetRootNode() []byte {
	return self.rootnode.NodeHash
}

func (self *Node) Add(u *SWARMDBUser, k []byte, v Val, swarmdb *SwarmDB, columntype ColumnType) {
	log.Debug(fmt.Sprintf("HashDB Add ", self))
	self.Version++
	self.NodeKey = []byte("0")
	self.columnType = columntype
	self.add(u, NewNode(k, v), self.Version, self.NodeKey, swarmdb, columntype)
	return
}

func (self *Node) add(u *SWARMDBUser, addnode *Node, version int, nodekey []byte, swarmdb *SwarmDB, columntype ColumnType) (newnode *Node) {
	kh := keyhash(addnode.Key)
	bin := hashbin(kh, self.Level)
	self.NodeKey = nodekey
	self.Stored = false
	addnode.Stored = false
	addnode.columnType = columntype

	if self.Loaded == false {
		self.load(u, swarmdb, columntype)
		self.Loaded = true
	}

	if self.Next || self.Root {
		if self.Bin[bin] != nil {
			newnodekey := string(self.NodeKey) + "|" + strconv.Itoa(bin)
			if self.Bin[bin].Loaded == false {
				self.Bin[bin].load(u, swarmdb, columntype)
			}
			self.Bin[bin] = self.Bin[bin].add(u, addnode, version, []byte(newnodekey), swarmdb, columntype)
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
			addnode.Level = self.Level + 1
			addnode.Loaded = true
			addnode.Stored = false
			addnode.Next = false
			addnode.NodeKey = []byte(string(self.NodeKey) + "|" + strconv.Itoa(bin))
			//sdata := make([]byte, 64*4)
			sdata := make([]byte, 4096)
			copy(sdata[64:], convertToByte(addnode.Value))
			copy(sdata[96:], addnode.Key)
			//rd := bytes.NewReader(sdata)
			//wg := &sync.WaitGroup{}
			/*
				//dhash, _ := swarmdb.Store(rd, int64(len(sdata)), wg, nil)
				dhash, _ := swarmdb.StoreDBChunk(sdata, 1)
				//wg.Wait()
				addnode.NodeHash = dhash
				log.Debug(fmt.Sprintf("hashdb add bin leaf %d %v", bin, dhash))
			*/
			self.Bin[bin] = addnode
		}
	} else {
		if strings.Compare(string(self.Key), string(addnode.Key)) == 0 {
			sdata := make([]byte, 4096)
			copy(sdata[64:], convertToByte(addnode.Value))
			copy(sdata[96:], addnode.Key)
			dhash, _ := swarmdb.StoreDBChunk(u, sdata, 1)
			addnode.NodeHash = dhash
			self.Value = addnode.Value
			return self
		}
		if len(self.Key) == 0 {
			//sdata := make([]byte, 64*4)
			sdata := make([]byte, 4096)
			copy(sdata[64:], convertToByte(addnode.Value))
			copy(sdata[96:], addnode.Key)
			//rd := bytes.NewReader(sdata)
			//wg := &sync.WaitGroup{}
			//dhash, _ := swarmdb.Store(rd, int64(len(sdata)), wg)
			//dhash, _ := swarmdb.StoreDBChunk(sdata, 1)
			//wg.Done()
			//addnode.NodeHash = dhash
			addnode.Next = false
			addnode.Loaded = true
			self = addnode
			return self
		}
		n := newRootNode(nil, nil, self.Level, version, self.NodeKey)
		n.Next = true
		n.Root = self.Root
		n.Level = self.Level
		n.Loaded = true
		addnode.Level = self.Level + 1
		cself := self
		cself.Level = self.Level + 1
		cself.Loaded = true
		n.add(u, addnode, version, self.NodeKey, swarmdb, columntype)
		n.add(u, cself, version, self.NodeKey, swarmdb, columntype)
		///////////
		//n.NodeHash = self.storeBinToNetwork(swarmdb)
		//swarmdb.Put([]byte(n.NodeKey), n.NodeHash)
		n.Loaded = true
		return n
	}
	var svalue string
	for i, b := range self.Bin {
		if b != nil {
			svalue = svalue + "|" + strconv.Itoa(i)
		}
	}
	///////////
	//self.NodeHash = self.storeBinToNetwork(swarmdb)
	self.Loaded = true
	return self
}

func compareVal(a, b Val) int {
	if va, ok := a.([]byte); ok {
		if vb, ok := b.([]byte); ok {
			return bytes.Compare(bytes.Trim(va, "\x00"), bytes.Trim(vb, "\x00"))
		}
	}
	return 100
}

func compareValType(a, b Val, columntype ColumnType) int {
	if va, ok := a.([]byte); ok {
		if vb, ok := b.([]byte); ok {
			switch columntype {
			case CT_INTEGER, CT_FLOAT:
				for i := 0; i < 8; i++ {
					if va[i] > vb[i] {
						return 1
					} else if va[i] < vb[i] {
						return -1
					}
				}
				return 0
			default:
				return bytes.Compare(bytes.Trim(va, "\x00"), bytes.Trim(vb, "\x00"))
			}
		}
	}
	return 100
}

func convertToByte(a Val) []byte {
	if va, ok := a.([]byte); ok {
		return []byte(va)
	}
	if va, ok := a.(storage.Key); ok {
		return []byte(va)
	} else if va, ok := a.(string); ok {
		return []byte(va)
	}
	return nil
}

func (self *Node) storeBinToNetwork(u *SWARMDBUser, swarmdb *SwarmDB) []byte {
	storedata := make([]byte, 66*64)

	if self.Next || self.Root {
		binary.LittleEndian.PutUint64(storedata[0:8], uint64(1))
	} else {
		binary.LittleEndian.PutUint64(storedata[0:8], uint64(0))
	}
	binary.LittleEndian.PutUint64(storedata[9:32], uint64(self.Level))

	for i, bin := range self.Bin {
		//copy(storedata[64+i*32:], bin.NodeHash[0:32])
		if bin != nil {
			copy(storedata[64+i*32:], bin.NodeHash)
		}
	}
	//rd := bytes.NewReader(storedata)
	//wg := &sync.WaitGroup{}
	adhash, _ := swarmdb.StoreDBChunk(u, storedata, 1)
	//fmt.Printf("add hash node %x\n", adhash)
	//wg.Wait()
	return adhash
}

func (self *HashDB) Get(u *SWARMDBUser, k []byte) ([]byte, bool, error) {
	stack := newStack()
	ret := self.rootnode.Get(u, k, self.swarmdb, self.columnType, stack)
	value := bytes.Trim(convertToByte(ret), "\x00")
	b := true
	if ret == nil {
		b = false
		var err *KeyNotFoundError
		return nil, b, err
	}
	return value, b, nil
}

func (self *HashDB) getStack(u *SWARMDBUser, k []byte) ([]byte, *stack_t, error) {
	stack := newStack()
	ret := self.rootnode.Get(u, k, self.swarmdb, self.columnType, stack)
	value := bytes.Trim(convertToByte(ret), "\x00")
	if ret == nil {
		var err *KeyNotFoundError
		return nil, nil, err
	}
	return value, stack, nil
}

func (self *Node) Get(u *SWARMDBUser, k []byte, swarmdb *SwarmDB, columntype ColumnType, stack *stack_t) Val {
	kh := keyhash(k)
	bin := hashbin(kh, self.Level)

	if self.Loaded == false {
		self.load(u, swarmdb, columntype)
		self.Loaded = true
	}

	if self.Bin[bin] == nil {
		return nil
	}
	if self.Bin[bin].Loaded == false {
		self.Bin[bin].load(u, swarmdb, columntype)
	}
	if self.Bin[bin].Next {
		stack.Push(bin)
		return self.Bin[bin].Get(u, k, swarmdb, columntype, stack)
	} else {
		if compareValType(k, self.Bin[bin].Key, columntype) == 0 && len(convertToByte(self.Bin[bin].Value)) > 0 {
			stack.Push(bin)
			return self.Bin[bin].Value
		} else {
			return nil
		}
	}
	return nil
}

func (self *Node) load(u *SWARMDBUser, swarmdb *SwarmDB, columnType ColumnType) {
	buf, err := swarmdb.RetrieveDBChunk(u, self.NodeHash)
	lf := int64(binary.LittleEndian.Uint64(buf[0:8]))
	if err != nil && err != io.EOF {
		fmt.Printf("\nError loading node: [%s]", err)
		self.Loaded = false
		self.Next = false
		return
	}
	emptybyte := make([]byte, 32)
	if lf == 1 {
		for i := 0; i < 64; i++ {
			binnode := NewNode(nil, nil)
			binnode.NodeHash = make([]byte, 32)
			binnode.NodeHash = buf[64+32*i : 64+32*(i+1)]
			binnode.Loaded = false
			binnode.Level = self.Level + 1
			if binnode.NodeHash == nil || bytes.Compare(binnode.NodeHash, emptybyte) == 0 {
				self.Bin[i] = nil
			} else {
				self.Bin[i] = binnode
			}
		}
		self.Next = true
	} else {
		var pos int

		for pos = 96; pos < len(buf); pos++ {
			if buf[pos] == 0 {
				break
			}
		}
		if pos == 96 && bytes.Compare(buf[96:96+32], emptybyte) != 0 {
			pos = 96 + 32
		}
		if columnType == CT_INTEGER {
			pos = 96 + 8
		}
		self.Key = buf[96:pos]
		self.Value = buf[64:96]
		self.Next = false
		if len(bytes.Trim(convertToByte(self.Value), "\x00")) == 0 {
			self.Key = nil
			self.Value = nil
			self.Loaded = true
			self.Next = false
			return
		}
	}
	self.Loaded = true
}

func (self *HashDB) Insert(u *SWARMDBUser, k []byte, v []byte) (bool, error) {
	res, b, _ := self.Get(u, k)
	if res != nil || b {
		err := fmt.Errorf("%s is already in Database", string(k))
		return false, err
	}
	_, err := self.Put(u, k, v)
	return true, err
}

func (self *HashDB) Delete(u *SWARMDBUser, k []byte) (bool, error) {
	_, b := self.rootnode.Delete(u, k, self.swarmdb, self.columnType)
	return b, nil
}

func (self *Node) Delete(u *SWARMDBUser, k []byte, swarmdb *SwarmDB, columntype ColumnType) (newnode *Node, found bool) {
	found = false
	if self.Loaded == false {
		self.load(u, swarmdb, columntype)
	}
	stack := newStack()
	ret := self.Get(u, k, swarmdb, columntype, stack)
	if ret == nil {
		return self, false
	}
	kh := keyhash(k)
	bin := hashbin(kh, self.Level)

	if self.Bin[bin] == nil {
		return nil, found
	}

	if self.Bin[bin].Next {
		self.Bin[bin], found = self.Bin[bin].Delete(u, k, swarmdb, columntype)
		if found {
			bincount := 0
			pos := -1
			for i, b := range self.Bin[bin].Bin {
				if b != nil {
					bincount++
					pos = i
				}
			}
			if bincount == 1 && self.Bin[bin].Bin[pos].Next == false {
				self.Bin[bin].Bin[pos].Level = self.Bin[bin].Level
				self.Bin[bin].Bin[pos] = self.Bin[bin].Bin[pos].shiftUpper()
				self.Bin[bin] = self.Bin[bin].Bin[pos]
			}
			self.Stored = false
			self.Bin[bin].Stored = false
		}
		return self, found
	} else {
		if self.Bin[bin].Loaded == false {
			self.Bin[bin].load(u, swarmdb, columntype)
		}
		if len(self.Bin[bin].Key) == 0 {
			return self, false
		}
		match := compareValType(k, self.Bin[bin].Key, columntype)
		if match != 0 {
			return self, found
		}
		self.Stored = false
		found = true
		self.Bin[bin] = nil
	}
	return self, found
}

func (self *Node) shiftUpper() *Node {
	for i, bin := range self.Bin {
		if bin != nil {
			if bin.Next == true {
				bin = bin.shiftUpper()
			}
			bin.Level = bin.Level - 1
			self.Bin[i] = bin
		}
	}
	return self
}

func (self *Node) Update(updatekey []byte, updatevalue []byte) (newnode *Node, err error) {
	kh := keyhash(updatekey)
	bin := hashbin(kh, self.Level)

	if self.Bin[bin] == nil {
		return self, nil
	}

	if self.Bin[bin].Next {
		return self.Bin[bin].Update(updatekey, updatevalue)
	} else {
		self.Bin[bin].Value = updatevalue
		return self, nil
		//return self.Bin[bin].Value
	}
	err = fmt.Errorf("couldn't find the key for updating")
	return self, err
}

func (self *HashDB) Close(u *SWARMDBUser) (bool, error) {
	return true, nil
}

func (self *HashDB) StartBuffer(u *SWARMDBUser) (bool, error) {
	self.buffered = true
	return true, nil
}

func (self *HashDB) FlushBuffer(u *SWARMDBUser) (bool, error) {
	if self.buffered == false {
		//var err *NoBufferError
		//return false, err
	}
	_, err := self.rootnode.flushBuffer(u, self.swarmdb)
	if err != nil {
		return false, err
	}
	self.buffered = false
	return true, err
}

func (self *Node) flushBuffer(u *SWARMDBUser, swarmdb *SwarmDB) ([]byte, error) {
	for _, bin := range self.Bin {
		if bin != nil {
			if bin.Next == true && bin.Stored == false {
				_, err := bin.flushBuffer(u, swarmdb)
				if err != nil {
					return nil, err
				}
			} else if bin.Stored == false && len(bytes.Trim(convertToByte(bin.Value), "\x00")) > 0 {
				sdata := make([]byte, 4096)
				copy(sdata[64:], convertToByte(bin.Value))
				copy(sdata[96:], bin.Key)
				dhash, err := swarmdb.StoreDBChunk(u, sdata, 1)
				if err != nil {
					return nil, err
				}
				bin.NodeHash = dhash
				bin.Stored = true
			}
		}
	}
	self.NodeHash = self.storeBinToNetwork(u, swarmdb)
	self.Stored = true
	return self.NodeHash, nil
}

func (self *HashDB) Print(u *SWARMDBUser) {
	self.rootnode.print(u, self.swarmdb, self.columnType)
	return
}

func (self *Node) print(u *SWARMDBUser, swarmdb *SwarmDB, columnType ColumnType) {
	for binnum, bin := range self.Bin {
		if bin != nil {
			if bin.Loaded == false {
				bin.load(u, swarmdb, columnType)
				bin.Loaded = true
			}
			if bin.Next != true {
				fmt.Printf("leaf key = %v Value = %x binnum = %d level = %d Value len = %d\n", bin.Key, bin.Value, binnum, bin.Level, len(bytes.Trim(convertToByte(bin.Value), "\x00")))
			} else {
				fmt.Printf("node key = %v Value = %x binnum = %d level = %d\n", bin.Key, bin.Value, binnum, bin.Level)
				bin.print(u, swarmdb, columnType)
			}
		}
	}
}

func (self *HashDB) Seek(u *SWARMDBUser, k []byte) (*HashdbCursor, bool, error) {
	ret, stack, err := self.getStack(u, k)
	if err != nil {
		return nil, false, err
	}
	if ret == nil {
		return nil, false, fmt.Errorf("No Data")
	}
	cursor, err := newHashdbCursor(self)
	if err != nil {
		return nil, false, err
	}
	node := self.rootnode
	for i := 0; i < stack.Size()-1; i++ {
		bin := stack.GetPos(i)
		node = node.Bin[bin]
	}
	cursor.bin = stack
	cursor.node = node
	cursor.level = stack.Size()
	return cursor, true, nil
}

func (self *HashDB) SeekFirst(u *SWARMDBUser) (*HashdbCursor, error) {
	cursor, err := newHashdbCursor(self)
	if err != nil {
		return nil, err
	}
	err = cursor.seeknext(u)
	if err != nil {
		return nil, err
	}
	return cursor, nil
}

func (self *HashDB) SeekLast(u *SWARMDBUser) (*HashdbCursor, error) {
	cursor, err := newHashdbCursor(self)
	if err != nil {
		return nil, err
	}
	err = cursor.seekprev(u)
	if err != nil {
		return nil, err
	}
	return cursor, nil
}

func newHashdbCursor(hashdb *HashDB) (*HashdbCursor, error) {
	cursor := &HashdbCursor{
		hashdb: hashdb,
		level:  0,
		bin:    newStack(),
		node:   hashdb.rootnode,
	}
	return cursor, nil
}

func (self *HashdbCursor) Next(u *SWARMDBUser) ([]byte, []byte, error) {
	pos := self.bin.GetLast()
	k := convertToByte(self.node.Bin[pos].Key)
	v := bytes.Trim(convertToByte(self.node.Bin[pos].Value), "\x00")
	var err error
	if len(bytes.Trim(convertToByte(v), "\x00")) == 0 {
		err = self.seeknext(u)
		pos = self.bin.GetLast()
		k = convertToByte(self.node.Bin[pos].Key)
		v = convertToByte(self.node.Bin[pos].Value)
	}

	err = self.seeknext(u)
	if err != nil {
		return k, v, err
	}
	if len(bytes.Trim(convertToByte(self.node.Bin[self.bin.GetLast()].Value), "\x00")) == 0 {
		err = self.seeknext(u)
	}
	return k, v, err
}

func (self *HashdbCursor) Prev(u *SWARMDBUser) ([]byte, []byte, error) {
	pos := self.bin.GetLast()
	k := convertToByte(self.node.Bin[pos].Key)
	v := convertToByte(self.node.Bin[pos].Value)
	err := self.seekprev(u)
	if err != nil {
		return k, v, err
	}
	if len(bytes.Trim(convertToByte(self.node.Bin[self.bin.GetLast()].Value), "\x00")) == 0 {
		err = self.seekprev(u)
	}
	return k, v, err
}

func (self *HashdbCursor) seek(u *SWARMDBUser, k []byte) error {
	return nil
}

func (self *HashdbCursor) seeknext(u *SWARMDBUser) error {
	l := self.level
	if self.node.Loaded == false {
		self.node.load(u, self.hashdb.swarmdb, self.hashdb.columnType)
	}

	lastpos := self.bin.GetLast()
	if lastpos < 0 {
		lastpos = 0
	} else {
		lastpos = lastpos + 1
	}
	for i := lastpos; i < 64; i++ {
		if self.node.Bin[i] != nil && self.node.Bin[i].Value != 0 {
			if self.node.Bin[i].Loaded == false {
				self.node.Bin[i].load(u, self.hashdb.swarmdb, self.hashdb.columnType)
			}
			if lastpos == 0 {
				self.level = l + 1
			}
			if self.node.Bin[i].Next == true {
				self.node = self.node.Bin[i]
				self.bin.Pop()
				self.bin.Push(i)
				self.bin.size = self.bin.size + 1
				if self.seeknext(u) == nil {
					return nil
				}
			} else {
				self.bin.Pop()
				self.bin.Push(i)
				return nil
			}
		}
	}
	if self.level == 0 {
		return io.EOF
	}
	self.level = self.level - 1
	bnum, err := self.bin.Pop()
	bnum, err = self.bin.Pop()
	if err != nil {
		return io.EOF
	}
	if bnum < 63 {
		self.bin.Push(bnum)
	} else {
		if self.bin.Size() == 0 {
			return io.EOF
		}
		bnum, _ := self.bin.Pop()
		self.bin.Push(bnum + 1)
		self.level = self.level - 1
	}
	self.node = self.hashdb.rootnode
	for i := 0; i < self.bin.Size()-1; i++ {
		if self.bin.GetPos(i) == -1 {
			return fmt.Errorf("No Data")
		}
		if self.node.Bin[self.bin.GetPos(i)] == nil {
		} else {
			if self.node.Bin[self.bin.GetPos(i)].Loaded == false {
				self.node.Bin[self.bin.GetPos(i)].load(u, self.hashdb.swarmdb, self.hashdb.columnType)
			}
			self.node = self.node.Bin[self.bin.GetPos(i)]
			//return nil
		}
	}
	err = self.seeknext(u)
	return err
}

func (self *HashdbCursor) seekprev(u *SWARMDBUser) error {
	l := self.level
	if self.node.Loaded == false {
		self.node.load(u, self.hashdb.swarmdb, self.hashdb.columnType)
	}

	lastpos := self.bin.GetLast()
	if lastpos < 0 {
		lastpos = 63
	} else if lastpos == 0 {
		lastpos = 63
	} else {
		lastpos = lastpos - 1
	}
	for i := lastpos; i >= 0; i-- {
		if self.node.Bin[i] != nil && self.node.Bin[i].Value != 0 {
			if self.node.Bin[i].Loaded == false {
				self.node.Bin[i].load(u, self.hashdb.swarmdb, self.hashdb.columnType)
			}
			self.level = l + 1
			if self.node.Bin[i].Next == true {
				self.node = self.node.Bin[i]
				self.bin.Pop()
				self.bin.Push(i)
				self.bin.size = self.bin.size + 1
				if self.seekprev(u) == nil {
					return nil
				}
			} else {
				self.bin.Pop()
				self.bin.Push(i)
				return nil
			}
		}
	}
	self.bin.Pop()
	if self.level == 0 {
		return io.EOF
	}
	self.level = self.level - 1
	bnum, err := self.bin.Pop()
	if err != nil {
		return io.EOF
	}

	if bnum != 0 {
		self.bin.Push(bnum)
	} else {
		if self.bin.Size() == 0 {
			return io.EOF
		}
		bnum, _ := self.bin.Pop()
		self.bin.Push(bnum - 1)
		self.level = self.level - 1
	}
	self.node = self.hashdb.rootnode
	for i := 0; i < self.bin.Size()-1; i++ {
		if self.bin.GetPos(i) == -1 {
			return fmt.Errorf("No Data")
		}
		if self.node.Bin[self.bin.GetPos(i)] == nil {
		} else {
			if self.node.Bin[self.bin.GetPos(i)].Loaded == false {
				self.node.Bin[self.bin.GetPos(i)].load(u, self.hashdb.swarmdb, self.hashdb.columnType)
			}
			self.node = self.node.Bin[self.bin.GetPos(i)]
			//return nil
		}
	}
	err = self.seekprev(u)
	return err
}

func (self *HashdbCursor) seeklast() error {
	return nil
}

type stack_t struct {
	data []int
	size int
}

func newStack() *stack_t {
	s := stack_t{
		data: make([]int, STACK_SIZE),
		size: 0,
	}
	for i := 0; i < STACK_SIZE; i++ {
		s.data[i] = -1
	}
	return &s
}

func (self *stack_t) Push(add int) error {
	if self.size+1 > STACK_SIZE {
		return fmt.Errorf("over max stack")
	}
	self.data[self.size] = add
	self.size = self.size + 1
	return nil
}

func (self *stack_t) Pop() (int, error) {
	if self.size == 0 {
		return -1, fmt.Errorf("nothing in stack")
	}
	pos := self.data[self.size-1]
	self.data[self.size-1] = -1
	self.size = self.size - 1
	return pos, nil
}

func (self *stack_t) GetLast() int {
	if self.size <= 0 {
		return -1
	}
	return self.data[self.size-1]
}

func (self *stack_t) GetFirst() int {
	return self.data[0]
}

func (self *stack_t) GetPos(pos int) int {
	if self.size < pos {
		return -1
	}
	return self.data[pos]
}

func (self *stack_t) Size() int {
	return self.size
}
