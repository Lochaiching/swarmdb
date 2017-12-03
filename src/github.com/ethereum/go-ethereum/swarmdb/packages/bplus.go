package swarmdb

import (
	"bytes"
	"github.com/ethereum/go-ethereum/swarm/api"
	// "github.com/ethereum/go-ethereum/swarmdb/packages"
	"fmt"
	"io"
	"sync"
)

type Database interface {
	// Open: reads in root hashid from ENS
	// Possible Errors: TableNotExistError, NetworkError
	Open(owner string, tableName string, columnName string) (bool, error)

	// Insert: adds key-value pair (value is an entire recrod)
	// ok - returns true if new key added
	// Possible Errors: KeySizeError, ValueSizeError, DuplicateKeyError, NetworkError, BufferOverflowError
	Insert(key []byte, value []byte) (bool, error)

	// Put -- inserts/updates key-value pair (value is an entire record)
	// ok - returns true if new key added
	// Possible Errors: KeySizeError, ValueSizeError, NetworkError, BufferOverflowError
	Put(key []byte, value []byte) (bool, error)

	// Get - gets value of key (value is an entire record)
	// ok - returns true if key found, false if not found
	// Possible errors: KeySizeError, NetworkError
	Get(key []byte) ([]byte, bool, error)

	// Delete - deletes key
	// ok - returns true if key found, false if not found
	// Possible errors: KeySizeError, NetworkError, BufferOverflowError
	Delete(key []byte) (bool, error)

	// Start/Flush - any buffered updates will be flushed to SWARM on FlushBuffer
	// ok - returns true if buffer started / flushed
	// Possible errors: NoBufferError, NetworkError
	StartBuffer() (bool, error)
	FlushBuffer() (bool, error)

	// Close - if buffering, then will flush buffer
	// ok - returns true if operation successful
	// Possible errors: NetworkError
	Close() (bool, error)

	// prints what is in memory
	Print()
}

type OrderedDatabase interface {
	Database

	// Seek -- moves cursor to key k
	// ok - returns true if key found, false if not found
	// Possible errors: KeySizeError, NetworkError
	Seek(k []byte /*K*/) (e OrderedDatabaseCursor, ok bool, err error)
}

type OrderedDatabaseCursor interface {
	Next() (k []byte /*K*/, v []byte /*V*/, err error)
	Prev() (k []byte /*K*/, v []byte /*V*/, err error)
}

type TableNotExistError struct {
}

func (t *TableNotExistError) Error() string {
	return fmt.Sprintf("Table does not exist")
}

type KeyNotFoundError struct {
}

func (t *KeyNotFoundError) Error() string {
	return fmt.Sprintf("Key not found")
}

type KeySizeError struct {
}

func (t *KeySizeError) Error() string {
	return fmt.Sprintf("Key size too large")
}

type ValueSizeError struct {
}

func (t *ValueSizeError) Error() string {
	return fmt.Sprintf("Value size too large")
}

type DuplicateKeyError struct {
}

func (t *DuplicateKeyError) Error() string {
	return fmt.Sprintf("Duplicate key error")
}

type NetworkError struct {
}

func (t *NetworkError) Error() string {
	return fmt.Sprintf("Network error")
}

type NoBufferError struct {
}

func (t *NoBufferError) Error() string {
	return fmt.Sprintf("No buffer error")
}

type BufferOverflowError struct {
}

func (t *BufferOverflowError) Error() string {
	return fmt.Sprintf("Buffer overflow error")
}

const (
	kx        = 3
	kd        = 3
	ENS_DIR   = "ens"
	DATA_DIR = "/tmp/joy"
	CHUNK_SIZE = 4096
	KV_SIZE = 64
	K_SIZE = 32
	V_SIZE = 32
	HASH_SIZE = 32
)


func init() {
	if kd < 1 {
		panic(fmt.Errorf("kd %d: out of range", kd))
	}

	if kx < 2 {
		panic(fmt.Errorf("kx %d: out of range", kx))
	}
}

var (
	btDPool = sync.Pool{New: func() interface{} { return &d{} }}
	btEPool = btEpool{sync.Pool{New: func() interface{} { return &Enumerator{} }}}
	btTPool = btTpool{sync.Pool{New: func() interface{} { return &Tree{} }}}
	btXPool = sync.Pool{New: func() interface{} { return &x{} }}
)

//type Key []byte
//type Val []byte

type btTpool struct{ sync.Pool }

func (p *btTpool) get(cmp Cmp) *Tree {
	x := p.Get().(*Tree)
	x.cmp = cmp
	return x
}

type btEpool struct{ sync.Pool }

func (p *btEpool) get(err error, hit bool, i int, k []byte /*K*/, q *d, t *Tree, ver int64) *Enumerator {
	x := p.Get().(*Enumerator)
	x.err, x.hit, x.i, x.k, x.q, x.t, x.ver = err, hit, i, k, q, t, ver
	return x
}

type (
	// Cmp compares a and b. Return value is:
	//
	//	< 0 if a <  b
	//	  0 if a == b
	//	> 0 if a >  b
	//
	Cmp func(a, b []byte /*K*/) int

	d struct { // data page
		c int
		d [2*kd + 1]de
		n *d
		p *d

		// used in open, insert, delete
		hashid    []byte
		dirty     bool
		notloaded bool

		// used for linked list traversal
		prevhashid []byte
		nexthashid []byte
	}

	de struct { // d element
		k []byte // interface{} /*K*/
		v []byte // interface{} /*V*/
	}

	// Enumerator captures the state of enumerating a tree. It is returned
	// from the Seek* methods. The enumerator is aware of any mutations
	// made to the tree in the process of enumerating it and automatically
	// resumes the enumeration at the proper key, if possible.
	//
	// However, once an Enumerator returns io.EOF to signal "no more
	// items", it does no more attempt to "resync" on tree mutation(s).  In
	// other words, io.EOF from an Enumerator is "sticky" (idempotent).
	Enumerator struct {
		err error
		hit bool
		i   int
		k   []byte /*K*/
		q   *d
		t   *Tree
		ver int64
	}

	// Tree is a B+tree.
	Tree struct {
		c     int
		cmp   Cmp
		first *d
		last  *d
		r     interface{}
		ver   int64

		api   *api.Api
		buffered   bool
		owner      []byte
		tableName  []byte
		columnName []byte
		hashid     []byte
	}

	xe struct { // x element
		ch interface{}
		k  []byte // interface{} /*K*/
	}

	x struct { // index page
		c int
		x [2*kx + 2]xe

		// used in open, insert, delete
		hashid    []byte
		dirty     bool
		notloaded bool
	}
)

var ( // R/O zero values
	zd  d
	zde de
	ze  Enumerator
	zk  []byte // interface{} /*K*/
	zt  Tree
	zx  x
	zxe xe
)

func clr(q interface{}) {
	switch x := q.(type) {
	case *x:
		for i := 0; i <= x.c; i++ { // Ch0 Sep0 ... Chn-1 Sepn-1 Chn
			clr(x.x[i].ch)
		}
		*x = zx
		btXPool.Put(x)
	case *d:
		*x = zd
		btDPool.Put(x)
	}
}

// -------------------------------------------------------------------------- x

func newX(ch0 interface{}) *x {
	r := btXPool.Get().(*x)
	r.x[0].ch = ch0
	return r
}

func (q *x) extract(i int) {
	q.c--
	if i < q.c {
		copy(q.x[i:], q.x[i+1:q.c+1])
		q.x[q.c].ch = q.x[q.c+1].ch
		q.x[q.c].k = zk  // GC
		q.x[q.c+1] = zxe // GC
	}
}

func (q *x) insert(i int, k []byte /*K*/, ch interface{}) *x {
	c := q.c
	if i < c {
		q.x[c+1].ch = q.x[c].ch
		copy(q.x[i+2:], q.x[i+1:c])
		q.x[i+1].k = q.x[i].k
	}
	c++
	q.c = c
	q.x[i].k = k
	q.x[i+1].ch = ch
	q.dirty = true
	return q
}

func (q *x) siblings(i int) (l, r *d) {
	if i >= 0 {
		if i > 0 {
			l = q.x[i-1].ch.(*d)
		}
		if i < q.c {
			r = q.x[i+1].ch.(*d)
		}
	}
	return
}

// -------------------------------------------------------------------------- d

func (l *d) mvL(r *d, c int) {
	copy(l.d[l.c:], r.d[:c])
	copy(r.d[:], r.d[c:r.c])
	l.c += c
	r.c -= c
}

func (l *d) mvR(r *d, c int) {
	copy(r.d[c:], r.d[:r.c])
	copy(r.d[:c], l.d[l.c-c:])
	r.c += c
	l.c -= c
}

// ----------------------------------------------------------------------- Tree

// BPlusTree returns a newly created, empty Tree. The compare function is used
// for key collation.

func NewBPlusTreeDB(api *api.Api) *Tree {
	t := btTPool.get(cmp)
	t.api = api
	return t
}

func (t *Tree) Open(owner []byte, tableName []byte, columnName []byte) (ok bool, err error) {
	t.owner = owner
	t.tableName = tableName
	t.columnName = columnName
	// t.kaddb = NewKadDB(owner, tableName, columnName)

	hashid, err := t.api.GetIndexRootHash(t.tableName)
	fmt.Printf("Index:%s => %x\n", tableName, hashid)
	if err != nil {
		fmt.Printf("ERR %v\n", err)
		return ok, err
	} else {
		t.hashid = hashid
		t.SWARMGet()
		return true, nil
	}

}

func (t *Tree) StartBuffer() (ok bool, err error) {
	t.buffered = true
	return true, nil
}

func (t *Tree) FlushBuffer() (ok bool, err error) {
	if t.buffered {
		t.buffered = false
		new_hashid, changed := t.SWARMPut() 
		if ( changed ) {
			t.api.StoreIndexRootHash(t.tableName, []byte(new_hashid))
			return true, nil
		}
	} else {
		var nberr *NoBufferError
		return false, nberr
	}
	return true, nil
}

// Close performs Clear and recycles t to a pool for possible later reuse. No
// references to t should exist or such references must not be used afterwards.
func (t *Tree) Close() (ok bool, err error) {
	if t.buffered {
		t.FlushBuffer()
	}
	t.Clear()
	*t = zt
	btTPool.Put(t)
	return true, nil
}

func get_chunk_nodetype(buf []byte) (nodetype string) {
	return string(buf[CHUNK_SIZE-65:CHUNK_SIZE-64]) 
}

func set_chunk_nodetype(buf []byte, nodetype string) {
	copy(buf[CHUNK_SIZE-65:], []byte(nodetype))
}

func (t *Tree) SWARMGet() (success bool) {
	// do a read from local file system, filling in: (a) hashid and (b) items
	buf, err := t.api.RetrieveDBChunk(t.hashid)
	if err != nil {
		fmt.Printf("SWARMGet FAIL (T): [%s]\n", err)
		// return false
	}
	fmt.Printf("GOT %x [%v]\n", t.hashid, buf)
	nodetype := get_chunk_nodetype(buf)
	if nodetype == "X" {
		// create X node
		t.r = btXPool.Get().(*x)
		switch z := t.r.(type) {
		case (*x):
			for  i := 0 ; i < 32; i++ {
				k := buf[i*KV_SIZE:i*KV_SIZE+K_SIZE]
				hashid := buf[i*KV_SIZE+K_SIZE:i*KV_SIZE+KV_SIZE]
				z.c++
				x := btXPool.Get().(*x)
				z.x[i].ch = x
				if err != nil {
					z.x[i].k = zk // "ZZZZZ"
				} else {
					z.x[i].k = k
				}
				x.notloaded = true
				x.hashid = hashid
				fmt.Printf(" LOAD-X|%d|%s\n", i, x.hashid)
			}
		}
	} else { 
		// create D node
		t.r = btDPool.Get().(*d)
		switch z := t.r.(type) {
		case (*d):
			for  i := 0 ; i < 32; i++ {
				k := buf[i*KV_SIZE:i*KV_SIZE+32]
				hashid := buf[i*KV_SIZE+32:i*64+64]
				if valid_hashid(hashid) && i < 2*kd  {
					z.c++
					x := btDPool.Get().(*d)
					z.d[i].k = k
					z.d[i].v = hashid
					x.notloaded = true
					x.hashid = hashid
					fmt.Printf(" LOAD-D %d|k:%s|i:%x\n", i, string(k), x.hashid)
				}
			}
		}
	}

	switch z := t.r.(type) {
	case (*x):
		z.c--
	}
	fmt.Printf("SWARMGet T: [%x]\n", t.hashid)
	return true
}

func valid_hashid(hashid []byte) (valid bool) {
	valid = true
	for i := 0; i < len(hashid); i++ {
		if ( hashid[i] != 0 )  {
			return false
		}
	}
	return valid
}

func (q *x) SWARMGet(api *api.Api) (changed bool) {
	// X|0|29022ceec0d104f84d40b6cd0b0aa52fcf676b52e4f5660e9c070e09cc8c693b|437
	// do a read from local file system, filling in: (a) hashid and (b) items
	buf, err := api.RetrieveDBChunk(q.hashid)
	if err != nil {
		fmt.Printf("SWARMGet FAIL X: [%s]\n", err)
		// return false
	}
	for  i := 0 ; i < 32; i++ {
		k := buf[i*64:i*64+31]
		node_type := buf[i*64+31:i*64+32]
		hashid := buf[i*64+32:i*64+63]
		if string(node_type) == "X" {
			// X|0|29022ceec0d104f84d40b6cd0b0aa52fcf676b52e4f5660e9c070e09cc8c693b|437
			x := btXPool.Get().(*x)
			q.x[i].ch = x
			q.x[i].k = k
			
			x.notloaded = true
			x.hashid = hashid
			fmt.Printf(" LOAD-X|%d|%s\n", i, x.hashid)
		} else if string(node_type) == "D" {
			q.c++
			// D|0|29022ceec0d104f84d40b6cd0b0aa52fcf676b52e4f5660e9c070e09cc8c693b|437
			x := btDPool.Get().(*d)
			q.x[i].ch = x
			q.x[i].k = []byte(k)
			x.notloaded = true
			x.hashid = hashid
			fmt.Printf(" LOAD-D1|%d|%s\n", i, x.hashid)
		}
	}
	q.notloaded = false
	fmt.Printf("SWARMGet X: [%x]\n", q.hashid)
	return true
}

func (q *d) SWARMGet(api *api.Api) (changed bool) {
	// do a read from local file system, filling in: (a) hashid and (b) items
	buf, err := api.RetrieveDBChunk(q.hashid)
	fmt.Printf("Retrieve: %v => %v\n", buf,  err)
	if err != nil {
		fmt.Printf("SWARMGet FAIL (D): [%s]\n", err)
		// return false
	}
	for  i := 0 ; i < 32; i++ {
		k := buf[i*64:i*64+31]
		node_type := buf[i*64+31:i*64+32]
		hashid := buf[i*64+32:i*64+63]
		if string(node_type) == "C" || true {
			q.c++
			q.d[i].k = k
			q.d[i].v = hashid
			q.notloaded = false
		}
	}
	q.prevhashid = buf[CHUNK_SIZE-64:CHUNK_SIZE-32]
	q.nexthashid = buf[CHUNK_SIZE-32:CHUNK_SIZE]
	q.notloaded = false
	fmt.Printf("SWARMGet D: [%x]\n", q.hashid)
	return true
}

func (t *Tree) SWARMPut() (new_hashid []byte, changed bool) {
	q := t.r
	if q == nil {
		return
	}

	switch x := q.(type) {
	case *x: // intermediate node -- descend on the next pass
		fmt.Printf("ROOT XNode %s [dirty=%v|notloaded=%v]\n", x.hashid, x.dirty, x.notloaded)
		new_hashid, changed = x.SWARMPut(t.api)
		if changed {
			t.hashid = x.hashid
		}
	case *d: // data node -- EXACT match
		fmt.Printf("ROOT DNode %s [dirty=%v|notloaded=%v]\n", x.hashid, x.dirty, x.notloaded)
		new_hashid, changed = x.SWARMPut(t.api)
		if changed {
			t.hashid = x.hashid
		}
	}

	return new_hashid, changed
}

func (q *x) SWARMPut(api *api.Api) (new_hashid []byte, changed bool) {
	// recurse through children
	fmt.Printf("put XNode [c=%d] %s  [dirty=%v|notloaded=%v]\n", q.c, q.hashid, q.dirty, q.notloaded)
	for i := 0; i <= q.c; i++ {
		switch z := q.x[i].ch.(type) {
		case *x:
			if z.dirty {
				z.SWARMPut(api)
			}
		case *d:
			if z.dirty {
				z.SWARMPut(api)
			}
		}
	}

	// compute the data here 
	sdata := make([]byte, CHUNK_SIZE)
	for i := 0; i <= q.c; i++ {
		switch z := q.x[i].ch.(type) {
		case *x:
			copy(sdata[i*64:], q.x[i].k)     // max 32 bytes 
			copy(sdata[i*64+32:], z.hashid)  // max 32 bytes
		case *d:
			copy(sdata[i*64:], q.x[i].k)     // max 32 bytes 
			copy(sdata[i*64+32:], z.hashid)  // max 32 bytes
		}
	}

	new_hashid, err := api.StoreDBChunk(sdata)
	if err != nil {
		return q.hashid, false
	}
	q.hashid = new_hashid
	return new_hashid, true
}

func (q *d) SWARMPut(api *api.Api) (new_hashid []byte, changed bool) {
	fmt.Printf("put DNode [c=%d] [dirty=%v|notloaded=%v, prev=%s, next=%s]\n", q.c, q.dirty, q.notloaded, q.prevhashid, q.nexthashid)
	if q.n != nil {
		if q.n.dirty {
			q.n.SWARMPut(api)
		}
		q.nexthashid = q.n.hashid
		fmt.Printf(" -- NEXT: %s [%v]\n", q.nexthashid, q.n.dirty)
	}
	q.dirty = false

	if q.p != nil {
		if q.p.dirty {
			q.p.SWARMPut(api)
		}
		q.prevhashid = q.p.hashid
		fmt.Printf(" -- PREV: %s [%v]\n", q.prevhashid, q.p.dirty)
	}

	fmt.Printf("N: %v P: %v\n", q.n, q.p) //  q.prevhashid, q.nexthashid

	sdata := make([]byte, CHUNK_SIZE)
	for i := 0; i < q.c; i++ {
		copy(sdata[i*KV_SIZE:], q.d[i].k)     // max 32 bytes 
		copy(sdata[i*KV_SIZE+K_SIZE:], q.d[i].v)  // max 32 bytes
	}

	if q.p != nil {
		copy(sdata[CHUNK_SIZE-HASH_SIZE*2:], q.prevhashid) // 32 bytes
	}
	if q.n != nil {
		copy(sdata[CHUNK_SIZE-HASH_SIZE*2:], q.nexthashid)  // 32 bytes
	}

	new_hashid, err := api.StoreDBChunk(sdata) 

	if err != nil {
		return q.hashid, false
	}
	q.hashid = new_hashid
	return new_hashid, true
}

// Clear removes all K/V pairs from the tree.
func (t *Tree) Clear() {
	if t.r == nil {
		return
	}

	clr(t.r)
	t.c, t.first, t.last, t.r = 0, nil, nil, nil
	t.ver++
}

func (t *Tree) cat(p *x, q, r *d, pi int) {
	t.ver++
	q.mvL(r, r.c)
	if r.n != nil {
		r.n.p = q
	} else {
		t.last = q
	}
	q.n = r.n
	r.dirty = true
	q.dirty = true
	*r = zd
	btDPool.Put(r)
	if p.c > 1 {
		p.extract(pi)
		p.x[pi].ch = q
		return
	}

	switch x := t.r.(type) {
	case *x:
		*x = zx
		btXPool.Put(x)
	case *d:
		*x = zd
		btDPool.Put(x)
	}
	t.r = q
}

func (t *Tree) catX(p, q, r *x, pi int) {
	t.ver++
	q.x[q.c].k = p.x[pi].k
	copy(q.x[q.c+1:], r.x[:r.c])
	q.c += r.c + 1
	q.x[q.c].ch = r.x[r.c].ch
	*r = zx
	btXPool.Put(r)
	if p.c > 1 {
		p.c--
		pc := p.c
		if pi < pc {
			p.x[pi].k = p.x[pi+1].k
			copy(p.x[pi+1:], p.x[pi+2:pc+1])
			p.x[pc].ch = p.x[pc+1].ch
			p.x[pc].k = zk     // GC
			p.x[pc+1].ch = nil // GC
		}
		return
	}

	switch x := t.r.(type) {
	case *x:
		*x = zx
		btXPool.Put(x)
	case *d:
		*x = zd
		btDPool.Put(x)
	}
	t.r = q
}

// Delete removes the k's KV pair, if it exists, in which case Delete returns true.
func (t *Tree) Delete(k []byte /*K*/) (ok bool, err error) {
	pi := -1
	var p *x
	q := t.r
	if q == nil {
		var kerr *KeyNotFoundError
		return false, kerr
	}
	for {
		checkload(t.api, q)
		var i int
		i, ok = t.find(q, k)
		if ok {
			switch x := q.(type) {
			case *x:
				if x.c < kx && q != t.r {
					x, i = t.underflowX(p, x, pi, i)
				}
				pi = i + 1
				p = x
				q = x.x[pi].ch
				x.dirty = true // optimization: this should really be if something is *actually* deleted
				continue
			case *d:
				t.extract(x, i)
				if x.c >= kd {
					return true, nil
				}

				if q != t.r {
					t.underflow(p, x, pi)
				} else if t.c == 0 {
					t.Clear()
				}
				x.dirty = true // we found the key and  actually deleted it!
				return true, nil
			}
		}

		switch x := q.(type) {
		case *x:
			if x.c < kx && q != t.r {
				x, i = t.underflowX(p, x, pi, i)
			}
			pi = i
			p = x
			q = x.x[i].ch
			x.dirty = true // optimization: this should really be if something is *actually* deleted
		case *d:
			var kerr *KeyNotFoundError
			return false, kerr // we got to the bottom and key was not found
		}
	}
}

func (t *Tree) extract(q *d, i int) { // (r interface{} /*V*/) {
	t.ver++
	//r = q.d[i].v // prepared for Extract
	q.c--
	if i < q.c {
		copy(q.d[i:], q.d[i+1:q.c+1])
	}
	q.d[q.c] = zde // GC
	t.c--
	return
}

func (t *Tree) find(q interface{}, k []byte /*K*/) (i int, ok bool) {
	var mk []byte /*K*/
	l := 0
	switch x := q.(type) {
	case *x:
		h := x.c - 1
		for l <= h {
			m := (l + h) >> 1
			mk = x.x[m].k
			switch cmp := t.cmp(k, mk); {
			case cmp > 0:
				l = m + 1
			case cmp == 0:
				return m, true
			default:
				h = m - 1
			}
		}
	case *d:
		h := x.c - 1
		for l <= h {
			m := (l + h) >> 1
			mk = x.d[m].k
			switch cmp := t.cmp(k, mk); {
			case cmp > 0:
				l = m + 1
			case cmp == 0:
				return m, true
			default:
				h = m - 1
			}
		}
	}
	return l, false
}

func checkload(api *api.Api, q interface{}) {
	switch x := q.(type) {
	case *x: // intermediate node -- descend on the next pass
		if x.notloaded {
			x.SWARMGet(api)
		}
	case *d: // data node -- EXACT match
		if x.notloaded {
			x.SWARMGet(api)
		}
	}
}

// Get returns the value associated with k and true if it exists. Otherwise Get
// returns (zero-value, false).
func (t *Tree) Get(k []byte /*K*/) (v []byte /*V*/, ok bool, err error) {
	q := t.r
	if q == nil {
		return
	}

	for {
		checkload(t.api, q)

		var i int
		// binary search on the node => i
		if i, ok = t.find(q, k); ok {
			// found it
			switch x := q.(type) {
			case *x: // intermediate node -- descend on the next pass
				q = x.x[i+1].ch
				continue
			case *d: // data node -- EXACT match
				// kaddb.Get(t.dpa, x.d[i].v)
				return x.d[i].v, ok, nil
			}
		}
		// descend down the tree using the binary search
		switch x := q.(type) {
		case *x:
			q = x.x[i].ch
		default:
			return zk, ok, nil
		}
	}
}

func (t *Tree) insert(q *d, i int, k []byte /*K*/, v []byte /*V*/) *d {
	t.ver++
	c := q.c
	if i < c {
		copy(q.d[i+1:], q.d[i:c])
	}
	c++
	q.c = c
	q.d[i].k = k
	q.d[i].v = v
	t.c++
	q.dirty = true
	return q
}

func print_spaces(nspaces int) {
	for i := 0; i < nspaces; i++ {
		fmt.Printf("  ")
	}
}

// NOTE: this only prints the portion of the tree that is actually LOADED
func (t *Tree) Print() {
	q := t.r
	if q == nil {
		return
	}

	switch x := q.(type) {
	case *x: // intermediate node -- descend on the next pass
		fmt.Printf("ROOT Node %s [dirty=%v|notloaded=%v]\n%s", x.hashid, x.dirty, x.notloaded)
		x.print(0)
	case *d: // data node -- EXACT match
		fmt.Printf("ROOT Node %s [dirty=%v|notloaded=%v]\n", x.hashid, x.dirty, x.notloaded)
		x.print(0)
	}
	return
}

func (q *x) print(level int) {
	print_spaces(level)
	fmt.Printf("XNode [c=%d] %s (LEVEL %d) [dirty=%v|notloaded=%v]\n", q.c, q.hashid, level, q.dirty, q.notloaded)
	for i := 0; i <= q.c; i++ {
		print_spaces(level + 1)
		fmt.Printf("Child %d|%v KEY = %v\n", i, level+1, string(q.x[i].k))
		switch z := q.x[i].ch.(type) {
		case *x:
			z.print(level + 1)
		case *d:
			z.print(level + 1)
		}
	}
	return
}

func (q *d) print(level int) {
	print_spaces(level)
	fmt.Printf("DNode [c=%s] (LEVEL %d) [dirty=%v|notloaded=%v|prev=%s|next=%s]\n", q.c, level, q.dirty, q.notloaded, q.prevhashid, q.nexthashid)
	for i := 0; i < q.c; i++ {
		print_spaces(level + 1)
		fmt.Printf("DATA %d (L%d)|%v|%v\n", i, level+1, string(q.d[i].k), string(q.d[i].v))
	}
	return
}

func (t *Tree) overflow(p *x, q *d, pi, i int, k []byte /*K*/, v []byte /*V*/) {
	t.ver++
	l, r := p.siblings(pi)

	if l != nil && l.c < 2*kd && i != 0 {
		l.mvL(q, 1)
		t.insert(q, i-1, k, v)
		p.x[pi-1].k = q.d[0].k
		return
	}

	if r != nil && r.c < 2*kd {
		if i < 2*kd {
			q.mvR(r, 1)
			t.insert(q, i, k, v)
			p.x[pi].k = r.d[0].k
			return
		}

		t.insert(r, 0, k, v)
		p.x[pi].k = k
		return
	}

	t.split(p, q, pi, i, k, v)
}

// Seek returns an Enumerator positioned on an item such that k >= item's key.
// ok reports if k == item.key The Enumerator's position is possibly after the
// last item in the tree.
func (t *Tree) Seek(k []byte /*K*/) (e OrderedDatabaseCursor, ok bool, err error) {
	q := t.r
	if q == nil {
		e = btEPool.get(nil, false, 0, k, nil, t, t.ver)
		return
	}

	for {
		checkload(t.api, q)
		var i int
		if i, ok = t.find(q, k); ok {
			switch x := q.(type) {
			case *x:
				q = x.x[i+1].ch
				continue
			case *d: // err, hit, i, k, q, t, ver
				return btEPool.get(nil, ok, i, k, x, t, t.ver), true, nil
			}
		}

		switch x := q.(type) {
		case *x:
			q = x.x[i].ch
		case *d:
			return btEPool.get(nil, ok, i, k, x, t, t.ver), false, nil
		}
	}
}

func (t *Tree) Put(k []byte /*K*/, v []byte /*V*/) (okresult bool, err error) {
	//dbg("--- PRE Set(%v, %v)\n%s", k, v, t.dump())
	//defer func() {
	//	dbg("--- POST\n%s\n====\n", t.dump())
	//}()

	pi := -1
	var p *x
	q := t.r
	if q == nil {
		// returns a "d" element which is a linked list (c = int, d array of data elements, n (next), p (prev)
		z := t.insert(btDPool.Get().(*d), 0, k, v)
		t.r, t.first, t.last = z, z, z
		return
	}

	// go down each level, from the "x" intermediate nodes to the "d" data nodes
	for {
		checkload(t.api, q)
		i, ok := t.find(q, k)
		if ok {
			// the key is found
			switch x := q.(type) {
			case *x:
				// for the intermediate level
				i++
				if x.c > 2*kx {
					x, i = t.splitX(p, x, pi, i)
				}
				pi = i
				p = x
				q = x.x[i].ch
				continue
			case *d:
				// kaddb.Put(t.dpa, t.owner, t.tableName, k, v)
				x.d[i].v = v
				x.dirty = true // we updated the value but did not insert anything
			}
			return
		}

		switch x := q.(type) {
		case *x:
			if x.c > 2*kx {
				x, i = t.splitX(p, x, pi, i)
			}
			pi = i
			p = x
			q = x.x[i].ch
			x.dirty = true // we updated the value at the intermediate node
		case *d:
			switch {
			case x.c < 2*kd: // insert
				t.insert(x, i, k, v)
			default:
				t.overflow(p, x, pi, i, k, v)
			}
			x.dirty = true // we inserted the value at the intermediate node or leaf node
			return
		}
	}
}

func (t *Tree) Insert(k []byte /*K*/, v []byte /*V*/) (okres bool, err error) {
	pi := -1
	var p *x
	q := t.r
	if q == nil {
		// returns a "d" element which is a linked list (c = int, d array of data elements, n (next), p (prev)
		z := t.insert(btDPool.Get().(*d), 0, k, v)
		t.r, t.first, t.last = z, z, z
		return
	}

	// go down each level, from the "x" intermediate nodes to the "d" data nodes
	for {
		checkload(t.api, q)
		i, ok := t.find(q, k)
		if ok {
			var dkerr *DuplicateKeyError
			return false, dkerr
		}

		switch x := q.(type) {
		case *x:
			if x.c > 2*kx {
				x, i = t.splitX(p, x, pi, i)
			}
			pi = i
			p = x
			q = x.x[i].ch
			x.dirty = true // we updated the value at the intermediate node
		case *d:
			switch {
			case x.c < 2*kd: // insert
				t.insert(x, i, k, v)
			default:
				t.overflow(p, x, pi, i, k, v)
			}
			x.dirty = true // we inserted the value at the intermediate node or leaf node
			return true, nil
		}
	}
}

func (t *Tree) split(p *x, q *d, pi, i int, k []byte /*K*/, v []byte /*V*/) {
	// fmt.Printf("SPLIT!\n")
	t.ver++
	r := btDPool.Get().(*d)
	if q.n != nil {
		// insert new node into linked list
		r.n = q.n // new node "next" points to old node "next"
		r.n.p = r // new node "prev" points to old node
	} else {
		// its the last node of the linked list!
		t.last = r
	}
	q.n = r // old node "next" points to new node
	r.p = q // new node "prev" points to prev node
	r.dirty = true
	q.dirty = true

	copy(r.d[:], q.d[kd:2*kd])
	for i := range q.d[kd:] {
		q.d[kd+i] = zde
	}
	q.c = kd
	r.c = kd
	var done bool
	if i > kd {
		done = true
		t.insert(r, i-kd, k, v)
	}
	if pi >= 0 {
		p.insert(pi, r.d[0].k, r)
	} else {
		t.r = newX(q).insert(0, r.d[0].k, r)
	}
	if done {
		return
	}

	t.insert(q, i, k, v)
}

func (t *Tree) splitX(p *x, q *x, pi int, i int) (*x, int) {
	t.ver++
	r := btXPool.Get().(*x)
	copy(r.x[:], q.x[kx+1:])
	q.c = kx
	r.c = kx
	r.dirty = true
	if pi >= 0 {
		p.insert(pi, q.x[kx].k, r)
	} else {
		t.r = newX(q).insert(0, q.x[kx].k, r)
	}

	q.x[kx].k = zk
	for i := range q.x[kx+1:] {
		q.x[kx+i+1] = zxe
	}
	if i > kx {
		q = r
		i -= kx + 1
	}

	return q, i
}

func (t *Tree) underflow(p *x, q *d, pi int) {
	t.ver++
	l, r := p.siblings(pi)

	if l != nil && l.c+q.c >= 2*kd {
		l.mvR(q, 1)
		p.x[pi-1].k = q.d[0].k
		return
	}

	if r != nil && q.c+r.c >= 2*kd {
		q.mvL(r, 1)
		p.x[pi].k = r.d[0].k
		r.d[r.c] = zde // GC
		return
	}

	if l != nil {
		t.cat(p, l, q, pi-1)
		return
	}

	t.cat(p, q, r, pi)
}

func (t *Tree) underflowX(p *x, q *x, pi int, i int) (*x, int) {
	t.ver++
	var l, r *x

	if pi >= 0 {
		if pi > 0 {
			l = p.x[pi-1].ch.(*x)
		}
		if pi < p.c {
			r = p.x[pi+1].ch.(*x)
		}
	}

	if l != nil && l.c > kx {
		q.x[q.c+1].ch = q.x[q.c].ch
		copy(q.x[1:], q.x[:q.c])
		q.x[0].ch = l.x[l.c].ch
		q.x[0].k = p.x[pi-1].k
		q.c++
		i++
		l.c--
		p.x[pi-1].k = l.x[l.c].k
		return q, i
	}

	if r != nil && r.c > kx {
		q.x[q.c].k = p.x[pi].k
		q.c++
		q.x[q.c].ch = r.x[0].ch
		p.x[pi].k = r.x[0].k
		copy(r.x[:], r.x[1:r.c])
		r.c--
		rc := r.c
		r.x[rc].ch = r.x[rc+1].ch
		r.x[rc].k = zk
		r.x[rc+1].ch = nil
		return q, i
	}

	if l != nil {
		i += l.c + 1
		t.catX(p, l, q, pi-1)
		q = l
		return q, i
	}

	t.catX(p, q, r, pi)
	return q, i
}

// ----------------------------------------------------------------- Enumerator

// Close recycles e to a pool for possible later reuse. No references to e
// should exist or such references must not be used afterwards.
func (e *Enumerator) Close() {
	*e = ze
	btEPool.Put(e)
}

// Next returns the currently enumerated item, if it exists and moves to the
// next item in the key collation order. If there is no item to return, err ==
// io.EOF is returned.
func (e *Enumerator) Next() (k []byte /*K*/, v []byte /*V*/, err error) {
	if err = e.err; err != nil {
		fmt.Printf("1 err %v\n", err)
		return
	}

	/*
		if e.ver != e.t.ver {
			fmt.Printf("2 new seek\n")
			f, _, _ := e.t.Seek(e.k)
			*e = *f
			f.Close()
		}
	*/
	if e.q == nil {
		e.err, err = io.EOF, io.EOF
		fmt.Printf("3 eof\n")
		return
	}

	if e.i >= e.q.c {
		if err = e.next(); err != nil {
			fmt.Printf("4 err %v\n", err)
			return
		}
	}

	i := e.q.d[e.i]
	k, v = i.k, i.v
	e.k, e.hit = k, true
	e.next()
	return
}

func (e *Enumerator) next() error {
	if e.q == nil {
		e.err = io.EOF
		fmt.Printf("5 EOF\n")
		return io.EOF
	}

	switch {
	case e.i < e.q.c-1:
		e.i++
	default:
		if len(e.q.nexthashid) > 0 && e.q.n == nil {
			// fmt.Printf(" LOAD %v\n", e.q.nexthashid);
			r := btDPool.Get().(*d)
			r.p = e.q
			r.hashid = e.q.nexthashid
			r.SWARMGet(e.t.api)
			e.q = r
			e.i = 0
		} else {
			if e.q, e.i = e.q.n, 0; e.q == nil {
				// fmt.Printf("6 EOF\n")
				e.err = io.EOF
			}
		}
	}

	return e.err
}

// Prev returns the currently enumerated item, if it exists and moves to the
// previous item in the key collation order. If there is no item to return, err
// == io.EOF is returned.
func (e *Enumerator) Prev() (k []byte /*K*/, v []byte /*V*/, err error) {
	if err = e.err; err != nil {
		return
	}
	/*
		if e.ver != e.t.ver {
			f, _, _ := e.t.Seek(e.k)
			*e = *f
			f.Close()
		}
	*/
	if e.q == nil {
		e.err, err = io.EOF, io.EOF
		return
	}

	if !e.hit {
		// move to previous because Seek overshoots if there's no hit
		if err = e.prev(); err != nil {
			return
		}
	}

	if e.i >= e.q.c {
		if err = e.prev(); err != nil {
			return
		}
	}

	i := e.q.d[e.i]
	k, v = i.k, i.v
	e.k, e.hit = k, true
	e.prev()
	return
}

func (e *Enumerator) prev() error {
	if e.q == nil {
		e.err = io.EOF
		return io.EOF
	}

	switch {
	case e.i > 0:
		e.i--
	default:
		if e.q = e.q.p; e.q == nil {
			e.err = io.EOF
			break
		}

		e.i = e.q.c - 1
	}
	return e.err
}

func cmp(a, b []byte) int {
	// Compare returns an integer comparing two byte slices lexicographically.
	// The result will be 0 if a==b, -1 if a < b, and +1 if a > b. A nil argument is equivalent to an empty slice.
	return bytes.Compare(a, b)
}
