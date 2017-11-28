package main

import (
	"bufio"
	"bytes"
	"crypto/sha256"
	"fmt"
	"io"
	"math/rand"
	"os"
	"strconv"
	"strings"
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
	CHUNK_DIR = "chunk"
)

// from table's ENS
func putTableENS(tableName string, hashid string) (succ bool, err error) {
	path := fmt.Sprintf("%s/%s", ENS_DIR, tableName)
	// do a write to local file system with all the items and the children
	f, err := os.Create(path)
	if err != nil {
		panic(err)
	}
	defer f.Close()

	fmt.Printf(" Wrote ENS: [%s => %s]\n", tableName, hashid)
	f.WriteString(hashid)
	f.Sync()
	return true, nil
}

// from table's ENS
func getTableENS(tableName string) (hashid string, err error) {
	var terr *TableNotExistError
	path := fmt.Sprintf("%s/%s", ENS_DIR, tableName)
	file, err := os.Open(path)
	if err != nil {
		fmt.Print("getTableENS FAIL: [%s]\n", path)
		return hashid, terr
	}
	defer file.Close()

	var line string
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		line = scanner.Text()
		if len(line) > 32 {
			return line, nil
		}
	}
	return hashid, terr
}

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

type Key []byte
type Val []byte

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
		hashid    string
		dirty     bool
		notloaded bool

		// used for linked list traversal
		prevhashid string
		nexthashid string
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

		buffered   bool
		owner      string
		tableName  string
		columnName string
		hashid     string // computed at the end
	}

	xe struct { // x element
		ch interface{}
		k  []byte // interface{} /*K*/
	}

	x struct { // index page
		c int
		x [2*kx + 2]xe

		// used in open, insert, delete
		hashid    string
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

func BPlusTree() *Tree {
	t := btTPool.get(cmp)
	return t
}

func (t *Tree) Open(owner string, tableName string, columnName string) (ok bool, err error) {
	t.owner = owner
	t.tableName = tableName
	t.columnName = columnName
	hashid, err := getTableENS(t.tableName)
	if err != nil {
		return ok, err
	} else {
		t.hashid = hashid
		fmt.Printf("Open %s => hashid %s\n", tableName, hashid)
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
		if t.SWARMPut() {
			putTableENS(t.tableName, t.hashid)

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

func (t *Tree) SWARMGet() (success bool) {
	// do a read from local file system, filling in: (a) hashid and (b) items
	path := fmt.Sprintf("%s/%s", CHUNK_DIR, t.hashid)
	file, err := os.Open(path)
	if err != nil {
		fmt.Printf("SWARMGet FAIL: [%s]\n", path)
		return false
	}
	defer file.Close()
	var line string
	scanner := bufio.NewScanner(file)
	created := false
	for scanner.Scan() {
		line = scanner.Text()
		sa := strings.Split(line, "|")
		if sa[0] == "X" {
			if created {
			} else {
				// create X node
				t.r = btXPool.Get().(*x)
				created = true
			}
			i, _ := strconv.Atoi(sa[1])
			hashid := sa[2]
			k := []byte(sa[3])
			// X|0|29022ceec0d104f84d40b6cd0b0aa52fcf676b52e4f5660e9c070e09cc8c693b|437
			switch z := t.r.(type) {
			case (*x):
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
		} else if sa[0] == "D" {
			if created {
			} else {
				// create D node
				t.r = btXPool.Get().(*x)
				created = true
			}
			i, _ := strconv.Atoi(sa[1])
			hashid := sa[2]
			k := []byte(sa[3])
			v := []byte(sa[3])
			// D|0|50379fa9432e99a614f63433ae3ac731a389bd520ad39104509c218703a95b86|6
			switch z := t.r.(type) {
			case (*x):
				z.c++
				x := btDPool.Get().(*d)
				z.x[i].ch = x
				z.x[i].k = k
				x.d[i].k = k
				x.d[i].v = v

				x.notloaded = true
				x.hashid = hashid
				fmt.Printf(" LOAD-D|%d|%s\n", i, x.hashid)
			}
		}
	}
	switch z := t.r.(type) {
	case (*x):
		z.c--
	}
	fmt.Printf("SWARMGet SUCC: [%s]\n", t.hashid)
	return true
}

func (q *x) SWARMGet() (changed bool) {
	// do a read from local file system, filling in: (a) hashid and (b) items
	path := fmt.Sprintf("%s/%s", CHUNK_DIR, q.hashid)
	file, err := os.Open(path)
	if err != nil {
		fmt.Printf("SWARMGet FAIL: [%s]\n", path)
		return false
	}
	defer file.Close()
	var line string
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		line = scanner.Text()
		sa := strings.Split(line, "|")
		if sa[0] == "X" {
			// q.c++
			i, _ := strconv.Atoi(sa[1])
			hashid := sa[2]
			k := []byte(sa[3])
			// X|0|29022ceec0d104f84d40b6cd0b0aa52fcf676b52e4f5660e9c070e09cc8c693b|437
			x := btXPool.Get().(*x)
			q.x[i].ch = x
			q.x[i].k = k

			x.notloaded = true
			x.hashid = hashid
			fmt.Printf(" LOAD-X|%d|%s\n", i, x.hashid)

		} else if sa[0] == "D" {
			q.c++
			i, _ := strconv.Atoi(sa[1])
			hashid := sa[2]
			k := sa[3]
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
	fmt.Printf("SWARMGet SUCC: [%s]\n", q.hashid)
	return true
}

func (q *d) SWARMGet() (changed bool) {
	// do a read from local file system, filling in: (a) hashid and (b) items
	path := fmt.Sprintf("%s/%s", CHUNK_DIR, q.hashid)
	file, err := os.Open(path)
	if err != nil {
		fmt.Printf("SWARMGet FAIL: [%s]\n", path)
		return false
	}
	defer file.Close()
	var line string
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		line = scanner.Text()
		sa := strings.Split(line, "|")
		if sa[0] == "C" {
			q.c++
			// s := fmt.Sprintf("C|%d|%v|%v\n", i, q.d[i].k, q.d[i].v)
			i, _ := strconv.Atoi(sa[1])

			//x := btDPool.Get().(*d)
			q.d[i].k = []byte(sa[2])
			q.d[i].v = []byte(sa[3])
			q.notloaded = false
			//fmt.Printf(" LOAD-C|%d|%v|%v\n", i, k, v)
		}
		if sa[0] == "P" {
			q.prevhashid = sa[1]
			//fmt.Printf(" LOAD-P|%s\n", q.prevhashid)
		}

		if sa[0] == "N" {
			q.nexthashid = sa[1]
			//fmt.Printf(" LOAD-N|%s\n", q.nexthashid)
		}
	}
	q.notloaded = false
	fmt.Printf("SWARMGet SUCC: [%s]\n", q.hashid)
	return true
}

func (t *Tree) SWARMPut() (changed bool) {
	q := t.r
	if q == nil {
		return
	}

	switch x := q.(type) {
	case *x: // intermediate node -- descend on the next pass
		fmt.Printf("ROOT Node %s [dirty=%v|notloaded=%v]\n", x.hashid, x.dirty, x.notloaded)
		changed = x.SWARMPut()
		if changed {
			t.hashid = x.hashid
		}
	case *d: // data node -- EXACT match
		fmt.Printf("ROOT Node %s [dirty=%v|notloaded=%v]\n", x.hashid, x.dirty, x.notloaded)
		changed = x.SWARMPut()
		if changed {
			t.hashid = x.hashid
		}
	}

	return changed
}

func (q *x) SWARMPut() (changed bool) {
	// recurse through children
	old_hashid := q.hashid
	fmt.Printf("put XNode [c=%d] %s  [dirty=%v|notloaded=%v]\n", q.c, q.hashid, q.dirty, q.notloaded)
	for i := 0; i <= q.c; i++ {
		switch z := q.x[i].ch.(type) {
		case *x:
			if z.dirty {
				z.SWARMPut()
			}
		case *d:
			if z.dirty {
				z.SWARMPut()
			}
		}
	}

	// compute the hash
	h := sha256.New()
	for i := 0; i <= q.c; i++ {
		s := fmt.Sprintf("%v", q.x[i].k)
		h.Write([]byte(s))
		switch z := q.x[i].ch.(type) {
		case *x:
			h.Write([]byte(z.hashid))
		case *d:
			h.Write([]byte(z.hashid))
		}
	}
	q.hashid = fmt.Sprintf("%x", h.Sum(nil))
	if q.hashid != old_hashid || len(old_hashid) < 1 {
		fn := fmt.Sprintf("%s/%s", CHUNK_DIR, q.hashid)
		fmt.Printf("CHANGED %s\n", fn)
		// do a write to local file system with all the items and the children
		f, err := os.Create(fn)
		if err != nil {
			panic(err)
		}
		defer f.Close()

		// write the children with "C"
		for i := 0; i <= q.c; i++ {
			switch z := q.x[i].ch.(type) {
			case *x:
				h.Write([]byte(z.hashid))
				s := fmt.Sprintf("X|%d|%s|%v\n", i, z.hashid, q.x[i].k)
				fmt.Printf(s)
				f.WriteString(s)
			case *d:
				h.Write([]byte(z.hashid))
				s := fmt.Sprintf("D|%d|%s|%v\n", i, z.hashid, q.x[i].k)
				fmt.Printf(s)
				f.WriteString(s)
			}
		}
		f.Sync()
		return true
	} else {
		fmt.Printf("not changed\n")
		return false
	}
}

func (q *d) SWARMPut() (changed bool) {
	// build hash
	old_hashid := q.hashid
	fmt.Printf("put DNode [c=%d] [dirty=%v|notloaded=%v, prev=%s, next=%s]\n", q.c, q.dirty, q.notloaded, q.prevhashid, q.nexthashid)
	h := sha256.New()
	for i := 0; i < q.c; i++ {
		s := fmt.Sprintf("%v", q.d[i].k)
		h.Write([]byte(s))
		s = fmt.Sprintf("%v", q.d[i].v)
		h.Write([]byte(s))
	}
	if q.n != nil {
		if q.n.dirty {
			q.n.SWARMPut()
		}
		q.nexthashid = q.n.hashid
		fmt.Printf(" -- NEXT: %s [%v]\n", q.nexthashid, q.n.dirty)
	}
	q.dirty = false

	if q.p != nil {
		if q.p.dirty {
			q.p.SWARMPut()
		}
		q.prevhashid = q.p.hashid
		fmt.Printf(" -- PREV: %s [%v]\n", q.prevhashid, q.p.dirty)
	}

	fmt.Printf("N: %v P: %v\n", q.n, q.p) //  q.prevhashid, q.nexthashid

	q.hashid = fmt.Sprintf("%x", h.Sum(nil))
	if q.hashid != old_hashid || len(old_hashid) < 1 {
		fn := fmt.Sprintf("%s/%s", CHUNK_DIR, q.hashid)
		fmt.Printf("CHANGED %s\n", fn)
		// do a write to local file system with all the items and the children
		f, err := os.Create(fn)
		if err != nil {
			panic(err)
		}
		defer f.Close()

		// write the children with "C"
		for i := 0; i < q.c; i++ {
			s := fmt.Sprintf("C|%d|%v|%v\n", i, string(q.d[i].k), string(q.d[i].v))
			fmt.Printf(s)
			f.WriteString(s)
		}

		// write the children with "C"
		if q.p != nil {
			s := fmt.Sprintf("P|%s\n", q.prevhashid)
			fmt.Printf(s)
			f.WriteString(s)
		}
		if q.n != nil {
			s := fmt.Sprintf("N|%s\n", q.nexthashid)
			fmt.Printf(s)
			f.WriteString(s)
		}
		f.Sync()
		return true
	} else {
		fmt.Printf("not changed\n")
	}
	return false
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

// Delete removes the k's KV pair, if it exists, in which case Delete returns
// true.
func (t *Tree) Delete(k []byte /*K*/) (ok bool, err error) {
	pi := -1
	var p *x
	q := t.r
	if q == nil {
		var kerr *KeyNotFoundError
		return false, kerr
	}
	for {
		checkload(q)
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

func checkload(q interface{}) {
	switch x := q.(type) {
	case *x: // intermediate node -- descend on the next pass
		if x.notloaded {
			x.SWARMGet()
		}
	case *d: // data node -- EXACT match
		if x.notloaded {
			x.SWARMGet()
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
		checkload(q)

		var i int
		// binary search on the node => i
		if i, ok = t.find(q, k); ok {
			// found it
			switch x := q.(type) {
			case *x: // intermediate node -- descend on the next pass
				q = x.x[i+1].ch
				continue
			case *d: // data node -- EXACT match
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
		checkload(q)
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
		checkload(q)
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
		checkload(q)
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
	fmt.Printf("SPLIT!\n")
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
			r.SWARMGet()
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

func testTable(tableName string, r OrderedDatabase) {
	// open table [only gets the root node]
	vals := rand.Perm(20)
	// write 20 values into B-tree (only kept in memory)
	r.StartBuffer()
	for _, i := range vals {
		k := []byte(fmt.Sprintf("%06x", i))
		v := []byte(fmt.Sprintf("valueof%06x", i))
		fmt.Printf("Insert %d %v %v\n", i, string(k), string(v))
		r.Put(k, v)
	}
	// this writes B+tree to SWARM
	r.FlushBuffer() // tableName
	r.Print()

	r.StartBuffer()
	r.Put([]byte("000004"), []byte("Sammy2"))
	r.Put([]byte("000009"), []byte("Happy2"))
	r.Put([]byte("00000e"), []byte("Leroy2"))
	g, _, _ := r.Get([]byte("00000d"))
	fmt.Printf("GET: %v\n", g)
	r.FlushBuffer()
	r.Print()

	// ENUMERATOR
	res, _, _ := r.Seek([]byte("000004"))
	for k, v, err := res.Next(); err == nil; k, v, err = res.Next() {
		fmt.Printf(" K: %v V: %v\n", string(k), string(v))
	}

}

func main() {
	owner := "0x34c7fc051eae78f8c37b82387a50a5458b8f7018"
	tableName := "testtable"
	columnName := "email"
	r := BPlusTree()
	r.Open(owner, tableName, columnName)
	testTable(tableName, r)
}
