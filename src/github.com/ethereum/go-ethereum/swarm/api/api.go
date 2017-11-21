// Copyright 2016 The go-ethereum Authors
// This file is part of the go-ethereum library.
//
// The go-ethereum library is free software: you can redistribute it and/or modify
// it under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// The go-ethereum library is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU Lesser General Public License for more details.
//
// You should have received a copy of the GNU Lesser General Public License
// along with the go-ethereum library. If not, see <http://www.gnu.org/licenses/>.

package api

import (
	"fmt"
	"io"
	"net/http"
	"regexp"
	"strings"
	"sync"

	"bytes"
	"crypto/sha256"
	"mime"
	"path/filepath"
	"reflect"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/log"
	"github.com/ethereum/go-ethereum/swarm/storage"
)

var (
	hashMatcher      = regexp.MustCompile("^[0-9A-Fa-f]{64}")
	slashes          = regexp.MustCompile("/+")
	domainAndVersion = regexp.MustCompile("[@:;,]+")
)

type Resolver interface {
	Resolve(string) (common.Hash, error)
	Register(string) (*types.Transaction, error)
}

/*
Api implements webserver/file system related content storage and retrieval
on top of the dpa
it is the public interface of the dpa which is included in the ethereum stack
*/
type Api struct {
	dpa *storage.DPA
	dns Resolver
	hashdbroot  *Node
 	ldb  *storage.LDBDatabase 
 	manifestroot []byte
 	trie *manifestTrie
}

//the api constructor initialises
func NewApi(dpa *storage.DPA, dns Resolver) (self *Api) {
	self = &Api{
		dpa: dpa,
		dns: dns,
	}
	return
}

func NewApiTest(dpa *storage.DPA, dns Resolver, ldb *storage.LDBDatabase) (self *Api) {
	rn, err := ldb.Get([]byte("RootNode"))
	hr := NewRootNode([]byte("RootNode"), nil)
	if err == nil{
		hr.NodeHash = rn
	}
    self = &Api{
		dpa: dpa,
		dns: dns,
		ldb:  ldb,
		trie: nil,
		hashdbroot:  hr,
    }
    return
}

// to be used only in TEST
func (self *Api) Upload(uploadDir, index string) (hash string, err error) {
	fs := NewFileSystem(self)
	hash, err = fs.Upload(uploadDir, index)
	return hash, err
}

func (self *Api) StoreDB(data io.Reader, size int64, wg *sync.WaitGroup) (key storage.Key, err error) {
	return self.dpa.StoreDB(data, size, wg, nil)
}

func (self *Api) StoreHashDB(tkey []byte, data io.Reader, size int64, wg *sync.WaitGroup) (key storage.Key, err error) {
	key, err = self.dpa.Store(data, size, wg, nil)
	self.HashDBAdd([]byte(tkey), key, wg)
	return 
}

func (self *Api)HashDBAdd(k []byte, v Val, wg *sync.WaitGroup){
	log.Debug(fmt.Sprintf("HashDBAdd %v \n", self.hashdbroot))
	self.hashdbroot.Add(k, v, self)
}

// DPA reader API
func (self *Api) Retrieve(key storage.Key) storage.LazySectionReader {
	return self.dpa.Retrieve(key)
}

func (self *Api) Store(data io.Reader, size int64, wg *sync.WaitGroup) (key storage.Key, err error) {
	return self.dpa.Store(data, size, wg, nil)
}


type ErrResolve error

// DNS Resolver
func (self *Api) Resolve(uri *URI) (storage.Key, error) {
	log.Trace(fmt.Sprintf("Resolving : %v", uri.Addr))

	// if the URI is immutable, check if the address is a hash
	isHash := hashMatcher.MatchString(uri.Addr)
	if uri.Immutable() {
		if !isHash {
			return nil, fmt.Errorf("immutable address not a content hash: %q", uri.Addr)
		}
		return common.Hex2Bytes(uri.Addr), nil
	}

	// if DNS is not configured, check if the address is a hash
	if self.dns == nil {
		if !isHash {
			return nil, fmt.Errorf("no DNS to resolve name: %q", uri.Addr)
		}
		return common.Hex2Bytes(uri.Addr), nil
	}

	// try and resolve the address
	resolved, err := self.dns.Resolve(uri.Addr)
	if err == nil {
		return resolved[:], nil
	} else if !isHash {
		return nil, err
	}
	return common.Hex2Bytes(uri.Addr), nil
}

// Put provides singleton manifest creation on top of dpa store
func (self *Api) Put(content, contentType string) (storage.Key, error) {
	r := strings.NewReader(content)
	wg := &sync.WaitGroup{}
	key, err := self.dpa.Store(r, int64(len(content)), wg, nil)
	if err != nil {
		return nil, err
	}
	manifest := fmt.Sprintf(`{"entries":[{"hash":"%v","contentType":"%s"}]}`, key, contentType)
	r = strings.NewReader(manifest)
	key, err = self.dpa.Store(r, int64(len(manifest)), wg, nil)
	if err != nil {
		return nil, err
	}
	wg.Wait()
	return key, nil
}

// Put provides singleton manifest creation on top of dpa store
func (self *Api) PutTest(content, contentType, deviceid, email, phone string) (storage.Key, error) {
	log.Debug(fmt.Sprintf("api PutTest1:-%s-%s-%s-", deviceid, email, phone))
	if len(deviceid) == 0 && len(email) == 0 && len(phone) == 0 {
		return nil, fmt.Errorf("no key is in data", )
	}
	deviceid = strings.TrimSpace(deviceid)
	email = strings.TrimSpace(email)
	phone = strings.TrimSpace(phone)
	log.Debug(fmt.Sprintf("api PutTest1:-%s-%s-%s-", deviceid, email, phone))
    	r := strings.NewReader(content)
    	wg := &sync.WaitGroup{}
    	key, err := self.dpa.Store(r, int64(len(content)), wg, nil)
    	if err != nil {
        	return nil, err
    	}
	keys := fmt.Sprintf("%v", key)
    	dbwg := &sync.WaitGroup{}
	dbcontent := deviceid+keys
	rdb := strings.NewReader(dbcontent)
	dbkey, dberr := self.dpa.StoreDB(rdb, int64(len(dbcontent)), dbwg, nil)
    	if dberr != nil {
        	return nil, err
    	}
    	log.Debug(fmt.Sprintf("api PutTest StoreDB: %s %s %s", deviceid, key, dbkey))
/*
    entry := &ManifestEntry{
		Hash: key,
        ContentType: contentType,
        Size:        int64(len(content)),
        ModTime:     time.Now(),
    }
	manifest, err := json.Marshal(&entry)
*/
	//self.ldb.Put([]byte(deviceid), []byte(manifest))

/*
    manifest := fmt.Sprintf(`{"entries":[{"hash":"%v","contentType":"%s"}]}`, key, contentType)
    r = strings.NewReader(string(manifest))
    key, err = self.dpa.Store(r, int64(len(manifest)), wg, nil)
    if err != nil {
        return nil, err
    }
*/
	if len(deviceid) > 0 {
		log.Debug(fmt.Sprintf("api PutTest PUT: %s %s %s", deviceid, key, string(key)))
    		self.ldb.Put([]byte(deviceid), []byte(keys))
	}
	if len(email) > 0 {
		log.Debug(fmt.Sprintf("api PutTest PUT: %s %s %s", email, key, string(key)))
    		self.ldb.Put([]byte(email), []byte(keys))
	}
	if len(phone) > 0 {
		log.Debug(fmt.Sprintf("api PutTest PUT: %s %s %s", phone, key, string(key)))
    		self.ldb.Put([]byte(phone), []byte(keys))
	}
    	//self.ldb.Put([]byte(deviceid), []byte("testtest"))
	log.Debug(fmt.Sprintf("api PutTest2: %s %s %s", deviceid, key, string(key)))
	log.Debug(fmt.Sprintf("ldb type %v %v ", reflect.TypeOf(deviceid), reflect.TypeOf(key)))

	log.Debug(fmt.Sprintf("call Api.SubmitManifest"))
	self.SubmitManifest()
	log.Debug(fmt.Sprintf("finish Api.SubmitManifest"))

    	wg.Wait()
    	return key, nil
}

func (self *Api) PutTable(content, contentType, id, tableid string) (storage.Key, error) {
	uid := tableid + "_" + id
    	r := strings.NewReader(content)
    	wg := &sync.WaitGroup{}
    	key, err := self.dpa.Store(r, int64(len(content)), wg, nil)
	log.Debug(fmt.Sprintf("api PutTable: %s %s %s", id, tableid, uid, string(key)))
	if err != nil {
 	       return nil, err
    	}
	keys := fmt.Sprintf("%v", key)
   	self.ldb.Put([]byte(uid), []byte(keys))
	log.Debug(fmt.Sprintf("api PutTest2: %s %s", uid, string(key)))
	log.Debug(fmt.Sprintf("call Api.SubmitManifest"))
	self.SubmitManifest()
	log.Debug(fmt.Sprintf("finish Api.SubmitManifest"))

    	wg.Wait()
    	return key, nil
}

// Get uses iterative manifest retrieval and prefix matching
// to resolve basePath to content using dpa retrieve
// it returns a section reader, mimeType, status and an error
func (self *Api) Get(key storage.Key, path string) (reader storage.LazySectionReader, mimeType string, status int, err error) {
	log.Trace(fmt.Sprintf("Get key %v, (%s)", key, path))
	trie, err := loadManifest(self.dpa, key, nil)
	if err != nil {
		log.Warn(fmt.Sprintf("loadManifestTrie error: %v", err))
		return
	}

	log.Trace(fmt.Sprintf("getEntry(%s)", path))

	entry, _ := trie.getEntry(path)
	log.Trace(fmt.Sprintf("getmain entry 1: %v '%v'", entry, path))

	if entry != nil {
		key = common.Hex2Bytes(entry.Hash)
		status = entry.Status
		mimeType = entry.ContentType
		log.Trace(fmt.Sprintf("content lookup key: '%v' (%v)", key, mimeType))
		log.Trace(fmt.Sprintf("content lookup key: %v '%v' (%v)", entry.Hash, key, mimeType))
		reader = self.dpa.Retrieve(key)
	} else {
		status = http.StatusNotFound
		err = fmt.Errorf("manifest entry for '%s' not found", path)
		log.Warn(fmt.Sprintf("%v", err))
	}
	return
}

func (self *Api) GetHashDB(path string) (value storage.Key) {
	log.Trace(fmt.Sprintf("GetHashDB start: %v %v", path, self.hashdbroot))
	v := self.hashdbroot.Get([]byte(path), self)
	if v == nil {
		return nil
	}
	value = convertToByte(v)
	log.Trace(fmt.Sprintf("GetHashDB res: %v '%v' %v", path, value))
	return
}

// will move it to hashdb.go
func cv(a Val)[]byte{
	log.Trace(fmt.Sprintf("convertToByte cv: %v %v ", a, reflect.TypeOf(a)))
	if va, ok := a.([]byte); ok{
		log.Trace(fmt.Sprintf("convertToByte cv: %v '%v' %s", a, va, string(va)))
		return []byte(va)
	}
	if va, ok := a.(storage.Key); ok{
		log.Trace(fmt.Sprintf("convertToByte cv key: %v '%v' %s", a, va, string(va)))
		return []byte(va)
	}
	return nil
}

func (self *Api) GetTableData(table string)([]byte) {
	log.Debug(fmt.Sprintf("api GetTableData: ", table))
	key, err := self.ldb.Get([]byte(table))
	if err != nil {
		return nil
	}
	return key 
}
func (self *Api) StoreTableData(table string, rootkey []byte)([]byte) {
	self.ldb.Put([]byte(table), []byte(rootkey))
	return  rootkey
}

func (self *Api) GetManifestRoot()(storage.Key) {
    	key, _ := self.ldb.Get([]byte("manifestroot"))
	return key
}

//func (self *Api) GetTest(key storage.Key, path string) (reader storage.LazySectionReader, mimeType string, status int, err error) {
func (self *Api) GetTest(path string) (reader storage.LazySectionReader, mimeType string, status int, err error) {
	log.Debug(fmt.Sprintf("api GetTest: ", path))
	key, _ := self.ldb.Get([]byte("manifestroot"))

	if key == nil{
		key, _ = self.NewManifest()
	}
	log.Debug(fmt.Sprintf("api GetTest %v self.manifestroot: %s",key, self.manifestroot))
	if string(key) != string(self.manifestroot){
		log.Debug(fmt.Sprintf("api GetTest read trie: %v",key))
    	self.trie, err = loadManifest(self.dpa, common.Hex2Bytes(string(key)), nil)
		log.Debug(fmt.Sprintf("api GetTest read trie done %v: ",key))
    	if err != nil {
        	log.Warn(fmt.Sprintf("loadManifestTrie error: %v", err))
        	return
    	}
		self.manifestroot = key
	}
	trie := self.trie
	log.Debug(fmt.Sprintf("api GetTest self.manifestroot: ",self.manifestroot))

    	log.Trace(fmt.Sprintf("getEntry(%s)", path))

    	entry, _ := trie.getEntry(path)
	var ldbkey []byte

	log.Trace(fmt.Sprintf("gettest entry 1: %v '%v'", entry, path))
	if entry == nil {
		ldbkey, _ =self.ldb.Get([]byte(path))
    	log.Trace(fmt.Sprintf("gettest entry 1.5: %v '%v'", entry, path))
	    log.Warn(fmt.Sprintf("entry null key ldb %v", ldbkey))
	}
	var newpath string
	if entry == nil && ldbkey == nil {
		h256 := sha256.New()
		h256.Write([]byte(path))
		newpath = fmt.Sprintf("%x", h256.Sum(nil))
    	entry, _ = trie.getEntry(newpath)
    	log.Trace(fmt.Sprintf("gettest entry 2: %v '%v'", entry, newpath))
	}
		
	if entry == nil {
		ldbkey, _ =self.ldb.Get([]byte(newpath))
    	log.Trace(fmt.Sprintf("gettest entry 3: %v '%v'", entry, newpath))
	    log.Warn(fmt.Sprintf("entry null key ldb %v", ldbkey))
	}

    	if entry != nil {
        	key = common.Hex2Bytes(entry.Hash)
	        status = entry.Status
        	mimeType = entry.ContentType
	        log.Trace(fmt.Sprintf("content lookup key: %v '%v' (%v)", entry.Hash, key, mimeType))
        	reader = self.dpa.Retrieve(key)
	} else if ldbkey != nil{
		
	} else {
        	status = http.StatusNotFound
        	err = fmt.Errorf("manifest entry for '%s' not found", path)
        	log.Warn(fmt.Sprintf("%v", err))
    	}
    	return
}


func (self *Api) Modify(key storage.Key, path, contentHash, contentType string) (storage.Key, error) {
	quitC := make(chan bool)
	trie, err := loadManifest(self.dpa, key, quitC)
	if err != nil {
		return nil, err
	}
	if contentHash != "" {
		entry := newManifestTrieEntry(&ManifestEntry{
			Path:        path,
			ContentType: contentType,
		}, nil)
		entry.Hash = contentHash
		trie.addEntry(entry, quitC)
	} else {
		trie.deleteEntry(path, quitC)
	}

	if err := trie.recalcAndStore(); err != nil {
		return nil, err
	}
	return trie.hash, nil
}

func (self *Api) AddFile(mhash, path, fname string, content []byte, nameresolver bool) (storage.Key, string, error) {

	uri, err := Parse("bzz:/" + mhash)
	if err != nil {
		return nil, "", err
	}
	mkey, err := self.Resolve(uri)
	if err != nil {
		return nil, "", err
	}

	// trim the root dir we added
	if path[:1] == "/" {
		path = path[1:]
	}

	entry := &ManifestEntry{
		Path:        filepath.Join(path, fname),
		ContentType: mime.TypeByExtension(filepath.Ext(fname)),
		Mode:        0700,
		Size:        int64(len(content)),
		ModTime:     time.Now(),
	}

	mw, err := self.NewManifestWriter(mkey, nil)
	if err != nil {
		return nil, "", err
	}

	fkey, err := mw.AddEntry(bytes.NewReader(content), entry)
	if err != nil {
		return nil, "", err
	}

	newMkey, err := mw.Store()
	if err != nil {
		return nil, "", err

	}

	return fkey, newMkey.String(), nil
}

func (self *Api) RemoveFile(mhash, path, fname string, nameresolver bool) (string, error) {

	uri, err := Parse("bzz:/" + mhash)
	if err != nil {
		return "", err
	}
	mkey, err := self.Resolve(uri)
	if err != nil {
		return "", err
	}

	// trim the root dir we added
	if path[:1] == "/" {
		path = path[1:]
	}

	mw, err := self.NewManifestWriter(mkey, nil)
	if err != nil {
		return "", err
	}

	err = mw.RemoveEntry(filepath.Join(path, fname))
	if err != nil {
		return "", err
	}

	newMkey, err := mw.Store()
	if err != nil {
		return "", err
	}

	return newMkey.String(), nil
}

func (self *Api) SubmitManifest(){
	log.Debug(fmt.Sprintf("Api.SubmitManifest start: "))
    	mkey, err := self.ldb.Get([]byte("manifestroot"))
    	testkey, err := self.ldb.Get([]byte("nooooooooooo"))
	log.Debug(fmt.Sprintf("Api.SubmitManifest mkey: %v %s %v", mkey, string(mkey), testkey))
	mkey = nil
	if mkey == nil{
		log.Debug(fmt.Sprintf("Api.SubmitManifest getting new mkey: %v %s", mkey, string(mkey)))
		mkey, _ = self.NewManifest()
	}else{
		mkey = common.Hex2Bytes(string(mkey))
	}
	log.Debug(fmt.Sprintf("Api.SubmitManifest mkey: %v %s", mkey, string(mkey)))
/*
    quitC := make(chan bool)
	trie, err := loadManifest(self.dpa, mkey, quitC)
	if err != nil{
		log.Debug("load Manifest err %v", err)
	}
/////////////////////
    iter := self.ldb.NewIterator()
    defer iter.Release()
    contentType := "text/plain; charset=utf-8"
    for iter.Next(){
		ikey := iter.Key()
		ivalue := iter.Value()
       	entry := newManifestTrieEntry(&ManifestEntry{
           	Path:       string(ikey),  
           	ContentType: contentType,
			ModTime:     time.Now(),
       	}, nil)
       	entry.Hash = string(ivalue)
		log.Debug(fmt.Sprintf("Api.SubmitManifest iter key = %v value = %v: ", string(ikey), string(ivalue)))
		if string(ikey) != "testdevice222"{
       		trie.addEntry(entry, quitC)
    		if err := trie.recalcAndStore(); err != nil {
        //return nil, err
				log.Debug(fmt.Sprintf("Api.SubmitManifest recalc error %v", err))
        		return
    		}
		}
		log.Debug(fmt.Sprintf("Api.SubmitManifest iter added key = %v value = %v: %v", string(ikey), string(ivalue), trie.hash))
	}
	
	log.Debug(fmt.Sprintf("Api.SubmitManifest -recalc before[%v]: ", trie.hash))
    if err := trie.recalcAndStore(); err != nil {
        //return nil, err
        return
    }
	log.Debug(fmt.Sprintf("Api.SubmitManifest- recalc after[%v]: ", trie.hash))
    keys := fmt.Sprintf("%v", trie.hash)
    self.ldb.Put([]byte("manifestroot"), []byte(keys))
	log.Debug(fmt.Sprintf("Api.SubmitManifest[%v]: ", trie.hash))
*/
	
    	mw, err := self.NewManifestWriter(mkey, nil)
	if err != nil {
		log.Debug(fmt.Sprintf("SubmitManifest NewManifestWriter error %v", err))
	}

	iter := self.ldb.NewIterator()
	defer iter.Release()
	contentType := "text/plain; charset=utf-8"
	for iter.Next(){
		ikey := iter.Key()
		ivalue := iter.Value()
        	entry := &ManifestEntry{
            		Path:       string(ikey),  
            		ContentType: contentType,
			ModTime:     time.Now(),
        	}
        	entry.Hash = string(ivalue)
    		err := mw.AddPath(entry)
		log.Debug(fmt.Sprintf("AddPath error %v", err))
    	}	

	newkey, err := mw.Store()
	keys := fmt.Sprintf("%v", newkey)
    	self.ldb.Put([]byte("manifestroot"), []byte(keys))
	log.Debug(fmt.Sprintf("dns type = %s %v", reflect.TypeOf(self.dns), self.dns))
	//ens := (*ens.ENS)(self.dns)
	self.dns.Register("wolktable.eth")
	//ens.Register("wolktable.eth")
	log.Debug(fmt.Sprintf("Api.SubmitManifest[%v]: ", keys))
	//log.Debug(fmt.Sprintf("Api.SubmitManifest[%s]%s: ", fkey, string(newkey)))
}


func (self *Api) AppendFile(mhash, path, fname string, existingSize int64, content []byte, oldKey storage.Key, offset int64, addSize int64, nameresolver bool) (storage.Key, string, error) {

	buffSize := offset + addSize
	if buffSize < existingSize {
		buffSize = existingSize
	}

	buf := make([]byte, buffSize)

	oldReader := self.Retrieve(oldKey)
	io.ReadAtLeast(oldReader, buf, int(offset))

	newReader := bytes.NewReader(content)
	io.ReadAtLeast(newReader, buf[offset:], int(addSize))

	if buffSize < existingSize {
		io.ReadAtLeast(oldReader, buf[addSize:], int(buffSize))
	}

	combinedReader := bytes.NewReader(buf)
	totalSize := int64(len(buf))

	// TODO(jmozah): to append using pyramid chunker when it is ready
	//oldReader := self.Retrieve(oldKey)
	//newReader := bytes.NewReader(content)
	//combinedReader := io.MultiReader(oldReader, newReader)

	uri, err := Parse("bzz:/" + mhash)
	if err != nil {
		return nil, "", err
	}
	mkey, err := self.Resolve(uri)
	if err != nil {
		return nil, "", err
	}

	// trim the root dir we added
	if path[:1] == "/" {
		path = path[1:]
	}

	mw, err := self.NewManifestWriter(mkey, nil)
	if err != nil {
		return nil, "", err
	}

	err = mw.RemoveEntry(filepath.Join(path, fname))
	if err != nil {
		return nil, "", err
	}

	entry := &ManifestEntry{
		Path:        filepath.Join(path, fname),
		ContentType: mime.TypeByExtension(filepath.Ext(fname)),
		Mode:        0700,
		Size:        totalSize,
		ModTime:     time.Now(),
	}

	fkey, err := mw.AddEntry(io.Reader(combinedReader), entry)
	if err != nil {
		return nil, "", err
	}

	newMkey, err := mw.Store()
	if err != nil {
		return nil, "", err

	}

	return fkey, newMkey.String(), nil

}

func (self *Api) BuildDirectoryTree(mhash string, nameresolver bool) (key storage.Key, manifestEntryMap map[string]*manifestTrieEntry, err error) {

	uri, err := Parse("bzz:/" + mhash)
	if err != nil {
		return nil, nil, err
	}
	key, err = self.Resolve(uri)
	if err != nil {
		return nil, nil, err
	}

	quitC := make(chan bool)
	rootTrie, err := loadManifest(self.dpa, key, quitC)
	if err != nil {
		return nil, nil, fmt.Errorf("can't load manifest %v: %v", key.String(), err)
	}

	manifestEntryMap = map[string]*manifestTrieEntry{}
	err = rootTrie.listWithPrefix(uri.Path, quitC, func(entry *manifestTrieEntry, suffix string) {
		manifestEntryMap[suffix] = entry
	})

	return key, manifestEntryMap, nil
}
