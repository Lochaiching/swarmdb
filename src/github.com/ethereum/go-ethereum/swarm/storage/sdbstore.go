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

package storage

import (
	//"encoding/json"
	"fmt"
	//"path/filepath"
	"sync"
	//"time"
	//"bytes"
	//"strings"

	//"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/log"
	//"github.com/ethereum/go-ethereum/swarmdb"
)

/*
SdbStore is a cloud storage access abstaction layer for swarm
it contains the shared logic of network served chunk store/retrieval requests
both local (coming from DPA api) and remote (coming from peers via bzz protocol)
it implements the ChunkStore interface and embeds LocalStore

It is called by the bzz protocol instances via Depo (the store/retrieve request handler)
a protocol instance is running on each peer, so this is heavily parallelised.
SdbStore falls back to a backend (CloudStorage interface)
implemented by bzz/network/forwarder. forwarder or IPFS or IPΞS
*/
type SdbStore struct {
	//localStore *LocalStore
	localStore *MemStore
	cloud      CloudStore
	//swarmdb	   *swarmdb.SwarmDB
	lock       sync.Mutex
}

// backend engine for cloud store
// It can be aggregate dispatching to several parallel implementations:
// bzz/network/forwarder. forwarder or IPFS or IPΞS
/*
type CloudStore interface {
	Store(*Chunk)
	StoreDB([]byte, []byte, *CloudOption)
	Deliver(*Chunk)
	Retrieve(*Chunk)
	RetrieveDB(*Chunk)
}

type StoreParams struct {
	ChunkDbPath   string
	DbCapacity    uint64
	CacheCapacity uint
	Radius        int
}

func NewStoreParams(path string) (self *StoreParams) {
	return &StoreParams{
		ChunkDbPath:   filepath.Join(path, "chunks"),
		DbCapacity:    defaultDbCapacity,
		CacheCapacity: defaultCacheCapacity,
		Radius:        defaultRadius,
	}
}
*/

// netstore contructor, takes path argument that is used to initialise dbStore,
// the persistent (disk) storage component of LocalStore
// the second argument is the hive, the connection/logistics manager for the node
//func NewSdbStore(lstore *LocalStore, cloud CloudStore, swarmdb *swarmdb.SwarmDB) *SdbStore {
func NewSdbStore(lstore *MemStore, cloud CloudStore) *SdbStore {
	return &SdbStore{
		localStore: lstore,
		cloud:      cloud,
		//swarmdb:    swarmdb,
	}
}

/*
const (
	// maximum number of peers that a retrieved message is delivered to
	requesterCount = 3
)

var (
	// timeout interval before retrieval is timed out
	searchTimeout = 3 * time.Second
)
*/

func (self *SdbStore) Put(entry *Chunk) {
	log.Debug(fmt.Sprintf("[wolk-cloudstore] SdbStore.Put: entry %v %v", entry, entry.Key))
	//chunk, _ := self.localStore.memStore.Get(entry.Key)
	chunk, _ := self.localStore.Get(entry.Key)
	log.Debug(fmt.Sprintf("memcheck [wolk-cloudstore] SdbStore.Put: memstore %v %v %v", entry.Key, chunk, &chunk))
	
//if entry.Size == 0, it's from storeRequest
	if chunk != nil && chunk.Req != nil && chunk.SData == nil && entry.Size == 0{
//TODO: need to check version information to store the latest version  
//	if chunk != nil && chunk.Req != nil {
		chunk.SData = entry.SData
		chunk.Options = entry.Options
		close(chunk.Req.C)
		chunk.Req = nil
//		self.localStore.memStore.Put(chunk)
		self.localStore.Put(chunk)
//TODO :if version check here, don't need this line or add some process for finding result.
		log.Debug(fmt.Sprintf("memcheck [wolk-cloudstore] SdbStore.Put in if statement: memstore %v %v %v", entry.Key, chunk, &chunk))
		log.Debug(fmt.Sprintf("[wolk-cloudstore] SdbStore.Put: closing Req.C"))
		entry.Size = int64(len(entry.SData))
	}
//TODO: add delivery method. need to modify it once it gets the version info.
	self.cloud.StoreDB(entry)
}

// called by dbchunkstore only
func (self *SdbStore) Get(key Key) (*Chunk, error) {
	// no data and no request status
	log.Debug(fmt.Sprintf("[wolk-cloudstore] SdbStore.Get: key %v", key))
	chunk := NewChunk(key, newRequestStatus(key))
	//self.localStore.memStore.Put(chunk)
	self.localStore.Put(chunk)
	go self.cloud.RetrieveDB(chunk)
	log.Debug(fmt.Sprintf("memcheck [wolk-cloudstore] SdbStore.Get: key %v val %d address %v %v", key, len(chunk.SData), chunk, &chunk))
	return chunk, nil
}

// Close netstore
func (self *SdbStore) Close() {
	return
}
