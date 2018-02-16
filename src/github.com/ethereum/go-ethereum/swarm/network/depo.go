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

package network

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"time"
	"github.com/ethereum/go-ethereum/log"
	"github.com/ethereum/go-ethereum/swarm/storage"
	"github.com/ethereum/go-ethereum/swarmdb"
)

// Handler for storage/retrieval related protocol requests
// implements the StorageHandler interface used by the bzz protocol
type Depo struct {
	hashfunc   storage.Hasher
	localStore storage.ChunkStore
	netStore   storage.ChunkStore
	sdbStore   storage.ChunkStore
	swarmdb   *swarmdb.SwarmDB // will be change to ChunkStore
}

func NewDepo(hash storage.Hasher, localStore, remoteStore storage.ChunkStore, sdbStore storage.ChunkStore, swarmdb *swarmdb.SwarmDB) *Depo {
	return &Depo{
		hashfunc:   hash,
		localStore: localStore,
		netStore:   remoteStore, // entrypoint internal
		sdbStore:   sdbStore, 
		swarmdb:   swarmdb, 	//will be changed to ChunkStore 
	}
}


// Handles UnsyncedKeysMsg after msg decoding - unsynced hashes upto sync state
// * the remote sync state is just stored and handled in protocol
// * filters through the new syncRequests and send the ones missing
// * back immediately as a deliveryRequest message
// * empty message just pings back for more (is this needed?)
// * strict signed sync states may be needed.
func (self *Depo) HandleUnsyncedKeysMsg(req *unsyncedKeysMsgData, p *peer) error {
	unsynced := req.Unsynced
	var missing []*syncRequest
	var chunk *storage.Chunk
	var err error
	for _, req := range unsynced {
		log.Trace(fmt.Sprintf("Depo.HandleUnsyncedKeysMsg: received req %v %v", req, req.Key))
		if req.Priority == 3{
			ret, _, err := self.swarmdb.RetrieveDB([]byte(req.Key))
//////// debug
//			ret = nil
			if err != nil || ret == nil{
				missing = append(missing, req)
			}
///// Mayumi : need to implement to store data to swarmdb : check version???
		} else {
			// skip keys that are found,
			chunk, err = self.localStore.Get(storage.Key(req.Key[:]))
			if err != nil || chunk.SData == nil {
				missing = append(missing, req)
			}
		}
	}
        log.Debug(fmt.Sprintf("[wolk-cloudstore] depo.HandleUnsyncedKeysMsg :received %v unsynced keys: %v missing. new state: %v", len(unsynced), len(missing), req.State))
	log.Debug(fmt.Sprintf("Depo.HandleUnsyncedKeysMsg: received %v unsynced keys: %v missing. new state: %v", len(unsynced), len(missing), req.State))
	// send delivery request with missing keys
	err = p.deliveryRequest(missing)
	if err != nil {
		return err
	}
	// set peers state to persist
	p.syncState = req.State
	return nil
}

// Handles deliveryRequestMsg
// * serves actual chunks asked by the remote peer
// by pushing to the delivery queue (sync db) of the correct priority
// (remote peer is free to reprioritize)
// * the message implies remote peer wants more, so trigger for
// * new outgoing unsynced keys message is fired
func (self *Depo) HandleDeliveryRequestMsg(req *deliveryRequestMsgData, p *peer) error {
        log.Debug(fmt.Sprintf("[wolk-cloudstore] depo.HandleDeliveryRequestMsg :received %v from %v", req, p))
	log.Trace(fmt.Sprintf("Depo.HandleDeliveryRequestMsg: req %v %v", req, p))

	deliver := req.Deliver
	// queue the actual delivery of a chunk ()
	log.Trace(fmt.Sprintf("Depo.HandleDeliveryRequestMsg: received %v delivery requests: %v", len(deliver), deliver))
	for _, sreq := range deliver {
		// TODO: look up in cache here or in deliveries
		// priorities are taken from the message so the remote party can
		// reprioritise to at their leisure
		// r = self.pullCached(sreq.Key) // pulls and deletes from cache
		Push(p, sreq.Key, sreq.Priority)
	}

	// sends it out as unsyncedKeysMsg
	p.syncer.sendUnsyncedKeys()
	return nil
}

// the entrypoint for store requests coming from the bzz wire protocol
// if key found locally, return. otherwise
// remote is untrusted, so hash is verified and chunk passed on to NetStore
func (self *Depo) HandleStoreRequestMsg(req *storeRequestMsgData, p *peer) {
	var islocal bool
	req.from = p
	chunk, err := self.localStore.Get(req.Key)
	switch {
	case err != nil:
		log.Trace(fmt.Sprintf("Depo.handleStoreRequest: %v not found locally. create new chunk/request", req.Key))
		// not found in memory cache, ie., a genuine store request
		// create chunk
		chunk = storage.NewChunk(req.Key, nil)

	case chunk.SData == nil:
		// found chunk in memory store, needs the data, validate now
		log.Trace(fmt.Sprintf("Depo.HandleStoreRequest: %v. request entry found", req))

	default:
		// data is found, store request ignored
		// this should update access count?
		log.Trace(fmt.Sprintf("Depo.HandleStoreRequest: %v found locally. ignore.", req))
		islocal = true
		//return
	}

	hasher := self.hashfunc()
	hasher.Write(req.SData)
	log.Trace(fmt.Sprintf("Depo.HandleStoreRequest: SData: %v %v", req.SData, req))
	
	if !bytes.Equal(hasher.Sum(nil), req.Key) {
		// data does not validate, ignore
		// TODO: peer should be penalised/dropped?
		log.Warn(fmt.Sprintf("Depo.HandleStoreRequest: chunk invalid. store request ignored: %v", req))
		return
	}

	if islocal {
		return
	}
	// update chunk with size and data
	chunk.SData = req.SData // protocol validates that SData is minimum 9 bytes long (int64 size  + at least one byte of data)
	chunk.Size = int64(binary.LittleEndian.Uint64(req.SData[0:8]))
	log.Trace(fmt.Sprintf("delivery of %v from %v", chunk, p))
	chunk.Source = p
	self.netStore.Put(chunk)
}

func (self *Depo) HandleSdbStoreRequestMsg(req *sDBStoreRequestMsgData, p *peer) {
        log.Debug(fmt.Sprintf("[wolk-cloudstore] depo.HandleSdbStoreRequestMsg :received %v from %v", req.Key, p))
        log.Trace(fmt.Sprintf("Depo.HandleSdbStoreRequest: %v %v", req.Key, p))
        req.from = p
        ret, opt, err := self.swarmdb.RetrieveDB(req.Key)
        log.Debug(fmt.Sprintf("depo.HandleSdbStoreRequestMsg :option %v from %v", req.option, p))
	var ropt storage.CloudOption
/* debug */
	jerr := json.Unmarshal([]byte(req.option), &ropt)
	if jerr != nil{
        	log.Debug(fmt.Sprintf("depo.HandleSdbStoreRequestMsg :json error option %v  %v", req.option, jerr))
		return
	}
/* */
	ropt.Source = p
	if ropt.Version <= opt.Version{
	///////debug commented out
		//return
		return
	}
	if err != nil{
        	self.swarmdb.StoreDB([]byte(req.Key), req.SData, &ropt)
	}
///// Mayumi :need to change args. 
	//jopt, err := json.Marshal(ropt)
/// TODO: review options
	chunk := storage.NewChunk(req.Key, nil)
	chunk.SData = req.SData
	chunk.Options = []byte(req.option)
	chunk.Source = p
        log.Debug(fmt.Sprintf("[wolk-cloudstore] depo.HandleSdbStoreRequestMsg :storing to sdbStore %v from %v with %v", req.Key, p, chunk))
	self.sdbStore.Put(chunk)
        //self.swarmdb.SwarmStore.StoreDB([]byte(req.Key), req.SData, jopt)
	//self.localStore.memStore.Get(k)
        //self.netStore.PutDB([]byte(req.Key), req.SData, &ropt)
		
        switch {
        case err != nil:
                log.Trace(fmt.Sprintf("Depo.HandleSdbStoreRequest: %v not found locally. create new chunk/request", req.Key))

        case ret== nil:
                log.Trace(fmt.Sprintf("Depo.HandleSdbStoreRequest: %v. request entry found", req))

        default:
                // data is found, store request ignored
                // this should update access count?
                log.Trace(fmt.Sprintf("Depo.HandleSdbStoreRequest: %v found locally. ignore.", req))
        }
	return
}

// entrypoint for retrieve requests coming from the bzz wire protocol
// checks swap balance - return if peer has no credit
func (self *Depo) HandleRetrieveRequestMsg(req *retrieveRequestMsgData, p *peer) {
	req.from = p
	// swap - record credit for 1 request
	// note that only charge actual reqsearches
	var err error
	if p.swap != nil {
		err = p.swap.Add(1)
	}
	if err != nil {
		log.Warn(fmt.Sprintf("Depo.HandleRetrieveRequest: %v - cannot process request: %v", req.Key.Log(), err))
		return
	}

	// call storage.NetStore#Get which
	// blocks until local retrieval finished
	// launches cloud retrieval
	chunk, _ := self.netStore.Get(req.Key)
	req = self.strategyUpdateRequest(chunk.Req, req)
	// check if we can immediately deliver
	if chunk.SData != nil {
		log.Trace(fmt.Sprintf("Depo.HandleRetrieveRequest: %v - content found, delivering...", req.Key.Log()))

		if req.MaxSize == 0 || int64(req.MaxSize) >= chunk.Size {
			sreq := &storeRequestMsgData{
				Id:             req.Id,
				Key:            chunk.Key,
				SData:          chunk.SData,
				requestTimeout: req.timeout, //
			}
			p.syncer.addRequest(sreq, DeliverReq)
		} else {
			log.Trace(fmt.Sprintf("Depo.HandleRetrieveRequest: %v - content found, not wanted", req.Key.Log()))
		}
	} else {
		log.Trace(fmt.Sprintf("Depo.HandleRetrieveRequest: %v - content not found locally. asked swarm for help. will get back", req.Key.Log()))
	}
}

func (self *Depo) HandleSdbRetrieveRequestMsg(req *retrieveRequestMsgData, p *peer) {
	log.Debug(fmt.Sprintf("[wolk-cloudstore] depo.HandleSdbRetrieveRequestMsg :received %v from %v", req.Key, p))
	req.from = p

	// SwarmDBSwap: Check balance and send money if needed
	// swap - record credit for 1 request
	// note that only charge actual reqsearches
	var err error
	if p.swapDB != nil {
		err = p.swapDB.Add(1)
	}
	if err != nil {
		log.Warn(fmt.Sprintf("Depo.HandleSdbRetrieveRequestMsg: %v - cannot process request p.swapDB.Add(1): %v", req.Key.Log(), err))
		return
	}

	// okay to ignore err since it means this node doesn't have the key's result
	ret, opt, _ := self.swarmdb.RetrieveDB([]byte(req.Key))
////TODO : check what is needed
        //req = self.strategyUpdateRequest(chunk.Req, req)

        // check if we can immediately deliver
	if ret != nil {
		jopt, err := json.Marshal(opt)
		if err != nil{
                	log.Debug(fmt.Sprintf("Depo.HandleSdbRetrieveRequest: json err %v %v %v", req.Key.Log(), opt, err))
		}
                log.Trace(fmt.Sprintf("Depo.HandleSdbRetrieveRequest: %v - content found, delivering...", req.Key.Log()))
		sreq := &sDBStoreRequestMsgData{
			Id:             req.Id,
			Key:            req.Key,
			SData:          ret,
			option:         string(jopt),
			rtype:          2,
			requestTimeout: req.timeout, //
		}
                log.Debug(fmt.Sprintf("Depo.HandleSdbRetrieveRequest: %v - sreq", req.Key.Log(), sreq))
                p.syncer.addRequest(sreq, StoreDBReq)
	} else {
                log.Trace(fmt.Sprintf("Depo.HandleSdbRetrieveRequest: %v - content not found locally. asked swarm for help. will get back", req.Key.Log()))
	}
}

// add peer request the chunk and decides the timeout for the response if still searching
func (self *Depo) strategyUpdateRequest(rs *storage.RequestStatus, origReq *retrieveRequestMsgData) (req *retrieveRequestMsgData) {
	log.Trace(fmt.Sprintf("Depo.strategyUpdateRequest: key %v", origReq.Key.Log()))
	// we do not create an alternative one
	req = origReq
	if rs != nil {
		self.addRequester(rs, req)
		req.setTimeout(self.searchTimeout(rs, req))
	}
	return
}

// decides the timeout promise sent with the immediate peers response to a retrieve request
// if timeout is explicitly set and expired
func (self *Depo) searchTimeout(rs *storage.RequestStatus, req *retrieveRequestMsgData) (timeout *time.Time) {
	reqt := req.getTimeout()
	t := time.Now().Add(searchTimeout)
	if reqt != nil && reqt.Before(t) {
		return reqt
	} else {
		return &t
	}
}

/*
adds a new peer to an existing open request
only add if less than requesterCount peers forwarded the same request id so far
note this is done irrespective of status (searching or found)
*/
func (self *Depo) addRequester(rs *storage.RequestStatus, req *retrieveRequestMsgData) {
	log.Trace(fmt.Sprintf("Depo.addRequester: key %v - add peer to req.Id %v", req.Key.Log(), req.Id))
	list := rs.Requesters[req.Id]
	rs.Requesters[req.Id] = append(list, req)
}

