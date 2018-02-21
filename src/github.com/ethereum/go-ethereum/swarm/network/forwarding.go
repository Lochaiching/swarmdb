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
  //"encoding/json"
	"fmt"
	"math/rand"
	"time"
	"github.com/ethereum/go-ethereum/log"
	"github.com/ethereum/go-ethereum/swarm/storage"
)

const requesterCount = 3

/*
forwarder implements the CloudStore interface (use by storage.NetStore)
and serves as the cloud store backend orchestrating storage/retrieval/delivery
via the native bzz protocol
which uses an MSB logarithmic distance-based semi-permanent Kademlia table for
* recursive forwarding style routing for retrieval
* smart syncronisation
*/

type forwarder struct {
	hive *Hive
}

func NewForwarder(hive *Hive) *forwarder {
	return &forwarder{hive: hive}
}

// generate a unique id uint64
func generateId() uint64 {
	r := rand.New(rand.NewSource(time.Now().UnixNano()))
	return uint64(r.Int63())
}

var searchTimeout = 3 * time.Second

// forwarding logic
// logic propagating retrieve requests to peers given by the kademlia hive
func (self *forwarder) Retrieve(chunk *storage.Chunk) {
	log.Trace(fmt.Sprintf("forwarder.Retrieve: hive %s ", self.hive.String()))
	peers := self.hive.getPeers(chunk.Key, 0)
	log.Trace(fmt.Sprintf("forwarder.Retrieve: %v - received %d peers from KÎ›Ã�ÎžMLIÎ›... %v", chunk.Key.Log(), len(peers), peers))
OUT:
	for _, p := range peers {
		log.Trace(fmt.Sprintf("forwarder.Retrieve: sending retrieveRequest %v to peer [%v] %v", chunk.Key.Log(), p, chunk.Key))
		for _, recipients := range chunk.Req.Requesters {
			for _, recipient := range recipients {
				req := recipient.(*retrieveRequestMsgData)
				if req.from.Addr() == p.Addr() {
					continue OUT
				}
			}
		}
		req := &retrieveRequestMsgData{
			Key: chunk.Key,
			Id:  generateId(),
		}
		var err error
		if p.swap != nil {
			err = p.swap.Add(-1)
		}
		if err == nil {
			p.retrieve(req)
			break OUT
		}
		log.Warn(fmt.Sprintf("forwarder.Retrieve: unable to send retrieveRequest to peer [%v]: %v", chunk.Key.Log(), err))
	}
}

// TODO: will be saparated from forwarder
func (self *forwarder) RetrieveDB(chunk *storage.Chunk) {
        log.Trace(fmt.Sprintf("[wolk-cloudstore] forwarder.RetrieveDB: hive %s ", self.hive.String()))
        peers := self.hive.getPeers(chunk.Key, 0)
        log.Trace(fmt.Sprintf("forwarder.RetrieveDB: %v - received %d peers from KÎ›Ã�ÎžMLIÎ›... %v", chunk.Key.Log(), len(peers), peers))
OUT:
        for _, p := range peers {
                log.Trace(fmt.Sprintf("forwarder.Retrieve DB: sending retrieveRequest %v to peer [%v] %v", chunk.Key.Log(), p, chunk.Key))
                for _, recipients := range chunk.Req.Requesters {
                        for _, recipient := range recipients {
                                req := recipient.(*sDBRetrieveRequestMsgData)
                                if req.from.Addr() == p.Addr() {
                                        continue OUT
                                }
                        }
                }
                req := &sDBRetrieveRequestMsgData{
                        Key: chunk.Key,
                        Id:  generateId(),
                }

			//SWARMDB payment
			var err error
			if p.swarmdb.SwapDB != nil {
				err = p.swarmdb.SwapDB.Add(-1)
			}
			if err != nil {
				log.Warn(fmt.Sprintf("forwarder.RetrieveDB: - cannot process request p.swapDB.Add(-1): %v", err))
			}

                if err == nil {
                        p.sDBRetrieve(req)
                        break OUT
                }
                log.Warn(fmt.Sprintf("forwarder.RetrieveDB: unable to send retrieveRequest to peer [%v]: %v", chunk.Key.Log(), err))
        }
}

// requests to specific peers given by the kademlia hive
// except for peers that the store request came from (if any)
// delivery queueing taken care of by syncer
func (self *forwarder) Store(chunk *storage.Chunk) {
	var n int
	msg := &storeRequestMsgData{
		Key:   chunk.Key,
		SData: chunk.SData,
		stype:	0,
	}
	
	var source *peer
	if chunk.Source != nil {
		source = chunk.Source.(*peer)
	}
	for _, p := range self.hive.getPeers(chunk.Key, 0) {
		log.Trace(fmt.Sprintf("forwarder.Store: %v %v", p, chunk))

		if p.syncer != nil && (source == nil || p.Addr() != source.Addr()) {
			n++
			Deliver(p, msg, PropagateReq)
		}
	}
	log.Trace(fmt.Sprintf("forwarder.Store: sent to %v peers (chunk = %v)", n, chunk))
}

//TODO: will be separated from forwarder. 
func (self *forwarder) StoreDB(chunk *storage.Chunk) {
        log.Debug(fmt.Sprintf("[wolk-cloudstore] forwarder.StoreDB :request peers to store swarmdb :%v", chunk.Key))
        var n int
        msg := &sDBStoreRequestMsgData{
                Key:   chunk.Key,
                SData: chunk.SData,
		rtype : 2,
		option : string(chunk.Options),
        }

        var source *peer
        if chunk.Source != nil{
               source = chunk.Source.(*peer)
        }

        for _, p := range self.hive.getPeers(chunk.Key, 0) {
                log.Debug(fmt.Sprintf("forwarder.StoreDB: %v %v", p, chunk))

                if p.syncer != nil && (source == nil || p.Addr() != source.Addr()) {
                log.Debug(fmt.Sprintf("forwarder.StoreDB source check: %v %v", p, source))
                        n++
                        Deliver(p, msg, StoreDBReq)
                }
        }
        log.Debug(fmt.Sprintf("forwarder.StoreDB: sent to %v peers (key = %v)", n, chunk.Key))
}


func (self *forwarder) DeliverDB(){
}


func (self *forwarder) StoreTest(chunk *storage.Chunk) {
    var n int
    msg := &storeRequestMsgData{
        Key:   chunk.Key,
        SData: chunk.SData,
    }
    var source *peer
    if chunk.Source != nil {
        source = chunk.Source.(*peer)
    }
    for _, p := range self.hive.getPeers(chunk.Key, 0) {
        log.Trace(fmt.Sprintf("forwarder.Store: %v %v", p, chunk))

        if p.syncer != nil && (source == nil || p.Addr() != source.Addr()) {
            n++
            Deliver(p, msg, PropagateReq)
        }
    }
    log.Trace(fmt.Sprintf("forwarder.Store: sent to %v peers (chunk = %v)", n, chunk))
}


// once a chunk is found deliver it to its requesters unless timed out
func (self *forwarder) Deliver(chunk *storage.Chunk) {
	// iterate over request entries
	for id, requesters := range chunk.Req.Requesters {
		counter := requesterCount
		msg := &storeRequestMsgData{
			Key:   chunk.Key,
			SData: chunk.SData,
			stype: 0, 
		}
		log.Trace(fmt.Sprintf("forwarder.Deliver: msg %v %v  %v", chunk.Key, chunk.SData, counter))
		var n int
		var req *retrieveRequestMsgData
		// iterate over requesters with the same id
		for id, r := range requesters {
			req = r.(*retrieveRequestMsgData)
			if req.timeout == nil || req.timeout.After(time.Now()) {
				log.Trace(fmt.Sprintf("forwarder.Deliver: %v -> %v", req.Id, req.from))
				msg.Id = uint64(id)
				Deliver(req.from, msg, DeliverReq)
				n++
				counter--
				if counter <= 0 {
					break
				}
			}
		}
		log.Trace(fmt.Sprintf("forwarder.Deliver: submit chunk %v (request id %v) for delivery to %v peers", chunk.Key.Log(), id, n))
	}
}

// initiate delivery of a chunk to a particular peer via syncer#addRequest
// depending on syncer mode and priority settings and sync request type
// this either goes via confirmation roundtrip or queued or pushed directly
func Deliver(p *peer, req interface{}, ty int) {
        log.Debug(fmt.Sprintf("[wolk-cloudstore] forwarder.Deliver :request peer %v to store (%T) :%d", p, req, ty))
	p.syncer.addRequest(req, ty)
}

// push chunk over to peer
func Push(p *peer, key storage.Key, priority uint) {
        log.Debug(fmt.Sprintf("[wolk-cloudstore] forwarder.Push :request peer %v to deliver (%T) :%d", p, key, priority))
	p.syncer.doDelivery(key, priority, p.syncer.quit)
}
