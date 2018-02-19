// Copyright (c) 2018 Wolk Inc.  All rights reserved.

// The SWARMDB library is free software: you can redistribute it and/or modify
// it under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// The SWARMDB library is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU Lesser General Public License for more details.
//
// You should have received a copy of the GNU Lesser General Public License
// along with the go-ethereum library. If not, see <http://www.gnu.org/licenses/>.

package swarmdb

import (
	"bytes"
	"crypto/sha256"
	"fmt"
	"github.com/ethereum/go-ethereum/common"
	ethcommon "github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/log"
	"github.com/ethereum/go-ethereum/rlp"
	// _ "github.com/mattn/go-sqlite3"
	"github.com/syndtr/goleveldb/leveldb"
	"github.com/syndtr/goleveldb/leveldb/util"
	"time"
)

const (
	hashChunkSize = 4000
	epochSeconds  = 600
)

type DBChunkstore struct {
	ldb      *leveldb.DB
	km       *KeyManager
	netstats *Netstats
	farmer   ethcommon.Address
	filepath string
}

type DBChunk struct {
	Val []byte
	Enc byte
}

type ChunkLog struct {
	Farmer           string `json:"farmer"`
	ChunkID          string `json:"chunkID"`
	ChunkHash        []byte `json:"-"`
	ChunkBD          int    `json:"chunkBD"`
	ChunkSD          int    `json:"chunkSD"`
	ReplicationLevel int    `json:"rep"`
	Renewable        int    `json:"renewable"`
}

func NewDBChunkStore(config *SWARMDBConfig, netstats *Netstats) (self *DBChunkstore, err error) {
	path := config.ChunkDBPath
	ldb, err := leveldb.OpenFile(path, nil)
	if err != nil {
		return self, err
	}

	km, errKM := NewKeyManager(config)
	if errKM != nil {
		return nil, GenerateSWARMDBError(errKM, fmt.Sprintf("[dbchunkstore:NewDBChunkStore] NewKeyManager %s", errKM.Error()))
	}

	userWallet := config.Address

	walletAddr := common.HexToAddress(userWallet)

	self = &DBChunkstore{
		ldb:      ldb,
		km:       &km,
		farmer:   walletAddr,
		filepath: path,
	}
	return self, nil
}

func (self *DBChunkstore) GetKeyManager() (km *KeyManager) {
	return self.km
}

func (self *DBChunkstore) StoreKChunk(u *SWARMDBUser, key []byte, val []byte, encrypted int) (err error) {
	_, err = self.storeChunkInDB(u, val, encrypted, key)
	return err
}

func (self *DBChunkstore) StoreChunk(u *SWARMDBUser, val []byte, encrypted int) (key []byte, err error) {
	return self.storeChunkInDB(u, val, encrypted, key)
}

func (self *DBChunkstore) storeChunkInDB(u *SWARMDBUser, val []byte, encrypted int, k []byte) (key []byte, err error) {
	if len(val) < CHUNK_SIZE {
		return nil, &SWARMDBError{message: fmt.Sprintf("[dbchunkstore:StoreChunk] Chunk too small (< %s)| %x", CHUNK_SIZE, val), ErrorCode: 439, ErrorMessage: "Unable to Store Chunk"}
	}
	var finalSdata []byte
	if len(k) > 0 {
		finalSdata = make([]byte, CHUNK_SIZE)
		key = k
		recordData := val[KNODE_START_ENCRYPTION : CHUNK_SIZE-41] //MAJOR TODO: figure out how we pass in to ensure <=4096
		log.Debug(fmt.Sprintf("Key: [%x][%v] After Loop recordData length (%d) and start pos %d", key, key, len(recordData), KNODE_START_ENCRYPTION))
		copy(finalSdata[0:KNODE_START_ENCRYPTION], val[0:KNODE_START_ENCRYPTION])
		copy(finalSdata[KNODE_START_ENCRYPTION:CHUNK_SIZE], recordData)
		val = finalSdata

	} else {
		inp := make([]byte, hashChunkSize)
		copy(inp, val[0:hashChunkSize])
		h := sha256.New() // TODO: Update this
		h.Write([]byte(inp))
		key = h.Sum(nil)

	}

	var chunk DBChunk
	if encrypted > 0 {
		// TODO: add { chunkBirthDT, chunkStoreDT, renewal, minReplication, maxReplication, payer, version }
		log.Debug(fmt.Sprintf("StoreChunk: Encrypted bit when saving was: %d", encrypted))
		val = self.km.EncryptData(u, val)
		chunk.Enc = 1
	}
	chunk.Val = val
	data, err := rlp.EncodeToBytes(chunk)
	if err != nil {
		return key, err
	}
	err = self.ldb.Put(key, data, nil)
	if err != nil {
		return key, &SWARMDBError{message: fmt.Sprintf("[dbchunkstore:StoreChunk] Exec %s | encrypted:%s", err.Error(), encrypted), ErrorCode: 439, ErrorMessage: "Unable to Store Chunk"}
	}
	//fmt.Printf("storeChunkInDB enc: %d [%x] -- %x\n", chunk.Enc, key, data)

	// TODO: the TS here should be the FIRST time the chunk is originally written
	ts := int64(time.Now().Unix())
	epochPrefix := epochBytesFromTimestamp(ts)
	ekey := append(epochPrefix, key...)
	// fmt.Printf("%d --> %x --> %x\n", ts, epochPrefix, ekey)

	data = []byte("1")
	if err != nil {
		return key, err
	}
	err = self.ldb.Put(ekey, data, nil)
	if err != nil {
		return key, &SWARMDBError{message: fmt.Sprintf("[dbchunkstore:StoreChunk] Exec %s | encrypted:%s", err.Error(), encrypted), ErrorCode: 439, ErrorMessage: "Unable to Store Chunk"}
	}

	return key, nil
}

func (self *DBChunkstore) RetrieveChunk(u *SWARMDBUser, key []byte) (val []byte, err error) {
	data, err := self.ldb.Get(key, nil)
	if err == leveldb.ErrNotFound {
		val = make([]byte, CHUNK_SIZE)
		return val, nil
	} else if err != nil {
		return val, err
	}
	c := new(DBChunk)
	err = rlp.Decode(bytes.NewReader(data), c)
	if err != nil {
		return val, &SWARMDBError{message: fmt.Sprintf("[dbchunkstore:RetrieveChunk] Prepare %s", err.Error()), ErrorCode: 440, ErrorMessage: "Unable to Retrieve Chunk"}
	}
	val = c.Val
	if c.Enc > 0 {
		val, err = self.km.DecryptData(u, val)
		if err != nil {
			return val, &SWARMDBError{message: fmt.Sprintf("[dbchunkstore:RetrieveChunk] DecryptData %s", err.Error()), ErrorCode: 440, ErrorMessage: "Unable to Retrieve Chunk"}
		}

	}
	return val, nil
}

func (self *DBChunkstore) RetrieveKChunk(u *SWARMDBUser, key []byte) (val []byte, err error) {
	val, err = self.RetrieveChunk(u, key)
	if err != nil {
		return val, err // TODO
	}
	jsonRecord := val[KNODE_START_ENCRYPTION:]
	return bytes.TrimRight(jsonRecord, "\x00"), nil
}

func epochBytesFromTimestamp(ts int64) (out []byte) {
	return IntToByte(int(ts / epochSeconds))
}

func (self *DBChunkstore) GenerateBuyerLog(startTS int64, endTS int64) (err error) {
	for ts := startTS; ts < endTS; ts += epochSeconds {
		epochPrefix := epochBytesFromTimestamp(ts)
		iter := self.ldb.NewIterator(util.BytesPrefix(epochPrefix), nil)
		for iter.Next() {
			epochkey := iter.Key()
			key := epochkey[8:]
			fmt.Printf("%x\n", key)
			// data, err := self.ldb.Get(key, nil)
			// chunklog, err := json.Marshal(c)
			// sql_readall := fmt.Sprintf("SELECT chunkKey,strftime('%s',chunkBirthDT) as chunkBirthTS, strftime('%s',chunkStoreDT) as chunkStoreTS, maxReplication, renewal FROM chunk where chunkBD >= %d and chunkBD < %d", time.Unix(startTS, 0).Format(time.RFC3339), time.Unix(endTS, 0).Format(time.RFC3339))
		}
		iter.Release()
		err = iter.Error()
	}
	return nil
}

func (self *SwapDB) GenerateSwapLog(startTS int64, endTS int64) (err error) {
	return nil
}
