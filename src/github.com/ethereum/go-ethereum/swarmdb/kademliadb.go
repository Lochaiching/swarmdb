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
	"github.com/ethereum/go-ethereum/log"
)

// TODO: document this

const (
	chunkSize = 4096
)

func NewKademliaDB(dbChunkstore *DBChunkstore) (*KademliaDB, error) {
	kd := new(KademliaDB)
	kd.dbChunkstore = dbChunkstore
	kd.nodeType = []byte("K")
	return kd, nil
}

func (self *KademliaDB) Open(owner []byte, tableName []byte, column []byte, encrypted int) (bool, error) {
	self.owner = owner
	self.tableName = tableName
	self.column = column
	self.encrypted = encrypted

	return true, nil
}

func (self *KademliaDB) buildSdata(key []byte, value []byte) (mergedBodycontent []byte, err error) {
	contentPrefix := BuildSwarmdbPrefix(self.owner, self.tableName, key)

	var metadataBody []byte
	metadataBody = make([]byte, 156)
	copy(metadataBody[0:40], self.owner)
	copy(metadataBody[40:41], self.nodeType)
	copy(metadataBody[41:42], IntToByte(self.encrypted))
	copy(metadataBody[42:43], IntToByte(self.autoRenew))
	copy(metadataBody[43:51], IntToByte(self.minReplication))
	copy(metadataBody[51:59], IntToByte(self.maxReplication))

	unencryptedMetadata := metadataBody[0:59]
	msg_hash := SignHash(unencryptedMetadata)

	copy(metadataBody[59:91], msg_hash)

	km := self.dbChunkstore.GetKeyManager()
	sdataSig, errSign := km.SignMessage(msg_hash)
	if errSign != nil {
		return mergedBodycontent, &SWARMDBError{message: `[kademliadb:buildSdata] SignMessage ` + errSign.Error()}
	}

	// TODO: document this
	copy(metadataBody[91:156], sdataSig)
	log.Debug("Metadata is [%+v]", metadataBody)

	mergedBodycontent = make([]byte, chunkSize)
	copy(mergedBodycontent[:], metadataBody)
	copy(mergedBodycontent[512:544], contentPrefix)
	copy(mergedBodycontent[577:], value) // expected to be the encrypted body content

	log.Debug("Merged Body Content: [%v]", mergedBodycontent)
	return mergedBodycontent, err
}

func (self *KademliaDB) Put(u *SWARMDBUser, k []byte, v []byte) (b []byte, err error) {
	self.autoRenew = u.AutoRenew
	self.minReplication = u.MinReplication
	self.maxReplication = u.MaxReplication
	sdata, errS := self.buildSdata(k, v)
	if errS != nil {
		return b, &SWARMDBError{message: `[kademliadb:Put] buildSdata ` + errS.Error()}
	}

	hashVal := sdata[512:544] // 32 bytes
	log.Debug(fmt.Sprintf("Kademlia Encrypted Bit: %d", self.encrypted))
	errStore := self.dbChunkstore.StoreKChunk(u, hashVal, sdata, self.encrypted)
	if errStore != nil {
		return hashVal, GenerateSWARMDBError(err, `[kademliadb:Put] StoreKChunk `+errStore.Error())
	}
	return hashVal, nil
}

func (self *KademliaDB) GetByKey(u *SWARMDBUser, k []byte) ([]byte, error) {
	chunkKey := self.GenerateChunkKey(k)
	content, err := self.Get(u, chunkKey)
	if err != nil {
		return nil, &SWARMDBError{message: fmt.Sprintf("[kademliadb:GetByKey] Get - Key not found: %s", err.Error())}
	}
	return content, nil
}

func (self *KademliaDB) Get(u *SWARMDBUser, h []byte) ([]byte, error) {
	contentReader, err := self.dbChunkstore.RetrieveKChunk(u, h)
	if bytes.Trim(contentReader, "\x00") == nil {
		return nil, nil
	}
	if err != nil {
		return nil, &SWARMDBError{message: fmt.Sprintf("[kademliadb:Get] RetrieveKChunk - Key not found: %s", err.Error())}
	}
	return contentReader, nil
}

func (self *KademliaDB) GenerateChunkKey(k []byte) []byte {
	owner := self.owner
	table := self.tableName
	id := k
	contentPrefix := BuildSwarmdbPrefix(owner, table, id)
	log.Debug(fmt.Sprintf("\nIn GenerateChunkKey prefix Owner: [%s] Table: [%s] ID: [%s] == [%v](%s)", owner, table, id, contentPrefix, contentPrefix))
	return contentPrefix
}

func BuildSwarmdbPrefix(owner []byte, table []byte, id []byte) []byte {
	// TODO: add checks for valid type / length for building
	prepLen := len(owner) + len(table) + len(id)
	prepBytes := make([]byte, prepLen)
	copy(prepBytes[0:], owner)
	copy(prepBytes[len(owner):], table)
	copy(prepBytes[len(owner)+len(table):], id)
	h256 := sha256.New()
	h256.Write([]byte(prepBytes))
	prefix := h256.Sum(nil)

	log.Debug(fmt.Sprintf("\nIn BuildSwarmdbPrefix prepstring[%s] and prefix[%s] in Bytes [%v] with size [%v]", prepBytes, prefix, []byte(prefix), len([]byte(prefix))))
	return (prefix)
}

func (self *KademliaDB) Close() (bool, error) {
	return true, nil
}

func (self *KademliaDB) FlushBuffer() (bool, error) {
	return true, nil
}

func (self *KademliaDB) StartBuffer() (bool, error) {
	return true, nil
}

func (self *KademliaDB) Print() {
	return
}

// TODO: Implement Delete
func (self *KademliaDB) Delete(k []byte) (succ bool, err error) {
	/*
		_, err := self.Put(k, nil)
		if err != nil {
			return false, err
		}
		return true, err
	*/
	return succ, err
}
