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
	_ "github.com/mattn/go-sqlite3"
	"github.com/syndtr/goleveldb/leveldb"
)

const (
	minChunkSize = 4000
)

type DBChunkstore struct {
	ldb      *leveldb.DB
	km       *KeyManager
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

func NewDBChunkStore(path string) (self *DBChunkstore, err error) {
	ldb, err := leveldb.OpenFile(path, nil)
	if err != nil {
		return self, err
	}

	config, errConfig := LoadSWARMDBConfig(SWARMDBCONF_FILE)
	if errConfig != nil {
		return nil, GenerateSWARMDBError(errConfig, fmt.Sprintf("[dbchunkstore:NewDBChunkStore] LoadSWARMDBConfig - KeyManager Config Loading %s", errConfig.Error()))
	}
	km, errKM := NewKeyManager(&config)
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
	/*
		recordData := val[KNODE_START_ENCRYPTION : 4096-41] //MAJOR TODO: figure out how we pass in to ensure <=4096
		if encrypted == 1 {
			recordData = self.km.EncryptData(u, recordData)
			//rev,_ := self.km.DecryptData(u, recordData)
			log.Debug(fmt.Sprintf("Key: [%x][%v] Encrypted : %s [%v]", key, key, recordData, recordData))
			//log.Debug(fmt.Sprintf("Key: [%x][%v] Decrypted : %s [%v]", key, key, rev, rev))
		}

		var finalSdata [4096]byte
		log.Debug(fmt.Sprintf("Key: [%x][%v] After Loop recordData length (%d) and start pos %d", key, key, len(recordData), KNODE_START_ENCRYPTION))
		copy(finalSdata[0:KNODE_START_ENCRYPTION], val[0:KNODE_START_ENCRYPTION])
		copy(finalSdata[KNODE_START_ENCRYPTION:4096], recordData)

		if err2 != nil {
			return &SWARMDBError{message: fmt.Sprintf("[dbchunkstore:StoreKChunk] Exec - Insert%s | data:%x | Encrypted: %s ", err2.Error(), finalSdata, encrypted), ErrorCode: 439, ErrorMessage: "Failure storing K node Chunk"}
		}
	*/
	return nil
}

func (self *DBChunkstore) RetrieveKChunk(u *SWARMDBUser, key []byte) (val []byte, err error) {
	/*
		var kV []byte
		var bdt []byte
		var sdt []byte
		var enc int

		jsonRecord := val[KNODE_START_ENCRYPTION:]
		trimmedJson := bytes.TrimRight(jsonRecord, "\x00")
		var retVal []byte
		retVal = trimmedJson
		if enc == 1 {
			retVal, err = self.km.DecryptData(u, trimmedJson)
			if err != nil {
				log.Debug(fmt.Sprintf("ERROR when Decrypting Data : %s", err.Error()))
				return val, GenerateSWARMDBError(err, fmt.Sprintf("[dbchunkstore:RetrieveKChunk] DecryptData %s", err.Error()))
			}

			retVal = bytes.TrimRight(retVal, "\x00")
		}
	*/
	return val, nil
}

func (self *DBChunkstore) StoreChunk(u *SWARMDBUser, val []byte, encrypted int) (key []byte, err error) {

	if len(val) < minChunkSize {
		return nil, &SWARMDBError{message: fmt.Sprintf("[dbchunkstore:StoreChunk] Chunk too small (< %s)| %x", minChunkSize, val), ErrorCode: 439, ErrorMessage: "Unable to Store Chunk"}
	}
	log.Debug(fmt.Sprintf("StoreChunk: Encrypted bit when saving was: %d", encrypted))
	inp := make([]byte, minChunkSize)
	copy(inp, val[0:minChunkSize])
	h := sha256.New()
	h.Write([]byte(inp))
	key = h.Sum(nil)

	var chunk DBChunk
	if encrypted > 0 {
		// chunkBirthDT, chunkStoreDT, renewal, minReplication, maxReplication, payer, version
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
	return key, nil
}

func (self *DBChunkstore) RetrieveChunk(u *SWARMDBUser, key []byte) (val []byte, err error) {

	data, err := self.ldb.Get(key, nil)
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

func (self *SwapDB) GenerateSwapLog(startTS int64, endTS int64) (err error) {
	/*
		sql_readall := fmt.Sprintf("SELECT swapID, sender, beneficiary, amount, sig FROM swap where checkBirthDT >= %s and checkBirthDT < %s", time.Unix(startTS, 0).Format(time.RFC3339), time.Unix(endTS, 0).Format(time.RFC3339))
		rows, err := self.db.Query(sql_readall)
		if err != nil {
			return &SWARMDBError{message: fmt.Sprintf("[swapdb:GenerateSwapLog] Query %s", err.Error())}
		}
		defer rows.Close()

		var result []SwapLog
		for rows.Next() {
			c := SwapLog{}
			err = rows.Scan(&c.SwapID, &c.Sender, &c.Beneficiary, &c.Amount, &c.Sig)
			if err != nil {
				return &SWARMDBError{message: fmt.Sprintf("[swapdb:GenerateSwapLog] Scan %s", err.Error())}
			}

			l, err2 := json.Marshal(c)
			if err != nil {
				return &SWARMDBError{message: fmt.Sprintf("[swapdb:GenerateSwapLog] Marshal %s", err2.Error())}
			}
			fmt.Printf("%s\n", l)
			result = append(result, c)
		}
		rows.Close()
	*/
	return nil
}

func (self *DBChunkstore) GenerateBuyerLog(startTS int64, endTS int64) (err error) {
	/*
		farmerAddr := self.farmer.Hex()
		sql_readall := fmt.Sprintf("SELECT chunkKey,strftime('%s',chunkBirthDT) as chunkBirthTS, strftime('%s',chunkStoreDT) as chunkStoreTS, maxReplication, renewal FROM chunk where chunkBD >= %d and chunkBD < %d", time.Unix(startTS, 0).Format(time.RFC3339), time.Unix(endTS, 0).Format(time.RFC3339))
		rows, err := self.db.Query(sql_readall)
		if err != nil {
			return &SWARMDBError{message: fmt.Sprintf("[dbchunkstore:GenerateFarmerLog] Query %s", err.Error()), ErrorCode: 468, ErrorMessage: "Error Generating Farmer Log"}
		}
		defer rows.Close()

		var result []ChunkLog
		for rows.Next() {
			c := ChunkLog{}
			c.Farmer = farmerAddr

			err2 := rows.Scan(&c.ChunkHash, &c.ChunkBD, &c.ChunkSD, &c.ReplicationLevel, &c.Renewable)
			if err2 != nil {
				return &SWARMDBError{message: fmt.Sprintf("[dbchunkstore:GenerateFarmerLog] Scan %s", err2.Error()), ErrorCode: 468, ErrorMessage: "Error Generating Farmer Log"}
			}
			c.ChunkID = fmt.Sprintf("%x", c.ChunkHash)
			chunklog, err := json.Marshal(c)
			if err != nil {
				return &SWARMDBError{message: fmt.Sprintf("[dbchunkstore:GenerateFarmerLog] Marshal %s", err.Error()), ErrorCode: 468, ErrorMessage: "Error Generating Farmer Log"}
			}
			if false {
				fmt.Printf("%s\n", chunklog)
			}
			result = append(result, c)
		}
		rows.Close()
	*/
	return nil
}

func (self *DBChunkstore) GenerateFarmerLog(startTS int64, endTS int64) (err error) {
	/*
		farmerAddr := self.farmer.Hex()
		sql_readall := fmt.Sprintf("SELECT chunkKey FROM chunk where chunkBD >= %s and chunkBD < %s", time.Unix(startTS, 0).Format(time.RFC3339), time.Unix(endTS, 0).Format(time.RFC3339))
		rows, err := self.db.Query(sql_readall)
		if err != nil {
			return &SWARMDBError{message: fmt.Sprintf("[dbchunkstore:GenerateFarmerLog] Query %s", err.Error()), ErrorCode: 468, ErrorMessage: "Error Generating Farmer Log"}
		}
		defer rows.Close()

		var result []ChunkLog
		for rows.Next() {
			c := ChunkLog{}
			c.Farmer = farmerAddr

			err2 := rows.Scan(&c.ChunkHash, &c.ChunkBD, &c.ChunkSD, &c.ReplicationLevel, &c.Renewable)
			if err2 != nil {
				return &SWARMDBError{message: fmt.Sprintf("[dbchunkstore:GenerateFarmerLog] Scan %s", err2.Error()), ErrorCode: 468, ErrorMessage: "Error Generating Farmer Log"}
			}
			c.ChunkID = fmt.Sprintf("%x", c.ChunkHash)
			chunklog, err := json.Marshal(c)
			if err != nil {
				return &SWARMDBError{message: fmt.Sprintf("[dbchunkstore:GenerateFarmerLog] Marshal %s", err.Error()), ErrorCode: 468, ErrorMessage: "Error Generating Farmer Log"}
			}
			if false {
				fmt.Printf("%s\n", chunklog)
			}
			result = append(result, c)
		}
		rows.Close()
	*/
	return nil
}
