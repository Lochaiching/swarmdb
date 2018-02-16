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
	"database/sql"
	"sync"
	"github.com/ethereum/go-ethereum/crypto"
	// "encoding/binary"
	"encoding/json"
	"fmt"
	"github.com/ethereum/go-ethereum/common"
	_ "github.com/mattn/go-sqlite3"
	// "io/ioutil"
	 "math/big"
	// "os"
	"time"
	"github.com/ethereum/go-ethereum/log"
)

type SwapCheck struct {
	SwapID      []byte
	Sender      common.Address // address of sender
	Beneficiary common.Address
	Amount      *big.Int
	Timestamp   []byte
	Sig         []byte // signature Sign(Keccak256(contract, beneficiary, amount), prvKey)
}

type SwapLog struct {
	SwapID      string `json:"swapID"`
	Sender      string `json:"sender"`
	Beneficiary string `json:"beneficiary"`
	Amount      int    `json:"amount"`
	Sig         string `json:"sig"` // of sender or beneficiary
	CheckBD     int    `json:"chunkSD"`
}

type Promise interface{}

// interface for the peer protocol for testing or external alternative payment
type Protocol interface {
	SDBPay(int, Promise) // units, payment proof
	Drop()
	String() string
}

type SwapDB struct {
	lock    sync.Mutex // mutex for balance access
	balance int        // units of chunk/retrieval request	
	db       *sql.DB
	filepath string
	proto   Protocol   // peer communication protocol
	remotePayAt  uint   // remote peer's PayAt
	localAddress common.Address
	peerAddress common.Address
}

// path = "/tmp/swap.db"
func NewSwapDB(path string, proto Protocol, remotePayAt uint, localAddress common.Address, peerAddress common.Address) (self *SwapDB, err error) {
	db, err := sql.Open("sqlite3", path)
	if err != nil || db == nil {
		return nil, &SWARMDBError{message: fmt.Sprintf("[swapdb:NewSwapDB] Open %s", err.Error())}
	}

	//Local Chunk table
	sql_table := `
    CREATE TABLE IF NOT EXISTS swap (
    swapID TEXT NOT NULL PRIMARY KEY,
    sender TEXT,
    beneficiary TEXT,
    amount INTEGER DEFAULT 1,
    sig    TEXT,
    checkBirthDT DATETIME
    );
    `
	_, err = db.Exec(sql_table)
	if err != nil {
		return nil, &SWARMDBError{message: fmt.Sprintf("[swapdb:NewSwapDB] Exec - SQLite Chunk Table Creation %s", err.Error())}
	}

	localAddressHex := localAddress.Hex()
	peerAddressHex := peerAddress.Hex()
	log.Debug(fmt.Sprintf("[wolk-cloudstore] swapdb.NewSwapDB Beneficiary: local: %v peer: %v", localAddressHex, peerAddressHex))

	self = &SwapDB{
		balance:   0,
		db:       db,
		filepath: path,
		proto:   proto,
		//remotePayAt:  remotePayAt,
		remotePayAt:  3,
		localAddress: localAddress,
		peerAddress: peerAddress,
	}

	return self, nil
}

// Add(n)
// n > 0 called when promised/provided n units of service
// n < 0 called when used/requested n units of service
func (self *SwapDB) Add(n int) error {
	log.Debug(fmt.Sprintf("[wolk-cloudstore] swapdb.Add amount: %v" , n))
	defer self.lock.Unlock()
	self.lock.Lock()
	self.balance += n
	log.Debug(fmt.Sprintf("[wolk-cloudstore] swapdb.Add self.balance: %v self.remotePayAt: %v" , self.balance, self.remotePayAt))
	if self.balance <= -int(self.remotePayAt) {
		self.Issue()
	}
	return nil
}

// Issue creates a new signed by the farmer's private key for the beneficiary and amount
//func (self *SwapDB) Issue(km *KeyManager, u *SWARMDBUser, beneficiary common.Address, amount int) (ch *SwapCheck, err error) {
func (self *SwapDB) Issue() (err error) {
	localAddressHex := self.localAddress.Hex()
	peerAddressHex := self.peerAddress.Hex()
	amount := self.balance
	log.Debug(fmt.Sprintf("[wolk-cloudstore] swapdb.Issue local: %v peer: %v self.balance: %v" , localAddressHex, peerAddressHex, amount))

//	defer self.lock.Unlock()
//	self.lock.Lock()
/*	
	if amount < 0 {
		return ch, &SWARMDBError{message: fmt.Sprintf("[swapdb:Issue] Check Amount must be positive %d", amount)}
	}
*/
	// compute the swapID = Keccak256(sender, beneficiary, amount, timestamp ...)
	timestamp := time.Now()	
	timestampStr:= timestamp.String()                   // 2009-11-10 23:00:00 +0000 UTC m=+0.000000001
	timestampSubstring := string(timestampStr[0:19])    // 2009-11-10 23:00:00
	timestampByte := []byte(timestampSubstring)         // [50 48 48 57 45 49 49 45 49 48 32 50 51 58 48 48 58 48 48]      size 19 byte
	
	amount8 := IntToByte(amount)
	var raw []byte
	raw = make([]byte, 67)
	copy(raw[0:], self.localAddress[:20])
	copy(raw[20:], self.peerAddress[:20])
	copy(raw[40:], amount8[:8])
	copy(raw[48:], timestampByte[:19])	
	swapID := crypto.Keccak256(raw)
	
	var sig []byte
	sig = []byte{49, 50, 51} // 1 2 3	
	log.Debug(fmt.Sprintf("[wolk-cloudstore] swapdb.Issue swapID: %v sender: %v beneficiary: %v amount: %v  sig: %v", swapID, self.localAddress, self.peerAddress, amount, sig))

	//sig, err = km.SignMessage(swapID)
	//if err != nil {
	//	return ch, &SWARMDBError{message: fmt.Sprintf("[swapdb:Issue] SignMessage %s", err.Error())}
	//} else {
		
		// insert into
		sql_add := `INSERT OR REPLACE INTO swap ( swapID, sender, beneficiary, amount, sig, checkBirthDT) values(?, ?, ?, ?, ?, CURRENT_TIMESTAMP)`
		stmt, err := self.db.Prepare(sql_add)
		if err != nil {
			return &SWARMDBError{message: fmt.Sprintf("[swapdb:Issue] Prepare %s", err.Error())}
		}
		defer stmt.Close()

		swapID_str := fmt.Sprintf("%x", swapID)
		sender_str := fmt.Sprintf("%x", self.localAddress)
		beneficiary_str := fmt.Sprintf("%x", self.peerAddress)
		amount_int := amount 
		sig_str := fmt.Sprintf("%x", sig)

		//	l := &SwapLog {
		//		SwapID: swapID_str,
		//		Sender:  sender_str,
		//		Beneficiary: beneficiary_str,
		//		Amount:      amount_int,
		//		Sig: sig_str,
		//	}

		_, err = stmt.Exec(swapID_str, sender_str, beneficiary_str, amount_int, sig_str)
		if err != nil {
			return &SWARMDBError{message: fmt.Sprintf("[swapdb:Issue] Exec %s", err.Error())}
		}
		stmt.Close()
		/*
		ch = &SwapCheck{
			SwapID:      swapID,
			Sender:      common.HexToAddress(u.Address),
			Beneficiary: beneficiary,
			Amount:      amount,
			Sig:         sig,
		}
		return ch, nil
		*/

		amountB := big.NewInt(int64(-self.balance))
		ch := &SwapCheck{
		 	SwapID:      swapID, // []byte
			Sender:      self.localAddress, // common.Address // address of sender
			Beneficiary: self.peerAddress, // common.Address
			Amount:      amountB, // int
			Timestamp:   timestampByte,   //size 19 byte
			Sig:         sig, // []byte // signature Sign(Keccak256(contract, beneficiary, amount), prvKey)
		}

		self.proto.SDBPay(-amount, ch)
		
		log.Debug(fmt.Sprintf("[wolk-cloudstore] swapdb.Issue self.balance: %v", self.balance))		
		self.balance = self.balance - amount
		log.Debug(fmt.Sprintf("[wolk-cloudstore] swapdb.Issue self.balance: %v", self.balance))		
		return  nil
	//}
}

// receive(units, promise) is called by the protocol when a payment msg is received
// returns error if promise is invalid.
func (self *SwapDB) Receive(units int, promise Promise) error {
	
	ch := promise.(*SwapCheck)
	
 	swapIDIssue := ch.SwapID        // []byte
	sender := ch.Sender       // common.Address // address of sender
	beneficiary := ch.Beneficiary // common.Address
	amountB := ch.Amount       // big.NewIn
	timestamp := ch.Timestamp    //size 19 byte
	sig := ch.Sig
	
	amount8 := IntToByte(-units)
	var raw []byte
	raw = make([]byte, 67)
	copy(raw[0:], sender[:20])
	copy(raw[20:], beneficiary[:20])
	copy(raw[40:], amount8[:8])
	copy(raw[48:], timestamp[:19])	
	swapIDReceive := crypto.Keccak256(raw)
	
	price := big.NewInt(int64(units))
	 
	if price.Cmp(amountB) != 0 {
		return &SWARMDBError{message: fmt.Sprintf("[swapdb:Receive] units != amount")}
	} else { 
		//if (sig != sig) {    // TO DO change from swapID to sig
		if (bytes.Equal(swapIDIssue, swapIDReceive) ) { 	
	
			log.Debug(fmt.Sprintf("[wolk-cloudstore] swapdb.Receive swapID: %v sender: %v beneficiary: %v amount: %v  sig: %v", swapIDReceive, sender, beneficiary, amountB, sig))
			 
			// insert into
			sql_add := `INSERT OR REPLACE INTO swap ( swapID, sender, beneficiary, amount, sig, checkBirthDT) values(?, ?, ?, ?, ?, CURRENT_TIMESTAMP)`
			stmt, err := self.db.Prepare(sql_add)
			if err != nil {
				return &SWARMDBError{message: fmt.Sprintf("[swapdb:Issue] Prepare %s", err.Error())}
			}
			defer stmt.Close()
	
			swapID_str := fmt.Sprintf("%x", swapIDReceive)
			sender_str := fmt.Sprintf("%x", sender)
			beneficiary_str := fmt.Sprintf("%x", beneficiary)
			amount_int := units // int
			sig_str := fmt.Sprintf("%x", sig)
			 
			//	l := &SwapLog {
			//		SwapID: swapID_str,
			//		Sender:  sender_str,
			//		Beneficiary: beneficiary_str,
			//		Amount:      amount_int,
			//		Sig: sig_str,
			//	}
			 
			_, err = stmt.Exec(swapID_str, sender_str, beneficiary_str, amount_int, sig_str)
			if err != nil {
				return &SWARMDBError{message: fmt.Sprintf("[swapdb:Issue] Exec %s", err.Error())}
			}
			stmt.Close()
	
			log.Debug(fmt.Sprintf("[wolk-cloudstore] swapdb.Receive self.balance: %v", self.balance))
			self.balance = self.balance - units
			log.Debug(fmt.Sprintf("[wolk-cloudstore] swapdb.Receive self.balance: %v", self.balance))
			
		} else {
			return &SWARMDBError{message: fmt.Sprintf("[swapdb:Receive] sig != sig")}
		}
	}
	return nil
}


//func (self *SwapDB) GenerateSwapLog(u *SWARMDBUser) (err error) {
func (self *SwapDB) GenerateSwapLog() (err error) {
	//sql_readall := `SELECT swapID, sender, beneficiary, amount, sig FROM swap`
	//rows, err := self.db.Query(sql_readall)
	db, err := sql.Open("sqlite3", "/tmp/swap.db")
	rows, err := db.Query("SELECT swapID, sender, beneficiary, amount, sig FROM swap")
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
		if err2 != nil {
			return &SWARMDBError{message: fmt.Sprintf("[swapdb:GenerateSwapLog] Marshal %s", err2.Error())}

		}
		
		fmt.Printf("%s\n", l)
		result = append(result, c)
	}
	rows.Close()
	return nil
}
