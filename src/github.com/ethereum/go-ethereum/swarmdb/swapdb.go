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
	// "bytes"
	"database/sql"
	"github.com/ethereum/go-ethereum/crypto"
	//"encoding/binary"
	"fmt"
	"github.com/ethereum/go-ethereum/common"
	_ "github.com/mattn/go-sqlite3"
	// "io/ioutil"
	// "math/big"
	// "os"
	// "time"
)

type SwapCheck struct {
	SwapID      []byte
	Sender      common.Address // address of sender
	Beneficiary common.Address
	Amount      int
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

type SwapDB struct {
	db       *sql.DB
	filepath string
}

// path = "swap.db"
func NewSwapDB(path string) (self *SwapDB, err error) {
	// ts := time.Now()
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

	self = &SwapDB{
		db:       db,
		filepath: path,
	}
	return self, nil
}

// Issue creates a new signed by the farmer's private key for the beneficiary and amount
func (self *SwapDB) Issue(km *KeyManager, u *SWARMDBUser, beneficiary common.Address, amount int) (ch *SwapCheck, err error) {
	// defer self.lock.Unlock()
	// self.lock.Lock()
	if amount < 0 {
		return ch, &SWARMDBError{message: fmt.Sprintf("[swapdb:Issue] Check Amount must be positive %d", amount)}
	}

	// compute the swapID = Keccak256(sender, beneficiary, amount, timestamp ...)
	amount8 := IntToByte(amount)
	var raw []byte
	raw = make([]byte, 88)
	copy(raw[0:], []byte(u.Address))
	copy(raw[40:], beneficiary.Bytes())
	copy(raw[80:], amount8)
	swapID := crypto.Keccak256(raw)

	var sig []byte
	sig, err = km.SignMessage(swapID)
	if err != nil {
		return ch, &SWARMDBError{message: fmt.Sprintf("[swapdb:Issue] SignMessage %s", err.Error())}
	} else {
		// insert into
		sql_add := `INSERT OR REPLACE INTO swap ( swapID, sender, beneficiary, amount, sig, checkBirthDT) values(?, ?, ?, ?, ?, CURRENT_TIMESTAMP)`
		stmt, err := self.db.Prepare(sql_add)
		if err != nil {
			return ch, &SWARMDBError{message: fmt.Sprintf("[swapdb:Issue] Prepare %s", err.Error())}
		}
		defer stmt.Close()

		swapID_str := fmt.Sprintf("%x", swapID)
		sender_str := fmt.Sprintf("%x", u.Address)
		beneficiary_str := fmt.Sprintf("%x", beneficiary)
		amount_int := 31415 // amount
		sig_str := fmt.Sprintf("%x", swapID)
		/*
			l := &SwapLog {
				SwapID: swapID_str,
				Sender:  sender_str,
				Beneficiary: beneficiary_str,
				Amount:      amount_int,
				Sig: sig_str,
			}
		*/
		_, err = stmt.Exec(swapID_str, sender_str, beneficiary_str, amount_int, sig_str)
		if err != nil {
			return ch, &SWARMDBError{message: fmt.Sprintf("[swapdb:Issue] Exec %s", err.Error())}
		}
		stmt.Close()

		ch = &SwapCheck{
			SwapID:      swapID,
			Sender:      common.HexToAddress(u.Address),
			Beneficiary: beneficiary,
			Amount:      amount,
			Sig:         sig,
		}
		return ch, nil
	}
}

