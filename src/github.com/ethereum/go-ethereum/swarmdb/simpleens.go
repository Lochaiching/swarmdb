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
	// "database/sql"

	"encoding/json"
	"fmt"
    	"io/ioutil"
	_ "github.com/mattn/go-sqlite3"
	"log"
	"path/filepath"

	"strings"
	// "encoding/hex"
	"github.com/ethereum/go-ethereum/accounts/abi/bind"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/ethclient"
	elog "github.com/ethereum/go-ethereum/log"
)

type ENSSimple struct {
	auth *bind.TransactOpts
	sens *Simplestens
}


type ENSSimpleConfig struct{
	Ipaddress	string	`json:"ipaddress,omitempty"`
}

func NewENSSimple(path string) (ens ENSSimple, err error) {
// TODO: using temporary config file
	confdir, err := ioutil.ReadDir("/var/www/vhosts/data/swarmdb")
	var ipaddress string
	ipaddress = "/var/www/vhosts/data/geth.ipc"
	if err == nil{
		var conffilename string
		for _, cf := range confdir{
        		if strings.HasPrefix(cf.Name(), "ens") {
                		conffilename =  cf.Name()
        		}
		}
		fullconf := filepath.Join("/var/www/vhosts/data/swarmdb", conffilename)
		dat, _ := ioutil.ReadFile(fullconf)
		var conf ENSSimpleConfig
		err = json.Unmarshal(dat, &conf)
		ipaddress = conf.Ipaddress
	}
	elog.Debug(fmt.Sprintf("SimpleENS ipaddress = %s", ipaddress))
	
	// Create an IPC based RPC connection to a remote node
	conn, err := ethclient.Dial(ipaddress)


	if err != nil {
		log.Fatalf("Failed to connect to the Ethereum client: %v", err)
	}


// TODO: need to get the dir (or filename) from config
    	files, err := ioutil.ReadDir("/var/www/vhosts/data/keystore")
	var filename string
        for _, file := range files {
        	if strings.HasPrefix(file.Name(), "UTC") {
                	filename =  file.Name()
        	}
	}
        fullpath := filepath.Join("/var/www/vhosts/data/keystore", filename)
	k, err := ioutil.ReadFile(fullpath)
	key := fmt.Sprintf("%s", k)
	
	auth, err := bind.NewTransactor(strings.NewReader(string(key)), "mdotm")

	if err != nil {
		log.Fatalf("Failed to create authorized transactor: %v", err)
	} else {
		ens.auth = auth
	}

	// Instantiate the contract and display its name

	sens, err := NewSimplestens(common.HexToAddress("0x7e29ab7c40aaf6ca52270643b57c46c7766ca31d"), conn)

	if err != nil {
		elog.Debug(fmt.Sprintf("NewSimplestens failed %v", err))
		log.Fatalf("Failed to instantiate a Simplestens contract: %v", err)
	} else {
		elog.Debug(fmt.Sprintf("NewSimplestens success %v", sens))
		ens.sens = sens
	}

	// -------------------
	/*
		db, err := sql.Open("sqlite3", path)
		if err != nil {
			return ens, err
		}
		if db == nil {
			return ens, err
		}
		ens.db = db
		ens.filepath = path

		sql_table := `
		CREATE TABLE IF NOT EXISTS ens (
		indexName TEXT NOT NULL PRIMARY KEY,
		roothash BLOB,
		storeDT DATETIME
		);
		`

		_, err = db.Exec(sql_table)
		if err != nil {
			return ens, err
		}
	*/
	return ens, nil
}

func (self *ENSSimple) StoreRootHash(indexName []byte, roothash []byte) (err error) {
	var i32 [32]byte
	var r32 [32]byte
	copy(i32[0:], indexName)
	copy(r32[0:], roothash)
	elog.Debug(fmt.Sprintf("ENSSimple StoreRootHash %x roothash %x", indexName, roothash))
	fmt.Printf("ENSSimple StoreRootHash %x roothash %x\n", indexName, roothash)

	tx, err2 := self.sens.SetContent(self.auth, i32, r32)
	fmt.Printf("return store %x %v\n", tx, err2)
	if err2 != nil {
		return err // log.Fatalf("Failed to set Content: %v", err2)
	}
	fmt.Printf("i32: %x r32: %x tx: %v\n", i32, r32, tx.Hash())

	/*
		sql_add := `INSERT OR REPLACE INTO ens ( indexName, roothash, storeDT ) values(?, ?, CURRENT_TIMESTAMP)`
		stmt, err := self.db.Prepare(sql_add)
		if err != nil {
			return (err)
		}
		defer stmt.Close()

		_, err2 := stmt.Exec(indexName, roothash)
		if err2 != nil {
			return (err2)
		}
	*/
	elog.Debug(fmt.Sprintf("ENSSimple StoreRootHash %x roothash %x", indexName, roothash))
	return nil
}

func (self *ENSSimple) GetRootHash(indexName []byte) (val []byte, err error) {
	elog.Debug(fmt.Sprintf("ENSSimple GotRootHash %v", indexName))
	
	/*
		sql := `SELECT roothash FROM ens WHERE indexName = $1`
		stmt, err := self.db.Prepare(sql)
		if err != nil {
			return val, err
		}
		defer stmt.Close()

		rows, err := stmt.Query(indexName)
		if err != nil {
			return nil, err
		}
		defer rows.Close()

		for rows.Next() {
			err2 := rows.Scan(&val)
			if err2 != nil {
				return nil, err2
			}
			return val, nil
		}
	*/
	/*b, err := hex.DecodeString("9f5cd92e2589fadd191e7e7917b9328d03dc84b7a67773db26efb7d0a4635677")
	if err != nil {
		log.Fatalf("Failed to hexify %v", err)
	} */
	var b2 [32]byte
	copy(b2[0:], indexName)
	//s, err := sens.Content(b)
	s, err := self.sens.Content(nil, b2)
	if err != nil {
		elog.Debug(fmt.Sprintf("ENSSimple GotRootHash err %v %v", indexName, err))
		fmt.Printf("GetContent failed:  %v", err)
		return val, err
	}
	val = make([]byte, 32)
	for i := range s {
		val[i] = s[i]
		if i == 31 {
			break
		}
	}
	//copy(val[0:], s[0:32])
	fmt.Printf("indexName: [%x] => s: [%x] val: [%x]\n", indexName, s, val)
	elog.Debug(fmt.Sprintf("ENSSimple GotRootHash %x s %x val %x", indexName, s, val))
	return val, nil
}
