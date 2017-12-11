package common

import (
	//	"fmt"
	"strconv"
	"encoding/json"
	//	"sync"
	"bytes"
	// leaf "github.com/ethereum/go-ethereum/swarmdb/leaf"
)


func NewSwarmDB() *SwarmDB {
	sd := new(SwarmDB)

	// ownerID, tableName => *Table
	sd.tables = make(map[string]map[string]*Table)

	dbchunkstore, err := NewDBChunkStore("/tmp/chunk.db")
	if err != nil {
		// TODO: PANIC
	} else {
		sd.dbchunkstore = dbchunkstore
	}

	ens, err := NewENSSimulation("/tmp/ens.db")
	if err != nil {
		// TODO: PANIC
	} else {
		sd.ens = ens
	}

	kaddb, err := NewKademliaDB(sd)
	if err != nil {
	} else {
		sd.kaddb = kaddb
	}

	return sd
}


// DBChunkStore  API
func (self *SwarmDB) RetrieveKDBChunk(key []byte) (val []byte, err error) {
	return self.dbchunkstore.RetrieveKChunk(key)
}

func (self *SwarmDB) StoreKDBChunk(key []byte, val []byte) (err error) {
	return self.dbchunkstore.StoreKChunk(key, val)
}

func (self *SwarmDB) PrintDBChunk(keytype KeyType, hashid []byte, c []byte) {
	self.dbchunkstore.PrintDBChunk(keytype, hashid, c)
}

// ENSSimulation  API
func (self *SwarmDB) GetIndexRootHash(indexName []byte) (roothash []byte, err error) {
	return self.ens.GetIndexRootHash(indexName)
}

func (self *SwarmDB) StoreIndexRootHash(indexName []byte, roothash []byte) (err error) {
	return self.ens.StoreIndexRootHash(indexName, roothash)
}

// Table
func (self SwarmDB) NewTable(tablename string) *Table {
	t := new(Table)
	t.swarmdb = self
	t.tablename = tablename
	t.indexes = make(map[string]*IndexInfo)
	return t
}

func (t *Table) CreateTable(tablename string, option []TableOption) (err error) {
	buf := make([]byte, 4096)
	for i, columninfo := range option {
		copy(buf[2048+i*64:], columninfo.Index)
		copy(buf[2048+i*64+26:], strconv.Itoa(columninfo.Primary))
		copy(buf[2048+i*64+27:], "9")
		copy(buf[2048+i*64+28:], strconv.Itoa(columninfo.KeyType))
		copy(buf[2048+i*64+30:], columninfo.TreeType)
	}
	// need to store KDB??
	swarmhash, err := t.swarmdb.StoreDBChunk(buf)
	if err != nil {
		return
	}
	err = t.swarmdb.StoreIndexRootHash([]byte(tablename), []byte(swarmhash))
	return err
}

func (t *Table) OpenTable(tablename string) (err error) {
	t.indexes = make(map[string]*IndexInfo)
	/// get Table RootHash to  retrieve the table descriptor
	roothash, err := t.swarmdb.GetIndexRootHash([]byte(t.tablename))
	if err != nil {
		return err
	}
	setprimary := false
	indexdata, err := t.swarmdb.RetrieveDBChunk(roothash)
	if err != nil {
		return err
	}
	indexbuf := indexdata
	for i := 2048; i < 4096; i = i + 64 {
		//    if
		buf := make([]byte, 64)
		copy(buf, indexbuf[i:i+64])
		if buf[0] == 0 {
			break
		}
		indexinfo := new(IndexInfo)
		indexinfo.indexname = string(bytes.Trim(buf[:25], "\x00"))
		indexinfo.primary, _ = strconv.Atoi(string(buf[26:27]))
		indexinfo.active, _ = strconv.Atoi(string(buf[27:28]))
		indexinfo.keytype, _ = strconv.Atoi(string(buf[28:29]))
		indexinfo.indextype = string(buf[30:32])
		copy(indexinfo.roothash, buf[31:63])
		switch indexinfo.indextype {
		case "BT":
			indexinfo.dbaccess = NewBPlusTreeDB(t.swarmdb, indexinfo.roothash, KeyType(indexinfo.keytype))
			if err != nil {
				return err
			}
		case "HD":
			indexinfo.dbaccess, err = NewHashDB(indexinfo.roothash, t.swarmdb)
			if err != nil {
				return err
			}
		}
		t.indexes[indexinfo.indexname] = indexinfo
		if indexinfo.primary == 1 {
			if !setprimary {
				t.primary = indexinfo.indexname
			} else {
				var rerr *RequestFormatError
				return rerr
			}
		}
	}
	return nil
}

//Owner: X Table: contacts Index: Email Key: rodney@wolk.com VAL: { age: 20, loc: "sm", email: "rodney@wolk.com" }
func (t *Table) Put(value string) (err error) {
	/// store value to kdb and get a hash
	var evalue interface{}
	if err := json.Unmarshal([]byte(value), &evalue); err != nil {
		//return err
	}
	pvalue := evalue.(map[string]interface{})[t.primary]
	
	t.swarmdb.kaddb.Open([]byte(t.ownerID), []byte(t.tablename), []byte(t.primary))
	khash, err := t.swarmdb.kaddb.Put([]byte(pvalue.(string)), []byte(value))

	// PRIMARY INDEX ONLY -- need to put every indexes but currently added only for the primary index
	_, err = t.indexes[t.primary].dbaccess.Put([]byte(pvalue.(string)), []byte(khash))
	return err
}

func (t *Table) Insert(key, value string) error {
	index := t.primary
	/// store value to kdb and get a hash
	_, b, err := t.indexes[index].dbaccess.Get([]byte(key))
	if b {
		var derr *DuplicateKeyError
		return derr
	}
	if err != nil {
		return err
	}

	t.swarmdb.kaddb.Open([]byte(t.ownerID), []byte(t.tablename), []byte(index))

	khash, err := t.swarmdb.kaddb.Put([]byte(key), []byte(key))
	if err != nil {
		return err
	}
	_, err = t.indexes[index].dbaccess.Insert([]byte(key), []byte(khash))
	return err
}

func (t *Table) Get(key string) ([]byte, error) {
	index := t.primary
	if t.indexes[index] == nil {
		var cerr *NoColumnError
		return nil, cerr
	}
	_, _, err := t.indexes[index].dbaccess.Get([]byte(key))
	if err != nil {
		return nil, err
	}

	// get value from kdb
	kres, _, _ := t.swarmdb.kaddb.GetByKey([]byte(key))
	fres := bytes.Trim(kres, "\x00")
	return fres, err
}

func (t *Table) Delete(key string) (err error) {
	for _, ip := range t.indexes {
		_, err := ip.dbaccess.Delete([]byte(key))
		if err != nil {
			return err
		}
	}
	return nil
}

func (t *Table) StartBuffer() (err error) {
	for _, ip := range t.indexes {
		_, err := ip.dbaccess.StartBuffer()
		if err != nil {
			return err
		}
	}
	return nil
}

func (t *Table) FlushBuffer() (err error) {
	for _, ip := range t.indexes {
		_, err := ip.dbaccess.FlushBuffer()
		if err != nil {
			return err
		}
		roothash, err := ip.dbaccess.GetRootHash()
		indexname := t.tablename + ":" + ip.indexname
		ip.roothash = roothash
		err = t.swarmdb.StoreIndexRootHash([]byte(indexname), roothash)
		if err != nil {
			return err
		}
	}
	err = t.updateTableInfo()
	return err
}

func (t *Table) updateTableInfo() (err error) {
	buf := make([]byte, 4096)
	i := 0
	for idx, ivalue := range t.indexes {
		copy(buf[2048+i*64:], idx)
		copy(buf[2048+i*64+26:], strconv.Itoa(ivalue.primary))
		copy(buf[2048+i*64+27:], strconv.Itoa(ivalue.active))
		copy(buf[2048+i*64+28:], strconv.Itoa(ivalue.keytype))
		copy(buf[2048+i*64+30:], ivalue.indextype)
		copy(buf[2048+i*64+30:], ivalue.roothash)
		i++
	}
	swarmhash, err := t.swarmdb.StoreDBChunk(buf)
	if err != nil {
		return
	}
	err = t.swarmdb.StoreIndexRootHash([]byte(t.tablename), []byte(swarmhash))
	return err
}

