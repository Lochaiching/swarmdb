package common

import (
	"bytes"
	"encoding/json"
	//"os"
	"fmt"
	"strconv"
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
func (self SwarmDB) NewTable(ownerID string, tablename string) *Table {
	t := new(Table)
	t.swarmdb = self
	t.ownerID = ownerID
	t.tablename = tablename
	t.indexes = make(map[string]*IndexInfo)
	return t
}

func (t *Table) CreateTable(option []TableOption) (err error) {
	buf := make([]byte, 4096)
	for i, columninfo := range option {
		copy(buf[2048+i*64:], columninfo.Index)
		b := make([]byte, 1)
		b[0] = byte(columninfo.Primary)
		copy(buf[2048+i*64+26:], b) // strconv.Itoa(columninfo.Primary))

		b[0] = byte(columninfo.KeyType)
		copy(buf[2048+i*64+28:], b) // strconv.Itoa(columninfo.KeyType))

		b[0] = byte(columninfo.TreeType)
		copy(buf[2048+i*64+30:], b) // columninfo.TreeType)
	}
	swarmhash, err := t.swarmdb.StoreDBChunk(buf)
	if err != nil {
		return
	}
	err = t.swarmdb.StoreIndexRootHash([]byte(t.tablename), []byte(swarmhash))
	return err
}

func (t *Table) OpenTable() (err error) {
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
                indexinfo.primary = uint8(buf[26])
                indexinfo.keytype = KeyType(buf[28])  //:29
                indexinfo.treetype = TreeType(buf[30])
                indexinfo.roothash =  buf[32:]
		switch indexinfo.treetype {
		case TT_BPLUSTREE:
			indexinfo.dbaccess = NewBPlusTreeDB(t.swarmdb, indexinfo.roothash, KeyType(indexinfo.keytype))
			if err != nil {
				return err
			}
		case TT_HASHTREE:
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
func convertStringToKey(keyType KeyType, key string) (k []byte) {
	k = make([]byte, 32)
	switch ( keyType ) {
	case KT_INTEGER:
		// convert using atoi to int
		i, _ := strconv.Atoi(key)
		k8 := IntToByte(i)  // 8 byte
		copy(k, k8) // 32 byte
	case KT_STRING:
		copy(k, []byte(k))
	case KT_FLOAT:
		f, _ := strconv.ParseFloat(key, 64)
		k8 := FloatToByte(f) // 8 byte
			copy(k, k8) // 32 byte
	case KT_BLOB:
		// TODO: do this correctly with JSON treatment of binary 
		copy(k, []byte(key))
	}
	return k
}
	
func (t *Table) Put(value string) (err error) {
	/// store value to kdb and get a hash
	var evalue interface{}
	if err := json.Unmarshal([]byte(value), &evalue); err != nil {
		//return err
	}

	// TODO: make this robust!
	pvalue := evalue.(map[string]interface{})[t.primary]
	k := convertStringToKey(t.indexes[t.primary].keytype, pvalue.(string))

	t.swarmdb.kaddb.Open([]byte(t.ownerID), []byte(t.tablename), []byte(t.primary))
	fmt.Printf("KADDB Open - OwnerID: [%s] Table: [%s] Primary: %v => (%v)\n", t.ownerID, t.tablename, t.primary, pvalue.(string))
	khash, err := t.swarmdb.kaddb.Put(k, []byte(value))
	// PRIMARY INDEX ONLY -- need to put every indexes but currently added only for the primary index
	fmt.Printf(" primary: %v dbaccess: %v  k: %v v(%d bytes): %v\n", t.primary, t.indexes[t.primary].dbaccess, pvalue.(string), len(khash), khash)
	_, err = t.indexes[t.primary].dbaccess.Put(k, []byte(khash))
	/*switch x := t.indexes[t.primary].dbaccess.(type) {
	case (*Tree):
		 fmt.Printf("B+ tree Print\n")
		x.Print()
	}*/
	return err
}

func (t *Table) Insert(key string, value string) error {
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
	k := convertStringToKey(t.indexes[t.primary].keytype, key)
	khash, err := t.swarmdb.kaddb.Put(k, []byte(value))
	if err != nil {
		return err
	}
	_, err = t.indexes[index].dbaccess.Insert(k, []byte(khash))
	return err
}

func (t *Table) Get(key string) ([]byte, error) {
	index := t.primary
	if t.indexes[index] == nil {
		var cerr *NoColumnError
		return nil, cerr
	}
	k := convertStringToKey(t.indexes[t.primary].keytype, key)

	_, _, err := t.indexes[index].dbaccess.Get(k)
	if err != nil {
		return nil, err
	}

	// get value from kdb
	kres, _, _ := t.swarmdb.kaddb.GetByKey([]byte(key))
	fres := bytes.Trim(kres, "\x00")
	return fres, err
}


func (t *Table) Delete(key string) (ok bool, err error) {
	k := convertStringToKey(t.indexes[t.primary].keytype, key)
	ok = false
	for _, ip := range t.indexes {
		ok2, err := ip.dbaccess.Delete(k)
		if err != nil {
			return ok2, err
		}
		if ok2 {
			ok = true
		}
	}
	return ok, nil
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
		b := make([]byte, 1)

                copy(buf[2048+i*64:], idx)
		
		b[0] = byte(ivalue.primary)
                copy(buf[2048+i*64+26:], b)

		b[0] = byte(ivalue.keytype)
                copy(buf[2048+i*64+28:], b) // byte(ivalue.keytype)

		b[0] = byte(ivalue.treetype)
                copy(buf[2048+i*64+30:], b)

                copy(buf[2048+i*64+32:], ivalue.roothash)
		i++
	}
	swarmhash, err := t.swarmdb.StoreDBChunk(buf)
	if err != nil {
		return
	}
	err = t.swarmdb.StoreIndexRootHash([]byte(t.tablename), []byte(swarmhash))
	return err
}
