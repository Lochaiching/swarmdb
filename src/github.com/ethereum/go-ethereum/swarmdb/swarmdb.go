package common

import (
	"bytes"
	"encoding/json"
	//"os"
	"fmt"
	// "strconv"
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

func (self SwarmDB) PrintDBChunk(keytype KeyType, hashid []byte, c []byte) {
	self.dbchunkstore.PrintDBChunk(keytype, hashid, c)
}

func (self SwarmDB) RetrieveDBChunk(key []byte) (val []byte, err error) {
	val, err = self.dbchunkstore.RetrieveChunk(key)
	return val, err
}

func (self SwarmDB) StoreDBChunk(val []byte) (key []byte, err error) {
	key, err = self.dbchunkstore.StoreChunk(val)
	return key, err
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

func (t *Table) SetPrimary( p string) (err error) {
	t.primary = p
	return nil
}

func (t *Table) OpenTable() (err error) {
	t.indexes = make(map[string]*IndexInfo)
	/// get Table RootHash to  retrieve the table descriptor
	roothash, err := t.swarmdb.GetIndexRootHash([]byte(t.tablename))
	if err != nil {
		fmt.Printf("Error retrieving Index Root Hash for table [%s]: %s", t.tablename, err)
		return err
	}
	//fmt.Printf("Retrieve Root HASH: %v\n", roothash)
	setprimary := false
	indexdata, err := t.swarmdb.RetrieveDBChunk(roothash)
	if err != nil {
		fmt.Printf("Error retrieving Index Root Hash: %s", err)
		return err
	}
	indexbuf := indexdata
	fmt.Printf("index data is: [%s]", indexdata)
	//Rodney: Need to put something in place to make sure we appropriately handle EMPTY indexdata/buf
	for i := 2048; i < 4096; i = i + 64 {
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
			//fmt.Printf("Opening BPlus %s (primary %v, keytype %d)  = %v\n", indexinfo.indexname, indexinfo.primary, indexinfo.keytype,  indexinfo.roothash)
			bplustree := NewBPlusTreeDB(t.swarmdb, indexinfo.roothash, KeyType(indexinfo.keytype))
			// bplustree.Print()
			indexinfo.dbaccess = bplustree
			if err != nil {
				return err
			}
		case TT_HASHTREE:
			indexinfo.dbaccess, err = NewHashDB(indexinfo.roothash, t.swarmdb)
			if err != nil {
				return err
			}
		}
		fmt.Printf("IndexInfo.IndexName [%s]", indexinfo.indexname)
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
	fmt.Printf("table after OpenTable: primary key [%v] number of indeces (%b) \n", t.primary, len(t.indexes))
	return nil
}

func (t *Table) Put(value string) (err error) {
	/// store value to kdb and get a hash
	var evalue interface{}
	if err := json.Unmarshal([]byte(value), &evalue); err != nil {
		//return err
	}
	fmt.Printf("\nVALUE passedin to PUT is [%s] t primary is [%s]\n", evalue, t.primary)
	// TODO: make this robust!
	pvalue := evalue.(map[string]interface{})[t.primary]
	if pvalue == nil {
		return fmt.Errorf("No primary key %s specified in input", t.primary)
	} else {
	}
	k := make([]byte, 32)
	switch svalue := pvalue.(type) {
	case (string):
		k = convertStringToKey(t.indexes[t.primary].keytype, svalue) 
		// fmt.Printf("Primary %s KeyType: %v => %s so k:[%s]\n", t.primary, t.indexes[t.primary].keytype, svalue, k)
	default:
		fmt.Printf("Unknown Type: %v\n", pvalue)
		
	}

	t.swarmdb.kaddb.Open([]byte(t.ownerID), []byte(t.tablename), []byte(t.primary))
	khash, err := t.swarmdb.kaddb.Put(k, []byte(value))
	// fmt.Printf("KADDB Key: %s => (value: %s)  hash(%v)", k, value, khash)
	_, err = t.indexes[t.primary].dbaccess.Put(k, []byte(khash))
	if err != nil {
	}
	if t.buffered {
		//fmt.Printf("Buffered\n");
	} else {
		err = t.FlushBuffer()
		if ( err != nil ) {
			fmt.Printf("flushing err %v\n");
		} else {
			
		}
	}
	/*
	switch x := t.indexes[t.primary].dbaccess.(type) {
	case (*Tree):
		//fmt.Printf("B+ tree Print (%s)\n", value)
		//x.Print()
		//fmt.Printf("-------\n\n");
	}
	 */
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

	//fmt.Printf(" GET: primary %s => keytype: %d\n", index, t.indexes[t.primary].keytype)
	k := convertStringToKey(t.indexes[t.primary].keytype, key)
	//fmt.Printf(" k: %v\n", k)
	
	v, _, err := t.indexes[index].dbaccess.Get(k)
	if err != nil {
		return nil, err
	}
	//fmt.Printf(" GET RESULTv: %v\n", v)
	// get value from kdb
	kres, _, _ := t.swarmdb.kaddb.Get(v)
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
	if t.buffered  {
		t.FlushBuffer()
	} else {
		t.buffered = true
	}

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
			fmt.Printf(" ERR1 %v\n", err)
			return err
		}
		roothash, err := ip.dbaccess.GetRootHash()
		indexname := t.tablename + ":" + ip.indexname
		ip.roothash = roothash
		err = t.swarmdb.StoreIndexRootHash([]byte(indexname), roothash)
		//fmt.Printf(" index: %s => %v\n", indexname, roothash)
		if err != nil {
			fmt.Printf(" ERR2 %v\n", err)
			return err
		}
	}
	err = t.updateTableInfo()
	if err != nil {
		fmt.Printf(" err %v \n", err)
		return err;
	}
	return nil
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
		return err
	}
	err = t.swarmdb.StoreIndexRootHash([]byte(t.tablename), []byte(swarmhash))
	if err != nil {
		return err
	} else {
		// fmt.Printf("Store [%s] => [%v]\n", t.tablename, swarmhash)
	}
	return nil
}
