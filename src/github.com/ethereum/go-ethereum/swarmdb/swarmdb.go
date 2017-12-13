package common

import (
	"bytes"
	"encoding/json"
	//"os"
	"fmt"
	"reflect"
	"github.com/ethereum/go-ethereum/swarmdb/log"
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

	sd.Logger = swarmdblog.NewLogger()

	return sd
}

// DBChunkStore  API
func (self *SwarmDB) RetrieveKDBChunk(key []byte) (val []byte, err error) {
	return self.dbchunkstore.RetrieveKChunk(key)
}

func (self *SwarmDB) StoreKDBChunk(key []byte, val []byte) (err error) {
	return self.dbchunkstore.StoreKChunk(key, val)
}

func (self SwarmDB) PrintDBChunk(columnType ColumnType, hashid []byte, c []byte) {
	self.dbchunkstore.PrintDBChunk(columnType, hashid, c)
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
func (self *SwarmDB) GetRootHash(columnName []byte) (roothash []byte, err error) {
	return self.ens.GetRootHash(columnName)
}

func (self *SwarmDB) StoreRootHash(columnName []byte, roothash []byte) (err error) {
	return self.ens.StoreRootHash(columnName, roothash)
}

// Table
func (self SwarmDB) NewTable(ownerID string, tableName string) *Table {
	t := new(Table)
	t.swarmdb = self
	t.ownerID = ownerID
	t.tableName = tableName
	t.columns = make(map[string]*ColumnInfo)
	return t
}

func (t *Table) CreateTable(columns []Column, bid float64, replication int, encrypted int) (err error) {
	columnsMax := 30
	if len(columns) > columnsMax {
		fmt.Printf("\nMax Allowed Columns for a table is %s and you submit %s", columnsMax, len(columns))
	}
	buf := make([]byte, 4096)
	for i, columninfo := range columns {
		copy(buf[2048+i*64:], columninfo.ColumnName)
		b := make([]byte, 1)
		b[0] = byte(columninfo.Primary)
		copy(buf[2048+i*64+26:], b)

		b[0] = byte(columninfo.ColumnType)
		copy(buf[2048+i*64+28:], b)

		b[0] = byte(columninfo.IndexType)
		copy(buf[2048+i*64+30:], b) // columninfo.IndexType)
	}
	copy(buf[4000:],FloatToByte(bid))
	copy(buf[4008:],IntToByte(replication)
	copy(buf[4016:],IntToByt(encrypted)
	swarmhash, err := t.swarmdb.StoreDBChunk(buf)
	if err != nil {
		return
	}
	err = t.swarmdb.StoreRootHash([]byte(t.tableName), []byte(swarmhash))
	return err
}

func (t *Table) OpenTable() (err error) {
	t.swarmdb.Logger.Debug(fmt.Sprintf("swarmdb.go:OpenTable|%s", t.tableName))
	t.columns = make(map[string]*ColumnInfo)
	/// get Table RootHash to  retrieve the table descriptor
	roothash, err := t.swarmdb.GetRootHash([]byte(t.tableName))
	if err != nil {
		fmt.Printf("Error retrieving Index Root Hash for table [%s]: %s", t.tableName, err)
		return err
	}
	setprimary := false
	columndata, err := t.swarmdb.RetrieveDBChunk(roothash)
	if err != nil {
		fmt.Printf("Error retrieving Index Root Hash: %s", err)
		return err
	}
	columnbuf := columndata

	for i := 2048; i < 4000; i = i + 64 {
		buf := make([]byte, 64)
		copy(buf, columnbuf[i:i+64])
		if buf[0] == 0 {
			break
		}
		columninfo := new(ColumnInfo)
		columninfo.columnName = string(bytes.Trim(buf[:25], "\x00"))
		columninfo.primary = uint8(buf[26])
		columninfo.columnType = ColumnType(buf[28]) //:29
		columninfo.indexType = IndexType(buf[30])
		columninfo.roothash = buf[32:]
		fmt.Printf(" columnName: %s (%d) roothash: %x", columninfo.columnName, columninfo.primary , columninfo.roothash);
		switch columninfo.indexType {
		case IT_BPLUSTREE:
			bplustree := NewBPlusTreeDB(t.swarmdb, columninfo.roothash, ColumnType(columninfo.columnType))
			// bplustree.Print()
			columninfo.dbaccess = bplustree
			if err != nil {
				return err
			}
		case IT_HASHTREE:
			columninfo.dbaccess, err = NewHashDB(columninfo.roothash, t.swarmdb)
			if err != nil {
				return err
			}
		}
		t.columns[columninfo.columnName] = columninfo
		if columninfo.primary == 1 {
			if !setprimary {
				t.primaryColumnName = columninfo.columnName
			} else {
				var rerr *RequestFormatError
				return rerr
			}
		}
	}
	t.bid, err = strconv.ParseFloat(string(bytes.Trim(columnbuf[4000:4031],"\x00")),64)
	if err != nil {
		fmt.Printf("\nErr found in %s", err)
	}
	t.replication,_ = strconv.Atoi(string(bytes.Trim(columnbuf[4032:4063],"\x00")))
	t.encrypted,_ = strconv.Atoi(string(columnbuf[4064]))
	//fmt.Printf("\nT bid: %f | T.Rep: %d | T encrypted: %d", t.bid, t.replication, t.encrypted)
	return nil
}


func (t *Table) Put(value string) (err error) {
	t.swarmdb.Logger.Debug(fmt.Sprintf("swarmdb.go:Put|%s", value))
	/// store value to kdb and get a hash
	var evalue interface{}
	if err := json.Unmarshal([]byte(value), &evalue); err != nil {
		//return err
	}

	// TODO: make this robust!
	jsonrecord, ok := evalue.(map[string]interface{})
	if ! ok {
		return fmt.Errorf("Invalid JSON record: %s", value)
	}

	k := make([]byte, 32)
	if pvalue, ok2 := jsonrecord[t.primaryColumnName]; ok2 {
		switch svalue := pvalue.(type) {
		case (int):
			i := fmt.Sprintf("%d", svalue)
			k = convertStringToKey(t.columns[t.primaryColumnName].columnType, i)
		case (float64):
			f := ""
			switch t.columns[t.primaryColumnName].columnType {
			case CT_INTEGER:
				f = fmt.Sprintf("%d", int(svalue))
			case CT_FLOAT:
				f = fmt.Sprintf("%f", svalue)
			case CT_STRING:
				f = fmt.Sprintf("%f", svalue)
			}
			k = convertStringToKey(t.columns[t.primaryColumnName].columnType, f)
		case (string):
			k = convertStringToKey(t.columns[t.primaryColumnName].columnType, svalue)
		default:
			return fmt.Errorf("Unknown Type: %v\n", reflect.TypeOf(svalue))
		}
	} else {
		return fmt.Errorf("No primary key %s specified in input", t.primaryColumnName)
	}

	fmt.Printf("\nT bid: %f | T.Rep: %d | T encrypted: %d", t.bid, t.replication, t.encrypted)
	t.swarmdb.kaddb.Open([]byte(t.ownerID), []byte(t.tableName), []byte(t.primaryColumnName))
	khash, err := t.swarmdb.kaddb.Put(k, []byte(value))
	//fmt.Printf("KADDB Key: %s => (value: %s)  hash(%v)\n", k, value, khash)
	_, err = t.columns[t.primaryColumnName].dbaccess.Put(k, []byte(khash))
	if err != nil {
	}
	if t.buffered {

	} else {
		err = t.FlushBuffer()
		if err != nil {
			fmt.Printf("flushing err %v\n")
		} else {

		}
	}
/*
		switch x := t.columns[t.primaryColumnName].dbaccess.(type) {
		case (*Tree):
			fmt.Printf("B+ tree Print (%s)\n", value)
			x.Print()
			fmt.Printf("-------\n\n")
		}
*/

	return err
}

func (t *Table) Insert(key string, value string) error {
	t.swarmdb.Logger.Debug(fmt.Sprintf("swarmdb.go:Insert|%s", value))
	primaryColumnName := t.primaryColumnName
	/// store value to kdb and get a hash
	_, b, err := t.columns[primaryColumnName].dbaccess.Get([]byte(key))
	if b {
		var derr *DuplicateKeyError
		return derr
	}
	if err != nil {
		return err
	}

	t.swarmdb.kaddb.Open([]byte(t.ownerID), []byte(t.tableName), []byte(primaryColumnName))
	k := convertStringToKey(t.columns[primaryColumnName].columnType, key)
	khash, err := t.swarmdb.kaddb.Put(k, []byte(value))
	if err != nil {
		return err
	}
	_, err = t.columns[primaryColumnName].dbaccess.Insert(k, []byte(khash))
	return err
}


func (t *Table) Get(key string) ([]byte, error) {
	t.swarmdb.Logger.Debug(fmt.Sprintf("swarmdb.go:Get|%s", key))
	primaryColumnName := t.primaryColumnName
	if t.columns[primaryColumnName] == nil {
		var cerr *NoColumnError
		return nil, cerr
	}

	k := convertStringToKey(t.columns[primaryColumnName].columnType, key)
	//fmt.Printf(" k: %v\n", k)

	v, _, err := t.columns[primaryColumnName].dbaccess.Get(k)
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
	t.swarmdb.Logger.Debug(fmt.Sprintf("swarmdb.go:Delete|%s", key))
	primaryColumnName := t.primaryColumnName
	k := convertStringToKey(t.columns[primaryColumnName].columnType, key)
	// fmt.Printf(" DELETE %x\n", k)
	ok = false
	for _, ip := range t.columns {
		ok2, err := ip.dbaccess.Delete(k)
		if err != nil {
			fmt.Printf("ERROR: %v\n", err)
			return ok2, err
		}
		if ok2 {
			ok = true
		}
	}
	return ok, nil
}

func (t *Table) StartBuffer() (err error) {
	t.swarmdb.Logger.Debug(fmt.Sprintf("swarmdb.go:StartBuffer|%s", t.primaryColumnName))
	if t.buffered {
		t.FlushBuffer()
	} else {
		t.buffered = true
	}

	for _, ip := range t.columns {
		_, err := ip.dbaccess.StartBuffer()
		if err != nil {
			return err
		}
	}
	return nil
}

func (t *Table) FlushBuffer() (err error) {
	t.swarmdb.Logger.Debug(fmt.Sprintf("swarmdb.go:FlushBuffer|%s", t.primaryColumnName))

	for _, ip := range t.columns {
		_, err := ip.dbaccess.FlushBuffer()
		if err != nil {
			fmt.Printf(" ERR1 %v\n", err)
			return err
		}
		roothash, err := ip.dbaccess.GetRootHash()
		ip.roothash = roothash
	}
	err = t.updateTableInfo()
	if err != nil {
		fmt.Printf(" err %v \n", err)
		return err
	}
	return nil
}

func (t *Table) updateTableInfo() (err error) {
	buf := make([]byte, 4096)
	i := 0
	for column_num, c := range t.columns {
		b := make([]byte, 1)

		copy(buf[2048+i*64:], column_num)

		b[0] = byte(c.primary)
		copy(buf[2048+i*64+26:], b)

		b[0] = byte(c.columnType)
		copy(buf[2048+i*64+28:], b)

		b[0] = byte(c.indexType)
		copy(buf[2048+i*64+30:], b)

		copy(buf[2048+i*64+32:], c.roothash)
		i++
	}
	swarmhash, err := t.swarmdb.StoreDBChunk(buf)
	if err != nil {
		return err
	}
	err = t.swarmdb.StoreRootHash([]byte(t.tableName), []byte(swarmhash))
	// fmt.Printf(" STORE ROOT HASH [%s] ==> %x\n", t.tableName, swarmhash)
	if err != nil {
		fmt.Printf("StoreRootHash ERROR %v\n", err)
		return err
	} else {
	}
	return nil
}
