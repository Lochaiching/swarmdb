package swarmdb

import (
	"bytes"
	//	"encoding/binary"
	"encoding/json"
	"fmt"
	"github.com/ethereum/go-ethereum/swarmdb/log"
	"reflect"
	//"strconv"
)

func NewSwarmDB() *SwarmDB {
	sd := new(SwarmDB)

	// ownerID, tableName => *Table
	sd.tables = make(map[string]*Table)
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

	kaddb, err := NewKademliaDB(dbchunkstore)
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

/*
func (self *SwarmDB) StoreKDBChunk(key []byte, val []byte) (err error) {
	return self.dbchunkstore.StoreKChunk(key, val)
}
*/

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

// parse sql and return rows in bulk (order by, group by, etc.)
func (self SwarmDB) QuerySelect(sql string) (rows []Row, err error) {
	// parse query to get the tableName, OpenTable, run Scan operation and filter out rows
	// Alina: implementing with Scan
	return rows, nil
}

func (self SwarmDB) QueryInsert(sql string) (err error) {
	// Alina: implementing with Put (=> Insert)
	return nil
}

func (self SwarmDB) QueryDelete(sql string) (err error) {
	// Alina: implementing with Delete
	return nil
}

func (self SwarmDB) Query(sql string) (err error) {
	// Alina: dispatch to QuerySelect/Insert/Update/Delete
	return nil
}

func (self SwarmDB) Scan(ownerID string, tableName string, columnName string, ascending bool) (err error) {
	tblKey := fmt.Sprintf("%s|%s", ownerID, tableName)
	if tbl, ok := self.tables[tblKey]; ok {
		tbl.Scan(columnName, ascending)
	} else {
		return fmt.Errorf("No such table to scan %s - %s", ownerID, tableName)
	} 
	return nil
	
}

func (t *Table) Scan(columnName string, ascending bool) (rows []Row, err error) {
	column, err := t.getColumn(columnName)
	if err != nil {
		fmt.Printf(" err %v \n", err)
		return rows, err
	}
	c := column.dbaccess.(OrderedDatabase)
	// TODO: Error checking
	if ascending {
		res, err := c.SeekFirst()
		if err != nil {
		} else {
			records := 0
			for k, v, err := res.Next(); err == nil; k, v, err = res.Next() {
				fmt.Printf(" *int*> %d: K: %s V: %v\n", records, KeyToString(column.columnType, k), v)
				// put this into "Row" form
				records++
			}
		}
	} else {
		res, err := c.SeekLast()
		if err != nil {
		} else {
			records := 0
			for k, v, err := res.Prev(); err == nil; k, v, err = res.Prev() {
				fmt.Printf(" *int*> %d: K: %s V: %v\n", records, KeyToString(column.columnType, k), v)
				// put this into "Row" form
				records++
			}
		}
	}
	return rows, nil
}


// Table
func (self SwarmDB) NewTable(ownerID string, tableName string) *Table {
	t := new(Table)
	t.swarmdb = self
	t.ownerID = ownerID
	t.tableName = tableName
	t.columns = make(map[string]*ColumnInfo)

	// register the Table in SwarmDB
	tblKey := fmt.Sprintf("%s|%s", ownerID, tableName)
	self.tables[tblKey] = t
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
	bidBytes := FloatToByte(bid)
	copy(buf[4000:4008], bidBytes)
	copy(buf[4008:4016], IntToByte(replication))
	copy(buf[4016:4024], IntToByte(encrypted))
	swarmhash, err := t.swarmdb.StoreDBChunk(buf)
	if err != nil {
		return
	}
	err = t.swarmdb.StoreRootHash([]byte(t.tableName), []byte(swarmhash))
	return err
}

func (t *Table) SetPrimary(p string) (err error) {
	t.primaryColumnName = p
	return nil
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

	primaryColumnType := ColumnType(CT_INTEGER)
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
		secondary := false
		if columninfo.primary == 0 {
			secondary = true
		} else {
			primaryColumnType = columninfo.columnType // TODO: what if primary is stored *after* the secondary?  would break this..
		}
		fmt.Printf(" columnName: %s (%d) roothash: %x (secondary: %v)", columninfo.columnName, columninfo.primary, columninfo.roothash, secondary)
		switch columninfo.indexType {
		case IT_BPLUSTREE:
			bplustree := NewBPlusTreeDB(t.swarmdb, columninfo.roothash, ColumnType(columninfo.columnType), secondary, ColumnType(primaryColumnType))
			// bplustree.Print()
			columninfo.dbaccess = bplustree
			if err != nil {
				return err
			}
		case IT_HASHTREE:
			columninfo.dbaccess, err = NewHashDB(columninfo.roothash, t.swarmdb, ColumnType(columninfo.columnType))
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
	t.bid = BytesToFloat(columnbuf[4000:4008])
	t.replication = BytesToInt64(columnbuf[4008:4016])
	t.encrypted = BytesToInt64(columnbuf[4016:4024])
	return nil
}

func convertJSONValueToKey(columnType ColumnType, pvalue interface{}) (k []byte, err error) {
	switch svalue := pvalue.(type) {
	case (int):
		i := fmt.Sprintf("%d", svalue)
		k = convertStringToKey(columnType, i)
	case (float64):
		f := ""
		switch columnType {
		case CT_INTEGER:
			f = fmt.Sprintf("%d", int(svalue))
		case CT_FLOAT:
			f = fmt.Sprintf("%f", svalue)
		case CT_STRING:
			f = fmt.Sprintf("%f", svalue)
		}
		k = convertStringToKey(columnType, f)
	case (string):
		k = convertStringToKey(columnType, svalue)
	default:
		return k, fmt.Errorf("Unknown Type: %v\n", reflect.TypeOf(svalue))
	}
	return k, nil
}


func (t *Table) Put(value string) (err error) {
	t.swarmdb.Logger.Debug(fmt.Sprintf("swarmdb.go:Put|%s", value))
	var evalue interface{}
	if err := json.Unmarshal([]byte(value), &evalue); err != nil {
		//return err
	}

	// TODO: make this robust!
	jsonrecord, ok := evalue.(map[string]interface{})
	if !ok {
		return fmt.Errorf("Invalid JSON record: %s", value)
	}
	k := make([]byte, 32)

	for _, c := range t.columns {
		//fmt.Printf("\nProcessing a column %s and primary is %d", c.columnName, c.primary)
		if c.primary > 0 {
			if pvalue, ok := jsonrecord[t.primaryColumnName]; ok {
				k, _ = convertJSONValueToKey(t.columns[t.primaryColumnName].columnType, pvalue)
			} else {
				return fmt.Errorf("\nPrimary key %s not specified in input", t.primaryColumnName)
			}
			t.swarmdb.kaddb.Open([]byte(t.ownerID), []byte(t.tableName), []byte(t.primaryColumnName), t.bid, t.replication, t.encrypted)
			khash, err := t.swarmdb.kaddb.Put(k, []byte(value))
			if err != nil {
				fmt.Errorf("\nKademlia Put Failed")
				// TODO
			}
			fmt.Printf(" - primary  %s | %x\n", c.columnName, k)
			_, err = t.columns[c.columnName].dbaccess.Put(k, khash)
//			t.columns[c.columnName].dbaccess.Print()
		} else {
			k2 := make([]byte, 32)
			if pvalue, ok := jsonrecord[c.columnName]; ok {
				k2, _ = convertJSONValueToKey(c.columnType, pvalue)
				if err != nil {
					// TODO
				}
			} else {
				//this is ok
				//return fmt.Errorf("Column [%s] not found in [%+v]", c.columnName, jsonrecord)
			}
			fmt.Printf(" - secondary %s %x | %x\n", c.columnName, k2, k)
			_, err = t.columns[c.columnName].dbaccess.Put(k2, k)
			if err != nil {
				fmt.Errorf("\nDB Put Failed")
			} else {
			}
			//t.columns[c.columnName].dbaccess.Print()
		}
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

	t.swarmdb.kaddb.Open([]byte(t.ownerID), []byte(t.tableName), []byte(primaryColumnName), t.bid, t.replication, t.encrypted)
	k := convertStringToKey(t.columns[primaryColumnName].columnType, key)
	khash, err := t.swarmdb.kaddb.Put(k, []byte(value))
	if err != nil {
		return err
	}
	_, err = t.columns[primaryColumnName].dbaccess.Insert(k, []byte(khash))
	return err
}

func (t *Table) getPrimaryColumn() (c *ColumnInfo, err error) {
	return t.getColumn( t.primaryColumnName )
}

func (t *Table) getColumn(columnName string) (c *ColumnInfo, err error) {
	if t.columns[columnName] == nil {
		var cerr *NoColumnError
		return c, cerr
	}
	return t.columns[columnName], nil
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
	if len(v) > 0 {
		//fmt.Printf(" GET RESULTv: %v\n", v)
		// get value from kdb
		kres, _, _ := t.swarmdb.kaddb.GetByKey(k)
		fres := bytes.Trim(kres, "\x00")
		return fres, err
	} else {
		return []byte(""), nil
	}
}

func (t *Table) Delete(key string) (ok bool, err error) {
	t.swarmdb.Logger.Debug(fmt.Sprintf("swarmdb.go:Delete|%s", key))
	primaryColumnName := t.primaryColumnName
	k := convertStringToKey(t.columns[primaryColumnName].columnType, key)
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
