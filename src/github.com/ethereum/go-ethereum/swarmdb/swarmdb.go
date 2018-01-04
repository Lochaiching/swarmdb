package swarmdb

import (
	"bytes"
	//"encoding/binary"
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
		fmt.Printf("NO CHUNK STORE!\n")
	} else {
		sd.dbchunkstore = dbchunkstore
	}

	ens, err := NewENSSimulation("/tmp/ens.db")
	if err != nil {
		// TODO: PANIC
		fmt.Printf("NO ENS!\n")
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
func (self SwarmDB) QuerySelect(query *QueryOption) (rows []Row, err error) {
	//where to switch on bplus or hashdb?

	var rawRows []Row
	for _, column := range query.RequestColumns {

		colRows, err := self.Scan(query.TableOwner, query.Table, column.ColumnName, query.Ascending)
		if err != nil {
			return rows, err
		}
		for _, colRow := range colRows {
			dupe := false
			for _, row := range rawRows {
				if checkDuplicateRow(row, colRow) {
					dupe = true
					break
				}
			}
			if dupe == false {
				rawRows = append(rawRows, colRow)
			}
		}
	}

	//filter for correct columns and Where/Having
	for _, row := range rawRows {
		fRow := filterRowByColumns(&row, query.RequestColumns)
		if len(fRow.cells) == 0 {
			return rows, err //no columns found
		}

		add := false
		if _, ok := fRow.cells[query.Where.Left]; ok {
			/* //need to make these type dependent b/c of interface{}, also needs to be moved to a diff fcn
			switch query.Where.Operator {
			case "=":
				if fRow.cells[query.Where.Left] == query.Where.Right {
					add = true
				}
			case "<":
				if fRow.cells[query.Where.Left] < query.Where.Right {
					add = true
				}

			case "<=":
				if fRow.cells[query.Where.Left] <= query.Where.Right {
					add = true
				}

			case ">=":
				if fRow.cells[query.Where.Left] >= query.Where.Right {
					add = true
				}

			default:
				return rows, fmt.Errorf("Operator [%s] not found", query.Where.Operator)
			}
			*/

		} else {
			return rows, fmt.Errorf("query.Where.Left [%s] not found in filtered row [%+v]", query.Where.Left, fRow)
		}
		if add == true {
			rows = append(rows, fRow)
		}
	}

	//Put it in order for Ascending/GroupBy

	return rows, nil

}

func (self SwarmDB) QueryInsert(query *QueryOption) (err error) {
	// Alina: implementing with Put (=> Insert)

	
	return nil
}
func (self SwarmDB) QueryUpdate(query *QueryOption) (err error) {
	// Alina: implementing with Put (=> Insert)
	return nil
}

func (self SwarmDB) QueryDelete(query *QueryOption) (err error) {
	// Alina: implementing with Delete
	return nil
}

func (self SwarmDB) Query(query *QueryOption) (rows []Row, err error) {
	switch query.Type {
	case "Select":
		rows, err := self.QuerySelect(query)
		if err != nil {
			return rows, err
		}
		if len(rows) == 0 {
			return rows, fmt.Errorf("select query came back empty")
		}
		return rows, err
	case "Insert":
		err = self.QueryInsert(query)
		return rows, err

	case "Update":
		err = self.QueryUpdate(query)
		return rows, err

	case "Delete":
		err = self.QueryDelete(query)
		return rows, err
	}
	return rows, nil
}

func (self SwarmDB) Scan(ownerID string, tableName string, columnName string, ascending int) (rows []Row, err error) {

	tblKey := self.GetTableKey(ownerID, tableName)
	if tbl, ok := self.tables[tblKey]; ok {
		rows, err = tbl.Scan(columnName, ascending)
	} else {
		return rows, fmt.Errorf("No such table to scan %s - %s", ownerID, tableName)
	}
	return rows, nil

}

func (self SwarmDB) GetTable(ownerID string, tableName string) (tbl *Table, err error) {

	if len(tableName) == 0 {
		return tbl, fmt.Errorf("Invalid table [%s]", tableName)
	}
	tblKey := self.GetTableKey(ownerID, tableName)

	if tbl, ok := self.tables[tblKey]; ok {
		fmt.Printf("\nprimary column name GetTable: %s -> columns: %v\n", tbl.columns, tbl.primaryColumnName)
		return tbl, nil
	} else {
		// this should throw an error if the table is not created
		tbl = self.NewTable(ownerID, tableName)
		err = tbl.OpenTable()
		if err != nil {
			return tbl, err
		}
		return tbl, nil
	}
}

func (self *SwarmDB) SelectHandler(ownerID string, data string) (resp string, err error) {
	// var rerr *RequestFormatError
	d, err := parseData(data)
	if err != nil {
		fmt.Printf("problem: %s\n", err)
		return resp, err
	}

	tblKey := self.GetTableKey(d.Owner, d.Table)

	switch d.RequestType {
	case "CreateTable":
		if len(d.Table) == 0 || len(d.Columns) == 0 {
			return resp, fmt.Errorf(`ERR: empty table and column`)
		}
		//Upon further review, could make a NewTable and then call this from tbl. ---
		_, err := self.CreateTable(ownerID, d.Table, d.Columns, d.Bid, d.Replication, d.Encrypted)
		if err != nil {
			return resp, err
		}
		return "ok", err
	case "Put":
		tbl, err := self.GetTable(ownerID, d.Table)
		if err != nil {
			fmt.Printf("err1: %s\n", err)
			return resp, err
		} else {
			err2 := tbl.Put(d.Rows)
			if err2 != nil {
				fmt.Printf("Err putting: %s", err2)
				return resp, fmt.Errorf("\nError trying to 'Put' [%s] -- Err: %s")
			} else {
				return "ok", nil
			}
		}
	case "Get":
		if len(d.Key) == 0 {
			return resp, fmt.Errorf("Missing key in GET")
		}
		tbl, err := self.GetTable(ownerID, d.Table)
		if err != nil {
			return resp, err
		}
		ret, err := tbl.Get(d.Key)
		if err != nil {
			return resp, err
		} else {
			return string(ret), nil
		}
	case "Insert":
		if len(d.Key) == 0 {
			return resp, fmt.Errorf("Missing Key/Value")
		}
		tbl, err := self.GetTable(ownerID, d.Table)
		if err != nil {
			return resp, err
		}
		err2 := tbl.Insert(d.Rows)
		if err2 != nil {
			return resp, err2
		}
		return "ok", nil
	case "Delete":
		if len(d.Key) == 0 {
			return resp, fmt.Errorf("Missing key")
		}
		tbl, err := self.GetTable(ownerID, d.Table)
		if err != nil {
			return resp, err
		}
		_, err2 := tbl.Delete(d.Key)
		if err2 != nil {
			return resp, err2
		}
		return "ok", nil
		/*
			case "StartBuffer":
				err := tbl.StartBuffer()
				ret := "okay"
				if err != nil{
					ret = err.Error()
				}
				return ret
			case "FlushBuffer":
				err := tbl.FlushBuffer()
				ret := "okay"
				if err != nil{
					ret = err.Error()
				}
				return ret
		*/
	case "Query":
		fmt.Printf("\nReceived GETQUERY\n")
		if len(d.RawQuery) == 0 {
			return resp, fmt.Errorf("RawQuery is blank")
		}

		query, err := ParseQuery(d.RawQuery)
		if err != nil {
			fmt.Printf("err comes from query: [%s]\n", d.RawQuery)
			return resp, err
		}
		if len(d.Table) == 0 {
			fmt.Printf("Getting Table from Query rather than data obj\n")
			d.Table = query.Table //since table is specified in the query we do not have get it as a separate input
		}

		tbl, err := self.GetTable(ownerID, d.Table)
		fmt.Printf("Returned table [%+v] when calling gettable with Owner[%s], Table[%s]\n", tbl, ownerID, d.Table)
		tblInfo, err := tbl.GetTableInfo()
		if err != nil {
			fmt.Printf("tblInfo err \n")
			return resp, err
		}
		query.TableOwner = d.Owner //probably should check the owner against the tableinfo owner here
		
		fmt.Printf("Table info gotten: [%+v] \n", tblInfo)
		fmt.Printf("QueryOption is: [%+v] \n", query)

		/*
			fmt.Printf("The other way of getting tableinfo\n")
			tblKey := self.GetTableKey(d.Owner, d.Table)
			tblInfo, err := self.tables[tblKey].GetTableInfo()
			if err != nil {
			        return resp, err
			}
		*/

		//checking validity of columns
		for _, reqCol := range query.RequestColumns {
			if _, ok := tblInfo[reqCol.ColumnName]; !ok {
				return resp, fmt.Errorf("Requested col [%s] does not exist in table [%+v]\n", reqCol.ColumnName, tblInfo)
			}
		}

		//checking the Where clause
		if len(query.Where.Left) > 0 {
			if _, ok := tblInfo[query.Where.Left]; !ok {
				return resp, fmt.Errorf("Query col [%s] does not exist in table\n", query.Where.Left)
			}

			//checking if the query is just a primary key Get
			if query.Where.Left == tbl.primaryColumnName && query.Where.Operator == "=" {
				fmt.Printf("Calling Get from Query\n")
				byteRow, err := tbl.Get(query.Where.Right)
				if err != nil {
					fmt.Printf("Error Calling Get from Query [%s]\n", err)
					return resp, err
				}
				row, err := tbl.byteArrayToRow(byteRow)
				fmt.Printf("Response row from Get: %s (%v)\n", row, row)
				if err != nil {
					return resp, err
				}

				filteredRow := filterRowByColumns(&row, query.RequestColumns)
				fmt.Printf("\nResponse filteredrow from Get: %s (%v)", filteredRow.cells, filteredRow.cells)
				retJson, err := json.Marshal(filteredRow.cells)
				if err != nil {
					return resp, err
				}
				return string(retJson), nil
			}
		}
		fmt.Printf("\nAbout to process query [%s]", query)
		//process the query
		qRows, err := self.Query(&query)
		fmt.Printf("\nQRows: [%+v]",qRows)
		if err != nil {
			fmt.Printf("\nError processing query [%s] | Error: %s", query, err)
			return resp, err
		}
		resp, err = rowDataToJson(qRows)
		if err != nil {
			return resp, err
		}
		return resp, nil

	case "GetTableInfo":
		tblcols, err := self.tables[tblKey].GetTableInfo()
		if err != nil {
			return resp, err
		}
		tblinfo, err := json.Marshal(tblcols)
		if err != nil {
			return resp, err
		}
		return string(tblinfo), nil
	}
	return resp, fmt.Errorf("RequestType invalid: [%s]", d.RequestType)
}

func parseData(data string) (*RequestOption, error) {
	udata := new(RequestOption)
	if err := json.Unmarshal([]byte(data), udata); err != nil {
		fmt.Printf("BIG PROBLEM parsing [%s] | Error: %v\n", data, err)
		return nil, err
	}
	return udata, nil
}

func (t *Table) Scan(columnName string, ascending int) (rows []Row, err error) {
	column, err := t.getColumn(columnName)
	if err != nil {
		fmt.Printf(" err %v \n", err)
		return rows, err
	}
	c := column.dbaccess.(OrderedDatabase)
	// TODO: Error checking
	if ascending == 1 {
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
	t.swarmdb = &self
	t.ownerID = ownerID
	t.tableName = tableName
	t.columns = make(map[string]*ColumnInfo)

	// register the Table in SwarmDB
	tblKey := self.GetTableKey(ownerID, tableName)
	self.tables[tblKey] = t
	return t
}

func (swdb *SwarmDB) CreateTable(ownerID string, tableName string, columns []Column, bid float64, replication int, encrypted int) (tbl *Table, err error) {
	columnsMax := 30
	primaryColumnName := ""
	if len(columns) > columnsMax {
		fmt.Printf("\nMax Allowed Columns for a table is %s and you submit %s", columnsMax, len(columns))
	}
	buf := make([]byte, 4096)
	tbl = swdb.NewTable(ownerID, tableName)
	for i, columninfo := range columns {
		copy(buf[2048+i*64:], columninfo.ColumnName)
		b := make([]byte, 1)
		b[0] = byte(columninfo.Primary)
		copy(buf[2048+i*64+26:], b)

		b[0] = byte(columninfo.ColumnType)
		copy(buf[2048+i*64+28:], b)

		b[0] = byte(columninfo.IndexType)
		copy(buf[2048+i*64+30:], b) // columninfo.IndexType
		// fmt.Printf(" column: %v\n", columninfo)
		if columninfo.Primary > 0 {
			primaryColumnName = columninfo.ColumnName
			// fmt.Printf("  [%s] ---> primary\n", primaryColumnName)
		} else {
			// fmt.Printf("  ---> NOT primary\n")
		}
	}
	bidBytes := FloatToByte(bid)
	copy(buf[4000:4008], bidBytes)
	copy(buf[4008:4016], IntToByte(replication))
	copy(buf[4016:4024], IntToByte(encrypted))
	swarmhash, err := swdb.StoreDBChunk(buf)
	if err != nil {
		fmt.Printf(" problem storing chunk\n")
		return
	}
	tbl.primaryColumnName = primaryColumnName
	tbl.tableName = tableName

	fmt.Printf(" CreateTable primary: [%s] (%s) store root hash:  %s hash:[%x]\n", tbl.primaryColumnName, tbl.ownerID, tableName, swarmhash)
	err = swdb.StoreRootHash([]byte(tableName), []byte(swarmhash))
	if err != nil {
		return tbl, err
	} else {
		err = tbl.OpenTable()
		if err != nil {
			return tbl, err
		} else {
			return tbl, nil
		}
	}
}

func (t *Table) OpenTable() (err error) {
	t.swarmdb.Logger.Debug(fmt.Sprintf("swarmdb.go:OpenTable|%s", t.tableName))
	t.columns = make(map[string]*ColumnInfo)
	/// get Table RootHash to  retrieve the table descriptor
	roothash, err := t.swarmdb.GetRootHash([]byte(t.tableName))
	fmt.Printf("opening table @ %s roothash %s\n", t.tableName, roothash)
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
			fmt.Printf("skip!\n")
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
		fmt.Printf("\n columnName: %s (%d) roothash: %x (secondary: %v) columnType: %d", columninfo.columnName, columninfo.primary, columninfo.roothash, secondary, columninfo.columnType)
		switch columninfo.indexType {
		case IT_BPLUSTREE:
			bplustree := NewBPlusTreeDB(*t.swarmdb, columninfo.roothash, ColumnType(columninfo.columnType), secondary, ColumnType(primaryColumnType))
			// bplustree.Print()
			columninfo.dbaccess = bplustree
			if err != nil {
				return err
			}
		case IT_HASHTREE:
			columninfo.dbaccess, err = NewHashDB(columninfo.roothash, *t.swarmdb, ColumnType(columninfo.columnType))
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

func (t *Table) Put(jsonrecord map[string]string) (err error) {
	value, err0 := json.Marshal(jsonrecord)
	if err0 != nil {
		return err0
	} else {
		t.swarmdb.Logger.Debug(fmt.Sprintf("swarmdb.go:Put|%s", value))
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
			// fmt.Printf(" - primary  %s | %x\n", c.columnName, k)
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

	return nil
}

func (t *Table) Insert(value map[string]string) (err error) {
	/*
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
	*/
	return err
}

func (t *Table) getPrimaryColumn() (c *ColumnInfo, err error) {
	return t.getColumn(t.primaryColumnName)
}

func (t *Table) getColumn(columnName string) (c *ColumnInfo, err error) {
	if t.columns[columnName] == nil {
		var cerr *NoColumnError
		return c, cerr
	}
	return t.columns[columnName], nil
}

func (t *Table) byteArrayToRow(byteData []byte) (out Row, err error) {
	var row Row
	row.cells = make(map[string]interface{})
	//row.primaryKeyValue = t.primaryColumnName
	if err := json.Unmarshal(byteData, &row.cells); err != nil {
		return out, err
	}
	return row, nil
}

func (t *Table) Get(key string) (out []byte, err error) {
	t.swarmdb.Logger.Debug(fmt.Sprintf("swarmdb.go:Get|%s", key))
	primaryColumnName := t.primaryColumnName
	if t.columns[primaryColumnName] == nil {
		fmt.Printf("NO COLUMN ERROR\n")
		var cerr *NoColumnError
		return nil, cerr
	} else {
		// fmt.Printf("READY\n")
	}
	t.swarmdb.kaddb.Open([]byte(t.ownerID), []byte(t.tableName), []byte(t.primaryColumnName), t.bid, t.replication, t.encrypted)
	fmt.Printf("\n GET key: (%s)%v\n", key, key)
	k := convertStringToKey(t.columns[primaryColumnName].columnType, key)
	fmt.Printf("\n GET k: (%s)%v\n", k, k)

	v, _, err2 := t.columns[primaryColumnName].dbaccess.Get(k)
	fmt.Printf("\n v retrieved from db traversal get = %s", v)
	if err2 != nil {
		fmt.Printf("\nError traversing tree: %s", err.Error())
		return nil, err2
	}
	if len(v) > 0 {
		// get value from kdb
		kres, _, err3 := t.swarmdb.kaddb.GetByKey(k)
		if err3 != nil {
			return out, err3
		}
		fres := bytes.Trim(kres, "\x00")
		return fres, nil
	} else {
		fmt.Printf("\n MISSING RECORD %s\n", key)
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

func (swdb *SwarmDB) GetTableKey(owner string, tableName string) (key string) {
	return (fmt.Sprintf("%s|%s", owner, tableName))
}

func (t *Table) GetTableInfo() (tblInfo map[string]Column, err error) {
	//var columns []Column
	fmt.Printf("\n in GetTableInfo with table [%+v] \n", t)
	tblInfo = make(map[string]Column)
	for cname, c := range t.columns {
		var cinfo Column
		cinfo.ColumnName = cname
		cinfo.IndexType = c.indexType
		cinfo.Primary = int(c.primary)
		cinfo.ColumnType = c.columnType
		//	fmt.Printf("\nProcessing columng [%s]", cname)
		if _, ok := tblInfo[cname]; ok { //would mean for some reason there are two cols named the same thing
			fmt.Printf("\nERROR: Duplicate column? [%s]", cname)
			return tblInfo, err
		}
		tblInfo[cname] = cinfo
		//columns = append(columns, cinfo)
	}
	//jcolumns, err := json.Marshal(columns)

	//return string(jcolumns), err
	return tblInfo, err
}
