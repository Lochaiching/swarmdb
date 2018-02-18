package swarmdb

import (
	"bytes"
	"encoding/json"
	"fmt"
	"github.com/ethereum/go-ethereum/crypto"
	"github.com/ethereum/go-ethereum/log"
	"io"
	"strconv"
)

type Table struct {
	buffered          bool
	swarmdb           *SwarmDB
	tableName         string
	Owner             string
	Database          string
	roothash          []byte
	columns           map[string]*ColumnInfo
	primaryColumnName string
	encrypted         int
}

type ColumnInfo struct {
	columnName string
	indexType  IndexType
	roothash   []byte
	dbaccess   Database
	primary    uint8
	columnType ColumnType
}

type Row map[string]interface{}

//primaryKeyValue interface{}

func NewRow() (r Row) {
	r = make(map[string]interface{})
	return r
}

func (r Row) Set(columnName string, val interface{}) {
	r[columnName] = val
}

func (t *Table) OpenTable(u *SWARMDBUser) (err error) {

	t.columns = make(map[string]*ColumnInfo)

	/// get Table RootHash to  retrieve the table descriptor
	tblKey := t.swarmdb.GetTableKey(t.Owner, t.Database, t.tableName)
	roothash, err := t.swarmdb.GetRootHash(u, []byte(tblKey))
	log.Debug(fmt.Sprintf("[table:OpenTable] opening table @ %s roothash [%x]\n", t.tableName, roothash))

	if err != nil {
		return GenerateSWARMDBError(err, fmt.Sprintf("[table:OpenTable] GetRootHash for table [%s]: %v", tblKey, err))
	}
	if len(roothash) == 0 {
		return &SWARMDBError{message: fmt.Sprintf("[table:OpenTable] Empty root hash"), ErrorCode: 403, ErrorMessage: fmt.Sprintf("Table Does Not Exist: TableName [%s] Owner [%s]", t.tableName, t.Owner)}
	}
	setprimary := false
	columndata, err := t.swarmdb.RetrieveDBChunk(u, roothash)
	if err != nil {
		return GenerateSWARMDBError(err, fmt.Sprintf("[table:OpenTable] RetrieveDBChunk %s", err.Error()))
	}
	t.encrypted = BytesToInt(columndata[4000:4024])
	fmt.Sprintf("[table:OpenTable] t.encrypted [%d] buf [%+v]", t.encrypted, columndata[4000:4024])
	columnbuf := columndata
	primaryColumnType := ColumnType(CT_INTEGER)
	for i := 2048; i < 4000; i = i + 64 {
		buf := make([]byte, 64)
		copy(buf, columnbuf[i:i+64])
		if buf[0] == 0 {
			// fmt.Printf("\nin swarmdb.OpenTable, skip!\n")
			break
		}
		columninfo := new(ColumnInfo)
		columninfo.columnName = string(bytes.Trim(buf[:25], "\x00"))
		columninfo.primary = uint8(buf[26])
		columninfo.columnType, _ = ByteToColumnType(buf[28]) //:29
		columninfo.indexType = ByteToIndexType(buf[30])
		columninfo.roothash = buf[32:]
		secondary := false
		if columninfo.primary == 0 {
			secondary = true
		} else {
			primaryColumnType = (columninfo.columnType) // TODO: what if primary is stored *after* the secondary?  would break this..
		}
		// fmt.Printf("\n columnName: %s (%d) roothash: %x (secondary: %v) columnType: %d", columninfo.columnName, columninfo.primary, columninfo.roothash, secondary, columninfo.columnType)
		switch columninfo.indexType {
		case IT_BPLUSTREE:
			bplustree, err := NewBPlusTreeDB(u, t.swarmdb, columninfo.roothash, ColumnType(columninfo.columnType), secondary, ColumnType(primaryColumnType), t.encrypted)
			if err != nil {
				return GenerateSWARMDBError(err, fmt.Sprintf("[table:OpenTable] NewBPlusTreeDB %s", err.Error()))
			}
			columninfo.dbaccess = bplustree
		case IT_HASHTREE:
			columninfo.dbaccess, err = NewHashDB(u, columninfo.roothash, t.swarmdb, ColumnType(columninfo.columnType), t.encrypted)
			if err != nil {
				return GenerateSWARMDBError(err, fmt.Sprintf("[table:OpenTable] NewHashDB %s", err.Error()))
			}
		}
		t.columns[columninfo.columnName] = columninfo
		// fmt.Printf("  --- OpenTable columns: %s ==> %v ==> %v\n", columninfo.columnName, columninfo, t.columns)
		if columninfo.primary == 1 {
			if !setprimary {
				t.primaryColumnName = columninfo.columnName
			} else {
				var rerr RequestFormatError
				return &rerr
			}
		}
	}
	log.Debug(fmt.Sprintf("OpenTable [%s] with Owner [%s] Database [%s] Returning with Columns: %v\n", t.tableName, t.Owner, t.Database, t.columns))
	return nil
}

func (t *Table) getPrimaryColumn() (c *ColumnInfo, err error) {
	return t.getColumn(t.primaryColumnName)
}

func (t *Table) getColumn(columnName string) (c *ColumnInfo, err error) {
	if t.columns[columnName] == nil {
		return c, &NoColumnError{tableName: t.tableName, tableOwner: t.Owner, columnName: columnName}
	}
	return t.columns[columnName], nil
}

func (t *Table) byteArrayToRow(byteData []byte) (out Row, err error) {
	res := NewRow()
	if len(byteData) == 0 {
		return res, nil
	}
	if err := json.Unmarshal(byteData, &res); err != nil {
		return res, &SWARMDBError{message: fmt.Sprintf("[table:byteArrayToRow] Unmarshal %s for [%s]", err.Error(), byteData), ErrorCode: 436, ErrorMessage: "Unable to converty byte array to Row Object"}
	}

	row := NewRow()

	for colName, cell := range res {
		colDef := t.columns[colName]
		switch a := cell.(type) {
		case int, int8, int16, int32, int64, uint8, uint16, uint32, uint64:
			switch colDef.columnType {
			case CT_STRING:
				row[colName] = fmt.Sprintf("%d", a)
				break
			case CT_INTEGER:
				row[colName] = a
				break
			case CT_FLOAT:
				row[colName] = float64(a.(int))
			}
			break
		case float64:
			switch colDef.columnType {
			case CT_STRING:
				row[colName] = fmt.Sprintf("%f", cell)
				break
			case CT_INTEGER:
				row[colName] = int(a)
				break
			case CT_FLOAT:
				row[colName] = a
			}
			break
		case string:
			switch colDef.columnType {
			case CT_INTEGER:
				row[colName], err = strconv.Atoi(a)

			case CT_STRING:
				row[colName] = a
			case CT_FLOAT:
				row[colName], err = strconv.ParseFloat(a, 64)
			}
			break
		}
	}
	return row, nil
}

func (self *Table) buildSdata(u *SWARMDBUser, key []byte, value []byte) (mergedBodycontent []byte, err error) {
	contentPrefix := BuildSwarmdbPrefix([]byte(self.Owner), []byte(self.Database), []byte(self.tableName), key)
	log.Debug(fmt.Sprintf("[table:buildSdata] contentPrefix is: %s", contentPrefix))

	var metadataBody []byte
	metadataBody = make([]byte, 286)
	//TODO: Use Constants
	copy(metadataBody[0:32], []byte(self.Owner))
	copy(metadataBody[32:64], []byte(self.Database))
	copy(metadataBody[64:96], []byte(self.tableName))
	copy(metadataBody[KNODE_START_CHUNKKEY:KNODE_END_CHUNKKEY], contentPrefix)
	copy(metadataBody[128:160], self.Owner)  //TODO: Chunk needs to use Address of Owner instead of owner
	copy(metadataBody[160:161], []byte("K")) //TODO: Define nodeType representation -- self.nodeType)
	copy(metadataBody[161:162], IntToByte(u.AutoRenew))
	copy(metadataBody[162:163], IntToByte(u.MinReplication))
	copy(metadataBody[163:164], IntToByte(u.MaxReplication))
	copy(metadataBody[164:165], IntToByte(self.encrypted))
	//TODO: Reserved for birthTimestamp -- copy(metadataBody[165:173], IntToByte(birthtimestamp))
	//TODO: lastupdate timestamp -- copy(metadataBody[173:181], IntToByte(lastupdttimestamp))
	//TODO: Version -- copy(metadataBody[181:189], IntToByte(version))

	unencryptedMetadata := metadataBody[0:189]
	msg_hash := SignHash(unencryptedMetadata)

	//TODO: msg_hash --
	copy(metadataBody[189:221], msg_hash)

	km := self.swarmdb.dbchunkstore.GetKeyManager()
	sdataSig, errSign := km.SignMessage(msg_hash)
	if errSign != nil {
		return mergedBodycontent, &SWARMDBError{message: `[kademliadb:buildSdata] SignMessage ` + errSign.Error(), ErrorCode: 455, ErrorMessage: "Keymanager Unable to Sign Message"}
	}

	//TODO: Sig -- document this
	copy(metadataBody[221:286], sdataSig)
	log.Debug(fmt.Sprintf("Metadata is [%+v]", metadataBody))

	mergedBodycontent = make([]byte, CHUNK_SIZE)
	copy(mergedBodycontent[:], metadataBody)
	copy(mergedBodycontent[KNODE_START_ENCRYPTION:], value) // expected to be the encrypted body content

	log.Debug(fmt.Sprintf("Merged Body Content: [%v]", mergedBodycontent))
	return mergedBodycontent, err
}

func (t *Table) GenerateKChunkKey(k []byte) []byte {
	owner := []byte(t.Owner)
	database := []byte(t.Database)
	table := []byte(t.tableName)
	id := k
	contentPrefix := BuildSwarmdbPrefix(owner, database, table, id)
	log.Debug(fmt.Sprintf("In GenerateChunkKey prefix Owner: [%s] DB: [%s], Table: [%s] ID: [%s] == [%v](%x)", owner, database, table, id, contentPrefix, contentPrefix))
	return contentPrefix
}

func BuildSwarmdbPrefix(owner []byte, database []byte, table []byte, id []byte) []byte {
	// TODO: add checks for valid type / length for building
	prepLen := len(owner) + len(database) + len(table) + len(id)
	prepBytes := make([]byte, prepLen)
	copy(prepBytes[0:], owner)
	copy(prepBytes[len(owner):], database)
	copy(prepBytes[len(owner)+len(database):], table)
	copy(prepBytes[len(owner)+len(database)+len(table):], id)
	prefix := crypto.Keccak256([]byte(prepBytes))

	log.Debug(fmt.Sprintf("In BuildSwarmdbPrefix prepstring[%s] and prefix[%s] in Bytes [%v] with size [%v]", prepBytes, prefix, []byte(prefix), len([]byte(prefix))))
	return (prefix)
}

func (t *Table) Get(u *SWARMDBUser, key []byte) (out []byte, ok bool, err error) {
	primaryColumnName := t.primaryColumnName
	if t.columns[primaryColumnName] == nil {
		return nil, false, &NoColumnError{tableName: t.tableName, tableOwner: t.Owner}
	}
	//t.swarmdb.kaddb.Open([]byte(t.Owner), []byte(t.tableName), []byte(t.primaryColumnName), t.encrypted)
	// fmt.Printf("\n GET key: (%s)%v\n", key, key)

	log.Debug("About to Get from DB")
	_, ok, err = t.columns[primaryColumnName].dbaccess.Get(u, key)
	if err != nil {
		log.Debug(fmt.Sprintf("[table:Get] dbaccess.Get %s", err.Error()))
		return nil, false, GenerateSWARMDBError(err, fmt.Sprintf("[table:Get] dbaccess.Get %s", err.Error()))
	}
	if !ok {
		return out, false, nil
	}
	log.Debug("About to Generate Key")

	chunkKey := t.GenerateKChunkKey(key)
	log.Debug(fmt.Sprintf("ChunkKey generated is: %s", chunkKey))
	contentReader, err := t.swarmdb.dbchunkstore.RetrieveKChunk(u, chunkKey)
	if bytes.Trim(contentReader, "\x00") == nil {
		log.Debug(fmt.Sprintf("RETURNING NIL CHUNK [%s]", out))
		return out, false, nil
	}
	if err != nil {
		return nil, false, GenerateSWARMDBError(err, fmt.Sprintf("[table:Get] RetrieveKChunk - Cannot Retrieve Chunk (%s): %s", contentReader, err.Error()))
	}
	log.Debug(fmt.Sprintf("[dbchunkstore:Get] returning [%s]", contentReader))
	fres := bytes.Trim(contentReader, "\x00")
	return fres, true, nil
}

func (t *Table) Delete(u *SWARMDBUser, key interface{}) (ok bool, err error) {
	k, err := convertJSONValueToKey(t.columns[t.primaryColumnName].columnType, key)
	if err != nil {
		return ok, GenerateSWARMDBError(err, fmt.Sprintf("[table:Delete] convertJSONValueToKey %s", err.Error()))
	}
	ok = false
	for _, ip := range t.columns {
		ok2, err := ip.dbaccess.Delete(u, k)
		if err != nil {
			return ok2, GenerateSWARMDBError(err, fmt.Sprintf("[table:Delete] dbaccess.Delete %s", err.Error()))
		}
		if ok2 {
			ok = true
		} else {
			// TODO: if the index delete fails, what should be done?
		}
	}
	// TODO: K node deletion
	return ok, nil
}

func (t *Table) StartBuffer(u *SWARMDBUser) (err error) {
	if t.buffered {
		t.FlushBuffer(u)
	} else {
		t.buffered = true
	}

	for _, ip := range t.columns {
		_, err := ip.dbaccess.StartBuffer(u)
		if err != nil {
			return GenerateSWARMDBError(err, fmt.Sprintf("[table:StartBuffer] dbaccess.StartBuffer %s", err.Error()))
		}
	}
	return nil
}

func (t *Table) FlushBuffer(u *SWARMDBUser) (err error) {
	for _, ip := range t.columns {
		_, err := ip.dbaccess.FlushBuffer(u)
		if err != nil {
			return GenerateSWARMDBError(err, fmt.Sprintf("[table:FlushBuffer] dbaccess.FlushBuffer %s", err.Error()))
		}
		roothash := ip.dbaccess.GetRootHash()
		ip.roothash = roothash
	}
	err = t.updateTableInfo(u)
	if err != nil {
		return GenerateSWARMDBError(err, fmt.Sprintf("[table:FlushBuffer] updateTableInfo %s", err.Error()))
	}
	return nil
}

func (t *Table) updateTableInfo(u *SWARMDBUser) (err error) {
	buf := make([]byte, 4096)
	i := 0
	for column_num, c := range t.columns {
		b := make([]byte, 1)

		copy(buf[2048+i*64:], column_num)

		b[0] = byte(c.primary)
		copy(buf[2048+i*64+26:], b)

		ctInt, _ := ColumnTypeToInt(c.columnType)
		b[0] = byte(ctInt)
		copy(buf[2048+i*64+28:], b)

		itInt := IndexTypeToInt(c.indexType)
		b[0] = byte(itInt)
		copy(buf[2048+i*64+30:], b)

		copy(buf[2048+i*64+32:], c.roothash)
		i++
	}
	//update encryption buffer bytes
	copy(buf[4000:4024], IntToByte(t.encrypted))
	swarmhash, err := t.swarmdb.StoreDBChunk(u, buf, t.encrypted)
	if err != nil {
		return GenerateSWARMDBError(err, fmt.Sprintf("[table:updateTableInfo] StoreDBChunk %s", err.Error()))
	}
	tblKey := t.swarmdb.GetTableKey(t.Owner, t.Database, t.tableName)
	err = t.swarmdb.StoreRootHash(u, []byte(tblKey), []byte(swarmhash))
	if err != nil {
		return GenerateSWARMDBError(err, fmt.Sprintf("[table:updateTableInfo] StoreRootHash %s", err.Error()))
	}
	return nil
}

func (t *Table) DescribeTable() (tblInfo map[string]Column, err error) {
	//var columns []Column
	log.Debug(fmt.Sprintf("DescribeTable with table [%+v] \n", t))
	tblInfo = make(map[string]Column)
	for cname, c := range t.columns {
		// fmt.Printf("\nProcessing column [%s]", cname)
		var cinfo Column
		cinfo.ColumnName = cname
		cinfo.IndexType = c.indexType
		cinfo.Primary = int(c.primary)
		cinfo.ColumnType = c.columnType
		if _, ok := tblInfo[cname]; ok { // if ok, would mean for some reason there are two cols named the same thing
			return tblInfo, &SWARMDBError{message: fmt.Sprintf("[table:DescribeTable] Duplicate column: [%s]", cname), ErrorCode: -1, ErrorMessage: "Table has Duplicate columns?"} //TODO: how would this occur?
		}
		tblInfo[cname] = cinfo
	}
	log.Debug(fmt.Sprintf("Returning from DescribeTable with table [%+v] \n", tblInfo))
	return tblInfo, nil
}

func (t *Table) Scan(u *SWARMDBUser, columnName string, ascending int) (rows []Row, err error) {
	column, err := t.getColumn(columnName)
	if err != nil {
		return rows, GenerateSWARMDBError(err, fmt.Sprintf("[table:Scan] getColumn %s", err.Error()))
	}
	if t.primaryColumnName != columnName {
		return rows, &SWARMDBError{message: fmt.Sprintf("[table:Scan] Skipping column %s", columnName), ErrorCode: -1, ErrorMessage: "Query Filters currently only supported on the primary key"}
	}

	var c OrderedDatabase
	switch ctype := column.dbaccess.(type) {
	case (OrderedDatabase):
		c = column.dbaccess.(OrderedDatabase)
	default:
		return rows, &SWARMDBError{message: fmt.Sprintf("Attempt to scan a table with a column [%s] with an unsupported index type [%s]", columnName, ctype), ErrorCode: 431, ErrorMessage: fmt.Sprintf("Scans on Column [%s] not unsupported due to indextype", columnName)}
	}

	if ascending == 1 {
		res, err := c.SeekFirst(u)
		if err == io.EOF {
			return rows, nil
		} else if err != nil {
			return rows, GenerateSWARMDBError(err, fmt.Sprintf("[table:Scan] SeekFirst %s ", err.Error()))
		} else {
			records := 0
			for k, v, err := res.Next(u); err == nil; k, v, err = res.Next(u) {
				//fmt.Printf("\n *int*> %d: K: %s V: %v \n", records, KeyToString(column.columnType, k), v)
				row, ok, errG := t.Get(u, k)
				if errG != nil {
					return rows, GenerateSWARMDBError(err, fmt.Sprintf("[table:Scan] Get %s", errG.Error()))
				}
				if ok {
					rowObj, errR := t.byteArrayToRow(row)
					if errR != nil {
						return rows, GenerateSWARMDBError(err, fmt.Sprintf("[table:Scan] byteArrayToRow [%s] bytearray to row: [%s]", v, errR.Error()))
					}
					// fmt.Printf("table Scan, row set: %+v\n", row)
					rows = append(rows, rowObj)
					records++
				}
			}
		}
	} else {
		res, err := c.SeekLast(u)
		if err != nil {
			return rows, GenerateSWARMDBError(err, fmt.Sprintf("[table:Scan] SeekLast %s", err.Error()))
		} else {
			records := 0
			for k, v, err := res.Prev(u); err == nil; k, v, err = res.Prev(u) {
				if false {
					fmt.Printf(" *int*> %d: K: %s V: %v\n", records, KeyToString(CT_STRING, k), KeyToString(column.columnType, v))
				}
				row, ok, errG := t.Get(u, k)
				if errG != nil {
					return rows, GenerateSWARMDBError(err, fmt.Sprintf("[table:Scan] Get %s", errG.Error()))
				}
				if ok {
					rowObj, errR := t.byteArrayToRow(row)
					if errR != nil {
						return rows, GenerateSWARMDBError(err, fmt.Sprintf("[table:Scan] byteArrayToRow %s", err.Error()))
					}
					rows = append(rows, rowObj)
					records++
				}
			}
		}
	}
	log.Debug(fmt.Sprintf("table Scan, rows returned: %+v\n", rows))
	return rows, nil
}

func (t *Table) Put(u *SWARMDBUser, row map[string]interface{}) (err error) {

	rawvalue, err := json.Marshal(row)
	if err != nil {
		return &SWARMDBError{message: fmt.Sprintf("[table:Put] Marshal %s", err.Error()), ErrorCode: 435, ErrorMessage: "Invalid Row Data"}
	}

	k := make([]byte, 32)

	for _, c := range t.columns {
		//fmt.Printf("\nProcessing a column %s and primary is %d", c.columnName, c.primary)
		if c.primary > 0 {

			pvalue, ok := row[t.primaryColumnName]
			if !ok {
				return &SWARMDBError{message: fmt.Sprintf("[table:Put] Primary key %s not specified in input", t.primaryColumnName), ErrorCode: 428, ErrorMessage: "Row missing primary key"}
			}
			k, err = convertJSONValueToKey(t.columns[t.primaryColumnName].columnType, pvalue)
			if err != nil {
				return GenerateSWARMDBError(err, fmt.Sprintf("[table:Put] convertJSONValueToKey %s", err.Error()))
			}

			//t.swarmdb.kaddb.Open([]byte(t.Owner), []byte(t.tableName), []byte(t.primaryColumnName), t.encrypted)
			/*
				khash, err := t.swarmdb.kaddb.Put(u, k, []byte(rawvalue)) // TODO: use u (sk) in kaddb
				if err != nil {
					return GenerateSWARMDBError(err, fmt.Sprintf("[table:Put] kaddb.Put %s", err.Error()))
				}
			*/
			v := []byte(rawvalue)
			sdata, errS := t.buildSdata(u, k, v)
			if errS != nil {
				return GenerateSWARMDBError(err, `[kademliadb:Put] buildSdata `+errS.Error())
			}

			hashVal := sdata[KNODE_START_CHUNKKEY:KNODE_END_CHUNKKEY] // 32 bytes
			log.Debug(fmt.Sprintf("Kademlia Encrypted Bit: %d", t.encrypted))
			errStore := t.swarmdb.dbchunkstore.StoreKChunk(u, hashVal, sdata, t.encrypted)
			if errStore != nil {
				return GenerateSWARMDBError(err, `[table:Put] StoreKChunk `+errStore.Error())
			}

			// fmt.Printf(" - primary  %s | %x\n", c.columnName, k)
			_, err = t.columns[c.columnName].dbaccess.Put(u, k, hashVal)
			if err != nil {
				return GenerateSWARMDBError(err, fmt.Sprintf("[table:Put] dbaccess.Put %s", err.Error()))
			}
		} else {
			k2 := make([]byte, 32)
			var errPvalue error
			pvalue, ok := row[c.columnName]
			if !ok {
				//OK b/c non-primary keys aren't required for rows
				continue
			}
			k2, errPvalue = convertJSONValueToKey(c.columnType, pvalue)
			if errPvalue != nil {
				return GenerateSWARMDBError(errPvalue, fmt.Sprintf("[table:Put] convertJSONValueToKey %s", errPvalue.Error()))
			}

			// fmt.Printf(" - secondary %s %x | %x\n", c.columnName, k2, k)
			_, err = t.columns[c.columnName].dbaccess.Put(u, k2, k)
			if err != nil {
				return GenerateSWARMDBError(err, fmt.Sprintf("[table:Put] dbaccess.Put %s", err.Error()))
			}
			//t.columns[c.columnName].dbaccess.Print()
		}
	}

	if t.buffered {
		// do nothing until FlushBuffer called
	} else {
		err = t.FlushBuffer(u)
		if err != nil {
			return GenerateSWARMDBError(err, fmt.Sprintf("[table:Put] FlushBuffer %s", err.Error()))
		}
	}
	return nil
}

func (t *Table) assignRowColumnTypes(rows []Row) ([]Row, error) {
	// fmt.Printf("assignRowColumnTypes: %v\n", t.columns)
	for _, row := range rows {
		for name, value := range row {
			if c, ok := t.columns[name]; ok {
				switch c.columnType {
				case CT_INTEGER:
					switch value.(type) {
					case int:
						row[name] = value.(int)
					case float64:
						row[name] = int(value.(float64))
						log.Debug(fmt.Sprintf("Converting value[%s] from float64 to int => [%d][%s]\n", value, row[name]))
					case string:
						f, err := strconv.ParseFloat(value.(string), 64)
						if err != nil {
							return rows, &SWARMDBError{message: fmt.Sprintf("[table:assignRowColumnTypes] TypeConversion Error: value [%v] does not match column type [%v]", value, t.columns[name].columnType), ErrorCode: 427, ErrorMessage: fmt.Sprintf("The value passed in for [%s] cannot be converted to integer type", name)}
						}
						row[name] = int(f)
					default:
						return rows, &SWARMDBError{message: fmt.Sprintf("[table:assignRowColumnTypes] TypeConversion Error: value [%v] does not match column type [%v]", value, t.columns[name].columnType), ErrorCode: 427, ErrorMessage: fmt.Sprintf("The value passed in for [%s] is of an unsupported type", name)}
					}
				case CT_STRING:
					switch value.(type) {
					case string:
						row[name] = value.(string)
					case int:
						row[name] = strconv.Itoa(value.(int))
					case float64:
						row[name] = strconv.FormatFloat(value.(float64), 'f', -1, 64)
						//TODO: handle err
						log.Debug(fmt.Sprintf("Converting value[%s] from float64 to string => [%s]\n", value, row[name]))
					default:
						return rows, &SWARMDBError{message: fmt.Sprintf("[table:assignRowColumnTypes] TypeConversion Error: value [%v] does not match column type [%v]", value, t.columns[name].columnType), ErrorCode: 427, ErrorMessage: fmt.Sprintf("The value passed in for [%s] is of an unsupported type", name)}
					}
				case CT_FLOAT:
					switch value.(type) {
					case float64:
						row[name] = value.(float64)
					case int:
						row[name] = float64(value.(int))
					case string:
						f, err := strconv.ParseFloat(value.(string), 64)
						if err != nil {
							return rows, &SWARMDBError{message: fmt.Sprintf("[table:assignRowColumnTypes] TypeConversion Error: value [%v] does not match column type [%v]", value, t.columns[name].columnType), ErrorCode: 427, ErrorMessage: fmt.Sprintf("The value passed in for [%s] is of an unsupported type", name)}
						}
						row[name] = f
					default:
						return rows, &SWARMDBError{message: fmt.Sprintf("[table:assignRowColumnTypes] TypeConversion Error: value [%v] does not match column type [%v]", value, t.columns[name].columnType), ErrorCode: 427, ErrorMessage: fmt.Sprintf("The value passed in for [%s] is of an unsupported type", name)}
					}
				//case CT_BLOB:
				// TODO: add blob support
				default:
					return rows, &SWARMDBError{message: fmt.Sprintf("[table:assignRowColumnTypes] Coltype not found", value, t.columns[name].columnType), ErrorCode: 427, ErrorMessage: fmt.Sprintf("The value passed in for [%s] is of an unsupported type", name)}
				}
			} else {
				return rows, &SWARMDBError{message: fmt.Sprintf("[table:assignRowColumnTypes] Invalid column %s", name), ErrorCode: 404, ErrorMessage: fmt.Sprintf("Column Does Not Exist in table definition: [%s]", name)}
			}
		}
	}
	return rows, nil
}

//TODO: could overload the operators so this isn't so clunky
func (t *Table) applyWhere(rawRows []Row, where Where) (outRows []Row, err error) {
	for _, row := range rawRows {
		if _, ok := row[where.Left]; !ok {
			continue
			//TODO: confirm we're not letting columns in the WHERE clause that don't exist in the table get this far
			//return outRows, &SWARMDBError{message:"Where clause col %s doesn't exist in table", ErrorCode:, ErrorMessage:""}
		}
		colType := t.columns[where.Left].columnType
		right, err := stringToColumnType(where.Right, colType)
		//TODO: Should we be checking that the type of where.Right matches the colType?
		if err != nil {
			return outRows, GenerateSWARMDBError(err, fmt.Sprintf("[table:applyWhere] stringToColumnType %s", err.Error()))
		}
		log.Debug(fmt.Sprintf("ColType [%d] and Right [%s]", colType, right))
		fRow := NewRow()
		switch where.Operator {
		case "=":
			switch colType {
			case CT_INTEGER:
				if row[where.Left].(int) == right.(int) {
					fRow = row
				}
			case CT_FLOAT:
				if row[where.Left].(float64) == right.(float64) {
					fRow = row
				}
			case CT_STRING:
				if row[where.Left].(string) == right.(string) {
					fRow = row
				}
			}
		case "<":
			switch colType {
			case CT_INTEGER:
				if row[where.Left].(int) < right.(int) {
					fRow = row
				}
			case CT_FLOAT:
				if row[where.Left].(float64) < right.(float64) {
					fRow = row
				}
			case CT_STRING:
				if row[where.Left].(string) < right.(string) {
					fRow = row
				}
			}
		case "<=":
			switch colType {
			case CT_INTEGER:
				if row[where.Left].(int) <= right.(int) {
					fRow = row
				}
			case CT_FLOAT:
				if row[where.Left].(float64) <= right.(float64) {
					fRow = row
				}
			case CT_STRING:
				if row[where.Left].(string) <= right.(string) {
					fRow = row
				}
			}
		case ">":
			switch colType {
			case CT_INTEGER:
				log.Debug("FOUND INTEGER")
				if row[where.Left].(int) > right.(int) {
					fRow = row
				} else {
					log.Debug("[]BAD NEWS!")
				}
			case CT_FLOAT:
				log.Debug("FOUND FLOAT")
				if row[where.Left].(float64) > right.(float64) {
					fRow = row
				}
			case CT_STRING:
				log.Debug("FOUND STRING")
				if row[where.Left].(string) > right.(string) {
					fRow = row
				}
			}
		case ">=":
			switch colType {
			case CT_INTEGER:
				if row[where.Left].(int) >= right.(int) {
					fRow = row
				}
			case CT_FLOAT:
				if row[where.Left].(float64) >= right.(float64) {
					fRow = row
				}
			case CT_STRING:
				if row[where.Left].(string) >= right.(string) {
					fRow = row
				}
			}
		case "!=":
			switch colType {
			case CT_INTEGER:
				if row[where.Left].(int) != right.(int) {
					fRow = row
				}
			case CT_FLOAT:
				if row[where.Left].(float64) != right.(float64) {
					fRow = row
				}
			case CT_STRING:
				if row[where.Left].(string) != right.(string) {
					fRow = row
				}
			}
		}
		outRows = append(outRows, fRow)
	}
	return outRows, nil
}
