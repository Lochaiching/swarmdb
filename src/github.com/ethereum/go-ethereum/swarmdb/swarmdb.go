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
	//"encoding/binary"
	"encoding/json"
	//"errors"
	"fmt"
	"github.com/ethereum/go-ethereum/log"
	"github.com/ethereum/go-ethereum/swarmdb/log"
	"path/filepath"
	"reflect"
	"strconv"
)

const (
	OK_RESPONSE = "ok" // TODO: Compare to err
)

func NewSwarmDB(ensPath string, chunkDBPath string) (swdb *SwarmDB, err error) {
	sd := new(SwarmDB)
	sd.tables = make(map[string]*Table)
	chunkdbFileName := "chunk.db"
	dbChunkStoreFullPath := filepath.Join(chunkDBPath, chunkdbFileName)
	dbchunkstore, err := NewDBChunkStore(dbChunkStoreFullPath)
	if err != nil {
		return swdb, &SWARMDBError{message: `[swarmdb:NewSwarmDB] NewDBChunkStore ` + err.Error()}
	} else {
		sd.dbchunkstore = dbchunkstore
	}

	//default /tmp/ens.db
	ensdbFileName := "ens.db"
	ensdbFullPath := filepath.Join(ensPath, ensdbFileName)
	ens, errENS := NewENSSimulation(ensdbFullPath)
	if errENS != nil {
		return swdb, &SWARMDBError{message: `[swarmdb:NewSwarmDB] NewENSSimulation ` + errENS.Error()}
	} else {
		sd.ens = ens
	}

	kaddb, err := NewKademliaDB(dbchunkstore)
	if err != nil {
		return swdb, &SWARMDBError{message: `[swarmdb:NewSwarmDB] NewKademliaDB ` + err.Error()}
	} else {
		sd.kaddb = kaddb
	}

	sd.Logger = swarmdblog.NewLogger()
	return sd, nil
}

// DBChunkStore  API
func (self *SwarmDB) PrintDBChunk(columnType ColumnType, hashid []byte, c []byte) {
	self.dbchunkstore.PrintDBChunk(columnType, hashid, c)
}

func (self *SwarmDB) RetrieveDBChunk(u *SWARMDBUser, key []byte) (val []byte, err error) {
	val, err = self.dbchunkstore.RetrieveChunk(u, key)
	return val, err
}

func (self *SwarmDB) StoreDBChunk(u *SWARMDBUser, val []byte, encrypted int) (key []byte, err error) {
	key, err = self.dbchunkstore.StoreChunk(u, val, encrypted)
	return key, err
}

// ENSSimulation  API
func (self *SwarmDB) GetRootHash(u *SWARMDBUser, tblKey []byte /* GetTableKeyValue */) (roothash []byte, err error) {
	log.Debug(fmt.Sprintf("[GetRootHash] Getting Root Hash for (%s)[%x] ", tblKey, tblKey))
	return self.ens.GetRootHash(u, tblKey)
}

func (self *SwarmDB) StoreRootHash(u *SWARMDBUser, fullTableName []byte /* GetTableKey Value */, roothash []byte) (err error) {
	return self.ens.StoreRootHash(u, fullTableName, roothash)
}

// parse sql and return rows in bulk (order by, group by, etc.)
func (self *SwarmDB) QuerySelect(u *SWARMDBUser, query *QueryOption) (rows []Row, err error) {
	table, err := self.GetTable(u, query.TableOwner, query.Table, query.Encrypted)
	if err != nil {
		return rows, GenerateSWARMDBError(err, `[swarmdb:QuerySelect] GetTable `+err.Error())
	}

	//var rawRows []Row
	log.Debug("QueryTableOwner is: [%s]\n", query.TableOwner)
	colRows, err := self.Scan(u, query.TableOwner, query.Table, table.primaryColumnName, query.Ascending)
	if err != nil {
		return rows, GenerateSWARMDBError(err, `[swarmdb:QuerySelect] Scan `+err.Error())
	}
	// fmt.Printf("\nColRows = [%+v]", colRows)

	//apply WHERE
	whereRows, err := table.applyWhere(colRows, query.Where)
	if err != nil {
		return rows, GenerateSWARMDBError(err, `[swarmdb:QuerySelect] applyWhere `+err.Error())
	}
	// fmt.Printf("\nQuerySelect applied where rows: %+v\n", whereRows)

	// fmt.Printf("\nNumber of WHERE rows returned : %d", len(whereRows))
	//filter for requested columns
	for _, row := range whereRows {
		// fmt.Printf("QS b4 filterRowByColumns row: %+v\n", row)
		fRow := filterRowByColumns(&row, query.RequestColumns)
		// fmt.Printf("QS after filterRowByColumns row: %+v\n", fRow)
		if len(fRow.Cells) > 0 {
			rows = append(rows, fRow)
		}
	}
	// fmt.Printf("\nNumber of FINAL rows returned : %d", len(rows))

	//TODO: Put it in order for Ascending/GroupBy
	// fmt.Printf("\nQS returning: %+v\n", rows)
	return rows, nil
}

// Insert is for adding new data to the table
// example: 'INSERT INTO tablename (col1, col2) VALUES (val1, val2)
func (self *SwarmDB) QueryInsert(u *SWARMDBUser, query *QueryOption) (err error) {

	table, err := self.GetTable(u, query.TableOwner, query.Table, query.Encrypted)
	if err != nil {
		return &SWARMDBError{message: `[swarmdb:QueryInsert] GetTable ` + err.Error()}
	}
	for _, row := range query.Inserts {
		// check if primary column exists in Row
		if _, ok := row.Cells[table.primaryColumnName]; !ok {
			return &SWARMDBError{message: fmt.Sprintf("[swarmdb:QueryInsert] Insert row %+v needs primary column '%s' value", row, table.primaryColumnName)}
		}
		// check if Row already exists
		convertedKey, err := convertJSONValueToKey(table.columns[table.primaryColumnName].columnType, row.Cells[table.primaryColumnName])
		if err != nil {
			return &SWARMDBError{message: fmt.Sprintf("[swarmdb:QueryInsert] convertJSONValueToKey - %s", err.Error())}
		}
		existingByteRow, err := table.Get(u, convertedKey)
		if err != nil {
			return GenerateSWARMDBError(err, fmt.Sprintf("[swarmdb:QueryInsert] Get - %s", err.Error()))
		}
		if len(existingByteRow) > 0 {
			existingRow, errB := table.byteArrayToRow(existingByteRow)
			if errB != nil {
				return &SWARMDBError{message: fmt.Sprintf("[swarmdb:QueryInsert] byteArrayToRow - %s", errB.Error())}
			}
			return &SWARMDBError{message: fmt.Sprintf("[swarmdb:QueryInsert] Insert row key %s already exists: %+v", row.Cells[table.primaryColumnName], existingRow), ErrorCode: 434, ErrorMessage: fmt.Sprintf("Record with key [%s] already exists.  If you wish to modify, please use UPDATE SQL statement or PUT", convertedKey)}
		}
		// put the new Row in
		err = table.Put(u, row.Cells)
		if err != nil {
			return &SWARMDBError{message: fmt.Sprintf("[swarmdb:QueryInsert] Put %s", err.Error())}
		}
	}
	return nil
}

// Update is for modifying existing data in the table (can use a Where clause)
// example: 'UPDATE tablename SET col1=value1, col2=value2 WHERE col3 > 0'
func (self *SwarmDB) QueryUpdate(u *SWARMDBUser, query *QueryOption) (err error) {
	table, err := self.GetTable(u, query.TableOwner, query.Table, query.Encrypted)
	if err != nil {
		return &SWARMDBError{message: fmt.Sprintf("[swarmdb:QueryUpdate] GetTable %s", err.Error())}
	}

	// get all rows with Scan, using primary key column
	rawRows, err := self.Scan(u, query.TableOwner, query.Table, table.primaryColumnName, query.Ascending)
	if err != nil {
		return &SWARMDBError{message: fmt.Sprintf("[swarmdb:QueryUpdate] Scan %s", err.Error())}
	}

	// check to see if Update cols are in pulled set
	for colname, _ := range query.Update {
		if _, ok := table.columns[colname]; !ok {
			return &SWARMDBError{message: fmt.Sprintf("[swarmdb:QueryUpdate] Update SET column name %s is not in table", colname)}
		}
	}

	// apply WHERE clause
	filteredRows, err := table.applyWhere(rawRows, query.Where)
	if err != nil {
		return &SWARMDBError{message: fmt.Sprintf("[swarmdb:QueryUpdate] applyWhere %s", err.Error())}
	}

	// set the appropriate columns in filtered set
	for i, row := range filteredRows {
		for colname, value := range query.Update {
			if _, ok := row.Cells[colname]; !ok {
				return &SWARMDBError{message: fmt.Sprintf("[swarmdb:QueryUpdate] Update SET column name %s is not in filtered rows", colname)}
			}
			filteredRows[i].Cells[colname] = value
		}
	}

	// put the changed rows back into the table
	for _, row := range filteredRows {
		err := table.Put(u, row.Cells)
		if err != nil {
			return &SWARMDBError{message: fmt.Sprintf("[swarmdb:QueryUpdate] Put %s", err.Error())}
		}
	}

	return nil
}

//Delete is for deleting data rows (can use a Where clause, not just a key)
//example: 'DELETE FROM tablename WHERE col1 = value1'
func (self *SwarmDB) QueryDelete(u *SWARMDBUser, query *QueryOption) (err error) {

	table, err := self.GetTable(u, query.TableOwner, query.Table, query.Encrypted)
	if err != nil {
		return &SWARMDBError{message: fmt.Sprintf("[swarmdb:QueryDelete] GetTable %s", err.Error())}
	}

	//get all rows with Scan, using Where's specified col
	rawRows, err := self.Scan(u, query.TableOwner, query.Table, query.Where.Left, query.Ascending)
	if err != nil {
		return &SWARMDBError{message: fmt.Sprintf("[swarmdb:QueryDelete] Scan %s", err.Error())}
	}

	//apply WHERE clause
	filteredRows, err := table.applyWhere(rawRows, query.Where)
	if err != nil {
		return &SWARMDBError{message: fmt.Sprintf("[swarmdb:QueryDelete] applyWhere %s", err.Error())}
	}

	//delete the selected rows
	for _, row := range filteredRows {
		ok, err := table.Delete(u, row.Cells[table.primaryColumnName].(string))
		if err != nil {
			return &SWARMDBError{message: fmt.Sprintf("[swarmdb:QueryDelete] Delete %s", err.Error())}
		}
		if !ok {
			// TODO: if !ok, what should happen? return appropriate response -- number of records affected
		}
	}
	return nil
}

func (t *Table) assignRowColumnTypes(rows []Row) ([]Row, error) {
	// fmt.Printf("assignRowColumnTypes: %v\n", t.columns)
	for _, row := range rows {
		for name, value := range row.Cells {
			if c, ok := t.columns[name]; ok {
				switch c.columnType {
				case CT_INTEGER:
					switch value.(type) {
					case int:
						row.Cells[name] = value.(int)
					case float64:
						row.Cells[name] = int(value.(float64))
						log.Debug(fmt.Sprintf("Converting value[%s] from float64 to int => [%d][%s]\n", value, row.Cells[name]))
					default:
						return rows, &SWARMDBError{message: fmt.Sprintf("[swarmdb:assignRowColumnTypes] TypeConversion Error: value [%v] does not match column type [%v]", value, t.columns[name].columnType), ErrorCode: 427, ErrorMessage: "The value passed in for [%s] is not of the defined type"}
					}
				case CT_STRING:
					switch value.(type) {
					case string:
						row.Cells[name] = value.(string)
					case int:
						row.Cells[name] = strconv.Itoa(value.(int))
					case float64:
						row.Cells[name] = strconv.FormatFloat(value.(float64), 'f', -1, 64)
						//TODO: handle err
						log.Debug(fmt.Sprintf("Converting value[%s] from float64 to string => [%s]\n", value, row.Cells[name]))
					default:
						return rows, &SWARMDBError{message: fmt.Sprintf("[swarmdb:assignRowColumnTypes] TypeConversion Error: value [%v] does not match column type [%v]", value, t.columns[name].columnType), ErrorCode: 427, ErrorMessage: "The value passed in for [%s] is not of the defined type"}
					}
				case CT_FLOAT:
					switch value.(type) {
					case float64:
						row.Cells[name] = value.(float64)
					case int:
						row.Cells[name] = float64(value.(int))
					default:
						return rows, &SWARMDBError{message: fmt.Sprintf("[swarmdb:assignRowColumnTypes] TypeConversion Error: value [%v] does not match column type [%v]", value, t.columns[name].columnType), ErrorCode: 427, ErrorMessage: "The value passed in for [%s] is not of the defined type"}
					}
				//case CT_BLOB:
				// TODO: add blob support
				default:
					return rows, &SWARMDBError{message: fmt.Sprintf("[swarmdb:assignRowColumnTypes] Coltype not found", value, t.columns[name].columnType), ErrorCode: 427, ErrorMessage: "The value passed in for [%s] is not of the defined type"}
				}
			} else {
				return rows, &SWARMDBError{message: fmt.Sprintf("[swarmdb:assignRowColumnTypes] Invalid column %s", name), ErrorCode: 404, ErrorMessage: fmt.Sprintf("Column Does Not Exist in table definition: [%s]", name)}
			}
		}
	}
	return rows, nil
}

//TODO: could overload the operators so this isn't so clunky
func (t *Table) applyWhere(rawRows []Row, where Where) (outRows []Row, err error) {
	for _, row := range rawRows {
		if _, ok := row.Cells[where.Left]; !ok {
			continue
			//TODO: confirm we're not letting columns in the WHERE clause that don't exist in the table get this far
			//return outRows, &SWARMDBError{message:"Where clause col %s doesn't exist in table", ErrorCode:, ErrorMessage:""}
		}
		colType := t.columns[where.Left].columnType
		right, err := stringToColumnType(where.Right, colType)
		if err != nil {
			return outRows, GenerateSWARMDBError(err, fmt.Sprintf("[swarmdb:applyWhere] stringToColumnType %s", err.Error()))
		}
		fRow := NewRow()
		switch where.Operator {
		case "=":
			switch colType {
			case CT_INTEGER:
				if row.Cells[where.Left].(int) == right.(int) {
					fRow.Cells = row.Cells
				}
			case CT_FLOAT:
				if row.Cells[where.Left].(float64) == right.(float64) {
					fRow.Cells = row.Cells
				}
			case CT_STRING:
				if row.Cells[where.Left].(string) == right.(string) {
					fRow.Cells = row.Cells
				}
			}
		case "<":
			switch colType {
			case CT_INTEGER:
				if row.Cells[where.Left].(int) < right.(int) {
					fRow.Cells = row.Cells
				}
			case CT_FLOAT:
				if row.Cells[where.Left].(float64) < right.(float64) {
					fRow.Cells = row.Cells
				}
			case CT_STRING:
				if row.Cells[where.Left].(string) < right.(string) {
					fRow.Cells = row.Cells
				}
			}
		case "<=":
			switch colType {
			case CT_INTEGER:
				if row.Cells[where.Left].(int) <= right.(int) {
					fRow.Cells = row.Cells
				}
			case CT_FLOAT:
				if row.Cells[where.Left].(float64) <= right.(float64) {
					fRow.Cells = row.Cells
				}
			case CT_STRING:
				if row.Cells[where.Left].(string) <= right.(string) {
					fRow.Cells = row.Cells
				}
			}
		case ">":
			switch colType {
			case CT_INTEGER:
				if row.Cells[where.Left].(int) > right.(int) {
					fRow.Cells = row.Cells
				}
			case CT_FLOAT:
				if row.Cells[where.Left].(float64) > right.(float64) {
					fRow.Cells = row.Cells
				}
			case CT_STRING:
				if row.Cells[where.Left].(string) > right.(string) {
					fRow.Cells = row.Cells
				}
			}
		case ">=":
			switch colType {
			case CT_INTEGER:
				if row.Cells[where.Left].(int) >= right.(int) {
					fRow.Cells = row.Cells
				}
			case CT_FLOAT:
				if row.Cells[where.Left].(float64) >= right.(float64) {
					fRow.Cells = row.Cells
				}
			case CT_STRING:
				if row.Cells[where.Left].(string) >= right.(string) {
					fRow.Cells = row.Cells
				}
			}
		case "!=":
			switch colType {
			case CT_INTEGER:
				if row.Cells[where.Left].(int) != right.(int) {
					fRow.Cells = row.Cells
				}
			case CT_FLOAT:
				if row.Cells[where.Left].(float64) != right.(float64) {
					fRow.Cells = row.Cells
				}
			case CT_STRING:
				if row.Cells[where.Left].(string) != right.(string) {
					fRow.Cells = row.Cells
				}
			}
		}
		outRows = append(outRows, fRow)
	}
	return outRows, nil
}

func (self *SwarmDB) Query(u *SWARMDBUser, query *QueryOption) (rows []Row, err error) {
	switch query.Type {
	case "Select":
		rows, err := self.QuerySelect(u, query)
		if err != nil {
			return rows, GenerateSWARMDBError(err, fmt.Sprintf("[swarmdb:Query] QuerySelect %s", err.Error()))
		}
		if len(rows) == 0 {
			return rows, GenerateSWARMDBError(err, fmt.Sprintf("[swarmdb:Query] select query came back empty"))
		}
		return rows, nil
	case "Insert":
		err = self.QueryInsert(u, query)
		if err != nil {
			return rows, GenerateSWARMDBError(err, fmt.Sprintf("[swarmdb:Query] QueryInsert %s", err.Error()))
		}
		return rows, nil
	case "Update":
		err = self.QueryUpdate(u, query)
		if err != nil {
			return rows, GenerateSWARMDBError(err, fmt.Sprintf("[swarmdb:Query] QueryUpdate %s", err.Error()))
		}
		return rows, nil
	case "Delete":
		err = self.QueryDelete(u, query)
		if err != nil {
			return rows, GenerateSWARMDBError(err, fmt.Sprintf("[swarmdb:Query] QueryDelete %s", err.Error()))
		}
		return rows, nil
	}
	return rows, nil
}

func (self *SwarmDB) Scan(u *SWARMDBUser, tableOwnerID string, tableName string, columnName string, ascending int) (rows []Row, err error) {
	tblKey := self.GetTableKey(tableOwnerID, tableName)
	tbl, ok := self.tables[tblKey]
	if !ok {
		return rows, &SWARMDBError{message: fmt.Sprintf("[swarmdb:Scan] No such table to scan [%s] - [%s]", tableOwnerID, tblKey)}
	}
	rows, err = tbl.Scan(u, columnName, ascending)
	if err != nil {
		return rows, GenerateSWARMDBError(err, fmt.Sprintf("[swarmdb:Scan] Error doing table scan: [%s] %s", columnName, err.Error()))
	}
	rows, err = tbl.assignRowColumnTypes(rows)
	if err != nil {
		return rows, &SWARMDBError{message: fmt.Sprintf("[swarmdb:Scan] Error assigning column types to row values")}
	}
	// fmt.Printf("swarmdb Scan finished ok: %+v\n", rows)
	return rows, nil

}

func (self *SwarmDB) GetTable(u *SWARMDBUser, tableOwnerID string, tableName string, encrypted int) (tbl *Table, err error) {
	if len(tableName) == 0 {
		return tbl, &SWARMDBError{message: fmt.Sprintf("[swarmdb:GetTable] tablename missing "), ErrorCode: 426, ErrorMessage: "Table Name Missing"}
	}
	if len(tableOwnerID) == 0 {
		return tbl, &SWARMDBError{message: fmt.Sprintf("[swarmdb:GetTable] tableowner missing "), ErrorCode: 430, ErrorMessage: "Table Owner Missing"}
		//tableOwnerID = u.Address
	}
	tblKey := self.GetTableKey(tableOwnerID, tableName)
	// fmt.Printf("\nGetting Table [%s] with the Owner [%s] from TABLES [%v]", tableName, tableOwnerID, self.tables)
	if tbl, ok := self.tables[tblKey]; ok {
		log.Debug(fmt.Sprintf("Table[%v] with Owner [%s] found in tables, it is: %+v\n", tblKey, tableOwnerID, tbl))
		// fmt.Printf("\nprimary column name GetTable: %+v -> columns: %+v\n", tbl.columns, tbl.primaryColumnName)
		return tbl, nil
	} else {
		tbl = self.NewTable(tableOwnerID, tableName, encrypted) // TODO: check why encrypted is a parameter? -- Also, changing from NewTable(u.Address ... to NewTable(tableOwnerID
		err = tbl.OpenTable(u)
		if err != nil {
			wlkErr := GenerateSWARMDBError(err, fmt.Sprintf("[swarmdb:GetTable] OpenTable %s", err.Error()))
			return tbl, wlkErr
		}
		self.RegisterTable(u.Address, tableName, tbl)
		return tbl, nil
	}
}

// TODO: when there are errors, the error must be parsable make user friendly developer errors that can be trapped by Node.js, Go library, JS CLI
func (self *SwarmDB) SelectHandler(u *SWARMDBUser, data string) (resp string, err error) {

	log.Debug("SelectHandler Input: %s\n", data)
	// var rerr *RequestFormatError
	d, err := parseData(data)
	if err != nil {
		return resp, &SWARMDBError{message: fmt.Sprintf("[swarmdb:SelectHandler] parseData %s", err.Error()), ErrorCode: 417, ErrorMessage: "Request Not Parseable"}
	}

	var tblKey string
	var tbl *Table
	var tblInfo map[string]Column
	if d.RequestType != "CreateTable" {
		tblKey = self.GetTableKey(d.TableOwner, d.Table)
		tbl, err = self.GetTable(u, d.TableOwner, d.Table, d.Encrypted)
		log.Debug(fmt.Sprintf("GetTable returned table: [%+v]", tbl))
		if err != nil {
			return resp, GenerateSWARMDBError(err, fmt.Sprintf("[swarmdb:GetTable] OpenTable %s", err.Error()))
		}
		tblInfo, err = tbl.GetTableInfo()
		if err != nil {
			return resp, GenerateSWARMDBError(err, fmt.Sprintf("[swarmdb:SelectHandler] GetTableInfo %s", err.Error()))
		}
	}

	switch d.RequestType {
	case "CreateTable":
		if len(d.Table) == 0 || len(d.Columns) == 0 {
			return resp, &SWARMDBError{message: fmt.Sprintf("[swarmdb:SelectHandler] empty table and column"), ErrorCode: 417, ErrorMessage: "Invalid [CreateTable] Request: Missing Table and/or Columns"}
		}
		//TODO: Upon further review, could make a NewTable and then call this from tbl. ---
		_, err := self.CreateTable(u, d.Table, d.Columns, d.Encrypted)
		if err != nil {
			return resp, err
			//return resp, &SWARMDBError{message: fmt.Sprintf("[swarmdb:SelectHandler] CreateTable %s", err.Error()) }
		}
		return OK_RESPONSE, err
	case "Put":
		d.Rows, err = tbl.assignRowColumnTypes(d.Rows)
		//fmt.Printf("\nPut DATA: [%+v]\n", d)
		if err != nil {
			wlkErr := GenerateSWARMDBError(err, fmt.Sprintf("[swarmdb:SelectHandler] assignRowColumnTypes %s", err.Error()))
			return resp, wlkErr
		}

		//error checking for primary column, and valid columns
		for _, row := range d.Rows {
			log.Debug("checking row %v\n", row)
			if _, ok := row.Cells[tbl.primaryColumnName]; !ok {
				return resp, &SWARMDBError{message: fmt.Sprintf("[swarmdb:SelectHandler] Put row %+v needs primary column '%s' value", row, tbl.primaryColumnName), ErrorCode: 428, ErrorMessage: "Row missing primary key"}
			}
			for columnName, _ := range row.Cells {
				if _, ok := tblInfo[columnName]; !ok {
					return resp, &SWARMDBError{message: fmt.Sprintf("[swarmdb:SelectHandler] Put row %+v has unknown column %s", row, columnName), ErrorCode: 429, ErrorMessage: fmt.Sprintf("Row contains unknown column [%s]", columnName)}
				}
			}
			// check to see if row already exists in table (no overwriting, TODO: check if that is right??)
			/* TODO: we want to have PUT blindly update.  INSERT will fail on duplicate and need to confirm what to do if multiple rows attempted to be inserted and just some are dupes
			primaryColumnType := tbl.columns[tbl.primaryColumnName].columnType
			convertedKey, err := convertJSONValueToKey(primaryColumnType, row.Cells[tbl.primaryColumnName])
			if err != nil {
				wlkErr := GenerateSWARMDBError( err, fmt.Sprintf("[swarmdb:SelectHandler] convertJSONValueToKey %s", err.Error()) )
				return resp, wlkErr
			}
			validBytes, err := tbl.Get(u, convertedKey)
			if err == nil {
				validRow, err2 := tbl.byteArrayToRow(validBytes)
				if err2 != nil {
					return resp, &SWARMDBError{message: fmt.Sprintf("[swarmdb:SelectHandler] byteArrayToRow %s", err2.Error())}
				}
				err := &SWARMDBError{message: fmt.Sprintf("[swarmdb:SelectHandler] Row with that primary key already exists: %+v", validRow.Cells)}
				fmt.Printf("resp: [%v], err: [%v]\n", resp, err)
				return resp, err
			} else {
				fmt.Printf("good, row wasn't found\n")
			}
			*/
		}

		//put the rows in
		for _, row := range d.Rows {
			err = tbl.Put(u, row.Cells)
			if err != nil {
				wlkErr := GenerateSWARMDBError(err, fmt.Sprintf("[swarmdb:SelectHandler] Put %s", err.Error()))
				return resp, wlkErr
			}
		}
		return OK_RESPONSE, nil

	case "Get":
		if isNil(d.Key) {
			return resp, &SWARMDBError{message: fmt.Sprintf("[swarmdb:SelectHandler] Get - Missing Key"), ErrorCode: 433, ErrorMessage: "GET Request Missing Key"}
		}
		primaryColumnType := tbl.columns[tbl.primaryColumnName].columnType
		convertedKey, err := convertJSONValueToKey(primaryColumnType, d.Key)
		if err != nil {
			return resp, GenerateSWARMDBError(err, fmt.Sprintf("[swarmdb:SelectHandler] convertJSONValueToKey %s", err.Error()))
		}
		ret, err := tbl.Get(u, convertedKey)
		if err != nil {
			//fmt.Printf(fmt.Sprintf("[swarmdb:SelectHandler] Get %s", err.Error()))
			return resp, GenerateSWARMDBError(err, fmt.Sprintf("[swarmdb:SelectHandler] Get %s", err.Error()))
		}
		return string(ret), nil
	case "Delete":
		if isNil(d.Key) {
			return resp, GenerateSWARMDBError(err, fmt.Sprintf("[swarmdb:SelectHandler] Delete is Missing Key"))
		}
		_, err = tbl.Delete(u, d.Key)
		if err != nil {
			return resp, GenerateSWARMDBError(err, fmt.Sprintf("[swarmdb:SelectHandler] Delete %s", err.Error()))
		}
		return OK_RESPONSE, nil
	case "StartBuffer":
		err = tbl.StartBuffer(u)
		if err != nil {
			return resp, GenerateSWARMDBError(err, fmt.Sprintf("[swarmdb:SelectHandler] StartBuffer %s", err.Error()))
		}
		return OK_RESPONSE, nil
	case "FlushBuffer":
		err = tbl.FlushBuffer(u)
		if err != nil {
			return resp, GenerateSWARMDBError(err, fmt.Sprintf("[swarmdb:SelectHandler] FlushBuffer %s", err.Error()))
		}
		return OK_RESPONSE, nil
	case "Query":
		if len(d.RawQuery) == 0 {
			return resp, &SWARMDBError{message: fmt.Sprintf("[swarmdb:SelectHandler] RawQuery is blank"), ErrorCode: 425, ErrorMessage: "Invalid Query Request. Missing Rawquery"}
		}
		query, err := ParseQuery(d.RawQuery)
		query.Encrypted = d.Encrypted
		if err != nil {
			return resp, &SWARMDBError{message: fmt.Sprintf("[swarmdb:SelectHandler] ParseQuery [%s] %s", d.RawQuery, err.Error()), ErrorCode: 401, ErrorMessage: "SQL Parsing error: [SQLError in SQL]"}
		}
		if len(d.Table) == 0 {
			// fmt.Printf("Getting Table from Query rather than data obj\n")
			//TODO: check if empty even after query.Table check
			d.Table = query.Table //since table is specified in the query we do not have get it as a separate input
		}

		// fmt.Printf("right before GetTable, u: %v, d.TableOwner: %v, d.Table: %v \n", u, d.TableOwner, d.Table)
		query.TableOwner = d.TableOwner //probably should check the owner against the tableinfo owner here

		//fmt.Printf("Table info gotten: [%+v]\n", tblInfo)
		// fmt.Printf("QueryOption is: [%+v]\n", query)

		//checking validity of columns
		for _, reqCol := range query.RequestColumns {
			if _, ok := tblInfo[reqCol.ColumnName]; !ok {
				return resp, &SWARMDBError{message: fmt.Sprintf("[swarmdb:SelectHandler] Requested col [%s] does not exist in table [%+v]", reqCol.ColumnName, tblInfo), ErrorCode: 404, ErrorMessage: fmt.Sprintf("Column Does Not Exist in table definition: [%s]", reqCol.ColumnName)}
			}
		}

		//checking the Where clause
		if len(query.Where.Left) > 0 {
			if _, ok := tblInfo[query.Where.Left]; !ok {
				return resp, &SWARMDBError{message: fmt.Sprintf("[swarmdb:SelectHandler] Query col [%s] does not exist in table", query.Where.Left), ErrorCode: 432, ErrorMessage: fmt.Sprintf("WHERE Clause contains invalid column [%s]", query.Where.Left)}
			}

			//checking if the query is just a primary key Get
			if query.Where.Left == tbl.primaryColumnName && query.Where.Operator == "=" {
				// fmt.Printf("Calling Get from Query\n")
				convertedKey, err := convertJSONValueToKey(tbl.columns[tbl.primaryColumnName].columnType, query.Where.Right)
				if err != nil {
					wlkErr := GenerateSWARMDBError(err, fmt.Sprintf("[swarmdb:SelectHandler] convertJSONValueToKey %s", err.Error()))
					return resp, wlkErr
				}

				byteRow, err := tbl.Get(u, convertedKey)
				if err != nil {
					return resp, GenerateSWARMDBError(err, fmt.Sprintf("[swarmdb:SelectHandler] Get %s", err.Error()))
				}

				row, err := tbl.byteArrayToRow(byteRow)
				// fmt.Printf("Response row from Get: %s (%v)\n", row, row)
				if err != nil {
					return resp, GenerateSWARMDBError(err, fmt.Sprintf("[swarmdb:SelectHandler] byteArrayToRow %s", err.Error()))
				}

				filteredRow := filterRowByColumns(&row, query.RequestColumns)
				// fmt.Printf("\nResponse filteredrow from Get: %s (%v)", filteredRow.Cells, filteredRow.Cells)
				retJson, err := json.Marshal(filteredRow.Cells)
				if err != nil {
					return resp, GenerateSWARMDBError(err, fmt.Sprintf("[swarmdb:SelectHandler] Marshal %s", err.Error()))
				}
				return string(retJson), nil
			}
		}

		// process the query
		qRows, err := self.Query(u, &query)
		// fmt.Printf("\nQRows: [%+v]", qRows)
		if err != nil {
			return resp, GenerateSWARMDBError(err, fmt.Sprintf("[swarmdb:SelectHandler] Query [%+v] %s", query, err.Error()))
		}
		resp, err = rowDataToJson(qRows)
		if err != nil {
			return resp, GenerateSWARMDBError(err, fmt.Sprintf("[swarmdb:SelectHandler] rowDataToJson %s", err.Error()))
		}

		if resp == "null" {
			resp = OK_RESPONSE
		}
		return resp, nil
	case "GetTableInfo":
		tblcols, err := self.tables[tblKey].GetTableInfo()
		if err != nil {
			return resp, GenerateSWARMDBError(err, fmt.Sprintf("[swarmdb:SelectHandler] GetTableInfo %s", err.Error()))
		}
		tblinfo, err := json.Marshal(tblcols)
		if err != nil {
			return resp, GenerateSWARMDBError(err, fmt.Sprintf("[swarmdb:SelectHandler] Marshal %s", err.Error()))
		}
		return string(tblinfo), nil
	}
	return resp, &SWARMDBError{message: fmt.Sprintf("[swarmdb:SelectHandler] RequestType invalid: [%s]", d.RequestType), ErrorCode: 418, ErrorMessage: "Request Invalid"}
}

func parseData(data string) (*RequestOption, error) {
	udata := new(RequestOption)
	if err := json.Unmarshal([]byte(data), udata); err != nil {
		return nil, &SWARMDBError{message: fmt.Sprintf("[swarmdb:parseData] Unmarshal %s", err.Error()), ErrorCode: 432, ErrorMessage: "Unable to Parse Request"}
	}
	return udata, nil
}

func (t *Table) Scan(u *SWARMDBUser, columnName string, ascending int) (rows []Row, err error) {
	column, err := t.getColumn(columnName)
	if err != nil {
		return rows, GenerateSWARMDBError(err, fmt.Sprintf("[swarmdb:Scan] getColumn %s", err.Error()))
	}
	if t.primaryColumnName != columnName {
		return rows, &SWARMDBError{message: fmt.Sprintf("[swarmdb:Scan] Skipping column %s", columnName), ErrorCode: -1, ErrorMessage: "Query Filters currently only supported on the primary key"}
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
		if err != nil {
			return rows, GenerateSWARMDBError(err, fmt.Sprintf("[swarmdb:Scan] SeekFirst %s ", err.Error()))
		} else {
			records := 0
			for k, v, err := res.Next(u); err == nil; k, v, err = res.Next(u) {
				fmt.Printf("\n *int*> %d: K: %s V: %v \n", records, KeyToString(column.columnType, k), v)
				row, errG := t.Get(u, k)
				if errG != nil {
					return rows, GenerateSWARMDBError(err, fmt.Sprintf("[swarmdb:Scan] Get %s", errG.Error()))
				}
				rowObj, errR := t.byteArrayToRow(row)
				if errR != nil {
					return rows, GenerateSWARMDBError(err, fmt.Sprintf("[swarmdb:Scan] byteArrayToRow [%s] bytearray to row: [%s]", v, errR.Error()))
				}
				// fmt.Printf("table Scan, row set: %+v\n", row)
				rows = append(rows, rowObj)
				records++
			}
		}
	} else {
		res, err := c.SeekLast(u)
		if err != nil {
			return rows, GenerateSWARMDBError(err, fmt.Sprintf("[swarmdb:Scan] SeekLast %s", err.Error()))
		} else {
			records := 0
			for k, v, err := res.Prev(u); err == nil; k, v, err = res.Prev(u) {
				fmt.Printf(" *int*> %d: K: %s V: %v\n", records, KeyToString(CT_STRING, k), KeyToString(column.columnType, v))
				row, err := t.byteArrayToRow(v)
				if err != nil {
					return rows, GenerateSWARMDBError(err, fmt.Sprintf("[swarmdb:Scan] byteArrayToRow %s", err.Error()))
				}
				fmt.Printf("table Scan, row set: %+v\n", row)
				rows = append(rows, row)
				records++
			}
		}
	}
	fmt.Printf("table Scan, rows returned: %+v\n", rows)
	return rows, nil
}

func (self *SwarmDB) NewTable(ownerID string, tableName string, encrypted int) *Table {
	t := new(Table)
	t.swarmdb = self
	t.ownerID = ownerID
	t.tableName = tableName
	t.encrypted = encrypted
	t.columns = make(map[string]*ColumnInfo)

	return t
}

func (self *SwarmDB) RegisterTable(ownerID string, tableName string, t *Table) {
	// register the Table in SwarmDB
	tblKey := self.GetTableKey(ownerID, tableName)
	self.tables[tblKey] = t
}

//TODO: need to make sure the types of the columns are correct
func (swdb *SwarmDB) CreateTable(u *SWARMDBUser, tableName string, columns []Column, encrypted int) (tbl *Table, err error) {
	columnsMax := 30
	primaryColumnName := ""
	if len(columns) > columnsMax {
		return tbl, &SWARMDBError{message: fmt.Sprintf("[swarmdb:CreateTable] Max Allowed Columns for a table is %s and you submit %s", columnsMax, len(columns)), ErrorCode: 409, ErrorMessage: fmt.Sprintf("Max Allowed Columns exceeded - [%d] supplied, max is [MaxNumColumns]", len(columns), columnsMax)}
	}

	//error checking
	for _, columninfo := range columns {
		if columninfo.Primary > 0 {
			if len(primaryColumnName) > 0 {
				return tbl, &SWARMDBError{message: fmt.Sprintf("[swarmdb:CreateTable] More than one primary column"), ErrorCode: 406, ErrorMessage: "Multiple Primary keys specified in Create Table"}
			}
			primaryColumnName = columninfo.ColumnName
		}
		if !CheckColumnType(columninfo.ColumnType) {
			return tbl, &SWARMDBError{message: fmt.Sprintf("[swarmdb:CreateTable] bad columntype"), ErrorCode: 407, ErrorMessage: "Invalid ColumnType: [columnType]"}
		}
		if !CheckIndexType(columninfo.IndexType) {
			return tbl, &SWARMDBError{message: fmt.Sprintf("[swarmdb:CreateTable] bad indextype"), ErrorCode: 408, ErrorMessage: "Invalid IndexType: [indexType]"}
		}
	}
	if len(primaryColumnName) == 0 {
		return tbl, &SWARMDBError{message: fmt.Sprintf("[swarmdb:CreateTable] no primary column indicated"), ErrorCode: 405, ErrorMessage: "No Primary Key specified in Create Table"}
	}

	buf := make([]byte, 4096)
	fmt.Printf("\nCreating Table [%s] with the Owner [%s]", tableName, u.Address)
	tbl = swdb.NewTable(u.Address, tableName, encrypted)
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
	}

	//Could (Should?) be less bytes, but leaving space in case more is to be there
	copy(buf[4000:4024], IntToByte(tbl.encrypted))

	log.Debug(fmt.Sprintf("Storing Table with encrypted bit set to %d [%v]", tbl.encrypted), buf[4000:4024])
	swarmhash, err := swdb.StoreDBChunk(u, buf, tbl.encrypted)
	if err != nil {
		return tbl, GenerateSWARMDBError(err, fmt.Sprintf("[swarmdb:CreateTable] StoreDBChunk %s", err.Error()))
	}
	tbl.primaryColumnName = primaryColumnName

	log.Debug(fmt.Sprintf("CreateTable (ownerID [%s] tableName: [%s]) Primary: [%s] Roothash:[%x]\n", tbl.ownerID, tbl.tableName, tbl.primaryColumnName, swarmhash))
	tblKey := swdb.GetTableKey(tbl.ownerID, tbl.tableName)
	err = swdb.StoreRootHash(u, []byte(tblKey), []byte(swarmhash))
	if err != nil {
		return tbl, GenerateSWARMDBError(err, fmt.Sprintf("[swarmdb:CreateTable] StoreRootHash %s", err.Error()))
	}

	err = tbl.OpenTable(u)
	if err != nil {
		return tbl, GenerateSWARMDBError(err, fmt.Sprintf("[swarmdb:CreateTable] OpenTable %s", err.Error()))
	}
	swdb.RegisterTable(u.Address, tableName, tbl)
	return tbl, nil
}

func isNil(a interface{}) bool {
	if a == nil { // || reflect.ValueOf(a).IsNil()  {
		return true
	}
	return false
}

func (t *Table) OpenTable(u *SWARMDBUser) (err error) {

	t.columns = make(map[string]*ColumnInfo)

	/// get Table RootHash to  retrieve the table descriptor
	tblKey := t.swarmdb.GetTableKey(t.ownerID, t.tableName)
	log.Debug(fmt.Sprintf("[OpenTable] Getting RootHash for %s", tblKey))
	roothash, err := t.swarmdb.GetRootHash(u, []byte(tblKey))
	//fmt.Printf("opening table @ %s roothash [%x]\n", t.tableName, roothash)
	if err != nil {
		return GenerateSWARMDBError(err, fmt.Sprintf("[swarmdb:OpenTable] GetRootHash for table [%s]: %v", tblKey, err))
	}
	if len(roothash) == 0 {
		return &SWARMDBError{message: fmt.Sprintf("[swarmdb:OpenTable] Empty root hash"), ErrorCode: 403, ErrorMessage: fmt.Sprintf("Table Does Not Exist: TableName [%s] OwnerID [%s]", t.tableName, t.ownerID)}
	}
	setprimary := false
	columndata, err := t.swarmdb.RetrieveDBChunk(u, roothash)
	if err != nil {
		return GenerateSWARMDBError(err, fmt.Sprintf("[swarmdb:OpenTable] RetrieveDBChunk %s", err.Error()))
	}
	t.encrypted = BytesToInt(columndata[4000:4024])
	log.Debug(fmt.Sprintf("[swarmdb:OpenTable] t.encrypted [%d] buf [%+v]", t.encrypted, columndata[4000:4024]))
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
		columninfo.columnType = ColumnType(buf[28]) //:29
		columninfo.indexType = IndexType(buf[30])
		columninfo.roothash = buf[32:]
		secondary := false
		if columninfo.primary == 0 {
			secondary = true
		} else {
			primaryColumnType = columninfo.columnType // TODO: what if primary is stored *after* the secondary?  would break this..
		}
		// fmt.Printf("\n columnName: %s (%d) roothash: %x (secondary: %v) columnType: %d", columninfo.columnName, columninfo.primary, columninfo.roothash, secondary, columninfo.columnType)
		switch columninfo.indexType {
		case IT_BPLUSTREE:
			bplustree, err := NewBPlusTreeDB(u, *t.swarmdb, columninfo.roothash, ColumnType(columninfo.columnType), secondary, ColumnType(primaryColumnType))
			if err != nil {
				return GenerateSWARMDBError(err, fmt.Sprintf("[swarmdb:OpenTable] NewBPlusTreeDB %s", err.Error()))
			}
			columninfo.dbaccess = bplustree
		case IT_HASHTREE:
			columninfo.dbaccess, err = NewHashDB(u, columninfo.roothash, *t.swarmdb, ColumnType(columninfo.columnType))
			if err != nil {
				return GenerateSWARMDBError(err, fmt.Sprintf("[swarmdb:OpenTable] NewHashDB %s", err.Error()))
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
	log.Debug(fmt.Sprintf("OpenTable Encrypted (%d) tble [%s] with OwnerID of [%s] Returning with Columns: %v\n", t.encrypted, t.tableName, t.ownerID, t.columns))
	return nil
}

func convertJSONValueToKey(columnType ColumnType, pvalue interface{}) (k []byte, err error) {
	// fmt.Printf(" *** convertJSONValueToKey: CONVERT %v (columnType %v)\n", pvalue, columnType)
	switch svalue := pvalue.(type) {
	case (int):
		i := fmt.Sprintf("%d", svalue)
		k = StringToKey(columnType, i)
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
		k = StringToKey(columnType, f)
	case (string):
		k = StringToKey(columnType, svalue)
	default:
		return k, &SWARMDBError{message: fmt.Sprintf("[swarmdb:convertJSONValueToKey] Unknown Type: %v", reflect.TypeOf(svalue)), ErrorCode: 429, ErrorMessage: fmt.Sprintf("Column Value is an unsupported type of [%s]", svalue)}
	}
	return k, nil
}

func convertMapValuesToStrings(in map[string]interface{}) (map[string]string, error) {
	out := make(map[string]string)
	var err error
	for key, value := range in {
		switch value := value.(type) {
		case int:
			out[key] = strconv.Itoa(value)
		case int64:
			out[key] = strconv.FormatInt(value, 10)
		case float64:
			out[key] = strconv.FormatFloat(value, 'f', -1, 64)
		case string:
			out[key] = value
		default:
			err = fmt.Errorf("value %v has unknown type", value)
		}
	}
	if err != nil {
		return out, &SWARMDBError{message: fmt.Sprintf("[swarmdb:convertMapValuesToStrings] %s", err.Error())}
	}
	return out, nil
}

func (t *Table) Put(u *SWARMDBUser, row map[string]interface{}) (err error) {

	rawvalue, err := json.Marshal(row)
	if err != nil {
		return &SWARMDBError{message: fmt.Sprintf("[swarmdb:Put] Marshal %s", err.Error()), ErrorCode: 435, ErrorMessage: "Invalid Row Data"}
	}

	k := make([]byte, 32)

	for _, c := range t.columns {
		//fmt.Printf("\nProcessing a column %s and primary is %d", c.columnName, c.primary)
		if c.primary > 0 {

			pvalue, ok := row[t.primaryColumnName]
			if !ok {
				return &SWARMDBError{message: fmt.Sprintf("[swarmdb:Put] Primary key %s not specified in input", t.primaryColumnName), ErrorCode: 428, ErrorMessage: "Row missing primary key"}
			}
			k, err = convertJSONValueToKey(t.columns[t.primaryColumnName].columnType, pvalue)
			if err != nil {
				return GenerateSWARMDBError(err, fmt.Sprintf("[swarmdb:Put] convertJSONValueToKey %s", err.Error()))
			}

			t.swarmdb.kaddb.Open([]byte(t.ownerID), []byte(t.tableName), []byte(t.primaryColumnName), t.encrypted)
			khash, err := t.swarmdb.kaddb.Put(u, k, []byte(rawvalue)) // TODO: use u (sk) in kaddb
			if err != nil {
				return GenerateSWARMDBError(err, fmt.Sprintf("[swarmdb:Put] kaddb.Put %s", err.Error()))
			}
			// fmt.Printf(" - primary  %s | %x\n", c.columnName, k)
			_, err = t.columns[c.columnName].dbaccess.Put(u, k, khash)
			if err != nil {
				return GenerateSWARMDBError(err, fmt.Sprintf("[swarmdb:Put] dbaccess.Put %s", err.Error()))
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
				return GenerateSWARMDBError(errPvalue, fmt.Sprintf("[swarmdb:Put] convertJSONValueToKey %s", errPvalue.Error()))
			}

			// fmt.Printf(" - secondary %s %x | %x\n", c.columnName, k2, k)
			_, err = t.columns[c.columnName].dbaccess.Put(u, k2, k)
			if err != nil {
				return GenerateSWARMDBError(err, fmt.Sprintf("[swarmdb:Put] dbaccess.Put %s", err.Error()))
			}
			//t.columns[c.columnName].dbaccess.Print()
		}
	}

	if t.buffered {
		// do nothing until FlushBuffer called
	} else {
		err = t.FlushBuffer(u)
		if err != nil {
			return GenerateSWARMDBError(err, fmt.Sprintf("[swarmdb:Put] FlushBuffer %s", err.Error()))
		}
	}
	return nil
}

func (t *Table) getPrimaryColumn() (c *ColumnInfo, err error) {
	return t.getColumn(t.primaryColumnName)
}

func (t *Table) getColumn(columnName string) (c *ColumnInfo, err error) {
	if t.columns[columnName] == nil {
		return c, &NoColumnError{tableName: t.tableName, tableOwner: t.ownerID, columnName: columnName}
	}
	return t.columns[columnName], nil
}

func (t *Table) byteArrayToRow(byteData []byte) (out Row, err error) {
	row := NewRow()
	if err := json.Unmarshal(byteData, &row.Cells); err != nil {
		return row, &SWARMDBError{message: fmt.Sprintf("[swarmdb:byteArrayToRow] Unmarshal %s for [%s]", err.Error(), byteData), ErrorCode: 436, ErrorMessage: "Unable to converty byte array to Row Object"}
	}
	return row, nil
}

func (t *Table) Get(u *SWARMDBUser, key []byte) (out []byte, err error) {
	primaryColumnName := t.primaryColumnName
	if t.columns[primaryColumnName] == nil {
		return nil, &NoColumnError{tableName: t.tableName, tableOwner: t.ownerID}
	}
	t.swarmdb.kaddb.Open([]byte(t.ownerID), []byte(t.tableName), []byte(t.primaryColumnName), t.encrypted)
	// fmt.Printf("\n GET key: (%s)%v\n", key, key)

	_, ok, err := t.columns[primaryColumnName].dbaccess.Get(u, key)
	if err != nil {
		return nil, GenerateSWARMDBError(err, fmt.Sprintf("[swarmdb:Get] dbaccess.Get %s", err.Error()))
	}
	if !ok {
		return []byte(""), nil
	}
	// get value from kdb
	kres, err := t.swarmdb.kaddb.GetByKey(u, key)
	if err != nil {
		return out, GenerateSWARMDBError(err, fmt.Sprintf("[swarmdb:Get] kaddb.GetByKey %s", err.Error()))
	}
	fres := bytes.Trim(kres, "\x00")
	return fres, nil
}

func (t *Table) Delete(u *SWARMDBUser, key interface{}) (ok bool, err error) {
	k, err := convertJSONValueToKey(t.columns[t.primaryColumnName].columnType, key)
	if err != nil {
		return ok, GenerateSWARMDBError(err, fmt.Sprintf("[swarmdb:Delete] convertJSONValueToKey %s", err.Error()))
	}
	ok = false
	for _, ip := range t.columns {
		ok2, err := ip.dbaccess.Delete(u, k)
		if err != nil {
			return ok2, GenerateSWARMDBError(err, fmt.Sprintf("[swarmdb:Delete] dbaccess.Delete %s", err.Error()))
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
			return GenerateSWARMDBError(err, fmt.Sprintf("[swarmdb:StartBuffer] dbaccess.StartBuffer %s", err.Error()))
		}
	}
	return nil
}

func (t *Table) FlushBuffer(u *SWARMDBUser) (err error) {
	for _, ip := range t.columns {
		_, err := ip.dbaccess.FlushBuffer(u)
		if err != nil {
			return GenerateSWARMDBError(err, fmt.Sprintf("[swarmdb:FlushBuffer] dbaccess.FlushBuffer %s", err.Error()))
		}
		roothash := ip.dbaccess.GetRootHash()
		ip.roothash = roothash
	}
	err = t.updateTableInfo(u)
	if err != nil {
		return GenerateSWARMDBError(err, fmt.Sprintf("[swarmdb:FlushBuffer] updateTableInfo %s", err.Error()))
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

		b[0] = byte(c.columnType)
		copy(buf[2048+i*64+28:], b)

		b[0] = byte(c.indexType)
		copy(buf[2048+i*64+30:], b)

		copy(buf[2048+i*64+32:], c.roothash)
		i++
	}
	//update encryption buffer bytes
	copy(buf[4000:4024], IntToByte(t.encrypted))
	swarmhash, err := t.swarmdb.StoreDBChunk(u, buf, t.encrypted)
	if err != nil {
		return GenerateSWARMDBError(err, fmt.Sprintf("[swarmdb:updateTableInfo] StoreDBChunk %s", err.Error()))
	}
	tblKey := t.swarmdb.GetTableKey(t.ownerID, t.tableName)
	err = t.swarmdb.StoreRootHash(u, []byte(tblKey), []byte(swarmhash))
	if err != nil {
		return GenerateSWARMDBError(err, fmt.Sprintf("[swarmdb:updateTableInfo] StoreRootHash %s", err.Error()))
	}
	return nil
}

func (swdb *SwarmDB) GetTableKey(owner string, tableName string) (key string) {
	return fmt.Sprintf("%s|%s", owner, tableName)
}

func (t *Table) GetTableInfo() (tblInfo map[string]Column, err error) {
	//var columns []Column
	log.Debug(fmt.Sprintf("GetTableInfo with table [%+v] \n", t))
	tblInfo = make(map[string]Column)
	for cname, c := range t.columns {
		// fmt.Printf("\nProcessing column [%s]", cname)
		var cinfo Column
		cinfo.ColumnName = cname
		cinfo.IndexType = c.indexType
		cinfo.Primary = int(c.primary)
		cinfo.ColumnType = c.columnType
		if _, ok := tblInfo[cname]; ok { // if ok, would mean for some reason there are two cols named the same thing
			return tblInfo, &SWARMDBError{message: fmt.Sprintf("[swarmdb:GetTableInfo] Duplicate column: [%s]", cname), ErrorCode: -1, ErrorMessage: "Table has Duplicate columns?"} //TODO: how would this occur?
		}
		tblInfo[cname] = cinfo
	}
	log.Debug(fmt.Sprintf("Returning from GetTableInfo with table [%+v] \n", tblInfo))
	return tblInfo, nil
}
