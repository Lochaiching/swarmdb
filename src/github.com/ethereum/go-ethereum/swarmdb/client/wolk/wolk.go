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

// Wolk - SWARMDB Command line

// This combines the Otto Javascript parser (used in Ethereum Geth client) with a SQL parser
// that aims to specify a clear Javascript + Go bridge to { Kademlia, HashDB, B+tree } indexes.
package main

import (
	"fmt"
	otto "github.com/robertkrimen/otto"
	repl "github.com/robertkrimen/otto/repl"
	//"github.com/xwb1989/sqlparser"
	"encoding/json"
	"github.com/ethereum/go-ethereum/swarmdb"
	//"os"
	"strconv"
	"strings"
)

type WConnection struct {
	IP    string
	Port  int
	Owner string //owner of database + tables
	//Databases map[string]*Database // string = database name
	//IsOpen   bool
	SDBConnection *swarmdb.SWARMDBConnection
}

type WDatabase struct {
	//Tables   map[string]*Table //string = table name
	Encrypted   *int
	Name        string
	SDBDatabase *swarmdb.SWARMDBDatabase
}

type WTable struct {
	Name     string
	SDBTable *swarmdb.SWARMDBTable
}

var OpenConnections map[string]*WConnection

//var session *Session
//var DBC *swarmdb.SWARMDBConnection

//TODO: standardize "user-friendly" errs
//TODO: take out fmt.Printf stmts
func main() {

	vm := otto.New()
	var err error
	OpenConnections = make(map[string]*WConnection)

	vm.Set("openConnection", func(call otto.FunctionCall) otto.Value {

		connection := new(WConnection)

		//read and process parameter. arg0 is either empty, or a json of ip, port, owner, or any combo of those three.
		arg0 := call.Argument(0).String()
		if err = json.Unmarshal([]byte(arg0), &connection); err != nil {
			//swdberr := &swarmdb.SWARMDBError{ErrorCode: 400, ErrorMessage: `Bad JSON Supplied: [` + arg0 + `]`}
			//result, _ := vm.ToValue(swdberr.Print())
			//TODO: Error Checking
			//return result
		}

		//open up the connection
		dbc, err := swarmdb.NewSWARMDBConnection(connection.IP, connection.Port)
		if err != nil {
			errmsg := err.(*swarmdb.SWARMDBError).Print()
			if strings.Contains(errmsg, "Dial") {
				errmsg = errmsg + "Please start swarmdb first.\n"
			}
			fmt.Printf(errmsg)
			result, _ := vm.ToValue(errmsg)
			return result
		}
		connection.SDBConnection = &dbc
		if len(connection.Owner) == 0 {
			//no input tableowner means session owner is table owner
			connection.Owner = connection.SDBConnection.Owner
			//fmt.Printf("session's owner gotten from dbc: %v\n", session.Owner)
		} else {
			connection.SDBConnection.Owner = connection.Owner
		}
		if _, ok := OpenConnections[connection.Owner]; ok {
			//connection is already open. is that a problem?
		} else {
			OpenConnections[connection.Owner] = connection
		}

		fmt.Printf("connection opened\n")
		result, _ := vm.ToValue(connection)
		//TODO: Error Checking
		return result

	})

	vm.Set("openDatabase", func(call otto.FunctionCall) otto.Value {

		//read and process parameters. arg0 is connection object. arg1 is json of {database name and encrypted bit}
		arg0 := call.Argument(0).Object()
		arg1 := call.Argument(1).String()

		owner, _ := arg0.Get("Owner")
		ownerName, _ := owner.ToString()
		if _, ok := OpenConnections[ownerName]; !ok {
			swdberr := &swarmdb.SWARMDBError{ErrorCode: 000, ErrorMessage: `Connection is not open`}
			result, _ := vm.ToValue(swdberr.Print())
			//TODO: Error Checking
			return result
		}

		var db WDatabase
		if err = json.Unmarshal([]byte(arg1), &db); err != nil {
			swdberr := &swarmdb.SWARMDBError{ErrorCode: 400, ErrorMessage: `Bad JSON Supplied: [` + arg1 + `]`}
			result, _ := vm.ToValue(swdberr.Print())
			//TODO: Error Checking
			return result
		}
		if len(db.Name) == 0 {
			swdberr := &swarmdb.SWARMDBError{ErrorCode: 000, ErrorMessage: `No Database specified`}
			result, _ := vm.ToValue(swdberr.Print())
			//TODO: Error Checking
			return result
		}
		if db.Encrypted == nil {
			*db.Encrypted = 1 //just make it default if it's not there
		}

		//check if database has already been opened
		sdbc := OpenConnections[ownerName].SDBConnection
		if _, ok := sdbc.Databases[db.Name]; ok {
			swdberr := &swarmdb.SWARMDBError{ErrorCode: 000, ErrorMessage: `Database ` + ownerName + `|` + db.Name + ` has already been opened`}
			result, _ := vm.ToValue(swdberr.Print())
			//TODO: Error Checking
			return result
		}

		//open up the database
		sdbDatabase, err := sdbc.OpenDatabase(db.Name, *db.Encrypted)
		if err != nil {
			result, _ := vm.ToValue(err.(*swarmdb.SWARMDBError).Print())
			//TODO: Error Checking
			return result
		}

		//add it to the bookkeeping
		db.SDBDatabase = sdbDatabase //adds to our WDatabase
		fmt.Printf("database opened\n")
		result, _ := vm.ToValue(db)
		return result
	})

	/*

		//TODO: do we want to do this?
		vm.Set("closeConnection", func(call otto.FunctionCall) otto.Value {
			if !connection.IsOpen {
				swdberr := &swarmdb.SWARMDBError{ErrorCode: 0, ErrorMessage: `Connection Is Not Open`}
				result, _ := vm.ToValue(swdberr.Print())
				//TODO: Error Checking
				return result
			}

			//TODO: need to close the connection?? this is a stub:

				//err = DBC.Close(connection.DBTable)
				//if err != nil {
				//	fmt.Printf("Err: %v\n", err)
				//	return result
				//}

			result, _ := vm.ToValue("connection closed\n")
			//TODO: Error Checking
			return result

		})
	*/

	/*
		vm.Set("createTable", func(call otto.FunctionCall) otto.Value {

			if !connection.IsOpen {
				swdberr := &swarmdb.SWARMDBError{ErrorCode: 0, ErrorMessage: `Connection Is Not Open`}
				result, _ := vm.ToValue(swdberr.Print())
				//TODO: Error Checking
				return result
			}
			raw := call.Argument(0).String()

			var in []map[string]interface{}
			if err := json.Unmarshal([]byte(raw), &in); err != nil {
				errmsg := fmt.Sprintf(`Bad JSON Supplied: [%v]`, raw)
				swdberr := &swarmdb.SWARMDBError{ErrorCode: 400, ErrorMessage: errmsg}
				result, _ := vm.ToValue(swdberr.Print())
				//TODO: Error Checking
				return result
			}
			if len(in) == 0 {
				swdberr := &swarmdb.SWARMDBError{ErrorCode: 0, ErrorMessage: `Columns Not Specified`}
				result, _ := vm.ToValue(swdberr.Print())
				//TODO: Error Checking
				return result
			}
			//Bid check would go here if we were accepting bids

			var sCols []swarmdb.Column
			hasPrimary := false
			for _, col := range in {
				var sCol swarmdb.Column
				colbyte, err := json.Marshal(col) //.(map[string]interface{}))
				if err != nil {
					err = fmt.Errorf("ERR: %+v, %v\n", col, err)
					result, _ := vm.ToValue(err.Error())
					return result
				}
				colbyte = replaceSwarmDBTypes(colbyte)
				if err := json.Unmarshal(colbyte, &sCol); err != nil {
					err = fmt.Errorf("ERR: %+v, %v\n", col, err)
					result, _ := vm.ToValue(err.Error())
					//TODO: Error Checking
					return result
				}
				if len(sCol.ColumnName) == 0 {
					err = fmt.Errorf("ERR: %+v, needs column name\n", sCol)
					result, _ := vm.ToValue(err.Error())
					//TODO: Error Checking
					return result
				}
				if sCol.ColumnType == 0 {
					err = fmt.Errorf("ERR %+v, needs column type\n", sCol)
					result, _ := vm.ToValue(err.Error())
					//TODO: Error Checking
					return result
				}
				if sCol.IndexType == 0 {
					err = fmt.Errorf("ERR %+v, needs index type\n", sCol)
					result, _ := vm.ToValue(err.Error())
					//TODO: Error Checking
					return result
				}
				if sCol.Primary == 1 {
					hasPrimary = true
				}
				sCols = append(sCols, sCol)
			}
			if !hasPrimary {
				result, _ := vm.ToValue("ERR: needs primary key\n")
				//TODO: Error Checking
				return result
			}

			connection.DBTable, err = DBC.CreateTable(connection.Owner, connection.Database, connection.TableName, sCols, 0)
			if err != nil {
				result, _ := vm.ToValue(err.(*swarmdb.SWARMDBError).Print())
				//TODO: Error Checking
				return result
			}

			result, _ := vm.ToValue("table created\n")
			//TODO: Error Checking
			return result

			//tbl_name := call.Argument(0).String()       // e.g. "contacts"
			//tbl_descriptor := call.Argument(1).Object() // Export(  ) // {"column": "email", "type": "string", "primary": true, "index": "hash" }
			//column, _ := tbl_descriptor.Get("column")
			//column_string, _ := column.ToString()
			//primary, _ := tbl_descriptor.Get("primary")
			//primary_string, _ := primary.ToString
			//primary_bool, _ := primary.ToBoolean()
			//index, _ := tbl_descriptor.Get("index")
			//index_string, _ := index.ToString()

			//make columns into Columns
			//succ := swarmdb.CreateTable(index_string, tbl_name, column_string, primary_bool, index_string)

		})

	*/
	/*
		vm.Set("put", func(call otto.FunctionCall) otto.Value {

			if !connection.IsOpen {
				result, _ := vm.ToValue("ERR: Please open connection first\n")
				//TODO: Error Checking
				return result
			}
			raw := call.Argument(0).String()

			var in []map[string]interface{}
			if err := json.Unmarshal([]byte(raw), &in); err != nil {
				err = fmt.Errorf("ERR: %v\n", err)
				result, _ := vm.ToValue(err.Error())
				//TODO: Error Checking
				return result
			}
			if len(in) == 0 {
				result, _ := vm.ToValue("ERR: No rows specified\n")
				//TODO: Error Checking
				return result
			}
			//if we were doing bid check, it would be here

			var sRows []swarmdb.Row
			for _, row := range in {
				sRow := swarmdb.NewRow()
				sRow.Cells = row //.(map[string]interface{})
				sRows = append(sRows, sRow)
			}

			_, err := connection.DBTable.Put(sRows)
			//fmt.Printf("response: [%v] err: [%v]\n", response, err)
			if err != nil {
				result, _ := vm.ToValue(err.(*swarmdb.SWARMDBError).Print())
				//TODO: Error Checking
				return result
			}

			result, _ := vm.ToValue("row(s) added\n")
			//TODO: Error Checking
			return result


				//tablename := call.Argument(0).String()
				//jsonrecord := call.Argument(1).String()

				//test for correct json input of record
				//record := make(map[string]interface{})
				//if err := json.Unmarshal([]byte(jsonrecord), &record); err != nil {
					fmt.Printf(err.Error() + ". Please try again\n")
					result, _ := vm.ToValue(false)
					return result
				//}

				//err := swarmdb.AddRecord(tablename, jsonrecord)
				//if err != nil {
				//	result, _ := vm.ToValue(err.Error())
				//	return result
				//}

		})

		vm.Set("get", func(call otto.FunctionCall) otto.Value {
			if !connection.IsOpen {
				result, _ := vm.ToValue("ERR: Please open connection first\n")
				//TODO: Error Checking
				return result
			}
			raw := call.Argument(0).String()
			//fmt.Printf("key:\n%s\n", raw)

			dbResponse, err := connection.DBTable.Get(raw)
			if err != nil {
				result, _ := vm.ToValue(err.(*swarmdb.SWARMDBError).Print())
				//TODO: Error Checking
				return result
			}
			result, _ := vm.ToValue(dbResponse)
			//TODO: Error Checking
			return result

		})

		vm.Set("query", func(call otto.FunctionCall) otto.Value {

			if !connection.IsOpen {
				result, _ := vm.ToValue("ERR: Please open connection first\n")
				//TODO: Error Checking
				return result
			}
			raw := call.Argument(0).String()
			//fmt.Printf("raw:\n%s\n", raw)
			//fmt.Printf("connection used: %+v\n", connection)
			dbResponse, err := connection.DBTable.Query(raw)
			if err != nil {
				result, _ := vm.ToValue(err.(*swarmdb.SWARMDBError).Print())
				//TODO: Error Checking
				return result
			}
			result, _ := vm.ToValue(dbResponse)
			//TODO: Error Checking
			return result
		})

		//TODO:
		//vm.Set("delete", func(call otto.FunctionCall) otto.Value {
		//}

		vm.Set("quit", func(call otto.FunctionCall) otto.Value {
			os.Exit(0) //TODO this doesn't work
			return otto.Value{}
		})

		// VERY BASIC TEST CASES
		//vm.Run(`createTable("contacts", {"column": "email", "type": "string", "primary": true, "index": "hash" })`)
		//vm.Run(`add("contacts", { "email": "rodney@wolk.com", "name": "Rodney", "age": 38 })`)
		//vm.Run(`get("contacts", "rodney@wolk.com")`)
		//vm.Run(`get("email", "r256hashZZ7")`)
		//vm.Run(`query("select name, age from contacts where age >= 38");`)
	*/
	//run swarmdb prompt
	if err := repl.RunWithPrompt(vm, "swarmdb> "); err != nil {
		panic(err)
	}
}

//func NewConnection() *Connection {
//return &Connection{"wolkowner", "db", "", false, nil}
//}

func replaceSwarmDBTypes(in []byte) []byte {
	str := string(in)
	str = strings.Replace(str, `"IT_HASHTREE"`, strconv.Itoa(swarmdb.IT_HASHTREE), -1)
	str = strings.Replace(str, `"IT_BPLUSTREE"`, strconv.Itoa(swarmdb.IT_BPLUSTREE), -1)
	//str = strings.Replace(str, `"IT_FULLTEXT"`, strconv.Itoa(swarmdb.IT_FULLTEXT), -1)
	//str = strings.Replace(str, `"IT_FRACTALTREE"`, strconv.Itoa(swarmdb.IT_FRACTALTREE), -1)
	str = strings.Replace(str, `"CT_INTEGER"`, strconv.Itoa(swarmdb.CT_INTEGER), -1)
	str = strings.Replace(str, `"CT_STRING"`, strconv.Itoa(swarmdb.CT_STRING), -1)
	str = strings.Replace(str, `"CT_FLOAT"`, strconv.Itoa(swarmdb.CT_FLOAT), -1)
	//str = strings.Replace(str, `"CT_BLOB"`, strconv.Itoa(swarmdb.CT_BLOB), -1)
	return []byte(str)
}
