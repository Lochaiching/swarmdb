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
	"os"
	"strconv"
	"strings"
)

type Session struct {
	TableOwner string //might need []byte or hex?
	Encrypted  *int
	//Replication *int
	TableName string
	IsOpen    bool
	DBTable   *swarmdb.SWARMDBTable
}

var session *Session
var DBC *swarmdb.SWARMDBConnection

//TODO: standardize "user-friendly" errs
//TODO: take out fmt.Printf stmts
func main() {

	vm := otto.New()
	session = NewSession()
	var err error

	dbc, err := swarmdb.NewSWARMDBConnection()
	DBC = &dbc
	if err != nil {
		fmt.Printf(err.(*swarmdb.SWARMDBError).Print())
		os.Exit(0)
	}

	vm.Set("openSession", func(call otto.FunctionCall) otto.Value {

		//if session.IsOpen {
		//	result, _ := vm.ToValue("Please close open session")
		//      return result
		//}

		//assume new session
		session = NewSession()

		arg0 := call.Argument(0).String()
		if err := json.Unmarshal([]byte(arg0), &session); err != nil {
			swdberr := &swarmdb.SWARMDBError{ErrorCode: 400, ErrorMessage: `Bad JSON Supplied: [` + arg0 + `]`}
			result, _ := vm.ToValue(swdberr.Print())
			//TODO: Error Checking
			return result
		}

		if session.Encrypted == nil {
			*session.Encrypted = 1 //if encrypted is omitted, defaults to yes
		}
		//if session.Replication == nil {
		//??
		//}
		if len(session.TableName) == 0 {
			swdberr := &swarmdb.SWARMDBError{ErrorCode: 426, ErrorMessage: `Table Name Missing`}
			result, _ := vm.ToValue(swdberr.Print())
			//TODO: Error Checking
			return result
		}
		if len(session.TableOwner) == 0 {
			//no input tableowner means session owner is table owner
			session.TableOwner = DBC.GetOwnerID()
			//fmt.Printf("session's tableowner gotten from dbc: %v\n", session.TableOwner)
		}

		//open up session with table specified
		session.DBTable, err = DBC.Open(session.TableName, session.TableOwner, *session.Encrypted)
		if err != nil {
			result, _ := vm.ToValue(err.(*swarmdb.SWARMDBError).Print())
			return result
		}

		session.IsOpen = true
		//fmt.Printf("session is: %+v\n", session)
		result, _ := vm.ToValue("session opened\n")
		//TODO: Error Checking
		return result

	})

	//TODO: do we want to do this?
	vm.Set("closeSession", func(call otto.FunctionCall) otto.Value {
		if !session.IsOpen {
			swdberr := &swarmdb.SWARMDBError{ErrorCode: 0, ErrorMessage: `Session Is Not Open`}
			result, _ := vm.ToValue(swdberr.Print())
			//TODO: Error Checking
			return result
		}

		//TODO: need to close the connection?? this is a stub:
		/*
			err = DBC.Close(session.DBTable)
			if err != nil {
				fmt.Printf("Err: %v\n", err)
				return result
			}
		*/
		result, _ := vm.ToValue("session closed\n")
		//TODO: Error Checking
		return result

	})

	vm.Set("createTable", func(call otto.FunctionCall) otto.Value {

		if !session.IsOpen {
			swdberr := &swarmdb.SWARMDBError{ErrorCode: 0, ErrorMessage: `Session Is Not Open`}
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

		//TODO: need to check for duplicate table here (need to hook up 'get table info' or use ens)

		session.DBTable, err = DBC.CreateTable(session.TableOwner, *session.Encrypted, session.TableName, sCols)
		if err != nil {
			result, _ := vm.ToValue(err.(*swarmdb.SWARMDBError).Print())
			//TODO: Error Checking
			return result
		}

		result, _ := vm.ToValue("table created\n")
		//TODO: Error Checking
		return result

		/*

			tbl_name := call.Argument(0).String()       // e.g. "contacts"
			tbl_descriptor := call.Argument(1).Object() // Export(  ) // {"column": "email", "type": "string", "primary": true, "index": "hash" }
			column, _ := tbl_descriptor.Get("column")
			column_string, _ := column.ToString()
			primary, _ := tbl_descriptor.Get("primary")
			primary_string, _ := primary.ToString
			primary_bool, _ := primary.ToBoolean()
			index, _ := tbl_descriptor.Get("index")
			index_string, _ := index.ToString()

			//make columns into Columns
			succ := swarmdb.CreateTable(index_string, tbl_name, column_string, primary_bool, index_string)
		*/
	})

	vm.Set("put", func(call otto.FunctionCall) otto.Value {

		if !session.IsOpen {
			result, _ := vm.ToValue("ERR: Please open session first\n")
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

		_, err := session.DBTable.Put(sRows)
		//fmt.Printf("response: [%v] err: [%v]\n", response, err)
		if err != nil {
			result, _ := vm.ToValue(err.(*swarmdb.SWARMDBError).Print())
			//TODO: Error Checking
			return result
		}

		result, _ := vm.ToValue("row(s) added\n")
		//TODO: Error Checking
		return result

		/*
			tablename := call.Argument(0).String()
			jsonrecord := call.Argument(1).String()

			//test for correct json input of record
			record := make(map[string]interface{})
			if err := json.Unmarshal([]byte(jsonrecord), &record); err != nil {
				fmt.Printf(err.Error() + ". Please try again\n")
				result, _ := vm.ToValue(false)
				return result
			}

			//err := swarmdb.AddRecord(tablename, jsonrecord)
			if err != nil {
				result, _ := vm.ToValue(err.Error())
				return result
			}
		*/
	})

	vm.Set("get", func(call otto.FunctionCall) otto.Value {
		if !session.IsOpen {
			result, _ := vm.ToValue("ERR: Please open session first\n")
			//TODO: Error Checking
			return result
		}
		raw := call.Argument(0).String()
		//fmt.Printf("key:\n%s\n", raw)

		dbResponse, err := session.DBTable.Get(raw)
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

		if !session.IsOpen {
			result, _ := vm.ToValue("ERR: Please open session first\n")
			//TODO: Error Checking
			return result
		}
		raw := call.Argument(0).String()
		//fmt.Printf("raw:\n%s\n", raw)
		//fmt.Printf("session used: %+v\n", session)
		dbResponse, err := session.DBTable.Query(raw)
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

	//run swarmdb prompt
	if err := repl.RunWithPrompt(vm, "swarmdb> "); err != nil {
		panic(err)
	}
}

func NewSession() *Session {
	return &Session{"", nil, "", false, nil}
}

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
