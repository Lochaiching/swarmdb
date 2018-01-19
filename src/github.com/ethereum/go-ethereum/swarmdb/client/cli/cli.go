/*
Wolk - SWARMDB Command line

This is a STUB that combines the Otto Javascript parser (used in Ethereum Geth client) with a SQL parser
that aims to specify a clear Javascript + Go bridge to { Kademlia, HashDB, B+tree } indexes.

Setup:
 go get -v github.com/robertkrimen/otto/otto
 go get -v github.com/robertkrimen/otto/repl
 go get -v github.com/xwb1989/sqlparser


Major TODOs:
 Rodney + Mayumi to connect {SWARMDB_createTable, SWARMDB_add, SWARMDB_get } to dispatcher based on Table descriptor
 Alina to develop JSON object (in SWARMDB_get) + JSON array  (in SWARMDB_query) interface

Current stub demo:
[sourabh@www6001 swarmdb]$ ./swarmdb
SWARMDB_createTable(contacts, column: email primary: true index: hash)
SWARMDB_add(contacts,  key: email value: rodney@wolk.com key: name value: Rodney key: age value: 38)
SWARMDB_get(contacts, rodney@wolk.com)
SWARMDB_query(contacts,  field 0: name field 1: age)
swarmdb> query("select name, age, email from contacts")
SWARMDB_query(contacts,  field 0: name field 1: age field 2: email)
true
swarmdb> get("contacts", "alina@wolk.com")
SWARMDB_get(contacts, alina@wolk.com)
true
swarmdb> createTable("contacts", {"column": "email", "primary": true, "index": "btree"})
SWARMDB_createTable(contacts, column: email primary: true index: btree)
true
swarmdb> add("contacts", {"email": "alina@wolk.com", "gender": "F", "name": "Alina Chu"})
SWARMDB_add(contacts,  key: email value: alina@wolk.com key: gender value: F key: name value: Alina Chu)
true
swarmdb> quit();
*/

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

type IncomingInfo struct {
	//Bid  float64
	Info []interface{} //usually []map[string]interface{}
}

type IncomingGet struct {
	//Bid float64
	Key string //must be primary key value
}

var session *Session
var DBC *swarmdb.SWARMDBConnection

//TODO: for production, take out TEST_NOCONNECT
var TEST_NOCONNECT bool

//TODO: standardize "user-friendly" errs
//TODO: take out fmt.Printf stmts
func main() {

	vm := otto.New()
	session = NewSession()
	var err error

	TEST_NOCONNECT = false

	if !TEST_NOCONNECT {
		dbc, err := swarmdb.NewSWARMDBConnection()
		DBC = &dbc
		if err != nil {
			fmt.Printf("Err: %v\n", err)
			os.Exit(0)
		}
	} else {
		fmt.Printf("DBC, err := swarmdb.NewSWARMDBConnection()\n")
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
			result, _ := vm.ToValue(err.Error())
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
			result, _ := vm.ToValue("No table name")
			//TODO: Error Checking
			return result
		}
		if len(session.TableOwner) == 0 {
			//no input tableowner means session owner is table owner
			if !(TEST_NOCONNECT) {
				session.TableOwner = DBC.GetOwnerID()
				fmt.Printf("session's tableowner gotten from dbc: %v\n", session.TableOwner)
			} else {
				session.TableOwner = "faketableowner"
			}
		} else {
			fmt.Printf("session's tableowner is: %v\n", session.TableOwner)
		}

		//open up session with table specified
		if !(TEST_NOCONNECT) {
			session.DBTable, err = DBC.Open(session.TableName, session.TableOwner, *session.Encrypted)
		} else {
			fmt.Printf("DBC.Open(%v, %v)\n", session.TableName, *session.Encrypted)
		}
		fmt.Printf("opening session...\n")

		if err != nil {
			result, _ := vm.ToValue(err.Error())
			return result
		}

		fmt.Printf("Session opened.\n")
		session.IsOpen = true
		fmt.Printf("session is: %+v\n", session)
		result, _ := vm.ToValue(true)
		//TODO: Error Checking
		return result

	})

	vm.Set("closeSession", func(call otto.FunctionCall) otto.Value {
		if !session.IsOpen {
			result, _ := vm.ToValue("No open session to close")
			//TODO: Error Checking
			return result
		}

		//need to close the connection?? this is a stub:
		/*
			err = DBC.Close(session.DBTable)
			if err != nil {
				fmt.Printf("Err: %v\n", err)
				return result
			}
		*/
		result, _ := vm.ToValue(true)
		//TODO: Error Checking
		return result

	})

	vm.Set("createTable", func(call otto.FunctionCall) otto.Value {

		if !session.IsOpen {
			result, _ := vm.ToValue("Please open session first.")
			//TODO: Error Checking
			return result
		}
		raw := call.Argument(0).String()
		fmt.Printf("raw:\n%s\n", raw)

		var in IncomingInfo
		if err := json.Unmarshal([]byte(raw), &in); err != nil {
			result, _ := vm.ToValue(err.Error())
			//TODO: Error Checking
			return result
		}
		fmt.Printf("incoming table:\n%+v\n", in)

		if len(in.Info) == 0 {
			result, _ := vm.ToValue("No table columns specified")
			//TODO: Error Checking
			return result
		}

		//if in.Bid == float64(0) {
		//	result, _ := vm.ToValue("Cannot have 0 bid")
		//TODO: Error Checking
		//	return result
		//}

		var sCols []swarmdb.Column
		hasPrimary := false
		for _, col := range in.Info {
			var sCol swarmdb.Column
			colbyte, _ := json.Marshal(col.(map[string]interface{}))
			//TODO: Error Checking
			colbyte = replaceSwarmDBTypes(colbyte)
			if err := json.Unmarshal(colbyte, &sCol); err != nil {
				result, _ := vm.ToValue(err.Error())
				//TODO: Error Checking
				return result
			}
			if len(sCol.ColumnName) == 0 {
				result, _ := vm.ToValue("needs column name")
				//TODO: Error Checking
				return result
			}
			if sCol.ColumnType == 0 {
				result, _ := vm.ToValue("needs column type")
				//TODO: Error Checking
				return result
			}
			if sCol.IndexType == 0 {
				result, _ := vm.ToValue("needs index type")
				//TODO: Error Checking
				return result
			}
			if sCol.Primary == 1 {
				hasPrimary = true
			}
			sCols = append(sCols, sCol)
		}
		if !hasPrimary {
			result, _ := vm.ToValue("needs primary key")
			//TODO: Error Checking
			return result
		}

		//need to check for duplicate table here (need to hook up 'get table info' or use ens)

		if !TEST_NOCONNECT {
			session.DBTable, err = DBC.CreateTable(session.TableOwner, *session.Encrypted, session.TableName, sCols)
			if err != nil {
				result, _ := vm.ToValue(err.Error())
				//TODO: Error Checking
				return result
			}
		} else {
			fmt.Printf("DBC.CreateTable(%v, %v, %v, %+v)\n", session.TableOwner, *session.Encrypted, session.TableName, sCols)
		}

		fmt.Printf("Success.\n")
		result, _ := vm.ToValue(true)
		//TODO: Error Checking
		return result

		/*

			tbl_name := call.Argument(0).String()       // e.g. "contacts"
			tbl_descriptor := call.Argument(1).Object() // Export(  ) // {"column": "email", "type": "string", "primary": true, "index": "hash" }
			column, _ := tbl_descriptor.Get("column")
			//TODO: Error Checking
			column_string, _ := column.ToString()
			//TODO: Error Checking
			primary, _ := tbl_descriptor.Get("primary")
			//TODO: Error Checking
			primary_string, _ := primary.ToString
			//TODO: Error Checking
			primary_bool, _ := primary.ToBoolean()
			//TODO: Error Checking
			index, _ := tbl_descriptor.Get("index")
			//TODO: Error Checking
			index_string, _ := index.ToString()
			//TODO: Error Checking

			//make columns into Columns
			succ := swarmdb.CreateTable(index_string, tbl_name, column_string, primary_bool, index_string)
		*/
	})

	// swarmdb> add("contacts", { "email": "rodney@wolk.com", "name": "Rodney", "age": 38 })
	// true

	vm.Set("addRow", func(call otto.FunctionCall) otto.Value {

		if !session.IsOpen {
			result, _ := vm.ToValue("Please open session first.")
			//TODO: Error Checking
			return result
		}
		raw := call.Argument(0).String()
		fmt.Printf("raw:\n%s\n", raw)

		var in IncomingInfo
		if err := json.Unmarshal([]byte(raw), &in); err != nil {
			result, _ := vm.ToValue(err.Error())
			//TODO: Error Checking
			return result
		}
		fmt.Printf("incoming rows:\n%+v\n", in)
		if len(in.Info) == 0 {
			result, _ := vm.ToValue("No rows specified")
			//TODO: Error Checking
			return result
		}
		//if in.Bid == float64(0) {
		//	result, _ := vm.ToValue("Cannot have 0 bid")
		//TODO: Error Checking
		//	return result
		//}

		var sRows []swarmdb.Row
		for _, row := range in.Info {
			sRow := swarmdb.NewRow()
			sRow.Cells = row.(map[string]interface{})
			/*
				if err := json.Unmarshal([]byte(row.(string)), &sRow.Cells); err != nil {
					result, _ := vm.ToValue(err.Error())
					return result
				}*/

			//should check for primary key in each row?
			//should check for duplicate rows?
			//should check for rows that already exist here? -- or kick the can down the line?
			sRows = append(sRows, sRow)
		}

		var response string
		if !TEST_NOCONNECT {
			response, err = session.DBTable.Put(sRows)
			if err != nil {
				result, _ := vm.ToValue(err.Error())
				//TODO: Error Checking
				return result
			}
		} else {
			fmt.Printf("session.DBTable.Put(%+v)\n", sRows)
		}

		result, _ := vm.ToValue(response)
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

	// swarmdb> get("contacts", "rodney@wolk.com")
	// { "email": "rodney@wolk.com", "name": "Rodney", "age": 38 }
	vm.Set("get", func(call otto.FunctionCall) otto.Value {
		if !session.IsOpen {
			result, _ := vm.ToValue("Please open session first.")
			//TODO: Error Checking
			return result
		}
		raw := call.Argument(0).String()
		fmt.Printf("key:\n%s\n", raw)

		if !TEST_NOCONNECT {
			dbResponse, err := session.DBTable.Get(raw)
			if err != nil {
				result, _ := vm.ToValue(err.Error())
				//TODO: Error Checking
				return result
			}
			result, _ := vm.ToValue(dbResponse)
			//TODO: Error Checking
			return result

		} else {
			fmt.Printf("session.DBTable.Get(%s)\n", raw)
			result, _ := vm.ToValue("test response")
			//TODO: Error Checking
			return result
		}

	})

	// swarmdb> query("select name, age from contacts where email = 'rodney@wolk.com'")
	// records should come back with in an array
	// [ {"name":"Sourabh Niyogi", "age":45 }, {"name":"Francesca Niyogi", "age":49} ...]
	vm.Set("query", func(call otto.FunctionCall) otto.Value {

		if !session.IsOpen {
			result, _ := vm.ToValue("Please open session first.")
			//TODO: Error Checking
			return result
		}
		raw := call.Argument(0).String()
		fmt.Printf("raw:\n%s\n", raw)
		fmt.Printf("session used: %+v\n", session)
		if !TEST_NOCONNECT {
			dbResponse, err := session.DBTable.Query(raw)
			if err != nil {
				result, _ := vm.ToValue(err.Error())
				//TODO: Error Checking
				return result
			}
			result, _ := vm.ToValue(dbResponse)
			//TODO: Error Checking
			return result

		} else {
			fmt.Printf("session.DBTable.Query(%s)\n", raw)
			result, _ := vm.ToValue("test response")
			//TODO: Error Checking
			return result
		}

	})

	//TODO:
	//vm.Set("delete", func(call otto.FunctionCall) otto.Value {
	//}

	vm.Set("quit", func(call otto.FunctionCall) otto.Value {
		os.Exit(0)
		return otto.Value{}
	})

	// VERY BASIC TEST CASES
	//vm.Run(`createTable("contacts", {"column": "email", "type": "string", "primary": true, "index": "hash" })`)
	//vm.Run(`add("contacts", { "email": "rodney@wolk.com", "name": "Rodney", "age": 38 })`)
	//vm.Run(`get("contacts", "rodney@wolk.com")`)
	vm.Run(`get("email", "r256hashZZ7")`)
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
