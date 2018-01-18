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
	TableOwner  string //might need []byte or hex?
	Encrypted   *int
	Replication *int
	TableName   string
	IsOpen      bool
	DBTable     *swarmdb.SWARMDBTable
}

type IncomingInfo struct {
	Bid  float64
	Info []interface{} //usually []map[string]interface{}
}

type IncomingGet struct {
	Bid float64
	Key string //must be primary key value
}

var session *Session
var DBC *swarmdb.SWARMDBConnection

func main() {

	vm := otto.New()
	session = NewSession()
	var err error
	
		DBC, err := swarmdb.NewSWARMDBConnection()
		if err != nil {
			fmt.Printf("Err: %v\n", err)
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
			result, _ := vm.ToValue(err.Error())
			return result
		}

		if session.Encrypted == nil {
			*session.Encrypted = 1 //if encrypted is omitted, defaults to yes
		}
		if session.Replication == nil {
			//??
		}
		if len(session.TableName) == 0 {
			result, _ := vm.ToValue("No table name")
			return result
		}

		//open up session with table specified
		session.DBTable, err = DBC.Open(session.TableName)
		fmt.Printf("opening session...\n")

		if err != nil {
			result, _ := vm.ToValue(err.Error())
			return result
		}

		if len(session.TableOwner) == 0 {
			//no input tableowner means session owner is table owner
			session.TableOwner = DBC.GetOwnerID()
			//session.TableOwner = "0xfaketableowner"
		}

		fmt.Printf("Session opened.\n")
		session.IsOpen = true
		fmt.Printf("session is: %+v\n", session)
		result, _ := vm.ToValue(true)
		return result

	})

	vm.Set("closeSession", func(call otto.FunctionCall) otto.Value {
		if !session.IsOpen {
			result, _ := vm.ToValue("No open session to close")
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
		return result

	})

	vm.Set("createTable", func(call otto.FunctionCall) otto.Value {

		if !session.IsOpen {
			result, _ := vm.ToValue("Please open session first.")
			return result
		}
		raw := call.Argument(0).String()
		fmt.Printf("raw:\n%s\n", raw)

		var in IncomingInfo
		if err := json.Unmarshal([]byte(raw), &in); err != nil {
			result, _ := vm.ToValue(err.Error())
			return result
		}
		fmt.Printf("incoming table:\n%+v\n", in)

		if len(in.Info) == 0 {
			result, _ := vm.ToValue("No table columns specified")
			return result
		}

		if in.Bid == float64(0) {
			result, _ := vm.ToValue("Cannot have 0 bid")
			return result
		}

		var sCols []swarmdb.Column
		hasPrimary := false
		for _, col := range in.Info {
			var sCol swarmdb.Column
			colbyte, _ := json.Marshal(col.(map[string]interface{}))
			colbyte = replaceSwarmDBTypes(colbyte)
			if err := json.Unmarshal(colbyte, &sCol); err != nil {
				result, _ := vm.ToValue(err.Error())
				return result
			}
			if len(sCol.ColumnName) == 0 {
				result, _ := vm.ToValue("needs column name")
				return result
			}
			if sCol.ColumnType == 0 {
				result, _ := vm.ToValue("needs column type")
				return result
			}
			if sCol.IndexType == 0 {
				result, _ := vm.ToValue("needs index type")
				return result
			}
			if sCol.Primary == 1 {
				hasPrimary = true
			}
			sCols = append(sCols, sCol)
		}
		if !hasPrimary {
			result, _ := vm.ToValue("needs primary key")
			return result
		}

		//need to check for duplicate table here (need to hook up 'get table info' or use ens)

		session.DBTable, err = DBC.CreateTable(session.TableOwner, *session.Encrypted, *session.Replication, in.Bid, session.TableName, sCols)
		if err != nil {
			result, _ := vm.ToValue(err.Error())
			return result
		}

		fmt.Printf("Success.\n")
		result, _ := vm.ToValue(true)
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

	// swarmdb> add("contacts", { "email": "rodney@wolk.com", "name": "Rodney", "age": 38 })
	// true

	vm.Set("addRow", func(call otto.FunctionCall) otto.Value {

		if !session.IsOpen {
			result, _ := vm.ToValue("Please open session first.")
			return result
		}
		raw := call.Argument(0).String()
		fmt.Printf("raw:\n%s\n", raw)

		var in IncomingInfo
		if err := json.Unmarshal([]byte(raw), &in); err != nil {
			result, _ := vm.ToValue(err.Error())
			return result
		}
		fmt.Printf("incoming rows:\n%+v\n", in)
		if len(in.Info) == 0 {
			result, _ := vm.ToValue("No rows specified")
			return result
		}
		if in.Bid == float64(0) {
			result, _ := vm.ToValue("Cannot have 0 bid")
			return result
		}

		var sRows []swarmdb.Row
		for _, row := range in.Info {
			var sRow swarmdb.Row
			sRow.Cells = make(map[string]interface{})
			if err := json.Unmarshal(row, &sRow.Cells); err != nil {
				result, _ := vm.ToValue(err.Error())
				return result
			}
			//should check for primary key in each row?
			//should check for duplicate rows?
			//should check for rows that already exist here? -- or kick the can down the line?
			sRows = append(sRows, sRow)
		}
		response, err := session.DBTable.Put(in.Bid, sRows)
		if err != nil {
			result, _ := vm.ToValue(err.Error())
			return result
		}

		result, _ := vm.ToValue(response)
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
		/*
			tbl_name := call.Argument(0).String() // e.g. "contacts"
			id := call.Argument(1).String()       // e.g. "id"
			jsonrecord, err := swarmdb.GetRecord(tbl_name, id)
			if err != nil {
				res, _ := vm.ToValue(err.Error())
				return res
			}
			res, _ := vm.ToValue(jsonrecord)
			return res
		*/

		res, _ := vm.ToValue(true)
		return res

	})

	// swarmdb> query("select name, age from contacts where email = 'rodney@wolk.com'")
	// records should come back with in an array
	// [ {"name":"Sourabh Niyogi", "age":45 }, {"name":"Francesca Niyogi", "age":49} ...]
	vm.Set("query", func(call otto.FunctionCall) otto.Value {
		/*
			sql := call.Argument(0).String()
			jsonarray, err := swarmdb.Query(sql)
			if err != nil {
				res, _ := vm.ToValue(err.Error())
				return res
			}
			res, _ := vm.ToValue(jsonarray)
			return res
		*/

	})

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
	return &Session{"", nil, nil, "", false, nil}
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
