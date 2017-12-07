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
	//"github.com/ethereum/go-ethereum/swarm/api"
	//"github.com/ethereum/go-ethereum/swarmdb/packages"
	"github.com/ethereum/go-ethereum/swarmdb/client"
	// "io"
	"os"
	//"strings"
	"encoding/json"
)

type Table struct {
	Table   string `json:"table"`
	Column  string `json:"column"`
	Primary bool   `json:"primary"`
	Index   string `json:"index"`
}

func main() {
	vm := otto.New()
	// swarmdb> createTable("contacts", {"column": "email", "type": "string", "primary": true, "index": "hash" })
	vm.Set("createTable", func(call otto.FunctionCall) otto.Value {
		tbl_name := call.Argument(0).String()       // e.g. "contacts"
		tbl_descriptor := call.Argument(1).Object() // Export(  ) // {"column": "email", "type": "string", "primary": true, "index": "hash" }
		column, _ := tbl_descriptor.Get("column")
		column_string, _ := column.ToString()
		primary, _ := tbl_descriptor.Get("primary")
		primary_bool, _ := primary.ToBoolean()
		index, _ := tbl_descriptor.Get("index")
		index_string, _ := index.ToString()
		succ := client.CreateTable(tbl_name, column_string, primary_bool, index_string)
		res, _ := vm.ToValue(succ)
		return res
	})

	// swarmdb> add("contacts", { "email": "rodney@wolk.com", "name": "Rodney", "age": 38 })
	// true
	vm.Set("add", func(call otto.FunctionCall) otto.Value {
		tablename := call.Argument(0).String()
		jsonrecord := call.Argument(1).String()

		//test for correct json input of record
		record := make(map[string]interface{})
		if err := json.Unmarshal([]byte(jsonrecord), &record); err != nil {
			fmt.Printf(err.Error() + ". Please try again\n")
			result, _ := vm.ToValue(false)
			return result
		}

		err := client.AddRecord(tablename, jsonrecord)
		if err != nil {
			result, _ := vm.ToValue(err.Error())
			return result
		}
		result, _ := vm.ToValue(true)
		return result
	})

	// swarmdb> get("contacts", "rodney@wolk.com")
	// { "email": "rodney@wolk.com", "name": "Rodney", "age": 38 }
	vm.Set("get", func(call otto.FunctionCall) otto.Value {
		tbl_name := call.Argument(0).String() // e.g. "contacts"
		id := call.Argument(1).String()       // e.g. "id"
		jsonrecord, err := client.GetRecord(tbl_name, id)
		if err != nil {
			res, _ := vm.ToValue(err.Error())
			return res
		}
		res, _ := vm.ToValue(jsonrecord)
		return res
	})

	// swarmdb> query("select name, age from contacts where email = 'rodney@wolk.com'")
	// records should come back with in an array
	// [ {"name":"Sourabh Niyogi", "age":45 }, {"name":"Francesca Niyogi", "age":49} ...]
	vm.Set("query", func(call otto.FunctionCall) otto.Value {
		sql := call.Argument(0).String()
		jsonarray, err := client.Query(sql)
		if err != nil {
			res, _ := vm.ToValue(err.Error())
			return res
		}
		res, _ := vm.ToValue(jsonarray)
		return res

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
