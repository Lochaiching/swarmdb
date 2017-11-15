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
	"github.com/robertkrimen/otto"
	"github.com/robertkrimen/otto/repl"
	"github.com/xwb1989/sqlparser"
	// "io"
	"os"
	//"strings"
	//"encoding/json"
)

type Table struct {
	Table   string `json:"table"`
	Column  string `json:"column"`
	Primary bool   `json:"primary"`
	Index   string `json:"index"`
}

func SWARMDB_createTable(tbl_name string, column string, primary bool, index string) (succ bool) {
	fmt.Printf("SWARMDB_createTable(%v, column: %v primary: %v index: %v)\n", tbl_name, column, primary, index)
	// RODNEY/MAYUMI: CONNECT TO dispatch.go -- create table descriptor (in LocalDB + ENS), ...
	return true
}

func SWARMDB_add(tbl_name string, rec *otto.Object) (succ bool) {
	// RODNEY/MAYUMI: CONNECT TO dispatch.go -- get table descriptor, get primary key's index type, ...
	fmt.Printf("SWARMDB_add(%s, ", tbl_name)
	for _, k := range rec.Keys() {
		v, _ := rec.Get(k)
		fmt.Printf(" key: %s value: %s", k, v)
	}
	fmt.Printf(")\n")
	return true
}

//func SWARMDB_get(tbl_name string, id string) (rec *otto.Object) {
func SWARMDB_get(tbl_name string, id string) (json string) {
	// ALINA: FIGURE OUT HOW A JSON OBJECT SHOULD BE RETURNED

	// RODNEY/MAYUMI: CONNECT TO dispatch.go
	// get table descriptor, and based on the primary key's index, call dispatch.go
	fmt.Printf("SWARMDB_get(%s, %s)\n", tbl_name, id)

	//let's say this is the answer out of the swarmdb: (tbl_name: contacts, id: rodeny@wolk.com)
	json = `{ "email": "rodney@wolk.com", "name": "Rodney", "age": 38 }`

	//return rec
	return json

}

//func SWARMDB_query(sql string) (rec otto.Value) {
func SWARMDB_query(sql string) (jsonarray []string, err error) {
	// ALINA: FIGURE OUT HOW AN *** ARRAY ***  of JSON OBJECTS SHOULD BE RETURNED
	// fmt.Printf("sql is: %s\n", sql)
	stmt, err := sqlparser.Parse(sql)
	if err != nil {
		fmt.Printf("sqlparser.Parse err: %v\n", err)
		//return otto.Value{}
		return jsonarray, err
	}
	fmt.Printf("SWARMDB_query(%s, ", sqlparser.String(stmt.(*sqlparser.Select).From))

	for i, e := range stmt.(*sqlparser.Select).SelectExprs {
		fmt.Printf(" field %d: %+v", i, sqlparser.String(e)) // stmt.(*sqlparser.Select).SelectExprs)
	}
	fmt.Printf(")\n")
	//return otto.Value{}

	//pretending this is the solution to whatever the query puts out...
	jsonarray = append(jsonarray, `{ "email": "rodney@wolk.com", "name": "Rodney", "age": 38 }`)
	jsonarray = append(jsonarray, `{ "email": "alina@wolk.com", "name": "Alina", "age": 35 }`)
	return jsonarray, nil
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
		succ := SWARMDB_createTable(tbl_name, column_string, primary_bool, index_string)
		res, _ := vm.ToValue(succ)
		return res
	})

	// swarmdb> add("contacts", { "email": "rodney@wolk.com", "name": "Rodney", "age": 38 })
	// true
	vm.Set("add", func(call otto.FunctionCall) otto.Value {
		tbl_name := call.Argument(0).String()
		rec := call.Argument(1).Object()
		succ := SWARMDB_add(tbl_name, rec)
		res, _ := vm.ToValue(succ)
		return res
	})
	// swarmdb> get("contacts", "rodney@wolk.com")
	// { "email": "rodney@wolk.com", "name": "Rodney", "age": 38 }
	vm.Set("get", func(call otto.FunctionCall) otto.Value {
		tbl_name := call.Argument(0).String() // e.g. "contacts"
		id := call.Argument(1).String()       // e.g. "id"
		// ALINA: FIGURE OUT HOW A JSON OBJECT SHOULD BE RETURNED
		json := SWARMDB_get(tbl_name, id)
		//res, _ := vm.ToValue(`JSON.parse(json)`)
		//res, _ := vm.ToValue(rec)
		res, _ := vm.ToValue(json)
		return res
	})
	// swarmdb> query("select name, age from contacts where email = 'rodney@wolk.com'")
	// records should come back with in an array
	// [ {"name":"Sourabh Niyogi", "age":45 }, {"name":"Francesca Niyogi", "age":49} ...]
	vm.Set("query", func(call otto.FunctionCall) otto.Value {
		sql := call.Argument(0).String()
		// ALINA: FIGURE OUT HOW AN ARRAY of JSON OBJECT SHOULD BE RETURNED
		jsonarray, _ := SWARMDB_query(sql)
		/*
			if err != nil {
				fmt.Printf("query err... %v\n", err)
				return
			}
		*/
		// res, _ := vm.ToValue(records)
		res, _ := vm.ToValue(jsonarray)
		return res
	})

	vm.Set("quit", func(call otto.FunctionCall) otto.Value {
		os.Exit(0)
		return otto.Value{}
	})

	// VERY BASIC TEST CASES
	vm.Run(`createTable("contacts", {"column": "email", "type": "string", "primary": true, "index": "hash" })`)
	vm.Run(`add("contacts", { "email": "rodney@wolk.com", "name": "Rodney", "age": 38 })`)
	vm.Run(`get("contacts", "rodney@wolk.com")`)
	vm.Run(`query("select name, age from contacts where age >= 38");`)

	if err := repl.RunWithPrompt(vm, "swarmdb> "); err != nil {
		panic(err)
	}
}
