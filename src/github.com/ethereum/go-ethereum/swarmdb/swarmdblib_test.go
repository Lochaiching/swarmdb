package swarmdb_test

import (
	//"encoding/json"
	"fmt"
	//"strings"
	"reflect"
	"swarmdb"
	"testing"
)

//func TestProcessRequestResponseCommand(t *testing.T) {
//}

func TestAllSwarmdbLib(t *testing.T) {
	//TODO: start up wolkdb?

	//tests current port and ip in config file only
	//TODO: think about testing remort ports
	fmt.Printf("NewSwarmDBConnection test\n")
	dbc, err := swarmdb.NewSWARMDBConnection("", 0)
	if err != nil {
		t.Fatal(err)
	}

	fmt.Printf("OpenDatabase does not exist test - no encryption\n")
	db, err := dbc.OpenDatabase("testnoexist-database", 0) //case no encryption
	if err == nil {
		t.Fatal("testnoexist-database should not exist on this swarmdb node")
	}
	fmt.Printf("OpenDatabase does not exist test - encryption\n")
	db, err = dbc.OpenDatabase("testnoexist-database", 1) //case encryption
	if err == nil {
		t.Fatal("testnoexist-database should not exist on this swarmdb node")
	}
	err = nil

	fmt.Printf("CreateDatabase test - no encryption\n")
	db, err = dbc.CreateDatabase("test-database", 0) //case no encryption
	if err != nil {
		t.Fatal(err)
	}
	fmt.Printf("CreateDatabase test - encryption\n")
	db, err = dbc.CreateDatabase("testencrypt-database", 1) //case encryption
	if err != nil {
		t.Fatal(err)
	}

	fmt.Printf("CreateDatabase already exists test\n")
	db, err = dbc.CreateDatabase("test-database", 0)
	if err == nil {
		t.Fatal("test-database should not be able to be created, already exists")
	}
	err = nil

	fmt.Printf("ListDatabases test\n")
	databases, err := dbc.ListDatabases()
	expected := []swarmdb.Row{
		swarmdb.Row{"database": "test-database"},
		swarmdb.Row{"database": "testencrypt-database"},
	}
	if !reflect.DeepEqual(databases, expected) {
		fmt.Printf("  databases %+v\n, expected: %+v\n", databases, expected)
		t.Fatal("listdatabases failed.")
	}

	//test OpenDatabase with incorrect encryption. //TODO: what should happen?
	//fmt.Printf("OpenDatabase test with incorrect encryption\n")
	//db, err = dbc.OpenDatabase("testencrypt-database", 0)
	//if err == nil {
	//	t.Fatal("testencrypt-database has encryption 1, not encryption 0")
	//}
	//err = nil

	fmt.Printf("OpenDatabase test\n")
	db, err = dbc.OpenDatabase("test-database", 0)
	if err != nil {
		t.Fatal(err)
	}

	fmt.Printf("OpenTable does not exist test\n")
	tbl, err := db.OpenTable("testnoexist-table")
	if err == nil {
		t.Fatal("testnoexist-table should not exist on test-database")
	}
	err = nil

	fmt.Printf("CreateTable test\n")
	columns := []swarmdb.Column{
		swarmdb.Column{ColumnName: "email", ColumnType: swarmdb.CT_STRING, IndexType: swarmdb.IT_BPLUSTREE, Primary: 1},
		swarmdb.Column{ColumnName: "age", ColumnType: swarmdb.CT_INTEGER, IndexType: swarmdb.IT_BPLUSTREE, Primary: 0},
	}
	tbl, err = db.CreateTable("testtable", columns)
	if err != nil {
		t.Fatal(err)
	}

	fmt.Printf("OpenTable test\n")
	tbl, err = db.OpenTable("testtable")
	if err != nil {
		t.Fatal(err)
	}

	fmt.Printf("ListTables test\n")
	tables, err := db.ListTables()
	if err != nil {
		t.Fatal(err)
	}
	expected = []swarmdb.Row{
		swarmdb.Row{"table": "testtable"},
	}
	if !reflect.DeepEqual(tables, expected) {
		fmt.Printf("  tables: %+v, expected: %+v\n", tables, expected)
		t.Fatal("listtables failed")
	}

	fmt.Printf("DescribeTable test\n")
	description, err := tbl.DescribeTable()
	if err != nil {
		t.Fatal(err)
	}
	expected = []swarmdb.Row{
		swarmdb.Row{
			"ColumnName": "email",
			"ColumnType": float64(swarmdb.CT_STRING),
			"IndexType":  float64(swarmdb.IT_BPLUSTREE),
			"Primary":    float64(1)},
		swarmdb.Row{
			"ColumnName": "age",
			"ColumnType": float64(swarmdb.CT_INTEGER),
			"IndexType":  float64(swarmdb.IT_BPLUSTREE),
			"Primary":    float64(0)},
	}
	if !reflect.DeepEqual(description, expected) {
		fmt.Printf("  describe table: %+v, expected: %+v\n", description, expected)
		t.Fatal("describetable failed")
	}

	fmt.Printf("Put test\n")
	rowtoadd := swarmdb.Row{"email": "test@test.com", "age": 23}
	err = tbl.Put(rowtoadd)
	if err != nil {
		t.Fatal(err)
	}
	fmt.Printf("Put test - multiple rows\n")
	rowstoadd := []swarmdb.Row{
		swarmdb.Row{"email": "b@test.com", "age": 2},
		swarmdb.Row{"email": "c@ytest.com", "age": 5},
	}
	err = tbl.Put(rowstoadd)
	if err != nil {
		t.Fatal(err)
	}

	fmt.Printf("Get test\n")
	rowsgotten, err := tbl.Get("b@test.com")
	if err != nil {
		t.Fatal(err)
	}
	expected = []swarmdb.Row{
		swarmdb.Row{"age": float64(2), "email": "b@test.com"},
	}
	if !reflect.DeepEqual(rowsgotten, expected) {
		fmt.Printf("  get: %+v, expected: %+v\n", rowsgotten, expected)
		t.Fatal("row gotten does not match expected")
	}

	fmt.Printf("Delete test\n")
	err = tbl.Delete("b@test.com")
	if err != nil {
		t.Fatal(err)
	}
	rowsgotten, err = tbl.Get("b@test.com")
	if len(rowsgotten) > 0 {
		fmt.Printf("  rows gotten: %+v\n", rowsgotten)
		t.Fatal("delete failed")
	}
	//this returns no error if there are no rows gotten. is that ok?

	fmt.Printf("Query test\n")
	rowsgotten, err = tbl.Query("select email, age from testtable where age < 10")
	if err != nil {
		t.Fatal(err)
	}
	expected = []swarmdb.Row{
		swarmdb.Row{"age": float64(5), "email": "c@ytest.com"},
	}
	if !reflect.DeepEqual(rowsgotten, expected) {
		fmt.Printf("  query: %+v, expected: %+v\n", rowsgotten, expected)
		t.Fatal("queried row does not match expected")
	}
	_, err = tbl.Query("select email, age from badtable where age < 10")
	if err == nil {
		t.Fatal(err)
	}

	fmt.Printf("DropTable test\n")
	err = db.DropTable("testtable")
	if err != nil {
		t.Fatal(err)
	}
	_, err = db.OpenTable("testtable")
	if err == nil {
		fmt.Printf("  table 'testtable' should have been dropped\n")
		t.Fatal("drop table failed")
	}
	err = nil

	fmt.Printf("DropDatabase test\n")
	err = dbc.DropDatabase("test-database")
	if err != nil {
		t.Fatal(err)
	}
	_, err = dbc.OpenDatabase("test-database", 0)
	if err == nil {
		fmt.Printf("  database 'test-database' should have been dropped")
		t.Fatal("drop database failed")
	}

	//test Scan
	//test table Close
	//test swarmdbconnection Close

}
