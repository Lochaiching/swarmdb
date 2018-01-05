package swarmdb

import (
	"encoding/json"
	"fmt"
	"github.com/ethereum/go-ethereum/log"
	"github.com/xwb1989/sqlparser"
)

/* from types.go:
type Column struct {
        ColumnName string     `json:"columnname,omitempty"` // e.g. "accountID"
        IndexType  IndexType  `json:"indextype,omitempty"`  // IT_BTREE
        ColumnType ColumnType `json:"columntype,omitempty"`
        Primary    int        `json:"primary,omitempty"`
}

type RequestOption struct {
        RequestType string   `json:"requesttype"` //"OpenConnection, Insert, Get, Put, etc"
        Owner       string   `json:"owner,omitempty"`
        Table       string   `json:"table,omitempty"` //"contacts"
        Index       string   `json:"index,omitempty"`
        Key         string   `json:"key,omitempty"`   //value of the key, like "rodney@wolk.com"
        Value       string   `json:"value,omitempty"` //value of val, usually the whole json record
        Columns     []Column `json:"columns",omitempty"`
}*/

//columntypes exp: {"name":"string", "age":"int", "gender":"string"}
func CreateTable(indextype string, table string, primarykey string, columntype map[string]string) (err error) {

	if len(table) == 0 {
		return fmt.Errorf("no table name")
	}
	if len(primarykey) == 0 {
		return fmt.Errorf("no primary key")
	}
	var req RequestOption
	req.RequestType = "CreateTable"
	req.Table = table

	//primary key generation
	var primarycol Column
	primarycol.ColumnName = primarykey
	primarycol.IndexType, err = convertStringToIndexType(indextype)
	if err != nil {
		return err
	}
	primarycol.ColumnType, _ = convertStringToColumnType(columntype[primarykey])
	if err != nil {
		return err
	}
	primarycol.Primary = 1
	req.Columns = append(req.Columns, primarycol)

	//secondary key generation
	for col, coltype := range columntype {
		if col != primarykey {
			var secondarycol Column
			secondarycol.ColumnName = col
			secondarycol.ColumnType, err = convertStringToColumnType(coltype)
			if err != nil {
				return err
			}
			secondarycol.Primary = 0
			req.Columns = append(req.Columns, secondarycol)
		}
	}

	fmt.Printf("swarmdb.CreateTable( %+v\n)", table)

	//new swarmdbserver
	//err = swarmdbserver.OpenConnection(owner?)
	//err = swarmdbserver.OpenTable(table)
	//err = swarmdbserver.CreateTable(req)
	//swarmdbserver.CloseClientConnection

	return nil
}

//value is a "record" in json format
//key is most likely the primary key
func AddRecord(owner string, table string, key string, value string) (err error) {

	if len(owner) == 0 {
		return fmt.Errorf("no owner")
	}
	if len(table) == 0 {
		return fmt.Errorf("no table name")
	}
	if len(key) == 0 {
		return fmt.Errorf("no key")
	}
	if len(value) == 0 {
		return fmt.Errorf("no value")
	}

	var req RequestOption
	req.RequestType = "Insert" //does not allow duplicates...?
	req.Owner = owner
	req.Table = table
	req.Key = key

	var vmap Row
	if err := json.Unmarshal([]byte(value), &vmap.Cells); err != nil {
		return fmt.Errorf("record is not proper json")
	}
	// vjson, _ := json.Marshal(vmap) //re-marshal to clean up any odd formatting
	req.Rows = append(req.Rows, vmap)
	fmt.Printf("swarmdb.AddRecord(%+v)\n", req)

	//new swarmdbserver
	//err = swarmdbserver.OpenConnection
	//err = swarmdbserver.OpenTable(table)
	//err = swarmdbserver.PUT(req)
	//swarmdbserver.CloseConnection

	return nil
}

//id should be prim key
//func GetRecord(tbl_name string, id string) (jsonrecord string, err error) {
func GetRecord(owner string, table string, key string) (value string, err error) {

	if len(owner) == 0 {
		return value, fmt.Errorf("no owner")
	}
	if len(table) == 0 {
		return value, fmt.Errorf("no table name")
	}
	if len(key) == 0 {
		return value, fmt.Errorf("no key")
	}

	var req RequestOption
	req.RequestType = "Get"
	req.Owner = owner
	req.Table = table
	req.Key = key
	fmt.Printf("swarmdb.GetRecord(%+v)\n", req)

	//new swarmdbserver
	//err = swarmdbserver.OpenConnection
	//err = swarmdbserver.OpenTable(table)
	//err = swarmdbserver.GET(req)
	//swarmdbserver.CloseClientConnection

	//let's say this is the answer out of the swarmdb: (tbl_name: contacts, id: rodeny@wolk.com)
	jsonrecord := `{ "email": "rodney@wolk.com", "name": "Rodney", "age": 38 }`

	//return rec
	return jsonrecord, nil
}

//data should be a pointer not actual structure
func GetQuery(owner string, table string, query string) (data []string, err error) {

	if len(owner) == 0 {
		return data, fmt.Errorf("no owner")
	}
	if len(table) == 0 {
		return data, fmt.Errorf("no table name")
	}
	if len(query) == 0 {
		return data, fmt.Errorf("no query")
	}

	var req RequestOption
	req.RequestType = "Get"
	req.Owner = owner
	req.Table = table
	req.Encrypted = 1 //encrypted means table is? or data being passed back and forth is?
	req.Bid = float64(1.11) //need to get this from the user
	//req.Replication = ?

	//quick parse sql for formatting issues
	_, err = sqlparser.Parse(query)
	if err != nil {
		fmt.Printf("sqlparser.Parse err: %v\n", err)
		return data, err

	}

	// call swarmdb handler here with query, is ok.
	return data, err



}

func logDebug(format string, v ...interface{}) {
	log.Debug(fmt.Sprintf("[SWARMDB] HTTP: "+format, v...))
}


/*
//best place to call open/close client connections?
func openConnection() (err error) {
	//diff kinds of clients? how to decide which?
	return nil
}

func closeConnection() (err error) {
	//diff kinds of clients? how to decide which?
	//need garbage collection?
	return nil
}

func openTable() (err error) {
}
*/
