//only types which interact externally are here. internal swarmdb types are not.
package common

import (
	"encoding/json"
	//"fmt"
)

type Column struct {
	ColumnName string     `json:"columnname,omitempty"` // e.g. "accountID"
	IndexType  IndexType  `json:"indextype,omitempty"`  // IT_BTREE
	ColumnType ColumnType `json:"columntype,omitempty"`
	Primary    int        `json:"primary,omitempty"`
}

type RequestOption struct {
	RequestType string      `json:"requesttype"` //"OpenConnection, Insert, Get, Put, etc"
	Owner       string      `json:"owner,omitempty"`
	Database    string      `json:"database,omitempty"`
	Table       string      `json:"table,omitempty"` //"contacts"
	Encrypted   int         `json:"encrypted,omitempty"`
	Key         interface{} `json:"key,omitempty"`  //value of the key, like "rodney@wolk.com"
	Rows        []Row       `json:"rows,omitempty"` //value of val, usually the whole json record
	Columns     []Column    `json:"columns,omitempty"`
	RawQuery    string      `json:"query,omitempty"` //"Select name, age from contacts where email = 'blah'"
}

type SWARMDBResponse struct {
	Error            *SWARMDBError `json:"error,omitempty"`
	ErrorCode        int           `json:"errorcode,omitempty"`
	ErrorMessage     string        `json:"errormessage,omitempty"`
	Data             []Row         `json:"data,omitempty"`
	AffectedRowCount int           `json:"affectedrowcount,omitempty"`
	MatchedRowCount  int           `json:"matchedrowcount,omitempty"`
}

func (resp *SWARMDBResponse) String() string {
	return resp.Stringify()
}

func (resp *SWARMDBResponse) Stringify() string {
	/*
	   wolkErr, ok := resp.Error.(*swarmdb.SWARMDBError)
	   if !ok {
	           return (`{ "errorcode":-1, "errormessage":"UNKNOWN ERROR"}`) //TODO: Make Default Error Handling
	   }
	   if wolkErr.ErrorCode == 0 { //FYI: default empty int is 0. maybe should be a pointer.  //TODO this is a hack with what errors are being returned right now
	           //fmt.Printf("wolkErr.ErrorCode doesn't exist\n")
	           respObj.ErrorCode = 474
	           respObj.ErrorMessage = resp.Error.Error()
	   } else {
	           respObj.ErrorCode = wolkErr.ErrorCode
	           respObj.ErrorMessage = wolkErr.ErrorMessage
	   }
	*/
	jbyte, jErr := json.Marshal(resp)
	if jErr != nil {
		//fmt.Printf("Error: [%s] [%+v]", jErr.Error(), resp)
		return `{ "errorcode":474, "errormessage":"ERROR Encountered Generating Response"}` //TODO: Make Default Error Handling
	}
	jstr := string(jbyte)
	return jstr
}

type ColumnType string
type IndexType string
type RequestType string

//note: these are the only consts needed for client, swarmdb has a much larger list
const (
	CT_INTEGER = "INTEGER"
	CT_STRING  = "STRING"
	CT_FLOAT   = "FLOAT"
	CT_BLOB    = "BLOB"

	IT_NONE      = "NONE"
	IT_HASHTREE  = "HASH"
	IT_BPLUSTREE = "BPLUS"
	IT_FULLTEXT  = "FULLTEXT"

	RT_CREATE_DATABASE = "CreateDatabase"
	RT_LIST_DATABASES  = "ListDatabases"
	RT_DROP_DATABASE   = "DropDatabase"

	RT_CREATE_TABLE   = "CreateTable"
	RT_DESCRIBE_TABLE = "DescribeTable"
	RT_LIST_TABLES    = "ListTables"
	RT_DROP_TABLE     = "DropTable"
	RT_CLOSE_TABLE    = "CloseTable" //moon branch only

	RT_START_BUFFER = "StartBuffer"
	RT_FLUSH_BUFFER = "FlushBuffer"

	RT_PUT    = "Put"
	RT_GET    = "Get"
	RT_DELETE = "Delete"
	RT_QUERY  = "Query"
	RT_SCAN   = "Scan"
)
