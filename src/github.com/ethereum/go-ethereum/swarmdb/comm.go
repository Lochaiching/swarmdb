package swarmdb

import (
	"bufio"
	"github.com/ethereum/go-ethereum/swarmdb/keymanager"
	"net"
)

//for passing request data from client to server if the request needs Table data
type Column struct {
	ColumnName string     `json:"columnname,omitempty"` // e.g. "accountID"
	IndexType  IndexType  `json:"indextype,omitempty"`  // IT_BTREE
	ColumnType ColumnType `json:"columntype,omitempty"`
	Primary    int        `json:"primary,omitempty"`
}

//for passing request data from client to server
type RequestOption struct {
	RequestType string      `json:"requesttype"` //"OpenConnection, Insert, Get, Put, etc"
	Owner       string      `json:"owner,omitempty"`
	Table       string      `json:"table,omitempty"` //"contacts"
	Encrypted   int         `json:"encrypted,omitempty"`
	Bid         float64     `json:"bid,omitempty"`
	Replication int         `json:"replication,omitempty"`
	Key         string      `json:"key,omitempty"`   //value of the key, like "rodney@wolk.com"
	Value       string      `json:"value,omitempty"` //value of val, usually the whole json record
	Row         SWARMDBRow  `json:"row,omitempty"`
	Columns     []Column    `json:"columns,omitempty"`
	RawQuery    string      `json:"rawquery,omitempty"` //"Select name, age from contacts where email = 'blah'"
	Query       QueryOption `json:"query,omitempty"`    //Parsed query
}

type SWARMDBConnection struct {
	connection net.Conn
	keymanager keymanager.KeyManager
	ownerID    string
	nrw        *bufio.ReadWriter
}

type SWARMDBTable struct {
	conn      *SWARMDBConnection
	tableName string
}

type SWARMDBRow struct {
	cells map[string]string
}
