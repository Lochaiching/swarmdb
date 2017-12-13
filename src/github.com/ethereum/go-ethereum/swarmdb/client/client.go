package client

import (
	"encoding/json"
	"fmt"
	"github.com/ethereum/go-ethereum/log"
	common "github.com/ethereum/go-ethereum/swarmdb"
	"github.com/xwb1989/sqlparser"
	"strings"
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

	var req common.RequestOption
	req.RequestType = "CreateTable"
	req.Table = table

	//primary key call
	var primarycol common.Column
	primarycol.ColumnName = primarykey
	primarycol.IndexType = indextype
	primarycol.ColumnType = columntype[primarykey]
	primarycol.Primary = 1
	req.Columns = append(req.Columns, primarycol)

	//secondary key calls
	for col, coltype := range columntype {
		if col != primarykey {
			var secondarycol common.Column
			secondarycol.ColumnName = col
			secondarycol.IndexType = coltype
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

	var req common.RequestOption
	req.RequestType = "Insert" //does not allow duplicates...?
	req.Owner = owner
	req.Table = table
	req.Key = key

	vmap := make(map[string]interface{})
	if err := json.Unmarshal([]byte(value), &vmap); err != nil {
		return fmt.Errorf("record is not proper json")
	}
	vjson, _ := json.Marshal(vmap) //re-marshal to clean up any odd formatting
	req.Value = string(vjson)
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

	//fmt.Printf("swarmdb.SWARMDB_get(%s, %s)\n", tbl_name, id)

	var req common.RequestOption
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
func Query(owner string, table string, sql string) (data []string, err error) {

	var req common.RequestOption
	req.RequestType = "Get"
	req.Owner = owner
	req.Table = table

	///here

	//parse sql
	stmt, err := sqlparser.Parse(sql)
	if err != nil {
		fmt.Printf("sqlparser.Parse err: %v\n", err)
		return data, err
	}
	node := stmt.(*sqlparser.Select)
	//for i, e := range node.SelectExprs {
	//	fmt.Printf("FIELD %d: %+v\n", i, sqlparser.String(e)) // stmt.(*sqlparser.Select).SelectExprs)
	//}
	//really should only have 1 table
	//for i, e := range node.From {
	//	fmt.Printf("FROM %d: %s \n", i, sqlparser.String(e))
	//}
	if node.From == nil {
		return data, fmt.Errorf("no table specified")
	}

	//get table
	// fill in GET

	//data, primarykeycol = GET(node.From[0])  //not sure a primary key col is a feature we'll have
	//pretending this is the solution to whatever the query puts out... (i.e. the whole contacts table)
	var dataget []string
	dataget = append(dataget, `{ "email": "rodney@wolk.com", "name": "Rodney", "age": 38 }`)
	dataget = append(dataget, `{ "email": "alina@wolk.com", "name": "Alina", "age": 35 }`)
	dataget = append(dataget, `{ "email": "who@wolk.com", "name": "Who", "age": 38 }`)
	//pretending it also turns out a primary key col:
	//primarykeycol := "email"

	//pretending that this is no primary key, just json rows of data:
	if node.Where != nil {
		fmt.Printf("WHERE: %s \n", readable(node.Where.Expr))

		switch n := node.Where.Expr.(type) {
		case *sqlparser.OrExpr:
			// need >, <, >=, <=
			left := strings.Split(sqlparser.String(n.Left), "=")
			left[0] = strings.TrimSpace(left[0])
			left[1] = strings.TrimSpace(left[1])
			right := strings.Split(sqlparser.String(n.Right), "=")
			right[0] = strings.TrimSpace(right[0])
			right[1] = strings.TrimSpace(right[1])
			for _, record := range dataget {
				rmap := make(map[string]interface{})
				if err := json.Unmarshal([]byte(record), &rmap); err != nil {
					return data, err
				}
				if (rmap[left[0]] == left[1]) || rmap[right[0]] == right[1] {
					data = append(data, record)
				}

			}
		case *sqlparser.AndExpr:
			left := strings.Split(sqlparser.String(n.Left), "=")
			leftkey := strings.TrimSpace(left[0])
			leftkey = strings.Replace(leftkey, `'`, "", -1)
			leftkey = strings.Replace(leftkey, `"`, "", -1)
			//leftval := strings.TrimSpace(left[1])
			leftval := strings.TrimSpace(left[1])
			leftval = strings.Replace(leftval, `'`, "", -1)
			leftval = strings.Replace(leftval, `"`, "", -1)
			fmt.Printf("left: %+v, %+v\n", leftkey, leftval)
			right := strings.Split(sqlparser.String(n.Right), "=")
			rightkey := strings.TrimSpace(right[0])
			rightkey = strings.Replace(rightkey, `'`, "", -1)
			rightkey = strings.Replace(rightkey, `"`, "", -1)
			//right[0] = strings.TrimSpace(right[0])

			fmt.Printf("right: %+v\n", right)
			for _, record := range dataget {
				r := make(map[string]interface{})
				if err := json.Unmarshal([]byte(record), &r); err != nil {
					return data, err
				}
				fmt.Printf("rmap: %+v\n", r)
				fmt.Printf("rmap's left0: %+v\n", r[leftkey])
				fmt.Printf("left1: %+v\n", leftval)
				if r[leftkey] == leftval {
					fmt.Printf("left is good\n")
				}
				if r[rightkey] == right[1] {
					fmt.Printf("right is good\n")
				}
				if (r[leftkey] == leftval) && r[rightkey] == right[1] {
					fmt.Printf("both are good. adding data\n")
					data = append(data, record)
				}
			}
		case *sqlparser.IsExpr:

		}
	}

	//fmt.Printf("HAVING: %s \n", sqlparser.String(node.Having))

	//fmt.Printf("GROUPBY: %s \n", sqlparser.String(node.GroupBy))

	//fmt.Printf("ORDER BY: %s \n", sqlparser.String(node.OrderBy))

	//fmt.Printf(")\n")
	fmt.Printf("data: %+v\n", data)
	return data, nil
}

/*
func cleanExpression(n sqlparser.Expr, operand string) (leftkey string, leftval string, rightkey string, rightval string) {
	left := strings.Split(sqlparser.String(n.Left), operand)
	leftkey = cleanValue(left[0])
	leftval = cleanValue(left[1])
	fmt.Printf("left: %+v, %+v\n", leftkey, leftval)
	right := strings.Split(sqlparser.String(n.Right), operand)
	rightkey = cleanValue(right[0])
	rightval = cleanValue(right[1])
	fmt.Printf("right: %+v, %+v\n", rightkey, rightval)
	return leftkey, leftval, rightkey, rightval
}

func cleanValue(val string) string {
	val = strings.TrimSpace(val)
	val = strings.Replace(val, `'`, "", -1)
	val = strings.Replace(leftval, `"`, "", -1)
	return val
}
*/



func SwarmDbUploadKademlia(owner string, table string, key string, content string) {
	/*
	   kvlen := int64(len(content))
	   dbwg := &sync.WaitGroup{}
	   rdb := strings.NewReader(content)

	   //Take the Hash returned for the stored 'Main' content and store it
	   raw_indexkey, err := s.api.StoreDB(rdb, kvlen, dbwg)
	   if err != nil {
	           //s.Error(w, r, err)
	           return
	   }
	   logDebug("Index content stored (kv=[%v]) for raw_indexkey.Log [%s] [%+v] (size of [%+v])", string(content), raw_indexkey.Log(), raw_indexkey, kvlen)
	*/
}

func SwarmDbDownloadKademlia(owner string, table string, id string) (record map[string]interface{}, err error) {
	/*
	           keylen := 64 ///////..........
	           dummy := bytes.Repeat([]byte("Z"), keylen)

	           contentPrefix := BuildSwarmdbPrefix(owner, table, id)
	           newkeybase := contentPrefix+string(dummy)
	           chunker := storage.NewTreeChunker(storage.NewChunkerParams())
	           rd := strings.NewReader(newkeybase)
	           key, err := chunker.Split(rd, int64(len(newkeybase)), nil, nil, nil, false)
	           log.Debug(fmt.Sprintf("In HandleGetDB prefix [%v] dummy %v newkeybase %v key %v", contentPrefix, dummy, newkeybase, key))

	   	fmt.Println("Key: ", key)
	           contentReader := api.Retrieve(key) //swarmdbapi.
	           if _, err := contentReader.Size(nil); err != nil {
	                   log.Debug("key not found %s: %s", key, err)
	                   //http.NotFound(w, &r.Request)
	                   return
	           }
	           if err != nil {
	                   //s.Error(w, r, err)
	                   return
	           }

	           contentReaderSize,_ := contentReader.Size(nil)
	           contentBytes := make( []byte, contentReaderSize )
	           _,_ = contentReader.ReadAt( contentBytes, 0 )

	           encryptedContentBytes := bytes.TrimRight(contentBytes[577:],"\x00")
	           //encryptedContentBytes := contentBytes[len(contentPrefix):]
	           log.Debug(fmt.Sprintf("In HandledGetDB Retrieved 'mainhash' v[%v] s[%s] ", encryptedContentBytes, encryptedContentBytes))

	           //decrypted_reader := DecryptData(encryptedContentBytes)
	*/
	return record, err

}

func logDebug(format string, v ...interface{}) {
	log.Debug(fmt.Sprintf("[SWARMDB] HTTP: "+format, v...))
}

//func andOperation(data ) {
//}

func readable(node sqlparser.Expr) string {
	switch node := node.(type) {
	case *sqlparser.OrExpr:
		return fmt.Sprintf("(%s or %s)", readable(node.Left), readable(node.Right))
	case *sqlparser.AndExpr:
		//fmt.Printf("got to AND expr (%s and %s)", readable(node.Left), readable(node.Right))
		return fmt.Sprintf("got to AND expr (%s and %s)", readable(node.Left), readable(node.Right))
	case *sqlparser.BinaryExpr:
		return fmt.Sprintf("(%s %s %s)", readable(node.Left), node.Operator, readable(node.Right))
	case *sqlparser.IsExpr:
		return fmt.Sprintf("(%s %s)", readable(node.Expr), node.Operator)
	default:
		return sqlparser.String(node)
	}
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
