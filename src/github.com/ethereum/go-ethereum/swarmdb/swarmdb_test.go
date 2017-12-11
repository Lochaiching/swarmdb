package common_test

import (
	"fmt"
	common "github.com/ethereum/go-ethereum/swarmdb"
	"testing"
)

func TestTable(t *testing.T) {
	swarmdb := common.NewSwarmDB()

	// CreateTable
	var option []common.TableOption
	o := common.TableOption{Index: "testindex1", Primary: 1, TreeType: "BT", KeyType: 1}
	option = append(option, o)
	tbl := swarmdb.NewTable("owner1")
	tbl.CreateTable("testtable", option)

	// OpenTable
	tbl.OpenTable("testtable")

	// Put
	putstr := `{"testindex1":"value2"}`
	tbl.Put(putstr)

	// Get
	res, err := tbl.Get("testindex1")
	fmt.Printf("Get %s %v \n", string(res), err)

	// Get
	fres, ferr := tbl.Get("testindex1")
	fmt.Printf("Get %s %v \n", string(fres), ferr)
	//t.CloseTable()
}
