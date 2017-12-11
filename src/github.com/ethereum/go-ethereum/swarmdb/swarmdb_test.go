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
	o := common.TableOption{Index: "email", Primary: 1, TreeType: "BT", KeyType: common.KT_STRING}
	option = append(option, o)
	tbl := swarmdb.NewTable("owner1", "testtable")
	tbl.CreateTable(option)

	// OpenTable
	tbl.OpenTable()

	putstr := `{"email":"rodney@wolk.com", "age": 38, "gender": "M", "weight": 172.5}`
	tbl.Put(putstr)

	putstr = `{"email":"sourabh@wolk.com", "age": 45, "gender": "M", "weight": 210.5}`
	tbl.Put(putstr)
	// Put
	for i := 1; i < 100; i++ {
		g := "F"
		w := float64(i) + .314159
		putstr = fmt.Sprintf(`{"email":"test%03d@wolk.com", "age": %d, "gender": "%s", "weight": %f}`,
			i, i, g, w)

		g = "M"
		w = float64(i) + float64(0.414159)
		putstr = fmt.Sprintf(`{"email":"test%03d@wolk.com", "age": %d, "gender": "%s", "weight": %f}`,
			i, i, g, w)
		tbl.Put(putstr)
	}

	// Get
	res, err := tbl.Get("rodney@wolk.com")
	fmt.Printf("Get %s %v \n", string(res), err)

	// Get
	fres, ferr := tbl.Get("test010@wolk.com")
	fmt.Printf("Get %s %v \n", string(fres), ferr)
	//t.CloseTable()

}
