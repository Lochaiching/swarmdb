package server_test

import (
	"encoding/json"
	"fmt"
	common "github.com/ethereum/go-ethereum/swarmdb"
	"github.com/ethereum/go-ethereum/swarmdb/server"
	"io/ioutil"
	"os"
	"testing"
)

// func NewTCPIPServer(swarmdb SwarmDB, l net.Listener) *TCPIPServer
func testTCPIPServer(t *testing.T, f func(*server.TCPIPServer)) {
	datadir, err := ioutil.TempDir("", "tcptest")
	if err != nil {
		t.Fatalf("unable to create temp dir: %v", err)
	}
	os.RemoveAll(datadir)
	defer os.RemoveAll(datadir)
	swarmdb := common.NewSwarmDB()
	svr := server.NewTCPIPServer(swarmdb, nil)
	if err != nil {
		fmt.Println("hashdb open error")
	}
	f(svr)
}

func TestCreateTable(t *testing.T) {
	swarmdb := common.NewSwarmDB()
	svr := server.NewTCPIPServer(swarmdb, nil)
	var testData server.IncomingInfo
	var testColumn []common.Column
	testColumn = make([]common.Column, 3)
	testColumn[0].ColumnName = "email"
	testColumn[0].Primary = 1                     // What if this is inconsistent?
	testColumn[0].IndexType = common.IT_BPLUSTREE //  What if this is inconsistent?
	testColumn[0].ColumnType = common.CT_STRING

	testColumn[1].ColumnName = "yob"
	testColumn[1].Primary = 0                     // What if this is inconsistent?
	testColumn[1].IndexType = common.IT_BPLUSTREE //  What if this is inconsistent?
	testColumn[1].ColumnType = common.CT_INTEGER

	testColumn[2].ColumnName = "location"
	testColumn[2].Primary = 0                     // What if this is inconsistent?
	testColumn[2].IndexType = common.IT_BPLUSTREE //  What if this is inconsistent?
	testColumn[2].ColumnType = common.CT_STRING

	var testReqOption common.RequestOption

	testReqOption.RequestType = "CreateTable"
	testReqOption.Owner = "0xf6b55acbbc49f4524aa48d19281a9a77c54de10f"
	testReqOption.Table = "contacts"
	testReqOption.Bid = 7.07
	testReqOption.Replication = 3
	testReqOption.Encrypted = 1
	testReqOption.Columns = testColumn

	marshalTestReqOption, err := json.Marshal(testReqOption)
	fmt.Printf("JSON --> %s", marshalTestReqOption)
	if err != nil {
		t.Fatalf("error marshaling testReqOption: %s", err)

	}
	testData.Data = string(marshalTestReqOption)
	testData.Address = ""
	svr.TestAddClient(testReqOption.Owner, testReqOption.Table, "email")
	svr.SelectHandler(&testData)
}

func TestOpenTable(t *testing.T) {
	swarmdb := common.NewSwarmDB()
	svr := server.NewTCPIPServer(swarmdb, nil)
	var testData server.IncomingInfo

	var testReqOption common.RequestOption

	testReqOption.RequestType = "OpenTable"
	testReqOption.Owner = "0xf6b55acbbc49f4524aa48d19281a9a77c54de10f"
	testReqOption.Table = "contacts"

	marshalTestReqOption, err := json.Marshal(testReqOption)
	fmt.Printf("\nJSON --> %s", marshalTestReqOption)
	if err != nil {
		t.Fatalf("error marshaling testReqOption: %s", err)
	}
	testData.Data = string(marshalTestReqOption)
	testData.Address = ""
	svr.TestAddClient(testReqOption.Owner, testReqOption.Table, "email")
	svr.SelectHandler(&testData)
}

func OpenTable(svr *server.TCPIPServer, owner string, table string) {
	var testReqOption common.RequestOption
	var testData server.IncomingInfo

	testReqOption.RequestType = "OpenTable"
	testReqOption.Owner = "0xf6b55acbbc49f4524aa48d19281a9a77c54de10f"
	testReqOption.Table = "contacts"

	marshalTestReqOption, err := json.Marshal(testReqOption)
	if err != nil {
		fmt.Printf("error marshaling testReqOption: %s", err)
	}
	testData.Data = string(marshalTestReqOption)
	testData.Address = ""
	svr.TestAddClient(testReqOption.Owner, testReqOption.Table, "email")
	svr.SelectHandler(&testData)
}

func TestPut(t *testing.T) {
	swarmdb := common.NewSwarmDB()
	svr := server.NewTCPIPServer(swarmdb, nil)
	var testData server.IncomingInfo

	var testReqOption common.RequestOption
	testReqOption.RequestType = "Put"
	testReqOption.Owner = "0xf6b55acbbc49f4524aa48d19281a9a77c54de10f"
	testReqOption.Table = "contacts"
	testReqOption.Key = "rodneytest1@wolk.com"
	testReqOption.Value = `{"name": "Rodney", "age": 37, "email": "rodneytest1@wolk.com"}`

	marshalTestReqOption, err := json.Marshal(testReqOption)
	fmt.Printf("\nJSON --> %s", marshalTestReqOption)
	if err != nil {
		t.Fatalf("error marshaling testReqOption: %s", err)
	}
	OpenTable(svr, testReqOption.Owner, testReqOption.Table)
	testData.Data = string(marshalTestReqOption)
	testData.Address = ""
	svr.TestAddClient(testReqOption.Owner, testReqOption.Table, "email")
	svr.SelectHandler(&testData)
}

func TestGet(t *testing.T) {
	swarmdb := common.NewSwarmDB()
	svr := server.NewTCPIPServer(swarmdb, nil)
	var testData server.IncomingInfo

	var testReqOption common.RequestOption
	testReqOption.RequestType = "Get"
	testReqOption.Owner = "0xf6b55acbbc49f4524aa48d19281a9a77c54de10f"
	testReqOption.Table = "contacts"
	testReqOption.Key = "rodneytest1@wolk.com"

	marshalTestReqOption, err := json.Marshal(testReqOption)
	fmt.Printf("\nJSON --> %s", marshalTestReqOption)
	if err != nil {
		t.Fatalf("error marshaling testReqOption: %s", err)
	}
	OpenTable(svr, testReqOption.Owner, testReqOption.Table)
	testData.Data = string(marshalTestReqOption)
	testData.Address = ""
	svr.TestAddClient(testReqOption.Owner, testReqOption.Table, "email")
	svr.SelectHandler(&testData)
}

func TestPutGet(t *testing.T) {
	swarmdb := common.NewSwarmDB()
	svr := server.NewTCPIPServer(swarmdb, nil)
	var testData server.IncomingInfo

	var testReqOption common.RequestOption
	testReqOption.RequestType = "Put"
	testReqOption.Owner = "0xf6b55acbbc49f4524aa48d19281a9a77c54de10f"
	testReqOption.Table = "contacts"
	testReqOption.Key = "alinatest@wolk.com"
	testReqOption.Value = `{"name": "Alina", "age": 35, "email": "alinatest@wolk.com"}`

	marshalTestReqOption, err := json.Marshal(testReqOption)
	fmt.Printf("\nJSON --> %s", marshalTestReqOption)
	if err != nil {
		t.Fatalf("error marshaling testReqOption: %s", err)
	}
	OpenTable(svr, testReqOption.Owner, testReqOption.Table)
	testData.Data = string(marshalTestReqOption)
	testData.Address = ""
	svr.TestAddClient(testReqOption.Owner, testReqOption.Table, "email")
	svr.SelectHandler(&testData)

	testReqOption.RequestType = "Get"
	testReqOption.Owner = "0xf6b55acbbc49f4524aa48d19281a9a77c54de10f"
	testReqOption.Table = "contacts"
	testReqOption.Key = "alinatest@wolk.com"

	marshalTestReqOption, err = json.Marshal(testReqOption)
	fmt.Printf("\nJSON --> %s", marshalTestReqOption)
	if err != nil {
		t.Fatalf("error marshaling testReqOption: %s", err)
	}
	OpenTable(svr, testReqOption.Owner, testReqOption.Table)
	testData.Data = string(marshalTestReqOption)
	testData.Address = ""
	svr.TestAddClient(testReqOption.Owner, testReqOption.Table, "email")
	svr.SelectHandler(&testData)
}
