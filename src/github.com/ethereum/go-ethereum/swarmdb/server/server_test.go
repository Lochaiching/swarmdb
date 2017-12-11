package server_test

import (
	"fmt"
	"io/ioutil"
	"os"
	common "github.com/ethereum/go-ethereum/swarmdb"
	"github.com/ethereum/go-ethereum/swarmdb/server"
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

<<<<<<< HEAD:src/github.com/ethereum/go-ethereum/swarmdb/server/server_test.go
func TestCreateTable(t *testing.T) {
	testTCPIPServer(t, func(svr *server.TCPIPServer) {
		// send JSON messages into TCPIPServer 
/*
		o := common.TableOption{Index: "testindex1", Primary: 1, TreeType: "BT", KeyType: 1}
		var option []common.TableOption
		option = append(option, o)
		svr.NewConnection("owner1")
		svr.CreateTable("testtable", option)
		svr.OpenTable("testtable", "testconnection")
		//svr.Put("testindex1", "key", "value", "testconnection")
		putstr := `{"testindex1":"value2"}`
		svr.Put(putstr, "testconnection")
		res, err := svr.Get("testindex1", "key", "testconnection")
		fmt.Printf("Get %s %v \n", string(res), err)
		fres, ferr := svr.Get("testindex1", "value2", "testconnection")
		fmt.Printf("Get %s %v \n", string(fres), ferr)
		fberr := svr.StartBuffer("testconnection")
		if fberr == nil{
			fmt.Printf("StartBuffer \n")
		}else {
			fmt.Printf("StartBuffer err = %v\n", fberr)
		}
			
		fberr = svr.FlushBuffer("testconnection")
		if fberr == nil{
			fmt.Printf("FlushBuffer \n")
		}else {
			fmt.Printf("FlushBuffer err = %v\n", fberr)
		}
		//svr.CloseTable()
*/
	})
}
