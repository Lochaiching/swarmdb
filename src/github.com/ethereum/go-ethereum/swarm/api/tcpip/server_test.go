package tcpip

import (
        "fmt"
	"io/ioutil"
	"os"
        //"github.com/ethereum/go-ethereum/log"
        "github.com/ethereum/go-ethereum/swarmdb/database"
        "github.com/ethereum/go-ethereum/swarm/api"
        "github.com/ethereum/go-ethereum/swarm/storage"
	"testing"
)

func testTcpServer(t *testing.T, f func(*Server)) {
	datadir, err := ioutil.TempDir("", "tcptest")
	if err != nil {
		t.Fatalf("unable to create temp dir: %v", err)
	}
	os.RemoveAll(datadir)
	defer os.RemoveAll(datadir)
	dpa, err := storage.NewLocalDPA(datadir)
	if err != nil {
		return
	}
        ldb, err:= storage.NewLDBDatabase("/tmp/ldb")
	if err != nil{
		fmt.Println("ldb error ", err)
	}
	api := api.NewApiTest(dpa, nil, ldb)
	dpa.Start()
	sdb := swarmdb.NewSwarmDB(api, ldb)
	//StartTCPServer
	svr := NewServer(sdb, nil)
	
	if err != nil{
		fmt.Println("hashdb open error")
	}
	f(svr)
	dpa.Stop()
}

func TestTcpServerCreateTable(t *testing.T){
	testTcpServer(t, func(svr *Server){
		o := TableOption{Index: "testindex1", Primary: 1, TreeType:"HD", KeyType:1}
		var option []TableOption
		option = append(option, o)
		svr.NewConnection("owner1", "testconnection")
		svr.CreateTable("testtable", option, "testconnection")
		svr.OpenTable("testtable", "testconnection")
		svr.Put("testindex1", "key", "value", "testconnection")
		//svr.Get("ttestindex", "testconnection")
		//svr.CloseTable()
	})
}	

