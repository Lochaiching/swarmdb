package tcpip

import (
	"fmt"
	"io/ioutil"
	"os"
	//"github.com/ethereum/go-ethereum/log"
	"github.com/ethereum/go-ethereum/swarm/api"
	"github.com/ethereum/go-ethereum/swarm/api/tcpip"
	"github.com/ethereum/go-ethereum/swarm/storage"
	common "github.com/ethereum/go-ethereum/swarmdb/common"
	"github.com/ethereum/go-ethereum/swarmdb/database"
	"testing"
)

func testTcpServer(t *testing.T, f func(*tcpip.Server)) {
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
	api := api.NewApi(dpa, nil)
	dpa.Start()
	sdb := swarmdb.NewSwarmDB(api)
	//StartTCPServer
	svr := tcpip.NewServer(sdb, nil)

	if err != nil {
		fmt.Println("hashdb open error")
	}
	f(svr)
	dpa.Stop()
}

func TestTcpServerCreateTable(t *testing.T) {
	testTcpServer(t, func(svr *tcpip.Server) {
		o := common.TableOption{Index: "testindex1", Primary: 1, TreeType: "HD", KeyType: 1}
		var option []common.TableOption
		option = append(option, o)
		svr.NewConnection("owner1", "testconnection")
		svr.CreateTable("testtable", option, "testconnection")
		svr.OpenTable("testtable", "testconnection")
		//svr.Put("testindex1", "key", "value", "testconnection")
		putstr := `{"testindex1":"value2"}`
		svr.Put(putstr, "testconnection")
		res, err := svr.Get("testindex1", "key", "testconnection")
		fmt.Printf("Get %s %v \n", string(res), err)
		fres, ferr := svr.Get("testindex1", "value2", "testconnection")
		fmt.Printf("Get %s %v \n", string(fres), ferr)
		//svr.CloseTable()
	})
}
