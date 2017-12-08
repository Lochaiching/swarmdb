package swarmdb

import (
//	"fmt"
	"strings"
	"sync"
	"github.com/ethereum/go-ethereum/swarm/api"
	"github.com/ethereum/go-ethereum/swarm/storage"
	common "github.com/ethereum/go-ethereum/swarmdb/common"
)

type SwarmDB struct {
	tablelist map[string]map[string]indexinfo
	Api       *api.Api
}

type indexinfo struct {
	//roothash	storage.Key
	//roothash []byte
	database common.Database
}

func NewSwarmDB(api *api.Api) *SwarmDB {
	sd := new(SwarmDB)
	sd.Api = api
	sd.tablelist = make(map[string]map[string]indexinfo)
	return sd
}

func (self *SwarmDB) GetIndexRootHash(tablename string) (roothash []byte, err error) {
	return self.Api.GetIndexRootHash([]byte(tablename))
}

func (self *SwarmDB) RetrieveFromSwarm(key storage.Key) storage.LazySectionReader {
	return self.Api.Retrieve(key)
}

/*
func (self *SwarmDB) Open(tablename string) error {
	if _, ok := self.tablelist[tablename]; !ok {
		td, err := self.readTableData([]byte(tablename))
		if err != nil {
			return err
		}
		self.tablelist[tablename] = td
	}
	return nil
}

func (self *SwarmDB) OpenIndex(tablename, indexname string) common.Database {
	return self.tablelist[tablename][indexname].database
}

func (self *SwarmDB) readTableData(tablename []byte) (map[string]indexinfo, error) {
	/// going to move it to either swarm or ens or pss
	data, err := self.ldb.Get(tablename)
	if err != nil {
		return nil, err
	}
	indexmap := make(map[string]indexinfo)
	fmt.Println(data)

	/////////dummy
	n, err := self.ldb.Get([]byte("RootNode"))

	for i := 0; i < 64; i++ {
		if data[2096+i*32] == 0 {
			return indexmap, nil
		}
		var idxinfo indexinfo
		name := data[2096+i*64 : 2096+i*64+28]
		itype := data[i*64+2048+28 : i*64+2048+30]
		hash := data[i*64+2048+32 : 2096+(i+1)*64]
		if hash == nil {
			hash = n ////////////dummy
		}
		switch string(itype) {
		case "BP":
		//	idxinfo.database = swarmdb.NewBPlusTreeDB(self.api)
		case "HD":
		//	idxinfo.database, _ = swarmdb.NewHashDB(self.Api)
		case "KD":
		//	idxinfo.database, _ = swarmdb.NewKademliaDB(self.Api)
		default:
			//		idxinfo.database, _ = swarmdb.NewHashDB(self.api)
		}
		indexmap[string(name)] = idxinfo
		i++
	}
	return indexmap, err
}
*/


func (self *SwarmDB) StoreIndexRootHash(name []byte, roothash []byte) (err error) {
       return self.Api.StoreIndexRootHash(name, roothash)
}


func (self *SwarmDB)StoreToSwarm(content string) (storage.Key, error){
       r := strings.NewReader(content)
       wg := &sync.WaitGroup{}
       key, err := self.Api.Store(r, int64(len(content)), wg)
       wg.Wait()
       if err != nil {
               return nil, err
       }
       return key, err
}

