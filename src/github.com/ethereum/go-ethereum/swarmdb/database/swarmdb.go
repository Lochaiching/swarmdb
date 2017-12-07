package swarmdb

import (
	"fmt"
	"github.com/ethereum/go-ethereum/swarm/api"
	"github.com/ethereum/go-ethereum/swarm/storage"
	"github.com/ethereum/go-ethereum/swarmdb/packages"
)

type SwarmDB struct {
	tablelist map[string]map[string]indexinfo
	ldb       *storage.LDBDatabase
	api       *api.Api
}

type indexinfo struct {
	//roothash	storage.Key
	//roothash []byte
	database Database
}

/*
type tabledata struct{
	indextype string
	primary bool
	rootnode []byte
}
*/

func NewSwarmDB(api *api.Api, ldb *storage.LDBDatabase) *SwarmDB {
	sd := new(SwarmDB)
	sd.api = api
	sd.ldb = ldb
	sd.tablelist = make(map[string]map[string]indexinfo)
	return sd
}

func (self *SwarmDB)GetIndexRootHash(tablename string) (roothash []byte, err error) {
	return self.api.GetIndexRootHash(tablename)
}

func (self *SwarmDB)RetrieveFromSwarm(key storage.Key) storage.LazySectionReader {
	return self.api.Retrieve(key)
}

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

func (self *SwarmDB) OpenIndex(tablename, indexname string) Database {
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
		case "HD":
		//	idxinfo.database, _ = swarmdb.NewHashDB(self.api)
		case "KD":
			idxinfo.database, _ = swarmdb.NewKademliaDB(self.api)
		default:
			idxinfo.database, _ = swarmdb.NewKademliaDB(self.api)
		}
		indexmap[string(name)] = idxinfo
		i++
	}
	return indexmap, err
}
