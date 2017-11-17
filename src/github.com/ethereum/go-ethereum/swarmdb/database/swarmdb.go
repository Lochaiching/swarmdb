package swarmdb

import(
    "fmt"
    "github.com/ethereum/go-ethereum/swarm/api"
    "github.com/ethereum/go-ethereum/swarm/storage"
)

type SwarmDB struct {
	tablelist map[string]tableinfo 
 	ldb  *storage.LDBDatabase 
	api  *api.Api
}

type tableinfo struct{
	//roothash	storage.Key
	roothash []byte
	database Database
}

type tabledata struct{
	indextype string
	primary bool
	rootnode []byte
}

func NewSwarmDB(api *api.Api, ldb *storage.LDBDatabase) (*SwarmDB){
	sd := new(SwarmDB)
	sd.api = api
	sd.ldb = ldb
	sd.tablelist = make(map[string]tableinfo)
	return sd
}

func (self *SwarmDB)Open(tablename string) (Database, error){
	if _, ok := self.tablelist[tablename]; !ok {
		td, _ := self.readTableData([]byte(tablename))
		var ti tableinfo
		ti.roothash = td.rootnode
		switch td.indextype{
		case "HD": ti.database, _ = api.NewHashDB(ti.roothash, self.api)
		default : ti.database, _ = api.NewHashDB(ti.roothash, self.api)
		}
		self.tablelist[tablename] = ti
	}
	return self.tablelist[tablename].database, nil
}

func (self *SwarmDB)readTableData(tablename []byte)(tabledata, error){
	data, err := self.ldb.Get(tablename)
	var td tabledata
	fmt.Println(data)
	
/////////dummy
	td.indextype = "HD"
	td.primary = true
	n, err := self.ldb.Get([]byte("RootNode"))
	td.rootnode = n 
	return td, err
}
