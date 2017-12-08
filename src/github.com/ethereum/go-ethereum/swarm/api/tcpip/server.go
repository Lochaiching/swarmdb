package tcpip

import (
	"bufio"
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	//	"log"
	"net"
		//"strings"
	"github.com/ethereum/go-ethereum/log"
	//"github.com/ethereum/go-ethereum/swarm/api"
	"github.com/ethereum/go-ethereum/swarmdb/common"
	"github.com/ethereum/go-ethereum/swarmdb/database"
	tree "github.com/ethereum/go-ethereum/swarmdb/tree"
	leaf "github.com/ethereum/go-ethereum/swarmdb/leaf"
	"strconv"
	"sync"
)

/* moved to swarmdb/common pkg
type RequestOption struct {
        RequestType  string        `json:"requesttype"` //"OpenConnection, Insert, Get, Put, etc"
        Owner        string        `json:"owner,omitempty"`
        Table        string        `json:"table,omitempty"`           //"contacts"
	Index        string        `json:"index,omitempty"`
        Key          string        `json:"key,omitempty"`   //value of the key, like "rodney@wolk.com"
        Value        string        `json:"value,omitempty"` //value of val, usually the whole json record
        TableOptions []TableOption `json:"tableoptions",omitempty"`
}
type TableOption struct {
        TreeType  string `json:"treetype,omitempty"`
        Index     string `json:"index,omitempty"`
        KeyType   int    `json:"keytype,omitempty"`
        Primary   int    `json:"primary,omitempty"`
}
*/

type TableInfo struct {
	tablename string
	roothash  []byte
	indexes   map[string]*IndexInfo
	primary	  string
	counter   int   //// not supported yet. 
}

func (svr *Server) NewTableInfo(tablename string) TableInfo {
	var tbl TableInfo
	tbl.tablename = tablename
	tbl.indexes = make(map[string]*IndexInfo)
	return tbl
}

type IndexInfo struct {
	indexname string
	indextype string
	roothash  []byte
	dbaccess  common.Database
	active    int
	primary   int
	keytype   int
}

type OwnerInfo struct {
	name   string
	passwd string
	tables map[string]*TableInfo
}

func NewOwnerInfo(name, passwd string) OwnerInfo {
	var owner OwnerInfo
	owner.name = name
	owner.passwd = passwd
	owner.tables = make(map[string]*TableInfo)
	return owner
}

type IncomingInfo struct {
	data    string
	address string
}

type Client struct {
	conn     net.Conn
	incoming chan *IncomingInfo
	outgoing chan string
	reader   *bufio.Reader
	writer   *bufio.Writer
	//owner	OwnerInfo
	//databases	map[string]map[string]*common.Database
}

type ClientInfo struct {
	owner          *OwnerInfo
	tables         map[string]*TableInfo
	openedtablename string
	openedtable    *TableInfo
	kaddb       *leaf.KademliaDB
}

func newClient(connection net.Conn) *Client {
	writer := bufio.NewWriter(connection)
	reader := bufio.NewReader(connection)
	client := &Client{
		conn:     connection,
		incoming: make(chan *IncomingInfo),
		outgoing: make(chan string),
		reader:   reader,
		writer:   writer,
		//databases: make(map[string]map[string]*common.Database),
	}
	go client.read()
	go client.write()
	return client
}

func (client *Client) read() {
	for {
		line, err := client.reader.ReadString('\n')
		if err == io.EOF {
			client.conn.Close()
			break
		}
		if err != nil {
			////////
		}
		incoming := new(IncomingInfo)
		incoming.data = line
		incoming.address = client.conn.RemoteAddr().String()
		//client.incoming <- line
		client.incoming <- incoming
		fmt.Printf("[%s]Read:%s", client.conn.RemoteAddr(), line)
	}
}
func (client *Client) write() {
	for data := range client.outgoing {
		client.writer.WriteString(data)
		//client.writer.Write(data)
		client.writer.Flush()
		fmt.Printf("[%s]Write:%s\n", client.conn.RemoteAddr(), data)
	}
}

type Server struct {
	swarmdb  *swarmdb.SwarmDB
	listener net.Listener
	clients  []*Client
	conn     chan net.Conn
	lock     sync.Mutex
	incoming chan *IncomingInfo
	outgoing chan string
	// owner -> table name -> index name -> (index hash root -> pointer)
	//databases	map[string]map[string]map[string]*indexdata
	owners      map[string]*OwnerInfo
	tables      map[string]map[string]*TableInfo
	clientInfos map[string]*ClientInfo
}

type ServerConfig struct {
	Addr string
	Port string
}

func NewServer(db *swarmdb.SwarmDB, l net.Listener) *Server {
	sv := new(Server)
	sv.swarmdb = db
	sv.listener = l
	sv.clients = make([]*Client, 0)
	sv.conn = make(chan net.Conn)
	sv.incoming = make(chan *IncomingInfo)
	sv.outgoing = make(chan string)
	sv.tables = make(map[string]map[string]*TableInfo)
	sv.owners = make(map[string]*OwnerInfo)
	sv.clientInfos = make(map[string]*ClientInfo)
	//kdb, _ := leaf.NewKademliaDB(db.Api)
	//sv.kaddb = kdb
	return sv
}

func StartTCPServer(swarmdb *swarmdb.SwarmDB, config *ServerConfig) {
	log.Debug(fmt.Sprintf("tcp StartTCPServer"))

	//listen, err := net.Listen("tcp", config.Port)
	l, err := net.Listen("tcp", ":2000")
	log.Debug(fmt.Sprintf("tcp StartTCPServer with 2000"))

	svr := NewServer(swarmdb, l)
	if err != nil {
		//log.Fatal(err)
		log.Debug(fmt.Sprintf("err"))
	}
	//defer svr.listener.Close()

	svr.listen()
	for {
		conn, err := svr.listener.Accept()
		if err != nil {
		}
		svr.conn <- conn
	}
	if err != nil {
		//	log.Fatal(err)
		log.Debug(fmt.Sprintf("err"))
	}
	defer svr.listener.Close()
}

func (svr *Server) listen() {
	go func() {
		for {
			select {
			case conn := <-svr.conn:
				svr.addClient(conn)
			case data := <-svr.incoming:
				svr.selectHandler(data)
			}
		}
	}()
}

func parseData(data string) (*common.RequestOption, error) {
	udata := new(common.RequestOption)
	if err := json.Unmarshal([]byte(data), udata); err != nil {
		return nil, err
	}
	return udata, nil
}

func (svr *Server) selectHandler(data *IncomingInfo) {
	var rerr *common.RequestFormatError
	d, err := parseData(data.data)
	if err != nil {
		svr.outgoing <- err.Error()
		return
	}
	switch d.RequestType {
	case "OpenClient":
		if len(d.Owner) == 0{
			svr.outgoing <- rerr.Error()
			return
		}
		err := svr.NewConnection(string(d.Owner), data.address)
		resp := "okay"
		if err != nil {
			resp = err.Error()
		}
		svr.outgoing <- resp
	case "OpenTable":
		if len(d.Table) == 0{
			svr.outgoing <- rerr.Error()
			return
		}
		err := svr.OpenTable(string(d.Table), data.address)
		resp := "okay"
		if err != nil {
			resp = err.Error()
		}
		svr.outgoing <- resp
	case "CloseTable":
	case "CreateTable":
		if len(d.Table) == 0 || len(d.TableOptions) == 0{
			svr.outgoing <- rerr.Error()
			return
		}
		svr.CreateTable(string(d.Table), d.TableOptions, data.address)
	case "Insert":
		if len(d.Index) == 0 || len(d.Key) == 0 || len(d.Value) == 0{
			svr.outgoing <- rerr.Error()
			return
		}
		err := svr.Insert(d.Index, d.Key, d.Value, data.address)
		if err != nil{
			svr.outgoing <- rerr.Error()
		}
		svr.outgoing <- "okay"
	case "Put":
		if len(d.Index) == 0 || len(d.Key) == 0 || len(d.Value) == 0{
			svr.outgoing <- rerr.Error()
			return
		}
		//err := svr.Put(d.Index, d.Key, d.Value, data.address)
		err := svr.Put(d.Value, data.address)
                if err != nil{
                        svr.outgoing <- err.Error()
                }

	case "Get":
		if len(d.Index) == 0 || len(d.Key) == 0 {
			svr.outgoing <- rerr.Error()
			return
		}
		ret, err:= svr.Get(d.Index, d.Key, data.address)
		sret := string(ret)
		if err != nil{
			sret = err.Error()
		}
		svr.outgoing <- sret
	case "Delete":
		if len(d.Index) == 0 || len(d.Key) == 0 {
			svr.outgoing <- rerr.Error()
			return
		}
	}
	svr.outgoing <- "RequestType Error"
	return
}

func (svr *Server) addClient(conn net.Conn) {
	client := newClient(conn)
	/// this one is not good. need to change it
	svr.clients = append(svr.clients, client)
	go func() {
		for {
			svr.incoming <- <-client.incoming
			client.outgoing <- <-svr.outgoing
		}
	}()
}

func (svr *Server) NewConnection(ownername string, address string) (err error){
	//////  do authentication if needed.

	if _, ok := svr.owners[string(ownername)]; !ok {
		svr.owners[ownername] = svr.loadOwnerInfo(ownername)
		svr.tables[ownername] = make(map[string]*TableInfo)
		svr.owners[ownername].tables = svr.tables[ownername]
	}
	if _, ok := svr.clientInfos[address]; !ok {
		cl := new(ClientInfo)
		cl.owner = svr.owners[ownername]
		cl.tables = svr.tables[ownername]
		cl.kaddb, err = leaf.NewKademliaDB(svr.swarmdb.Api)
		if err != nil{
			return err	
		}
		svr.clientInfos[address] = cl
	} else {
	}
///// authentication needed

	//fmt.Println("NewConnection address", address, "cl", svr.clientInfos[address])
	return nil
}

func (svr *Server) loadOwnerInfo(ownername string) *OwnerInfo {
	owner := new(OwnerInfo)
	owner.name = ownername
	/// authentication
	/*
		//owner, err := svr.swarmdb.checkOwner(ownername)
		if err != nil{
			return nil, err
		}
	*/
	return owner
}

func (svr *Server) CreateTable(tablename string, option []common.TableOption, address string) (err error) {
	buf := make([]byte, 4096)
	for i, columninfo := range option {
		copy(buf[2048+i*64:], columninfo.Index)
		copy(buf[2048+i*64+26:], strconv.Itoa(columninfo.Primary))
		copy(buf[2048+i*64+27:], "9")
		copy(buf[2048+i*64+28:], strconv.Itoa(columninfo.KeyType))
		copy(buf[2048+i*64+30:], columninfo.TreeType)
	}
	// need to store KDB??
	swarmhash, err := svr.swarmdb.StoreToSwarm(string(buf))
	if err != nil {
		return
	}
	err = svr.swarmdb.StoreIndexRootHash([]byte(tablename), []byte(swarmhash))
	return err
}

func (svr *Server) OpenTable(tablename string, address string) (err error) {
	cl := svr.clientInfos[address]
	owner := svr.owners[cl.owner.name]
	if _, ok := svr.tables[cl.owner.name][tablename]; !ok {
		///// get table info
		svr.tables[cl.owner.name][tablename], err = svr.loadTableInfo(cl.owner.name, tablename)
		if err != nil {
			return err
		}
	}
	owner.tables[tablename] = svr.tables[cl.owner.name][tablename]
	cl.tables[tablename] = svr.tables[cl.owner.name][tablename]
	for index := range svr.owners[cl.owner.name].tables[tablename].indexes {
		cl.tables[tablename].indexes[index] = svr.owners[cl.owner.name].tables[tablename].indexes[index]
	}
	cl.openedtable = cl.tables[tablename]
	cl.openedtablename = tablename
	svr.clientInfos[address] = cl
	return nil
}

func (svr *Server) loadTableInfo(owner string, tablename string) (*TableInfo, error) {
	table := svr.NewTableInfo(tablename)
	table.indexes = make(map[string]*IndexInfo)
	/// get TableInfo
	roothash, err := svr.swarmdb.GetIndexRootHash(tablename)
	if err != nil {
		return nil, err
	}
	setprimary := false
	indexdata := svr.swarmdb.RetrieveFromSwarm(roothash)
	indexdatasize, _ := indexdata.Size(nil)
	indexbuf := make([]byte, indexdatasize)
	_, _ = indexdata.ReadAt(indexbuf, 0)
	for i := 2048; i < 4096; i = i + 64 {
		//    if
		buf := make([]byte, 64)
		copy(buf, indexbuf[i:i+64])
		if buf[0] == 0 {
			break
		}
		indexinfo := new(IndexInfo)
		indexinfo.indexname = string(bytes.Trim(buf[:25], "\x00"))
		indexinfo.primary, _ = strconv.Atoi(string(buf[26:27]))
		indexinfo.active, _ = strconv.Atoi(string(buf[27:28]))
		indexinfo.keytype, _ = strconv.Atoi(string(buf[28:29]))
		indexinfo.indextype = string(buf[30:32])
		copy(indexinfo.roothash, buf[31:63])
		switch indexinfo.indextype {
		case "BT" :
			indexinfo.dbaccess = tree.NewBPlusTreeDB(svr.swarmdb.Api, indexinfo.roothash, common.KeyType(indexinfo.keytype))
			if err != nil {
				return nil, err
			}
		case "HD":
			indexinfo.dbaccess, err = tree.NewHashDB(indexinfo.roothash, svr.swarmdb.Api)
			if err != nil {
				return nil, err
			}
		}
		table.indexes[indexinfo.indexname] = indexinfo
		if indexinfo.primary == 1{
			if !setprimary{
				table.primary = indexinfo.indexname
			}else{
        			var rerr *common.RequestFormatError
				return nil, rerr
			}
		}
	}
	return &table, nil
}

func (svr *Server) Put(value string, address string) (err error) {
	/// store value to kdb and get a hash
	cl := svr.clientInfos[address]
	var evalue interface{} 
        if err := json.Unmarshal([]byte(value), &evalue); err != nil {
                //return err
        }
	pvalue := evalue.(map[string]interface{})[cl.openedtable.primary]

	cl.kaddb.Open([]byte(cl.owner.name), []byte(cl.openedtable.tablename), []byte(cl.openedtable.primary))

	//khash := svr.kaddb.GenerateChunkKey([]byte(key))
	khash, err := cl.kaddb.Put([]byte(pvalue.(string)), []byte(value))
//////// need to put every indexes but currently added only for the primary index
	_, err = cl.tables[cl.openedtablename].indexes[cl.openedtable.primary].dbaccess.Put([]byte(pvalue.(string)), []byte(khash))
	return err
}
/*

func (svr *Server) Put(index, key, value string, address string) error {
	/// store value to kdb and get a hash
	cl := svr.clientInfos[address]
	cl.kaddb.Open([]byte(svr.clientInfos[address].owner.name), []byte(svr.clientInfos[address].openedtable.tablename), []byte(index))

	//khash := svr.kaddb.GenerateChunkKey([]byte(key))
	khash, err := cl.kaddb.Put([]byte(key), []byte(value))
	_, err = cl.tables[cl.openedtablename].indexes[index].dbaccess.Put([]byte(key), []byte(khash))
	return err
}
*/

func (svr *Server) Insert(index, key, value string, address string) error {
        /// store value to kdb and get a hash
        _, b, err := svr.clientInfos[address].openedtable.indexes[index].dbaccess.Get([]byte(key))
	if b {
		var derr *common.DuplicateKeyError
		return derr
	}
	if err != nil{
		return err
	}
	cl := svr.clientInfos[address]
        cl.kaddb.Open([]byte(svr.clientInfos[address].owner.name), []byte(svr.clientInfos[address].openedtable.tablename), []byte(index))

        //khash := cl.kaddb.GenerateChunkKey([]byte(key))
        khash, err := cl.kaddb.Put([]byte(key), []byte(key))
	if err != nil {	
		return err
	}
        _, err = svr.clientInfos[address].openedtable.indexes[index].dbaccess.Insert([]byte(key), []byte(khash))
        return err
}

func (svr *Server) Get(index, key string, address string) ([]byte, error) {
	cl := svr.clientInfos[address]
	if cl.openedtable.indexes[index] == nil{
		var cerr *common.NoColumnError
		return nil, cerr
	}
	_, _, err := cl.openedtable.indexes[index].dbaccess.Get([]byte(key))
	if err != nil {
		return nil, err
	}

	/// get value from kdb
	kres, _, _ := cl.kaddb.Get([]byte(key))
	fres := bytes.Trim(kres, "\x00")
	return fres, err
}
