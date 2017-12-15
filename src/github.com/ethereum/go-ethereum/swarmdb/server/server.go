package server

import (
	"bufio"
	//"bytes"
	"encoding/json"
	"fmt"
	"io"
	//"io/ioutil"
	"net"
	"github.com/ethereum/go-ethereum/log"
	//"os"
	common "github.com/ethereum/go-ethereum/swarmdb"
	//"strconv"
	"sync"
)


type ServerConfig struct {
	Addr string
	Port string
}

type IncomingInfo struct {
	Data    string
	Address string
}

type Client struct {
	conn     net.Conn
	incoming chan *IncomingInfo
	outgoing chan string
	reader   *bufio.Reader
	writer   *bufio.Writer
	table    *common.Table  // holds ownerID, tableName
}

type TCPIPServer struct {
	swarmdb  *common.SwarmDB
	listener net.Listener
	conn     chan net.Conn
	incoming chan *IncomingInfo
	outgoing chan string
	clients  []*Client
	lock     sync.Mutex
}

func NewTCPIPServer(swarmdb *common.SwarmDB, l net.Listener) *TCPIPServer {
	sv := new(TCPIPServer)
	sv.listener = l
	sv.clients = make([]*Client, 0)
	sv.conn = make(chan net.Conn)
	sv.incoming = make(chan *IncomingInfo)
	sv.outgoing = make(chan string)
	sv.swarmdb = swarmdb
	return sv
}

func StartTCPIPServer(swarmdb *common.SwarmDB, config *ServerConfig) {
	log.Debug(fmt.Sprintf("tcp StartTCPIPServer"))

	//listen, err := net.Listen("tcp", config.Port)
	l, err := net.Listen("tcp", ":2000")
	log.Debug(fmt.Sprintf("tcp StartTCPIPServer with 2000"))

	svr := NewTCPIPServer(swarmdb, l)
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
	//go client.read()
	//go client.write()
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
		incoming.Data = line
		incoming.Address = client.conn.RemoteAddr().String()
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

func (svr *TCPIPServer) addClient(conn net.Conn) {
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

func (svr *TCPIPServer) TestAddClient(owner string, tablename string, primary string) {
	//testConn := svr.NewConnection()
	client := newClient(nil)//testConn)	
	client.table = svr.swarmdb.NewTable(owner, tablename);
	//client.table.SetPrimary( primary )
	svr.clients = append(svr.clients, client)
}

func (svr *TCPIPServer) listen() {
	go func() {
		for {
			select {
			case conn := <-svr.conn:
				svr.addClient(conn)
			case data := <-svr.incoming:
				svr.SelectHandler(data)
			}
		}
	}()
}
func (svr *TCPIPServer) SelectHandler(data *IncomingInfo) {
	var rerr *common.RequestFormatError
	d, err := parseData(data.Data)
	if err != nil {
		//svr.outgoing <- err.Error()
		return
	}
	switch d.RequestType {
	/*
	case "OpenClient":
		if len(d.Owner) == 0{
			svr.outgoing <- rerr.Error()
			return
		}
		err := svr.NewConnection()
		resp := "okay"
		if err != nil {
			resp = err.Error()
		}
		svr.outgoing <- resp
	*/
	case "OpenTable":
		if len(d.Table) == 0{
			svr.outgoing <- rerr.Error()
			return
		}
		err := svr.clients[0].table.OpenTable()
		//resp := "okay"
		//fmt.Printf("Resp: %s ", resp)
		if err != nil {
			//resp = err.Error()
		}
		//svr.outgoing <- resp
	case "CloseTable":
	case "CreateTable":
		if len(d.Table) == 0 || len(d.Columns) == 0{
			svr.outgoing <- rerr.Error()
			return
		}
		svr.clients[0].table.CreateTable(d.Columns, d.Bid, d.Replication, d.Encrypted)
		fmt.Printf("\nFinished Create")
	/*
	case "Insert":
		if len(d.Index) == 0 || len(d.Key) == 0 || len(d.Value) == 0{
			svr.outgoing <- rerr.Error()
			return
		}
		err := svr.table.Insert(d.Key, d.Value)
		if err != nil{
			svr.outgoing <- rerr.Error()
		}
		svr.outgoing <- "okay"
	*/
	case "Put":
		if len(d.Value) == 0{
			//svr.outgoing <- rerr.Error()
			fmt.Printf("\nValue empty -- bad!")
			return
		}
		err := svr.clients[0].table.Put(d.Value)
                if err != nil{
			fmt.Printf("\nError trying to 'Put' [%s] -- Err: %s", d.Value, err)
                        //svr.outgoing <- err.Error()
                }
	case "Get":
		if len(d.Key) == 0 {
		//	svr.outgoing <- rerr.Error()
			return
		}
		ret, err:= svr.clients[0].table.Get(d.Key)
		sret := string(ret)
		fmt.Printf("\nResult of GET: %s\n", sret)
		if err != nil{
			sret = err.Error()
		}
		//svr.outgoing <- sret
	/*
	case "Delete":
		if len(d.Key) == 0 {
			svr.outgoing <- rerr.Error()
			return
		}
		err := svr.table.Delete(d.Key)
		ret := "okay"
		if err != nil{
			ret = err.Error()
		}
		svr.outgoing <- ret
	case "StartBuffer":
		err := svr.table.StartBuffer()
		ret := "okay"
		if err != nil{
			ret = err.Error()
		}
		svr.outgoing <- ret
	case "FlushBuffer":
		err := svr.table.FlushBuffer()
		ret := "okay"
		if err != nil{
			ret = err.Error()
		}
		svr.outgoing <- ret
*/
	}
	//svr.outgoing <- "RequestType Error"
	return
}


func parseData(data string) (*common.RequestOption, error) {
	udata := new(common.RequestOption)
	if err := json.Unmarshal([]byte(data), udata); err != nil {
		return nil, err
	}
	return udata, nil
}

func (svr *TCPIPServer) NewConnection() (err error){
	ownerID := "owner1"
	tableName := "testtable"
	svr.swarmdb.NewTable(ownerID, tableName)
	
	// svr.table = table

	return nil
}

