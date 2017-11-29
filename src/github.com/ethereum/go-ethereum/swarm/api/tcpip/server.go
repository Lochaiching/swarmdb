package tcpip

import (
	"io"
	"bufio"
//	"encoding/go"
	"fmt"
//	"log"
	"net"
	"strings"
	"sync"
        "github.com/ethereum/go-ethereum/log"
	"github.com/ethereum/go-ethereum/swarmdb/database"
)

type Ownerinfo struct{
	name	string
	passwd	string
}

type Client struct{
	conn     net.Conn
	incoming chan string
	outgoing chan string
	reader   *bufio.Reader
	writer   *bufio.Writer
	owner	Ownerinfo
	databases	map[string]map[string]*swarmdb.Database
}

type ClientInfo struct{
	owner	Ownerinfo
	databases	map[string]map[string]*swarmdb.Database
}


func newClient(connection net.Conn) *Client {
	writer := bufio.NewWriter(connection)
	reader := bufio.NewReader(connection)
	client := &Client{
		conn:     connection,
		incoming: make(chan string),
		outgoing: make(chan string),
		reader:   reader,
		writer:   writer,
		databases: make(map[string]map[string]*swarmdb.Database),
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
		client.incoming <- line
		fmt.Printf("[%s]Read:%s", client.conn.RemoteAddr(), line)
	}
}
func (client *Client) write() {
	for data := range client.outgoing {
		client.writer.WriteString(data)
		client.writer.Flush()
		fmt.Printf("[%s]Write:%s\n", client.conn.RemoteAddr(), data)
	}
}


type Server struct{
	swarmdb *swarmdb.SwarmDB
	listener net.Listener
	clients	[]*Client
	conn 	chan net.Conn
	//connections map[string]string	
	//conns 	[]net.Conn
	lock    sync.Mutex
	incoming	chan string
	outgoing	chan string
	owners	map[string]map[string]map[string]*swarmdb.Database
	clientInfos	map[string]*ClientInfo
}

type ServerConfig struct{
	Addr	string
	Port	string
}

type HandlerFunc func(map[string]interface{})

type funcCall struct {
                FuncName string
                Args     map[string]interface{}
}
var registeredHandlers map[string]HandlerFunc

func NewServer(db *swarmdb.SwarmDB, l  net.Listener)(*Server){
	sv := new(Server)
	sv.swarmdb = db
	sv.listener = l
	//sv.conns := make(chan net.Conn, 128)
	sv.clients = make([]*Client, 0)
	sv.conn = make(chan net.Conn)
	sv.incoming = make(chan string)
	sv.outgoing = make(chan string)
	sv.owners = make(map[string]map[string]map[string]*swarmdb.Database)
	sv.clientInfos = make(map[string]*ClientInfo)
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

        registeredHandlers = make(map[string]HandlerFunc)
        registeredHandlers["TcpipDispatch"] = TcpipDispatch


	svr.listen()
	for{
		conn, err := svr.listener.Accept()
		if err != nil{
		}
		svr.conn <- conn
	}
		if err != nil {
        		log.Debug(fmt.Sprintf("err"))
		}
/*
	//defer svr.listener.Close()
    		tcpipReader := bufio.NewReader(conn)
		message, _ := tcpipReader.ReadString('\n')
		newmessage := strings.ToUpper(message)
    		conn.Write([]byte(newmessage + "\n"))
                var data funcCall
                dec := gob.NewDecoder(tcpipReader)
                err = dec.Decode(&data)
                log.Debug(fmt.Sprintf("[TCPIP] GOB Data: [%v]", data))
                if err != nil {
                        log.Debug(fmt.Sprintf("[TCPIP] Error decoding GOB data:", err))
                        return 
                }
                log.Debug(fmt.Sprintf("[TCPIP] Trying to call [%v] ", data.FuncName))
                log.Debug(fmt.Sprintf("[TCPIP] Trying to call [%v] ", data.Args))
                registeredHandlers[data.FuncName](data.Args)
*/
}

func TcpipDispatch(args map[string]interface{}) {
        log.Debug(fmt.Sprintf("Printing Args from TCPIP_Printer: \n%#v\n", args))
}

func (svr *Server) listen(){
	go func(){
		for{
			select{
			case conn := <-svr.conn:
				svr.addClient(conn)
			case data := <- svr.incoming:
				svr.selectHandler(data)
			}
		}
	}()
}

type fd struct{
	handler	string
	jsonstr	[]byte
}
	

func parseData(data string)(*fd){
	d := strings.Split(data, " ")
	r := new(fd)
	r.handler = d[0]
	r.jsonstr = []byte(d[1])
	return r
}

func (svr *Server) selectHandler(data string){
	d := parseData(data)
	switch d.handler{
	case "NewClient": 
		//svr.setClientInfo(d.jsonstr)
	case "OpenDatabase":
		//svr.HandleOpenDatabase()
	case "OpenTable":
	case "PUT":
	case "GET":
	case "CloseDatabase":
	}
}

/*
func (svr *Server)setClientInfo(jsonstr []byte)(){
	//jsonstr ->str[]
	if owner, ok := svr.owners["dummy"]; !ok{
		//read owner info
		owner.name = "dummy"
		owner.passwd = "dummy"
		svr.owners["dummy"] = owner
	}
	var cl ClientInfo
	cl.owner = owner
	cl.databases = make(map[string]map[string]*swarmdb.Database)
	svr.clientInfos[conn.RemoteAddr()] = cl
}
*/
	

func (svr *Server)addClient(conn net.Conn){
	client := newClient(conn)
	svr.clients = append(svr.clients, client)
	go func(){
		for{
			svr.incoming <- <- client.incoming
			client.outgoing <- <- svr.outgoing
		}
	}()
}

/*
func (svr *Server) HandleNewConnection(username, password string)(string, error){
	//pw, _err := svr.swarmdb.ldb.Get([]byte(username))
	pw := ([]byte("password"))
	var err error
	if err != nil || strings.Compare(password, string(pw)) != 0{
		return "", err
	}
/// dummy
	svr.lock.Lock()
	sessionid := username 
	if _, ok := svr.connections[sessionid]; ok {
	}
	svr.connections[sessionid] = username
	svr.lock.Unlock()
	return sessionid, nil
}

func handleConnection(conn net.Conn) error {
	buf := make([]byte, 1024)
	for{
		n, err := conn.Read(buf)
		if n == 0 {
			fmt.Printf("[%s]Recv:EOF\n", conn.RemoteAddr())
			break
		}
		CheckError(err, "Read Error")
		fmt.Printf("[%s]Recv:%s\n", conn.RemoteAddr(), string(buf[:n]))
		conn.Write([]byte("OK\n"))
	}
}

func (svr *Server)HandleOpenTable(tablename string){
}

func (svr *Server)HandleOpenIndex(tablename, indexname string){
}


*/
