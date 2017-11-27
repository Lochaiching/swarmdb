package tcpip

import (
//	"io"
	"bufio"
	"log"
	"net"
	"strings"
	"sync"
	"github.com/ethereum/go-ethereum/swarmdb/database"
)

type Server struct{
	swarmdb *swarmdb.SwarmDB
	listener net.Listener
	connections map[string]string	
	lock    sync.Mutex
}

type ServerConfig struct{
	Addr	string
	Port	string
}

func NewServer(db *swarmdb.SwarmDB, l  net.Listener)(*Server){
	sv := new(Server)
	sv.swarmdb = db
	sv.listener = l
	return sv
}

func StartTCPServer(swarmdb *swarmdb.SwarmDB, config *ServerConfig) {
	//listen, err := net.Listen("tcp", config.Port)
        l, err := net.Listen("tcp", ":2000")
        
	svr := NewServer(swarmdb, l)
	if err != nil {
		log.Fatal(err)
	}
	//defer svr.listener.Close()

	for{
		conn, err := svr.listener.Accept()
		if err != nil {
			log.Fatal(err)
		}
	//defer svr.listener.Close()
    		message, _ := bufio.NewReader(conn).ReadString('\n')
		newmessage := strings.ToUpper(message)
    		conn.Write([]byte(newmessage + "\n"))
	}
}

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

func (svr *Server)HandleOpenTable(tablename string){
}

func (svr *Server)HandleOpenIndex(tablename, indexname string){
}



