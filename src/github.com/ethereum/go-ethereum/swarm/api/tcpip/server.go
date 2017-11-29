package tcpip

import (
//	"io"
	"encoding/gob"
	"fmt"
	"bufio"
	"net"
	"strings"
	"sync"
	"github.com/ethereum/go-ethereum/log"
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
	return sv
}

func StartTCPServer(swarmdb *swarmdb.SwarmDB) {
//func StartTCPServer(swarmdb *swarmdb.SwarmDB, config *ServerConfig) {
	//listen, err := net.Listen("tcp", config.Port)
        l, err := net.Listen("tcp", ":2000")
        
	svr := NewServer(swarmdb, l)
	if err != nil {
		//log.Fatal(err)
	}
	//defer svr.listener.Close()

        registeredHandlers = make(map[string]HandlerFunc)
        registeredHandlers["TcpipDispatch"] = TcpipDispatch

	for{
		conn, err := svr.listener.Accept()
		if err != nil {
			//log.Fatal(err)
		}
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
	}
}

func TcpipDispatch(args map[string]interface{}) {
        log.Debug(fmt.Sprintf("Printing Args from TCPIP_Printer: \n%#v\n", args))
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



