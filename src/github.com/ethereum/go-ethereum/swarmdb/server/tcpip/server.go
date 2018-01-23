package main

import (
	"fmt"
	"net"
	"time"
	"math/rand"
	"os"
	"encoding/hex"
	"strings"
	swarmdb "github.com/ethereum/go-ethereum/swarmdb"
	"io"
	"bufio"
	common "github.com/ethereum/go-ethereum/swarmdb"
	"github.com/ethereum/go-ethereum/crypto"
	"github.com/ethereum/go-ethereum/swarmdb/keymanager"
	"sync"
)

type ServerConfig struct {
	Addr string
	Port string
}

type Client struct {
	conn   net.Conn
	reader *bufio.Reader
	writer *bufio.Writer
	svr    *TCPIPServer
	table  *common.Table // holds ownerID, tableName
}

type TCPIPServer struct {
	swarmdb    *common.SwarmDB
	listener   net.Listener
	keymanager keymanager.KeyManager
	lock       sync.Mutex
}

const (
	CONN_HOST = "127.0.0.1" // telnet 10.128.0.7 8501
	CONN_PORT = "2000"
	CONN_TYPE = "tcp"
)


func RandStringRunes(n int) string {
	var letterRunes = []rune("0123456789abcdef")
	b := make([]rune, n)
	for i := range b {
		b[i] = letterRunes[rand.Intn(len(letterRunes))]
	}
	return string(b)
}


// Handles incoming requests.
func handleRequest(conn net.Conn, svr *TCPIPServer) {
	// generate a random 50 char challenge (64 hex chars)
	challenge := RandStringRunes(50)
	// challenge := "Hello, world!"
	reader := bufio.NewReader(conn)
	writer := bufio.NewWriter(conn)

	client := &Client{
		conn:   conn,
		reader: reader,
		writer: writer,
		svr:    svr,
	}

 	fmt.Fprintf(writer, "%s\n", challenge)
	writer.Flush()

	msg := fmt.Sprintf("\x19Ethereum Signed Message:\n%d%s", len(challenge), challenge)
	challenge_bytes := crypto.Keccak256([]byte(msg))
	
	resp, err := reader.ReadString('\n')
	if err != nil {
		fmt.Println("Error reading:", err.Error())
	} else {
		resp = strings.Trim(resp, "\r")
		resp = strings.Trim(resp, "\n")
	}

	// this should be the signed challenge, verify using valid_response
	response_bytes, err3 := hex.DecodeString(resp)
	if err3 != nil {
		fmt.Printf("ERR decoding response:[%s]\n", resp)
	}	
	verified, err := svr.keymanager.VerifyMessage(challenge_bytes, response_bytes)
	if err != nil {
		resp = "ERR"
	}  else if verified {
		resp = "OK"
	} else {
		resp = "INVALID"
	}
	fmt.Printf("%s Server Challenge [%s]-ethsign->[%x] Client %d byte Response:[%s] \n", resp,  challenge, challenge_bytes, len(response_bytes), resp);
	// fmt.Fprintf(writer, resp)
	writer.Flush()
	if ( resp == "OK" ) {
		for {
			str, err := client.reader.ReadString('\n')
			if err == io.EOF {
				conn.Close()
				break
			}
			if ( true ) {
				resp, err := svr.swarmdb.SelectHandler(keymanager.WOLKSWARMDB_ADDRESS, str)
				if err != nil {
					s := fmt.Sprintf("ERR: %s\n", err)
					fmt.Printf(s)
					writer.WriteString(s)
					writer.Flush()
				} else {
					fmt.Printf("Read: [%s] Wrote: [%s]\n", str, resp)
					writer.WriteString(resp + "\n")
					writer.Flush()
// 					fmt.Fprintf(client.writer, resp + "\n")
				}
			} else {
				writer.WriteString("OK\n")
				writer.Flush()
			}
		}
	} else {
		conn.Close()
	}
	// Close the connection when you're done with it.
}

func StartTCPIPServer(swarmdb *common.SwarmDB, config *ServerConfig) (err error) {
	sv := new(TCPIPServer)
	sv.swarmdb = swarmdb
	km, errkm := keymanager.NewKeyManager(keymanager.PATH, keymanager.WOLKSWARMDB_ADDRESS, keymanager.WOLKSWARMDB_PASSWORD)
	if errkm != nil {
		return err
	} else {
		sv.keymanager = km
	}

	// Listen for incoming connections.
	l, err := net.Listen(CONN_TYPE, CONN_HOST+":"+CONN_PORT)
	// l, err := net.Listen("tcp", fmt.Sprintf("127.0.0.1:%s", config.Port))
	if err != nil {
		fmt.Println("Error listening:", err.Error())
		os.Exit(1)
	} else {
		fmt.Println("Listening on " + CONN_HOST + ":" + CONN_PORT)
	}
	// Close the listener when the application closes.
	defer l.Close()

	// sv.listener = l

	// generate "truly" random strings
	rand.Seed(time.Now().UnixNano())
	for {
		// Listen for an incoming connection.
		conn, err := l.Accept()
		if err != nil {
			fmt.Println("Error accepting: ", err.Error())
			os.Exit(1)
		}
		// Handle connections in a new goroutine.
		go handleRequest(conn,  sv)
	}
}


func main() {
	fmt.Println("Launching server...")
	swdb := swarmdb.NewSwarmDB()
	tcpaddr := net.JoinHostPort("127.0.0.1", "2000")
	StartTCPIPServer(swdb, &ServerConfig{
		Addr: tcpaddr,
		Port: "2000",
	})
}




