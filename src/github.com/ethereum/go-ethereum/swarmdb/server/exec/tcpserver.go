package main

import (
	"fmt"
	swarmdb "github.com/ethereum/go-ethereum/swarmdb"
	tcpapi "github.com/ethereum/go-ethereum/swarmdb/server"
	"net"
)

func main() {
	fmt.Println("Launching server...")
	swdb := swarmdb.NewSwarmDB()
	tcpaddr := net.JoinHostPort("127.0.0.1", "2000")
	tcpapi.StartTCPIPServer(swdb, &tcpapi.ServerConfig{
		Addr: tcpaddr,
		Port: "2000",
	})
}
