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
	tcpaddr := net.JoinHostPort("127.0.0.1", "8503")
	tcpapi.StartTCPIPServer(swdb, &tcpapi.ServerConfig{
		Addr: tcpaddr,
		Port: "23456",
	})
}
