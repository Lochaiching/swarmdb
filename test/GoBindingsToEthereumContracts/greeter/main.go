package main

import (
	"fmt"
	"log"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/ethclient"
)

func main() {
	// Create an IPC based RPC connection to a remote node
//y	conn, err := ethclient.Dial("/home/karalabe/.ethereum/testnet/geth.ipc")
//  conn, err := ethclient.Dial("/var/www/vhosts/data/geth.ipc")      // this is working OK
    conn, err := ethclient.Dial("http://127.0.0.1:8545")              // this is working OK	   //  JSON-RPC Endpoint   https://github.com/ethereum/wiki/wiki/JSON-RPC
	if err != nil {
		log.Fatalf("Failed to connect to the Ethereum client: %v", err)
	}
	// Instantiate the contract and display its name
	greeter, err := NewGreeter(common.HexToAddress("0x4bb74b4f0a305da3f435175a92f1bedb0269ee60"), conn)
	if err != nil {
		log.Fatalf("Failed to instantiate a greeter contract: %v", err)
	}
	Greet, err := greeter.Greet(nil)
	if err != nil {
		log.Fatalf("Failed to retrieve Greet: %v", err)
	}
	fmt.Println("Greet:", Greet)
}