package main

import (
 "net"
 "fmt"
 "bufio"
"time"
	"strings"
 "os"
)

const (
	CONN_HOST = "10.128.0.7" // telnet 10.128.0.7 8501
	CONN_PORT = "8501"
	CONN_TYPE = "tcp"
)

func generate_challenge_response( nonce string, challenge string) (response string) {
	// use NaCl library + SGX enclave to generate challenge response
	ts := int32(time.Now().Unix())
	if ts % 2 == 0 {
		response = "validresponse"
	} else {
		response = "invalidresponse"
	}
	return response
}

func main() {
	conn, err := net.Dial(CONN_TYPE, CONN_HOST + ":" + CONN_PORT)
	if ( err != nil ) {
		fmt.Printf("Connection Error: %v\n", err)
		os.Exit(0);
	} 
	message, _ := bufio.NewReader(conn).ReadString('\n')
	sa := strings.Split(message, "|");
	if len(sa) > 1 {
		nonce := sa[0]
		challenge := strings.Trim(sa[1], "\n")
		response := generate_challenge_response(nonce, challenge)
		fmt.Printf("nonce:[%v] challenge:[%v] response:[%v]\n", nonce, challenge, response)
		fmt.Fprintf(conn, response + "\n")
		message2, _ := bufio.NewReader(conn).ReadString('\n')
		fmt.Printf("%s\n", message2)
		
	}
}
