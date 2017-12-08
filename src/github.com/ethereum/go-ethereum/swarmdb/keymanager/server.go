package main

import (
	"fmt"
	"net"
	"time"
	"math/rand"
	"os"
	"strings"
)

const (
	CONN_HOST = "10.128.0.7" // telnet 10.128.0.7 8501
	CONN_PORT = "8501"
	CONN_TYPE = "tcp"
)

func RandStringRunes(n int) string {
	var letterRunes = []rune("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ")
	b := make([]rune, n)
	for i := range b {
		b[i] = letterRunes[rand.Intn(len(letterRunes))]
	}
	return string(b)
}

func valid_response(resp string, nonce string, challenge string) (ok bool) {
	// use NaCl library to verify signature here

	// STUB
	fmt.Printf("Checking:[%s]\n", resp)
	if strings.Contains(resp, "invalid") {
		return false
	}  else {
		return true
	}
}

// Handles incoming requests.
func handleRequest(conn net.Conn, nonce string, challenge string) {
	// Make a buffer to hold incoming data.
	buf := make([]byte, 1024)
	// Read the incoming connection into the buffer.
	reqLen, err := conn.Read(buf)
	if err != nil {
		fmt.Println("Error reading:", err.Error())
	}
	
	// this should be the signed challenge, verify using valid_response
	resp := string(buf)
	resp = strings.Trim(resp, "\n")
	if valid_response(resp, nonce, challenge) {
		resp = "VALID"
	} else {
		resp = "INVALID"
	}
	s := fmt.Sprintf("%d:%s", reqLen, resp)
	conn.Write([]byte(s))
	// Close the connection when you're done with it.
	conn.Close()
}

func main() {
	// Listen for incoming connections.
	l, err := net.Listen(CONN_TYPE, CONN_HOST+":"+CONN_PORT)
	if err != nil {
		fmt.Println("Error listening:", err.Error())
		os.Exit(1)
	}
	// Close the listener when the application closes.
	defer l.Close()
	
	// generate truly random strings
	rand.Seed(time.Now().UnixNano())
	fmt.Println("Listening on " + CONN_HOST + ":" + CONN_PORT)
	for {
		// Listen for an incoming connection.
		conn, err := l.Accept()
		if err != nil {
			fmt.Println("Error accepting: ", err.Error())
			os.Exit(1)
		}
		// generate a random challenge and nonce
		challenge := RandStringRunes(32)
		nonce := RandStringRunes(24)
		s := fmt.Sprintf("%s|%s\n", challenge, nonce)
		conn.Write([]byte(s))

		// Handle connections in a new goroutine.
		go handleRequest(conn, nonce, challenge)
	}
}
