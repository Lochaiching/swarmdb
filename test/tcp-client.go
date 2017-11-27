package main

import "net"
import "fmt"
import "encoding/gob"
import "bufio"
//import "os"

type funcCall struct {
	FuncName string 
	Args map[string]interface{}
}

func main() {

	// connect to this socket
	conn, connerr := net.Dial("tcp", "127.0.0.1:5000")
	if connerr != nil {
		fmt.Print("Error: ", connerr)
		return
	}
	nrw := bufio.NewReadWriter(bufio.NewReader(conn), bufio.NewWriter(conn))
	enc := gob.NewEncoder(nrw)
	fmt.Fprintf(conn, "GOB\n")
	//_, err := nrw.WriteString("GOB\n")
	message, _ := bufio.NewReader(conn).ReadString('\n')
	fmt.Print("Message from server: [" + message + "]")
	var testStruct funcCall
	testStruct.FuncName = "TcpipDispatch"
	testStruct.Args = make( map[string]interface{} ) 
	testStruct.Args["one"]= 1
	err := enc.Encode(testStruct)
	if err != nil {
		fmt.Println("Encode failed for struct: %#v", testStruct)
	}
	fmt.Println(fmt.Sprintf("Passing struct: %#v", testStruct))
	fmt.Println("About to flush")
	err = nrw.Flush()
	if err != nil {
		fmt.Println("Flush failed.")
	}
/*
	if err != nil {
		fmt.Println("Could not write GOB data ("+strconv.Itoa(n)+" bytes written)")
	}
*/	
	/*
	for {
		// read in input from stdin
		reader := bufio.NewReader(os.Stdin)
		fmt.Print("Text to send: ")
		text, _ := reader.ReadString('\n')
		// send to socket
		fmt.Fprintf(conn, text+"\n")
		// listen for reply
		message, _ := bufio.NewReader(conn).ReadString('\n')
		fmt.Print("Message from server: " + message)
	}
	*/
}
