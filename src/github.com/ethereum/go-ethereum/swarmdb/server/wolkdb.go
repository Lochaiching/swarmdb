package main

import (
	"bufio"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"flag"
	"fmt"
	"github.com/ethereum/go-ethereum/crypto"
	"github.com/ethereum/go-ethereum/log"
	swarmdb "github.com/ethereum/go-ethereum/swarmdb"
	"github.com/rs/cors"
	"io"
	"io/ioutil"
	logger "log"
	"math/rand"
	"net"
	"net/http"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"
)

type Client struct {
	conn   net.Conn
	reader *bufio.Reader
	writer *bufio.Writer
	svr    *TCPIPServer
	table  *swarmdb.Table // holds ownerID, tableName
}

type TCPIPServer struct {
	swarmdb    *swarmdb.SwarmDB
	listener   net.Listener
	keymanager swarmdb.KeyManager
	lock       sync.Mutex
}

func RandStringRunes(n int) string {
	var letterRunes = []rune("0123456789abcdef")
	b := make([]rune, n)
	for i := range b {
		b[i] = letterRunes[rand.Intn(len(letterRunes))]
	}
	return string(b)
}

type HTTPServer struct {
	swarmdb    *swarmdb.SwarmDB
	listener   net.Listener
	keymanager swarmdb.KeyManager
	//lock       sync.Mutex
}

type SwarmDBReq struct {
	protocol string
	owner    string
	table    string
	key      string
}

func buildErrorResp(err error) string {
	var respObj swarmdb.SWARMDBResponse
	wolkErr, ok := err.(*swarmdb.SWARMDBError)
	if !ok {
		return (`{ "errorcode":-1, "errormessage":"UNKNOWN ERROR"}`) //TODO: Make Default Error Handling
	}
	if wolkErr.ErrorCode == 0 { //FYI: default empty int is 0. maybe should be a pointer.  //TODO this is a hack with what errors are being returned right now
		//fmt.Printf("wolkErr.ErrorCode doesn't exist\n")
		respObj.ErrorCode = 888
		respObj.ErrorMessage = err.Error()
	} else {
		respObj.ErrorCode = wolkErr.ErrorCode
		respObj.ErrorMessage = wolkErr.ErrorMessage
	}
	jbyte, jErr := json.Marshal(respObj)
	if jErr != nil {
		//fmt.Printf("Error: [%s] [%+v]", jErr.Error(), respObj)
		return `{ "errorcode":-1, "errormessage":"DEFAULT ERROR"}` //TODO: Make Default Error Handling
	}
	jstr := string(jbyte)
	return jstr
}

// Handles incoming TCPIP requests.
func handleTcpipRequest(conn net.Conn, svr *TCPIPServer) {
	// generate a random 50 char challenge (64 hex chars)
	challenge := RandStringRunes(50)
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

	var swErr swarmdb.SWARMDBError
	resp, err := reader.ReadString('\n')
	if err != nil {
		fmt.Printf("Problem reading RAW TCPIP input (%s).  ERROR:[%s]", resp, err.Error())
		swErr.SetError(fmt.Sprintf("Problem reading RAW TCPIP input (%s).  ERROR:[%s]", resp, err.Error()))
		log.Error(swErr.Error())
		//TODO: return a TCPIP error response
		tcpJson := buildErrorResp(err)
		writer.WriteString(tcpJson)
		writer.Flush()

	} else {
		resp = strings.Trim(resp, "\r")
		resp = strings.Trim(resp, "\n")
	}
	fmt.Printf("handleTcpipRequest response %v\n", resp)

	// this should be the signed challenge, verify using valid_response
	response_bytes, errDecoding := hex.DecodeString(resp)
	if errDecoding != nil {
		swErr.SetError(fmt.Sprintf("Problem decoding TCPIP input.  ERROR:[%s]", errDecoding.Error()))
		log.Error(swErr.Error())
		swErr.ErrorCode = 422
		swErr.ErrorMessage = fmt.Sprintf("Unable to Decode Response sent [%s]", resp)
		tcpJson := buildErrorResp(&swErr)
		writer.WriteString(tcpJson)
		writer.Flush()
	}

	u, err := svr.keymanager.VerifyMessage(challenge_bytes, response_bytes)
	if err != nil {
		log.Debug("\nERROR: %s", err.Error())
		tcpJson := buildErrorResp(err)
		writer.WriteString(tcpJson)
		writer.Flush()
		conn.Close()
	} else {
		log.Debug("%s Server Challenge [%s]-ethsign->[%x] Client %d byte Response:[%s] \n", resp, challenge, challenge_bytes, len(response_bytes), resp)
		writer.Flush()
		for {
			str, err := client.reader.ReadString('\n')
			if err == io.EOF {
				//TODO: return a TCPIP error response
				// Close the connection when done
				conn.Close()
				break
			}
			if true {
				if resp, err := svr.swarmdb.SelectHandler(u, string(str)); err != nil {
					log.Debug("ERROR: %+v", err)
					tcpJson := buildErrorResp(err)
					fmt.Printf("Read: [%s] Wrote: [%s]\n", str, tcpJson)
					_, err := writer.WriteString(tcpJson + "\n")
					if err != nil {
						fmt.Printf("writer err: %v\n", err)
						//TODO handle if writestring has err
					}
					writer.Flush()
				} else {
					fmt.Printf("Read: [%s] Wrote: [%s]\n", str, resp)
					_, err := writer.WriteString(resp + "\n")
					if err != nil {
						fmt.Printf("writer err: %v\n", err)
						//TODO handle if writestring has err
					}
					writer.Flush()
				}
			} else {
				writer.WriteString("OK\n")
				writer.Flush()
			}
		}
	}
}

func StartTcpipServer(sdb *swarmdb.SwarmDB, conf *swarmdb.SWARMDBConfig) (err error) {
	sv := new(TCPIPServer)
	sv.swarmdb = sdb
	km, errkm := swarmdb.NewKeyManager(conf)
	if errkm != nil {
		log.Error(errkm.Error())
		return errkm
	} else {
		sv.keymanager = km
	}

	host := swarmdb.SWARMDBCONF_LISTENADDR
	port := swarmdb.SWARMDBCONF_PORTTCP

	// Listen for incoming connections.
	if len(conf.ListenAddrTCP) > 0 {
		host = conf.ListenAddrTCP
	}
	if conf.PortTCP > 0 {
		port = conf.PortTCP
	}

	//TODO: Do we want default host/port if not in config?

	host_port := fmt.Sprintf("%s:%d", host, port)
	l, err := net.Listen("tcp", host_port)

	var swErr swarmdb.SWARMDBError
	if err != nil {
		swErr.SetError(fmt.Sprintf("Error trying to listen (tcp) on host/port [%s].  ERROR:[%s]", host_port, err))
		log.Error(swErr.Error())
		return err //TODO: investigate why returning swErr fails
		os.Exit(1) //TODO: should we exit?
	} else {
		log.Debug("TCPIP Server Listening on " + host_port)
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
		go handleTcpipRequest(conn, sv)
	}
}

func parsePath(path string) (swdbReq SwarmDBReq, err error) {
	pathParts := strings.Split(path, "/")
	if len(pathParts) < 2 {
		swErr := swarmdb.SWARMDBError{ErrorCode: -1, ErrorMessage: "Request URL invalid"}
		swErr.SetError("Invalid Path in Request URL")
		return swdbReq, &swErr
	} else {
		for k, v := range pathParts {
			switch k {
			case 1:
				swdbReq.protocol = v

			case 2:
				swdbReq.owner = v

			case 3:
				swdbReq.table = v

			case 4:
				swdbReq.key = v
			}
		}
	}
	return swdbReq, nil
}

func StartHttpServer(sdb *swarmdb.SwarmDB, config *swarmdb.SWARMDBConfig) {
	fmt.Println("\nstarting http server")
	httpSvr := new(HTTPServer)
	httpSvr.swarmdb = sdb
	km, errkm := swarmdb.NewKeyManager(config)
	if errkm != nil {
		//return errkm
		retJson := buildErrorResp(errkm)
		fmt.Printf(retJson)
		//TODO: show error to client
	} else {
		httpSvr.keymanager = km
	}
	var allowedOrigins []string
	/*
	   for _, domain := range strings.Split(config.CorsString, ",") {
	*/
	allowedOrigins = append(allowedOrigins, "corsdomain")
	// }
	c := cors.New(cors.Options{
		AllowedOrigins: allowedOrigins,
		AllowedMethods: []string{"POST", "GET", "DELETE", "PATCH", "PUT"},
		MaxAge:         600,
		AllowedHeaders: []string{"*"},
	})
	//sk, pk := GetKeys()
	hdlr := c.Handler(httpSvr)

	fmt.Printf("\nRunning ListenAndServe")
	fmt.Printf("\nHTTP Listening on %s and port %d\n", config.ListenAddrHTTP, config.PortHTTP)
	addr := net.JoinHostPort(config.ListenAddrHTTP, strconv.Itoa(config.PortHTTP))
	//go http.ListenAndServe(config.Addr, hdlr)
	logger.Fatal(http.ListenAndServe(addr, hdlr))
}

func (s *HTTPServer) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Access-Control-Allow-Origin", "*")
	w.Header().Set("Access-Control-Allow-Methods", "POST, GET, OPTIONS, PUT, DELETE")
	w.Header().Set("Access-Control-Allow-Headers", "Accept, Content-Type, Content-Length, Accept-Encoding, X-CSRF-Token, Authorization")
	if r.Method == "OPTIONS" {
		return
	}

	var swErr swarmdb.SWARMDBError
	encAuthString := r.Header["Authorization"]
	var vUser *swarmdb.SWARMDBUser
	var errVerified error
	bodyContent, errReadBody := ioutil.ReadAll(r.Body)
	if errReadBody != nil {
		//TODO: Handle Reading Body error
		swErr.SetError(fmt.Sprintf("[wolkdb:ServeHTTP] Error Reading Request Body.[%s]", errReadBody.Error()))
		log.Error(swErr.Error())
		swErr.ErrorCode = 422
		swErr.ErrorMessage = fmt.Sprintf("Error Reading Request Body: [%s]", errReadBody.Error())
		retJson := buildErrorResp(&swErr)
		fmt.Fprint(w, retJson)
	}
	reqJson := bodyContent
	//fmt.Println("HTTP %s request URL: '%s', Host: '%s', Path: '%s', Referer: '%s', Accept: '%s'", r.Method, r.RequestURI, r.URL.Host, r.URL.Path, r.Referer(), r.Header.Get("Accept"))
	swReq, err := parsePath(r.URL.Path)
	//TODO: parsePath Error
	if err != nil {
		retJson := buildErrorResp(err)
		fmt.Fprint(w, retJson)
	}

	if len(encAuthString) == 0 {
		//TODO: remove "backdoor"
		us := []byte(`{ "requesttype":"Put", "row":{"email":"rodney@wolk.com", "name":"Rodney F. Witcher", "age":370} }`)
		msg := fmt.Sprintf("\x19Ethereum Signed Message:\n%d%s", len(us), us)
		msg_hash := crypto.Keccak256([]byte(msg))
		fmt.Printf("\nMessage Hash: [%s][%x]", msg_hash, msg_hash)

		pa, _ := s.keymanager.SignMessage(msg_hash)
		//TODO: SignMessageError

		fmt.Printf("\nUser: [%s], Msg Hash [%x], SignedMsg: [%x]\n", us, msg_hash, pa)
		vUser, errVerified = s.keymanager.VerifyMessage(msg_hash, pa)
		if errVerified != nil {
			//TODO: Show Error to Client
		}
	} else {
		bodyContentSeed := bodyContent
		if r.Method == "GET" {
			bodyContentSeed = []byte(fmt.Sprintf("%s%s%s", swReq.owner, swReq.table, swReq.key))
		}
		encAuthStringParts := strings.SplitN(encAuthString[0], " ", 2)
		decAuthString, err := base64.StdEncoding.DecodeString(encAuthStringParts[1])
		if err != nil {
			return
		}

		fmt.Printf("\nDecAuthString: [%x][%s]", decAuthString, decAuthString)
		decAuthStringParts := strings.SplitN(string(decAuthString), ":", 2)
		inputSignedMsg := decAuthStringParts[0]

		msg := fmt.Sprintf("\x19Ethereum Signed Message:\n%d%s", len(bodyContentSeed), bodyContentSeed)
		msg_hash := crypto.Keccak256([]byte(msg))
		fmt.Printf("\nMessage Hash: [%s][%x]", msg_hash, msg_hash)

		decSignedMsg, errDecSignedMsg := hex.DecodeString(inputSignedMsg)
		if errDecSignedMsg != nil {
			swErr.SetError(fmt.Sprintf("[wolkdb:ServeHTTP] Error Decoding Signed Message.[%s] %s", inputSignedMsg, errDecSignedMsg.Error()))
			log.Error(swErr.Error())
			swErr.ErrorCode = 422
			swErr.ErrorMessage = fmt.Sprintf("Error Decoding Signed Message", errDecSignedMsg.Error())
			retJson := buildErrorResp(&swErr)
			fmt.Fprintf(w, retJson)
		}
		//fmt.Printf("\nSignedMsg: [%x][%s] | DecSignedMsg: [%x][%s]", signedMsg, signedMsg, decSignedMsg, decSignedMsg)

		vUser, errVerified = s.keymanager.VerifyMessage(msg_hash, decSignedMsg)
		if errVerified != nil {
			fmt.Printf("\nError: %s", errVerified)
			retJson := buildErrorResp(errVerified)
			fmt.Fprintf(w, retJson)
		}
	}
	verifiedUser := vUser

	var dataReq swarmdb.RequestOption
	if swReq.protocol != "swarmdb:" {
		//Invalid Protocol: Throw Error
		//fmt.Fprintf(w, "The protocol sent in: %s is invalid | %+v\n", swReq.protocol, swReq)
	} else {
		var err error
		if r.Method == "GET" {
			//fmt.Fprintf(w, "Processing [%s] protocol request with Body of () \n", swReq.protocol)
			dataReq.RequestType = "Get"
			dataReq.TableOwner = swReq.owner
			dataReq.Table = swReq.table
			dataReq.Key = swReq.key
			reqJson, err = json.Marshal(dataReq)
			if err != nil {
				//TODO: Return Error to Client
				swErr.SetError(fmt.Sprintf("[wolkdb:ServeHTTP] Error Marshaling request, %s", err.Error()))
				log.Error(swErr.Error())
				swErr.ErrorCode = 424
				swErr.ErrorMessage = fmt.Sprintf("Error Reading Request", err.Error())
				retJson := buildErrorResp(&swErr)
				fmt.Fprint(w, retJson)
			}
		} else if r.Method == "POST" {
			//fmt.Printf("\nBODY Json: %s", reqJson)

			var bodyMapInt interface{}
			json.Unmarshal(bodyContent, &bodyMapInt)
			//fmt.Println("\nProcessing [%s] protocol request with Body of (%s) \n", swReq.protocol, bodyMapInt)
			bodyMap := bodyMapInt.(map[string]interface{})
			if reqType, ok := bodyMap["requesttype"]; ok {
				dataReq.RequestType = reqType.(string)
				if dataReq.RequestType == "CreateTable" {
					dataReq.TableOwner = verifiedUser.Address //bodyMap["tableowner"].(string);
					//TODO: ValidateCreateTableRequest
				} else if dataReq.RequestType == "Query" {
					dataReq.TableOwner = swReq.table
					//Don't pass table for now (rely on Query parsing)
					if rq, ok := bodyMap["rawquery"]; ok {
						dataReq.RawQuery = rq.(string)
						reqJson, err = json.Marshal(dataReq)
						if err != nil {
							swErr.SetError(fmt.Sprintf("[wolkdb:ServeHTTP] Error Marshaling request, %s", err.Error()))
							log.Error(swErr.Error())
							swErr.ErrorCode = 424
							swErr.ErrorMessage = fmt.Sprintf("Error Reading Request", err.Error())
							retJson := buildErrorResp(&swErr)
							fmt.Fprint(w, retJson)
						}
					} else {
						//Invalid Query Request: rawquery missing
						swErr.SetError(fmt.Sprintf("[wolkdb:ServeHTTP] Invalid Query Request.  Missing RawQuery"))
						log.Error(swErr.Error())
						swErr.ErrorCode = 425
						swErr.ErrorMessage = fmt.Sprintf("Invalid Query Request. Missing Rawquery")
						retJson := buildErrorResp(&swErr)
						fmt.Fprint(w, retJson)
					}
				} else if dataReq.RequestType == "Put" {
					dataReq.Table = swReq.table
					dataReq.TableOwner = swReq.owner
					if row, ok := bodyMap["row"]; ok {
						newRow := swarmdb.Row{Cells: row.(map[string]interface{})}
						dataReq.Rows = append(dataReq.Rows, newRow)
					}
					reqJson, err = json.Marshal(dataReq)
					if err != nil {
						//TODO: Return Error to Client
						swErr.SetError(fmt.Sprintf("[wolkdb:ServeHTTP] Error Marshaling request, %s", err.Error()))
						log.Error(swErr.Error())
						swErr.ErrorCode = 424
						swErr.ErrorMessage = fmt.Sprintf("Error Reading Request", err.Error())
						retJson := buildErrorResp(&swErr)
						fmt.Fprintf(w, retJson)
					}
				}
			} else {
				fmt.Fprintf(w, "\nPOST operations require a requestType, (%+v), (%s)", bodyMap, bodyMap["requesttype"])
			}
		}
		//Redirect to SelectHandler after "building" GET RequestOption
		//fmt.Printf("Sending this JSON to SelectHandler (%s) and Owner=[%s]", reqJson, keymanager.WOLKSWARMDB_ADDRESS)
		response, errResp := s.swarmdb.SelectHandler(verifiedUser, string(reqJson))
		if errResp != nil {
			retJson := buildErrorResp(errResp)
			fmt.Fprint(w, retJson)
		} else {
			fmt.Fprintf(w, response)
		}
	}
}

func main() {
	configFileLocation := flag.String("config", swarmdb.SWARMDBCONF_FILE, "Full path location to SWARMDB configuration file.")
	//TODO: store this somewhere accessible to be used later
	initFlag := flag.Bool("init", false, "Used to initialize a new SWARMDB")
	flag.Parse()
	fmt.Println("Launching HTTP server...")

	// start swarm http proxy server
	if *initFlag {
		fmt.Printf("Initializing a new SWARMDB")
	}
	fmt.Printf("Starting SWARMDB using [%s]", *configFileLocation)

	if _, err := os.Stat(*configFileLocation); os.IsNotExist(err) {
		log.Debug("Default config file missing.  Building ..")
		_, err := swarmdb.NewKeyManagerWithoutConfig(*configFileLocation, swarmdb.SWARMDBCONF_DEFAULT_PASSPHRASE)
		if err != nil {
			//TODO
		}
	}

	config, err := swarmdb.LoadSWARMDBConfig(*configFileLocation)
	if err != nil {
		fmt.Printf("\n The config file location provided [%s] is invalid.  Exiting ...\n", *configFileLocation)
		os.Exit(1)
	}

	log.Root().SetHandler(log.LvlFilterHandler(log.Lvl(4), log.StreamHandler(os.Stderr, log.TerminalFormat(false))))

	swdb, err := swarmdb.NewSwarmDB(config.ChunkDBPath, config.ChunkDBPath)
	if err != nil {
		panic(fmt.Sprintf("Cannot start: %s", err.Error()))
	}
	go StartHttpServer(swdb, &config)
	fmt.Println("\nHttpServer Started\n")

	fmt.Println("Launching TCPIP server...\n")
	StartTcpipServer(swdb, &config)
}
