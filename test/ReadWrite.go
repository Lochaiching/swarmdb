package main

import (
    "bytes"
    "encoding/json"    
    "errors"
    "fmt"
    "io/ioutil"
    "net/http"
    "os"
)

/*

[root@www5009 test]# ./ReadWrite write '{"requestID":"04773d0de7e07c749a1f9cca75303111"}'
1a3234225869abd5c154711527687131cce161f524ec31beeb8893a0b2c4fdc0
[root@www5009 test]# ./ReadWrite read 1a3234225869abd5c154711527687131cce161f524ec31beeb8893a0b2c4fdc0
{"requestID":"04773d0de7e07c749a1f9cca75303111"}
[root@www5009 test]#


*/

const (
    //host = "localhost" 
    host = "50.225.47.159"      
    port = 8500
)

func main() {

    primaryArg, secondaryArg, err := validateArgs(os.Args)
    if err != nil {
        fmt.Println("an error occurred: " + err.Error())
        os.Exit(1)
    }
    
    switch primaryArg {
    case "read" :
        value, err := Read(secondaryArg)
        if err != nil {
            fmt.Println("an error occurred: " + err.Error())
        }    
        fmt.Println(value)
    case "write":
        key, err := Write(secondaryArg)
        if err != nil {
            fmt.Println("an error occurred: " + err.Error())
        }        
        fmt.Println(key)        
    }
}

// takes a hash key as an input
// attempts to read file from swarm and return a json string

func Read(key string) (string, error) {
    
    url := endpointWithKey(key)
            
    response, err := http.Get(url)
    if err != nil {
        return "", errors.New("swarm read error" + err.Error())
    }
    
    defer response.Body.Close()
    
    body, err := ioutil.ReadAll(response.Body)
    if err != nil {
        return "", errors.New("read response error" + err.Error())
    }

    return string(body), nil
}

// takes a json string as an input
// attempts to store file in swarm and return a hash key

func Write(json string) (string, error) {
        
    response, err := http.Post(endpoint(), "text/plain", bytes.NewBuffer([]byte(json)))
    if err != nil {
        return "", errors.New("swarm write error: " + err.Error())
    }
    
    defer response.Body.Close()
    
    body, err := ioutil.ReadAll(response.Body)
    if err != nil {
        return "", errors.New("write response error" + err.Error())
    }

    return string(body), nil
}

// takes all args sent to program via command line
// lightly validates args and attempts to return primary and secondary args

func validateArgs(args []string) (string, string, error) {
    
    if len(args) != 3 {
        return "", "", fmt.Errorf("invalid number of args. two are required");
    }
    
    primaryArg := args[1];
    if !(primaryArg == "write" || primaryArg == "read") {
        return "", "", errors.New("invalid primary arg.  must be read or write.")
    }
    
    secondaryArg := args[2]
    if secondaryArg == "" {
        return "", "", errors.New("invalid secondary arg.  if reading, must be key.  if writing, must be json string.")
    }
    
    if primaryArg == "write" && !isJson(secondaryArg) {
        return "", "", errors.New("invalid json in secondary arg")
    }
    
    return primaryArg, secondaryArg, nil
}

func isJson(value string) bool {
   var j json.RawMessage
   return json.Unmarshal([]byte(value), &j) == nil
}

func endpoint() string {
    return "http://" + host + ":" + fmt.Sprintf("%v", port) + "/bzz:/"
}

func endpointWithKey(key string) string {
    return endpoint() + key + "/"
}