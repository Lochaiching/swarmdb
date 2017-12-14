package main

/*
#include "./App/TEE.h"
#cgo LDFLAGS: -I./App -L. -ltee 
*/
import "C"

import (
	"fmt"
	//"unsafe"
)

//export test
func test() {
    fmt.Printf("intel sgx hello\n")
}

func main() {

    cstr := C.CString("TestThisSGX")
    // defer C.free(unsafe.Pointer(cstr))
    cString := C.getSha256(cstr)          // hash: 18497686A320B7DA753F7E18C58C4F2E18089D5816FDE68858878D22C8237E36
    gostr := C.GoString(cString)
    fmt.Println("Received hash (string) from C: " + gostr)

    //test()
}

 