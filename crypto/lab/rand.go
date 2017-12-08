package main

/*
#include <stdlib.h>
*/
import "C"

import "fmt"

func Random() int {
    return int(C.random())
}

func Seed(i int) {
    C.srandom(C.uint(i))
}

func main() {

	fmt.Println("Hello\n")
	
	Seed(11)
	var n = Random()
	
	fmt.Printf("rand: %d\n", n)
	
}

