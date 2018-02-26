package main

import (
  "fmt"
)
// test

func main() {

     fmt.Printf("Hi\n")
     sum := 0
     for i := 0; i < 10; i++ {
       sum = sum + i
     }
     fmt.Printf("%d\n", sum)

}
