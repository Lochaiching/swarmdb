// testing 
// I was testing (yaron)
// 



package main

import (
	
    "fmt"
	
)

//[KEY][0 19 242 30 50 241 235 141 200 50 61 101 134 92 142 60 177 45 93 120 145 144 189 217 127 102 4 162 0 57 1 186 51]
    
func main() {
	
    var ba1  [33]byte
    var ba2  []byte
    var ba3  [32]byte
        
            
    ba1 = [33]byte{0, 19, 242, 30, 50, 241, 235, 141, 200, 50, 61, 101, 134, 92, 142, 60, 177, 45, 93, 120, 145, 144, 189, 217, 127, 102, 4, 162, 0, 57, 1, 186, 51}
    fmt.Printf("ba1 byte array :%v\n\n", ba1)
    //str := int(ba[1:32])
    //fmt.Printf("str :%v\n\n", str)





	ba2 = append(ba1[:0], ba1[1:]...)
    fmt.Printf("ba2 byte array :%v\n%x\n", ba2, ba2)

	//secretkey := [32]byte{}
	
	var str string
	
	for i := range ba2 {
		ba3[i] = ba2[i]
		//fmt.Printf("ba3 byte array :%d %v\n\n", i, ba3[i])
		
		str = str +  fmt.Sprintf("%d",ba3[i])
		
		if i == 31 {
			break
		}
	}


    fmt.Printf("str :%s\n\n", str)
}





















