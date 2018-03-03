package main

import (
	"github.com/wolkdb/swarmdblib"
	"fmt"
	//"testing"
	//"text/template"
	//"bytes"
	"os"
	"math/rand"
	"time"
	"sync"
	"runtime"
	//"github.com/ethereum/go-ethereum/swarmdb"
	"flag"
)

//var tbl *swarmdblib.SWARMDBTable 

func main() {
	n := flag.Int("n", 1, "# of operations")
	c := flag.Int("c", 1, "# of concurrent operations")
	t := flag.Int("t", 1, "# of concurrent tbl")		
	flag.Parse()
	fmt.Printf("n: %d  c: %d  t: %d\n", *n, *c, *t)
	//setTable()	
	testPutT(*n, *c, *t)
	//testGet(*c)
	//closeTable() 
	fmt.Println("\nEND")	
}

func makeTimestamp() int64 {
    return time.Now().UnixNano() / int64(time.Millisecond)
}

func make_name(prefix string) (nm string) {
	return fmt.Sprintf("%s%d", prefix, int32(time.Now().Unix()))
}

// Rand String
func init() {
    rand.Seed(time.Now().UnixNano())
}

var letterRunes = []rune("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ")

func RandStringRunes(n int) string {
    b := make([]rune, n)
    for i := range b {
        b[i] = letterRunes[rand.Intn(len(letterRunes))]
    }
    return string(b)
}
// Rand String


type Info struct {
	 email string
	 age int
	 name string
}

type Contacts struct {
	info map[int]*Info 
}

var contacts = &Contacts{
	info:  make(map[int]*Info),
}

/*
func closeTable() {
	err := tbl.CloseTable()
	if err != nil {
		fmt.Println("CloseTable err: ", err.Error())
		os.Exit(0)
	}
}
*/
func setTable() *swarmdblib.SWARMDBTable {
	host := "localhost" //your SWARMDB node IP
	port := int(2001) //your SWARMDB node Port number
	owner := "test.eth" //your SWARMDB node owner address
	//y privateKey := "d4b6a877f7c45c85604778a8a0fced742f09a888bbfc5961e3dc8875a7f3f338" //your SWARMDB node private key
	//privateKey := "ac132267ca093a8fab54fe8b7a4ead146385409f74f70972d998f4ae0e73e489" //your SWARMDB node private key
	privateKey := "c4855bb2ec20a757134a13aba286576f5b72ac0c4ded1b8c6a8494014aff00c6" //your SWARMDB node private key
	
	conn, err := swarmdblib.NewSWARMDBConnection(host, port, owner, privateKey)
	if err != nil {
		fmt.Println("NewSWARMDBConnection err: ", err.Error())
		os.Exit(0)
	}
	
	databaseName := "swarmdbbench"
/*	dblist, err := conn.ListDatabases()
	if err != nil {
		fmt.Println("ListDatabases err: ", err.Error())
		os.Exit(0)
	}

	for _, v := range dblist {
		if(databaseName == v["database"]) { 
			err := conn.DropDatabase(databaseName)
			if err != nil {
				fmt.Println("DropDatabase err: ", err.Error())
				os.Exit(0)
			}
		}
	}

	encrypted := int(1)
	db, err := conn.CreateDatabase(databaseName, encrypted)
	if err != nil {
		fmt.Println("CreateDatabase err: ", err.Error())
		os.Exit(0)
	}
*/
	db, err := conn.OpenDatabase(databaseName)
	if err != nil {
		fmt.Println("OpenDatabase err: ", err.Error())
		os.Exit(0)
	}

	tableName := "contacts"
/*	columns :=
		[]swarmdblib.Column{
			swarmdblib.Column{
				ColumnName: "email",
				ColumnType: swarmdblib.CT_STRING,
				IndexType: swarmdblib.IT_BPLUSTREE,
				Primary: 1,
			},
			swarmdblib.Column{
				ColumnName: "age",
				ColumnType: swarmdblib.CT_INTEGER,
				IndexType: swarmdblib.IT_BPLUSTREE,
				Primary: 0,
			},
			swarmdblib.Column{
				ColumnName: "name",
				ColumnType: swarmdblib.CT_STRING,
				IndexType: swarmdblib.IT_BPLUSTREE,
				Primary: 0,
			},
		}

	tbl, err := db.CreateTable(tableName, columns)
	if err != nil {
		fmt.Println("CreateTable err: ", err.Error())
		os.Exit(0)
	}
*/	
	tbl, err := db.OpenTable(tableName)
	if err != nil {
		fmt.Println("OpenTable err: ", err.Error())
		os.Exit(0)
	}
/*
	rowToAdd := swarmdblib.Row{"email": "david@gmail.com", "age": 8, "name": "David Smith"}
	err = tbl.Put(rowToAdd)
	if err != nil {
		fmt.Println("Put err: ", err.Error())
		os.Exit(0)
	}

	primaryKey := "david@gmail.com";
	retrievedRow, err := tbl.Get(primaryKey)
	if err != nil {
		fmt.Println("Get err: ", err.Error())
		os.Exit(0)
	}
	fmt.Println("Get: ", retrievedRow)
*/
	fmt.Println("setTable")
	return tbl
}
	
func testPutT(n int, c int, t int) {

	fmt.Println("\ntestPutT---")
  
	tbl := make([]*swarmdblib.SWARMDBTable, t)

	for z:= 0; z < t; z++{
		tbl[z] = setTable()
		go func(n int, c int, z int, tbl *swarmdblib.SWARMDBTable){
			testPut(n, c, z, tbl)
		}(n, c, z, tbl[z])	 
	}
	var input string
	fmt.Scanln(&input)	
	
	fmt.Println("\ntestGeT---")
	
	for z:= 0; z < t; z++{
		tbl[z] = setTable()
		go func(n int, c int, z int, tbl *swarmdblib.SWARMDBTable){
			testGet(n, c, z, tbl)
		}(n, c, z, tbl[z])	 
	}
	var input2 string
	fmt.Scanln(&input2)	
	
	for z:= 0; z < t; z++{	
		err := tbl[z].CloseTable()
		if err != nil {
			fmt.Println("CloseTable err: ", err.Error())
			os.Exit(0)
		}
	}
		
	
/*	
	tbl := setTable()
	//go testPut(n, c, tbl)
	var wg sync.WaitGroup
	go func(){ 
		defer wg.Done()
		testPut(n, c, tbl)
		time.Sleep(30 * time.Second)
	}()
	wg.Wait()
	//time.Sleep(30 * time.Second)
	var input string
fmt.Scanln(&input)
*/
}

func testPut(n int, c int, z int, tbl *swarmdblib.SWARMDBTable) {

	fmt.Println("\ntestPut---")
	
	cpus := runtime.NumCPU()
	runtime.GOMAXPROCS(cpus)

	var wg sync.WaitGroup
	m :=new(sync.Mutex)
	semaphore := make(chan int, c)

	start := makeTimestamp()
	for i:= 0; i < n; i++{
		semaphore<-1
		wg.Add(1)
		go func(f int, z int,tbl *swarmdblib.SWARMDBTable){
			defer wg.Done()
			p := z*10 + f
			fmt.Println("chan in: ", p)			
			str := RandStringRunes(7)
			//fmt.Println(str)
			email := str + "@gmail.com"
			contacts.info[p] = &Info{email, 8, "David Smith"}
			//contacts.info[i].age = 45
			rowToAdd := swarmdblib.Row{"email": email, "age": 8, "name": "David Smith"}
			m.Lock()
			err := tbl.Put(rowToAdd)
			m.Unlock()
			if err != nil {
				fmt.Println("Put err: ", err.Error())
				os.Exit(0)
			}
			fmt.Println("chan out:   ", p)
			<-semaphore	
		}(i, z, tbl)	
	}	
	wg.Wait()
	end := makeTimestamp()
	fmt.Printf("TestPut start: %d end: %d total: %d Millisecond\n", start, end, end-start)
	//fmt.Println(contacts)
	//fmt.Println(contacts.info[1])

	for k, info := range contacts.info {
        fmt.Printf("[%d] email: %s ade: %d name: %s\n", k, info.email, info.age, info.name)
    }
	fmt.Println("testPut---\n")
}

func testGet(n int, c int, z int, tbl *swarmdblib.SWARMDBTable) {
	
	fmt.Println("\ntestGet---")
	
	cpus := runtime.NumCPU()
	runtime.GOMAXPROCS(cpus)

	var wg sync.WaitGroup
	m :=new(sync.Mutex)
	semaphore := make(chan int, c)

	start := makeTimestamp()	
	for k, info := range contacts.info {
		semaphore<-1
		wg.Add(1)
		go func(f int, email string){
			defer wg.Done()
			fmt.Println("chan in: ", f)
			//fmt.Printf("[%d] email: %s ade: %d name: %s\n", k, info.email, info.age, info.name)
			primaryKey := email
			m.Lock()
			retrievedRow, err := tbl.Get(primaryKey)
			m.Unlock()
			if err != nil {
				fmt.Println("Get err: ", err.Error())
				os.Exit(0)
			}
			fmt.Println("Get: ", retrievedRow)
			fmt.Println("chan out:   ", f)
			<-semaphore	
		}(k, info.email)
	}
	wg.Wait()
	end := makeTimestamp()
	fmt.Printf("TestGet start: %d end: %d total: %d Millisecond\n", start, end, end-start)
	fmt.Println("testGet---")	 
}

 




 













