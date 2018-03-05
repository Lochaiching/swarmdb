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
	//"sync"
	"runtime"
	//"github.com/ethereum/go-ethereum/swarmdb"
	"flag"
)

var tbl *swarmdblib.SWARMDBTable 

func main() {
	n := flag.Int("n", 1, "# of operations")
	flag.Parse()
	fmt.Printf("n: %d\n", *n)
	setTable()	
	testPut(*n)
	testGet()
	closeTable()
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

func closeTable() {
	err := tbl.CloseTable()
	if err != nil {
		fmt.Println("CloseTable err: ", err.Error())
		os.Exit(0)
	}
}
func setTable() {
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
	tbl, err = db.OpenTable(tableName)
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
}
	
func testPut(n int) {

	fmt.Println("\ntestPut---")
	
	cpus := runtime.NumCPU()
	runtime.GOMAXPROCS(cpus)

	start := makeTimestamp()
	for i:= 0; i < n; i++{
		str := RandStringRunes(7)
		//fmt.Println(str)
		email := str + "@gmail.com"
		contacts.info[i] = &Info{email, 8, "David Smith"}
		//contacts.info[i].age = 45
		rowToAdd := swarmdblib.Row{"email": email, "age": 8, "name": "David Smith"}
		err := tbl.Put(rowToAdd)
		if err != nil {
			fmt.Println("Put err: ", err.Error())
			os.Exit(0)
		}
	}
	end := makeTimestamp()
	fmt.Printf("TestPut start: %d end: %d total: %d Millisecond\n", start, end, end-start)
	//fmt.Println(contacts)
	//fmt.Println(contacts.info[1])

	for k, info := range contacts.info {
        fmt.Printf("[%d] email: %s ade: %d name: %s\n", k, info.email, info.age, info.name)
    }
	fmt.Println("testPut---\n")
}

func testGet() {
	
	fmt.Println("\ntestGet---")
	
	cpus := runtime.NumCPU()
	runtime.GOMAXPROCS(cpus)

	start := makeTimestamp()
	failure := 0	
	for _, info := range contacts.info {
		//fmt.Printf("[%d] email: %s ade: %d name: %s\n", k, info.email, info.age, info.name)
		primaryKey := info.email
		retrievedRow, err := tbl.Get(primaryKey)
		if err != nil {
			fmt.Println("Get err: ", err.Error())
			os.Exit(0)
		}
		fmt.Println("Get: ", retrievedRow)
		
		
		//fmt.Println("info.email: ", primaryKey)
		retrievedRowPrimaryKey := retrievedRow[0]["email"]
		//fmt.Println("retrievedRow[0]['email']: ", retrievedRowPrimaryKey)
		
		if(primaryKey != retrievedRowPrimaryKey) {
			failure = failure + 1
		}
		
	}
	end := makeTimestamp()
	fmt.Printf("TestGet start: %d end: %d total: %d Millisecond    Failure: %d\n", start, end, end-start, failure)
	fmt.Println("testGet---")	 
}


/*


[root@yaron-swarm-01 swarmdbbench]# go run swarmdbbench_n.go -n 100
n: 100
setTable

testPut---
TestPut start: 1520221205772 end: 1520223970155 total: 2764383 Millisecond
[12] email: HCLdvpX@gmail.com ade: 8 name: David Smith
[16] email: yHpRurt@gmail.com ade: 8 name: David Smith
[30] email: FgBirzH@gmail.com ade: 8 name: David Smith
[45] email: yDTyrSr@gmail.com ade: 8 name: David Smith
[46] email: FKRLQDI@gmail.com ade: 8 name: David Smith
[74] email: mgMjgZI@gmail.com ade: 8 name: David Smith
[99] email: GUoykop@gmail.com ade: 8 name: David Smith
[44] email: UcYcHne@gmail.com ade: 8 name: David Smith
[71] email: EPYagvD@gmail.com ade: 8 name: David Smith
[84] email: nWktOla@gmail.com ade: 8 name: David Smith
[83] email: tpYrfCZ@gmail.com ade: 8 name: David Smith
[24] email: wLrTacq@gmail.com ade: 8 name: David Smith
[35] email: GYfpDPf@gmail.com ade: 8 name: David Smith
[42] email: tJRNWes@gmail.com ade: 8 name: David Smith
[49] email: cJDPkim@gmail.com ade: 8 name: David Smith
[55] email: daMftlz@gmail.com ade: 8 name: David Smith
[76] email: uemrnSP@gmail.com ade: 8 name: David Smith
[78] email: QPnbvqt@gmail.com ade: 8 name: David Smith
[91] email: LNjZqtl@gmail.com ade: 8 name: David Smith
[95] email: THHlYMY@gmail.com ade: 8 name: David Smith
[96] email: KbdUUwJ@gmail.com ade: 8 name: David Smith
[0] email: zbJIeCx@gmail.com ade: 8 name: David Smith
[28] email: kJKlHIB@gmail.com ade: 8 name: David Smith
[40] email: HtBOfVw@gmail.com ade: 8 name: David Smith
[70] email: MbIgIly@gmail.com ade: 8 name: David Smith
[1] email: GJSnkOt@gmail.com ade: 8 name: David Smith
[8] email: OWoCHRl@gmail.com ade: 8 name: David Smith
[31] email: YmroMNs@gmail.com ade: 8 name: David Smith
[77] email: DLpOVcR@gmail.com ade: 8 name: David Smith
[90] email: iQoXewU@gmail.com ade: 8 name: David Smith
[4] email: gQuVvKC@gmail.com ade: 8 name: David Smith
[52] email: PplNOwI@gmail.com ade: 8 name: David Smith
[65] email: xFfsMKQ@gmail.com ade: 8 name: David Smith
[93] email: WyAVjwb@gmail.com ade: 8 name: David Smith
[75] email: mczQzdh@gmail.com ade: 8 name: David Smith
[9] email: ztksfJa@gmail.com ade: 8 name: David Smith
[13] email: XrcIMuy@gmail.com ade: 8 name: David Smith
[14] email: pzdScMr@gmail.com ade: 8 name: David Smith
[19] email: kmaKGbc@gmail.com ade: 8 name: David Smith
[20] email: grKzUtT@gmail.com ade: 8 name: David Smith
[23] email: MPExDJa@gmail.com ade: 8 name: David Smith
[47] email: rKtaYZm@gmail.com ade: 8 name: David Smith
[82] email: mwlAjco@gmail.com ade: 8 name: David Smith
[89] email: bIlKFGI@gmail.com ade: 8 name: David Smith
[73] email: uoiUEGU@gmail.com ade: 8 name: David Smith
[5] email: XbcqoyC@gmail.com ade: 8 name: David Smith
[38] email: udWUtJE@gmail.com ade: 8 name: David Smith
[51] email: bFILLpB@gmail.com ade: 8 name: David Smith
[54] email: FEklRhD@gmail.com ade: 8 name: David Smith
[60] email: UQqHLrw@gmail.com ade: 8 name: David Smith
[62] email: qqVzTdM@gmail.com ade: 8 name: David Smith
[72] email: kHuiIQh@gmail.com ade: 8 name: David Smith
[22] email: iGKMjex@gmail.com ade: 8 name: David Smith
[59] email: SZFMana@gmail.com ade: 8 name: David Smith
[69] email: hNzAFMK@gmail.com ade: 8 name: David Smith
[79] email: zZmdMTC@gmail.com ade: 8 name: David Smith
[85] email: mOaClOS@gmail.com ade: 8 name: David Smith
[88] email: xypEfQz@gmail.com ade: 8 name: David Smith
[36] email: bEALHvD@gmail.com ade: 8 name: David Smith
[39] email: VHetzjd@gmail.com ade: 8 name: David Smith
[50] email: ekwrcxM@gmail.com ade: 8 name: David Smith
[61] email: lboZJhw@gmail.com ade: 8 name: David Smith
[81] email: UagMgqF@gmail.com ade: 8 name: David Smith
[92] email: fgIRnhv@gmail.com ade: 8 name: David Smith
[7] email: pVHkFsc@gmail.com ade: 8 name: David Smith
[10] email: qjljURM@gmail.com ade: 8 name: David Smith
[33] email: apyHjRr@gmail.com ade: 8 name: David Smith
[41] email: yXgxssA@gmail.com ade: 8 name: David Smith
[43] email: KdtgUes@gmail.com ade: 8 name: David Smith
[48] email: ldeUegA@gmail.com ade: 8 name: David Smith
[3] email: GWEIelt@gmail.com ade: 8 name: David Smith
[17] email: ymBlAiO@gmail.com ade: 8 name: David Smith
[21] email: oonIGFM@gmail.com ade: 8 name: David Smith
[57] email: AJYJTgA@gmail.com ade: 8 name: David Smith
[63] email: VADQJXc@gmail.com ade: 8 name: David Smith
[11] email: ezXVzxH@gmail.com ade: 8 name: David Smith
[34] email: pSNYBtR@gmail.com ade: 8 name: David Smith
[56] email: FfxRknF@gmail.com ade: 8 name: David Smith
[64] email: oNYZwQU@gmail.com ade: 8 name: David Smith
[67] email: TqGFzMl@gmail.com ade: 8 name: David Smith
[98] email: sxHkkVD@gmail.com ade: 8 name: David Smith
[94] email: nlUaYZY@gmail.com ade: 8 name: David Smith
[6] email: mUPrNhQ@gmail.com ade: 8 name: David Smith
[25] email: NcDLKbS@gmail.com ade: 8 name: David Smith
[26] email: sIimoXf@gmail.com ade: 8 name: David Smith
[58] email: aGHoNgu@gmail.com ade: 8 name: David Smith
[68] email: xyKZQaE@gmail.com ade: 8 name: David Smith
[86] email: Xdefdzo@gmail.com ade: 8 name: David Smith
[87] email: wojxBML@gmail.com ade: 8 name: David Smith
[97] email: lPefzMU@gmail.com ade: 8 name: David Smith
[18] email: bDOVZbn@gmail.com ade: 8 name: David Smith
[27] email: BlmeFOb@gmail.com ade: 8 name: David Smith
[53] email: dPtRKVZ@gmail.com ade: 8 name: David Smith
[66] email: MhyOTNe@gmail.com ade: 8 name: David Smith
[2] email: fmuyPsb@gmail.com ade: 8 name: David Smith
[15] email: wbZfZmS@gmail.com ade: 8 name: David Smith
[29] email: EiofrVI@gmail.com ade: 8 name: David Smith
[32] email: YHegjMz@gmail.com ade: 8 name: David Smith
[37] email: eCdVYkQ@gmail.com ade: 8 name: David Smith
[80] email: TQLKorF@gmail.com ade: 8 name: David Smith
testPut---


testGet---
Get:  [map[age:8 email:pVHkFsc@gmail.com name:David Smith]]
Get:  [map[age:8 email:qjljURM@gmail.com name:David Smith]]
Get:  [map[age:8 email:apyHjRr@gmail.com name:David Smith]]
Get:  [map[age:8 email:yXgxssA@gmail.com name:David Smith]]
Get:  [map[age:8 email:KdtgUes@gmail.com name:David Smith]]
Get:  [map[age:8 email:ldeUegA@gmail.com name:David Smith]]
Get:  [map[name:David Smith age:8 email:GWEIelt@gmail.com]]
Get:  [map[age:8 email:ymBlAiO@gmail.com name:David Smith]]
Get:  [map[name:David Smith age:8 email:oonIGFM@gmail.com]]
Get:  [map[age:8 email:AJYJTgA@gmail.com name:David Smith]]
Get:  [map[age:8 email:VADQJXc@gmail.com name:David Smith]]
Get:  [map[email:ezXVzxH@gmail.com name:David Smith age:8]]
Get:  [map[age:8 email:pSNYBtR@gmail.com name:David Smith]]
Get:  [map[age:8 email:FfxRknF@gmail.com name:David Smith]]
Get:  [map[email:oNYZwQU@gmail.com name:David Smith age:8]]
Get:  [map[age:8 email:TqGFzMl@gmail.com name:David Smith]]
Get:  [map[age:8 email:sxHkkVD@gmail.com name:David Smith]]
Get:  [map[age:8 email:nlUaYZY@gmail.com name:David Smith]]
Get:  [map[name:David Smith age:8 email:mUPrNhQ@gmail.com]]
Get:  [map[name:David Smith age:8 email:NcDLKbS@gmail.com]]
Get:  [map[name:David Smith age:8 email:sIimoXf@gmail.com]]
Get:  [map[email:aGHoNgu@gmail.com name:David Smith age:8]]
Get:  [map[email:xyKZQaE@gmail.com name:David Smith age:8]]
Get:  [map[age:8 email:Xdefdzo@gmail.com name:David Smith]]
Get:  [map[age:8 email:wojxBML@gmail.com name:David Smith]]
Get:  [map[age:8 email:lPefzMU@gmail.com name:David Smith]]
Get:  [map[age:8 email:bDOVZbn@gmail.com name:David Smith]]
Get:  [map[name:David Smith age:8 email:BlmeFOb@gmail.com]]
Get:  [map[age:8 email:dPtRKVZ@gmail.com name:David Smith]]
Get:  [map[age:8 email:MhyOTNe@gmail.com name:David Smith]]
Get:  [map[name:David Smith age:8 email:fmuyPsb@gmail.com]]
Get:  [map[age:8 email:wbZfZmS@gmail.com name:David Smith]]
Get:  [map[email:EiofrVI@gmail.com name:David Smith age:8]]
Get:  [map[name:David Smith age:8 email:YHegjMz@gmail.com]]
Get:  [map[age:8 email:eCdVYkQ@gmail.com name:David Smith]]
Get:  [map[age:8 email:TQLKorF@gmail.com name:David Smith]]
Get:  [map[age:8 email:HCLdvpX@gmail.com name:David Smith]]
Get:  [map[age:8 email:yHpRurt@gmail.com name:David Smith]]
Get:  [map[email:FgBirzH@gmail.com name:David Smith age:8]]
Get:  [map[age:8 email:yDTyrSr@gmail.com name:David Smith]]
Get:  [map[age:8 email:FKRLQDI@gmail.com name:David Smith]]
Get:  [map[age:8 email:mgMjgZI@gmail.com name:David Smith]]
Get:  [map[age:8 email:GUoykop@gmail.com name:David Smith]]
Get:  [map[email:UcYcHne@gmail.com name:David Smith age:8]]
Get:  [map[age:8 email:EPYagvD@gmail.com name:David Smith]]
Get:  [map[age:8 email:nWktOla@gmail.com name:David Smith]]
Get:  [map[age:8 email:tpYrfCZ@gmail.com name:David Smith]]
Get:  [map[email:wLrTacq@gmail.com name:David Smith age:8]]
Get:  [map[name:David Smith age:8 email:GYfpDPf@gmail.com]]
Get:  [map[age:8 email:tJRNWes@gmail.com name:David Smith]]
Get:  [map[age:8 email:cJDPkim@gmail.com name:David Smith]]
Get:  [map[age:8 email:daMftlz@gmail.com name:David Smith]]
Get:  [map[age:8 email:uemrnSP@gmail.com name:David Smith]]
Get:  [map[age:8 email:QPnbvqt@gmail.com name:David Smith]]
Get:  [map[age:8 email:LNjZqtl@gmail.com name:David Smith]]
Get:  [map[age:8 email:THHlYMY@gmail.com name:David Smith]]
Get:  [map[age:8 email:KbdUUwJ@gmail.com name:David Smith]]
Get:  [map[age:8 email:zbJIeCx@gmail.com name:David Smith]]
Get:  [map[age:8 email:kJKlHIB@gmail.com name:David Smith]]
Get:  [map[age:8 email:HtBOfVw@gmail.com name:David Smith]]
Get:  [map[email:MbIgIly@gmail.com name:David Smith age:8]]
Get:  [map[age:8 email:GJSnkOt@gmail.com name:David Smith]]
Get:  [map[name:David Smith age:8 email:OWoCHRl@gmail.com]]
Get:  [map[name:David Smith age:8 email:YmroMNs@gmail.com]]
Get:  [map[age:8 email:DLpOVcR@gmail.com name:David Smith]]
Get:  [map[email:iQoXewU@gmail.com name:David Smith age:8]]
Get:  [map[email:gQuVvKC@gmail.com name:David Smith age:8]]
Get:  [map[age:8 email:PplNOwI@gmail.com name:David Smith]]
Get:  [map[name:David Smith age:8 email:xFfsMKQ@gmail.com]]
Get:  [map[age:8 email:WyAVjwb@gmail.com name:David Smith]]
Get:  [map[name:David Smith age:8 email:mczQzdh@gmail.com]]
Get:  [map[age:8 email:ztksfJa@gmail.com name:David Smith]]
Get:  [map[name:David Smith age:8 email:XrcIMuy@gmail.com]]
Get:  [map[age:8 email:pzdScMr@gmail.com name:David Smith]]
Get:  [map[age:8 email:kmaKGbc@gmail.com name:David Smith]]
Get:  [map[age:8 email:grKzUtT@gmail.com name:David Smith]]
Get:  [map[age:8 email:MPExDJa@gmail.com name:David Smith]]
Get:  [map[age:8 email:rKtaYZm@gmail.com name:David Smith]]
Get:  [map[name:David Smith age:8 email:mwlAjco@gmail.com]]
Get:  [map[name:David Smith age:8 email:bIlKFGI@gmail.com]]
Get:  [map[age:8 email:uoiUEGU@gmail.com name:David Smith]]
Get:  [map[age:8 email:XbcqoyC@gmail.com name:David Smith]]
Get:  [map[name:David Smith age:8 email:udWUtJE@gmail.com]]
Get:  [map[age:8 email:bFILLpB@gmail.com name:David Smith]]
Get:  [map[age:8 email:FEklRhD@gmail.com name:David Smith]]
Get:  [map[age:8 email:UQqHLrw@gmail.com name:David Smith]]
Get:  [map[age:8 email:qqVzTdM@gmail.com name:David Smith]]
Get:  [map[age:8 email:kHuiIQh@gmail.com name:David Smith]]
Get:  [map[age:8 email:iGKMjex@gmail.com name:David Smith]]
Get:  [map[age:8 email:SZFMana@gmail.com name:David Smith]]
Get:  [map[age:8 email:hNzAFMK@gmail.com name:David Smith]]
Get:  [map[age:8 email:zZmdMTC@gmail.com name:David Smith]]
Get:  [map[name:David Smith age:8 email:mOaClOS@gmail.com]]
Get:  [map[age:8 email:xypEfQz@gmail.com name:David Smith]]
Get:  [map[age:8 email:bEALHvD@gmail.com name:David Smith]]
Get:  [map[age:8 email:VHetzjd@gmail.com name:David Smith]]
Get:  [map[age:8 email:ekwrcxM@gmail.com name:David Smith]]
Get:  [map[age:8 email:lboZJhw@gmail.com name:David Smith]]
Get:  [map[age:8 email:UagMgqF@gmail.com name:David Smith]]
Get:  [map[age:8 email:fgIRnhv@gmail.com name:David Smith]]
TestGet start: 1520223970156 end: 1520223970258 total: 102 Millisecond    Failure: 0
testGet---

END
[root@yaron-swarm-01 swarmdbbench]#



*/


 




 













