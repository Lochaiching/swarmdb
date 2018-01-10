package swarmdb

import (
	"bytes"
	"crypto/sha256"
	"database/sql"
	//"encoding/binary"
	"encoding/json"
	"fmt"
	"github.com/ethereum/go-ethereum/common"
	_ "github.com/mattn/go-sqlite3"
	"io/ioutil"
	"math/big"
	"os"
	"time"
)

var (
	netCounter NetstatFile
)

const (
	minChunkSize = 4000
)

type DBChunk struct {
	Key          []byte // 32
	Val          []byte // 4096
	Owner        []byte // 42
	TableName    []byte // 32
	TableId      []byte // 32
	ChunkBirthDT int64
	ChunkStoreDT int64
}

type ChunkLog struct {
	Farmer           string `json:"farmer"`
	ChunkID          string `json:"chunkID"`
	ChunkHash        []byte `json:"-"`
	ChunkBD          int    `json:"chunkBD"`
	ChunkSD          int    `json:"chunkSD"`
	ReplicationLevel int    `json:"rep"`
	Renewable        int    `json:"renewable"`
	//Claimable        int
}

type ChunkStats struct {
	CurrentTS   int64 `json:"CurrentTS"`
	ChunkRead   int64 `json:"ChunkRead"`
	ChunkWrite  int64 `json:"ChunkWrite"`
	ChunkStored int64 `json:"ChunkStored"`
}

func (self *DBChunkstore) MarshalJSON() ([]byte, error) {
	logDT := time.Now()
	self.netstat.CStat["ChunkRL"].Add(self.netstat.CStat["ChunkR"], self.netstat.CStat["ChunkRL"])
	self.netstat.CStat["ChunkWL"].Add(self.netstat.CStat["ChunkW"], self.netstat.CStat["ChunkWL"])

	err := self.GetChunkStored()
	if err != nil {
		fmt.Printf("ChunkStore parsing error\n")
	}

	fileInfo, err := os.Stat(self.filepath)
	if err == nil {
		deltaBS := new(big.Int).SetInt64(fileInfo.Size())
		self.netstat.BStat["ByteS"].Sub(deltaBS, self.netstat.BStat["ByteSL"])
		self.netstat.BStat["ByteSL"] = deltaBS
	}

	var file = &NetstatFile{
		NodeID:        self.netstat.NodeID,
		WalletAddress: self.farmer.Hex(),
		Ticket:        make(map[string]string),
		ChunkStat:     make(map[string]string),
		ByteStat:      make(map[string]string),
		LaunchDT:      self.netstat.LaunchDT,
		LReadDT:       self.netstat.LReadDT,
		LWriteDT:      self.netstat.LWriteDT,
		LogDT:         &logDT,
	}

	for cc, cv := range self.netstat.CStat {
		file.ChunkStat[cc] = cv.String()
		if cc == "ChunkR" || cc == "ChunkW" || cc == "ChunkS" {
			self.netstat.CStat[cc] = big.NewInt(0)
		}
	}

	for bc, bv := range self.netstat.BStat {
		file.ByteStat[bc] = bv.String()
		if bc == "ByteR" || bc == "ByteS" || bc == "ByteW" {
			self.netstat.BStat[bc] = big.NewInt(0)
		}
	}

	for ticket, reward := range self.netstat.Claim {
		file.Ticket[ticket] = reward.String()
	}

	return json.Marshal(file)
}



func (self *DBChunkstore) UnmarshalJSON(data []byte) error {
	var file = NetstatFile{
		Claim: make(map[string]*big.Int),
		CStat: make(map[string]*big.Int),
		BStat: make(map[string]*big.Int),
	}
	err := json.Unmarshal(data, &file)
	if err != nil {
		fmt.Println(err)
		return err
	}

	self.farmer = common.HexToAddress(file.WalletAddress)

	var ok bool

	for ticket, reward := range file.Ticket {
		file.Claim[ticket], ok = new(big.Int).SetString(reward, 10)
		if !ok {
			return fmt.Errorf("Ticket %v amount set: unable to convert string to big integer: %v", ticket, reward)
		}
	}

	for cc, cv := range file.ChunkStat {
		if cc == "ChunkR" || cc == "ChunkW" || cc == "ChunkS" {
			file.CStat[cc] = big.NewInt(0)
		} else {
			file.CStat[cc], ok = new(big.Int).SetString(cv, 10)
			if !ok {
				return fmt.Errorf("%v loading failure: unable to convert string to big integer: %v", cc, cv)
			}
		}
	}

	for bc, bv := range file.ByteStat {
		if bc == "ByteW" || bc == "ByteR" {
			file.BStat[bc] = big.NewInt(0)
		} else {
			file.BStat[bc], ok = new(big.Int).SetString(bv, 10)
			if !ok {
				return fmt.Errorf("%v loading failure: unable to convert string to big integer: %v", bc, bv)
			}
		}
	}

	self.netstat = &file
	return nil
}

func (self *DBChunkstore) Save() (err error) {
	data, err := json.MarshalIndent(self, "", " ")
	if err != nil {
		return err
	}
	fmt.Printf("\n%v\n", string(data))
	return ioutil.WriteFile(self.statpath, data, os.ModePerm)
}

func (self *DBChunkstore) Flush() (err error) {
	data, err := json.Marshal(self)
	if err != nil {
		return err
	}
	netstatlog, err := os.OpenFile("netstat.log", os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0600)
	if err != nil {
		return err
	}
	defer netstatlog.Close()
	fmt.Fprintf(netstatlog, "%s\n", data)
	return nil
}

func NewDBChunkStore(path string) (self *DBChunkstore, err error) {
	ts := time.Now()
	claim := make(map[string]*big.Int)
	chunkstat := map[string]*big.Int{"ChunkR": big.NewInt(0), "ChunkW": big.NewInt(0), "ChunkS": big.NewInt(0), "ChunkRL": big.NewInt(0), "ChunkWL": big.NewInt(0), "ChunkSL": big.NewInt(0)}
	bytestat := map[string]*big.Int{"ByteR": big.NewInt(0), "ByteW": big.NewInt(0), "ByteS": big.NewInt(0), "ByteRL": big.NewInt(0), "ByteWL": big.NewInt(0), "ByteSL": big.NewInt(0)}

	db, err := sql.Open("sqlite3", path)
	if err != nil {
		return nil, err
	}
	if db == nil {
		return nil, err
	}
	//Local Chunk table

	sql_table := `
    CREATE TABLE IF NOT EXISTS chunk (
    chunkKey TEXT NOT NULL PRIMARY KEY,
    chunkVal BLOB,
    payer TEXT,
    encrypted INTEGER DEFAULT 1,
    renewal INTEGER DEFAULT 1,
    replication INTEGER DEFAULT 1,
    chunkBirthDT DATETIME,
    chunkStoreDT DATETIME
    );
    `
	netstat_table := `
    CREATE TABLE IF NOT EXISTS netstat (
    statDT  DATETIME NOT NULL PRIMARY KEY,
    rcnt INTEGER DEFAULT 0,
    wcnt INTEGER DEFAULT 0,
    scnt INTEGER DEFAULT 0
    );
    `
	_, err = db.Exec(sql_table)
	if err != nil {
		fmt.Printf("Error Creating Chunk Table")
		return nil, err
	}
	_, err = db.Exec(netstat_table)
	if err != nil {
		fmt.Printf("Error Creating Stat Table")
		return nil, err
	}
	config, errConfig := LoadSWARMDBConfig(SWARMDBCONF_FILE)
	if errConfig != nil {
		fmt.Printf("Error Creating KeyManager")
		return nil, err
	}

	km, errKm := NewKeyManager(&config)

	if errKm != nil {
		fmt.Printf("Error Creating KeyManager")
		return nil, err
	}

	userWallet := config.Address
	nodeid := config.GetNodeID()
	walletAddr := common.HexToAddress(userWallet)

	netstat := NetstatFile{
		NodeID:        nodeid,
		WalletAddress: userWallet,
		LaunchDT:      &ts,
		CStat:         chunkstat,
		BStat:         bytestat,
		Claim:         claim,
	}

	self = &DBChunkstore{
		db:       db,
		km:       &km,
		farmer:   walletAddr,
		netstat:  &netstat,
		filepath: path,
		statpath: "netstat.json",
	}
	return
}

func LoadDBChunkStore(path string) (self *DBChunkstore, err error) {
	var data []byte
	defaultDBPath := "netstat.json"

	data, err = ioutil.ReadFile(defaultDBPath)
	if err != nil {
		fmt.Printf("Error in Loading netStat from %s.. generating new Log instead\n", defaultDBPath)
		self, _ = NewDBChunkStore(path)
		return self, nil

	}

	self = new(DBChunkstore)
	self.netstat = new(NetstatFile)
	err = json.Unmarshal(data, &self)
	//err = json.Unmarshal(data, self.netstat)

	if err != nil {
		fmt.Printf("Error in Parsing netStat new Log created\n => %s\n", err)
		self, _ = NewDBChunkStore(path)
		return self, nil
	}

	db, err := sql.Open("sqlite3", path)
	if err != nil {
		return nil, err
	}
	if db == nil {
		return nil, err
	}

	config, errConfig := LoadSWARMDBConfig(SWARMDBCONF_FILE)
	if errConfig != nil {
		fmt.Printf("Error Creating KeyManager")
		return nil, err
	}

	km, errKm := NewKeyManager(&config)
	if errKm != nil {
		fmt.Printf("Error Creating KeyManager")
		return nil, err
	}

	self.db = db
	self.km = &km
	self.filepath = path
	self.statpath = defaultDBPath
	return
}

func (self *DBChunkstore) StoreKChunk(u *SWARMDBUser, k []byte, v []byte, encrypted int) (err error) {
	//TODO get OWNER from CHUNK or get it from swarmdb into dbchunkstore
	ts := time.Now()
	if len(v) < minChunkSize {
		return fmt.Errorf("chunk too small") // should be improved
	}

	sql_add := `INSERT OR REPLACE INTO chunk ( chunkKey, chunkVal, encrypted, chunkBirthDT, chunkStoreDT, renewal, replication) values(?, ?, ?, COALESCE((SELECT chunkBirthDT FROM chunk WHERE chunkKey=?),CURRENT_TIMESTAMP), COALESCE((SELECT chunkStoreDT FROM chunk WHERE chunkKey=? ), CURRENT_TIMESTAMP), ?, ?)`
	stmt, err := self.db.Prepare(sql_add)
	if err != nil {
		fmt.Printf("\nError Preparing into Table: [%s]", err)
		return (err)
	}
	defer stmt.Close()

	recordData := v[577:4096]
	encRecordData := self.km.EncryptData(u, recordData)

	var finalSdata [8192]byte
	copy(finalSdata[0:577], v[0:577])
	copy(finalSdata[577:], encRecordData)
	_, err2 := stmt.Exec(k[:32], finalSdata[0:], encrypted, k[:32], k[:32], u.AutoRenew, u.MaxReplication)
	if err2 != nil {
		fmt.Printf("\nError Inserting into Table: [%s]", err2)
		fmt.Printf("Putting in this data: [%s]", finalSdata)
		return (err2)
	}
	stmt.Close()
	self.netstat.LWriteDT = &ts
	self.netstat.CStat["ChunkW"].Add(self.netstat.CStat["ChunkW"], big.NewInt(1))
	//self.netstat.CStat["ChunkS"].Add(self.netstat.CStat["ChunkS"], big.NewInt(1))
	return nil
}

func (self *DBChunkstore) RetrieveKChunk(u *SWARMDBUser, key []byte) (val []byte, err error) {
	ts := time.Now()
	val = make([]byte, 8192)
	sql := `SELECT chunkKey, chunkVal, chunkBirthDT, chunkStoreDT, encrypted FROM chunk WHERE chunkKey = $1`
	stmt, err := self.db.Prepare(sql)
	if err != nil {
		fmt.Printf("Error preparing sql [%s] Err: [%s]", sql, err)
		return val, err
	}
	defer stmt.Close()

	rows, err := stmt.Query(key)
	if err != nil {
		fmt.Printf("Error preparing sql [%s] Err: [%s]", sql, err)
		return nil, err
	}
	defer rows.Close()

	for rows.Next() {
		var kV []byte
		var bdt []byte
		var sdt []byte
		var enc []byte

		err2 := rows.Scan(&kV, &val, &bdt, &sdt, &enc)
		if err2 != nil {
			return nil, err2
		}
		//fmt.Printf("\nQuery Key: [%x], [%s], [%s], [%s] with VAL: [%+v]", kV, bdt, sdt, enc, val)
		jsonRecord := val[577:]
		trimmedJson := bytes.TrimRight(jsonRecord, "\x00")
		decVal := self.km.DecryptData(u, trimmedJson)
		decVal = bytes.TrimRight(decVal, "\x00")
		self.netstat.LReadDT = &ts
		self.netstat.CStat["ChunkR"].Add(self.netstat.CStat["ChunkR"], big.NewInt(1))
		return decVal, nil
	}
	return val, nil
}

func (self *DBChunkstore) StoreChunk(u *SWARMDBUser, v []byte, encrypted int) (k []byte, err error) {
	ts := time.Now()
	if len(v) < minChunkSize {
		return k, fmt.Errorf("chunk too small") // should be improved
	}
	inp := make([]byte, minChunkSize)
	copy(inp, v[0:minChunkSize])
	h := sha256.New()
	h.Write([]byte(inp))
	k = h.Sum(nil)

	//sql_add := `INSERT OR REPLACE INTO chunk ( chunkKey, chunkVal, chunkBirthDT, chunkStoreDT ) values(?, ?, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)`
	sql_add := `INSERT OR REPLACE INTO chunk ( chunkKey, chunkVal, encrypted, chunkBirthDT, chunkStoreDT, renewal, replication) values(?, ?, ?, COALESCE((SELECT chunkStoreDT FROM chunk WHERE chunkKey=?),CURRENT_TIMESTAMP), COALESCE((SELECT chunkStoreDT FROM chunk WHERE chunkKey=? ), CURRENT_TIMESTAMP), ?, ?)`
	stmt, err := self.db.Prepare(sql_add)
	if err != nil {
		return k, err
	}
	defer stmt.Close()

	encVal := self.km.EncryptData(u, v)
	_, err2 := stmt.Exec(k, encVal, encrypted, k, k, u.AutoRenew, u.MaxReplication)
	if err2 != nil {
		fmt.Printf("\nError Inserting into Table: [%s]", err)
		return k, err2
	}
	stmt.Close()
	self.netstat.LWriteDT = &ts
	self.netstat.CStat["ChunkW"].Add(self.netstat.CStat["ChunkW"], big.NewInt(1))
	//self.netstat.CStat["ChunkS"].Add(self.netstat.CStat["ChunkS"], big.NewInt(1))
	return k, nil
}

func (self *DBChunkstore) RetrieveChunk(u *SWARMDBUser, key []byte) (val []byte, err error) {
	ts := time.Now()
	val = make([]byte, 8192)
	sql := `SELECT chunkVal FROM chunk WHERE chunkKey = $1`
	stmt, err := self.db.Prepare(sql)
	if err != nil {
		fmt.Printf("Error preparing sql [%s] Err: [%s]", sql, err)
		return val, err
	}
	defer stmt.Close()

	rows, err := stmt.Query(key)
	if err != nil {
		fmt.Printf("Error preparing sql [%s] Err: [%s]", sql, err)
		return nil, err
	}
	defer rows.Close()

	for rows.Next() {
		err2 := rows.Scan(&val)
		if err2 != nil {
			return nil, err2
		}
		decVal := self.km.DecryptData(u, val)
		self.netstat.LReadDT = &ts
		self.netstat.CStat["ChunkR"].Add(self.netstat.CStat["ChunkR"], big.NewInt(1))
		return decVal, nil
	}
	return val, nil
}

func valid_type(typ string) (valid bool) {
	if typ == "X" || typ == "D" || typ == "H" || typ == "K" || typ == "C" {
		return true
	}
	return false
}

func (self *DBChunkstore) PrintDBChunk(columnType ColumnType, hashid []byte, c []byte) {
	nodetype := string(c[4096-65 : 4096-64])
	if valid_type(nodetype) {
		fmt.Printf("Chunk %x ", hashid)
		fmt.Printf(" NodeType: %s ", nodetype)
		childtype := string(c[4096-66 : 4096-65])
		if valid_type(childtype) {
			fmt.Printf(" ChildType: %s ", childtype)
		}
		fmt.Printf("\n")
		if nodetype == "D" {
			p := make([]byte, 32)
			n := make([]byte, 32)
			copy(p, c[4096-64:4096-32])
			copy(n, c[4096-64:4096-32])
			if IsHash(p) {
				fmt.Printf(" PREV: %x ", p)
			} else {
				fmt.Printf(" PREV: *NULL* ", p)
			}
			if IsHash(n) {
				fmt.Printf("\tNEXT: %x ", n)
			} else {
				fmt.Printf("\tNEXT: *NULL* ", p)
			}
			fmt.Printf("\n")

		}
	}

	k := make([]byte, 32)
	v := make([]byte, 32)
	for i := 0; i < 32; i++ {
		copy(k, c[i*64:i*64+32])
		copy(v, c[i*64+32:i*64+64])
		if EmptyBytes(k) && EmptyBytes(v) {
		} else {
			fmt.Printf(" %d:\t%s\t%s\n", i, KeyToString(columnType, k), ValueToString(v))
		}
	}
	fmt.Printf("\n")
}

func (self *DBChunkstore) ScanAll() (err error) {
	ts := time.Now()
	sql_readall := `SELECT chunkKey, chunkVal,strftime('%s',chunkStoreDT) FROM chunk ORDER BY chunkStoreDT DESC`
	rows, err := self.db.Query(sql_readall)
	if err != nil {
		return err
	}
	defer rows.Close()

	var rcnt int64
	var result []DBChunk
	for rows.Next() {
		c := DBChunk{}
		err2 := rows.Scan(&c.Key, &c.Val, &c.ChunkStoreDT)
		if err2 != nil {
			return err2
		}
		rcnt++
		/*
			jsonRecord := c.Val[577:]
			trimmedJson := bytes.TrimRight(jsonRecord, "\x00")
			decVal := self.km.DecryptData(trimmedJson)
			c.Val = bytes.TrimRight(decVal, "\x00")
			fmt.Printf("[record] %x => %s [%v]\n", c.Key, c.Val, c.ChunkStoreDT)
		*/
		result = append(result, c)
	}
	rows.Close()

	sql_chunkRead := `INSERT OR REPLACE INTO netstat (statDT, rcnt) values(CURRENT_TIMESTAMP, ?)`
	stmt, err := self.db.Prepare(sql_chunkRead)
	if err != nil {
		return err
	}
	defer stmt.Close()

	_, err2 := stmt.Exec(rcnt)
	if err2 != nil {
		fmt.Printf("\nError updating stat Table: [%s]", err2)
		return err2
	}
	stmt.Close()
	self.netstat.LReadDT = &ts
	self.netstat.CStat["ChunkR"].Add(self.netstat.CStat["ChunkR"], new(big.Int).SetInt64(rcnt))
	return nil
}

func (self *DBChunkstore) GenerateFarmerLog() (err error) {

	farmerAddr := self.farmer.Hex()

	/*
	   currentTS:= time.Now().Unix()
	   contractInterval := 3600*7 //Test renewal interval
	*/

	sql_readall := `SELECT chunkKey,strftime('%s',chunkBirthDT) as chunkBirthTS, strftime('%s',chunkStoreDT) as chunkStoreTS, replication, renewal FROM chunk where replication > 0 ORDER BY chunkStoreTS DESC`
	rows, err := self.db.Query(sql_readall)
	if err != nil {
		return err
	}
	defer rows.Close()

	var result []ChunkLog
	for rows.Next() {
		c := ChunkLog{}
		c.Farmer = farmerAddr

		err2 := rows.Scan(&c.ChunkHash, &c.ChunkBD, &c.ChunkSD, &c.ReplicationLevel, &c.Renewable)
		if err2 != nil {
			return err2
		}
		c.ChunkID = fmt.Sprintf("%x", c.ChunkHash)
		chunklog, err := json.Marshal(c)
		if err != nil {
			return err
		}
		fmt.Printf("%s\n", chunklog)

		//fmt.Printf("%v|%x|%v|%v|%v|%v\n", c.Farmer, c.ChunkHash, c.ChunkBD, c.ChunkSD, c.ReplicationLevel, c.Renewable)
		result = append(result, c)
	}
	rows.Close()
	return nil
}

func (self *DBChunkstore) ClaimAll() (err error) {
	fmt.Printf("netCounter: %v\n", netCounter)
	fmt.Printf("self: %v\n", self)

	ticket := "9f2018c7dc1e31fb6708fd6bd0f8975bf704e5a0e8465fbef2b5e7e5fc37c4d8"
	reward := 121
	self.netstat.Claim[ticket] = new(big.Int).SetInt64(int64(reward))
	return nil
}

func (self *DBChunkstore) GetChunkStored() (err error) {
	sql_chunkTally := `SELECT count(*) FROM chunk`
	rows, err := self.db.Query(sql_chunkTally)
	if err != nil {
		return err
	}
	defer rows.Close()

	var result []ChunkStats
	chunkStored := int64(0)
	for rows.Next() {
		c := ChunkStats{}
		err2 := rows.Scan(&c.ChunkStored)
		if err2 != nil {
			fmt.Printf("ERROR:%s\n", err2)
			return err2
		}
		chunkStored += c.ChunkStored
		//fmt.Printf("[stat] Time %v => Read:%v | Write:%v | Stored:%v\n", c.CurrentTS, c.ChunkRead, c.ChunkWrite, c.ChunkStored)
		result = append(result, c)
	}
	rows.Close()
	self.netstat.CStat["ChunkS"].Sub(new(big.Int).SetInt64(chunkStored), self.netstat.CStat["ChunkSL"])
	self.netstat.CStat["ChunkSL"] = new(big.Int).SetInt64(chunkStored)
	return nil
}

func (self *DBChunkstore) GetChunkStat() (res string, err error) {
	sql_chunkTally := `SELECT strftime('%s',statDT) as STS, sum(rcnt), sum(wcnt), sum(scnt) FROM netstat group by strftime('%s',statDT) order by STS DESC`
	rows, err := self.db.Query(sql_chunkTally)
	if err != nil {
		return res, err
	}
	defer rows.Close()

	var result []ChunkStats
	for rows.Next() {
		c := ChunkStats{}
		err2 := rows.Scan(&c.CurrentTS, &c.ChunkRead, &c.ChunkWrite, &c.ChunkStored)
		if err2 != nil {
			fmt.Printf("ERROR:%s\n", err2)
			return res, err2
		}
		fmt.Printf("[stat] Time %v => Read:%v | Write:%v | Stored:%v\n", c.CurrentTS, c.ChunkRead, c.ChunkWrite, c.ChunkStored)
		result = append(result, c)
	}
	rows.Close()

	output, err := json.Marshal(result)
	if err != nil {
		return res, nil
	} else {
		return string(output), nil
	}
}
