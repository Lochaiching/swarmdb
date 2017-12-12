package common

import (
	"bytes"
	"crypto/sha256"
	"database/sql"
	//"encoding/binary"
	"encoding/json"
	"fmt"
	ethcommon "github.com/ethereum/go-ethereum/common"
	// "github.com/ethereum/go-ethereum/swarmdb/common"
	"github.com/ethereum/go-ethereum/swarmdb/keymanager"
	_ "github.com/mattn/go-sqlite3"
	"io/ioutil"
	"math/big"
	"os"
	"strconv"
	"time"
)

var (
	netCounter NetstatFile
)

type DBChunk struct {
	Key         []byte // 32
	Val         []byte // 4096
	Owner       []byte // 42
	BuyAt       []byte // 32
	Blocknumber []byte // 32
	Tablename   []byte // 32
	TableId     []byte // 32
	StoreDT     int64
}

type ChunkStat struct {
	CurrentTS   int64 `json:"CurrentTS`
	ChunkRead   int64 `json:"ChunkRead`
	ChunkWrite  int64 `json:"ChunkWrite`
	ChunkStored int64 `json:"ChunkStored"`
}

func (self *DBChunkstore) MarshalJSON() ([]byte, error) {

	var file = &NetstatFile{
		NodeID:        netCounter.NodeID,
		WalletAddress: self.farmer.Hex(),
		Claims:        make(map[string]string),
		ChunkStats:    make(map[string]string),
		LaunchDT:      netCounter.LaunchDT,
		LReadDT:       netCounter.LReadDT,
		LWriteDT:      netCounter.LWriteDT,
		LogDT:         time.Now(),
	}

	for chuckcol, chuckval := range netCounter.CStat {
		fmt.Printf("[%v] => %v\n", chuckcol, chuckval)
		file.ChunkStats[chuckcol] = chuckval.String()
	}

	for ticket, reward := range self.claims {
		file.Claims[ticket] = reward.String()
	}
	return json.Marshal(file)
}

func (self *DBChunkstore) UnmarshalJSON(data []byte) error {
	var file NetstatFile
	err := json.Unmarshal(data, &file)
	if err != nil {
		fmt.Println(err)
		return err
	}

	netCounter.LaunchDT = file.LaunchDT
	netCounter.LReadDT = file.LReadDT
	netCounter.LWriteDT = file.LWriteDT
	netCounter.LogDT = file.LogDT
	netCounter.NodeID = file.NodeID

	self.farmer = ethcommon.HexToAddress(file.WalletAddress)

	var ok bool
	for ticket, reward := range file.Claims {
		self.claims[ticket], ok = new(big.Int).SetString(reward, 10)
		if !ok {
			return fmt.Errorf("Ticket %v amount set: unable to convert string to big integer: %v", ticket, reward)
		}
	}

	prevchunkstat := make(map[string]*big.Int)

	for chuckcol, chuckval := range file.ChunkStats {
		prevchunkstat[chuckcol], ok = new(big.Int).SetString(chuckval, 10)
		if !ok {
			return fmt.Errorf("%v loading failure: unable to convert string to big integer: %v", chuckcol, chuckval)
		}

	}
	netCounter.CStat = prevchunkstat
	return nil
}

func (self *DBChunkstore) Save() (err error) {
	data, err := json.MarshalIndent(self, "", " ")
	if err != nil {
		return err
	}
	//self.log.Trace("Saving NetStat to disk", self.statpath)
	return ioutil.WriteFile(self.statpath, data, os.ModePerm)
}

func NewDBChunkStore(path string) (self *DBChunkstore, err error) {

	claims := make(map[string]*big.Int)
	chunkstat := make(map[string]*big.Int)
    chunkstat["ChunkR"], chunkstat["ChunkW"], chunkstat["ChunkS"], chunkstat["ChunkRL"], chunkstat["ChunkWL"], chunkstat["ChunkSL"] = big.NewInt(0), big.NewInt(0), big.NewInt(0), big.NewInt(0), big.NewInt(0), big.NewInt(0)

	db, err := sql.Open("sqlite3", path)
	if err != nil {
		return nil, err
	}
	if db == nil {
		return nil, err
	}
	// create table if not exists
	sql_table := `
    CREATE TABLE IF NOT EXISTS chunk (
    chunkKey TEXT NOT NULL PRIMARY KEY,
    chunkVal BLOB,
    Owner TEXT,
    BuyAt TEXT,
    BlockNumber TEXT,
    Tablename TEXT,
    Tableid TEXT,
    storeDT DATETIME
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

	km, errKm := keymanager.NewKeyManager("/tmp/blah")
	if errKm != nil {
		fmt.Printf("Error Creating KeyManager")
		return nil, err
	}

	nodeid := "abcd"
	userWallet := "0x56ad284968f2c2edb44c1380411c2c3b12b26c3f"
	walletAddr := ethcommon.HexToAddress(userWallet)

	netCounter.LaunchDT = time.Now()
	netCounter.NodeID = nodeid
	netCounter.CStat = chunkstat

	netstat := NetstatFile{
		NodeID:        netCounter.NodeID,
		WalletAddress: userWallet,
		LaunchDT:      netCounter.LaunchDT,
	}

	self = &DBChunkstore{
		db:       db,
		km:       &km,
		farmer:   walletAddr,
		claims:   claims,
		netstat:  &netstat,
		filepath: path,
		statpath: "netstat.json",
	}
	return
}

func LoadDBChunkStore(path string) (self *DBChunkstore, err error) {
	var data []byte
	devaultDBPath := "netstat.json"
	data, err = ioutil.ReadFile(devaultDBPath)
	if err != nil {
		fmt.Printf("Error in Loading netStat from %s.. generating new Log instead\n", devaultDBPath)
		self, _ = NewDBChunkStore(path)
		return self, nil

	}

	self = new(DBChunkstore)
	self.netstat = new(NetstatFile)
	err = json.Unmarshal(data, &self)
	//err = json.Unmarshal(data, self.netstat)
	//self.farmer = ethcommon.HexToAddress(self.netstat.WalletAddress)

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

	km, errKm := keymanager.NewKeyManager("/tmp/blah")
	if errKm != nil {
		fmt.Printf("Error Creating KeyManager")
		return nil, err
	}

	self.db = db
	self.km = &km
	self.filepath = path
	self.statpath = devaultDBPath
	return
}

func (self *DBChunkstore) StoreKChunk(k []byte, v []byte) (err error) {
	if len(v) < minChunkSize {
		return fmt.Errorf("chunk too small") // should be improved
	}

	sql_add := `INSERT OR REPLACE INTO chunk ( chunkKey, chunkVal, storeDT ) values(?, ?, CURRENT_TIMESTAMP)`
	stmt, err := self.db.Prepare(sql_add)
	if err != nil {
		fmt.Printf("\nError Preparing into Table: [%s]", err)
		return (err)
	}
	defer stmt.Close()

	recordData := v[577:4095]
	encRecordData := self.km.EncryptData(recordData)

	var finalSdata [8192]byte
	copy(finalSdata[0:566], v[0:576])
	copy(finalSdata[577:], encRecordData)
	_, err2 := stmt.Exec(k[:32], finalSdata[0:]) //TODO: why is k going in as 64 instead of 32?
	if err2 != nil {
		fmt.Printf("\nError Inserting into Table: [%s]", err2)
		fmt.Printf("Putting in this data: [%s]", finalSdata)
		return (err2)
	}
	stmt.Close()
	netCounter.LWriteDT = time.Now()
	netCounter.CStat["ChunkW"].Add(netCounter.CStat["ChunkW"], big.NewInt(1))
	netCounter.CStat["ChunkS"].Add(netCounter.CStat["ChunkS"], big.NewInt(1))
	return nil
}

const (
	minChunkSize = 4000
)

func (self *DBChunkstore) StoreChunk(v []byte) (k []byte, err error) {
	if len(v) < minChunkSize {
		return k, fmt.Errorf("chunk too small") // should be improved
	}
	inp := make([]byte, minChunkSize)
	copy(inp, v[0:minChunkSize])
	h := sha256.New()
	h.Write([]byte(inp))
	k = h.Sum(nil)

	sql_add := `INSERT OR REPLACE INTO chunk ( chunkKey, chunkVal, storeDT ) values(?, ?, CURRENT_TIMESTAMP)`
	stmt, err := self.db.Prepare(sql_add)
	if err != nil {
		return k, err
	}
	defer stmt.Close()

	encVal := self.km.EncryptData(v)
	_, err2 := stmt.Exec(k, encVal)
	if err2 != nil {
		fmt.Printf("\nError Inserting into Table: [%s]", err)
		return k, err2
	}
	stmt.Close()
	netCounter.LWriteDT = time.Now()
	netCounter.CStat["ChunkW"].Add(netCounter.CStat["ChunkW"], big.NewInt(1))
	netCounter.CStat["ChunkS"].Add(netCounter.CStat["ChunkS"], big.NewInt(1))
	return k, nil
}

func (self *DBChunkstore) RetrieveKChunk(key []byte) (val []byte, err error) {
	val = make([]byte, 8192)
	sql := `SELECT chunkVal FROM chunk WHERE chunkKey = $1`
	stmt, err := self.db.Prepare(sql)
	if err != nil {
		fmt.Printf("Error preparing sql [%s] Err: [%s]", sql, err)
		return val, err
	}
	defer stmt.Close()

	//rows, err := stmt.Query()
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
		jsonRecord := val[577:]
		trimmedJson := bytes.TrimRight(jsonRecord, "\x00")
		decVal := self.km.DecryptData(trimmedJson)
		decVal = bytes.TrimRight(decVal, "\x00")
		return decVal, nil
	}
	netCounter.LReadDT = time.Now()
	netCounter.CStat["ChunkR"].Add(netCounter.CStat["ChunkR"], big.NewInt(1))
	return val, nil
}

func (self *DBChunkstore) RetrieveChunk(key []byte) (val []byte, err error) {
	val = make([]byte, 8192)
	sql := `SELECT chunkVal FROM chunk WHERE chunkKey = $1`
	stmt, err := self.db.Prepare(sql)
	if err != nil {
		fmt.Printf("Error preparing sql [%s] Err: [%s]", sql, err)
		return val, err
	}
	defer stmt.Close()

	//rows, err := stmt.Query()
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
		decVal := self.km.DecryptData(val)
		return decVal, nil
	}
	netCounter.LReadDT = time.Now()
	netCounter.CStat["ChunkR"].Add(netCounter.CStat["ChunkR"], big.NewInt(1))
	return val, nil
}

func valid_type(typ string) (valid bool) {
	if typ == "X" || typ == "D" || typ == "H" || typ == "K" || typ == "C" {
		return true
	}
	return false
}

func (self *DBChunkstore) PrintDBChunk(keytype KeyType, hashid []byte, c []byte) {
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
			fmt.Printf(" %d:\t%s\t%s\n", i, KeyToString(keytype, k), ValueToString(v))
		}
	}
	fmt.Printf("\n")
}

func (self *DBChunkstore) ScanAll() (err error) {
	sql_readall := `SELECT chunkKey, chunkVal,strftime('%s',storeDT) FROM chunk ORDER BY storeDT DESC`
	rows, err := self.db.Query(sql_readall)
	if err != nil {
		return err
	}
	defer rows.Close()

	var rcnt int
	var result []DBChunk
	for rows.Next() {
		c := DBChunk{}
		err2 := rows.Scan(&c.Key, &c.Val, &c.StoreDT)
		if err2 != nil {
			return err2
		}
		rcnt++
		/*
			jsonRecord := c.Val[577:]
			trimmedJson := bytes.TrimRight(jsonRecord, "\x00")
			decVal := self.km.DecryptData(trimmedJson)
			c.Val = bytes.TrimRight(decVal, "\x00")
			fmt.Printf("[record] %x => %s [%v]\n", c.Key, c.Val, c.StoreDT)
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
	netCounter.LReadDT = time.Now()
	n, ok := new(big.Int).SetString(strconv.FormatInt(int64(rcnt), 10), 10) // bad representation
	if !ok {
		fmt.Printf("\nError in updating counter\n")
		return nil
	}
	netCounter.CStat["ChunkR"].Add(netCounter.CStat["ChunkR"], n)
	return nil
}

func (self *DBChunkstore) GetChunkStat() (res string, err error) {
	sql_chunkTally := `SELECT strftime('%s',statDT) as STS, sum(rcnt), sum(wcnt), sum(scnt) FROM netstat group by strftime('%s',statDT) order by STS DESC`
	rows, err := self.db.Query(sql_chunkTally)
	if err != nil {
		return res, err
	}
	defer rows.Close()

	var result []ChunkStat
	for rows.Next() {
		c := ChunkStat{}
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
