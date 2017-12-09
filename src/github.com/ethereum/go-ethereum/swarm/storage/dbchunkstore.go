package storage

import (
	"bytes"
	"crypto/sha256"
	"database/sql"
	//"encoding/binary"
	"encoding/json"
	"fmt"
	ethcommon "github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/swarmdb/common"
	"github.com/ethereum/go-ethereum/swarmdb/keymanager"
	_ "github.com/mattn/go-sqlite3"
	"io/ioutil"
	"math/big"
	"os"
	"time"
)

type DBChunkstore struct {
	db *sql.DB
	km *keymanager.KeyManager

	//file directory
	filepath string
	statpath string

	//persisted fields
	nodeid string
	farmer ethcommon.Address
	claims map[string]*big.Int

	//persisted stats
	chunkR int64
	chunkW int64
	chunkS int64

	//temp fields
	chunkRL int64
	chunkWL int64
	chunkSL int64

	launchDT time.Time
	lwriteDT time.Time
	logDT    time.Time
}

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

type netstatFile struct {
	NodeID        string
	WalletAddress string
	Claims        map[string]string
	LaunchDT      time.Time
	LogDT         time.Time
}

type ChunkStat struct {
	CurrentTS   int64 `json:"CurrentTS`
	ChunkRead   int64 `json:"ChunkRead`
	ChunkWrite  int64 `json:"ChunkWrite`
	ChunkStored int64 `json:"ChunkStored"`
}

func (self *DBChunkstore) MarshalJSON() ([]byte, error) {
	var file = &netstatFile{
		NodeID:        self.nodeid,
		WalletAddress: self.farmer.Hex(),
		Claims:        make(map[string]string),
		LaunchDT:      self.launchDT,
		LogDT:         time.Now(),
	}
	for ticket, reward := range self.claims {
		file.Claims[ticket] = reward.String()
	}
	return json.Marshal(file)
}

func (self *DBChunkstore) UnmarshalJSON(data []byte) error {
	var file netstatFile
	err := json.Unmarshal(data, &file)
	if err != nil {
		return err
	}

	self.launchDT = file.LaunchDT
	self.logDT = file.LogDT
	self.nodeid = file.NodeID
	self.farmer = ethcommon.HexToAddress(file.WalletAddress)

	var ok bool
	for ticket, reward := range file.Claims {
		self.claims[ticket], ok = new(big.Int).SetString(reward, 10)
		if !ok {
			return fmt.Errorf("Ticket %v amount set: unable to convert string to big integer: %v", ticket, reward)
		}
	}
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

	walletAddr := ethcommon.HexToAddress("0x56ad284968f2c2edb44c1380411c2c3b12b26c3f")

	self = &DBChunkstore{
		db:       db,
		km:       &km,
		filepath: path,
		statpath: "netstat.json",
		nodeid:   "1234",
		farmer:   walletAddr,
		claims:   claims,
		chunkR:   0,
		chunkW:   0,
		chunkS:   0,
		chunkRL:  0,
		chunkWL:  0,
		chunkSL:  0,
		launchDT: time.Now(),
		lwriteDT: time.Now(),
		logDT:    time.Now(),
	}

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
	return val, nil
}

func valid_type(typ string) (valid bool) {
	if typ == "X" || typ == "D" || typ == "H" || typ == "K" || typ == "C" {
		return true
	}
	return false
}

func (self *DBChunkstore) PrintDBChunk(keytype common.KeyType, hashid []byte, c []byte) {
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
			if common.IsHash(p) {
				fmt.Printf(" PREV: %x ", p)
			} else {
				fmt.Printf(" PREV: *NULL* ", p)
			}
			if common.IsHash(n) {
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
		if common.EmptyBytes(k) && common.EmptyBytes(v) {
		} else {
			fmt.Printf(" %d:\t%s\t%s\n", i, common.KeyToString(keytype, k), common.ValueToString(v))
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
