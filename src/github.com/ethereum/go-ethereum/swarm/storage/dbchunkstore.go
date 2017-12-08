package storage

import (
	"bytes"
	"crypto/sha256"
	"database/sql"
	//"encoding/binary"
	"fmt"
	"github.com/ethereum/go-ethereum/swarmdb/common"
	"github.com/ethereum/go-ethereum/swarmdb/keymanager"
	_ "github.com/mattn/go-sqlite3"
	//"math"
	//"time"
    "encoding/json"
)

type DBChunkstore struct {
	filepath string
	db       *sql.DB
	km       *keymanager.KeyManager
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

type ChunkStat struct {
    CurrentTS          int64     `json:"CurrentTS`
    ChunkRead          int64     `json:"ChunkRead`
    ChunkWrite         int64     `json:"ChunkWrite`
    ChunkStored        int64     `json:"ChunkStored"`
}

func NewDBChunkStore(path string) (dbcs DBChunkstore, err error) {
	db, err := sql.Open("sqlite3", path)
	if err != nil {
		return dbcs, err
	}
	if db == nil {
		return dbcs, err
	}
	dbcs.db = db
	dbcs.filepath = path
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
		return dbcs, err
	}
    _, err = db.Exec(netstat_table)
    if err != nil {
        fmt.Printf("Error Creating Stat Table")
        return dbcs, err
    }

	km, errKm := keymanager.NewKeyManager("/tmp/blah")
	if errKm != nil {
		fmt.Printf("Error Creating KeyManager")
		return dbcs, err
	}
	dbcs.km = &km
	return dbcs, nil
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

	recordData := v[577:]
	encRecordData := self.km.EncryptData(recordData)

	var finalSdata [4096]byte
	copy(finalSdata[0:566], v[0:576])
	copy(finalSdata[577:], encRecordData)
	var newFinalSData []byte
	copy(newFinalSData[0:], finalSdata[0:4095])
	fmt.Printf("\n\noriginal val [%v] encoded to [%v]", v, finalSdata[0:4095])
	_, err2 := stmt.Exec(k[:32], finalSdata) //TODO: why is k going in as 64 instead of 32?
	fmt.Printf("\noriginal val [%v] encoded to [%v]", v, newFinalSData)
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
	val = make([]byte, 4096)
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
		fmt.Printf("\nLength of val is: [%s] and contents are [%s](%v)",len(val), val, val)
		jsonRecord := val[577:]
		fmt.Printf("json record is: [%s]", jsonRecord)
		decVal := self.km.DecryptData(jsonRecord)
		decVal = bytes.TrimRight(decVal, "\x00")
		return decVal, nil
	}
	return val, nil
}

func (self *DBChunkstore) RetrieveChunk(key []byte) (val []byte, err error) {
	val = make([]byte, 4096)
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
        c.Val = self.km.DecryptData(c.Val)
        fmt.Printf("[record] %x => %s [%v]\n", c.Key, c.Val, c.StoreDT)
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
            fmt.Printf("ERROR:%s\n",err2)
            return res, err2
        }
        fmt.Printf("[stat] Time %v => Read:%v | Write:%v | Stored:%v\n", c.CurrentTS, c.ChunkRead, c.ChunkWrite, c.ChunkStored)
        result = append(result, c)
    }
    rows.Close()
    
    output, err := json.Marshal(result)
    if err != nil {
        return res, nil
    }else{
        return string(output), nil
    }
}
