package storage

import (
	"crypto/sha256"
	"database/sql"
	"encoding/binary"
	"fmt"
	"github.com/ethereum/go-ethereum/swarmdb/common"
	_ "github.com/mattn/go-sqlite3"
	"math"
	"time"
)

type DBChunkstore struct {
	filepath string
	db       *sql.DB
}

type DBChunk struct {
	Key         []byte // 32
	Val         []byte // 4096
	Owner       []byte // 42
	BuyAt       []byte // 32
	Blocknumber []byte // 32
	Tablename   []byte // 32
	TableId     []byte // 32
	StoreDT     *time.Time
}

func NewDBChunkStore(path string) (dbcs DBChunkstore, err error) {
	db, err := sql.Open("sqlite3", path)
	if err != nil {
		return dbcs, err
	}
	if db == nil {
		return dbcs, err
	}
	fmt.Printf("Created DB Chunkstore")
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
	_, err = db.Exec(sql_table)
	if err != nil {
		fmt.Printf("Error Creating Table")
		return dbcs, err
	}
	return dbcs, nil
}

func (self *DBChunkstore) StoreKChunk(k []byte, v []byte) (err error) {
	fmt.Printf("\nStartin StoreKChunk")
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

	_, err2 := stmt.Exec(k, v)
	if err2 != nil {
		fmt.Printf("\nError Inserting into Table: [%s]", err)
		return (err2)
	}
	fmt.Printf("\nEnding StoreKChunk")
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

	_, err2 := stmt.Exec(k, v)
	if err2 != nil {
		return k, err2
	}

	return k, nil
}

func (self *DBChunkstore) RetrieveChunk(key []byte) (val []byte, err error) {
	val = make([]byte, 4096)
	sql := `SELECT chunkVal FROM chunk WHERE chunkKey = $1`
	stmt, err := self.db.Prepare(sql)
	if err != nil {
		return val, err
	}
	defer stmt.Close()

	rows, err := stmt.Query(key)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	for rows.Next() {
		err2 := rows.Scan(&val)
		if err2 != nil {
			return nil, err2
		}
		return val, nil
	}
	return val, nil
}

func emptybytes(hashid []byte) (valid bool) {
	valid = true
	for i := 0; i < len(hashid); i++ {
		if hashid[i] != 0 {
			return false
		}
	}
	return valid
}

func valid_type(typ string) (valid bool) {
	if typ == "X" || typ == "D" || typ == "H" || typ == "K" || typ == "C" {
		return true
	}
	return false
}

func is_hash(hashid []byte) (valid bool) {
	cnt := 0
	for i := 0; i < len(hashid); i++ {
		if hashid[i] == 0 {
			cnt++
		}
	}
	if cnt > 3 {
		return false
	} else {
		return true
	}
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
			if is_hash(p) {
				fmt.Printf(" PREV: %x ", p)
			} else {
				fmt.Printf(" PREV: *NULL* ", p)
			}
			if is_hash(n) {
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
		if emptybytes(k) && emptybytes(v) {
		} else {
			fmt.Printf(" %d:\t", i)

			switch keytype {
			case common.KT_BLOB:
				fmt.Printf("%v", k)
			case common.KT_STRING:
				fmt.Printf("%s", string(k))
			case common.KT_INTEGER:
				a := binary.BigEndian.Uint64(k)
				fmt.Printf("%d", a)
			case common.KT_FLOAT:
				bits := binary.LittleEndian.Uint64(k)
				f := math.Float64frombits(bits)
				fmt.Printf("%f", f)
			}
			if is_hash(v) {
				fmt.Printf("\t%x\t", v)
			} else {
				fmt.Printf("\t%v\t", string(v))
			}
			fmt.Printf("\n")
		}
	}
	fmt.Printf("\n")
}

func (self *DBChunkstore) ScanAll() (err error) {
	sql_readall := `SELECT chunkKey, chunkVal, storeDT FROM chunk ORDER BY datetime(storeDT) DESC`
	rows, err := self.db.Query(sql_readall)
	if err != nil {
		return err
	}
	defer rows.Close()

	var result []DBChunk
	for rows.Next() {
		c := DBChunk{}
		err2 := rows.Scan(&c.Key, &c.Val, &c.StoreDT)
		if err2 != nil {
			return err2
		}
		result = append(result, c)
	}
	return nil
}
