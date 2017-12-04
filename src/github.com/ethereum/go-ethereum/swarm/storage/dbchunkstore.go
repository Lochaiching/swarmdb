package storage

import (
	"database/sql"
	"crypto/sha256"
	_ "github.com/mattn/go-sqlite3"
	"time"
)

type DBChunkstore struct {
	filepath  string
	db        *sql.DB
}

type DBChunk struct {
	Key []byte  // 32
	Val []byte  // 4096
	StoreDT *time.Time
}

func NewDBChunkStore(path string) (dbcs DBChunkstore, err error) {
	db, err := sql.Open("sqlite3", path)
	if err != nil { return dbcs, err }
	if db == nil { return dbcs, err } 
	dbcs.db = db
	dbcs.filepath = path
	// create table if not exists
	sql_table := `
	CREATE TABLE IF NOT EXISTS chunk (
	chunkKey TEXT NOT NULL PRIMARY KEY,
	chunkVal BLOB,
	storeDT DATETIME
	);
	`

	_, err = db.Exec(sql_table)
	if err != nil { 
		return dbcs, err
	}
	return dbcs, nil
}

func (self *DBChunkstore) StoreKChunk(k []byte, v []byte) (err error) {
	sql_add := `INSERT OR REPLACE INTO chunk ( chunkKey, chunkVal, storeDT ) values(?, ?, CURRENT_TIMESTAMP)`
	stmt, err := self.db.Prepare(sql_add)
	if err != nil { return(err) }
	defer stmt.Close()

	_, err2 := stmt.Exec(k, v)
	if err2 != nil { return(err2) }
	return nil
}

func (self *DBChunkstore) StoreChunk(v []byte) (k []byte, err error) {
	inp := make([]byte, 4000)
	copy(inp, v[0:4000])
	h := sha256.New()
	h.Write([]byte(inp))
	k = h.Sum(nil)

	sql_add := `INSERT OR REPLACE INTO chunk ( chunkKey, chunkVal, storeDT ) values(?, ?, CURRENT_TIMESTAMP)`
	stmt, err := self.db.Prepare(sql_add)
	if err != nil { return k, err }
	defer stmt.Close()

	_, err2 := stmt.Exec(k, v)
	if err2 != nil { return k, err2 }
	
	return k, nil
}

func (self *DBChunkstore) RetrieveChunk(key []byte) (val []byte, err error) {
	val = make([]byte, 4096)
	sql := `SELECT chunkVal FROM chunk WHERE chunkKey = $1`
	stmt, err := self.db.Prepare(sql)
	if err != nil { return val, err }
	defer stmt.Close()
	
	rows, err := stmt.Query(key)
	if err != nil { return nil, err }
	defer rows.Close()

	for rows.Next() {
		err2 := rows.Scan(&val)
		if err2 != nil { return nil, err2 }
		return val, nil
	}
	return val, nil
}

func (self *DBChunkstore) ScanAll() (err error) {
	sql_readall := `SELECT chunkKey, chunkVal, storeDT FROM chunk ORDER BY datetime(storeDT) DESC`
	rows, err := self.db.Query(sql_readall)
	if err != nil { return err }
	defer rows.Close()

	var result []DBChunk
	for rows.Next() {
		c := DBChunk{}
		err2 := rows.Scan(&c.Key, &c.Val, &c.StoreDT)
		if err2 != nil { return err2 }
		result = append(result, c)
	}
	return  nil
}

