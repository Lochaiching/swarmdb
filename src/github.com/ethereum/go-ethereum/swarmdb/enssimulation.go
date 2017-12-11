package common

import (
	"database/sql"
	_ "github.com/mattn/go-sqlite3"
	// "time"
)

func NewENSSimulation(path string) (ens ENSSimulation, err error) {
	db, err := sql.Open("sqlite3", path)
	if err != nil {
		return ens, err
	}
	if db == nil {
		return ens, err
	}
	ens.db = db
	ens.filepath = path

	sql_table := `
	CREATE TABLE IF NOT EXISTS ens (
	indexName TEXT NOT NULL PRIMARY KEY,
	roothash BLOB,
	storeDT DATETIME
	);
	`

	_, err = db.Exec(sql_table)
	if err != nil {
		return ens, err
	}
	return ens, nil
}

func (self *ENSSimulation) StoreIndexRootHash(indexName []byte, roothash []byte) (err error) {
	sql_add := `INSERT OR REPLACE INTO ens ( indexName, roothash, storeDT ) values(?, ?, CURRENT_TIMESTAMP)`
	stmt, err := self.db.Prepare(sql_add)
	if err != nil {
		return (err)
	}
	defer stmt.Close()

	_, err2 := stmt.Exec(indexName, roothash)
	if err2 != nil {
		return (err2)
	}
	return nil
}

func (self *ENSSimulation) GetIndexRootHash(indexName []byte) (val []byte, err error) {
	sql := `SELECT roothash FROM ens WHERE indexName = $1`
	stmt, err := self.db.Prepare(sql)
	if err != nil {
		return val, err
	}
	defer stmt.Close()

	rows, err := stmt.Query(indexName)
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
