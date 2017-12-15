//test for sql parse pkg

package main

import (
	"fmt"
	"github.com/xwb1989/sqlparser"
	"io"
	"strings"
)

func main() {

// 	sql := `select * from wolkpurchaser where country = "USA"`
	 sql := `select * from wolkpurchaser where name like "Rodney%"`
	fmt.Printf("sql is: %s\n", sql)

	stmt, err := sqlparser.Parse(sql)
	if err != nil {
		fmt.Printf("sqlparser.Parse err: %v\n", err)
		return
	}
	fmt.Printf("what is stmt? %+v\n", stmt)

	switch stmt := stmt.(type) {
	case *sqlparser.Select:
		fmt.Printf("got to Select; %+v ...process here.\n", stmt)
	case *sqlparser.Insert:
		fmt.Printf("got to Insert; %+v ...process here.\n", stmt)

	}

	//Alternative to read many queries from a io.Reader:
	fmt.Printf("\n Now trying to read many many queries from io.Reader: \n")
	sql = "INSERT INTO sampletable1 VALUES (1, 'a'); INSERT INTO sampletable2 VALUES (3, 4);"
	fmt.Printf("sql: %s\n", sql)

	r := strings.NewReader(sql)
	tokens := sqlparser.NewTokenizer(r)
	fmt.Printf("tokens found are: %+v\n", tokens)

	for {
		stmt, err := sqlparser.ParseNext(tokens)
		if err == io.EOF {
			break
		} else {
			fmt.Printf("considering stmt: %+v\n", stmt)
		}
	}

	return
}
