package swarmdb

import (
	"fmt"
	//"github.com/ethereum/go-ethereum/log"
	"github.com/xwb1989/sqlparser"
	"strconv"
)

//at the moment, only parses a query with a single un-nested where clause, i.e.
//'Select name, age from contacts where email = "rodney@wolk.com"'
func ParseQuery(rawQuery string) (query QueryOption, err error) {

	//fmt.Printf("\nin ParseQuery\n")
	stmt, err := sqlparser.Parse(rawQuery)
	if err != nil {
		fmt.Printf("sqlparser.Parse err\n")
		return query, err
	}

	switch stmt := stmt.(type) {
	case *sqlparser.Select:
		//buf := sqlparser.NewTrackedBuffer(nil)
		//stmt.Format(buf)
		//fmt.Printf("select: %v\n", buf.String())

		query.Type = "Select"
		for _, column := range stmt.SelectExprs {
			//fmt.Printf("select %d: %+v\n", i, sqlparser.String(column)) // stmt.(*sqlparser.Select).SelectExprs)
			var newcolumn Column
			newcolumn.ColumnName = sqlparser.String(column)
			//should somehow get IndexType, ColumnType, Primary from table itself...(not here?)
			query.RequestColumns = append(query.RequestColumns, newcolumn)
		}

		//From
		//fmt.Printf("from 0: %+v \n", sqlparser.String(stmt.From[0]))
		query.Table = sqlparser.String(stmt.From[0])

		//Where & Having
		//fmt.Printf("where or having: %s \n", readable(stmt.Where.Expr))
		if stmt.Where.Type == sqlparser.WhereStr { //Where
			//fmt.Printf("type: %s\n", stmt.Where.Type)
			query.Where, err = parseWhere(stmt.Where.Expr)
			//this is where recursion for nested parentheses should take place
			if err != nil {
				return query, err
			}
		} else if stmt.Where.Type == sqlparser.HavingStr { //Having
			fmt.Printf("type: %s\n", stmt.Where.Type)
			//fill in having
		}

		//GroupBy ([]Expr)
		//for _, g := range stmt.GroupBy {
		//	fmt.Printf("groupby: %s \n", readable(g))
		//}

		//OrderBy
		query.Ascending = 1 //default if nothing?

		//Limit
		return query, nil

	/* Other options inside Select:
	   type Select struct {
	   	Cache       string
	   	Comments    Comments
	   	Distinct    string
	   	Hints       string
	   	SelectExprs SelectExprs
	   	From        TableExprs
	   	Where       *Where
	   	GroupBy     GroupBy
	   	Having      *Where
	   	OrderBy     OrderBy
	   	Limit       *Limit
	   	Lock        string
	   }*/

	case *sqlparser.Insert:
		//for now, 1 row to insert only. still need to figure out multiple rows
		//i.e. INSERT INTO MyTable (id, name) VALUES (1, 'Bob'), (2, 'Peter'), (3, 'Joe')

		query.Type = "Insert"
		query.Ascending = 1 //default
		//fmt.Printf("Action: %s \n", stmt.Action)
		//fmt.Printf("Comments: %+v \n", stmt.Comments)
		//fmt.Printf("Ignore: %s \n", stmt.Ignore)
		query.Table = sqlparser.String(stmt.Table.Name)
		if len(stmt.Rows.(sqlparser.Values)) == 0 {
			return query, fmt.Errorf("in Insert, no values found")
		}
		if len(stmt.Rows.(sqlparser.Values)[0]) != len(stmt.Columns) {
			return query, fmt.Errorf("in Insert, mismatch # of cols & vals")
		}
		insertCells := make(map[string]interface{})
		for i, c := range stmt.Columns {
			col := sqlparser.String(c)
			if _, ok := insertCells[col]; ok {
				return query, fmt.Errorf("in Insert, can't have duplicate col %s", col)
			}
			//only detects string and float. how to do int? does it matter
			value := sqlparser.String(stmt.Rows.(sqlparser.Values)[0][i])
			if isQuoted(value) {
				insertCells[col] = trimQuotes(value)
			} else if isNumeric(value) {
				insertCells[col], err = strconv.ParseFloat(value, 64)
				if err != nil {
					return query, err
				}
			} else {
				return query, fmt.Errorf("in Insert, value %s has unknown type", value)
			}
			//insertCells[col] = trimQuotes(sqlparser.String(stmt.Rows.(sqlparser.Values)[0][i]))
		}
		query.Inserts = append(query.Inserts, Row{Cells: insertCells})
		//fmt.Printf("OnDup: %+v\n", stmt.OnDup)
		//fmt.Printf("Rows: %+v\n", stmt.Rows.(sqlparser.Values))
		//fmt.Printf("Rows: %+v\n", sqlparser.String(stmt.Rows.(sqlparser.Values)))
		//for i, v := range stmt.Rows.(sqlparser.Values)[0] {
		//	fmt.Printf("row: %v %+v\n", i, sqlparser.String(v))
		//}

	case *sqlparser.Update:

		query.Type = "Update"
		//fmt.Printf("Comments: %+v \n", stmt.Comments)
		query.Table = sqlparser.String(stmt.TableExprs[0])
		query.Update = make(map[string]interface{})
		for _, expr := range stmt.Exprs {
			col := sqlparser.String(expr.Name)
			//fmt.Printf("col: %+v\n", col)
			if _, ok := query.Update[col]; ok {
				return query, fmt.Errorf("in Update, can't hcave duplicate col %s", col)
			}
			value := readable(expr.Expr)
			if isQuoted(value) {
				query.Update[col] = trimQuotes(value)
			} else if isNumeric(value) {
				query.Update[col], err = strconv.ParseFloat(value, 64)
				if err != nil {
					return query, err
				}
			} else {
				return query, fmt.Errorf("in Update, value %s has unknown type", value)
			}
			//fmt.Printf("val: %v \n", query.Update[col])
		}

		//Where
		if stmt.Where.Type == sqlparser.WhereStr { //Where

			query.Where, err = parseWhere(stmt.Where.Expr)
			//this is where recursion for nested parentheses should take place
			if err != nil {
				return query, err
			}
			//fmt.Printf("Where: %+v\n", query.Where)

		}
		//what if no Where? throw an error or go ahead and modify all?

		//OrderBy
		query.Ascending = 1 //default if nothing?

		//Limit
		//fmt.Printf("Limit: %v \n", stmt.Limit)
		return query, nil

	case *sqlparser.Delete:
		query.Type = "Delete"
		query.Table = sqlparser.String(stmt.TableExprs[0])
		//fmt.Printf("Comments: %+v \n", stmt.Comments)

		//Targets
		for _, t := range stmt.Targets {
			fmt.Printf("Targets: %s\n", t.Name)
		}

		//Where
		if stmt.Where.Type == sqlparser.WhereStr { //Where
			query.Where, err = parseWhere(stmt.Where.Expr)
			//this is where recursion for nested parentheses should take place
			if err != nil {
				return query, err
			}
			//fmt.Printf("Where: %+v\n", query.Where)
		}
		//what if there is no Where? throw an error or go ahead and modify all?

		//OrderBy
		query.Ascending = 1 //default if nothing?

		//Limit
		//fmt.Printf("Limit: %v \n", stmt.Limit)

		return query, nil

		/* Other Options for type of Query:
		   func (*Union) iStatement()      {}
		   func (*Select) iStatement()     {}
		   func (*Insert) iStatement()     {}
		   func (*Update) iStatement()     {}
		   func (*Delete) iStatement()     {}
		   func (*Set) iStatement()        {}
		   func (*DDL) iStatement()        {}
		   func (*Show) iStatement()       {}
		   func (*Use) iStatement()        {}
		   func (*OtherRead) iStatement()  {}
		   func (*OtherAdmin) iStatement() {}
		*/

	}

	return query, err
}

func parseWhere(expr sqlparser.Expr) (where Where, err error) {

	switch expr := expr.(type) {
	case *sqlparser.OrExpr:
		where.Left = readable(expr.Left)
		where.Right = readable(expr.Right)
		where.Operator = "OR" //should be const
	case *sqlparser.AndExpr:
		where.Left = readable(expr.Left)
		where.Right = readable(expr.Right)
		where.Operator = "AND" //shoud be const
	case *sqlparser.IsExpr:
		where.Right = readable(expr.Expr)
		where.Operator = expr.Operator
	case *sqlparser.BinaryExpr:
		where.Left = readable(expr.Left)
		where.Right = readable(expr.Right)
		where.Operator = expr.Operator
	case *sqlparser.ComparisonExpr:
		where.Left = readable(expr.Left)
		where.Right = readable(expr.Right)
		where.Operator = expr.Operator
	default:
		err = fmt.Errorf("WHERE expression not found")
	}

	where.Right = trimQuotes(where.Right)

	return where, err
}

func trimQuotes(s string) string {
	if len(s) > 0 && s[0] == '\'' {
		s = s[1:]
	}
	if len(s) > 0 && s[len(s)-1] == '\'' {
		s = s[:len(s)-1]
	}
	return s
}

func isQuoted(s string) bool { //string
	if (len(s) > 0) && (s[0] == '\'') && (s[len(s)-1] == '\'') {
		return true
	}
	return false
}

func isNumeric(s string) bool { //float or int
	if _, err := strconv.ParseFloat(s, 64); err == nil {
		return true
	}
	return false
}

func readable(expr sqlparser.Expr) string {
	switch expr := expr.(type) {
	case *sqlparser.OrExpr:
		return fmt.Sprintf("(%s or %s)", readable(expr.Left), readable(expr.Right))
	case *sqlparser.AndExpr:
		return fmt.Sprintf("(%s and %s)", readable(expr.Left), readable(expr.Right))
	case *sqlparser.BinaryExpr:
		return fmt.Sprintf("(%s %s %s)", readable(expr.Left), expr.Operator, readable(expr.Right))
	case *sqlparser.IsExpr:
		return fmt.Sprintf("(%s %s)", readable(expr.Expr), expr.Operator)
	case *sqlparser.ComparisonExpr:
		return fmt.Sprintf("(%s %s %s)", readable(expr.Left), expr.Operator, readable(expr.Right))
	default:
		return sqlparser.String(expr)
	}
}
