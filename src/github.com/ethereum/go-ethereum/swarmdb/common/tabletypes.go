//only types from table.go that interact externally are here. no internal to swarmdb types.
package common

import (
	"fmt"
	"strconv"
)

type Row map[string]interface{}

func NewRow() (r Row) {
	r = make(map[string]interface{})
	return r
}

//assignRowColumnTypes' new version:
func assignRowColumnTypes(columns map[string]Column, rows []Row) ([]Row, error) {

	for _, row := range rows {
		for name, value := range row {
			if c, ok := columns[name]; !ok {
				return rows, &SWARMDBError{message: fmt.Sprintf("[swarmdblib:assignRowColumnTypes] Invalid column %s", name), ErrorCode: 404, ErrorMessage: fmt.Sprintf("Column Does Not Exist in table definition: [%s]", name)}
			} else {
				switch c.ColumnType {
				case CT_INTEGER:
					switch value.(type) {
					case int:
						row[name] = value.(int)
					case float64:
						row[name] = int(value.(float64))
					case string:
						f, err := strconv.ParseFloat(value.(string), 64)
						if err != nil {
							return rows, &SWARMDBError{message: fmt.Sprintf("[swarmdblib:assignRowColumnTypes] TypeConversion Error: value [%v] does not match column type [%v]", value, columns[name].ColumnType), ErrorCode: 427, ErrorMessage: fmt.Sprintf("The value passed in for [%s] cannot be converted to integer type", name)}
						}
						row[name] = int(f)
					default:
						return rows, &SWARMDBError{message: fmt.Sprintf("[swarmdblib:assignRowColumnTypes] TypeConversion Error: value [%v] does not match column type [%v]", value, columns[name].ColumnType), ErrorCode: 427, ErrorMessage: fmt.Sprintf("The value passed in for [%s] is of an unsupported type", name)}
					}
				case CT_STRING:
					switch value.(type) {
					case string:
						row[name] = value.(string)
					case int:
						row[name] = strconv.Itoa(value.(int))
					case float64:
						row[name] = strconv.FormatFloat(value.(float64), 'f', -1, 64)
						//TODO: handle err
					default:
						return rows, &SWARMDBError{message: fmt.Sprintf("[swarmdblib:assignRowColumnTypes] TypeConversion Error: value [%v] does not match column type [%v]", value, columns[name].ColumnType), ErrorCode: 427, ErrorMessage: fmt.Sprintf("The value passed in for [%s] is of an unsupported type", name)}
					}
				case CT_FLOAT:
					switch value.(type) {
					case float64:
						row[name] = value.(float64)
					case int:
						row[name] = float64(value.(int))
					case string:
						f, err := strconv.ParseFloat(value.(string), 64)
						if err != nil {
							return rows, &SWARMDBError{message: fmt.Sprintf("[swarmdblib:assignRowColumnTypes] TypeConversion Error: value [%v] does not match column type [%v]", value, columns[name].ColumnType), ErrorCode: 427, ErrorMessage: fmt.Sprintf("The value passed in for [%s] is of an unsupported type", name)}
						}
						row[name] = f
					default:
						return rows, &SWARMDBError{message: fmt.Sprintf("[swarmdblib:assignRowColumnTypes] TypeConversion Error: value [%v] does not match column type [%v]", value, columns[name].ColumnType), ErrorCode: 427, ErrorMessage: fmt.Sprintf("The value passed in for [%s] is of an unsupported type", name)}
					}
				//case CT_BLOB:
				// TODO: add blob support
				default:
					return rows, &SWARMDBError{message: fmt.Sprintf("[swarmdblib:assignRowColumnTypes] Coltype not found", value, columns[name].ColumnType), ErrorCode: 427, ErrorMessage: fmt.Sprintf("The value passed in for [%s] is of an unsupported type", name)}
				}
			}
		}
	}
	return rows, nil
}

//table.go's version:
/*
func (t *Table) assignRowColumnTypes(rows []Row) ([]Row, error) {

	// fmt.Printf("assignRowColumnTypes: %v\n", t.columns)
	for _, row := range rows {
		for name, value := range row {
			if c, ok := t.columns[name]; ok {
				switch c.columnType {
				case CT_INTEGER:
					switch value.(type) {
					case int:
						row[name] = value.(int)
					case float64:
						row[name] = int(value.(float64))
						log.Debug(fmt.Sprintf("Converting value[%s] from float64 to int => [%d][%s]\n", value, row[name]))
					case string:
						f, err := strconv.ParseFloat(value.(string), 64)
						if err != nil {
							return rows, &SWARMDBError{message: fmt.Sprintf("[table:assignRowColumnTypes] TypeConversion Error: value [%v] does not match column type [%v]", value, t.columns[name].columnType), ErrorCode: 427, ErrorMessage: fmt.Sprintf("The value passed in for [%s] cannot be converted to integer type", name)}
						}
						row[name] = int(f)
					default:
						return rows, &SWARMDBError{message: fmt.Sprintf("[table:assignRowColumnTypes] TypeConversion Error: value [%v] does not match column type [%v]", value, t.columns[name].columnType), ErrorCode: 427, ErrorMessage: fmt.Sprintf("The value passed in for [%s] is of an unsupported type", name)}
					}
				case CT_STRING:
					switch value.(type) {
					case string:
						row[name] = value.(string)
					case int:
						row[name] = strconv.Itoa(value.(int))
					case float64:
						row[name] = strconv.FormatFloat(value.(float64), 'f', -1, 64)
						//TODO: handle err
						log.Debug(fmt.Sprintf("Converting value[%s] from float64 to string => [%s]\n", value, row[name]))
					default:
						return rows, &SWARMDBError{message: fmt.Sprintf("[table:assignRowColumnTypes] TypeConversion Error: value [%v] does not match column type [%v]", value, t.columns[name].columnType), ErrorCode: 427, ErrorMessage: fmt.Sprintf("The value passed in for [%s] is of an unsupported type", name)}
					}
				case CT_FLOAT:
					switch value.(type) {
					case float64:
						row[name] = value.(float64)
					case int:
						row[name] = float64(value.(int))
					case string:
						f, err := strconv.ParseFloat(value.(string), 64)
						if err != nil {
							return rows, &SWARMDBError{message: fmt.Sprintf("[table:assignRowColumnTypes] TypeConversion Error: value [%v] does not match column type [%v]", value, t.columns[name].columnType), ErrorCode: 427, ErrorMessage: fmt.Sprintf("The value passed in for [%s] is of an unsupported type", name)}
						}
						row[name] = f
					default:
						return rows, &SWARMDBError{message: fmt.Sprintf("[table:assignRowColumnTypes] TypeConversion Error: value [%v] does not match column type [%v]", value, t.columns[name].columnType), ErrorCode: 427, ErrorMessage: fmt.Sprintf("The value passed in for [%s] is of an unsupported type", name)}
					}
				//case CT_BLOB:
				// TODO: add blob support
				default:
					return rows, &SWARMDBError{message: fmt.Sprintf("[table:assignRowColumnTypes] Coltype not found", value, t.columns[name].columnType), ErrorCode: 427, ErrorMessage: fmt.Sprintf("The value passed in for [%s] is of an unsupported type", name)}
				}
			} else {
				return rows, &SWARMDBError{message: fmt.Sprintf("[table:assignRowColumnTypes] Invalid column %s", name), ErrorCode: 404, ErrorMessage: fmt.Sprintf("Column Does Not Exist in table definition: [%s]", name)}
			}
		}
	}
	return rows, nil
}
*/
