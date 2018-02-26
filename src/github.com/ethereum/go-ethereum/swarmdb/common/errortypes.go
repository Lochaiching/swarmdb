//direct copy of error.go - all errors should be common
package common

import (
	"fmt"
)

type SWARMDBError struct {
	message      string
	ErrorCode    int
	ErrorMessage string
}

func (e *SWARMDBError) Error() string {
	return e.message
}

func (e *SWARMDBError) SetError(m string) {
	e.message = m
}

func GenerateSWARMDBError(err error, msg string) (swErr error) {
	if wolkErr, ok := err.(*SWARMDBError); ok {
		return &SWARMDBError{message: msg, ErrorCode: wolkErr.ErrorCode, ErrorMessage: wolkErr.ErrorMessage}
	} else {
		return &SWARMDBError{message: msg}
	}
}

type TableNotExistError struct {
	tableName string
	owner     string
	database  string
}

func (t *TableNotExistError) Error() string {
	return fmt.Sprintf("Table [%s] with Owner [%s] does not exist", t.tableName, t.owner)
}

type KeyNotFoundError struct {
}

func (t *KeyNotFoundError) Error() string {
	return fmt.Sprintf("Key not found")
}

type KeySizeError struct {
}

func (t *KeySizeError) Error() string {
	return fmt.Sprintf("Key size too large")
}

type ValueSizeError struct {
}

func (t *ValueSizeError) Error() string {
	return fmt.Sprintf("Value size too large")
}

type DuplicateKeyError struct {
}

func (t *DuplicateKeyError) Error() string {
	return fmt.Sprintf("Duplicate key error")
}

type NetworkError struct {
}

func (t *NetworkError) Error() string {
	return fmt.Sprintf("Network error")
}

type NoBufferError struct {
}

func (t *NoBufferError) Error() string {
	return fmt.Sprintf("No buffer error")
}

type BufferOverflowError struct {
}

func (t *BufferOverflowError) Error() string {
	return fmt.Sprintf("Buffer overflow error")
}

type RequestFormatError struct {
}

func (t *RequestFormatError) Error() string {
	return fmt.Sprintf("Request format error")
}

type NoColumnError struct {
	tableOwner string
	tableName  string
	columnName string
}

func (t *NoColumnError) Error() string {
	return fmt.Sprintf("No column [%s] in the table [%s] owned by [%s]", t.tableName, t.columnName, t.tableOwner)
}
