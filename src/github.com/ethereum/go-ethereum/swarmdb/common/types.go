package common

import (
	"encoding/binary"
	"fmt"
	"math"
)

type Database interface {
	// Insert: adds key-value pair (value is an entire recrod)
	// ok - returns true if new key added
	// Possible Errors: KeySizeError, ValueSizeError, DuplicateKeyError, NetworkError, BufferOverflowError
	Insert(key []byte, value []byte) (bool, error)

	// Put -- inserts/updates key-value pair (value is an entire record)
	// ok - returns true if new key added
	// Possible Errors: KeySizeError, ValueSizeError, NetworkError, BufferOverflowError
	Put(key []byte, value []byte) (bool, error)

	// Get - gets value of key (value is an entire record)
	// ok - returns true if key found, false if not found
	// Possible errors: KeySizeError, NetworkError
	Get(key []byte) ([]byte, bool, error)

	// Delete - deletes key
	// ok - returns true if key found, false if not found
	// Possible errors: KeySizeError, NetworkError, BufferOverflowError
	Delete(key []byte) (bool, error)

	// Start/Flush - any buffered updates will be flushed to SWARM on FlushBuffer
	// ok - returns true if buffer started / flushed
	// Possible errors: NoBufferError, NetworkError
	StartBuffer() (bool, error)
	FlushBuffer() (bool, error)

	// Close - if buffering, then will flush buffer
	// ok - returns true if operation successful
	// Possible errors: NetworkError
	Close() (bool, error)

	// prints what is in memory
	Print()
}

type OrderedDatabase interface {
	Database

	// Seek -- moves cursor to key k
	// ok - returns true if key found, false if not found
	// Possible errors: KeySizeError, NetworkError
	Seek(k []byte /*K*/) (e OrderedDatabaseCursor, ok bool, err error)
}

type OrderedDatabaseCursor interface {
	Next() (k []byte /*K*/, v []byte /*V*/, err error)
	Prev() (k []byte /*K*/, v []byte /*V*/, err error)
}

type KeyType int

const (
	KT_INTEGER = 1
	KT_STRING  = 2
	KT_FLOAT   = 3
	KT_BLOB    = 4
)

func KeyToString(keytype KeyType, k []byte) (out string) {
	switch keytype {
	case KT_BLOB:
		return fmt.Sprintf("%v", k)
	case KT_STRING:
		return fmt.Sprintf("%s", string(k))
	case KT_INTEGER:
		a := binary.BigEndian.Uint64(k)
		return fmt.Sprintf("%d [%x]", a, k)
	case KT_FLOAT:
		bits := binary.BigEndian.Uint64(k)
		f := math.Float64frombits(bits)
		return fmt.Sprintf("%f", f)
	}
	return "unknown key type"
}

type TableNotExistError struct {
}

func (t *TableNotExistError) Error() string {
	return fmt.Sprintf("Table does not exist")
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
