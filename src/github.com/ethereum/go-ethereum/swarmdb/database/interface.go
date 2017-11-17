package swarmdb

type Database interface{
// Insert + Update
	Put(key []byte, value []byte) error
// Insert only 
	Insert(key []byte, value []byte) error
	Get(key []byte)([]byte, error)
	Delete(key []byte) error
	Close()
	NewBatch() Batch
}

type Batch interface{
	Put(key []byte, value []byte) error
// same as flush
	Execute()error
}
