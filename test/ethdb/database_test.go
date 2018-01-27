// Copyright 2014 The go-ethereum Authors
// This file is part of the go-ethereum library.
//
// The go-ethereum library is free software: you can redistribute it and/or modify
// it under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// The go-ethereum library is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU Lesser General Public License for more details.
//
// You should have received a copy of the GNU Lesser General Public License
// along with the go-ethereum library. If not, see <http://www.gnu.org/licenses/>.

package ethdb_test

import (
	"bytes"
	"fmt"
	"github.com/syndtr/goleveldb/leveldb"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/rlp"
	// "github.com/ethereum/go-ethereum/core"

	// "github.com/syndtr/goleveldb/leveldb/errors"
	// "github.com/syndtr/goleveldb/leveldb/filter"
	// "github.com/syndtr/goleveldb/leveldb/iterator"
	// "github.com/syndtr/goleveldb/leveldb/opt"
	"github.com/syndtr/goleveldb/leveldb/util"
	"encoding/binary"
	"encoding/hex"
	"testing"
	 "os"
	// "github.com/ethereum/go-ethereum/common"
	// "github.com/ethereum/go-ethereum/ethdb"
)

var test_values = []string{"0x6200000000004ba1919248f52fd289907c779b9a0fbebd96844dfa9ca118926d123da69be45e839d0c"}

// encodeBlockNumber encodes a block number as big endian uint64
func encodeBlockNumber(number uint64) []byte {
	enc := make([]byte, 8)
	binary.BigEndian.PutUint64(enc, number)
	return enc
}

func lookup_block(db *leveldb.DB, number uint64, hash []byte, target []byte, limit int) (hit bool, err error) {
	n := encodeBlockNumber(number)

	// header
	headerdata, _ := db.Get(append(append([]byte("h"), n...), hash...), nil)
	header := new(types.Header)
	if err := rlp.Decode(bytes.NewReader(headerdata), &header); err != nil {
	}
	
	// body
	data, err := db.Get(append(append([]byte("b"), n...), hash...), nil)
	if err != nil {
		// return false, err
	}
	body := new(types.Body)
	if err := rlp.Decode(bytes.NewReader(data), &body); err != nil {
		//return false, err
	}
// 	fmt.Printf("BODY: %v\n", body)

	// transactions
	hit = false
	for i, tx := range body.Transactions {
		m := tx.To()
		if m != nil {
			if bytes.Compare(m.Bytes(), target) == 0 {
				if hit == false {
					fmt.Printf("HEADER: %v\n", header)
					hit = true
				} 
				fmt.Printf("GetBodyTx [NIYOGI] %d %s\n", i, tx)
			}
		}
	}

	if hit {
		transfer_topic, _ := hex.DecodeString("ddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef")
		data, _ := db.Get(append(append([]byte("r"), n...), hash...), nil)
		storageReceipts := []*types.ReceiptForStorage{}
		if err := rlp.DecodeBytes(data, &storageReceipts); err != nil {
		} else {
			receipts := make(types.Receipts, len(storageReceipts))
			for i, receipt := range storageReceipts {
				receipts[i] = (*types.Receipt)(receipt)
				if len(receipts[i].Logs) > 0 {
					matched := false
					for _, l := range receipts[i].Logs {
						if bytes.Compare(l.Address.Bytes(), target) == 0 {
							for _, t := range l.Topics {
								if bytes.Compare(t.Bytes(), transfer_topic) == 0 {
									matched = true
								}
							}
						}
					}
					if matched {
						fmt.Printf("  ***** RECEIPT %d Block # %d/%x - %s\n", i, number, hash, receipts[i])
						for _, l := range receipts[i].Logs {
							if bytes.Compare(l.Address.Bytes(), target) == 0 {
								// log_matched := false
								fmt.Printf("     LOG: %x concerning Address %x\n", l.TxHash, l.Address)
								// return fmt.Sprintf(`log: %x %x %x %x %d %x %d`, l.Address, l.Topics, l.Data, l.TxHash, l.TxIndex, l.BlockHash, l.Index)
							}
						}
					} else {
						// fmt.Printf("  ----- RECEIPT %d/%x - %s\n", num, hash, receipts[i])
					}
				}
			}
		}
	}


	if limit > 0 {
		lookup_block(db, number-1, header.ParentHash.Bytes(), target, limit-1)
	}
	return hit,  nil
}

func TestLDB(t *testing.T) {
	file := "/var/www/vhosts/data/geth/chaindata"
	db, err := leveldb.OpenFile(file, nil)
	if err != nil {
		fmt.Printf("err %v\n", err)
		t.Fatal(err)
	}

	// Transfer event
	transfer_topic, err := hex.DecodeString("ddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef")
	if err != nil {
		fmt.Printf("err %v\n", err)
		t.Fatal(err)
	}
	

	// wolkinc.eth
	target, _ := hex.DecodeString("f6b55acbbc49f4524aa48d19281a9a77c54de10f")

	// block 4370301
	hash_start, _ := hex.DecodeString("66e7a3285a7a6580eac6bd3ad98c8e677d0792ab67a9666c6735d1e48f0fda99")
	lookup_block(db, uint64(4370301), hash_start, target, 300)
	os.Exit(0)
/*
 Get the block body for:
 ***** RECEIPT 865316/7ee5e826da92eef93f62087c45f68f1cdee1c8be685be7e5fb8b9486ed3f519e - receipt{med=2b0e9a49d7f8d583f69a3083808b0ccd3e45cf7a5e402c1add86ea892e137071 cgas=50725 bloom=00000000000280000000000000000000000000000000000000000000000000000000000000000000004000000000000000000000000000000000000000800000000000000000000000000008000000000000000000000000000000000000000080000000000000000000000000000000000000000000000000000010000000000000000000000000000000000000000000004000000000000000000000000000000000000000000000002000000000000000000000000000000000000000000000000002000000000000000000000000000000004000000000000000000000000000000000000000000000000000000000000001000000000000000000000000 logs=[log: cb599a6f65d826f7a96ebe884a599f17fffc989b [ddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef 0000000000000000000000004df5698b1b0195fc44fe1d2d6037ce33f215c740 000000000000000000000000299d83de632906b1f532e745c0b458b918b9cec2] 000000000000000000000000000000000000000000000000000000000000000a c2b71c637b4724a75d43ad148c1dcc03c426d080657746ecf1b7d8fb627c75ca 0 7ee5e826da92eef93f62087c45f68f1cdee1c8be685be7e5fb8b9486ed3f519e 0]}
*/
	// the above
	// target, _ := hex.DecodeString("cb599a6f65d826f7a96ebe884a599f17fffc989b")

	// hash, _ := hex.DecodeString("7ee5e826da92eef93f62087c45f68f1cdee1c8be685be7e5fb8b9486ed3f519e")
	// os.Exit(0)
	// USE:  GetBody(db DatabaseReader, hash common.Hash, number uint64) *types.Body ==> THEN you get body.Transactions
	cnt := 0

	iter := db.NewIterator(util.BytesPrefix([]byte("r")), nil)
	for iter.Next() {
		// Remember that the contents of the returned slice should not be modified, and
		// only valid until the next call to Next.
		key := iter.Key()
		value := iter.Value()
		b := key[0:1]
		if bytes.Compare(b, []byte("h")) == 0 && ( len(key) == 10 || len(key)== 42 || len(key) == 41 ) { // HEADER 680000000000000f8583c7fg8401dd12694180e0380222028ada771dcb89b625e3c44c4b9b10e254b870
			num :=  binary.BigEndian.Uint64(key[1:9])
			if len(key) == 41 { // headerPrefix + num (uint64 big endian) + hash -> header
				header := new(types.Header)
				if err := rlp.Decode(bytes.NewReader(value), &header); err != nil {
					fmt.Printf("Invalid block header RLP\n")
					//return nil
				}
				hash := key[9:]
				if false {
					fmt.Printf("HEADER(block# %d, hash %x) = %s", num, hash, header)
				}
				// USE: GetHeader(db DatabaseReader, hash common.Hash, number uint64) *types.Header
				//      key: headerKey(hash common.Hash, number uint64) []byte
				/*  core/types/block.go -  Header represents a block header in the Ethereum blockchain.
				type Header struct {
					ParentHash  common.Hash    `json:"parentHash"       gencodec:"required"`
					UncleHash   common.Hash    `json:"sha3Uncles"       gencodec:"required"`
					Coinbase    common.Address `json:"miner"            gencodec:"required"`
					Root        common.Hash    `json:"stateRoot"        gencodec:"required"`
					TxHash      common.Hash    `json:"transactionsRoot" gencodec:"required"`
					ReceiptHash common.Hash    `json:"receiptsRoot"     gencodec:"required"`
					Bloom       Bloom          `json:"logsBloom"        gencodec:"required"`
					Difficulty  *big.Int       `json:"difficulty"       gencodec:"required"`
					Number      *big.Int       `json:"number"           gencodec:"required"`
					GasLimit    uint64         `json:"gasLimit"         gencodec:"required"`
					GasUsed     uint64         `json:"gasUsed"          gencodec:"required"`
					Time        *big.Int       `json:"timestamp"        gencodec:"required"`
					Extra       []byte         `json:"extraData"        gencodec:"required"`
					MixDigest   common.Hash    `json:"mixHash"          gencodec:"required"`
					Nonce       BlockNonce     `json:"nonce"            gencodec:"required"`
				}
				*/
			} else if len(key) == 10 {
				p := key[9:10] 
				if bytes.Compare(p, []byte("n")) == 0  {
					hash := value
					if false {
						fmt.Printf("HASH(block # %d) => %x (%x)\n", num, hash, p)
					}
				}
				// Simple map: blocknumber => hash
				// numSuffix           = []byte("n") // headerPrefix + num (uint64 big endian) + numSuffix -> hash
				// USE:  GetCanonicalHash(db DatabaseReader, number uint64) common.Hash
			} else if len(key) == 42 {
				p := key[41:42] 
				if bytes.Compare(p, []byte("t")) == 0 {
					hash := key[9:41]
					td := value
					// Simple map: number + hash => TD
					if false {
						fmt.Printf("TD(block # %d, hash: %x) = %x (%x)\n", num, hash, td, p)
					}
					// tdSuffix            = []byte("t") // headerPrefix + num (uint64 big endian) + hash + tdSuffix -> td
					// USE:  GetTd(db DatabaseReader, hash common.Hash, number uint64) *big.Int
				}
			}
		} else if bytes.Compare(b, []byte("b")) == 0 {
			/*
			   Block represents an entire block in the Ethereum blockchain.

			   type Block struct {
			   	header       *Header
			   	uncles       []*Header
			   	transactions Transactions

			   	// caches
			   	hash atomic.Value
			   	size atomic.Value

			   	// Td is used by package core to store the total difficulty
			   	// of the chain up to and including the block.
			   	td *big.Int

			   	// These fields are used by package eth to track
			   	// inter-peer block relay.
			   	ReceivedAt   time.Time
			   	ReceivedFrom interface{}
			   }
			   // Body is a simple (mutable, non-safe) data container for storing and moving a block's data contents (transactions and uncles) together.
			   type Body struct {
			   	Transactions []*Transaction
			   	Uncles       []*Header
			   }

			   // USE:  GetBody(db DatabaseReader, hash common.Hash, number uint64) *types.Body ==> THEN you get body.Transactions
			   //       Key: blockBodyKey(hash common.Hash, number uint64) []byte
			 */
				num := binary.BigEndian.Uint64(key[1:9])
			hash := key[9:41]
			// body := value
			body := new(types.Body)
			if err := rlp.Decode(bytes.NewReader(value), &body); err != nil {
				fmt.Printf("Invalid block body RLP\n")
			} 
			if len(body.Transactions) > 10 {
				fmt.Printf("BODY(num: %d, hash %x) => %d Transactions\n", num, hash, len(body.Transactions))
				for i, tx := range body.Transactions {
					// fmt.Printf("  --- Tx# %d  0x%x: value: %v gas: %v GasPrice: %v Nonce: %v\n", i, tx.Hash(), tx.Value(), tx.Gas(), tx.GasPrice(), tx.Nonce())
					fmt.Printf("  --- Tx# %d  0x%x: %s \n", i, tx.Hash(), tx.String())
				}
			}
			// bodyPrefix          = []byte("b") bodyPrefix + num (uint64 big endian) + hash -> block body
		} else if bytes.Compare(b, []byte("H")) == 0 { // blockHashPrefix + hash -> num (uint64 big endian)
			// 4800001bc5c2f174850f6ef2fa863991d6ced28c9bacd078b775ce808bdab9056a
			// Simple map: hash => blocknumber
			//hash := key[1:33]
			//num := value
			// fmt.Printf("BLOCKNUMBER(hash %x) = %x\n", hash, num)
			// USE: GetBlockNumber(db DatabaseReader, hash common.Hash) uint64
			cnt = cnt + 1
			if cnt % 10000 ==  0 {
				fmt.Printf("[%d]", cnt)
			}
		} else if bytes.Compare(b, []byte("r")) == 0  {
			//num :=  binary.BigEndian.Uint64(key[1:9])
			//hash := key[9:41]

			storageReceipts := []*types.ReceiptForStorage{}
			if err := rlp.DecodeBytes(value, &storageReceipts); err != nil {
				// fmt.Printf("Invalid receipt array RLP\n")
			} 
			// fmt.Printf("RECEIPTS(block# %d, hash %x) = %d \n", num, hash, len(storageReceipts))
			// Keccak256("Transfer(address,address,uint256)") == 0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef
			receipts := make(types.Receipts, len(storageReceipts))
			for i, receipt := range storageReceipts {
				receipts[i] = (*types.Receipt)(receipt)

				if len(receipts[i].Logs) > 0 {
					matched := false
					for _, l := range receipts[i].Logs {
						for _, t := range l.Topics {
							if bytes.Compare(t.Bytes(), transfer_topic) == 0 {
								matched = true
							}
						}
					}
					if matched {
/*						hit, err2 := lookup_block(db, num, hash, target, 1) 
						if err2 != nil {
						} else {
							if hit {
								fmt.Printf("  ***** RECEIPT %d/%x - %s\n", num, hash, receipts[i])
							}
						}
*/							
						
					} else {
						// fmt.Printf("  ----- RECEIPT %d/%x - %s\n", num, hash, receipts[i])
					}

				}
			}
			
			
			// RECEIPTS and LOGS -- blockReceiptsPrefix(t) + num (uint64 big endian) + hash -> block receipts
			// GetBlockReceipts(db DatabaseReader, hash common.Hash, number uint64) types.Receipts
			/*
				// Receipt represents the results of a transaction.
				type Receipt struct {

					// Consensus fields
					PostState         []byte `json:"root"`
					Status            uint   `json:"status"`
					CumulativeGasUsed uint64 `json:"cumulativeGasUsed" gencodec:"required"`
					Bloom             Bloom  `json:"logsBloom"         gencodec:"required"`
					Logs              []*Log `json:"logs"              gencodec:"required"`

					// Implementation fields (don't reorder!)
					TxHash          common.Hash    `json:"transactionHash" gencodec:"required"`
					ContractAddress common.Address `json:"contractAddress"`
					GasUsed         uint64         `json:"gasUsed" gencodec:"required"`
				}
				// Log represents a contract log event. These events are generated by the LOG opcode and
				// stored/indexed by the node.
				type Log struct {
					// Consensus fields:
					// address of the contract that generated the event
					Address common.Address `json:"address" gencodec:"required"`
					// list of topics provided by the contract.
					Topics []common.Hash `json:"topics" gencodec:"required"`
					// supplied by the contract, usually ABI-encoded
					Data []byte `json:"data" gencodec:"required"`

					// Derived fields. These fields are filled in by the node
					// but not secured by consensus.
					// block in which the transaction was included
					BlockNumber uint64 `json:"blockNumber"`
					// hash of the transaction
					TxHash common.Hash `json:"transactionHash" gencodec:"required"`
					// index of the transaction in the block
					TxIndex uint `json:"transactionIndex" gencodec:"required"`
					// hash of the block in which the transaction was included
					BlockHash common.Hash `json:"blockHash"`
					// index of the log in the receipt
					Index uint `json:"logIndex" gencodec:"required"`

					// The Removed field is true if this log was reverted due to a chain reorganisation.
					// You must pay attention to this field if you receive logs through a filter query.
					Removed bool `json:"removed"`
				}
			*/
		} else if bytes.Compare(b, []byte("l")) == 0 { // lookupPrefix + hash -> transaction/receipt lookup metadata
			// TRANSACTION LOOKUP
			// USE: GetTxLookupEntry(db DatabaseReader, hash common.Hash) (common.Hash, uint64, uint64)
			//      retrieves the positional metadata associated with a transaction hash to allow retrieving the transaction or receipt by hash.
			// Load the positional metadata from disk and bail if it fails
			/*
				type Transaction struct {
					data txdata
					// caches
					hash atomic.Value
					size atomic.Value
					from atomic.Value
				}

				type txdata struct {
					AccountNonce uint64          `json:"nonce"    gencodec:"required"`
					Price        *big.Int        `json:"gasPrice" gencodec:"required"`
					GasLimit     uint64          `json:"gas"      gencodec:"required"`
					Recipient    *common.Address `json:"to"       rlp:"nil"` // nil means contract creation
					Amount       *big.Int        `json:"value"    gencodec:"required"`
					Payload      []byte          `json:"input"    gencodec:"required"`

					// Signature values
					V *big.Int `json:"v" gencodec:"required"`
					R *big.Int `json:"r" gencodec:"required"`
					S *big.Int `json:"s" gencodec:"required"`

					// This is only used when marshaling to JSON.
					Hash *common.Hash `json:"hash" rlp:"-"`
				}
			*/
		} else if bytes.Compare(b, []byte("B")) == 0 { // lookupPrefix + hash -> transaction/receipt lookup metadata
			// BLOOM BITS
			// bloomBitsPrefix     = []byte("B") // bloomBitsPrefix + bit (uint16 big endian) + section (uint64 big endian) + hash -> bloom bits
			// GetBloomBits retrieves the compressed bloom bit vector belonging to the given section and bit index from the.
			// USE: GetBloomBits(db DatabaseReader, bit uint, section uint64, head common.Hash) ([]byte, error)
		} else {
			// fmt.Printf("KEY: %x (%d bytes)\n", key, len(key))
		}
	}
	iter.Release()
}
