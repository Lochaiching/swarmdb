package casper_test

import (
	"casper"
	"math/big"
	"testing"
	"fmt"
	"os"
	"strings"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/accounts/abi/bind"
	"github.com/ethereum/go-ethereum/ethclient"
	"github.com/ethereum/go-ethereum/rlp"
)

// logout_msg [num, num, bytes]
type LogoutMessage struct {
	ValidatorIndex *big.Int
	Epoch          *big.Int
	Sig            []byte  // 65 byte RSV
}

// vote_msg [num, bytes32, num, num, bytes]
type VoteMessage struct {
	ValidatorIndex *big.Int
	TargetHash     []byte  // 32
	TargetEpoch    *big.Int
	SourceEpoch    *big.Int
	Sig            []byte  // 65 byte RSV
}

func TestCasper(t *testing.T) {
	// GETH RPC INTERFACE
	conn, err := ethclient.Dial("http://localhost:8545")
	if err != nil {
		fmt.Printf("Failed to connect to the Ethereum client: %v\n", err)
		os.Exit(0)
	}
	
	// before running, create an account in geth for this validator to submit votes, shown below
	generic_addr := common.HexToAddress("90fb0de606507e989247797c6a30952cae4d5cbe")
	simplecasper_address := generic_addr  // TODO1: post casper to local geth and replace
	var key = `{"address":"90fb0de606507e989247797c6a30952cae4d5cbe","crypto":{"cipher":"aes-128-ctr","ciphertext":"54396d6ed0335e4b4874cd4440d24eabeca895fcbafb15d310c25c6b1e4bb306","cipherparams":{"iv":"e3a2457cf8420d3072e5adf118d31df8"},"kdf":"scrypt","kdfparams":{"dklen":32,"n":262144,"p":1,"r":8,"salt":"d25987f2f2429e53f51d87eb6474e3f12a67c63603fd860b558657cee19a6ea9"},"mac":"023fc8a29a6e323db43e0c7795d2d59d0c1f295a62cbb9bc625951fca9c385dd"},"id":"dc849ada-c6be-4f12-bfa2-5200ec560c2e","version":3}`

	// SETUP with: abigen --abi casper.abi --pkg main --type simplecasper --out simplecasper.go 
	casper, err := casper.NewSimplecasper(simplecasper_address, conn)
	if err != nil {
		fmt.Printf("Failed to connect to the Ethereum client: %v\n", err)
		os.Exit(0)
	}
	
	// auth is of type *bind.TransactOpts
	auth, err := bind.NewTransactor(strings.NewReader(key), "mdotm")
	if err != nil {
		fmt.Printf("Failed to create authorized transactor: %v", err)
		os.Exit(0)
	}

	// INIT
	epoch_length := big.NewInt(600)   
	withdrawal_delay := big.NewInt(600)
	owner := generic_addr
	sighasher := generic_addr        // TODO1: post this to local geth and replace
	purity_checker := generic_addr   // TODO1: post this to local geth and replace
	base_interest_factor := big.NewInt(4)
	base_penalty_factor := big.NewInt(3)
	min_deposit_size := big.NewInt(100000)
	tx, errT := casper.Init(auth, 
		// Epoch length, delay in epochs for withdrawing
		epoch_length, 
                withdrawal_delay, 
                owner, 
                sighasher,  
                purity_checker,
                // Base interest and base penalty factors
                base_interest_factor,
                base_penalty_factor,
                // Min deposit size (in wei)
                min_deposit_size);
	if errT != nil {
		fmt.Printf("Failed to Init", err)
		os.Exit(0)
	}
	fmt.Printf("Init Tx: %v\n", tx)
	
	// DEPOSIT: Send a deposit to join the validator set
	validation_addr := generic_addr  
	withdrawal_addr := generic_addr  
	tx, errT = casper.Deposit(auth, validation_addr, withdrawal_addr)
	if errT != nil {
		fmt.Printf("Failed to Deposit", err)
		os.Exit(0)
	}
	validator_index := big.NewInt(1)

	// INITIALIZE_EPOCH
	epoch := big.NewInt(1)
	// TODO1: casper.InitializeEpoch(auth, epoch)

	// VOTE
	var v VoteMessage
	v.ValidatorIndex = validator_index
	// v.TargetHash  bytes32
	v.TargetEpoch = big.NewInt(3)
	v.SourceEpoch = big.NewInt(1)
	// TODO2: v.Sig is signed message without the sig
	vote_msg, errM := rlp.EncodeToBytes(v)
	casper.Vote(auth, vote_msg)

	// LOGOUT
	var l LogoutMessage
	// [num - validator_index, num - epoch, bytes - sig]
	l.ValidatorIndex = big.NewInt(1)  // TODO
	l.Epoch = epoch
	// TODO2: l.Sig is signed message without the sig 
	logout_msg, errM := rlp.EncodeToBytes(l)
	if errM != nil {
		fmt.Printf("Failed to generate logout message", err)
		os.Exit(0)
	}
	tx, errT = casper.Logout(auth, logout_msg)
	if errT != nil {
		fmt.Printf("Failed to Logout", err)
		os.Exit(0)
	}

	// SLASH
	var v2 VoteMessage
	v2.ValidatorIndex = validator_index
	// v2.TargetHash  bytes32
	v2.TargetEpoch = big.NewInt(4)
	v2.SourceEpoch = big.NewInt(1)
	// TODO2: v2.Sig is signed message without the sig
	vote_msg2, errM = rlp.EncodeToBytes(v)

	tx, errT = casper.Slash(auth, vote_msg, vote_msg2)
	if errT != nil {
		fmt.Printf("Failed to Slash", err)
		os.Exit(0)
	}


	// WITHDRAW
	tx, errT = casper.Withdraw(auth, validator_index)
	if errT != nil {
		fmt.Printf("Failed to Withdraw", err)
		os.Exit(0)
	}


}
