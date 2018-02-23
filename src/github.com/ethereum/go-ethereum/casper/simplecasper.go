// Code generated - DO NOT EDIT.
// This file is a generated binding and any manual changes will be lost.

package casper

import (
	"math/big"
	"strings"
	"github.com/ethereum/go-ethereum/accounts/abi"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/accounts/abi/bind"
)

// SimplecasperABI is the input ABI used to generate the binding from.
const SimplecasperABI = "[{\"constant\":true,\"inputs\":[],\"name\":\"next_dynasty_wei_delta\",\"outputs\":[{\"name\":\"\",\"type\":\"uint256\"}],\"payable\":false,\"stateMutability\":\"view\",\"type\":\"function\"},{\"constant\":true,\"inputs\":[],\"name\":\"reward_factor\",\"outputs\":[{\"name\":\"\",\"type\":\"uint256\"}],\"payable\":false,\"stateMutability\":\"view\",\"type\":\"function\"},{\"constant\":true,\"inputs\":[],\"name\":\"total_destroyed\",\"outputs\":[{\"name\":\"\",\"type\":\"uint256\"}],\"payable\":false,\"stateMutability\":\"view\",\"type\":\"function\"},{\"constant\":true,\"inputs\":[{\"name\":\"\",\"type\":\"uint256\"}],\"name\":\"dynasty_start_epoch\",\"outputs\":[{\"name\":\"\",\"type\":\"uint256\"}],\"payable\":false,\"stateMutability\":\"view\",\"type\":\"function\"},{\"constant\":true,\"inputs\":[{\"name\":\"\",\"type\":\"uint256\"}],\"name\":\"deposit_scale_factor\",\"outputs\":[{\"name\":\"\",\"type\":\"uint256\"}],\"payable\":false,\"stateMutability\":\"view\",\"type\":\"function\"},{\"constant\":false,\"inputs\":[{\"name\":\"validator_index\",\"type\":\"uint256\"}],\"name\":\"withdraw\",\"outputs\":[],\"payable\":false,\"stateMutability\":\"nonpayable\",\"type\":\"function\"},{\"constant\":true,\"inputs\":[],\"name\":\"last_finalized_epoch\",\"outputs\":[{\"name\":\"\",\"type\":\"uint256\"}],\"payable\":false,\"stateMutability\":\"view\",\"type\":\"function\"},{\"constant\":true,\"inputs\":[],\"name\":\"get_total_curdyn_deposits\",\"outputs\":[{\"name\":\"\",\"type\":\"uint256\"}],\"payable\":false,\"stateMutability\":\"view\",\"type\":\"function\"},{\"constant\":false,\"inputs\":[{\"name\":\"logout_msg\",\"type\":\"bytes\"}],\"name\":\"logout\",\"outputs\":[],\"payable\":false,\"stateMutability\":\"nonpayable\",\"type\":\"function\"},{\"constant\":true,\"inputs\":[],\"name\":\"epoch_length\",\"outputs\":[{\"name\":\"\",\"type\":\"uint256\"}],\"payable\":false,\"stateMutability\":\"view\",\"type\":\"function\"},{\"constant\":true,\"inputs\":[],\"name\":\"second_next_dynasty_wei_delta\",\"outputs\":[{\"name\":\"\",\"type\":\"uint256\"}],\"payable\":false,\"stateMutability\":\"view\",\"type\":\"function\"},{\"constant\":true,\"inputs\":[],\"name\":\"expected_source_epoch\",\"outputs\":[{\"name\":\"\",\"type\":\"uint256\"}],\"payable\":false,\"stateMutability\":\"view\",\"type\":\"function\"},{\"constant\":true,\"inputs\":[{\"name\":\"\",\"type\":\"uint256\"}],\"name\":\"votes\",\"outputs\":[{\"name\":\"is_justified\",\"type\":\"bool\"},{\"name\":\"is_finalized\",\"type\":\"bool\"}],\"payable\":false,\"stateMutability\":\"view\",\"type\":\"function\"},{\"constant\":true,\"inputs\":[],\"name\":\"last_justified_epoch\",\"outputs\":[{\"name\":\"\",\"type\":\"uint256\"}],\"payable\":false,\"stateMutability\":\"view\",\"type\":\"function\"},{\"constant\":true,\"inputs\":[],\"name\":\"get_total_prevdyn_deposits\",\"outputs\":[{\"name\":\"\",\"type\":\"uint256\"}],\"payable\":false,\"stateMutability\":\"view\",\"type\":\"function\"},{\"constant\":true,\"inputs\":[],\"name\":\"dynasty\",\"outputs\":[{\"name\":\"\",\"type\":\"uint256\"}],\"payable\":false,\"stateMutability\":\"view\",\"type\":\"function\"},{\"constant\":false,\"inputs\":[{\"name\":\"_epoch_length\",\"type\":\"uint256\"},{\"name\":\"_withdrawal_delay\",\"type\":\"uint256\"},{\"name\":\"_owner\",\"type\":\"address\"},{\"name\":\"_sighasher\",\"type\":\"address\"},{\"name\":\"_purity_checker\",\"type\":\"address\"},{\"name\":\"_base_interest_factor\",\"type\":\"uint256\"},{\"name\":\"_base_penalty_factor\",\"type\":\"uint256\"},{\"name\":\"_min_deposit_size\",\"type\":\"uint256\"}],\"name\":\"init\",\"outputs\":[],\"payable\":false,\"stateMutability\":\"nonpayable\",\"type\":\"function\"},{\"constant\":false,\"inputs\":[{\"name\":\"validator_index\",\"type\":\"uint256\"}],\"name\":\"delete_validator\",\"outputs\":[],\"payable\":false,\"stateMutability\":\"nonpayable\",\"type\":\"function\"},{\"constant\":true,\"inputs\":[],\"name\":\"last_nonvoter_rescale\",\"outputs\":[{\"name\":\"\",\"type\":\"uint256\"}],\"payable\":false,\"stateMutability\":\"view\",\"type\":\"function\"},{\"constant\":true,\"inputs\":[],\"name\":\"total_prevdyn_deposits\",\"outputs\":[{\"name\":\"\",\"type\":\"uint256\"}],\"payable\":false,\"stateMutability\":\"view\",\"type\":\"function\"},{\"constant\":true,\"inputs\":[],\"name\":\"current_epoch\",\"outputs\":[{\"name\":\"\",\"type\":\"uint256\"}],\"payable\":false,\"stateMutability\":\"view\",\"type\":\"function\"},{\"constant\":true,\"inputs\":[],\"name\":\"get_main_hash_voted_frac\",\"outputs\":[{\"name\":\"\",\"type\":\"uint256\"}],\"payable\":false,\"stateMutability\":\"view\",\"type\":\"function\"},{\"constant\":true,\"inputs\":[],\"name\":\"main_hash_justified\",\"outputs\":[{\"name\":\"\",\"type\":\"bool\"}],\"payable\":false,\"stateMutability\":\"view\",\"type\":\"function\"},{\"constant\":true,\"inputs\":[],\"name\":\"total_curdyn_deposits\",\"outputs\":[{\"name\":\"\",\"type\":\"uint256\"}],\"payable\":false,\"stateMutability\":\"view\",\"type\":\"function\"},{\"constant\":true,\"inputs\":[],\"name\":\"get_recommended_source_epoch\",\"outputs\":[{\"name\":\"\",\"type\":\"uint256\"}],\"payable\":false,\"stateMutability\":\"view\",\"type\":\"function\"},{\"constant\":true,\"inputs\":[{\"name\":\"\",\"type\":\"uint256\"}],\"name\":\"dynasty_in_epoch\",\"outputs\":[{\"name\":\"\",\"type\":\"uint256\"}],\"payable\":false,\"stateMutability\":\"view\",\"type\":\"function\"},{\"constant\":true,\"inputs\":[],\"name\":\"get_recommended_target_hash\",\"outputs\":[{\"name\":\"\",\"type\":\"bytes32\"}],\"payable\":false,\"stateMutability\":\"view\",\"type\":\"function\"},{\"constant\":false,\"inputs\":[{\"name\":\"vote_msg_1\",\"type\":\"bytes\"},{\"name\":\"vote_msg_2\",\"type\":\"bytes\"}],\"name\":\"slash\",\"outputs\":[],\"payable\":false,\"stateMutability\":\"nonpayable\",\"type\":\"function\"},{\"constant\":true,\"inputs\":[],\"name\":\"nextValidatorIndex\",\"outputs\":[{\"name\":\"\",\"type\":\"uint256\"}],\"payable\":false,\"stateMutability\":\"view\",\"type\":\"function\"},{\"constant\":true,\"inputs\":[{\"name\":\"validator_index\",\"type\":\"uint256\"}],\"name\":\"get_deposit_size\",\"outputs\":[{\"name\":\"\",\"type\":\"uint256\"}],\"payable\":false,\"stateMutability\":\"view\",\"type\":\"function\"},{\"constant\":true,\"inputs\":[],\"name\":\"last_voter_rescale\",\"outputs\":[{\"name\":\"\",\"type\":\"uint256\"}],\"payable\":false,\"stateMutability\":\"view\",\"type\":\"function\"},{\"constant\":false,\"inputs\":[{\"name\":\"vote_msg\",\"type\":\"bytes\"}],\"name\":\"vote\",\"outputs\":[],\"payable\":false,\"stateMutability\":\"nonpayable\",\"type\":\"function\"},{\"constant\":true,\"inputs\":[],\"name\":\"withdrawal_delay\",\"outputs\":[{\"name\":\"\",\"type\":\"uint256\"}],\"payable\":false,\"stateMutability\":\"view\",\"type\":\"function\"},{\"constant\":false,\"inputs\":[{\"name\":\"validation_addr\",\"type\":\"address\"},{\"name\":\"withdrawal_addr\",\"type\":\"address\"}],\"name\":\"deposit\",\"outputs\":[],\"payable\":true,\"stateMutability\":\"payable\",\"type\":\"function\"}]"

// Simplecasper is an auto generated Go binding around an Ethereum contract.
type Simplecasper struct {
	SimplecasperCaller     // Read-only binding to the contract
	SimplecasperTransactor // Write-only binding to the contract
	SimplecasperFilterer   // Log filterer for contract events
}

// SimplecasperCaller is an auto generated read-only Go binding around an Ethereum contract.
type SimplecasperCaller struct {
	contract *bind.BoundContract // Generic contract wrapper for the low level calls
}

// SimplecasperTransactor is an auto generated write-only Go binding around an Ethereum contract.
type SimplecasperTransactor struct {
	contract *bind.BoundContract // Generic contract wrapper for the low level calls
}

// SimplecasperFilterer is an auto generated log filtering Go binding around an Ethereum contract events.
type SimplecasperFilterer struct {
	contract *bind.BoundContract // Generic contract wrapper for the low level calls
}

// SimplecasperSession is an auto generated Go binding around an Ethereum contract,
// with pre-set call and transact options.
type SimplecasperSession struct {
	Contract     *Simplecasper     // Generic contract binding to set the session for
	CallOpts     bind.CallOpts     // Call options to use throughout this session
	TransactOpts bind.TransactOpts // Transaction auth options to use throughout this session
}

// SimplecasperCallerSession is an auto generated read-only Go binding around an Ethereum contract,
// with pre-set call options.
type SimplecasperCallerSession struct {
	Contract *SimplecasperCaller // Generic contract caller binding to set the session for
	CallOpts bind.CallOpts       // Call options to use throughout this session
}

// SimplecasperTransactorSession is an auto generated write-only Go binding around an Ethereum contract,
// with pre-set transact options.
type SimplecasperTransactorSession struct {
	Contract     *SimplecasperTransactor // Generic contract transactor binding to set the session for
	TransactOpts bind.TransactOpts       // Transaction auth options to use throughout this session
}

// SimplecasperRaw is an auto generated low-level Go binding around an Ethereum contract.
type SimplecasperRaw struct {
	Contract *Simplecasper // Generic contract binding to access the raw methods on
}

// SimplecasperCallerRaw is an auto generated low-level read-only Go binding around an Ethereum contract.
type SimplecasperCallerRaw struct {
	Contract *SimplecasperCaller // Generic read-only contract binding to access the raw methods on
}

// SimplecasperTransactorRaw is an auto generated low-level write-only Go binding around an Ethereum contract.
type SimplecasperTransactorRaw struct {
	Contract *SimplecasperTransactor // Generic write-only contract binding to access the raw methods on
}

// NewSimplecasper creates a new instance of Simplecasper, bound to a specific deployed contract.
func NewSimplecasper(address common.Address, backend bind.ContractBackend) (*Simplecasper, error) {
	contract, err := bindSimplecasper(address, backend, backend, backend)
	if err != nil {
		return nil, err
	}
	return &Simplecasper{SimplecasperCaller: SimplecasperCaller{contract: contract}, SimplecasperTransactor: SimplecasperTransactor{contract: contract}, SimplecasperFilterer: SimplecasperFilterer{contract: contract}}, nil
}

// NewSimplecasperCaller creates a new read-only instance of Simplecasper, bound to a specific deployed contract.
func NewSimplecasperCaller(address common.Address, caller bind.ContractCaller) (*SimplecasperCaller, error) {
	contract, err := bindSimplecasper(address, caller, nil, nil)
	if err != nil {
		return nil, err
	}
	return &SimplecasperCaller{contract: contract}, nil
}

// NewSimplecasperTransactor creates a new write-only instance of Simplecasper, bound to a specific deployed contract.
func NewSimplecasperTransactor(address common.Address, transactor bind.ContractTransactor) (*SimplecasperTransactor, error) {
	contract, err := bindSimplecasper(address, nil, transactor, nil)
	if err != nil {
		return nil, err
	}
	return &SimplecasperTransactor{contract: contract}, nil
}

// NewSimplecasperFilterer creates a new log filterer instance of Simplecasper, bound to a specific deployed contract.
func NewSimplecasperFilterer(address common.Address, filterer bind.ContractFilterer) (*SimplecasperFilterer, error) {
	contract, err := bindSimplecasper(address, nil, nil, filterer)
	if err != nil {
		return nil, err
	}
	return &SimplecasperFilterer{contract: contract}, nil
}

// bindSimplecasper binds a generic wrapper to an already deployed contract.
func bindSimplecasper(address common.Address, caller bind.ContractCaller, transactor bind.ContractTransactor, filterer bind.ContractFilterer) (*bind.BoundContract, error) {
	parsed, err := abi.JSON(strings.NewReader(SimplecasperABI))
	if err != nil {
		return nil, err
	}
	return bind.NewBoundContract(address, parsed, caller, transactor, filterer), nil
}

// Call invokes the (constant) contract method with params as input values and
// sets the output to result. The result type might be a single field for simple
// returns, a slice of interfaces for anonymous returns and a struct for named
// returns.
func (_Simplecasper *SimplecasperRaw) Call(opts *bind.CallOpts, result interface{}, method string, params ...interface{}) error {
	return _Simplecasper.Contract.SimplecasperCaller.contract.Call(opts, result, method, params...)
}

// Transfer initiates a plain transaction to move funds to the contract, calling
// its default method if one is available.
func (_Simplecasper *SimplecasperRaw) Transfer(opts *bind.TransactOpts) (*types.Transaction, error) {
	return _Simplecasper.Contract.SimplecasperTransactor.contract.Transfer(opts)
}

// Transact invokes the (paid) contract method with params as input values.
func (_Simplecasper *SimplecasperRaw) Transact(opts *bind.TransactOpts, method string, params ...interface{}) (*types.Transaction, error) {
	return _Simplecasper.Contract.SimplecasperTransactor.contract.Transact(opts, method, params...)
}

// Call invokes the (constant) contract method with params as input values and
// sets the output to result. The result type might be a single field for simple
// returns, a slice of interfaces for anonymous returns and a struct for named
// returns.
func (_Simplecasper *SimplecasperCallerRaw) Call(opts *bind.CallOpts, result interface{}, method string, params ...interface{}) error {
	return _Simplecasper.Contract.contract.Call(opts, result, method, params...)
}

// Transfer initiates a plain transaction to move funds to the contract, calling
// its default method if one is available.
func (_Simplecasper *SimplecasperTransactorRaw) Transfer(opts *bind.TransactOpts) (*types.Transaction, error) {
	return _Simplecasper.Contract.contract.Transfer(opts)
}

// Transact invokes the (paid) contract method with params as input values.
func (_Simplecasper *SimplecasperTransactorRaw) Transact(opts *bind.TransactOpts, method string, params ...interface{}) (*types.Transaction, error) {
	return _Simplecasper.Contract.contract.Transact(opts, method, params...)
}

// Current_epoch is a free data retrieval call binding the contract method 0x9372b4e4.
//
// Solidity: function current_epoch() constant returns(uint256)
func (_Simplecasper *SimplecasperCaller) Current_epoch(opts *bind.CallOpts) (*big.Int, error) {
	var (
		ret0 = new(*big.Int)
	)
	out := ret0
	err := _Simplecasper.contract.Call(opts, out, "current_epoch")
	return *ret0, err
}

// Current_epoch is a free data retrieval call binding the contract method 0x9372b4e4.
//
// Solidity: function current_epoch() constant returns(uint256)
func (_Simplecasper *SimplecasperSession) Current_epoch() (*big.Int, error) {
	return _Simplecasper.Contract.Current_epoch(&_Simplecasper.CallOpts)
}

// Current_epoch is a free data retrieval call binding the contract method 0x9372b4e4.
//
// Solidity: function current_epoch() constant returns(uint256)
func (_Simplecasper *SimplecasperCallerSession) Current_epoch() (*big.Int, error) {
	return _Simplecasper.Contract.Current_epoch(&_Simplecasper.CallOpts)
}

// Deposit_scale_factor is a free data retrieval call binding the contract method 0x1b89212e.
//
// Solidity: function deposit_scale_factor( uint256) constant returns(uint256)
func (_Simplecasper *SimplecasperCaller) Deposit_scale_factor(opts *bind.CallOpts, arg0 *big.Int) (*big.Int, error) {
	var (
		ret0 = new(*big.Int)
	)
	out := ret0
	err := _Simplecasper.contract.Call(opts, out, "deposit_scale_factor", arg0)
	return *ret0, err
}

// Deposit_scale_factor is a free data retrieval call binding the contract method 0x1b89212e.
//
// Solidity: function deposit_scale_factor( uint256) constant returns(uint256)
func (_Simplecasper *SimplecasperSession) Deposit_scale_factor(arg0 *big.Int) (*big.Int, error) {
	return _Simplecasper.Contract.Deposit_scale_factor(&_Simplecasper.CallOpts, arg0)
}

// Deposit_scale_factor is a free data retrieval call binding the contract method 0x1b89212e.
//
// Solidity: function deposit_scale_factor( uint256) constant returns(uint256)
func (_Simplecasper *SimplecasperCallerSession) Deposit_scale_factor(arg0 *big.Int) (*big.Int, error) {
	return _Simplecasper.Contract.Deposit_scale_factor(&_Simplecasper.CallOpts, arg0)
}

// Dynasty is a free data retrieval call binding the contract method 0x7060054d.
//
// Solidity: function dynasty() constant returns(uint256)
func (_Simplecasper *SimplecasperCaller) Dynasty(opts *bind.CallOpts) (*big.Int, error) {
	var (
		ret0 = new(*big.Int)
	)
	out := ret0
	err := _Simplecasper.contract.Call(opts, out, "dynasty")
	return *ret0, err
}

// Dynasty is a free data retrieval call binding the contract method 0x7060054d.
//
// Solidity: function dynasty() constant returns(uint256)
func (_Simplecasper *SimplecasperSession) Dynasty() (*big.Int, error) {
	return _Simplecasper.Contract.Dynasty(&_Simplecasper.CallOpts)
}

// Dynasty is a free data retrieval call binding the contract method 0x7060054d.
//
// Solidity: function dynasty() constant returns(uint256)
func (_Simplecasper *SimplecasperCallerSession) Dynasty() (*big.Int, error) {
	return _Simplecasper.Contract.Dynasty(&_Simplecasper.CallOpts)
}

// Dynasty_in_epoch is a free data retrieval call binding the contract method 0xb8c8e8d1.
//
// Solidity: function dynasty_in_epoch( uint256) constant returns(uint256)
func (_Simplecasper *SimplecasperCaller) Dynasty_in_epoch(opts *bind.CallOpts, arg0 *big.Int) (*big.Int, error) {
	var (
		ret0 = new(*big.Int)
	)
	out := ret0
	err := _Simplecasper.contract.Call(opts, out, "dynasty_in_epoch", arg0)
	return *ret0, err
}

// Dynasty_in_epoch is a free data retrieval call binding the contract method 0xb8c8e8d1.
//
// Solidity: function dynasty_in_epoch( uint256) constant returns(uint256)
func (_Simplecasper *SimplecasperSession) Dynasty_in_epoch(arg0 *big.Int) (*big.Int, error) {
	return _Simplecasper.Contract.Dynasty_in_epoch(&_Simplecasper.CallOpts, arg0)
}

// Dynasty_in_epoch is a free data retrieval call binding the contract method 0xb8c8e8d1.
//
// Solidity: function dynasty_in_epoch( uint256) constant returns(uint256)
func (_Simplecasper *SimplecasperCallerSession) Dynasty_in_epoch(arg0 *big.Int) (*big.Int, error) {
	return _Simplecasper.Contract.Dynasty_in_epoch(&_Simplecasper.CallOpts, arg0)
}

// Dynasty_start_epoch is a free data retrieval call binding the contract method 0x0f9e163b.
//
// Solidity: function dynasty_start_epoch( uint256) constant returns(uint256)
func (_Simplecasper *SimplecasperCaller) Dynasty_start_epoch(opts *bind.CallOpts, arg0 *big.Int) (*big.Int, error) {
	var (
		ret0 = new(*big.Int)
	)
	out := ret0
	err := _Simplecasper.contract.Call(opts, out, "dynasty_start_epoch", arg0)
	return *ret0, err
}

// Dynasty_start_epoch is a free data retrieval call binding the contract method 0x0f9e163b.
//
// Solidity: function dynasty_start_epoch( uint256) constant returns(uint256)
func (_Simplecasper *SimplecasperSession) Dynasty_start_epoch(arg0 *big.Int) (*big.Int, error) {
	return _Simplecasper.Contract.Dynasty_start_epoch(&_Simplecasper.CallOpts, arg0)
}

// Dynasty_start_epoch is a free data retrieval call binding the contract method 0x0f9e163b.
//
// Solidity: function dynasty_start_epoch( uint256) constant returns(uint256)
func (_Simplecasper *SimplecasperCallerSession) Dynasty_start_epoch(arg0 *big.Int) (*big.Int, error) {
	return _Simplecasper.Contract.Dynasty_start_epoch(&_Simplecasper.CallOpts, arg0)
}

// Epoch_length is a free data retrieval call binding the contract method 0x4231bfe1.
//
// Solidity: function epoch_length() constant returns(uint256)
func (_Simplecasper *SimplecasperCaller) Epoch_length(opts *bind.CallOpts) (*big.Int, error) {
	var (
		ret0 = new(*big.Int)
	)
	out := ret0
	err := _Simplecasper.contract.Call(opts, out, "epoch_length")
	return *ret0, err
}

// Epoch_length is a free data retrieval call binding the contract method 0x4231bfe1.
//
// Solidity: function epoch_length() constant returns(uint256)
func (_Simplecasper *SimplecasperSession) Epoch_length() (*big.Int, error) {
	return _Simplecasper.Contract.Epoch_length(&_Simplecasper.CallOpts)
}

// Epoch_length is a free data retrieval call binding the contract method 0x4231bfe1.
//
// Solidity: function epoch_length() constant returns(uint256)
func (_Simplecasper *SimplecasperCallerSession) Epoch_length() (*big.Int, error) {
	return _Simplecasper.Contract.Epoch_length(&_Simplecasper.CallOpts)
}

// Expected_source_epoch is a free data retrieval call binding the contract method 0x5b03544a.
//
// Solidity: function expected_source_epoch() constant returns(uint256)
func (_Simplecasper *SimplecasperCaller) Expected_source_epoch(opts *bind.CallOpts) (*big.Int, error) {
	var (
		ret0 = new(*big.Int)
	)
	out := ret0
	err := _Simplecasper.contract.Call(opts, out, "expected_source_epoch")
	return *ret0, err
}

// Expected_source_epoch is a free data retrieval call binding the contract method 0x5b03544a.
//
// Solidity: function expected_source_epoch() constant returns(uint256)
func (_Simplecasper *SimplecasperSession) Expected_source_epoch() (*big.Int, error) {
	return _Simplecasper.Contract.Expected_source_epoch(&_Simplecasper.CallOpts)
}

// Expected_source_epoch is a free data retrieval call binding the contract method 0x5b03544a.
//
// Solidity: function expected_source_epoch() constant returns(uint256)
func (_Simplecasper *SimplecasperCallerSession) Expected_source_epoch() (*big.Int, error) {
	return _Simplecasper.Contract.Expected_source_epoch(&_Simplecasper.CallOpts)
}

// Get_deposit_size is a free data retrieval call binding the contract method 0xcdefe170.
//
// Solidity: function get_deposit_size(validator_index uint256) constant returns(uint256)
func (_Simplecasper *SimplecasperCaller) Get_deposit_size(opts *bind.CallOpts, validator_index *big.Int) (*big.Int, error) {
	var (
		ret0 = new(*big.Int)
	)
	out := ret0
	err := _Simplecasper.contract.Call(opts, out, "get_deposit_size", validator_index)
	return *ret0, err
}

// Get_deposit_size is a free data retrieval call binding the contract method 0xcdefe170.
//
// Solidity: function get_deposit_size(validator_index uint256) constant returns(uint256)
func (_Simplecasper *SimplecasperSession) Get_deposit_size(validator_index *big.Int) (*big.Int, error) {
	return _Simplecasper.Contract.Get_deposit_size(&_Simplecasper.CallOpts, validator_index)
}

// Get_deposit_size is a free data retrieval call binding the contract method 0xcdefe170.
//
// Solidity: function get_deposit_size(validator_index uint256) constant returns(uint256)
func (_Simplecasper *SimplecasperCallerSession) Get_deposit_size(validator_index *big.Int) (*big.Int, error) {
	return _Simplecasper.Contract.Get_deposit_size(&_Simplecasper.CallOpts, validator_index)
}

// Get_main_hash_voted_frac is a free data retrieval call binding the contract method 0x94113f78.
//
// Solidity: function get_main_hash_voted_frac() constant returns(uint256)
func (_Simplecasper *SimplecasperCaller) Get_main_hash_voted_frac(opts *bind.CallOpts) (*big.Int, error) {
	var (
		ret0 = new(*big.Int)
	)
	out := ret0
	err := _Simplecasper.contract.Call(opts, out, "get_main_hash_voted_frac")
	return *ret0, err
}

// Get_main_hash_voted_frac is a free data retrieval call binding the contract method 0x94113f78.
//
// Solidity: function get_main_hash_voted_frac() constant returns(uint256)
func (_Simplecasper *SimplecasperSession) Get_main_hash_voted_frac() (*big.Int, error) {
	return _Simplecasper.Contract.Get_main_hash_voted_frac(&_Simplecasper.CallOpts)
}

// Get_main_hash_voted_frac is a free data retrieval call binding the contract method 0x94113f78.
//
// Solidity: function get_main_hash_voted_frac() constant returns(uint256)
func (_Simplecasper *SimplecasperCallerSession) Get_main_hash_voted_frac() (*big.Int, error) {
	return _Simplecasper.Contract.Get_main_hash_voted_frac(&_Simplecasper.CallOpts)
}

// Get_recommended_source_epoch is a free data retrieval call binding the contract method 0xb2ae3f50.
//
// Solidity: function get_recommended_source_epoch() constant returns(uint256)
func (_Simplecasper *SimplecasperCaller) Get_recommended_source_epoch(opts *bind.CallOpts) (*big.Int, error) {
	var (
		ret0 = new(*big.Int)
	)
	out := ret0
	err := _Simplecasper.contract.Call(opts, out, "get_recommended_source_epoch")
	return *ret0, err
}

// Get_recommended_source_epoch is a free data retrieval call binding the contract method 0xb2ae3f50.
//
// Solidity: function get_recommended_source_epoch() constant returns(uint256)
func (_Simplecasper *SimplecasperSession) Get_recommended_source_epoch() (*big.Int, error) {
	return _Simplecasper.Contract.Get_recommended_source_epoch(&_Simplecasper.CallOpts)
}

// Get_recommended_source_epoch is a free data retrieval call binding the contract method 0xb2ae3f50.
//
// Solidity: function get_recommended_source_epoch() constant returns(uint256)
func (_Simplecasper *SimplecasperCallerSession) Get_recommended_source_epoch() (*big.Int, error) {
	return _Simplecasper.Contract.Get_recommended_source_epoch(&_Simplecasper.CallOpts)
}

// Get_recommended_target_hash is a free data retrieval call binding the contract method 0xcadbbfc1.
//
// Solidity: function get_recommended_target_hash() constant returns(bytes32)
func (_Simplecasper *SimplecasperCaller) Get_recommended_target_hash(opts *bind.CallOpts) ([32]byte, error) {
	var (
		ret0 = new([32]byte)
	)
	out := ret0
	err := _Simplecasper.contract.Call(opts, out, "get_recommended_target_hash")
	return *ret0, err
}

// Get_recommended_target_hash is a free data retrieval call binding the contract method 0xcadbbfc1.
//
// Solidity: function get_recommended_target_hash() constant returns(bytes32)
func (_Simplecasper *SimplecasperSession) Get_recommended_target_hash() ([32]byte, error) {
	return _Simplecasper.Contract.Get_recommended_target_hash(&_Simplecasper.CallOpts)
}

// Get_recommended_target_hash is a free data retrieval call binding the contract method 0xcadbbfc1.
//
// Solidity: function get_recommended_target_hash() constant returns(bytes32)
func (_Simplecasper *SimplecasperCallerSession) Get_recommended_target_hash() ([32]byte, error) {
	return _Simplecasper.Contract.Get_recommended_target_hash(&_Simplecasper.CallOpts)
}

// Get_total_curdyn_deposits is a free data retrieval call binding the contract method 0x3b812ec3.
//
// Solidity: function get_total_curdyn_deposits() constant returns(uint256)
func (_Simplecasper *SimplecasperCaller) Get_total_curdyn_deposits(opts *bind.CallOpts) (*big.Int, error) {
	var (
		ret0 = new(*big.Int)
	)
	out := ret0
	err := _Simplecasper.contract.Call(opts, out, "get_total_curdyn_deposits")
	return *ret0, err
}

// Get_total_curdyn_deposits is a free data retrieval call binding the contract method 0x3b812ec3.
//
// Solidity: function get_total_curdyn_deposits() constant returns(uint256)
func (_Simplecasper *SimplecasperSession) Get_total_curdyn_deposits() (*big.Int, error) {
	return _Simplecasper.Contract.Get_total_curdyn_deposits(&_Simplecasper.CallOpts)
}

// Get_total_curdyn_deposits is a free data retrieval call binding the contract method 0x3b812ec3.
//
// Solidity: function get_total_curdyn_deposits() constant returns(uint256)
func (_Simplecasper *SimplecasperCallerSession) Get_total_curdyn_deposits() (*big.Int, error) {
	return _Simplecasper.Contract.Get_total_curdyn_deposits(&_Simplecasper.CallOpts)
}

// Get_total_prevdyn_deposits is a free data retrieval call binding the contract method 0x679dea62.
//
// Solidity: function get_total_prevdyn_deposits() constant returns(uint256)
func (_Simplecasper *SimplecasperCaller) Get_total_prevdyn_deposits(opts *bind.CallOpts) (*big.Int, error) {
	var (
		ret0 = new(*big.Int)
	)
	out := ret0
	err := _Simplecasper.contract.Call(opts, out, "get_total_prevdyn_deposits")
	return *ret0, err
}

// Get_total_prevdyn_deposits is a free data retrieval call binding the contract method 0x679dea62.
//
// Solidity: function get_total_prevdyn_deposits() constant returns(uint256)
func (_Simplecasper *SimplecasperSession) Get_total_prevdyn_deposits() (*big.Int, error) {
	return _Simplecasper.Contract.Get_total_prevdyn_deposits(&_Simplecasper.CallOpts)
}

// Get_total_prevdyn_deposits is a free data retrieval call binding the contract method 0x679dea62.
//
// Solidity: function get_total_prevdyn_deposits() constant returns(uint256)
func (_Simplecasper *SimplecasperCallerSession) Get_total_prevdyn_deposits() (*big.Int, error) {
	return _Simplecasper.Contract.Get_total_prevdyn_deposits(&_Simplecasper.CallOpts)
}

// Last_finalized_epoch is a free data retrieval call binding the contract method 0x2eff8759.
//
// Solidity: function last_finalized_epoch() constant returns(uint256)
func (_Simplecasper *SimplecasperCaller) Last_finalized_epoch(opts *bind.CallOpts) (*big.Int, error) {
	var (
		ret0 = new(*big.Int)
	)
	out := ret0
	err := _Simplecasper.contract.Call(opts, out, "last_finalized_epoch")
	return *ret0, err
}

// Last_finalized_epoch is a free data retrieval call binding the contract method 0x2eff8759.
//
// Solidity: function last_finalized_epoch() constant returns(uint256)
func (_Simplecasper *SimplecasperSession) Last_finalized_epoch() (*big.Int, error) {
	return _Simplecasper.Contract.Last_finalized_epoch(&_Simplecasper.CallOpts)
}

// Last_finalized_epoch is a free data retrieval call binding the contract method 0x2eff8759.
//
// Solidity: function last_finalized_epoch() constant returns(uint256)
func (_Simplecasper *SimplecasperCallerSession) Last_finalized_epoch() (*big.Int, error) {
	return _Simplecasper.Contract.Last_finalized_epoch(&_Simplecasper.CallOpts)
}

// Last_justified_epoch is a free data retrieval call binding the contract method 0x5f611650.
//
// Solidity: function last_justified_epoch() constant returns(uint256)
func (_Simplecasper *SimplecasperCaller) Last_justified_epoch(opts *bind.CallOpts) (*big.Int, error) {
	var (
		ret0 = new(*big.Int)
	)
	out := ret0
	err := _Simplecasper.contract.Call(opts, out, "last_justified_epoch")
	return *ret0, err
}

// Last_justified_epoch is a free data retrieval call binding the contract method 0x5f611650.
//
// Solidity: function last_justified_epoch() constant returns(uint256)
func (_Simplecasper *SimplecasperSession) Last_justified_epoch() (*big.Int, error) {
	return _Simplecasper.Contract.Last_justified_epoch(&_Simplecasper.CallOpts)
}

// Last_justified_epoch is a free data retrieval call binding the contract method 0x5f611650.
//
// Solidity: function last_justified_epoch() constant returns(uint256)
func (_Simplecasper *SimplecasperCallerSession) Last_justified_epoch() (*big.Int, error) {
	return _Simplecasper.Contract.Last_justified_epoch(&_Simplecasper.CallOpts)
}

// Last_nonvoter_rescale is a free data retrieval call binding the contract method 0x8a484407.
//
// Solidity: function last_nonvoter_rescale() constant returns(uint256)
func (_Simplecasper *SimplecasperCaller) Last_nonvoter_rescale(opts *bind.CallOpts) (*big.Int, error) {
	var (
		ret0 = new(*big.Int)
	)
	out := ret0
	err := _Simplecasper.contract.Call(opts, out, "last_nonvoter_rescale")
	return *ret0, err
}

// Last_nonvoter_rescale is a free data retrieval call binding the contract method 0x8a484407.
//
// Solidity: function last_nonvoter_rescale() constant returns(uint256)
func (_Simplecasper *SimplecasperSession) Last_nonvoter_rescale() (*big.Int, error) {
	return _Simplecasper.Contract.Last_nonvoter_rescale(&_Simplecasper.CallOpts)
}

// Last_nonvoter_rescale is a free data retrieval call binding the contract method 0x8a484407.
//
// Solidity: function last_nonvoter_rescale() constant returns(uint256)
func (_Simplecasper *SimplecasperCallerSession) Last_nonvoter_rescale() (*big.Int, error) {
	return _Simplecasper.Contract.Last_nonvoter_rescale(&_Simplecasper.CallOpts)
}

// Last_voter_rescale is a free data retrieval call binding the contract method 0xe6b57366.
//
// Solidity: function last_voter_rescale() constant returns(uint256)
func (_Simplecasper *SimplecasperCaller) Last_voter_rescale(opts *bind.CallOpts) (*big.Int, error) {
	var (
		ret0 = new(*big.Int)
	)
	out := ret0
	err := _Simplecasper.contract.Call(opts, out, "last_voter_rescale")
	return *ret0, err
}

// Last_voter_rescale is a free data retrieval call binding the contract method 0xe6b57366.
//
// Solidity: function last_voter_rescale() constant returns(uint256)
func (_Simplecasper *SimplecasperSession) Last_voter_rescale() (*big.Int, error) {
	return _Simplecasper.Contract.Last_voter_rescale(&_Simplecasper.CallOpts)
}

// Last_voter_rescale is a free data retrieval call binding the contract method 0xe6b57366.
//
// Solidity: function last_voter_rescale() constant returns(uint256)
func (_Simplecasper *SimplecasperCallerSession) Last_voter_rescale() (*big.Int, error) {
	return _Simplecasper.Contract.Last_voter_rescale(&_Simplecasper.CallOpts)
}

// Main_hash_justified is a free data retrieval call binding the contract method 0x99787ac6.
//
// Solidity: function main_hash_justified() constant returns(bool)
func (_Simplecasper *SimplecasperCaller) Main_hash_justified(opts *bind.CallOpts) (bool, error) {
	var (
		ret0 = new(bool)
	)
	out := ret0
	err := _Simplecasper.contract.Call(opts, out, "main_hash_justified")
	return *ret0, err
}

// Main_hash_justified is a free data retrieval call binding the contract method 0x99787ac6.
//
// Solidity: function main_hash_justified() constant returns(bool)
func (_Simplecasper *SimplecasperSession) Main_hash_justified() (bool, error) {
	return _Simplecasper.Contract.Main_hash_justified(&_Simplecasper.CallOpts)
}

// Main_hash_justified is a free data retrieval call binding the contract method 0x99787ac6.
//
// Solidity: function main_hash_justified() constant returns(bool)
func (_Simplecasper *SimplecasperCallerSession) Main_hash_justified() (bool, error) {
	return _Simplecasper.Contract.Main_hash_justified(&_Simplecasper.CallOpts)
}

// NextValidatorIndex is a free data retrieval call binding the contract method 0xcc680dbb.
//
// Solidity: function nextValidatorIndex() constant returns(uint256)
func (_Simplecasper *SimplecasperCaller) NextValidatorIndex(opts *bind.CallOpts) (*big.Int, error) {
	var (
		ret0 = new(*big.Int)
	)
	out := ret0
	err := _Simplecasper.contract.Call(opts, out, "nextValidatorIndex")
	return *ret0, err
}

// NextValidatorIndex is a free data retrieval call binding the contract method 0xcc680dbb.
//
// Solidity: function nextValidatorIndex() constant returns(uint256)
func (_Simplecasper *SimplecasperSession) NextValidatorIndex() (*big.Int, error) {
	return _Simplecasper.Contract.NextValidatorIndex(&_Simplecasper.CallOpts)
}

// NextValidatorIndex is a free data retrieval call binding the contract method 0xcc680dbb.
//
// Solidity: function nextValidatorIndex() constant returns(uint256)
func (_Simplecasper *SimplecasperCallerSession) NextValidatorIndex() (*big.Int, error) {
	return _Simplecasper.Contract.NextValidatorIndex(&_Simplecasper.CallOpts)
}

// Next_dynasty_wei_delta is a free data retrieval call binding the contract method 0x01827825.
//
// Solidity: function next_dynasty_wei_delta() constant returns(uint256)
func (_Simplecasper *SimplecasperCaller) Next_dynasty_wei_delta(opts *bind.CallOpts) (*big.Int, error) {
	var (
		ret0 = new(*big.Int)
	)
	out := ret0
	err := _Simplecasper.contract.Call(opts, out, "next_dynasty_wei_delta")
	return *ret0, err
}

// Next_dynasty_wei_delta is a free data retrieval call binding the contract method 0x01827825.
//
// Solidity: function next_dynasty_wei_delta() constant returns(uint256)
func (_Simplecasper *SimplecasperSession) Next_dynasty_wei_delta() (*big.Int, error) {
	return _Simplecasper.Contract.Next_dynasty_wei_delta(&_Simplecasper.CallOpts)
}

// Next_dynasty_wei_delta is a free data retrieval call binding the contract method 0x01827825.
//
// Solidity: function next_dynasty_wei_delta() constant returns(uint256)
func (_Simplecasper *SimplecasperCallerSession) Next_dynasty_wei_delta() (*big.Int, error) {
	return _Simplecasper.Contract.Next_dynasty_wei_delta(&_Simplecasper.CallOpts)
}

// Reward_factor is a free data retrieval call binding the contract method 0x07dcf45b.
//
// Solidity: function reward_factor() constant returns(uint256)
func (_Simplecasper *SimplecasperCaller) Reward_factor(opts *bind.CallOpts) (*big.Int, error) {
	var (
		ret0 = new(*big.Int)
	)
	out := ret0
	err := _Simplecasper.contract.Call(opts, out, "reward_factor")
	return *ret0, err
}

// Reward_factor is a free data retrieval call binding the contract method 0x07dcf45b.
//
// Solidity: function reward_factor() constant returns(uint256)
func (_Simplecasper *SimplecasperSession) Reward_factor() (*big.Int, error) {
	return _Simplecasper.Contract.Reward_factor(&_Simplecasper.CallOpts)
}

// Reward_factor is a free data retrieval call binding the contract method 0x07dcf45b.
//
// Solidity: function reward_factor() constant returns(uint256)
func (_Simplecasper *SimplecasperCallerSession) Reward_factor() (*big.Int, error) {
	return _Simplecasper.Contract.Reward_factor(&_Simplecasper.CallOpts)
}

// Second_next_dynasty_wei_delta is a free data retrieval call binding the contract method 0x47a284c7.
//
// Solidity: function second_next_dynasty_wei_delta() constant returns(uint256)
func (_Simplecasper *SimplecasperCaller) Second_next_dynasty_wei_delta(opts *bind.CallOpts) (*big.Int, error) {
	var (
		ret0 = new(*big.Int)
	)
	out := ret0
	err := _Simplecasper.contract.Call(opts, out, "second_next_dynasty_wei_delta")
	return *ret0, err
}

// Second_next_dynasty_wei_delta is a free data retrieval call binding the contract method 0x47a284c7.
//
// Solidity: function second_next_dynasty_wei_delta() constant returns(uint256)
func (_Simplecasper *SimplecasperSession) Second_next_dynasty_wei_delta() (*big.Int, error) {
	return _Simplecasper.Contract.Second_next_dynasty_wei_delta(&_Simplecasper.CallOpts)
}

// Second_next_dynasty_wei_delta is a free data retrieval call binding the contract method 0x47a284c7.
//
// Solidity: function second_next_dynasty_wei_delta() constant returns(uint256)
func (_Simplecasper *SimplecasperCallerSession) Second_next_dynasty_wei_delta() (*big.Int, error) {
	return _Simplecasper.Contract.Second_next_dynasty_wei_delta(&_Simplecasper.CallOpts)
}

// Total_curdyn_deposits is a free data retrieval call binding the contract method 0x9caa34e2.
//
// Solidity: function total_curdyn_deposits() constant returns(uint256)
func (_Simplecasper *SimplecasperCaller) Total_curdyn_deposits(opts *bind.CallOpts) (*big.Int, error) {
	var (
		ret0 = new(*big.Int)
	)
	out := ret0
	err := _Simplecasper.contract.Call(opts, out, "total_curdyn_deposits")
	return *ret0, err
}

// Total_curdyn_deposits is a free data retrieval call binding the contract method 0x9caa34e2.
//
// Solidity: function total_curdyn_deposits() constant returns(uint256)
func (_Simplecasper *SimplecasperSession) Total_curdyn_deposits() (*big.Int, error) {
	return _Simplecasper.Contract.Total_curdyn_deposits(&_Simplecasper.CallOpts)
}

// Total_curdyn_deposits is a free data retrieval call binding the contract method 0x9caa34e2.
//
// Solidity: function total_curdyn_deposits() constant returns(uint256)
func (_Simplecasper *SimplecasperCallerSession) Total_curdyn_deposits() (*big.Int, error) {
	return _Simplecasper.Contract.Total_curdyn_deposits(&_Simplecasper.CallOpts)
}

// Total_destroyed is a free data retrieval call binding the contract method 0x099526bc.
//
// Solidity: function total_destroyed() constant returns(uint256)
func (_Simplecasper *SimplecasperCaller) Total_destroyed(opts *bind.CallOpts) (*big.Int, error) {
	var (
		ret0 = new(*big.Int)
	)
	out := ret0
	err := _Simplecasper.contract.Call(opts, out, "total_destroyed")
	return *ret0, err
}

// Total_destroyed is a free data retrieval call binding the contract method 0x099526bc.
//
// Solidity: function total_destroyed() constant returns(uint256)
func (_Simplecasper *SimplecasperSession) Total_destroyed() (*big.Int, error) {
	return _Simplecasper.Contract.Total_destroyed(&_Simplecasper.CallOpts)
}

// Total_destroyed is a free data retrieval call binding the contract method 0x099526bc.
//
// Solidity: function total_destroyed() constant returns(uint256)
func (_Simplecasper *SimplecasperCallerSession) Total_destroyed() (*big.Int, error) {
	return _Simplecasper.Contract.Total_destroyed(&_Simplecasper.CallOpts)
}

// Total_prevdyn_deposits is a free data retrieval call binding the contract method 0x93221484.
//
// Solidity: function total_prevdyn_deposits() constant returns(uint256)
func (_Simplecasper *SimplecasperCaller) Total_prevdyn_deposits(opts *bind.CallOpts) (*big.Int, error) {
	var (
		ret0 = new(*big.Int)
	)
	out := ret0
	err := _Simplecasper.contract.Call(opts, out, "total_prevdyn_deposits")
	return *ret0, err
}

// Total_prevdyn_deposits is a free data retrieval call binding the contract method 0x93221484.
//
// Solidity: function total_prevdyn_deposits() constant returns(uint256)
func (_Simplecasper *SimplecasperSession) Total_prevdyn_deposits() (*big.Int, error) {
	return _Simplecasper.Contract.Total_prevdyn_deposits(&_Simplecasper.CallOpts)
}

// Total_prevdyn_deposits is a free data retrieval call binding the contract method 0x93221484.
//
// Solidity: function total_prevdyn_deposits() constant returns(uint256)
func (_Simplecasper *SimplecasperCallerSession) Total_prevdyn_deposits() (*big.Int, error) {
	return _Simplecasper.Contract.Total_prevdyn_deposits(&_Simplecasper.CallOpts)
}

// Votes is a free data retrieval call binding the contract method 0x5df81330.
//
// Solidity: function votes( uint256) constant returns(is_justified bool, is_finalized bool)
func (_Simplecasper *SimplecasperCaller) Votes(opts *bind.CallOpts, arg0 *big.Int) (struct {
	Is_justified bool
	Is_finalized bool
}, error) {
	ret := new(struct {
		Is_justified bool
		Is_finalized bool
	})
	out := ret
	err := _Simplecasper.contract.Call(opts, out, "votes", arg0)
	return *ret, err
}

// Votes is a free data retrieval call binding the contract method 0x5df81330.
//
// Solidity: function votes( uint256) constant returns(is_justified bool, is_finalized bool)
func (_Simplecasper *SimplecasperSession) Votes(arg0 *big.Int) (struct {
	Is_justified bool
	Is_finalized bool
}, error) {
	return _Simplecasper.Contract.Votes(&_Simplecasper.CallOpts, arg0)
}

// Votes is a free data retrieval call binding the contract method 0x5df81330.
//
// Solidity: function votes( uint256) constant returns(is_justified bool, is_finalized bool)
func (_Simplecasper *SimplecasperCallerSession) Votes(arg0 *big.Int) (struct {
	Is_justified bool
	Is_finalized bool
}, error) {
	return _Simplecasper.Contract.Votes(&_Simplecasper.CallOpts, arg0)
}

// Withdrawal_delay is a free data retrieval call binding the contract method 0xeaa26f0f.
//
// Solidity: function withdrawal_delay() constant returns(uint256)
func (_Simplecasper *SimplecasperCaller) Withdrawal_delay(opts *bind.CallOpts) (*big.Int, error) {
	var (
		ret0 = new(*big.Int)
	)
	out := ret0
	err := _Simplecasper.contract.Call(opts, out, "withdrawal_delay")
	return *ret0, err
}

// Withdrawal_delay is a free data retrieval call binding the contract method 0xeaa26f0f.
//
// Solidity: function withdrawal_delay() constant returns(uint256)
func (_Simplecasper *SimplecasperSession) Withdrawal_delay() (*big.Int, error) {
	return _Simplecasper.Contract.Withdrawal_delay(&_Simplecasper.CallOpts)
}

// Withdrawal_delay is a free data retrieval call binding the contract method 0xeaa26f0f.
//
// Solidity: function withdrawal_delay() constant returns(uint256)
func (_Simplecasper *SimplecasperCallerSession) Withdrawal_delay() (*big.Int, error) {
	return _Simplecasper.Contract.Withdrawal_delay(&_Simplecasper.CallOpts)
}

// Delete_validator is a paid mutator transaction binding the contract method 0x7f09160d.
//
// Solidity: function delete_validator(validator_index uint256) returns()
func (_Simplecasper *SimplecasperTransactor) Delete_validator(opts *bind.TransactOpts, validator_index *big.Int) (*types.Transaction, error) {
	return _Simplecasper.contract.Transact(opts, "delete_validator", validator_index)
}

// Delete_validator is a paid mutator transaction binding the contract method 0x7f09160d.
//
// Solidity: function delete_validator(validator_index uint256) returns()
func (_Simplecasper *SimplecasperSession) Delete_validator(validator_index *big.Int) (*types.Transaction, error) {
	return _Simplecasper.Contract.Delete_validator(&_Simplecasper.TransactOpts, validator_index)
}

// Delete_validator is a paid mutator transaction binding the contract method 0x7f09160d.
//
// Solidity: function delete_validator(validator_index uint256) returns()
func (_Simplecasper *SimplecasperTransactorSession) Delete_validator(validator_index *big.Int) (*types.Transaction, error) {
	return _Simplecasper.Contract.Delete_validator(&_Simplecasper.TransactOpts, validator_index)
}

// Deposit is a paid mutator transaction binding the contract method 0xf9609f08.
//
// Solidity: function deposit(validation_addr address, withdrawal_addr address) returns()
func (_Simplecasper *SimplecasperTransactor) Deposit(opts *bind.TransactOpts, validation_addr common.Address, withdrawal_addr common.Address) (*types.Transaction, error) {
	return _Simplecasper.contract.Transact(opts, "deposit", validation_addr, withdrawal_addr)
}

// Deposit is a paid mutator transaction binding the contract method 0xf9609f08.
//
// Solidity: function deposit(validation_addr address, withdrawal_addr address) returns()
func (_Simplecasper *SimplecasperSession) Deposit(validation_addr common.Address, withdrawal_addr common.Address) (*types.Transaction, error) {
	return _Simplecasper.Contract.Deposit(&_Simplecasper.TransactOpts, validation_addr, withdrawal_addr)
}

// Deposit is a paid mutator transaction binding the contract method 0xf9609f08.
//
// Solidity: function deposit(validation_addr address, withdrawal_addr address) returns()
func (_Simplecasper *SimplecasperTransactorSession) Deposit(validation_addr common.Address, withdrawal_addr common.Address) (*types.Transaction, error) {
	return _Simplecasper.Contract.Deposit(&_Simplecasper.TransactOpts, validation_addr, withdrawal_addr)
}

// Init is a paid mutator transaction binding the contract method 0x775a04c8.
//
// Solidity: function init(_epoch_length uint256, _withdrawal_delay uint256, _owner address, _sighasher address, _purity_checker address, _base_interest_factor uint256, _base_penalty_factor uint256, _min_deposit_size uint256) returns()
func (_Simplecasper *SimplecasperTransactor) Init(opts *bind.TransactOpts, _epoch_length *big.Int, _withdrawal_delay *big.Int, _owner common.Address, _sighasher common.Address, _purity_checker common.Address, _base_interest_factor *big.Int, _base_penalty_factor *big.Int, _min_deposit_size *big.Int) (*types.Transaction, error) {
	return _Simplecasper.contract.Transact(opts, "init", _epoch_length, _withdrawal_delay, _owner, _sighasher, _purity_checker, _base_interest_factor, _base_penalty_factor, _min_deposit_size)
}

// Init is a paid mutator transaction binding the contract method 0x775a04c8.
//
// Solidity: function init(_epoch_length uint256, _withdrawal_delay uint256, _owner address, _sighasher address, _purity_checker address, _base_interest_factor uint256, _base_penalty_factor uint256, _min_deposit_size uint256) returns()
func (_Simplecasper *SimplecasperSession) Init(_epoch_length *big.Int, _withdrawal_delay *big.Int, _owner common.Address, _sighasher common.Address, _purity_checker common.Address, _base_interest_factor *big.Int, _base_penalty_factor *big.Int, _min_deposit_size *big.Int) (*types.Transaction, error) {
	return _Simplecasper.Contract.Init(&_Simplecasper.TransactOpts, _epoch_length, _withdrawal_delay, _owner, _sighasher, _purity_checker, _base_interest_factor, _base_penalty_factor, _min_deposit_size)
}

// Init is a paid mutator transaction binding the contract method 0x775a04c8.
//
// Solidity: function init(_epoch_length uint256, _withdrawal_delay uint256, _owner address, _sighasher address, _purity_checker address, _base_interest_factor uint256, _base_penalty_factor uint256, _min_deposit_size uint256) returns()
func (_Simplecasper *SimplecasperTransactorSession) Init(_epoch_length *big.Int, _withdrawal_delay *big.Int, _owner common.Address, _sighasher common.Address, _purity_checker common.Address, _base_interest_factor *big.Int, _base_penalty_factor *big.Int, _min_deposit_size *big.Int) (*types.Transaction, error) {
	return _Simplecasper.Contract.Init(&_Simplecasper.TransactOpts, _epoch_length, _withdrawal_delay, _owner, _sighasher, _purity_checker, _base_interest_factor, _base_penalty_factor, _min_deposit_size)
}

// Logout is a paid mutator transaction binding the contract method 0x42310c32.
//
// Solidity: function logout(logout_msg bytes) returns()
func (_Simplecasper *SimplecasperTransactor) Logout(opts *bind.TransactOpts, logout_msg []byte) (*types.Transaction, error) {
	return _Simplecasper.contract.Transact(opts, "logout", logout_msg)
}

// Logout is a paid mutator transaction binding the contract method 0x42310c32.
//
// Solidity: function logout(logout_msg bytes) returns()
func (_Simplecasper *SimplecasperSession) Logout(logout_msg []byte) (*types.Transaction, error) {
	return _Simplecasper.Contract.Logout(&_Simplecasper.TransactOpts, logout_msg)
}

// Logout is a paid mutator transaction binding the contract method 0x42310c32.
//
// Solidity: function logout(logout_msg bytes) returns()
func (_Simplecasper *SimplecasperTransactorSession) Logout(logout_msg []byte) (*types.Transaction, error) {
	return _Simplecasper.Contract.Logout(&_Simplecasper.TransactOpts, logout_msg)
}

// Slash is a paid mutator transaction binding the contract method 0xcc20f16b.
//
// Solidity: function slash(vote_msg_1 bytes, vote_msg_2 bytes) returns()
func (_Simplecasper *SimplecasperTransactor) Slash(opts *bind.TransactOpts, vote_msg_1 []byte, vote_msg_2 []byte) (*types.Transaction, error) {
	return _Simplecasper.contract.Transact(opts, "slash", vote_msg_1, vote_msg_2)
}

// Slash is a paid mutator transaction binding the contract method 0xcc20f16b.
//
// Solidity: function slash(vote_msg_1 bytes, vote_msg_2 bytes) returns()
func (_Simplecasper *SimplecasperSession) Slash(vote_msg_1 []byte, vote_msg_2 []byte) (*types.Transaction, error) {
	return _Simplecasper.Contract.Slash(&_Simplecasper.TransactOpts, vote_msg_1, vote_msg_2)
}

// Slash is a paid mutator transaction binding the contract method 0xcc20f16b.
//
// Solidity: function slash(vote_msg_1 bytes, vote_msg_2 bytes) returns()
func (_Simplecasper *SimplecasperTransactorSession) Slash(vote_msg_1 []byte, vote_msg_2 []byte) (*types.Transaction, error) {
	return _Simplecasper.Contract.Slash(&_Simplecasper.TransactOpts, vote_msg_1, vote_msg_2)
}

// Vote is a paid mutator transaction binding the contract method 0xe9dc0614.
//
// Solidity: function vote(vote_msg bytes) returns()
func (_Simplecasper *SimplecasperTransactor) Vote(opts *bind.TransactOpts, vote_msg []byte) (*types.Transaction, error) {
	return _Simplecasper.contract.Transact(opts, "vote", vote_msg)
}

// Vote is a paid mutator transaction binding the contract method 0xe9dc0614.
//
// Solidity: function vote(vote_msg bytes) returns()
func (_Simplecasper *SimplecasperSession) Vote(vote_msg []byte) (*types.Transaction, error) {
	return _Simplecasper.Contract.Vote(&_Simplecasper.TransactOpts, vote_msg)
}

// Vote is a paid mutator transaction binding the contract method 0xe9dc0614.
//
// Solidity: function vote(vote_msg bytes) returns()
func (_Simplecasper *SimplecasperTransactorSession) Vote(vote_msg []byte) (*types.Transaction, error) {
	return _Simplecasper.Contract.Vote(&_Simplecasper.TransactOpts, vote_msg)
}

// Withdraw is a paid mutator transaction binding the contract method 0x2e1a7d4d.
//
// Solidity: function withdraw(validator_index uint256) returns()
func (_Simplecasper *SimplecasperTransactor) Withdraw(opts *bind.TransactOpts, validator_index *big.Int) (*types.Transaction, error) {
	return _Simplecasper.contract.Transact(opts, "withdraw", validator_index)
}

// Withdraw is a paid mutator transaction binding the contract method 0x2e1a7d4d.
//
// Solidity: function withdraw(validator_index uint256) returns()
func (_Simplecasper *SimplecasperSession) Withdraw(validator_index *big.Int) (*types.Transaction, error) {
	return _Simplecasper.Contract.Withdraw(&_Simplecasper.TransactOpts, validator_index)
}

// Withdraw is a paid mutator transaction binding the contract method 0x2e1a7d4d.
//
// Solidity: function withdraw(validator_index uint256) returns()
func (_Simplecasper *SimplecasperTransactorSession) Withdraw(validator_index *big.Int) (*types.Transaction, error) {
	return _Simplecasper.Contract.Withdraw(&_Simplecasper.TransactOpts, validator_index)
}

