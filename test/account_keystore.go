package main

import (
	//"github.com/ethereum/go-ethereum/crypto"
	"github.com/ethereum/go-ethereum/accounts/keystore"
	"fmt"
    //"io/ioutil"
    //"golang.org/x/crypto/nacl/box"
    "github.com/ethereum/go-ethereum/accounts"
)

func main() {
	ks := keystore.NewKeyStore("/var/www/vhosts/data/keystore", keystore.StandardScryptN, keystore.StandardScryptP)
	fmt.Printf("\n\nks:%v\n\n", ks)
	 
	var ks_accounts []accounts.Account      //     type Account struct    in->   keystore/keystore.go
	ks_accounts = ks.Accounts()    
	fmt.Printf("Accounts:%v\n\n", ks_accounts)  
	  
	var ks_wallets []accounts.Wallet
	ks_wallets = ks.Wallets()               //     type Wallet interface  in->   accounts/accounts.go
	fmt.Printf("Wallets:%v\n\n", ks_wallets)
	   
	acc := 	ks_wallets[0].Accounts()    //     type Wallet interface  in->   accounts/accounts.go
	fmt.Printf("Wallets:%v\n\n", acc)  
	  
	  
	acc_address := ks_accounts[0].Address   //  Ethereum account address derived from the key
	fmt.Printf("acc_address:%v\n", acc_address)
	fmt.Printf("acc_address:%x\n\n", acc_address)	  
	return  
}


