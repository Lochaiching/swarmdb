package swarmdb

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
)

func (self *SWARMDBConfig) GetNodeID() (out string) {
	return "abcd"
}

func (self *SWARMDBConfig) GetSWARMDBUser() (u *SWARMDBUser) {
	for _, user := range self.Users {
		fmt.Printf("%x pk:%x sk:%x\n", user.Address, user.pk, user.sk);
		return &user
	}
	return u
}

func GenerateSampleSWARMDBConfig(privateKey string, address string, passphrase string) (c SWARMDBConfig) {

	var u SWARMDBUser
	u.Address = address
	u.Passphrase = passphrase
	u.MinReplication = 3
	u.MaxReplication = 5
	u.AutoRenew = 1
	u.Encrypted = 1

	c.ChunkDBPath     = "/swarmdb/data/keystore"
	c.Address = u.Address
	c.PrivateKey = privateKey
	c.TargetCostStorage = 2.14159
	c.TargetCostBandwidth = 3.14159
	c.Currency = "WLK" // USD, EUR etc.
	c.Users = append(c.Users, u)
	return c
}

func SaveSWARMDBConfig(c SWARMDBConfig, filename string) (err error) {
	// save file
	cout, err1 := json.Marshal(c)
	if err1 != nil {
		return err1
	} else {
		err := ioutil.WriteFile(filename, cout, 0644)
		return err
	}
}

func LoadSWARMDBConfig(filename string) (c SWARMDBConfig, err error) {
	// read file
	dat, err := ioutil.ReadFile(filename)
	if err != nil {
		return c, err
	}
	err1 := json.Unmarshal(dat, &c)
	if err1 != nil {
		return c, err1
	}
	return c, nil
}
