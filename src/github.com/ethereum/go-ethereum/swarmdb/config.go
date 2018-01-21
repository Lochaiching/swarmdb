package swarmdb

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
)

func (self *SWARMDBConfig) GetNodeID() (out string) {
	// TODO: replace with public key of farmer
	return "abcd"
}

func (self *SWARMDBConfig) GetSWARMDBUser() (u *SWARMDBUser) {
	for _, user := range self.Users {
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

	c.ListenAddrTCP = "127.0.0.1"
	c.PortTCP = 2000

	c.ListenAddrHTTP = "127.0.0.1"
	c.PortHTTP = 8500

	c.ChunkDBPath = "/swarmdb/data/keystore"

	c.Address = u.Address
	c.PrivateKey = privateKey

	c.Authentication = 1
	c.UsersKeyPath = "/swarmdb/data/keystore"
	c.Users = append(c.Users, u)

	c.Currency = "WLK" // USD, EUR etc.
	c.TargetCostStorage = 2.71828
	c.TargetCostBandwidth = 3.14159
	return c
}

func SaveSWARMDBConfig(c SWARMDBConfig, filename string) (err error) {
	// save file
	cout, err1 := json.Marshal(c)
	if err1 != nil {
		return &SWARMDBError{message: fmt.Sprintf("[config:SaveSWARMDBConfig] Marshal %s", err.Error())}
	} else {
		err := ioutil.WriteFile(filename, cout, 0644)
		if err != nil {
			return &SWARMDBError{message: fmt.Sprintf("[config:SaveSWARMDBConfig] WriteFile %s", err.Error())}
		}
	}
	return nil
}

func LoadSWARMDBConfig(filename string) (c SWARMDBConfig, err error) {
	// read file
	dat, err := ioutil.ReadFile(filename)
	if err != nil {
		return c, &SWARMDBError{message: fmt.Sprintf("[config:LoadSWARMDBConfig] ReadFile %s", err.Error())}
	}
	err = json.Unmarshal(dat, &c)
	if err != nil {
		return c, &SWARMDBError{message: fmt.Sprintf("[config:LoadSWARMDBConfig] Unmarshal %s", err.Error())}
	}
	return c, nil
}
