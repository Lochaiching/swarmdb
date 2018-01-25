// Copyright (c) 2018 Wolk Inc.  All rights reserved.

// The SWARMDB library is free software: you can redistribute it and/or modify
// it under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// The SWARMDB library is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU Lesser General Public License for more details.
//
// You should have received a copy of the GNU Lesser General Public License
// along with the go-ethereum library. If not, see <http://www.gnu.org/licenses/>.

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

	c.ListenAddrTCP = SWARMDBCONF_LISTENADDR
	c.PortTCP = SWARMDBCONF_PORTTCP

	c.ListenAddrHTTP = SWARMDBCONF_LISTENADDR
	c.PortHTTP = SWARMDBCONF_PORTHTTP

	c.Address = u.Address
	c.PrivateKey = privateKey

	c.Authentication = 1
	c.ChunkDBPath = SWARMDBCONF_CHUNKDB_PATH
	c.KeystorePath = SWARMDBCONF_KEYSTORE_PATH
	c.Users = append(c.Users, u)

	c.Currency = SWARMDBCONF_CURRENCY
	c.TargetCostStorage = SWARMDBCONF_TARGET_COST_STORAGE
	c.TargetCostBandwidth = SWARMDBCONF_TARGET_COST_BANDWIDTH
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
