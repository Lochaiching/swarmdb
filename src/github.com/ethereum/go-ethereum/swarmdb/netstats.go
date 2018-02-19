package swarmdb

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"math/big"
	"os"
	"path/filepath"
	"time"
)

type Netstats struct {
	NodeID        string
	WalletAddress string
	Path          string
	CStat         map[string]*big.Int
	LaunchDT      *time.Time
	LReadDT       *time.Time
	LWriteDT      *time.Time
	LogDT         *time.Time
}

type Netstatslog struct {
	NodeID        string
	WalletAddress string
	Stat          map[string]string
	LaunchDT      *time.Time
	LReadDT       *time.Time
	LWriteDT      *time.Time
	LogDT         *time.Time
}

func NewNetstats(config *SWARMDBConfig) (self *Netstats) {
	nodeID := fmt.Sprintf("%s:%d", config.ListenAddrTCP, config.PortTCP)

	var ns = &Netstats{
		NodeID:        nodeID,
		Path:          config.ChunkDBPath,
		WalletAddress: config.Address,
		CStat:         make(map[string]*big.Int),
	}
	ns.CStat["ChunkW"] = big.NewInt(0)
	ns.CStat["ChunkR"] = big.NewInt(0)
	ns.CStat["ChunkWL"] = big.NewInt(0)
	ns.CStat["ChunkRL"] = big.NewInt(0)
	fmt.Printf("Q: %s\n", ns.CStat)
	return ns
}

func (self *Netstats) StoreChunk() {
	//ts := time.Now()
	//self.LWriteDT = &ts
	self.CStat["ChunkW"].Add(self.CStat["ChunkW"], big.NewInt(1))
}

func (self *Netstats) ReadChunk() {
	// ts := time.Now()
	// self.LReadDT = &ts
	self.CStat["ChunkR"].Add(self.CStat["ChunkR"], big.NewInt(1))
}

func (self *Netstats) MarshalJSON() (data []byte, err error) {
	var l Netstatslog
	l.NodeID = self.NodeID
	l.WalletAddress = self.WalletAddress
	l.Stat = make(map[string]string)
	for cc, cv := range self.CStat {
		l.Stat[cc] = cv.String()
	}
	data, err = json.Marshal(l)
	if err != nil {
		return nil, &SWARMDBError{message: fmt.Sprintf("[netstats:MarshalJSON] Marshal %s", err.Error()), ErrorCode: 459, ErrorMessage: fmt.Sprintf("Unable to marshal")}
	} else {
		return data, nil
	}
}

func (self *Netstats) UnmarshalJSON(data []byte) (err error) {
	var ns Netstats
	ns.CStat = make(map[string]*big.Int)
	err = json.Unmarshal(data, &ns)
	if err != nil {
		return &SWARMDBError{message: fmt.Sprintf("[netstats:UnmarshalJSON]%s", err.Error()), ErrorCode: 460, ErrorMessage: fmt.Sprintf("Unable to unmarshal [%s]", data)}
	}
	// self.farmer = common.HexToAddress(file.WalletAddress)
	/*
		for cc, cv := range self.CStat {
			if cc == "ChunkR" || cc == "ChunkW" || cc == "ChunkS" || cc == "LogW" {
				self.CStat[cc] = big.NewInt(0)
			} else {
				ok := false
				self.CStat[cc], ok = new(big.Int).SetString(cv, 10)
				if !ok {
					return &SWARMDBError{message: fmt.Sprintf("[netstats:UnmarshalJSON] %v loading failure: unable to convert string to big integer: %v", cc, cv), ErrorCode: 460, ErrorMessage: fmt.Sprintf("Unable to unmarshal [%s]", data)}
				}
			}
		}*/
	return nil
}

func LoadNetstats(path string) (self *Netstats, err error) {
	var data []byte
	data, errLoad := ioutil.ReadFile(path)
	if errLoad != nil {
		return self, GenerateSWARMDBError(err, fmt.Sprintf("[netstats:LoadNetstats] %s", err.Error()))
	}

	errParse := json.Unmarshal(data, &self)
	if errParse != nil {
		return self, GenerateSWARMDBError(err, fmt.Sprintf("[netstats:LoadNetstats] %s", err.Error()))
	}
	return self, nil
}

func (self *Netstats) Save() (err error) {
	data, err := json.MarshalIndent(self, "", " ")
	if err != nil {
		return &SWARMDBError{message: fmt.Sprintf("[netstats:Save] MarshalIndent %s", err.Error()), ErrorCode: 461, ErrorMessage: "Unable to Save Netstats"}
	}
	netstatsFileName := "netstats.json"
	netstatsFullPath := filepath.Join(self.Path, netstatsFileName)
	err = ioutil.WriteFile(netstatsFullPath, data, os.ModePerm)
	if err != nil {
		return &SWARMDBError{message: fmt.Sprintf("[netstats:Save] WriteFile %s", err.Error()), ErrorCode: 461, ErrorMessage: "Unable to Save Netstats"}
	} else {
		fmt.Printf("netstats file written: [%s]\n", netstatsFullPath)
		return nil
	}
}

func (self *Netstats) Flush() (err error) {
	self.CStat["ChunkRL"].Add(self.CStat["ChunkR"], self.CStat["ChunkRL"])
	self.CStat["ChunkWL"].Add(self.CStat["ChunkW"], self.CStat["ChunkWL"])
	self.CStat["ChunkR"] = big.NewInt(0)
	self.CStat["ChunkW"] = big.NewInt(0)
	//self.CStat["LogWL"].Add(self.CStat["LogW"], self.CStat["LogWL"])

	data, err := json.Marshal(self)
	if err != nil {
		return &SWARMDBError{message: fmt.Sprintf("[netstats:Flush] Marshal %s", err.Error()), ErrorCode: 462, ErrorMessage: "Unable to Flush Netstats"}
	}
	netstatlog, err := os.OpenFile("netstat.log", os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0600)
	if err != nil {
		return &SWARMDBError{message: fmt.Sprintf("[netstats:Flush] OpenFile %s", err.Error()), ErrorCode: 462, ErrorMessage: "Unable to Flush Netstats"}
	}
	defer netstatlog.Close()
	fmt.Fprintf(netstatlog, "%s\n", data)
	return nil
}
