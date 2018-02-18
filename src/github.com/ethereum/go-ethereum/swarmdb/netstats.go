package swarmdb

import (
	"fmt"
	"os"
	"time"
	"encoding/json"
	"math/big"
	"io/ioutil"
)

type Netstats struct {
	NodeID        string
	WalletAddress string

	Stat          map[string]string
	CStat         map[string]*big.Int `json:"-"`

	LaunchDT      *time.Time
	LReadDT       *time.Time
	LWriteDT      *time.Time
	LogDT         *time.Time
}

func NewNetstats(nodeID string, walletAddress string) (self *Netstats) {
	var ns = &Netstats{
		Stat:     make(map[string]string),
		CStat:     make(map[string]*big.Int),
	}
	return ns
}

func (self *Netstats) StoreChunk() {
	ts := time.Now()

	self.LWriteDT = &ts
	self.CStat["ChunkW"].Add(self.CStat["ChunkW"], big.NewInt(1))
}

func (self *Netstats) ReadChunk() {
	ts := time.Now()
	self.LReadDT = &ts
	self.CStat["ChunkR"].Add(self.CStat["ChunkR"], big.NewInt(1))
}

func (self *Netstats) MarshalJSON() (data []byte, err error) {
	// logDT := time.Now()
	for cc, cv := range self.CStat {
		self.Stat[cc] = cv.String()
		if cc == "ChunkR" || cc == "ChunkW" || cc == "ChunkS" {
			self.CStat[cc] = big.NewInt(0)
		}
	}

	data, err = json.Marshal(self)
	if err != nil {
		return nil, &SWARMDBError{message: fmt.Sprintf("[netstats:MarshalJSON] Marshal %s", err.Error()), ErrorCode: 459, ErrorMessage: fmt.Sprintf("Unable to marshal")}
	} else {
		return data, nil
	}
}

func (self *Netstats) UnmarshalJSON(data []byte) (err error) {
	var ns Netstats
	ns.CStat =  make(map[string]*big.Int)
	err = json.Unmarshal(data, &ns)
	if err != nil {
		return &SWARMDBError{message: fmt.Sprintf("[netstats:UnmarshalJSON]%s", err.Error()), ErrorCode: 460, ErrorMessage: fmt.Sprintf("Unable to unmarshal [%s]", data)}
	}
	// self.farmer = common.HexToAddress(file.WalletAddress)

	for cc, cv := range self.Stat {
		if cc == "ChunkR" || cc == "ChunkW" || cc == "ChunkS" || cc == "LogW" {
			self.CStat[cc] = big.NewInt(0)
		} else {
			ok := false
			self.CStat[cc], ok = new(big.Int).SetString(cv, 10)
			if !ok {
				return &SWARMDBError{message: fmt.Sprintf("[netstats:UnmarshalJSON] %v loading failure: unable to convert string to big integer: %v", cc, cv), ErrorCode: 460, ErrorMessage: fmt.Sprintf("Unable to unmarshal [%s]", data)}
			}
		}
	}
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

func (self *Netstats) Save(path string) (err error) {
	data, err := json.MarshalIndent(self, "", " ")
	if err != nil {
		return &SWARMDBError{message: fmt.Sprintf("[netstats:Save] MarshalIndent %s", err.Error()), ErrorCode: 461, ErrorMessage: "Unable to Save Netstats"}
	}
	err = ioutil.WriteFile(path, data, os.ModePerm)
	if err != nil {
		return &SWARMDBError{message: fmt.Sprintf("[netstats:Save] WriteFile %s", err.Error()), ErrorCode: 461, ErrorMessage: "Unable to Save Netstats"}
	} else {
		return nil
	}
}

func (self *Netstats) Flush() (err error) {
	self.CStat["ChunkRL"].Add(self.CStat["ChunkR"], self.CStat["ChunkRL"])
	self.CStat["ChunkWL"].Add(self.CStat["ChunkW"], self.CStat["ChunkWL"])
	self.CStat["LogWL"].Add(self.CStat["LogW"], self.CStat["LogWL"])

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
//	chunkstat := map[string]*big.Int{"ChunkR": big.NewInt(0), "ChunkW": big.NewInt(0), "ChunkS": big.NewInt(0), "ChunkRL": big.NewInt(0), "ChunkWL": big.NewInt(0), "ChunkSL": big.NewInt(0)}
}


