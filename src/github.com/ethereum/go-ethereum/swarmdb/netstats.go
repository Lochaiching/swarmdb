package swarmdb

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"math/big"
	"os"
	"path/filepath"
	"strconv"
	"time"
)

type Netstats struct {
	NodeID        string
	WalletAddress string
	Path          string
	SStat         map[string]*big.Int
	LaunchDT      time.Time
	LReadDT       time.Time
	LWriteDT      time.Time
	LogDT         time.Time
}

type Netstatslog struct {
	NodeID        string
	WalletAddress string
	SStat         map[string]string
	LaunchDT      time.Time
	LReadDT       time.Time
	LWriteDT      time.Time
	LogDT         time.Time
}

func NewNetstats(config *SWARMDBConfig) (self *Netstats) {
	//nodeID := fmt.Sprintf("%s:%d", config.ListenAddrTCP, config.PortTCP)

	hostname, err := os.Hostname()
	if err != nil {
		hostname = "swarmdb"
	}
	ts := time.Now()
	var ns = &Netstats{
		NodeID:        hostname,
		Path:          "/tmp/",
		WalletAddress: config.Address,
		SStat:         make(map[string]*big.Int),
		LaunchDT:      ts,
	}
	ns.SStat["SwapI"] = big.NewInt(0)   // # of check issued
	ns.SStat["SwapIA"] = big.NewInt(0)  // amount of check issue
	ns.SStat["SwapIL"] = big.NewInt(0)  // # of check issued long-term
	ns.SStat["SwapIAL"] = big.NewInt(0) // amount of checks issued long-term

	ns.SStat["SwapR"] = big.NewInt(0)   // # of checks received
	ns.SStat["SwapRA"] = big.NewInt(0)  // amount of checks received
	ns.SStat["SwapRL"] = big.NewInt(0)  // # of checks received long-term
	ns.SStat["SwapRAL"] = big.NewInt(0) // amount of checks received long-term

	fmt.Printf("Q: %s\n", ns.SStat)

	t := time.NewTicker(20 * time.Second)
	go func(ns *Netstats) {
		for {
			ns.Flush()
			//time.Sleep(5*time.Second)
			<-t.C
		}
	}(ns)

	/*
		// this is working ... same as the ticker above
		go func(ns *Netstats) {
			for {
			   ns.Flush()
			   time.Sleep(5*time.Second)
			}
		}(ns)
	*/

	return ns
}

func (self *Netstats) AddIssue(amount int) (err error) {
	ts := time.Now()
	self.LWriteDT = ts
	self.SStat["SwapI"].Add(self.SStat["SwapI"], big.NewInt(1))
	self.SStat["SwapIA"].Add(self.SStat["SwapIA"], big.NewInt(int64(amount)))
	self.SStat["SwapIL"].Add(self.SStat["SwapIL"], big.NewInt(1))
	self.SStat["SwapIAL"].Add(self.SStat["SwapIAL"], big.NewInt(int64(amount)))
	return nil
}

func (self *Netstats) AddReceive(amount int) (err error) {
	ts := time.Now()
	self.LReadDT = ts
	self.SStat["SwapR"].Add(self.SStat["SwapR"], big.NewInt(1))
	self.SStat["SwapRA"].Add(self.SStat["SwapRA"], big.NewInt(int64(amount)))
	self.SStat["SwapRL"].Add(self.SStat["SwapRL"], big.NewInt(1))
	self.SStat["SwapRAL"].Add(self.SStat["SwapRAL"], big.NewInt(int64(amount)))
	return nil
}

func (self *Netstats) MarshalJSON() (data []byte, err error) {
	var l Netstatslog
	l.NodeID = self.NodeID
	l.WalletAddress = self.WalletAddress
	l.LaunchDT = self.LaunchDT
	l.LReadDT = self.LReadDT
	l.LWriteDT = self.LWriteDT
	l.LogDT = self.LogDT
	l.SStat = make(map[string]string)
	for sk, sv := range self.SStat {
		l.SStat[sk] = sv.String()
		if sk == "SwapI" || sk == "SwapIA" || sk == "SwapR" || sk == "SwapRA" {
			self.SStat[sk] = big.NewInt(0)
		}
	}
	data, err = json.Marshal(l)
	if err != nil {
		return nil, &SWARMDBError{message: fmt.Sprintf("[netstats:MarshalJSON] Marshal %s", err.Error()), ErrorCode: 459, ErrorMessage: fmt.Sprintf("Unable to marshal")}
	} else {
		return data, nil
	}
}

func (self *Netstats) UnmarshalJSON(data []byte) (err error) {
	var l Netstatslog
	l.SStat = make(map[string]string)
	err = json.Unmarshal(data, &l)
	if err != nil {
		return &SWARMDBError{message: fmt.Sprintf("[netstats:UnmarshalJSON]%s", err.Error()), ErrorCode: 460, ErrorMessage: fmt.Sprintf("Unable to unmarshal [%s]", data)}
	} else {
		self.SStat = make(map[string]*big.Int)
		for sk, sv := range l.SStat {
			i, _ := strconv.ParseInt(sv, 10, 64)
			self.SStat[sk] = big.NewInt(int64(i))
		}
		self.NodeID = l.NodeID
		self.WalletAddress = l.WalletAddress
		self.LaunchDT = l.LaunchDT
		self.LReadDT = l.LReadDT
		self.LWriteDT = l.LWriteDT
		self.LogDT = l.LogDT
	}
	return nil
}

func LoadNetstats() (self *Netstats, err error) {
	netstatsFileName := "netstats.json"
	netstatsFullPath := filepath.Join("/tmp/", netstatsFileName)
	var data []byte
	data, errLoad := ioutil.ReadFile(netstatsFullPath)
	if errLoad != nil {
		//return self, GenerateSWARMDBError(err, fmt.Sprintf("[netstats:LoadNetstats] %s", err.Error()))
		return self, &SWARMDBError{message: fmt.Sprintf("[netstats:LoadNetstats] %s", err.Error()), ErrorCode: 461, ErrorMessage: "LoadNetstats"}
	}

	errParse := json.Unmarshal(data, &self)
	if errParse != nil {
		//return self, GenerateSWARMDBError(err, fmt.Sprintf("[netstats:LoadNetstats] %s", err.Error()))
		return self, &SWARMDBError{message: fmt.Sprintf("[netstats:LoadNetstats] %s", err.Error()), ErrorCode: 461, ErrorMessage: "LoadNetstats"}
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
	ts := time.Now()
	self.LogDT = ts

	data, err := json.Marshal(self)
	if err != nil {
		return &SWARMDBError{message: fmt.Sprintf("[dbchunkstore:Flush] Marshal %s", err.Error()), ErrorCode: 462, ErrorMessage: "Unable to Flush DBChunkstore"}
	}
	netstatlog, err := os.OpenFile("/tmp/netstats.log", os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0600)
	if err != nil {
		return &SWARMDBError{message: fmt.Sprintf("[dbchunkstore:Flush] OpenFile %s", err.Error()), ErrorCode: 462, ErrorMessage: "Unable to Flush DBChunkstore"}
	}
	defer netstatlog.Close()
	fmt.Fprintf(netstatlog, "%s\n", data)

	self.SStat["SwapI"] = big.NewInt(0)
	self.SStat["SwapIA"] = big.NewInt(0)

	self.SStat["SwapR"] = big.NewInt(0)
	self.SStat["SwapRA"] = big.NewInt(0)

	return nil
}
