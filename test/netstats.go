package main

import (
	"fmt"
	"encoding/json"
	"time"
)
type NetStats struct {
	NodeID  string `json:NodeID,omitempty"`
	WalletAddress string `json:WalletAddress,omitempty"`
	WalletEmail   string `json:WalletEmail,omitempty"`
	WalletETHBalance float64 `json:WalletETHBalance,omitempty"`
	WalletWolkBalance float64 `json:WalletWolkBalance,omitempty"`
	NumBytesStoredTotal int  `json:NumBytesStoredTotal,omitempty"`
	NumBytesStored int  `json:NumBytesStored,omitempty"`
	NumChunksStoredTotal int `json:NumChunksStoredTotal,omitempty"`
	NumChunksStored int `json:NumChunksStored,omitempty"`
	NumBytesReadTotal int `json:NumBytesReadTotal,omitempty"`
	NumBytesRead int `json:NumBytesRead,omitempty"`
	NumChunksReadTotal int `json:NumChunksReadTotal,omitempty"`
	NumChunksRead int `json:NumChunksRead,omitempty"`
	NumBytesWriteTotal int  `json:NumBytesWriteTotal,omitempty"`
	NumBytesWrite int  `json:NumBytesWrite,omitempty"`
	NumChunksWriteTotal int  `json:NumChunksWriteTotal,omitempty"`
	NumChunksWrite int  `json:NumChunksWrite,omitempty"`
	NumClaimsSubmitTotal int `json:NumClaimsSubmitTotal,omitempty"`
	NumClaimsSubmit int `json:NumClaimsSubmit,omitempty"`
	NumClaimsFailedTotal int `json:NumClaimsFailedTotal,omitempty"`
	NumClaimsFailed int `json:NumClaimsFailed,omitempty"`
	NumVerificationsTotal int `json:NumVerificationsTotal,omitempty"`
	NumVerifications int `json:NumVerifications,omitempty"`
	NumWolkEarnedTotal float64 `json:NumWolkEarnedTotal,omitempty"`
	NumWolkEarned float64 `json:NumWolkEarned,omitempty"`
	CurrentTimestamp int `json:CurrentTimestamp,omitempty"`
	LastStartTimestamp int `json:LastStartTimestamp,omitempty"`
	LastClaimSubmitTimestamp int `json:LastClaimSubmitTimestamp,omitempty"`
	LastChunkReadTimestamp int `json:LastChunkReadTimestamp,omitempty"`
	LastChunkWriteTimestamp int `json:LastChunkWriteTimestamp,omitempty"`
}

func getSwarmNetStats() (s NetStats) {
	s.NodeID = "nodeid";

	s.WalletAddress = "0x0f29286476806084b880e22fd2149b59c7cc8900"
	s.WalletEmail = "payments@wolk.com"
	s.WalletETHBalance = 78.2       
	s.WalletWolkBalance = 1234.56
	s.NumBytesStoredTotal = 25000000000
	s.NumBytesStored = 25010
	s.NumChunksStoredTotal = 25000000000/4096
	s.NumChunksStored = 25010/4096
	s.NumBytesReadTotal = 500000000
	s.NumBytesRead = 500000
	s.NumChunksReadTotal = 50000000/4096
	s.NumChunksRead = 50000/4096
	s.NumBytesWriteTotal =  50000000000
	s.NumBytesWrite =  40000000
	s.NumChunksWriteTotal = 50000000000/4096
	s.NumChunksWrite = 40000000/4096
	s.NumClaimsSubmitTotal = 414
	s.NumClaimsSubmit = 2
	s.NumClaimsFailedTotal = 3
	s.NumClaimsFailed = 0 
	s.NumVerificationsTotal = 7
	s.NumVerifications = 1
	s.NumWolkEarnedTotal = 2345.67
	s.NumWolkEarned = 78.89
	current_time := int(time.Now().Unix())
	s.CurrentTimestamp = current_time
	s.LastStartTimestamp =1510780000
	s.LastClaimSubmitTimestamp = current_time - 9
	s.LastChunkReadTimestamp =  current_time - 8
	s.LastChunkWriteTimestamp = current_time - 7
	return s
}

func (stats *NetStats) ToString() (s string) {
	
	data, err := json.Marshal(stats)
	if err != nil {
		return "{}"
		
	} else {
		return string(data)
	}
}

func main() {
	netstats := getSwarmNetStats()
	fmt.Printf("%s\n", netstats.ToString());
}