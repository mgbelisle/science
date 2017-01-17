package paxos

import (
	"encoding/json"
)

const (
	readType   = "read"
	writeType  = "write"
	phase1Type = "phase1"
	phase2Type = "phase2"
)

func encodeMessage(msg *message) []byte {
	data, _ := json.Marshal(msg)
	return data
}

type message struct {
	Type  string `json:"t"`
	Key   uint64 `json:"k"`
	N     uint64 `json:"n"`
	Value []byte `json:"v"`
	Final bool   `json:"f"`
}
