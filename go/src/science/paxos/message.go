package paxos

import (
	"encoding/json"
)

const (
	readType = iota
	writeType
	phase1Type
	phase2Type
)

func encodeMessage(msg *message) []byte {
	data, _ := json.Marshal(msg)
	return data
}

type message struct {
	Type  int    `json:"type"`
	Key   uint64 `json:"key"`
	N     uint64 `json:"n"`
	Value []byte `json:"value"`
	Final bool   `json:"final"`
}
