package paxos

import (
	"encoding/json"
	"log"
)

const (
	readType   = "read"
	writeType  = "write"
	phase1Type = "phase1"
	phase2Type = "phase2"
)

// Response types are already known
func encodeMessage(msg *message) []byte {
	data, err := json.Marshal(msg)
	if err != nil {
		// Not sure how this could happen
		log.Fatalf("Error encoding message: %v", err)
	}
	return data
}

type message struct {
	Type  string `json:"t,omitempty"`
	Key   uint64 `json:"k,omitempty"`
	N     uint64 `json:"n,omitempty"`
	Value []byte `json:"v,omitempty"`
}
