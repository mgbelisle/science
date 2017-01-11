package paxos

import (
	"encoding/json"
	"log"
)

const (
	paxosType  = "paxos"
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
	Sender string `json:"s,omitempty"`
	Type   string `json:"t,omitempty"`
	N      int    `json:"n,omitempty"`
	Value  []byte `json:"v,omitempty"`
}
