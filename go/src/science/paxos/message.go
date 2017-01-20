package paxos

import (
	"crypto/rand"
	"encoding/json"
	"fmt"
)

const (
	phase1RequestType = iota
	phase1ResponseType
	phase2RequestType
	phase2ResponseType
)

func encodeMessage(msg *message) []byte {
	data, _ := json.Marshal(msg)
	return data
}

type message struct {
	Sender string `json:"sender"`
	OpID   string `json:"opId"`
	Type   int    `json:"type"`
	Key    uint64 `json:"key"`
	N      uint64 `json:"n"`
	Value  []byte `json:"value"`
	Final  bool   `json:"final"`

	// For reading/writing
	ResponseChan chan<- []byte `json:"-"`
	ErrChan      chan<- error  `json:"-"`
}

func opID() string {
	b := make([]byte, 32)
	rand.Read(b)
	return fmt.Sprintf("%x", b)
}
