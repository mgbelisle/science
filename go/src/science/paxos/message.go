package paxos

import (
	"crypto/rand"
	"encoding/json"
	"fmt"
)

const (
	readRequestType = iota
	readNackType
	write1RequestType
	write1ResponseType
	write1NackType
	write2RequestType
	write2ResponseType
	write2NackType
	finalType
)

func encodeMessage(msg *message) []byte {
	data, _ := json.Marshal(msg)
	return data
}

type message struct {
	Type      int    `json:"type"`
	Sender    string `json:"sender"`
	OpID      string `json:"opId"`
	Key       uint64 `json:"key"`
	Value     []byte `json:"value"`
	N         uint64 `json:"n"`
	AcceptedN uint64 `json:"acceptedN"`

	// For reading/writing
	ResponseChan chan<- []byte `json:"-"`
	ErrChan      chan<- error  `json:"-"`
}

func newOpID() string {
	bytes := make([]byte, 16)
	rand.Read(bytes)
	return fmt.Sprintf("%x", bytes)
}
