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
	finalType
)

func encodeMessage(msg *message) []byte {
	data, _ := json.Marshal(msg)
	return data
}

type message struct {
	Sender    string `json:"sender"`
	RequestID string `json:"requestId"`
	Type      int    `json:"type"`
	Key       uint64 `json:"key"`
	N         uint64 `json:"n"`
	Value     []byte `json:"value"`

	// For reading/writing
	ResponseChan chan<- []byte `json:"-"`
	ErrChan      chan<- error  `json:"-"`
}

func requestID() string {
	b := make([]byte, 16)
	rand.Read(b)
	return fmt.Sprintf("%x", b)
}
