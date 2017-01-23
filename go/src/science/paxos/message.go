package paxos

import (
	"encoding/json"
	"fmt"
)

const (
	phase1RequestType = iota
	phase1ResponseType
	phase2RequestType
	phase2ResponseType
	finalType
	nackType
)

func encodeMessage(msg *message) []byte {
	data, _ := json.Marshal(msg)
	return data
}

type message struct {
	Sender    string `json:"sender"`
	Type      int    `json:"type"`
	Key       uint64 `json:"key"`
	N         uint64 `json:"n"`
	AcceptedN uint64 `json:"acceptedN"`
	Value     []byte `json:"value"`

	// For reading/writing
	ResponseChan chan<- []byte `json:"-"`
	ErrChan      chan<- error  `json:"-"`
}

func messageID(msg *message) string {
	return fmt.Sprintf("%d:%d", msg.Key, msg.N)
}
