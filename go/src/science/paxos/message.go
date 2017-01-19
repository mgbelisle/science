package paxos

import (
	"crypto/rand"
	"encoding/json"
	"fmt"
)

const (
	readRequestType    = "readRequest"
	readResponseType   = "readResponse"
	writeRequestType   = "writeRequest"
	writeResponseType  = "writeResponse"
	phase1RequestType  = "phase1Request"
	phase1ResponseType = "phase1Response"
	phase2RequestType  = "phase2Request"
	phase2ResponseType = "phase2Response"
)

func encodeMessage(msg *message) []byte {
	data, _ := json.Marshal(msg)
	return data
}

type message struct {
	ID    string `json:"id"`
	Type  string `json:"type"`
	Key   uint64 `json:"key"`
	N     uint64 `json:"n"`
	Value []byte `json:"value"`
	Final bool   `json:"final"`
	Err   string `json:"err"`
}

func messageID() string {
	b := make([]byte, 32)
	rand.Read(b)
	return fmt.Sprintf("%x", b)
}
