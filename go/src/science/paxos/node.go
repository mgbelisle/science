package paxos

import (
	"encoding/json"
)

type Node struct {
	handler Handler
}

func Read(node Node, key uint64) ([]byte, error) {
	respBytes, err := node(encodeMessage(&message{
		Type: readType,
		Key:  key,
	}))
	if err != nil {
		return nil, err
	}
	resp := &message{}
	err = json.Unmarshal(respBytes, resp)
	return resp.Value, err
}

func Write(node Node, key uint64, value []byte) ([]byte, error) {
	respBytes, err := node(encodeMessage(&message{
		Type:  writeType,
		Key:   key,
		Value: value,
	}))
	if err != nil {
		return nil, err
	}
	resp := &message{}
	err = json.Unmarshal(respBytes, resp)
	return resp.Value, err
}
