package paxos

import (
	"encoding/json"
	"fmt"
	"sync"
)

type Node struct {
	handler Handler
}

type Handler func(request []byte) (response []byte, _ error)

func NewHandler(network *Network, storage *Storage) Handler {
	// State, uses mutex for accessing it
	mtx := sync.Mutex{}
	thisN := 0
	acceptedN := 0
	acceptedValue := []byte(nil)
	promisedN := 0
	promisedSender := ""

	node := func(request []byte) (response []byte, _ error) {
		msg := &message{}
		if err := json.Unmarshal(request, msg); err != nil {
			return nil, err
		}
		switch msg.Type {
		case writeType:
			// Paxos itself, keep trying rounds until successful
			for {
				mtx.Lock()
				thisN++
				phase1 := encodeMessage(&message{
					Type: phase1Type,
					N:    thisN,
				})
				mtx.Unlock()
				wg2 := sync.WaitGroup{}
				mtx2 := sync.Mutex{}
				alreadyAcceptedN := 0
				alreadyAcceptedValue := []byte(nil)
				numResponses := 0
				for id2, node2 := range network.nodes {
					// Contact each node in parallel
					wg2.Add(1)
					go func(id2 string, node2 Node) {
						defer wg2.Add(-1)
						response, err := node2.handler(phase1)
						if err != nil {
							network.stderrLogger.Printf("Node %s failed phase 1 with %s", id, id2)
							return
						}
						phase1Resp := &message{}
						if err := json.Unmarshal(response, phase1Resp); err != nil {
							network.stderrLogger.Printf("Error json decoding phase 1 response: %v\n%s", err, response)
							return
						}
						mtx2.Lock()
						defer mtx2.Unlock()
						if alreadyAcceptedN < phase1Resp.N {
							alreadyAcceptedN = phase1Resp.N
							alreadyAcceptedValue = phase1Resp.Value
						}
						numResponses++
					}(id2, node2)
				}
				wg2.Wait()
				if l := len(network.nodes); numResponses <= l/2 {
					network.stderrLogger.Printf("Node %s failed phase 1 %d/%d", id, l-numResponses, l)
					continue
				}
				value := msg.Value
				if 0 < alreadyAcceptedN {
					value = alreadyAcceptedValue
				}
				phase2 := encodeMessage(&message{
					Sender: id,
					Type:   phase2Type,
					N:      thisN,
					Value:  value,
				})
				numResponses = 0
				for id2, node2 := range network.nodes {
					if _, err := node2(phase2); err != nil {
						network.stderrLogger.Printf("Node %s failed phase 2 with %s", id, id2)
						continue
					}
					numResponses++
				}
				if l := len(network.nodes); numResponses <= l/2 {
					network.stderrLogger.Printf("Node %s failed phase 2 %d/%d", id, l-numResponses, l)
					continue
				}
				return encodeMessage(&message{
					Value: value,
				}), nil
			}
		case phase1Type:
			mtx.Lock()
			defer mtx.Unlock()
			if promisedN < msg.N {
				network.stdoutLogger.Printf("%s promised n=%d to %s", id, msg.N, msg.Sender)
				promisedN = msg.N
				promisedSender = msg.Sender
				return encodeMessage(&message{
					N:     acceptedN,
					Value: acceptedValue,
				}), nil
			}
			return nil, fmt.Errorf("%s cannot accept n=%d from %s, already promised n=%d to %s", id, msg.N, msg.Sender, promisedN, promisedSender)
		case phase2Type:
			mtx.Lock()
			defer mtx.Unlock()
			if msg.N < promisedN {
				return nil, fmt.Errorf("Cannot accept %d from %s, already promised %d to %s", msg.N, msg.Sender, promisedN, promisedSender)
			}
			network.stdoutLogger.Printf("%s accepted n=%d value=%s from %s", id, msg.N, msg.Value, msg.Sender)
			acceptedN = msg.N
			acceptedValue = msg.Value
			return nil, nil
		}
		return nil, fmt.Errorf("Illegal type: %s", msg.Type)
	}
	AddNode(network, node, id)
	return node
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
