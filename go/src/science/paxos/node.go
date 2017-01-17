package paxos

import (
	"encoding/json"
	"fmt"
	"sync"
	"sync/atomic"
)

type Node struct {
	handler Handler
}

type Handler func(request []byte) (response []byte, _ error)

func NewHandler(network *Network, storage *Storage) Handler {
	// A state mutex for each key
	stateMtxMapMtx := &sync.Mutex{} // The map itself needs a mutex
	stateMtxMap := map[uint64]*sync.Mutex{}
	getStateMtx := func(key uint64) *sync.Mutex {
		stateMtxMapMtx.Lock()
		defer stateMtxMapMtx.Unlock()
		stateMtx, ok := stateMtxMap[key]
		if !ok {
			stateMtx = &sync.Mutex{}
			stateMtxMap[key] = stateMtx
		}
		return stateMtx
	}

	return func(request []byte) (response []byte, _ error) {
		msg := &message{}
		if err := json.Unmarshal(request, msg); err != nil {
			return nil, err
		}
		stateMtx := getStateMtx(msg.Key)
		state, err := func() (*stateStruct, error) {
			stateMtx.Lock()
			defer stateMtx.Unlock()
			stateBytes, err := storage.Get(msg.Key)
			if err != nil {
				return nil, err
			}
			state, err := &stateStruct{}, error(nil)
			if 0 < len(stateBytes) {
				err := json.Unmarshal(stateBytes, state)
			}
			return state, err
		}()
		if err != nil {
			return nil, err
		}
		switch msg.Type {
		case readType:
			phase1 := encodeMessage(&message{Type: phase1Type})
			responseCounts := map[string]uint64{}
			mtx := &sync.Mutex{}
			wg := &sync.WaitGroup{}
			for node := range network.nodes {
				// TODO
			}
		case writeType:
			// Paxos itself, keep trying rounds until successful
			for {
				phase1, err := func() ([]byte, error) {
					stateMtx.Lock()
					defer stateMtx.Unlock()
					state.N++
					if err := storage.Put(msg.Key, encodeState(state)); err != nil {
						return nil, err
					}
					return encodeMessage(&message{
						Type: phase1Type,
						N:    state.N,
					}), nil
				}()
				if err != nil {
					network.stderrLogger.Printf("%v", err)
					continue
				}
				wg := sync.WaitGroup{}
				numResponses := uint64(0)
				for node2 := range network.nodes {
					// Contact each node in parallel
					wg.Add(1)
					go func(node2 *Node) {
						defer wg.Add(-1)
						response, err := node2.handler(phase1)
						if err != nil {
							network.stderrLogger.Printf("Failed phase 1: %v", err)
							return
						}
						phase1Resp := &message{}
						if err := json.Unmarshal(response, phase1Resp); err != nil {
							network.stderrLogger.Printf("Failed decoding phase 1 response: %v\n%s", err, response)
							return
						}
						stateMtx.Lock()
						defer stateMtx.Unlock()
						if state.AcceptedN < phase1Resp.N {
							state.AcceptedN = phase1Resp.N
							state.Value = phase1Resp.Value
						}
						if err := storage.Put(msg.Key, encodeState(state)); err != nil {
							network.stderrLogger.Printf("Failed storing state: %v", err)
							return
						}
						atomic.AddUint64(&numResponses, 1)
					}(node2)
				}
				wg.Wait()
				if l := len(network.nodes); int(numResponses) <= l/2 {
					network.stderrLogger.Printf("Failed phase 1 on %d/%d", l-int(numResponses), l)
					continue
				}
				
				phase2 := func() []byte {
					stateMtx.Lock()
					defer stateMtx.Unlock()
					value := msg.Value
					if 0 < state.AcceptedN {
						value = alreadyAcceptedValue
					}
					return  encodeMessage(&message{
						Type:   phase2Type,
						N:      thisN,
						Value:  value,
					})
				}()
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
