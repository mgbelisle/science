package paxos

import (
	"encoding/json"
	"fmt"
	"sync"
)

type Handler func(request []byte) (response []byte, _ error)

func NewHandler(network *Network, storage *Storage) Handler {
	stateMtx := &sync.Mutex{}
	getState := func(key uint64) (*stateStruct, error) {
		stateBytes, err := storage.Get(key)
		if err != nil {
			return nil, err
		}
		state, err := &stateStruct{}, error(nil)
		if 0 < len(stateBytes) {
			err = json.Unmarshal(stateBytes, state)
		}
		return state, err
	}
	putState := func(key uint64, state *stateStruct) error {
		stateBytes, err := json.Marshal(state)
		if err != nil {
			return err
		}
		return storage.Put(key, stateBytes)
	}

	return func(request []byte) (response []byte, _ error) {
		reqMsg := &message{}
		if err := json.Unmarshal(request, reqMsg); err != nil {
			return nil, err
		}

		switch reqMsg.Type {
		case readType:
			// TODO
		case writeType:
			// Paxos itself, keep trying rounds until successful
			for {
				// Phase 1
				phase1, err := func() ([]byte, error) {
					stateMtx.Lock()
					defer stateMtx.Unlock()
					state, err := getState(reqMsg.Key)
					if err != nil {
						return nil, err
					}
					state.N++
					if err := putState(reqMsg.Key, state); err != nil {
						return nil, err
					}
					return encodeMessage(&message{
						Type: phase1Type,
						N:    state.N,
					}), nil
				}()
				if err != nil {
					network.stderrLogger.Print(err)
					continue
				}

				wg := &sync.WaitGroup{}
				mtx := &sync.Mutex{}
				alreadyAcceptedN := uint64(0)
				alreadyAcceptedValue := []byte(nil)
				numResponses := 0
				for node := range network.nodes {
					// Contact each node in parallel
					wg.Add(1)
					go func(node *Node) {
						defer wg.Add(-1)
						response, err := node.handler(phase1)
						if err != nil {
							network.stderrLogger.Printf("Failed phase 1: %v", err)
							return
						}
						phase1Resp := &message{}
						if err := json.Unmarshal(response, phase1Resp); err != nil {
							network.stderrLogger.Printf("Failed decoding phase 1 response: %v\n%s", err, response)
							return
						}
						mtx.Lock()
						defer mtx.Unlock()
						if alreadyAcceptedN < phase1Resp.N {
							alreadyAcceptedN = phase1Resp.N
							alreadyAcceptedValue = phase1Resp.Value
						}
						numResponses++
					}(node)
				}
				wg.Wait()
				if l := len(network.nodes); numResponses <= l/2 {
					network.stderrLogger.Printf("Failed phase 1 on %d/%d", l-int(numResponses), l)
					continue
				}

				// Phase 2
				state, err = getState(reqMsg.Key) // State changed when it handled phase 1
				if err != nil {
					network.stderrLogger.Print(err)
					continue
				}
				value := reqMsg.Value
				if 0 < alreadyAcceptedN {
					value = alreadyAcceptedValue
				}
				phase2 := encodeMessage(&message{
					Type:  phase2Type,
					N:     state.N,
					Value: value,
				})
				wg = &sync.WaitGroup{}
				mtx = &sync.Mutex{}
				numResponses = 0
				for node := range network.nodes {
					wg.Add(1)
					go func(node *Node) {
						defer wg.Add(-1)
						if _, err := node.handler(phase2); err != nil {
							network.stderrLogger.Printf("Failed phase 2: %v", err)
							return
						}
						mtx.Lock()
						numResponses++
						mtx.Unlock()
					}(node)
				}
				wg.Wait()
				if l := len(network.nodes); numResponses <= l/2 {
					network.stderrLogger.Printf("Failed phase 2 on %d/%d", l-numResponses, l)
					continue
				}
				return encodeMessage(&message{Value: value}), nil
			}
		case phase1Type:
			state, err := getState()
			if err != nil {
				return nil, err
			}
			if state.PromisedN < request.N {
				network.stdoutLogger.Printf("Promised N=%d", request.N)
				state.PromisedN = request.N
				return &message{
					N:     state.AcceptedN,
					Value: state.Value,
				}, putState(state)
			}
			return nil, fmt.Errorf("Cannot accept N=%d, already promised N=%d", request.N, state.PromisedN)
		case phase2Type:
			state, err := getState()
			if err != nil {
				return nil, err
			}
			if request.N < state.PromisedN {
				return nil, fmt.Errorf("Cannot accept N=%d, already promised N=%d", request.N, state.PromisedN)
			}
			network.stdoutLogger.Printf("Accepted N=%d value=%s", request.N, request.Value)
			state.AcceptedN = request.N
			state.Value = request.Value
			return nil, putState(state)
		}
		return nil, fmt.Errorf("Illegal type: %s", request.Type)
	}
}
