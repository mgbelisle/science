package paxos

import (
	"encoding/json"
	"fmt"
	"sync"
)

type Handler func(request []byte) (response []byte, _ error)

func NewHandler(network *Network, storage *Storage) Handler {
	return func(request []byte) (response []byte, _ error) {
		reqMsg := &message{}
		if err := json.Unmarshal(request, reqMsg); err != nil {
			return nil, err
		}
		respChan, errChan := make(chan *message), make(chan error)
		fooChan <- &fooStruct{
			Request:  reqMsg,
			Response: respChan,
			Err:      errChan,
			Network:  network,
			Storage:  storage,
		}
		respMsg, err := <-respChan, <-errChan
		if err != nil {
			return nil, err
		}
		response, err = json.Marshal(respMsg)
		return response, err
	}
}

type fooStruct struct {
	Request  *message
	Response chan *message
	Err      chan error
	Network  *Network
	Storage  *Storage
}

type fooManager struct {
	Chan chan *fooStruct
	N    uint64
}

var fooChan = make(chan *fooStruct)

func init() {
	go func() {
		// Manage the goroutines, one for each key, and do cleanup once they are unused
		managerMap := map[uint64]*fooManager{}
		managerMtx := &sync.Mutex{}
		for foo := range fooChan {
			managerMtx.Lock()
			manager, ok := managerMap[foo.Request.Key]
			if !ok {
				manager = &fooManager{
					Chan: make(chan *fooStruct),
				}
				managerMap[foo.Request.Key] = manager
			}
			managerMtx.Unlock()
			if !ok {
				go func(fooChan chan *fooStruct) {
					for foo := range fooChan {
						managerMtx.Lock()
						manager.N++
						managerMtx.Unlock()
						response, err := handle(foo.Request, foo.Network, foo.Storage)
						managerMtx.Lock()
						manager.N--
						if manager.N == 0 {
							close(fooChan)
							delete(managerMap, foo.Request.Key)
						}
						managerMtx.Unlock()
						foo.Response <- response
						foo.Err <- err
					}
				}(manager.Chan)
			}
		}
	}()
}

func handle(request *message, network *Network, storage *Storage) (response *message, _ error) {
	getState := func() (*stateStruct, error) {
		stateBytes, err := storage.Get(request.Key)
		if err != nil {
			return nil, err
		}
		state, err := &stateStruct{}, error(nil)
		if 0 < len(stateBytes) {
			err := json.Unmarshal(stateBytes, state)
		}
		return state, err
	}
	switch request.Type {
	case readType:
		// TODO
	case writeType:
		// Paxos itself, keep trying rounds until successful
		for {
			// Phase 1
			state, err := getState()
			if err != nil {
				network.stderrLogger.Printf("%v", err)
				continue
			}
			state.N++
			if err := storage.Put(request.Key, encodeState(state)); err != nil {
				return nil, err
			}
			phase1 := encodeMessage(&message{
				Type: phase1Type,
				N:    state.N,
			})
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
			state, err = getState()
			if err != nil {
				network.stderrLogger.Printf("%v", err)
				continue
			}
			value := request.Value
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
						continue
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
			return &message{Value: value}, nil
		}
	case phase1Type:
		state, err := getState()
		if err != nil {
			return nil, err
		}
		if state.PromisedN < request.N {
			network.stdoutLogger.Printf("Promised N=%d", request.N)
			state.PromisedN = request.N
			// TODO: Put state
			return &message{
				N:     state.AcceptedN,
				Value: state.Value,
			}, nil
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
		// TODO: Put state
		return nil, nil
	}
	return nil, fmt.Errorf("Illegal type: %s", request.Type)
}
