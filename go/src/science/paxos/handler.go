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
						response, err := handle(foo.Request)
						managerMtx.Lock()
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

func handle(request *message) (response *message, _ error) {
	state, err := func() (*stateStruct, error) {
		stateBytes, err := storage.Get(request.Key)
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
	switch request.Type {
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
				if err := storage.Put(request.Key, encodeState(state)); err != nil {
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

			// Phase 1
			wg := sync.WaitGroup{}
			mtx := &sync.Mutex{}
			alreadyAcceptedN := uint64(0)
			alreadyAcceptedValue := []byte(nil)
			numResponses := 0
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
					mtx.Lock()
					defer mtx.Unlock()
					if state.AcceptedN < phase1Resp.N {
						alreadyAcceptedN = phase1Resp.N
						alreadyAcceptedValue = phase1Resp.Value
					}
					if err := storage.Put(request.Key, encodeState(state)); err != nil {
						network.stderrLogger.Printf("Failed storing state: %v", err)
						return
					}
					numResponses++
				}(node2)
			}
			wg.Wait()
			if l := len(network.nodes); numResponses <= l/2 {
				network.stderrLogger.Printf("Failed phase 1 on %d/%d", l-int(numResponses), l)
				continue
			}

			// Phase 2
			value := request.Value
			if 0 < alreadyAcceptedN {
				value = alreadyAcceptedValue
			}
			phase2 := func() []byte {
				stateMtx.Lock()
				defer stateMtx.Unlock()
				return encodeMessage(&message{
					Type:  phase2Type,
					N:     state.N,
					Value: value,
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
		if promisedN < request.N {
			network.stdoutLogger.Printf("%s promised n=%d to %s", id, request.N, request.Sender)
			promisedN = request.N
			promisedSender = request.Sender
			return encodeMessage(&message{
				N:     acceptedN,
				Value: acceptedValue,
			}), nil
		}
		return nil, fmt.Errorf("%s cannot accept n=%d from %s, already promised n=%d to %s", id, request.N, request.Sender, promisedN, promisedSender)
	case phase2Type:
		mtx.Lock()
		defer mtx.Unlock()
		if request.N < promisedN {
			return nil, fmt.Errorf("Cannot accept %d from %s, already promised %d to %s", request.N, request.Sender, promisedN, promisedSender)
		}
		network.stdoutLogger.Printf("%s accepted n=%d value=%s from %s", id, request.N, request.Value, request.Sender)
		acceptedN = request.N
		acceptedValue = request.Value
		return nil, nil
	}
	return nil, fmt.Errorf("Illegal type: %s", request.Type)
}
