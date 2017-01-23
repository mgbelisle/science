package paxos

import (
	"context"
	"encoding/json"
)

type Node struct {
	id        string
	channel   chan<- []byte
	readChan  chan<- *message
	writeChan chan<- *message
	cleanChan chan<- string
}

func NewNode(id string, channel <-chan []byte, network *Network, storage *Storage) *Node {
	// Everything from channel goes into channel2
	channel2 := make(chan []byte)
	go func() {
		for msgBytes := range channel {
			channel2 <- msgBytes
		}
		close(channel2)
	}()
	readChan := make(chan *message)
	writeChan := make(chan *message)
	cleanChan := make(chan string)

	go func() {
		msgMap := map[string]*message{}                                 // {opId: messageWithChannel}
		othersAcceptedNMap := map[string]uint64{}                       // {opId: n}
		othersAcceptedValueMap := map[string][]byte{}                   // {opId: value}
		proposedValueMap := map[string][]byte{}                         // {opId: value}
		write1WaitingMap := map[string]map[uint64]map[string]struct{}{} // {opId: {n: waiting}}
		write2WaitingMap := map[string]map[uint64]map[string]struct{}{} // {opId: {n: waiting}}
		readWaitingMap := map[string]map[string]struct{}{}              // {opId: waiting}
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

		for {
			select {
			case msgBytes, ok := <-channel2:
				if !ok {
					return // Channel is closed
				}
				network.stdoutLogger.Printf("%s: %s", id, msgBytes)

				msg := &message{}
				if err := json.Unmarshal(msgBytes, msg); err != nil {
					network.stderrLogger.Print(err)
					continue
				}

				// Get the state
				state, err := getState(msg.Key)
				if err != nil {
					network.stderrLogger.Print(err)
					continue
				}
				if state.Final && msg.Type != finalType {
					go func() {
						network.nodes[msg.Sender].channel <- encodeMessage(&message{
							Type:   finalType,
							Sender: id,
							OpID:   msg.OpID,
							Key:    msg.Key,
							Value:  state.Value,
						})
					}()
					continue
				}

				switch msg.Type {
				case readRequestType:
				case readNackType:
				case write1RequestType:
					if state.PromisedN < msg.N {
						// Promise
						state.PromisedN = msg.N
						if err := putState(msg.Key, state); err != nil {
							network.stderrLogger.Print(err)
							continue
						}
						// network.stdoutLogger.Printf("Promised N=%d to %s", msg.N, msg.Sender)
						go func() {
							network.nodes[msg.Sender].channel <- encodeMessage(&message{
								Type:   write1ResponseType,
								OpID:   msg.OpID,
								Sender: id,
								N:      state.AcceptedN,
								Key:    msg.Key,
								Value:  state.Value,
							})
						}()
					} else {
						go func() {
							network.nodes[msg.Sender].channel <- encodeMessage(&message{
								Type:   write1NackType,
								OpID:   msg.OpID,
								Sender: id,
								Key:    msg.Key,
							})
						}()
					}
				case write1ResponseType:
					if waitingMap1, ok := write1WaitingMap[msg.OpID]; ok {
						if waitingMap2, ok := waitingMap1[msg.N]; ok {
							if othersAcceptedNMap[msg.OpID] < msg.N {
								othersAcceptedNMap[msg.OpID] = msg.N
								othersAcceptedValueMap[msg.OpID] = msg.Value
							}
							delete(waitingMap2, msg.Sender)
							if n, w := len(network.nodes), len(waitingMap2); w < n-w {
								// Majority have responded
								delete(waitingMap1, msg.N) // No longer waiting on phase1

								value := proposedValueMap[msg.OpID]
								if 0 < othersAcceptedNMap[msg.OpID] {
									value = othersAcceptedValueMap[msg.OpID]
								}

								waitingMap2 := map[string]struct{}{}
								for id2, node2 := range network.nodes {
									waitingMap2[id2] = struct{}{}
									go func(node2 *Node) {
										node2.channel <- encodeMessage(&message{
											Type:   write2RequestType,
											OpID:   msg.OpID,
											Sender: id,
											N:      msg.N,
											Key:    msg.Key,
											Value:  value,
										})
									}(node2)
								}
							}
						}
					}
				case write1NackType:
					if waitingMap, ok := write2WaitingMap[msg.OpID]; ok {
						if _, ok := waitingMap[msg.N]; ok {
							delete(waitingMap, msg.N)
							msg2 := msgMap[msg.OpID]
							// Retry write
							go func() {
								writeChan <- msg2
							}()
						}
					}
				case write2RequestType:
					if state.PromisedN <= msg.N {
						state.AcceptedN = msg.N
						state.Value = msg.Value
						if err := putState(msg.Key, state); err != nil {
							network.stderrLogger.Print(err)
							continue
						}
						// network.stdoutLogger.Printf("Accepted Key=%d Value=%s Sender=%s N=%d", msg.Key, msg.Value, msg.Sender, msg.N)
						go func() {
							network.nodes[msg.Sender].channel <- encodeMessage(&message{
								Sender: id,
								OpID:   msg.OpID,
								Type:   write2ResponseType,
								N:      msg.N,
								Key:    msg.Key,
								Value:  msg.Value,
							})
						}()
					}
				case write2ResponseType:
					if waitingMap1, ok := write2WaitingMap[msg.OpID]; ok {
						if waitingMap2, ok := waitingMap1[msg.N]; ok {
							delete(waitingMap2, msg.Sender)
							if n, w := len(network.nodes), len(waitingMap2); w < n-w {
								// Majority have responded
								delete(waitingMap1, msg.N) // No longer waiting on phase2

								for _, node2 := range network.nodes {
									go func(node2 *Node) {
										node2.channel <- encodeMessage(&message{
											Type:   finalType,
											Sender: id,
											OpID:   msg.OpID,
											Key:    msg.Key,
											Value:  msg.Value,
										})
									}(node2)
								}
							}
						}
					}
				case write2NackType:
					if waitingMap, ok := write2WaitingMap[msg.OpID]; ok {
						if _, ok := waitingMap[msg.N]; ok {
							delete(waitingMap, msg.N)
							msg2 := msgMap[msg.OpID]
							// Retry write
							go func() {
								writeChan <- msg2
							}()
						}
					}
				case finalType:
					// network.stdoutLogger.Printf("Final Key=%d Value=%s", msg.Key, msg.Value)

					if msg2, ok := msgMap[msg.OpID]; ok {
						go func(msg2 *message) {
							msg2.ResponseChan <- msg.Value
							msg2.ErrChan <- nil
						}(msg2)

						if err := putState(msg.Key, &stateStruct{
							Value: msg.Value,
							Final: true,
						}); err != nil {
							network.stderrLogger.Print(err)
							continue
						}

						go func() {
							cleanChan <- msg.OpID
						}()
					}
				default:
					network.stderrLogger.Printf("Illegal message type: %d", msg.Type)
				}
			case msg := <-readChan:
				msgMap[msg.OpID] = msg
				waitingMap := map[string]struct{}{}
				readWaitingMap[msg.OpID] = waitingMap
				for id2, node2 := range network.nodes {
					waitingMap[id2] = struct{}{}
					go func(node2 *Node) {
						node2.channel <- encodeMessage(&message{
							OpID:   msg.OpID,
							Sender: id,
							Type:   readRequestType,
							Key:    msg.Key,
						})
					}(node2)
				}
			case msg := <-writeChan:
				state, err := getState(msg.Key)
				if err != nil {
					msg.ResponseChan <- nil
					msg.ErrChan <- err
					continue
				}
				state.N++
				if err := putState(msg.Key, state); err != nil {
					msg.ResponseChan <- nil
					msg.ErrChan <- err
					continue
				}

				proposedValueMap[msg.OpID] = msg.Value
				msgMap[msg.OpID] = msg
				waitingMap1, ok := write1WaitingMap[msg.OpID]
				if !ok {
					waitingMap1 = map[uint64]map[string]struct{}{}
					write1WaitingMap[msg.OpID] = waitingMap1
				}
				waitingMap2 := map[string]struct{}{}
				waitingMap1[state.N] = waitingMap2
				for id2, node2 := range network.nodes {
					waitingMap2[id2] = struct{}{}
					go func(node2 *Node) {
						node2.channel <- encodeMessage(&message{
							Type:   write1RequestType,
							Sender: id,
							OpID:   msg.OpID,
							N:      state.N,
							Key:    msg.Key,
						})
					}(node2)
				}
			case opID := <-cleanChan:
				delete(msgMap, opID)
				delete(othersAcceptedNMap, opID)
				delete(othersAcceptedValueMap, opID)
				delete(proposedValueMap, opID)
				delete(write1WaitingMap, opID)
				delete(write2WaitingMap, opID)
				delete(readWaitingMap, opID)
			}
		}
	}()

	return &Node{
		id:        id,
		channel:   channel2,
		readChan:  readChan,
		writeChan: writeChan,
		cleanChan: cleanChan,
	}
}

func Read(ctx context.Context, node *Node, key uint64) ([]byte, error) {
	respChan, errChan := make(chan []byte), make(chan error)
	opID := newOpID()
	go func() {
		node.readChan <- &message{
			OpID:         opID,
			Key:          key,
			ResponseChan: respChan,
			ErrChan:      errChan,
		}
	}()
	select {
	case resp := <-respChan:
		return resp, <-errChan
	case <-ctx.Done():
		go func() {
			node.cleanChan <- opID
		}()
		return nil, ctx.Err()
	}
}

func Write(ctx context.Context, node *Node, key uint64, value []byte) ([]byte, error) {
	respChan, errChan := make(chan []byte), make(chan error)
	opID := newOpID()
	go func() {
		node.writeChan <- &message{
			OpID:         opID,
			Key:          key,
			Value:        value,
			ResponseChan: respChan,
			ErrChan:      errChan,
		}
	}()
	select {
	case resp := <-respChan:
		return resp, <-errChan
	case <-ctx.Done():
		go func() {
			node.cleanChan <- opID
		}()
		return nil, ctx.Err()
	}
}
