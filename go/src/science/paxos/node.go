package paxos

import (
	"context"
	"crypto/sha512"
	"encoding/json"
	"fmt"
)

// A node does read and write operations on the entire network
//
//     node.Read(ctx, key)
//     node.Write(ctx, key, value)
type Node struct {
	id        string
	channel   chan<- []byte
	readChan  chan<- *message
	writeChan chan<- *message
	cleanChan chan<- string
}

// Creates a local node on the network with storage
func (network *Network) AddNode(id string, channel <-chan []byte, storage *Storage) *Node {
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
	network.channels[id] = channel2

	// Start a single goroutine for this node and communicate with it via channels to make it
	// all thread safe
	go func() {
		msgMap := map[string]*message{}                                 // {opId: messageWithChannel}
		othersAcceptedNMap := map[string]uint64{}                       // {opId: n}
		othersAcceptedValueMap := map[string][]byte{}                   // {opId: value}
		proposedValueMap := map[string][]byte{}                         // {opId: value}
		write1WaitingMap := map[string]map[uint64]map[string]struct{}{} // {opId: {n: {sender: null}}}
		write2WaitingMap := map[string]map[uint64]map[string]struct{}{} // {opId: {n: {sender: null}}}
		readWaitingMap := map[string]map[string]struct{}{}              // {opId: {sender: null}}
		readCountMap := map[string]map[string]int{}                     // {opId: {hash: count}}
		readValueMap := map[string][]byte{}                             // {opId: value}
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
				// If state is final, inform the sender
				if state.Final && msg.Type != finalType {
					go func() {
						network.channels[msg.Sender] <- encodeMessage(&message{
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
					go func() {
						network.channels[msg.Sender] <- encodeMessage(&message{
							OpID:   msg.OpID,
							Sender: id,
							Type:   readResponseType,
							Key:    msg.Key,
							Value:  state.Value,
						})
					}()
				case readResponseType:
					if waitingMap, ok := readWaitingMap[msg.OpID]; ok {
						if _, ok := waitingMap[msg.Sender]; ok {
							delete(waitingMap, msg.Sender)

							hash := getHash(msg.Value)
							countMap := readCountMap[msg.OpID]

							// If a majority is not possible, finish
							others := 0
							for hash2, count := range countMap {
								if hash2 != hash {
									others += count
								}
							}
							if l := len(network.channels); l-others <= others {
								// Majority not possible
								continue
							}

							countMap[hash]++
							if countMap[getHash(readValueMap[msg.OpID])] < countMap[hash] {
								readValueMap[msg.OpID] = msg.Value
							}
							if l, c := len(network.channels), countMap[hash]; l-c < c {
								if msg.Value == nil {
									if msg2, ok := msgMap[msg.OpID]; ok {
										delete(msgMap, msg.OpID)
										go func() {
											msg2.ResponseChan <- nil
											msg2.ErrChan <- nil
										}()
										go func() {
											cleanChan <- msg.OpID
										}()
									}
								} else {
									// Majority have same value
									for _, channel2 := range network.channels {
										go func(channel2 chan<- []byte) {
											channel2 <- encodeMessage(&message{
												Type:   finalType,
												Sender: id,
												OpID:   msg.OpID,
												Key:    msg.Key,
												Value:  msg.Value,
											})
										}(channel2)
									}
								}
							}
						}
					}
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
							network.channels[msg.Sender] <- encodeMessage(&message{
								Type:      write1ResponseType,
								OpID:      msg.OpID,
								Sender:    id,
								N:         msg.N,
								AcceptedN: state.AcceptedN,
								Key:       msg.Key,
								Value:     state.Value,
							})
						}()
					} else {
						go func() {
							network.channels[msg.Sender] <- encodeMessage(&message{
								Type:   write1NackType,
								OpID:   msg.OpID,
								Sender: id,
								N:      msg.N,
								Key:    msg.Key,
							})
						}()
					}
				case write1ResponseType:
					if waitingMap1, ok := write1WaitingMap[msg.OpID]; ok {
						if waitingMap2, ok := waitingMap1[msg.N]; ok {
							if othersAcceptedNMap[msg.OpID] < msg.AcceptedN {
								othersAcceptedNMap[msg.OpID] = msg.AcceptedN
								othersAcceptedValueMap[msg.OpID] = msg.Value
							}
							delete(waitingMap2, msg.Sender)
							if n, w := len(network.channels), len(waitingMap2); w < n-w {
								// Majority have responded
								delete(waitingMap1, msg.N) // No longer waiting on phase1

								value := proposedValueMap[msg.OpID]
								if 0 < othersAcceptedNMap[msg.OpID] {
									value = othersAcceptedValueMap[msg.OpID]
								}

								waitingMap3, ok := write2WaitingMap[msg.OpID]
								if !ok {
									waitingMap3 = map[uint64]map[string]struct{}{}
									write2WaitingMap[msg.OpID] = waitingMap3
								}
								waitingMap4 := map[string]struct{}{}
								waitingMap3[msg.N] = waitingMap4
								for id2, channel2 := range network.channels {
									waitingMap4[id2] = struct{}{}
									go func(channel2 chan<- []byte) {
										channel2 <- encodeMessage(&message{
											Type:   write2RequestType,
											OpID:   msg.OpID,
											Sender: id,
											N:      msg.N,
											Key:    msg.Key,
											Value:  value,
										})
									}(channel2)
								}
							}
						}
					}
				case write1NackType:
					if waitingMap, ok := write1WaitingMap[msg.OpID]; ok {
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
							network.channels[msg.Sender] <- encodeMessage(&message{
								Type:   write2ResponseType,
								Sender: id,
								OpID:   msg.OpID,
								N:      msg.N,
								Key:    msg.Key,
								Value:  msg.Value,
							})
						}()
					} else {
						go func() {
							network.channels[msg.Sender] <- encodeMessage(&message{
								Type:   write2NackType,
								OpID:   msg.OpID,
								Sender: id,
								N:      msg.N,
								Key:    msg.Key,
							})
						}()
					}
				case write2ResponseType:
					if waitingMap1, ok := write2WaitingMap[msg.OpID]; ok {
						if waitingMap2, ok := waitingMap1[msg.N]; ok {
							delete(waitingMap2, msg.Sender)
							if n, w := len(network.channels), len(waitingMap2); w < n-w {
								// Majority have responded
								delete(waitingMap1, msg.N) // No longer waiting on phase2

								for _, channel2 := range network.channels {
									go func(channel2 chan<- []byte) {
										channel2 <- encodeMessage(&message{
											Type:   finalType,
											Sender: id,
											OpID:   msg.OpID,
											Key:    msg.Key,
											Value:  msg.Value,
										})
									}(channel2)
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
				countMap := map[string]int{}
				readCountMap[msg.OpID] = countMap
				for id2, channel2 := range network.channels {
					waitingMap[id2] = struct{}{}
					go func(channel2 chan<- []byte) {
						channel2 <- encodeMessage(&message{
							OpID:   msg.OpID,
							Sender: id,
							Type:   readRequestType,
							Key:    msg.Key,
						})
					}(channel2)
				}
			case msg := <-writeChan:
				// Start a round of Paxos for the given key
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
				for id2, channel2 := range network.channels {
					waitingMap2[id2] = struct{}{}
					go func(channel2 chan<- []byte) {
						channel2 <- encodeMessage(&message{
							Type:   write1RequestType,
							Sender: id,
							OpID:   msg.OpID,
							N:      state.N,
							Key:    msg.Key,
						})
					}(channel2)
				}
			case opID := <-cleanChan:
				// Cleanup after timeouts
				delete(msgMap, opID)
				delete(othersAcceptedNMap, opID)
				delete(othersAcceptedValueMap, opID)
				delete(proposedValueMap, opID)
				delete(write1WaitingMap, opID)
				delete(write2WaitingMap, opID)
				delete(readWaitingMap, opID)
				delete(readCountMap, opID)
				delete(readValueMap, opID)
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

// Read a key. Returns nil when the value does not exist. Use context
// if you want a timeout or cancelation.
func (node *Node) Read(ctx context.Context, key uint64) ([]byte, error) {
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

// Write a value. Returns the value that belongs to the key, which may be different than what you
// tried to write if the key was already written.
func (node *Node) Write(ctx context.Context, key uint64, value []byte) ([]byte, error) {
	if value == nil {
		return nil, &ErrNilValue{}
	}
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

func getHash(value []byte) string {
	if value == nil {
		return ""
	}
	return fmt.Sprintf("%x", sha512.Sum512(value))
}
