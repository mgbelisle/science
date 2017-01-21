package paxos

import (
	"encoding/json"
	"io/ioutil"
	"log"
)

type Node struct {
	id           string
	channel      chan<- []byte
	readChan     chan<- *message
	writeChan    chan<- *message
	stdoutLogger *log.Logger
	stderrLogger *log.Logger
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
	node := &Node{
		id:           id,
		channel:      channel2,
		readChan:     readChan,
		writeChan:    writeChan,
		stdoutLogger: log.New(ioutil.Discard, "", log.LstdFlags),
		stderrLogger: log.New(ioutil.Discard, "", log.LstdFlags),
	}

	go func() {
		keyToMsgMap := map[uint64]map[*message]struct{}{} // {key: messages}
		othersAcceptedNMap := map[uint64]uint64{}         // {key: n}
		othersAcceptedValueMap := map[uint64][]byte{}     // {key: value}
		proposedValueMap := map[uint64][]byte{}
		requestIDToWaitingMap := map[string]map[string]struct{}{}
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

				msg := &message{}
				if err := json.Unmarshal(msgBytes, msg); err != nil {
					node.stderrLogger.Print(err)
					continue
				}

				// Get the state
				state, err := getState(msg.Key)
				if err != nil {
					node.stderrLogger.Print(err)
					continue
				}
				if state.Final && msg.Type != finalType {
					go func() {
						network.nodes[msg.Sender].channel <- encodeMessage(&message{
							Type:  finalType,
							Key:   msg.Key,
							Value: state.Value,
						})
					}()
					continue
				}

				switch msg.Type {
				case phase1RequestType:
					if state.PromisedN < msg.N {
						state.PromisedN = msg.N
						if err := putState(msg.Key, state); err != nil {
							node.stderrLogger.Print(err)
							continue
						}
						node.stdoutLogger.Printf("Promised N=%d to %s", msg.N, msg.Sender)
						go func() {
							network.nodes[msg.Sender].channel <- encodeMessage(&message{
								Sender: id,
								Type:   phase1ResponseType,
								N:      state.AcceptedN,
								Key:    msg.Key,
								Value:  state.Value,
							})
						}()
					}
				case phase1ResponseType:
					if waitingMap, ok := requestIDToWaitingMap[msg.RequestID]; ok {
						if othersAcceptedNMap[msg.Key] < msg.N {
							othersAcceptedNMap[msg.Key] = msg.N
							othersAcceptedValueMap[msg.Key] = msg.Value
						}
						delete(waitingMap, msg.Sender)
						if n, w := len(network.nodes), len(waitingMap); w < n-w {
							// Majority have responded, cleanup and send phase 2
							delete(requestIDToWaitingMap, msg.RequestID)
							value := proposedValueMap[msg.Key]
							if 0 < othersAcceptedNMap[msg.Key] {
								value = othersAcceptedValueMap[msg.Key]
							}

							reqID := requestID()
							waitingMap2 := map[string]struct{}{}
							requestIDToWaitingMap[reqID] = waitingMap2
							for id2, node2 := range network.nodes {
								waitingMap2[id2] = struct{}{}
								go func(node2 *Node) {
									node2.channel <- encodeMessage(&message{
										Sender:    id,
										RequestID: reqID,
										Type:      phase2RequestType,
										N:         state.N,
										Key:       msg.Key,
										Value:     value,
									})
								}(node2)
							}
						}
					}
				case phase2RequestType:
					if state.PromisedN <= msg.N {
						state.AcceptedN = msg.N
						state.Value = msg.Value
						if err := putState(msg.Key, state); err != nil {
							node.stdoutLogger.Print(err)
							continue
						}
						node.stdoutLogger.Printf("Accepted Key=%d Value=%s Sender=%s N=%d", msg.Key, msg.Value, msg.Sender, msg.N)
						go func() {
							network.nodes[msg.Sender].channel <- encodeMessage(&message{
								Sender: id,
								Type:   phase2ResponseType,
								Key:    msg.Key,
								Value:  msg.Value,
							})
						}()
					}
				case phase2ResponseType:
					if waitingMap, ok := requestIDToWaitingMap[msg.RequestID]; ok {
						delete(waitingMap, msg.Sender)
						if n, w := len(network.nodes), len(waitingMap); w < n-w {
							// Majority have responded
							delete(requestIDToWaitingMap, msg.RequestID)

							for _, node2 := range network.nodes {
								go func(node2 *Node) {
									node2.channel <- encodeMessage(&message{
										Type:  finalType,
										Key:   msg.Key,
										Value: msg.Value,
									})
								}(node2)
							}
						}
					}
				case finalType:
					for msg2 := range keyToMsgMap[msg.Key] {
						go func(msg2 *message) {
							msg2.ResponseChan <- msg.Value
							msg2.ErrChan <- nil
						}(msg2)
					}
					node.stdoutLogger.Printf("Final Key=%d Value=%s", msg.Key, msg.Value)

					if err := putState(msg.Key, &stateStruct{
						Value: msg.Value,
						Final: true,
					}); err != nil {
						node.stderrLogger.Print(err)
					}

					delete(keyToMsgMap, msg.Key)
					delete(othersAcceptedNMap, msg.Key)
					delete(othersAcceptedValueMap, msg.Key)
					delete(proposedValueMap, msg.Key)
				default:
					node.stderrLogger.Printf("Illegal message type: %d", msg.Type)
				}
			case msg := <-readChan:
				msg.ResponseChan <- nil
				msg.ErrChan <- nil
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

				messagesMap, ok := keyToMsgMap[msg.Key]
				if !ok {
					messagesMap := map[*message]struct{}{}
					keyToMsgMap[msg.Key] = messagesMap
				}
				messagesMap[msg] = struct{}{}
				reqID := requestID()
				waitingMap := map[string]struct{}{}
				requestIDToWaitingMap[reqID] = waitingMap
				for id2, node2 := range network.nodes {
					waitingMap[id2] = struct{}{}
					go func(node2 *Node) {
						node2.channel <- encodeMessage(&message{
							Sender:    id,
							RequestID: reqID,
							Type:      phase1RequestType,
							N:         state.N,
							Key:       msg.Key,
						})
					}(node2)
				}
			}
		}
	}()

	return node
}

func Read(node *Node, key uint64) ([]byte, error) {
	respChan, errChan := make(chan []byte), make(chan error)
	go func() {
		node.readChan <- &message{
			Key:          key,
			ResponseChan: respChan,
			ErrChan:      errChan,
		}
	}()
	return <-respChan, <-errChan
}

func Write(node *Node, key uint64, value []byte) ([]byte, error) {
	respChan, errChan := make(chan []byte), make(chan error)
	go func() {
		node.writeChan <- &message{
			Key:          key,
			Value:        value,
			ResponseChan: respChan,
			ErrChan:      errChan,
		}
	}()
	return <-respChan, <-errChan
}

func SetLoggers(node *Node, stdout, stderr *log.Logger) {
	node.stdoutLogger = stdout
	node.stderrLogger = stderr
}
