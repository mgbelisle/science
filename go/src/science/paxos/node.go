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

func RemoteNode(id string, channel chan<- []byte) *Node {
	return &Node{
		id:           id,
		channel:      channel,
		stdoutLogger: log.New(ioutil.Discard, "", log.LstdFlags),
		stderrLogger: log.New(ioutil.Discard, "", log.LstdFlags),
	}
}

func LocalNode(id string, channel <-chan []byte, network *Network, storage *Storage) *Node {
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
		ops := map[string]*message{} // {opId: msg}
		getState := func(key uint64) (*stateStruct, error) {
			stateBytes, err := storage.Get(key)
			if err != nil {
				return nil, err
			}
			state, err := &stateStruct{}, error(nil)
			if len(stateBytes) > 0 {
				err = json.Unmarshal(stateBytes, err)
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
				switch msg.Type {
				case phase1RequestType:
					state, err := getState(msg.Key)
					if err != nil {
						node.stderrLogger.Print(err)
						continue
					}
					if state.PromisedN < msg.N {
						state.PromisedN = msg.N
						if err := putState(msg.Key, state); err != nil {
							node.stderrLogger.Print(err)
							continue
						}
						node.stdoutLogger.Printf("%s promised N=%d to %s", id, msg.N, msg.Sender)
						go func() {
							network.nodes[msg.Sender].channel <- encodeMessage(&message{
								Sender: id,
								OpID:   msg.OpID,
								Type:   phase1ResponseType,
								N:      state.AcceptedN,
								Value:  state.Value,
							})
						}()
					}
				case phase1ResponseType:
				case phase2RequestType:
				case phase2ResponseType:
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
				ops[msg.OpID] = msg
				for _, node := range network.nodes {
					go func(node *Node) {
						node.channel <- encodeMessage(&message{
							Sender: id,
							OpID:   msg.OpID,
							Type:   phase1RequestType,
							N:      state.N,
						})
					}(node)
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
			OpID:         opID(),
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
			OpID:         opID(),
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
