package paxos

import (
	"encoding/json"
	"io/ioutil"
	"log"
)

type Node struct {
	channel      chan<- []byte
	readChan     chan<- *message
	writeChan    chan<- *message
	stdoutLogger *log.Logger
	stderrLogger *log.Logger
}

func RemoteNode(channel chan<- []byte) *Node {
	return &Node{
		channel:      channel,
		stdoutLogger: log.New(ioutil.Discard, "", log.LstdFlags),
		stderrLogger: log.New(ioutil.Discard, "", log.LstdFlags),
	}
}

func LocalNode(network *Network, channel <-chan []byte, storage *Storage) *Node {
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
		channel:      channel2,
		readChan:     readChan,
		writeChan:    writeChan,
		stdoutLogger: log.New(ioutil.Discard, "", log.LstdFlags),
		stderrLogger: log.New(ioutil.Discard, "", log.LstdFlags),
	}

	go func() {
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
					for node := range network.nodes {
						go func(node *Node) {
							node.channel <- encodeMessage(&message{})
						}(node)
					}
				case phase1ResponseType:
				case phase2RequestType:
				case phase2ResponseType:
				default:
					node.stderrLogger.Printf("Illegal message type: %d", msg.Type)
					continue
				}
			case msg := <-readChan:
				msg.ResponseChan <- nil
				msg.ErrChan <- nil
			case msg := <-writeChan:
				msg.ResponseChan <- nil
				msg.ErrChan <- nil
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
