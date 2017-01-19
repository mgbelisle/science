package paxos

import (
	"errors"
	"encoding/json"
	"io/ioutil"
	"log"
)

type Node struct {
	in        chan<- []byte
	out       <-chan []byte
	stdoutLogger *log.Logger
	stderrLogger *log.Logger
}

func LocalNode(in chan []byte, storage *Storage) *Node {
	node := &Node{
		in:        in,
		out:       make(chan []byte),
		stdoutLogger: log.New(ioutil.Discard, "", log.LstdFlags),
		stderrLogger: log.New(ioutil.Discard, "", log.LstdFlags),
	}

	go func() {

	}()

	return node
}

func RemoteNode(in chan<- []byte, out <-chan []byte) *Node {
	return &Node{
		in:        in,
		out:       out,
		stdoutLogger: log.New(ioutil.Discard, "", log.LstdFlags),
		stderrLogger: log.New(ioutil.Discard, "", log.LstdFlags),
	}
}

func Read(node *Node, key uint64) ([]byte, error) {
	msgID := messageID()
	go func() {
		node.in <- encodeMessage(&message{
			ID:   msgID,
			Type: readRequestType,
			Key:  key,
		})
	}()
	for outBytes := range node.out {
		outMsg := &message{}
		if err := json.Unmarshal(outBytes, outMsg); err != nil {
			node.stderrLogger.Print(err)
		}
		if outMsg.ID == msgID {
			if outMsg.Err != "" {
				return outMsg.Value, errors.New(outMsg.Err)
			}
			return outMsg.Value, nil
		}
		go func(bytes []byte) {
			node.out <- bytes
		}(outBytes)
	}
	
	return nil, nil
}

func Write(node *Node, key uint64, value []byte) ([]byte, error) {
	return nil, nil
}

func SetLoggers(node *Node, stdout, stderr *log.Logger) {
	node.stdoutLogger = stdout
	node.stderrLogger = stderr
}
