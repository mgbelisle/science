package paxos

import (
	// "encoding/json"
	"io/ioutil"
	"log"
)

type Node struct {
	channel      <-chan []byte
	stdoutLogger *log.Logger
	stderrLogger *log.Logger
}

func LocalNode(channel <-chan []byte, storage *Storage) *Node {
	node := &Node{
		channel:      channel,
		stdoutLogger: log.New(ioutil.Discard, "", log.LstdFlags),
		stderrLogger: log.New(ioutil.Discard, "", log.LstdFlags),
	}

	go func() {

	}()

	return node
}

func RemoteNode() *Node {
	return nil
}

func Read(node *Node, key uint64) ([]byte, error) {
	return nil, nil
}

func Write(node *Node, key uint64, value []byte) ([]byte, error) {
	return nil, nil
}

func SetLoggers(node *Node, stdout, stderr *log.Logger) {
	node.stdoutLogger = stdout
	node.stderrLogger = stderr
}
