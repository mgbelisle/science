package paxos

import (
	"io/ioutil"
	"log"
)

func NewNetwork() *Network {
	return &Network{
		nodes:        map[string]*Node{},
		stdoutLogger: log.New(ioutil.Discard, "", log.LstdFlags),
		stderrLogger: log.New(ioutil.Discard, "", log.LstdFlags),
	}
}

type Network struct {
	nodes        map[string]*Node
	stdoutLogger *log.Logger
	stderrLogger *log.Logger
}

func AddNode(network *Network, node *Node) {
	network.nodes[node.id] = node
}

func AddRemoteNode(network *Network, id string, channel chan<- []byte) {
	network.nodes[id] = &Node{
		id:      id,
		channel: channel,
	}
}

func SetLoggers(network *Network, stdout, stderr *log.Logger) {
	network.stdoutLogger = stdout
	network.stderrLogger = stderr
}
