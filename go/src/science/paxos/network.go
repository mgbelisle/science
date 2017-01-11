package paxos

import (
	"io"
	"io/ioutil"
	"log"
	"os"
)

func NewNetwork() *Network {
	return &Network{
		nodes:        map[string]Node{},
		stdoutLogger: logger(ioutil.Discard),
		stderrLogger: logger(ioutil.Discard),
	}
}

type Network struct {
	nodes        map[string]Node
	stdoutLogger *log.Logger
	stderrLogger *log.Logger
}

func AddNode(network *Network, node Node, id string) {
	network.nodes[id] = node
}

func logger(writer io.Writer) *log.Logger {
	return log.New(writer, "", log.LstdFlags)
}

func SetVerbose(network *Network) {
	network.stdoutLogger = logger(os.Stdout)
	network.stderrLogger = logger(os.Stderr)
}
