package paxos

import (
	"io/ioutil"
	"log"
)

func NewNetwork() *Network {
	return &Network{
		channels:     map[string]chan<- []byte{},
		stdoutLogger: log.New(ioutil.Discard, "", log.LstdFlags),
		stderrLogger: log.New(ioutil.Discard, "", log.LstdFlags),
	}
}

type Network struct {
	channels     map[string]chan<- []byte
	stdoutLogger *log.Logger
	stderrLogger *log.Logger
}

func (network *Network) AddRemoteNode(id string, channel chan<- []byte) {
	network.channels[id] = channel
}

func (network *Network) SetLoggers(stdout, stderr *log.Logger) {
	network.stdoutLogger = stdout
	network.stderrLogger = stderr
}
