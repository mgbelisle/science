// https://en.wikipedia.org/wiki/Paxos_(computer_science)
// http://research.microsoft.com/en-us/um/people/lamport/pubs/paxos-simple.pdf

package main

import (
	"fmt"
	"math/rand"
)

var processors = map[int]*Processor{}

func main() {
	for i := 1; i <= 5; i++ {
		processors[i] = NewProcessor(i)
	}
	for _, processor := range processors {
		fmt.Printf("%v: %v\n", processor.ID, processor.Consensus().ID)
	}
}

func NewProcessor(id int) *Processor {
	return &Processor{
		ID: id,
	}
}

type Processor struct {
	ID          int
	MessageChan chan interface{}
}

func (p *Processor) Consensus() *Proposal {
	return &Proposal{}
}

type Proposal struct {
	ID    int
	Value interface{}
}
