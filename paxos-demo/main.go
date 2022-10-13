// http://research.microsoft.com/en-us/um/people/lamport/pubs/paxos-simple.pdf
//
// Scenario: Five IMF spies must coordinate a meetup. If they show up at different spots then
// they die, and their communication channels are slow and unreliable. Thankfully, they all
// understand the paxos algorithm perfectly.
//
// $ go run ./paxos-demo/main.go

package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"sync"

	"github.com/mgbelisle/science/paxos"
)

const usagePrefix = `Runs the paxos toy problem

Usage: go run ./main.go [OPTIONS]

OPTIONS:
`

var (
	verboseFlag = flag.Bool("verbose", false, "Verbose output")
)

func main() {
	// Setup
	flag.Usage = func() {
		fmt.Fprint(os.Stdout, usagePrefix)
		flag.PrintDefaults()
	}
	flag.Parse()
	network := paxos.NewNetwork()
	if *verboseFlag {
		network.SetLoggers(log.New(os.Stdout, "", log.LstdFlags), log.New(os.Stderr, "", log.LstdFlags))
	}
	nodes := map[string]*paxos.Node{}
	wg := &sync.WaitGroup{}
	agents := map[string]string{
		"Ethan Hunt":      "Tokyo",
		"Jim Phelps":      "Vegas",
		"Luther Stickell": "Rio de Janeiro",
		"Jack Harmon":     "Shanghai",
		"Franz Krieger":   "Berlin",
	}
	for agent := range agents {
		node := network.AddNode(agent, make(<-chan []byte), paxos.MemoryStorage())
		nodes[agent] = node
	}

	// Each agent tries to write key 0 simultaneously
	for agent, proposal := range agents {
		wg.Add(1)
		go func(agent, proposal string) {
			defer wg.Add(-1)
			value, err := nodes[agent].Write(context.Background(), 0, []byte(proposal))
			if err != nil {
				log.Printf("%s error: %v", agent, err)
				return
			}
			fmt.Printf("%s: %s\n", agent, value)
		}(agent, proposal)
	}
	wg.Wait()
}
