package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"sync"

	"science/paxos"
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
		paxos.SetLoggers(network, os.Stdout, os.Stderr)
	}
	nodes := map[string]paxos.Node{}
	wg := sync.WaitGroup{}

	// Awesome scenario: Five spies must coordinate a meetup. If they show up at different spots
	// then they die, and their communication channels are slow and unreliable. Thankfully, they
	// all understand the paxos protocol perfectly.
	agents := map[string]string{
		"Ethan Hunt":      "Tokyo",
		"Jim Phelps":      "Vegas",
		"Luther Stickell": "Rio de Janeiro",
		"Jack Harmon":     "Shanghai",
		"Franz Krieger":   "Berlin",
	}
	for agent := range agents {
		nodes[agent] = paxos.AddNode(network, paxos.NewNode(paxos.NewHandler(network, paxos.MemoryStorage())))
	}
	for agent, proposal := range agents {
		wg.Add(1)
		go func(agent, proposal string) {
			defer wg.Add(-1)
			value, err := paxos.Consensus(nodes[agent], []byte(proposal))
			if err != nil {
				log.Printf("%s error: %v", agent, err)
				return
			}
			fmt.Printf("%s: %s\n", agent, value)
		}(agent, proposal)
	}
	wg.Wait()
}
