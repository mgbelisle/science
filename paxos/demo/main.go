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
	nodes := map[string]*paxos.Node{}
	wg := &sync.WaitGroup{}

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
		node := paxos.NewNode(make(chan []byte), paxos.MemoryStorage())
		if *verboseFlag {
			paxos.SetLoggers(node, log.New(os.Stdout, agent, log.LstdFlags), log.New(os.Stderr, agent, log.LstdFlags))
		}
		nodes[agent] = node
		paxos.AddNode(network, node)
	}
	for agent, proposal := range agents {
		wg.Add(1)
		go func(agent, proposal string) {
			defer wg.Add(-1)
			value, err := paxos.Write(nodes[agent], 0, []byte(proposal))
			if err != nil {
				log.Printf("%s error: %v", agent, err)
				return
			}
			fmt.Printf("%s: %s\n", agent, value)
		}(agent, proposal)
	}
	wg.Wait()
}
