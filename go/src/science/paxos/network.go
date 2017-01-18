package paxos

import (
	"io/ioutil"
	"log"
	"sync"
)

func NewNetwork() *Network {
	// Manage the goroutines, one for each key, and do cleanup once they are unused
	fooChan := make(chan *fooStruct)
	go func() {
		managerMap := map[uint64]*fooManager{}
		managerMtx := &sync.Mutex{}
		for foo := range fooChan {
			managerMtx.Lock()
			manager, ok := managerMap[foo.Request.Key]
			if !ok {
				manager = &fooManager{
					Chan: make(chan *fooStruct),
				}
				managerMap[foo.Request.Key] = manager
			}
			managerMtx.Unlock()
			if !ok {
				go func(fooChan chan *fooStruct) {
					for foo := range fooChan {
						managerMtx.Lock()
						manager.N++
						managerMtx.Unlock()
						response, err := handle(foo.Request, foo.Network, foo.Storage)
						managerMtx.Lock()
						manager.N--
						if manager.N == 0 {
							close(fooChan)
							delete(managerMap, foo.Request.Key)
						}
						managerMtx.Unlock()
						foo.Response <- response
						foo.Err <- err
					}
				}(manager.Chan)
			}
		}
	}()

	return &Network{
		nodes:        map[*Node]struct{}{},
		fooChan:      fooChan,
		stdoutLogger: log.New(ioutil.Discard, "", log.LstdFlags),
		stderrLogger: log.New(ioutil.Discard, "", log.LstdFlags),
	}
}

type Network struct {
	nodes        map[*Node]struct{}
	fooChan      chan *fooStruct
	stdoutLogger *log.Logger
	stderrLogger *log.Logger
}

func AddNode(network *Network, node *Node) {
	network.nodes[node] = struct{}{}
}

func SetLoggers(network *Network, stdout, stderr *log.Logger) {
	network.stdoutLogger = stdout
	network.stderrLogger = stderr
}
