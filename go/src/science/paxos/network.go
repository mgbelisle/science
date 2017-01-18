package paxos

import (
	"io/ioutil"
	"log"
	"sync"
)

func NewNetwork() *Network {
	// Manage the goroutines, one for each key, and do cleanup once they are unused
	handlerChan := make(chan *handlerStruct)
	go func() {
		managerMap := map[uint64]*handlerManager{}
		managerMapMtx := &sync.Mutex{}
		for handler := range handlerChan {
			managerMapMtx.Lock()
			manager, ok := managerMap[handler.Request.Key]
			if !ok {
				manager = &handlerManager{
					Chan:  make(chan *handlerStruct),
					Mutex: &sync.Mutex{},
				}
				managerMap[handler.Request.Key] = manager
			}
			managerMapMtx.Unlock()
			if !ok {
				go func(manager *handlerManager) {
					for handler := range manager.Chan {
						manager.Mutex.Lock()
						manager.N++
						manager.Mutex.Unlock()
						response, err := handle(handler.Request, handler.Network, handler.Storage)
						manager.Mutex.Lock()
						manager.N--
						if manager.N == 0 {
							close(handlerChan)
							delete(managerMap, handler.Request.Key)
						}
						manager.Mutex.Unlock()
						handler.Response <- response
						handler.Err <- err
					}
				}(manager)
			}
		}
	}()

	return &Network{
		nodes:        map[*Node]struct{}{},
		handlerChan:  handlerChan,
		stdoutLogger: log.New(ioutil.Discard, "", log.LstdFlags),
		stderrLogger: log.New(ioutil.Discard, "", log.LstdFlags),
	}
}

type Network struct {
	nodes        map[*Node]struct{}
	handlerChan  chan *handlerStruct
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
