package paxos

import (
	"io/ioutil"
	"log"
	"sync"
)

func NewNetwork() *Network {
	network := &Network{
		nodes:        map[*Node]struct{}{},
		handlerChan:  make(chan *handlerStruct),
		stdoutLogger: log.New(ioutil.Discard, "", log.LstdFlags),
		stderrLogger: log.New(ioutil.Discard, "", log.LstdFlags),
	}

	// Manage the goroutines, one for each key, and do cleanup once they are unused
	go func() {
		managerMap := map[uint64]*handlerManager{}
		managerMapMtx := &sync.Mutex{}
		for handler := range network.handlerChan {
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
						response, err := handle(handler.Request, network, handler.Storage)
						manager.Mutex.Lock()
						manager.N--
						if manager.N == 0 {
							close(manager.Chan)
							delete(managerMap, handler.Request.Key)
						}
						manager.Mutex.Unlock()
						handler.Response <- response
						handler.Err <- err
					}
				}(manager)
			}
			go func(handler *handlerStruct, manager *handlerManager) {
				manager.Chan <- handler
			}(handler, manager)
		}
	}()

	return network
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
