package paxos

func NewNetwork() *Network {
	return &Network{
		nodes: map[string]*Node{},
	}
}

type Network struct {
	nodes map[string]*Node
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
