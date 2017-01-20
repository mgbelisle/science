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
