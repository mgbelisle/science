package paxos

func NewNetwork() *Network {
	return &Network{
		nodes: map[*Node]struct{}{},
	}
}

type Network struct {
	nodes map[*Node]struct{}
}

func AddNode(network *Network, node *Node) {
	network.nodes[node] = struct{}{}
}
