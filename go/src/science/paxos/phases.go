package paxos

type phase1A struct {
	proposalN int
	channel   chan error
}

type phase1B struct {
	proposalN int
	leader    *Node
}

type phase2A struct {
}

type phase2B struct {
	proposalN int
	value     interface{}
}
