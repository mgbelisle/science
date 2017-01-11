package paxos

type ErrNilValue struct{}

func (e *ErrNilValue) Error() string {
	return "Value cannot be nil"
}
