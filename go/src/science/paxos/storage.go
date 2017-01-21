package paxos

type Storage struct {
	Get func(key uint64) (value []byte, _ error)
	Put func(key uint64, value []byte) error
}

func DiskStorage(dir string) *Storage {
	return &Storage{
		Get: func(key uint64) ([]byte, error) {
			return nil, nil // TODO
		},
		Put: func(key uint64, value []byte) error {
			return nil // TODO
		},
	}
}

// Non durable storage, only use for toy problems
func MemoryStorage() *Storage {
	m := map[uint64][]byte{}
	return &Storage{
		Get: func(key uint64) ([]byte, error) {
			return m[key], nil
		},
		Put: func(key uint64, value []byte) error {
			m[key] = value
			return nil
		},
	}
}

type stateStruct struct {
	N         uint64 `json:"n"`
	PromisedN uint64 `json:"promisedN"`
	Value     []byte `json:"value"`
	Final     bool   `json:"final"`
}
