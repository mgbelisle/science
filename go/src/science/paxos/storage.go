package paxos

type Storage struct {
	Get func(uint64) ([]byte, error)
	Put func(uint64, []byte) error
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

// Not durable storage, only use for toy problems
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

func encodeState(state *stateStruct) []byte {
	data, _ := json.Marshal(state)
	return data
}

type stateStruct struct {
	N         uint64 `json:"n"`
	PromisedN uint64 `json:"pn"`
	AcceptedN uint64 `json:"an"`
	Value     []byte `json:"v"`
	Final     bool   `json:"f"`
}
