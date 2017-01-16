package paxos

import ()

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
