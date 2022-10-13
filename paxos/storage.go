package paxos

import (
	"fmt"
	"io/ioutil"
	"os"
	"path"
)

// Storage is for persisting state, since nodes support failure. No need to implement your own
// mutexes since nodes are already thread safe.
type Storage struct {
	Get func(key uint64) (value []byte, _ error)
	Put func(key uint64, value []byte) error
}

// Durable storage on disk
func DiskStorage(dir string) *Storage {
	fname := func(key uint64) string {
		return path.Join(dir, fmt.Sprintf("%d.json", key))
	}
	return &Storage{
		Get: func(key uint64) ([]byte, error) {
			value, err := ioutil.ReadFile(fname(key))
			if os.IsNotExist(err) {
				return value, nil
			}
			return value, err
		},
		Put: func(key uint64, value []byte) error {
			err := os.MkdirAll(dir, 0700)
			if err != nil {
				return err
			}
			return ioutil.WriteFile(fname(key), value, 0600)
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
	AcceptedN uint64 `json:"acceptedN"`
	Value     []byte `json:"value"`
	Final     bool   `json:"final"`
}
