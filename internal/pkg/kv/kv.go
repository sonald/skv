package kv

import (
	"io"

	"github.com/sonald/skv/internal/pkg/storage"
)

const (
	DumpBySize = iota
	DumpByCount
)

type KV interface {
	Put(key string, value []byte) error
	Get(key string) ([]byte, error)
	Del(key string) error
	Scan(func(k string, v []byte) bool)
	MakeSnapshot(w io.WriteCloser) error
	GetSnapshot(r io.ReadCloser) (storage.Storage, error)
	Close()
	Stats()
}

type ServerConfig struct {
	Address    string
	ServerID   string
	Leader     bool
	State      string
	RpcAddress string
}

type Snapshot struct {
	Data storage.Storage
}
