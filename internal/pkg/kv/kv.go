package kv

const (
	DumpBySize = iota
	DumpByCount
)

type KV interface {
	Put(key string, value []byte) error
	Get(key string) ([]byte, error)
	Del(key string) error
	Scan(func(k string, v []byte) bool)
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
