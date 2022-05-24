package kv

const (
	DumpBySize = iota
	DumpByCount
)

type KV interface {
	Put(key string, value string) error
	Get(key string) (string, error)
	Del(key string) error
	Scan(func(k, v string) bool)
	Close()
	Stats()
}
