package storage

import "errors"

type Storage interface {
	Put(key string, value string) error
	Get(key string) (string, error)
	Del(key string) error
	Scan(func(k, v string) bool)
	Close()

	// estimated size
	Size() int
	// estimated key count
	Count() int
}

type Options struct {
	Args map[string]interface{}
}

const Megabyte = 1048576

var (
	ErrNotFound = errors.New("key does not found")
)

const (
	SegmentOpenMode = "SegmentOpenMode"
)

const (
	SegmentOpenModeRO = iota
	SegmentOpenModeWR
	SegmentOpenModeRW
)

var storages = make(map[string]func(Options) Storage)

func RegisterBackend(name string, impl func(Options) Storage) {
	storages[name] = impl
}

func GetStorage(name string, options Options) Storage {
	return storages[name](options)
}
