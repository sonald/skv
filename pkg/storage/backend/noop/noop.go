package noop

import (
	"github.com/sonald/skv/pkg/storage"
)

type NoopStorage struct {
}

func (ns *NoopStorage) Close() {
}

func (ns *NoopStorage) Size() int {
	return 0
}

func (ns *NoopStorage) Count() int {
	return 0
}

func (ns *NoopStorage) Scan(f func(k string, v string) bool) {
}

func (ns *NoopStorage) Put(key, value string) error {
	return storage.ErrNotFound
}

func (ns *NoopStorage) Del(key string) error {
	return storage.ErrNotFound
}

func (ns *NoopStorage) Get(key string) (string, error) {
	return "", storage.ErrNotFound
}
