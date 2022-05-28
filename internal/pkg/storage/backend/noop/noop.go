package noop

import (
	"github.com/sonald/skv/internal/pkg/storage"
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

func (ns *NoopStorage) Scan(f func(k *storage.InternalKey, v []byte) bool) {
}

func (ns *NoopStorage) Put(key *storage.InternalKey, value []byte) error {
	return storage.ErrNotFound
}

func (ns *NoopStorage) Del(key *storage.InternalKey) error {
	return storage.ErrNotFound
}

func (ns *NoopStorage) Get(key *storage.InternalKey) ([]byte, error) {
	return nil, storage.ErrNotFound
}
