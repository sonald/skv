package mem

import (
	"bytes"
	"github.com/huandu/skiplist"
	"github.com/sonald/skv/internal/pkg/storage"
	"log"
)

func init() {
	log.Println("memory backend init")
	storage.RegisterBackend("mem", NewMemStorage)
}

type MemStorage struct {
	sl   *skiplist.SkipList
	size int
}

func (ms MemStorage) Close() {
	ms.sl = nil
}

func (ms MemStorage) Size() int {
	return 0
}

func (ms MemStorage) Count() int {
	return ms.sl.Len()
}

func (ms MemStorage) Scan(f func(k *storage.InternalKey, v []byte) bool) {
	elem := ms.sl.Front()
	for elem != nil {
		if !f(elem.Key().(*storage.InternalKey), elem.Value.([]byte)) {
			return
		}

		elem = elem.Next()
	}
}

func (ms MemStorage) Put(key *storage.InternalKey, value []byte) error {
	//FIXME: test this overhead
	if elem := ms.sl.Find(key); elem == nil {
		ms.size += len(key.Key()) + len(value)
	} else {
		ms.size += len(value) - len(elem.Value.([]byte))
	}
	ms.sl.Set(key, value)
	return nil
}

func (ms MemStorage) Get(key *storage.InternalKey) ([]byte, error) {
	elem := ms.sl.Find(key)
	if elem == nil {
		return nil, storage.ErrNotFound
	}

	ikey := elem.Key().(*storage.InternalKey)
	if bytes.Compare(ikey.Key(), key.Key()) == 0 {
		switch ikey.Tag() {
		case storage.TagValue:
			return elem.Value.([]byte), nil
		case storage.TagTombstone:
			return nil, storage.ErrKeyDeleted
		}
	}
	return nil, storage.ErrNotFound
}

func (ms MemStorage) Del(key *storage.InternalKey) error {
	if elem := ms.sl.Find(key); elem == nil {
		return storage.ErrNotFound
	} else {
		ikey := elem.Key().(*storage.InternalKey)
		if bytes.Compare(ikey.Key(), key.Key()) != 0 {
			return storage.ErrNotFound
		}
		if ikey.Tag() != storage.TagTombstone {
			ms.size -= len(elem.Value.([]byte))
		}
		ms.sl.Set(key, []byte{})
	}

	return nil
}

func NewMemStorage(options storage.Options) storage.Storage {
	log.Printf("new memory storage %v\n", options)
	return &MemStorage{
		sl: skiplist.New(skiplist.GreaterThanFunc(storage.GreaterThan)),
	}
}
