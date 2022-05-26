package mem

import (
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

func (ms MemStorage) Scan(f func(k string, v string) bool) {
	elem := ms.sl.Front()
	for elem != nil {
		if !f(elem.Key().(string), elem.Value.(string)) {
			return
		}

		elem = elem.Next()
	}
}

func (ms MemStorage) Put(key string, value string) error {
	//FIXME: test this overhead
	if elem := ms.sl.Find(key); elem == nil {
		ms.size += len(key) + len(value)
	} else {
		ms.size += len(value) - len(elem.Value.(string))
	}
	ms.sl.Set(key, value)
	return nil
}

func (ms MemStorage) Get(key string) (string, error) {
	elem := ms.sl.Get(key)
	if elem == nil {
		return "", storage.ErrNotFound
	}

	return elem.Value.(string), nil
}

func (ms MemStorage) Del(key string) error {
	return nil
}

func NewMemStorage(options storage.Options) storage.Storage {
	log.Printf("new memory storage %v\n", options)
	return &MemStorage{
		sl: skiplist.New(skiplist.String),
	}
}
