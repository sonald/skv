package disk

import (
	"github.com/bits-and-blooms/bloom/v3"
	"github.com/sonald/skv/internal/pkg/storage"
	"log"
)

// implement both Filter and Storage
type BloomFilter struct {
	b *bloom.BloomFilter
	s storage.Storage
}

func (b *BloomFilter) Set(key string) {
	//log.Printf("bf: Set(%s)\n", key)
	b.b.Add([]byte(key))
}

func (b *BloomFilter) Check(key string) bool {
	log.Printf("bf: Check(%s) = %v\n", key, b.b.Test([]byte(key)))
	return b.b.Test([]byte(key))
}

func NewBloomFilter(s storage.Storage) Filter {
	bf := &BloomFilter{
		b: bloom.NewWithEstimates(10000, 0.001),
	}
	//load
	s.Scan(func(k, _ string) bool {
		bf.Set(k)
		return true
	})
	return bf
}
