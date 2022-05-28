package disk

import (
	"github.com/bits-and-blooms/bloom/v3"
	"github.com/sonald/skv/internal/pkg/storage"
)

// implement both Filter and Storage
type BloomFilter struct {
	b *bloom.BloomFilter
	s storage.Storage
}

func (b *BloomFilter) Set(key *storage.InternalKey) {
	//log.Printf("bf: Set(%s)\n", key)
	b.b.Add([]byte(key.Key()))
}

func (b *BloomFilter) Check(key *storage.InternalKey) bool {
	//log.Printf("bf: Check(%s) = %v\n", key.Key(), b.b.Test([]byte(key.Key())))
	if key.Tag() == storage.TagTombstone {
		return false
	}
	return b.b.Test([]byte(key.Key()))
}

//TODO: n comes from #keys of storage
func NewBloomFilter(s storage.Storage) Filter {
	bf := &BloomFilter{
		b: bloom.NewWithEstimates(10000, 0.001),
	}
	//load
	s.Scan(func(k *storage.InternalKey, _ []byte) bool {
		bf.Set(k)
		return true
	})
	return bf
}
