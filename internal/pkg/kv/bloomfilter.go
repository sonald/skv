package kv

import (
	"github.com/bits-and-blooms/bloom/v3"
	"github.com/sonald/skv/pkg/storage"
	"log"
)

type Filter interface {
	Set(key string)
	Check(key string) bool
}

type BloomFilter struct {
	b *bloom.BloomFilter
	s KV
}

func (b *BloomFilter) Set(key string) {
	log.Printf("bf: Set(%s)\n", key)
	b.b.Add([]byte(key))
}

func (b *BloomFilter) Check(key string) bool {
	log.Printf("bf: Check(%s) = %v\n", key, b.b.Test([]byte(key)))
	return b.b.Test([]byte(key))
}

func (b *BloomFilter) Stats() {
}

func (b *BloomFilter) Put(key string, value string) error {
	b.Set(key)
	return b.s.Put(key, value)
}

func (b *BloomFilter) Get(key string) (string, error) {
	if !b.Check(key) {
		return "", storage.ErrNotFound
	}

	return b.s.Get(key)
}

func (b *BloomFilter) Del(key string) error {
	//FIXME: what should we do?
	return b.s.Del(key)
}

func (b *BloomFilter) Scan(f func(k string, v string) bool) {
	b.s.Scan(f)
}

func (b *BloomFilter) Close() {
	b.s.Close()
}

func NewFilter(s KV) KV {
	bf := &BloomFilter{
		b: bloom.NewWithEstimates(1000000, 0.001),
		s: s,
	}
	//load
	bf.s.Scan(func(k, _ string) bool {
		bf.Set(k)
		return true
	})
	return bf
}
