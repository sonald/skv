package main

import (
	"fmt"
	kv "github.com/sonald/skv/pkg/kv"
	"math/rand"
)

// TODO
// storage: LSM-tree
//  0. encoding key and value (metadata (created_at, version))
// grpc interface
func main() {
	db := kv.NewKV()
	defer db.Close()
	r := rand.New(rand.NewSource(0xdeadbeef))

	for i := 0; i < 100; i++ {
		key := fmt.Sprintf("key%02d", r.Intn(40))
		db.Put(key, fmt.Sprintf("value%d", r.Int31()))
	}

	fmt.Println("scanning....")
	var seq int
	db.Scan(func(k, v string) bool {
		fmt.Printf("[%03d] %s - %s\n", seq, k, v)
		seq++

		return true
	})
}
