package main

import (
	"fmt"
	kv "github.com/sonald/skv/pkg/kv"
)

// TODO
// storage: LSM-tree
//  0. encoding key and value (metadata (created_at, version))
// grpc interface
func main() {
	db := kv.NewKV()
	defer db.Close()

	for i := 0; i < 40; i++ {
		key := fmt.Sprintf("key%04d", i)
		db.Put(key, key)
	}

	fmt.Println("scanning....")
	var seq int
	db.Scan(func(k, v string) bool {
		fmt.Printf("[%03d] %s - %s\n", seq, k, v)
		seq++
		return true
	})
}
