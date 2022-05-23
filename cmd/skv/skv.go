package main

import (
	"fmt"
	kv "github.com/sonald/skv/pkg/kv"
	"github.com/sonald/skv/pkg/rpc"
	"google.golang.org/grpc"
	"log"
	"math/rand"
	"net"
)

func main() {
	listener, err := net.Listen("tcp", "localhost:50062")
	if err != nil {
		log.Fatalln(err.Error())
	}

	var opts []grpc.ServerOption

	s := grpc.NewServer(opts...)
	rpc.RegisterSKVServer(s, NewSKVServer())
	if err := s.Serve(listener); err != nil {
		log.Printf("err: %s\n", err.Error())
	}
}

func randomTest() {
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
