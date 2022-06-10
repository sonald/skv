package main

import (
	"context"
	"fmt"
	"github.com/sonald/skv/internal/pkg/kv"
	rpc "github.com/sonald/skv/internal/pkg/rpc"
	"google.golang.org/grpc"
	"io"
	"log"
	"math/rand"
	"time"
)

func put(cli rpc.SKVClient, key string, value []byte) {
	var ctx, cancel = context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	p := &rpc.KeyValuePair{
		Key:   key,
		Value: value,
	}

	reply, err := cli.Put(ctx, p)
	if err != nil {
		log.Printf("reply: %s\n", err.Error())
		return
	}
	reply.GetError()
	//log.Printf("reply: %d\n", reply.GetError())
}

func getMeta(cli rpc.SKVClient) []kv.ServerConfig {
	var ctx, cancel = context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	req := &rpc.GetMetaRequest{Condition: ""}
	reply, err := cli.GetMeta(ctx, req)
	if err != nil {
		log.Printf("rely: %v\n", err.Error())
		return nil
	}

	var cfg []kv.ServerConfig
	for _, s := range reply.Servers {
		cfg = append(cfg, kv.ServerConfig{
			Address:  s.Address,
			ServerID: s.ServerID,
			Leader:   s.Leader,
			State:    s.State,
		})
	}

	return cfg
}

func get(cli rpc.SKVClient, key string) []byte {
	var ctx, cancel = context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	req := &rpc.GetRequest{Key: key, Level: rpc.ReadLevel_LinearRead}
	reply, err := cli.Get(ctx, req)
	if err != nil {
		log.Printf("reply: %s\n", err.Error())
		return nil
	}

	return reply.GetValue()
}

func del(cli rpc.SKVClient, key string) int32 {
	var ctx, cancel = context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	req := &rpc.DelRequest{Key: key}
	reply, err := cli.Del(ctx, req)
	if err != nil {
		log.Printf("reply: %s\n", err.Error())
		return 1
	}

	return reply.GetError()
}

func scan(cli rpc.SKVClient, f func(k string, v []byte) bool) {
	var ctx, cancel = context.WithTimeout(context.Background(), time.Hour)
	defer cancel()

	in := &rpc.ScanOption{
		Limits: 0,
		Prefix: "",
	}
	stream, err := cli.Scan(ctx, in, grpc.EmptyCallOption{})
	if err != nil {
		log.Printf("scan: %s\n", err.Error())
	}

	for {
		p, err := stream.Recv()
		if err == io.EOF {
			break
		}

		if err != nil {
			log.Printf("scan: %s\n", err.Error())
		}

		if !f(p.Key, p.Value) {
			break
		}
	}

}

func randomTest(cli rpc.SKVClient) {

	r := rand.New(rand.NewSource(0xdeadbeef))

	for i := 0; i < 100; i++ {
		key := fmt.Sprintf("key%02d", r.Intn(40))
		put(cli, key, []byte(fmt.Sprintf("value%d", r.Int31())))
	}

}
