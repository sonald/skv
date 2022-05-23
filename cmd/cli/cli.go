package main

import (
	"context"
	"fmt"
	"github.com/chzyer/readline"
	"github.com/sonald/skv/pkg/rpc"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"io"
	"log"
	"math/rand"
	"os"
	"strings"
	"time"
)

func put(cli rpc.SKVClient, key, value string) {
	var ctx, cancel = context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	p := &rpc.KeyValuePair{
		Key:   key,
		Value: value,
	}
	//log.Printf("put\n")
	reply, err := cli.Put(ctx, p)
	if err != nil {
		log.Printf("reply: %s\n", err.Error())
		return
	}
	reply.GetError()
	//log.Printf("reply: %d\n", reply.GetError())
}

func get(cli rpc.SKVClient, key string) string {
	var ctx, cancel = context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	req := &rpc.GetRequest{Key: key}
	reply, err := cli.Get(ctx, req)
	if err != nil {
		log.Printf("reply: %s\n", err.Error())
		return ""
	}

	return reply.Value
	//log.Printf("reply: %s\n", reply.Value)
}

func scan(cli rpc.SKVClient, f func(k, v string) bool) {
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
		put(cli, key, fmt.Sprintf("value%d", r.Int31()))
	}

}

var completer = readline.NewPrefixCompleter(
	readline.PcItem("?"),
	readline.PcItem("help"),
	readline.PcItem("list"),
	readline.PcItem("get"),
	readline.PcItem("put"),
	readline.PcItem("test"),
)

func main() {
	var opts []grpc.DialOption
	opts = append(opts, grpc.WithTransportCredentials(insecure.NewCredentials()))
	opts = append(opts, grpc.WithBlock())
	var ctx, cancel = context.WithTimeout(context.Background(), time.Second*5)
	defer cancel()

	conn, err := grpc.DialContext(ctx, "localhost:50062", opts...)

	if err != nil {
		log.Fatalf("dial fail: %s\n", err.Error())
	}
	defer conn.Close()

	cli := rpc.NewSKVClient(conn)
	if len(os.Args) == 1 {
		// enter interactive shell
	}

	rl, err := readline.NewEx(&readline.Config{
		Prompt:          "skv> ",
		HistoryFile:     "/tmp/skv_client_history",
		HistoryLimit:    1000,
		AutoComplete:    completer,
		InterruptPrompt: "",
		EOFPrompt:       "",
	})
	if err != nil {
		log.Fatalf(err.Error())
	}
	defer rl.Close()

	for {
		ln, err := rl.Readline()
		if err != nil {
			if err == readline.ErrInterrupt {
				rl.Clean()
				continue
			}

			if err == io.EOF {
				break
			}

			log.Printf("rl: %s\n", err.Error())
		}
		ln = strings.TrimSpace(ln)
		switch {
		case ln == "test":
			randomTest(cli)
		case strings.HasPrefix(ln, "list"):
			fmt.Println("scanning....")
			var seq int
			scan(cli, func(k, v string) bool {
				fmt.Printf("[%08d] %s - [%s]\n", seq, k, v)
				seq++

				return true
			})
		case strings.HasPrefix(ln, "put"):
			ln = strings.TrimSpace(ln[3:])
			i := strings.IndexByte(ln, ' ')
			if i == -1 {
				break
			}

			key := ln[:i]
			value := strings.TrimSpace(ln[i:])
			put(cli, key, value)

		case strings.HasPrefix(ln, "get"):
			val := get(cli, strings.TrimSpace(ln[3:]))
			fmt.Println(val)

		case ln == "help" || ln == "?":
			fmt.Printf("skv [list|put|get]\n")
		case ln == "exit":
			rl.Close()
		}
	}

}
