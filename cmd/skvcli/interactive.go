package main

import (
	"fmt"
	"github.com/chzyer/readline"
	"github.com/sonald/skv/internal/pkg/rpc"
	"io"
	"log"
	"strings"
)

var completer = readline.NewPrefixCompleter(
	readline.PcItem("?"),
	readline.PcItem("help"),
	readline.PcItem("list"),
	readline.PcItem("get"),
	readline.PcItem("put"),
	readline.PcItem("test"),
	readline.PcItem("del"),
)

func interactive(cli rpc.SKVClient) {
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
			scan(cli, func(k string, v []byte) bool {
				fmt.Printf("[%08d] %s - [%s]\n", seq, k, string(v))
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
			put(cli, key, []byte(value))

		case strings.HasPrefix(ln, "get"):
			val := get(cli, strings.TrimSpace(ln[3:]))
			fmt.Println(string(val))

		case strings.HasPrefix(ln, "del"):
			val := del(cli, strings.TrimSpace(ln[3:]))
			fmt.Println(string(val))

		case ln == "help" || ln == "?":
			fmt.Printf("skv [list|put|get]\n")
		case ln == "exit":
			rl.Close()
		}
	}

}
