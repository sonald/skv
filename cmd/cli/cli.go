package main

import (
	"context"
	"fmt"
	"github.com/chzyer/readline"
	"github.com/sonald/skv/internal/pkg/rpc"
	"github.com/spf13/cobra"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"io"
	"log"
	"os"
	"strings"
	"time"
)

var rootCmd = &cobra.Command{
	Use:   "skvcli",
	Short: "simple kv store",
	Long: `a simple kv store with LSM-tree style storage
 and raft-based cluster support
`,
	Example: "skvcli put/get",
	Run: func(cmd *cobra.Command, args []string) {
		host, _ := cmd.Flags().GetString("host")
		port, _ := cmd.Flags().GetString("port")

		cli, conn := startClient(host, port)
		defer conn.Close()
		interactive(cli)
	},
}

var putCmd = &cobra.Command{
	Use:   "put",
	Short: "put value into skv",

	Run: func(cmd *cobra.Command, args []string) {
		host, _ := cmd.Flags().GetString("host")
		port, _ := cmd.Flags().GetString("port")

		log.Println(args)
		if len(args) <= 1 {
			fmt.Println("need arguments [key, value]")
			return
		}
		cli, conn := startClient(host, port)
		defer conn.Close()

		put(cli, args[0], strings.Join(args[1:], " "))
	},
}

var getCmd = &cobra.Command{
	Use:   "get",
	Short: "get value from skv",

	Run: func(cmd *cobra.Command, args []string) {
		host, _ := cmd.Flags().GetString("host")
		port, _ := cmd.Flags().GetString("port")
		cli, conn := startClient(host, port)
		defer conn.Close()

		if len(args) == 0 {
			return
		}
		fmt.Println(get(cli, args[0]))

	},
}

var listCmd = &cobra.Command{
	Use:   "list",
	Short: "list all key-value pairs from skv",

	Run: func(cmd *cobra.Command, args []string) {
		host, _ := cmd.Flags().GetString("host")
		port, _ := cmd.Flags().GetString("port")

		cli, conn := startClient(host, port)
		defer conn.Close()

		fmt.Println("scanning....")
		var seq int
		scan(cli, func(k, v string) bool {
			fmt.Printf("[%08d] %s - [%s]\n", seq, k, v)
			seq++

			return true
		})
	},
}

func init() {
	rootCmd.PersistentFlags().StringP("port", "p", "9527", "skv coordinator's port")
	rootCmd.PersistentFlags().StringP("host", "H", "localhost", "listen host")

	rootCmd.AddCommand(putCmd, getCmd, listCmd)
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
	if err := rootCmd.Execute(); err != nil {
		fmt.Println(err.Error())
		os.Exit(1)
	}
}

func startClient(host, port string) (rpc.SKVClient, *grpc.ClientConn) {
	var opts []grpc.DialOption
	opts = append(opts, grpc.WithTransportCredentials(insecure.NewCredentials()))
	opts = append(opts, grpc.WithBlock())
	var ctx, cancel = context.WithTimeout(context.Background(), time.Second*5)
	defer cancel()

	server := fmt.Sprintf("%s:%s", host, port)
	conn, err := grpc.DialContext(ctx, server, opts...)
	if err != nil {
		log.Fatalf("dial fail: %s\n", err.Error())
	}

	return rpc.NewSKVClient(conn), conn
}

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
