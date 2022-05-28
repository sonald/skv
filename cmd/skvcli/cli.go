package main

import (
	"context"
	"fmt"
	"github.com/sonald/skv/internal/pkg/rpc"
	"github.com/spf13/cobra"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
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

		put(cli, args[0], []byte(strings.Join(args[1:], " ")))
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
		fmt.Println(string(get(cli, args[0])))

	},
}

var delCmd = &cobra.Command{
	Use:   "del",
	Short: "del value from skv",

	Run: func(cmd *cobra.Command, args []string) {
		host, _ := cmd.Flags().GetString("host")
		port, _ := cmd.Flags().GetString("port")
		cli, conn := startClient(host, port)
		defer conn.Close()

		if len(args) == 0 {
			return
		}
		fmt.Println(string(del(cli, args[0])))

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
		scan(cli, func(k string, v []byte) bool {
			fmt.Printf("[%08d] %s - [%s]\n", seq, k, string(v))
			seq++

			return true
		})
	},
}

func init() {
	rootCmd.PersistentFlags().StringP("port", "p", "9527", "skv coordinator's port")
	rootCmd.PersistentFlags().StringP("host", "H", "localhost", "listen host")

	rootCmd.AddCommand(putCmd, getCmd, listCmd, delCmd)
}

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
