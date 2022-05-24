package main

import (
	"fmt"
	"github.com/sonald/skv/pkg/rpc"
	"github.com/spf13/cobra"
	"google.golang.org/grpc"
	"log"
	"net"
	"os"
)

var ()

var rootCmd = &cobra.Command{
	Use:   "skv",
	Short: "simple kv store",
	Long: `a simple kv store with LSM-tree style storage
 and raft-based cluster support
`,
	Example: "skv ",
	Run: func(cmd *cobra.Command, args []string) {
		host, _ := cmd.Flags().GetString("host")
		port, _ := cmd.Flags().GetString("port")

		fmt.Printf("listening on %s\n", fmt.Sprintf("%s:%s", host, port))
		listener, err := net.Listen("tcp", fmt.Sprintf("%s:%s", host, port))
		if err != nil {
			log.Fatalln(err.Error())
		}

		var opts []grpc.ServerOption
		s := grpc.NewServer(opts...)
		rpc.RegisterSKVServer(s, NewSKVServer())
		if err := s.Serve(listener); err != nil {
			log.Printf("err: %s\n", err.Error())
		}
	},
}

var statusCmd = &cobra.Command{
	Use:     "status",
	Short:   "report status",
	Example: "skv status",
	Run: func(cmd *cobra.Command, args []string) {
		b, err := cmd.Flags().GetBool("verbose")
		if err != nil {
			return
		}

		if b {
			fmt.Println("status is really ok")
		} else {
			fmt.Println("status ok")
		}
	},
}

func init() {
	rootCmd.PersistentFlags().StringP("port", "p", "50062", "skv coordinator's port")
	rootCmd.PersistentFlags().StringP("host", "H", "localhost", "listen host")

	rootCmd.AddCommand(statusCmd)

	statusCmd.Flags().BoolP("verbose", "V", false, "report details")
}

func main() {
	if err := rootCmd.Execute(); err != nil {
		fmt.Println(err.Error())
		os.Exit(1)
	}

}
