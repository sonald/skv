package main

import (
	"fmt"
	"github.com/sonald/skv/pkg/rpc"
	"github.com/spf13/cobra"
	flag "github.com/spf13/pflag"
	"github.com/spf13/viper"
	"google.golang.org/grpc"
	"log"
	"net"
	"os"
	"strings"
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
		host := viper.Get("skv.host")
		port := viper.GetString("skv.port")

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
			viper.Debug()
		} else {
			fmt.Println("status ok")
		}
	},
}

var (
	cfg         string
	storageRoot string
	coreSets    = flag.NewFlagSet("core", flag.ContinueOnError)
)

func initConfig() {
	log.Printf("initConfig")

	if cfg != "" {
		viper.SetConfigFile(cfg)
		viper.SetConfigType("yaml")
	} else {
		viper.AddConfigPath(".")
		viper.AddConfigPath(storageRoot)
		viper.SetConfigName("skv")
	}

	viper.AutomaticEnv()
	viper.SetEnvPrefix("SKV")
	viper.SetEnvKeyReplacer(strings.NewReplacer(".", "_", "-", "_"))
	viper.BindEnv("skv.host", "host")
	viper.BindEnv("skv.port", "port")
	viper.BindPFlags(coreSets)
	viper.SetDefault("skv.port", "9527")
	if err := viper.ReadInConfig(); err != nil {
		log.Printf("read config failed: %s\n", err)
	}

	fmt.Printf("load config %s\n", viper.ConfigFileUsed())
}

func init() {
	cobra.OnInitialize(initConfig)

	coreSets.StringP("skv.port", "p", "", "skv coordinator's port")
	coreSets.StringP("skv.host", "H", "localhost", "listen host")
	coreSets.StringP("skv.root", "r", "/tmp/skv", "root path for storage")
	coreSets.StringVarP(&cfg, "skv.config", "c", "", "config path")
	rootCmd.Flags().SortFlags = true
	rootCmd.Flags().AddFlagSet(coreSets)

	rootCmd.AddCommand(statusCmd)

	statusCmd.Flags().BoolP("verbose", "V", false, "report details")
}

func main() {
	if err := rootCmd.Execute(); err != nil {
		fmt.Println(err.Error())
		os.Exit(1)
	}

}
