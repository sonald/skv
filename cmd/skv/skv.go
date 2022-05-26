package main

import (
	"context"
	"fmt"
	"github.com/sonald/skv/internal/pkg/kv"
	"github.com/sonald/skv/internal/pkg/rpc"
	"github.com/sonald/skv/internal/pkg/storage"
	"github.com/spf13/cobra"
	flag "github.com/spf13/pflag"
	"github.com/spf13/viper"
	"google.golang.org/grpc"
	"log"
	"net"
	"os"
	"strings"
	"time"
)

const (
	kSkvDebug          = "skv.debug"
	kSkvHost           = "skv.host"
	kSkvPort           = "skv.port"
	kSkvRoot           = "skv.root"
	kSkvDumpPolicy     = "skv.dumpPolicy"
	kSkvCountThreshold = "skv.countThreshold"
	kSkvSizeThreshold  = "skv.sizeThreshold"
	kSkvConfig         = "skv.config"
)

var rootCmd = &cobra.Command{
	Use:   "skv",
	Short: "simple kv store",
	Long: `a simple kv store with LSM-tree style storage
 and raft-based cluster support
`,
	Example: "skv ",
	Run: func(cmd *cobra.Command, args []string) {
		host := viper.GetString(kSkvHost)
		port := viper.GetString(kSkvPort)

		fmt.Printf("listening on %s\n", fmt.Sprintf("%s:%s", host, port))
		//listener, err := net.Listen("tcp", fmt.Sprintf("%s:%s", host, port))
		var ctx, cancel = context.WithTimeout(context.Background(), time.Second*2)
		defer cancel()

		lc := net.ListenConfig{}
		listener, err := lc.Listen(ctx, "tcp", fmt.Sprintf("%s:%s", host, port))
		if err != nil {
			log.Fatalln(err.Error())
		}

		var opts []grpc.ServerOption
		s := grpc.NewServer(opts...)

		var skopts = []kv.KVOption{
			kv.WithDebug(viper.GetBool(kSkvDebug)),
			kv.WithRoot(viper.GetString(kSkvRoot)),
			kv.WithDumpPolicy(viper.GetInt(kSkvDumpPolicy)),
			kv.WithDumpCountThreshold(viper.GetInt(kSkvCountThreshold)),
			kv.WithDumpSizeThreshold(viper.GetInt(kSkvSizeThreshold)),
		}

		rpc.RegisterSKVServer(s, NewSKVServer(skopts...))
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
		details, err := cmd.Flags().GetBool("verbose")
		if err != nil {
			return
		}

		fmt.Println("status ok")
		if details {
			//TODO: get statistics
			viper.Debug()
		}
	},
}

var (
	cfg         string
	storageRoot string
	coreSets    = flag.NewFlagSet("core", flag.ContinueOnError)
)

func initConfig() {
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
	viper.BindEnv(kSkvHost, "host")
	viper.BindEnv(kSkvPort, "port")
	viper.BindPFlags(coreSets)
	viper.SetDefault(kSkvPort, "9527")
	if err := viper.ReadInConfig(); err != nil {
		log.Printf("read config failed: %s\n", err)
	}

	fmt.Printf("load config %s\n", viper.ConfigFileUsed())
}

func init() {
	cobra.OnInitialize(initConfig)

	coreSets.BoolP(kSkvDebug, "D", false, "turn on debug")
	coreSets.StringP(kSkvPort, "p", "", "skv coordinator's port")
	coreSets.StringP(kSkvHost, "H", "localhost", "listen host")
	coreSets.StringP(kSkvRoot, "r", "/tmp/skv", "root path for storage")
	coreSets.StringVarP(&cfg, kSkvConfig, "c", "", "config path")
	coreSets.Int(kSkvDumpPolicy, kv.DumpByCount, "memtable dump to sstable policy")
	coreSets.IntP(kSkvSizeThreshold, "S", storage.Megabyte, "memtable size threshold")
	coreSets.IntP(kSkvCountThreshold, "C", 1024, "memtable key count threshold")

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
