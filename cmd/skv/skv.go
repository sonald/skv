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
	"os/signal"
	"strings"
	"syscall"
	"time"
)

const (
	kSkvDebug          = "skv.debug"
	kSkvHost           = "skv.host"
	kSkvPort           = "skv.port"
	kSkvRoot           = "skv.root"
	kSkvDumpPolicy     = "skv.dumpPolicy"
	kSkvCountThreshold = "skv.threshold.count"
	kSkvSizeThreshold  = "skv.threshold.size"
	kSkvConfig         = "skv.config"
)

const (
	kRaftBind             = "raft.bind"
	kRaftServerID         = "raft.id"
	kRaftStorageRoot      = "raft.storage"
	kRaftBootstrap        = "raft.bootstrap"
	kRaftBootstrapAddress = "raft.bootstrapAddress"
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

		var grpcServer *grpc.Server
		var db *SKVServerImpl

		sigchan := make(chan os.Signal, 1)
		signal.Notify(sigchan, os.Interrupt, syscall.SIGTERM)

		closed := make(chan struct{})

		go func() {
			var ctx, cancel = context.WithTimeout(context.Background(), time.Second*2)
			defer cancel()

			lc := net.ListenConfig{}
			listener, err := lc.Listen(ctx, "tcp", fmt.Sprintf("%s:%s", host, port))
			if err != nil {
				log.Fatalln(err.Error())
			}

			var opts []grpc.ServerOption
			grpcServer = grpc.NewServer(opts...)

			var kvOpts = []kv.KVOption{
				kv.WithDebug(viper.GetBool(kSkvDebug)),
				kv.WithRoot(viper.GetString(kSkvRoot)),
				kv.WithDumpPolicy(viper.GetInt(kSkvDumpPolicy)),
				kv.WithDumpCountThreshold(viper.GetInt(kSkvCountThreshold)),
				kv.WithDumpSizeThreshold(viper.GetInt(kSkvSizeThreshold)),
			}

			var serverId = viper.GetString(kRaftServerID)
			if len(serverId) == 0 {
				serverId = viper.GetString(kRaftBind)
			}

			var raftStorage = viper.GetString(kRaftStorageRoot)
			if len(raftStorage) == 0 {
				raftStorage = fmt.Sprintf("%s/raft", viper.GetString(kSkvRoot))
			}

			var bootstrap = viper.GetBool(kRaftBootstrap)

			var ndopts = []NodeOption{
				WithBind(viper.GetString(kRaftBind)),
				WithID(serverId),
				WithStorageRoot(raftStorage),
				WithBootstrap(viper.GetBool(kRaftBootstrap)),
			}
			if !bootstrap {
				if len(viper.GetString(kRaftBootstrapAddress)) == 0 {
					log.Fatalln("non-bootstrap node needs specify leader's address")
				}
				ndopts = append(ndopts, WithBootstrapAddress(viper.GetString(kRaftBootstrapAddress)))
			}

			db = NewSKVServer(kvOpts, ndopts)
			defer db.Shutdown()

			rpc.RegisterSKVServer(grpcServer, db)
			rpc.RegisterPeerServer(grpcServer, db)

			if err := grpcServer.Serve(listener); err != nil {
				if err != net.ErrClosed {
					log.Printf("err: %s\n", err.Error())
				}
			}

			log.Printf("grpc server quit\n")
			close(closed)
		}()

		select {
		case <-sigchan:
			log.Printf("interrupted\n")
			break
		case <-closed:
			log.Printf("shutdown\n")
			break
		}

		if db != nil {
			db.Shutdown()
		}

		if grpcServer != nil {
			grpcServer.GracefulStop()
		}

		select {
		case <-closed:
			break
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
		}
	},
}

var (
	cfg         string
	storageRoot string
	coreSets    = flag.NewFlagSet("core", flag.ContinueOnError)
	raftSets    = flag.NewFlagSet("raft", flag.ContinueOnError)
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
	viper.BindPFlags(raftSets)
	viper.SetDefault(kSkvPort, "9527")
	viper.SetDefault(kRaftBind, ":9528")
	if err := viper.ReadInConfig(); err != nil {
		log.Printf("read config failed: %s\n", err)
	} else {
		fmt.Printf("load config %s\n", viper.ConfigFileUsed())
	}
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

	raftSets.String(kRaftBootstrapAddress, "", "bootstrap node's address")
	raftSets.BoolP(kRaftBootstrap, "B", false, "this is bootstrap node")
	raftSets.String(kRaftBind, "", "raft bind address")
	raftSets.String(kRaftServerID, "", "raft node id, if empty, use bind address")
	raftSets.String(kRaftStorageRoot, "", "raft storage root, if empty, use skv root")

	rootCmd.Flags().SortFlags = true
	rootCmd.Flags().AddFlagSet(coreSets)
	rootCmd.Flags().AddFlagSet(raftSets)

	rootCmd.AddCommand(statusCmd)

	statusCmd.Flags().BoolP("verbose", "V", false, "report details")
}

func main() {
	if err := rootCmd.Execute(); err != nil {
		fmt.Println(err.Error())
		os.Exit(1)
	}

}
