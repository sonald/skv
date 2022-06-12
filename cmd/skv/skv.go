package main

import (
	"context"
	"fmt"
	"github.com/sonald/skv/internal/pkg/kv"
	"github.com/sonald/skv/internal/pkg/node"
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
	Run: func(_ *cobra.Command, _ []string) {
		host := viper.GetString(kSkvHost)
		port := viper.GetString(kSkvPort)

		localIP, err := firstAddress()
		if err != nil {
			log.Fatalln(err)
		}

		if host == "" {
			host = localIP
		}

		var rpcAddress = net.JoinHostPort(host, port)
		fmt.Printf("listening on [%s]\n", rpcAddress)

		var grpcServer *grpc.Server
		var dbnode *node.SKVServerImpl

		sigchan := make(chan os.Signal, 1)
		signal.Notify(sigchan, os.Interrupt, syscall.SIGTERM)

		closed := make(chan struct{})

		go func() {
			var ctx, cancel = context.WithTimeout(context.Background(), time.Second*2)
			defer cancel()

			lc := net.ListenConfig{}
			listener, err := lc.Listen(ctx, "tcp4", rpcAddress)
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

			var bind = viper.GetString(kRaftBind)
			tcpaddr, err := net.ResolveTCPAddr("tcp4", bind)
			if err != nil {
				log.Fatalf("invalid bind address: %s\n", bind)
			}
			if tcpaddr.IP == nil || tcpaddr.IP.IsUnspecified() {
				bind = fmt.Sprintf("%s:%d", localIP, tcpaddr.Port)
				log.Printf("update bind to %s\n", bind)
			}

			var serverId = viper.GetString(kRaftServerID)
			if len(serverId) == 0 {
				serverId = bind
			}

			var raftStorage = fmt.Sprintf("%s/raft", viper.GetString(kSkvRoot))

			var bootstrap = viper.GetBool(kRaftBootstrap)

			var ndopts = []node.NodeOption{
				node.WithDebug(viper.GetBool(kSkvDebug)),
				node.WithBind(bind),
				node.WithRpcAddress(rpcAddress),
				node.WithID(serverId),
				node.WithStorageRoot(raftStorage),
				node.WithBootstrap(bootstrap),
			}
			if !bootstrap {
				if len(viper.GetString(kRaftBootstrapAddress)) == 0 {
					log.Fatalln("non-bootstrap node needs specify leader's address")
				}
				ndopts = append(ndopts, node.WithBootstrapAddress(viper.GetString(kRaftBootstrapAddress)))
			}

			dbnode = node.NewSKVServer(kvOpts, ndopts)

			rpc.RegisterSKVServer(grpcServer, dbnode)
			rpc.RegisterPeerServer(grpcServer, dbnode)

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
			close(closed)
			break
		case <-closed:
			log.Printf("shutdown\n")
			break
		}

		dbnode.Shutdown()
		if grpcServer != nil {
			grpcServer.GracefulStop()
		}
	},
}

var statusCmd = &cobra.Command{
	Use:     "status",
	Short:   "report status",
	Example: "skv status",
	Run: func(cmd *cobra.Command, _ []string) {
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
	viper.BindEnv(kRaftBootstrapAddress, "join")
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
	coreSets.StringP(kSkvHost, "H", "", "listen host")
	coreSets.StringP(kSkvRoot, "r", "/tmp/skv", "root path for storage")
	coreSets.StringVarP(&cfg, kSkvConfig, "c", "", "config path")
	coreSets.Int(kSkvDumpPolicy, kv.DumpByCount, "memtable dump to sstable policy")
	coreSets.IntP(kSkvSizeThreshold, "S", storage.Megabyte, "memtable size threshold")
	coreSets.IntP(kSkvCountThreshold, "C", 1024, "memtable key count threshold")

	raftSets.String(kRaftBootstrapAddress, "", "bootstrap node's address")
	raftSets.BoolP(kRaftBootstrap, "B", false, "this is bootstrap node")
	raftSets.String(kRaftBind, "", "raft bind address")
	raftSets.String(kRaftServerID, "", "raft node id, if empty, use bind address")

	rootCmd.Flags().SortFlags = true
	rootCmd.Flags().AddFlagSet(coreSets)
	rootCmd.Flags().AddFlagSet(raftSets)

	rootCmd.AddCommand(statusCmd)

	statusCmd.Flags().BoolP("verbose", "V", false, "report details")
}

func firstAddress() (string, error) {
	ifs, err := net.Interfaces()
	if err != nil {
		return "", err
	}
	for _, iface := range ifs {
		if iface.Flags&net.FlagLoopback != 0 {
			continue
		}
		if iface.Flags&net.FlagUp == 0 {
			continue
		}

		as, err := iface.Addrs()
		if err != nil {
			continue
		}
		for _, a := range as {
			var ip net.IP
			switch v := a.(type) {
			case *net.IPNet:
				ip = v.IP
			case *net.IPAddr:
				ip = v.IP
			}

			if ip == nil || ip.IsLoopback() {
				continue
			}

			ip = ip.To4()
			if ip == nil {
				continue
			}

			return ip.String(), nil
		}
	}

	return "", nil
}
