package main

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"log"
	"net"
	"os"
	"time"

	"github.com/hashicorp/raft"
	bolt "github.com/hashicorp/raft-boltdb"
	"github.com/sonald/skv/internal/pkg/kv"
	"github.com/sonald/skv/internal/pkg/rpc"
	"github.com/sonald/skv/internal/pkg/storage"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type NodeOption func(kv *KVNode)

func WithDebug(b bool) NodeOption {
	return func(kv *KVNode) {
		kv.debug = b
	}
}

func WithRpcAddress(addr string) NodeOption {
	return func(kv *KVNode) {
		kv.rpcAddress = addr
	}
}

func WithStorageRoot(path string) NodeOption {
	return func(kv *KVNode) {
		kv.storageRoot = path
	}
}

func WithBootstrap(b bool) NodeOption {
	return func(kv *KVNode) {
		kv.bootstrap = b
	}
}

func WithBootstrapAddress(addr string) NodeOption {
	return func(kv *KVNode) {
		kv.bootstrapAddress = addr
	}
}

func WithBind(bind string) NodeOption {
	return func(kv *KVNode) {
		kv.bind = bind
	}
}

func WithID(id string) NodeOption {
	return func(kv *KVNode) {
		kv.serverId = id
	}
}

const (
	EventPut = 1 << iota
	EventDel
)

type LogEvent struct {
	kind    byte
	key     string
	payload []byte
}

func EncodePut(key string, value []byte) []byte {
	var buf bytes.Buffer

	err := buf.WriteByte(EventPut)
	if err != nil {
		return nil
	}

	var pair = [2][]byte{
		[]byte(key), value,
	}
	for _, p := range pair {
		data, err := storage.LengthPrefixed(p)
		if err != nil {
			log.Printf("Encode: %v\n", err)
			return nil
		}

		_, err = buf.Write(data)
		if err != nil {
			log.Printf("Encode: %v\n", err)
			return nil
		}
	}

	return buf.Bytes()
}

func EncodeDel(key string) []byte {
	var buf bytes.Buffer

	err := buf.WriteByte(EventDel)
	if err != nil {
		return nil
	}

	data, err := storage.LengthPrefixed([]byte(key))
	if err != nil {
		log.Printf("Encode: %v\n", err)
		return nil
	}

	_, err = buf.Write(data)
	if err != nil {
		log.Printf("Encode: %v\n", err)
		return nil
	}

	return buf.Bytes()
}

func (e *LogEvent) Encode() []byte {
	switch e.kind {
	case EventPut:
		return EncodePut(e.key, e.payload)
	case EventDel:
		return EncodeDel(e.key)
	}

	panic("invalid LogEvent")
}

func DecodeLogEvent(in []byte) *LogEvent {
	var o = bytes.NewReader(in)
	e := &LogEvent{}
	kd, err := o.ReadByte()
	if err != nil {
		return nil
	}
	e.kind = kd
	data, err := storage.ReadLengthPrefixed(o)
	if err != nil {
		log.Printf("DecodeLogEvent: %v\n", err)
		return nil
	}
	e.key = string(data)

	if e.kind == EventPut {
		e.payload, err = storage.ReadLengthPrefixed(o)
		if err != nil {
			log.Printf("DecodeLogEvent: %v\n", err)
			return nil
		}
	}
	return e
}

type KVNode struct {
	raft          *raft.Raft
	raftStore     *bolt.BoltStore
	raftTransport *raft.NetworkTransport
	db            kv.KV // shared with grpc service

	// if this is bootstrap node
	bootstrap      bool
	bind, serverId string
	storageRoot    string
	// address to send Add/Remove request
	rpcAddress string
	// bootstrapped node's rpcAddress
	bootstrapAddress string

	debug bool

	shutdown   chan struct{}
	raftNotify chan bool
	isShutdown bool
}

func (nd *KVNode) Apply(l *raft.Log) interface{} {
	if l.Type != raft.LogCommand {
		return nil
	}

	if l.Data != nil {
		e := DecodeLogEvent(l.Data)
		if e == nil {
			return fmt.Errorf("log contains invalid event data")
		}

		log.Printf("FSM.Apply(index %d, term %d)\n", l.Index, l.Term)

		if e.kind == EventPut {
			log.Printf("FSM.Apply Put(%s)\n", e.key)
			return nd.db.Put(e.key, e.payload)
		} else {
			log.Printf("FSM.Apply Del\n")
			return nd.db.Del(e.key)
		}
	}
	return nil
}

func (nd *KVNode) Snapshot() (raft.FSMSnapshot, error) {
	log.Printf("FSM.Snapshot\n")

	return &SKVSnapshot{}, nil
}

func (nd *KVNode) Restore(snapshot io.ReadCloser) error {
	log.Printf("FSM.Restore\n")
	return nil
}

func seqApply(cbs ...func() error) error {
	for _, cb := range cbs {
		if err := cb(); err != nil {
			return err
		}
	}

	return nil
}

func (nd *KVNode) execQuorumOperation(join bool) {
	var opts []grpc.DialOption
	opts = append(opts, grpc.WithTransportCredentials(insecure.NewCredentials()))
	opts = append(opts, grpc.WithBlock())

	var ctx, cancel = context.WithTimeout(context.Background(), time.Second*5)
	defer cancel()

	//FIXME: the problem is if leader changes or quits before this request send
	// Dial will timeout
	conn, err := grpc.DialContext(ctx, nd.bootstrapAddress, opts...)
	if err != nil {
		log.Printf("execQuorumOperation: dial fail: %s\n", err.Error())
		return
	}
	defer conn.Close()

	cli := rpc.NewPeerClient(conn)
	req := &rpc.PeerRequest{
		ServerID:  nd.serverId,
		Address:   nd.bind,
		PrevIndex: 0,
		//PrevIndex: nd.raft.LastIndex(),
	}
	if join {
		log.Printf("send join request to %v\n", nd.bootstrapAddress)
		_, err = cli.Join(context.Background(), req, grpc.EmptyCallOption{})
	} else {
		log.Printf("send quit request to %v\n", nd.bootstrapAddress)
		_, err = cli.Quit(context.Background(), req, grpc.EmptyCallOption{})
	}
	if err != nil {
		log.Fatalf("Join: %v\n", err)
	}
}

func (nd *KVNode) WaitForLeader(timeout time.Duration) error {
	log.Printf("wait for leader election\n")
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()

	timer := time.NewTimer(timeout)
	defer timer.Stop()

	for {
		select {
		case <-ticker.C:
			addr, _ := nd.raft.LeaderWithID()
			if len(addr) > 0 {
				return nil
			}

		case <-timer.C:
			return fmt.Errorf("wait for leader timeout")
		}
	}
}

func (nd *KVNode) WaitApplied(timeout time.Duration) error {
	log.Printf("wait for applied logs\n")
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()

	timer := time.NewTimer(timeout)
	defer timer.Stop()

	for {
		select {
		case <-timer.C:
			return fmt.Errorf("wait for applied timeout")

		case <-ticker.C:
			if nd.raft.AppliedIndex() >= nd.raft.LastIndex() {
				return nil
			}
		}
	}
	return nil
}

func storageExists(root string) bool {
	_, err := os.Stat(root)
	if err != nil && os.IsNotExist(err) {
		return false
	}

	return true
}

func NewKVNode(kv kv.KV, opts ...NodeOption) *KVNode {
	nd := &KVNode{
		db:         kv,
		shutdown:   make(chan struct{}),
		raftNotify: make(chan bool, 1),
	}

	for _, opt := range opts {
		opt(nd)
	}

	var err error
	var logs raft.LogStore
	var stable raft.StableStore
	var snaps raft.SnapshotStore
	var tcpaddr *net.TCPAddr
	var newNode bool

	var logsPath = fmt.Sprintf("%s/log_stable", nd.storageRoot)
	var snapPath = fmt.Sprintf("%s/raft-snapshot", nd.storageRoot)

	cfg := raft.DefaultConfig()
	cfg.LocalID = raft.ServerID(nd.serverId)
	cfg.NotifyCh = nd.raftNotify
	cfg.TrailingLogs = 16
	cfg.SnapshotInterval = time.Second * 120
	cfg.SnapshotThreshold = 1024

	// for debugging purpose, we increment frequence of snapshotting
	if nd.debug {
		cfg.SnapshotInterval = time.Second * 10
		cfg.SnapshotThreshold = 10
		cfg.TrailingLogs = 20
	}

	log.Printf("raft: bootstrap: %v, bind:%s, id:%s, root: %s, leader: %s\n", nd.bootstrap, nd.bind, nd.serverId,
		nd.storageRoot, nd.bootstrapAddress)

	defer func() {
		if nd.raft == nil {
			if nd.raftStore != nil {
				nd.raftStore.Close()
			}
		}
	}()

	err = seqApply(
		func() error {
			err = os.MkdirAll(snapPath, 0755)
			if err != nil && os.IsExist(err) {
				err = nil
			}
			return err
		},
		func() error {
			nd.raftStore, err = bolt.NewBoltStore(logsPath)
			return err
		},
		func() error {
			var cache *raft.LogCache
			cache, err = raft.NewLogCache(128, nd.raftStore)
			if err == nil {
				logs = cache
				stable = nd.raftStore
			}
			return err
		},
		func() error {
			snaps, err = raft.NewFileSnapshotStore(snapPath, 2, os.Stderr)
			return err
		},
		func() error {
			tcpaddr, err = net.ResolveTCPAddr("tcp", nd.bind)
			return err
		},
		func() error {
			nd.raftTransport, err = raft.NewTCPTransport(tcpaddr.String(), tcpaddr, 3, time.Second*3, os.Stderr)
			return err
		},
		func() error {
			var oldState bool
			oldState, err = raft.HasExistingState(logs, stable, snaps)
			newNode = !oldState
			return err
		},
		func() error {
			nd.raft, err = raft.NewRaft(cfg, nd, logs, stable, snaps, nd.raftTransport)
			return err
		},
	)

	if err != nil {
		log.Fatalf("NewKVNode: %v\n", err)
	}

	go func() {
		for {
			select {
			case leader := <-nd.raftNotify:
				if leader {
					a, i := nd.raft.LeaderWithID()
					log.Printf("leader changed to (%v, %v)\n", i, a)
				}
			case <-nd.shutdown:
				log.Printf("shutting down..., quit watcher\n")
				return
			}
		}
	}()

	if newNode {
		if nd.bootstrap {
			configuration := raft.Configuration{
				Servers: []raft.Server{
					{
						ID:      cfg.LocalID,
						Address: nd.raftTransport.LocalAddr(),
					},
				},
			}

			log.Printf("bootstrapping %v\n", nd.bind)
			bf := nd.raft.BootstrapCluster(configuration)
			if err := bf.Error(); err != nil {
				log.Printf("bootstrap abort: %v\n", err)
			}

		} else if len(nd.bootstrapAddress) > 0 {
			//this is not necessary, if we are not at startup stage
			nd.execQuorumOperation(true)

		}
	}

	nd.WaitForLeader(time.Second * 10)
	nd.WaitApplied(time.Second * 10)

	return nd
}

func (nd *KVNode) Put(key string, value []byte) error {
	if nd.raft.State() != raft.Leader {
		return fmt.Errorf("put: current node is not leader")
	}

	e := LogEvent{kind: EventPut, key: key, payload: value}
	future := nd.raft.Apply(e.Encode(), time.Millisecond*100)
	if err := future.Error(); err != nil {
		return err
	}

	return nil
}

func (nd *KVNode) Del(key string) error {
	if nd.raft.State() != raft.Leader {
		return fmt.Errorf("del: current node is not leader")
	}

	e := LogEvent{kind: EventDel, key: key}
	future := nd.raft.Apply(e.Encode(), time.Millisecond*100)
	if err := future.Error(); err != nil {
		return err
	}

	return nil
}

func (nd *KVNode) Get(key string, level rpc.ReadLevel) ([]byte, error) {
	if level != rpc.ReadLevel_LinearRead || nd.raft.State() != raft.Leader {
		return nil, fmt.Errorf("del: current node is not leader")
	}

	val, err := nd.db.Get(key)
	if err != nil && err == storage.ErrNotFound {
		return nil, nil
	}

	return val, nil
}

func (nd *KVNode) GetMeta() ([]kv.ServerConfig, error) {
	log.Printf("%+v\n", nd.raft.Stats())

	f := nd.raft.GetConfiguration()
	if err := f.Error(); err != nil {
		return nil, err
	}

	_, leaderID := nd.raft.LeaderWithID()

	var data []kv.ServerConfig
	for _, s := range f.Configuration().Servers {
		data = append(data, kv.ServerConfig{
			Address:    string(s.Address),
			ServerID:   string(s.ID),
			Leader:     leaderID == s.ID,
			State:      s.Suffrage.String(),
			RpcAddress: nd.rpcAddress,
		})
	}

	return data, nil
}

// Can only the leader do this
func (nd *KVNode) AddNode(serverID, bind string, prevIndex uint64) {
	if nd.raft.State() != raft.Leader {
		log.Printf("AddNote: current node is not leader\n")
		return
	}

	cfgFuture := nd.raft.GetConfiguration()
	if err := cfgFuture.Error(); err != nil {
		log.Fatalf("get configuration failed: %v\n", err)
	}

	for _, s := range cfgFuture.Configuration().Servers {
		if string(s.ID) == serverID {
			//TODO: check if address is the same
			log.Printf("%s is already in the quorum\n", serverID)
			return
		}
	}

	f := nd.raft.AddVoter(raft.ServerID(serverID), raft.ServerAddress(bind), prevIndex, 0)
	if err := f.Error(); err != nil {
		log.Printf("AddNode failed: %v\n", err)
	} else {
		log.Printf("AddNode(Self %s, %s) add %s done\n", nd.raft.String(), nd.bind, serverID)
	}

}

// Can only the leader do this
func (nd *KVNode) RemoveNode(serverID, bind string, prevIndex uint64) {
	if nd.raft.State() != raft.Leader {
		log.Printf("RemoveNote: current node is not leader\n")
		return
	}

	f := nd.raft.RemoveServer(raft.ServerID(serverID), prevIndex, 0)
	if err := f.Error(); err != nil {
		log.Printf("RemoveNode failed: %v\n", err)
	} else {
		log.Printf("RemoveNode(%s, %s) done\n", nd.raft.String(), nd.bind)
	}
}

func (nd *KVNode) Shutdown() {
	if nd.isShutdown {
		log.Printf("(%s, %s) already shutdown\n", nd.serverId, nd.bind)
		return
	}

	log.Printf("shutting down (%s, %s)\n", nd.serverId, nd.bind)
	close(nd.shutdown)

	nd.isShutdown = true

	if nd.raft != nil {
		if err := nd.raftStore.Close(); err != nil {
			log.Printf("Close raftStore failed: %v\n", err)
		}

		if err := nd.raftTransport.Close(); err != nil {
			log.Printf("Close raftTransport failed: %v\n", err)
		}
		f2 := nd.raft.Shutdown()
		if err := f2.Error(); err != nil {
			log.Fatalf("raft shutdown failed: %v\n", err)
		}
	}

	nd.db.Close()
}
