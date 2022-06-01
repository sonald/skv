package main

import (
	"bytes"
	"context"
	"fmt"
	"github.com/hashicorp/raft"
	bolt "github.com/hashicorp/raft-boltdb"
	"github.com/sonald/skv/internal/pkg/kv"
	"github.com/sonald/skv/internal/pkg/rpc"
	"github.com/sonald/skv/internal/pkg/storage"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"io"
	"log"
	"net"
	"os"
	"time"
)

type NodeOption func(kv *KVNode)

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

func WithStorageRoot(path string) NodeOption {
	return func(kv *KVNode) {
		kv.storageRoot = path
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
	raft *raft.Raft
	db   kv.KV // shared with grpc service

	// if this is bootstrap node
	bootstrap      bool
	bind, serverId string
	storageRoot    string
	// bootstrapped node's address
	bootstrapAddress string

	leader     bool
	shutdown   chan struct{}
	raftNotify chan bool
}

func (nd *KVNode) Apply(l *raft.Log) interface{} {
	if l.Data != nil {
		e := DecodeLogEvent(l.Data)
		if e == nil {
			return fmt.Errorf("Log contains invalid event data\n")
		}
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
	return nil, nil
}

func (nd *KVNode) Restore(snapshot io.ReadCloser) error {
	log.Printf("FSM.Restore\n")
	return nil
}

func seqApply(cbs ...func() error) {
	for _, cb := range cbs {
		if err := cb(); err != nil {
			log.Fatalf("NewKVNode: %v\n", err)
		}
	}
}

// connect to leader's grpc listen address
func (nd *KVNode) JoinQuorum() {
	var opts []grpc.DialOption
	opts = append(opts, grpc.WithTransportCredentials(insecure.NewCredentials()))
	opts = append(opts, grpc.WithBlock())

	var ctx, cancel = context.WithTimeout(context.Background(), time.Second*5)
	defer cancel()

	conn, err := grpc.DialContext(ctx, nd.bootstrapAddress, opts...)
	if err != nil {
		log.Fatalf("dial fail: %s\n", err.Error())
	}
	defer conn.Close()

	log.Printf("send join request to %v\n", nd.bootstrapAddress)

	cli := rpc.NewPeerClient(conn)
	req := &rpc.PeerRequest{
		ServerID:  nd.serverId,
		Address:   nd.bind,
		PrevIndex: nd.raft.LastIndex(),
	}
	_, err = cli.Join(context.Background(), req, grpc.EmptyCallOption{})
	if err != nil {
		log.Fatalf("Join: %v\n", err)
	}
	log.Printf("Join: done\n")
}

func (nd *KVNode) QuitQuorum() {
	var opts []grpc.DialOption
	opts = append(opts, grpc.WithTransportCredentials(insecure.NewCredentials()))
	opts = append(opts, grpc.WithBlock())

	var ctx, cancel = context.WithTimeout(context.Background(), time.Second*5)
	defer cancel()

	conn, err := grpc.DialContext(ctx, nd.bootstrapAddress, opts...)
	if err != nil {
		log.Fatalf("dial fail: %s\n", err.Error())
	}
	defer conn.Close()

	log.Printf("send quit request to %v\n", nd.bootstrapAddress)

	cli := rpc.NewPeerClient(conn)
	req := &rpc.PeerRequest{
		ServerID:  nd.serverId,
		Address:   nd.bind,
		PrevIndex: nd.raft.LastIndex(),
	}
	_, err = cli.Quit(context.Background(), req, grpc.EmptyCallOption{})
	if err != nil {
		log.Fatalf("Quit: %v\n", err)
	}
	log.Printf("Quit: done\n")
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
	var trans raft.Transport

	err = os.MkdirAll(nd.storageRoot, 0755)
	if err != nil && !os.IsExist(err) {
		log.Fatalf("NewSKVServer: %v\n", err)
	}

	var logsPath = fmt.Sprintf("%s/raft-log", nd.storageRoot)
	var stablePath = fmt.Sprintf("%s/raft-stable", nd.storageRoot)
	var snapPath = fmt.Sprintf("%s/raft-snapshot", nd.storageRoot)

	cfg := raft.DefaultConfig()
	cfg.LocalID = raft.ServerID(nd.serverId)
	cfg.NotifyCh = nd.raftNotify

	log.Printf("raft: bootstrap: %v, bind:%s, id:%s, root: %s, leader: %s\n", nd.bootstrap, nd.bind, nd.serverId,
		nd.storageRoot, nd.bootstrapAddress)
	seqApply(
		func() error {
			err = os.MkdirAll(snapPath, 0755)
			if err != nil && os.IsExist(err) {
				err = nil
			}
			return err
		},
		func() error {
			logs, err = bolt.NewBoltStore(logsPath)
			return err
		},
		func() error {
			stable, err = bolt.NewBoltStore(stablePath)
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
			trans, err = raft.NewTCPTransport(tcpaddr.String(), tcpaddr, 3, time.Second*3, os.Stderr)
			return err
		},
		func() error {
			nd.raft, err = raft.NewRaft(cfg, nd, logs, stable, snaps, trans)
			return err
		},
	)

	go func() {
		for {
			select {
			case leader := <-nd.raftNotify:
				nd.leader = leader
				log.Printf("**************** leader change (%v)\n", nd.raft.Leader())
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

	if nd.bootstrap {
		configuration := raft.Configuration{
			Servers: []raft.Server{
				{
					ID:      cfg.LocalID,
					Address: trans.LocalAddr(),
				},
			},
		}
		nd.raft.BootstrapCluster(configuration)
	} else {
		nd.JoinQuorum()
	}

	return nd
}

func (nd *KVNode) Put(key string, value []byte) error {
	if !nd.leader {
		return fmt.Errorf("Del: current node is not leader\n")
	}

	e := LogEvent{kind: EventPut, key: key, payload: value}
	future := nd.raft.Apply(e.Encode(), time.Millisecond*100)
	if err := future.Error(); err != nil {
		return err
	}

	return nil
}

func (nd *KVNode) Del(key string) error {
	if !nd.leader {
		return fmt.Errorf("Del: current node is not leader\n")
	}

	e := LogEvent{kind: EventDel, key: key}
	future := nd.raft.Apply(e.Encode(), time.Millisecond*100)
	if err := future.Error(); err != nil {
		return err
	}

	return nil
}

func (nd *KVNode) AddNode(serverID, bind string, prevIndex uint64) {
	if !nd.leader {
		log.Printf("AddNote: current node is not leader\n")
		return
	}
	f := nd.raft.AddVoter(raft.ServerID(serverID), raft.ServerAddress(bind), prevIndex, 0)
	if err := f.Error(); err != nil {
		log.Printf("AddNode failed: %v\n", err)
	} else {
		log.Printf("AddNode(%s, %s) done\n", nd.raft.String(), nd.bind)
	}

}

func (nd *KVNode) RemoveNode(serverID, bind string, prevIndex uint64) {
	if !nd.leader {
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

func (nd *KVNode) Shutdonw() {
	//TODO: quit quorum
	log.Printf("shutdown (%s, %s)\n", nd.serverId, nd.bind)
	nd.QuitQuorum()
	close(nd.shutdown)
	f := nd.raft.Shutdown()
	if err := f.Error(); err != nil {
		log.Fatalf("raft shutdown failed: %v\n", err)
	}
}
