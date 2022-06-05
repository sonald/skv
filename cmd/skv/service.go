package main

import (
	"context"
	"github.com/sonald/skv/internal/pkg/kv"
	"github.com/sonald/skv/internal/pkg/rpc"
	"github.com/sonald/skv/internal/pkg/storage"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"log"
)

// protoc --go_out=. --go_opt=paths=source_relative --go-grpc_out=. --go-grpc_opt=paths=source_relative pkg/rpc/service.proto

type SKVServerImpl struct {
	rpc.UnimplementedSKVServer
	rpc.UnimplementedPeerServer
	node *KVNode
}

func (skv *SKVServerImpl) Join(ctx context.Context, req *rpc.PeerRequest) (*rpc.PeerReply, error) {
	log.Printf("Join: id:%s, bind:%s, prevIndex: %v\n", req.GetServerID(), req.GetAddress(), req.GetPrevIndex())
	skv.node.AddNode(req.GetServerID(), req.GetAddress(), req.GetPrevIndex())
	return &rpc.PeerReply{}, nil
}

func (skv *SKVServerImpl) Quit(ctx context.Context, req *rpc.PeerRequest) (*rpc.PeerReply, error) {
	log.Printf("Quit: id:%s, bind:%s, prevIndex: %v\n", req.GetServerID(), req.GetAddress(), req.GetPrevIndex())
	skv.node.RemoveNode(req.GetServerID(), req.GetAddress(), req.GetPrevIndex())
	return &rpc.PeerReply{}, nil
}

func (skv *SKVServerImpl) Del(ctx context.Context, req *rpc.DelRequest) (*rpc.DelReply, error) {
	err := skv.node.Del(req.GetKey())
	log.Printf("Del: err %v\n", err)
	return &rpc.DelReply{Error: 0}, nil
}

func (skv *SKVServerImpl) Get(ctx context.Context, req *rpc.GetRequest) (*rpc.GetReply, error) {
	val, err := skv.node.db.Get(req.GetKey())
	if err == storage.ErrKeyDeleted {
		return &rpc.GetReply{Value: nil}, nil
	}
	return &rpc.GetReply{Value: val}, err
}

func (skv *SKVServerImpl) Put(ctx context.Context, req *rpc.KeyValuePair) (*rpc.PutReply, error) {
	log.Printf("Put(%s, %s)", req.Key, req.Value)
	err := skv.node.Put(req.GetKey(), req.GetValue())
	return &rpc.PutReply{Error: 0}, err
}

func (skv *SKVServerImpl) Scan(opts *rpc.ScanOption, stream rpc.SKV_ScanServer) error {
	var err error
	skv.node.db.Scan(func(k string, v []byte) bool {
		var p = &rpc.KeyValuePair{
			Key:   string(k),
			Value: v,
		}

		if err = stream.Send(p); err != nil {
			return false
		}

		return true
	})
	if err != nil {
		return status.Errorf(codes.Internal, err.Error())
	}
	return nil
}

func (skv *SKVServerImpl) GetMeta(ctx context.Context, req *rpc.GetMetaRequest) (*rpc.GetMetaReply, error) {
	var err error
	data, err := skv.node.GetMeta()
	if err != nil {
		return &rpc.GetMetaReply{}, err
	}

	var servers []*rpc.GetMetaReply_Server
	for _, s := range data {
		servers = append(servers, &rpc.GetMetaReply_Server{
			Address:    s.Address,
			ServerID:   s.ServerID,
			Leader:     s.Leader,
			State:      s.State,
			RpcAddress: s.RpcAddress,
		})
	}

	return &rpc.GetMetaReply{Servers: servers}, nil
}

func NewSKVServer(opts []kv.KVOption, ndopts []NodeOption) *SKVServerImpl {

	return &SKVServerImpl{
		node: NewKVNode(kv.NewKV(opts...), ndopts...),
	}
}

func (skv *SKVServerImpl) Shutdown() {
	skv.node.Shutdown()
}
