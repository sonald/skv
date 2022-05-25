package main

import (
	"context"
	"github.com/sonald/skv/internal/pkg/kv"
	pb "github.com/sonald/skv/pkg/rpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"log"
)

// protoc --go_out=. --go_opt=paths=source_relative --go-grpc_out=. --go-grpc_opt=paths=source_relative pkg/rpc/service.proto

type SKVServerImpl struct {
	pb.UnimplementedSKVServer
	db kv.KV
}

func (skv *SKVServerImpl) Get(ctx context.Context, req *pb.GetRequest) (*pb.GetReply, error) {
	val, err := skv.db.Get(req.GetKey())
	return &pb.GetReply{Value: val}, err
}
func (skv *SKVServerImpl) Put(ctx context.Context, req *pb.KeyValuePair) (*pb.PutReply, error) {
	log.Printf("Put(%s, %s)", req.Key, req.Value)
	err := skv.db.Put(req.GetKey(), req.GetValue())
	return &pb.PutReply{Error: 0}, err
}
func (skv *SKVServerImpl) Scan(opts *pb.ScanOption, stream pb.SKV_ScanServer) error {
	var err error
	skv.db.Scan(func(k string, v string) bool {
		var p = &pb.KeyValuePair{
			Key:   k,
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

func NewSKVServer(opts ...kv.KVOption) pb.SKVServer {
	return &SKVServerImpl{
		db: kv.NewKV(opts...),
	}
}

func (skv *SKVServerImpl) Shutdown() {
	skv.db.Close()
}
