// Code generated by protoc-gen-go-grpc. DO NOT EDIT.
// versions:
// - protoc-gen-go-grpc v1.2.0
// - protoc             v3.19.4
// source: pkg/rpc/service.proto

package rpc

import (
	context "context"
	grpc "google.golang.org/grpc"
	codes "google.golang.org/grpc/codes"
	status "google.golang.org/grpc/status"
)

// This is a compile-time assertion to ensure that this generated file
// is compatible with the grpc package it is being compiled against.
// Requires gRPC-Go v1.32.0 or later.
const _ = grpc.SupportPackageIsVersion7

// SKVClient is the client API for SKV service.
//
// For semantics around ctx use and closing/ending streaming RPCs, please refer to https://pkg.go.dev/google.golang.org/grpc/?tab=doc#ClientConn.NewStream.
type SKVClient interface {
	Get(ctx context.Context, in *GetRequest, opts ...grpc.CallOption) (*GetReply, error)
	Put(ctx context.Context, in *KeyValuePair, opts ...grpc.CallOption) (*PutReply, error)
	Scan(ctx context.Context, in *ScanOption, opts ...grpc.CallOption) (SKV_ScanClient, error)
}

type sKVClient struct {
	cc grpc.ClientConnInterface
}

func NewSKVClient(cc grpc.ClientConnInterface) SKVClient {
	return &sKVClient{cc}
}

func (c *sKVClient) Get(ctx context.Context, in *GetRequest, opts ...grpc.CallOption) (*GetReply, error) {
	out := new(GetReply)
	err := c.cc.Invoke(ctx, "/rpc.SKV/Get", in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *sKVClient) Put(ctx context.Context, in *KeyValuePair, opts ...grpc.CallOption) (*PutReply, error) {
	out := new(PutReply)
	err := c.cc.Invoke(ctx, "/rpc.SKV/Put", in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *sKVClient) Scan(ctx context.Context, in *ScanOption, opts ...grpc.CallOption) (SKV_ScanClient, error) {
	stream, err := c.cc.NewStream(ctx, &SKV_ServiceDesc.Streams[0], "/rpc.SKV/Scan", opts...)
	if err != nil {
		return nil, err
	}
	x := &sKVScanClient{stream}
	if err := x.ClientStream.SendMsg(in); err != nil {
		return nil, err
	}
	if err := x.ClientStream.CloseSend(); err != nil {
		return nil, err
	}
	return x, nil
}

type SKV_ScanClient interface {
	Recv() (*KeyValuePair, error)
	grpc.ClientStream
}

type sKVScanClient struct {
	grpc.ClientStream
}

func (x *sKVScanClient) Recv() (*KeyValuePair, error) {
	m := new(KeyValuePair)
	if err := x.ClientStream.RecvMsg(m); err != nil {
		return nil, err
	}
	return m, nil
}

// SKVServer is the server API for SKV service.
// All implementations must embed UnimplementedSKVServer
// for forward compatibility
type SKVServer interface {
	Get(context.Context, *GetRequest) (*GetReply, error)
	Put(context.Context, *KeyValuePair) (*PutReply, error)
	Scan(*ScanOption, SKV_ScanServer) error
	mustEmbedUnimplementedSKVServer()
}

// UnimplementedSKVServer must be embedded to have forward compatible implementations.
type UnimplementedSKVServer struct {
}

func (UnimplementedSKVServer) Get(context.Context, *GetRequest) (*GetReply, error) {
	return nil, status.Errorf(codes.Unimplemented, "method Get not implemented")
}
func (UnimplementedSKVServer) Put(context.Context, *KeyValuePair) (*PutReply, error) {
	return nil, status.Errorf(codes.Unimplemented, "method Put not implemented")
}
func (UnimplementedSKVServer) Scan(*ScanOption, SKV_ScanServer) error {
	return status.Errorf(codes.Unimplemented, "method Scan not implemented")
}
func (UnimplementedSKVServer) mustEmbedUnimplementedSKVServer() {}

// UnsafeSKVServer may be embedded to opt out of forward compatibility for this service.
// Use of this interface is not recommended, as added methods to SKVServer will
// result in compilation errors.
type UnsafeSKVServer interface {
	mustEmbedUnimplementedSKVServer()
}

func RegisterSKVServer(s grpc.ServiceRegistrar, srv SKVServer) {
	s.RegisterService(&SKV_ServiceDesc, srv)
}

func _SKV_Get_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(GetRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(SKVServer).Get(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: "/rpc.SKV/Get",
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(SKVServer).Get(ctx, req.(*GetRequest))
	}
	return interceptor(ctx, in, info, handler)
}

func _SKV_Put_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(KeyValuePair)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(SKVServer).Put(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: "/rpc.SKV/Put",
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(SKVServer).Put(ctx, req.(*KeyValuePair))
	}
	return interceptor(ctx, in, info, handler)
}

func _SKV_Scan_Handler(srv interface{}, stream grpc.ServerStream) error {
	m := new(ScanOption)
	if err := stream.RecvMsg(m); err != nil {
		return err
	}
	return srv.(SKVServer).Scan(m, &sKVScanServer{stream})
}

type SKV_ScanServer interface {
	Send(*KeyValuePair) error
	grpc.ServerStream
}

type sKVScanServer struct {
	grpc.ServerStream
}

func (x *sKVScanServer) Send(m *KeyValuePair) error {
	return x.ServerStream.SendMsg(m)
}

// SKV_ServiceDesc is the grpc.ServiceDesc for SKV service.
// It's only intended for direct use with grpc.RegisterService,
// and not to be introspected or modified (even as a copy)
var SKV_ServiceDesc = grpc.ServiceDesc{
	ServiceName: "rpc.SKV",
	HandlerType: (*SKVServer)(nil),
	Methods: []grpc.MethodDesc{
		{
			MethodName: "Get",
			Handler:    _SKV_Get_Handler,
		},
		{
			MethodName: "Put",
			Handler:    _SKV_Put_Handler,
		},
	},
	Streams: []grpc.StreamDesc{
		{
			StreamName:    "Scan",
			Handler:       _SKV_Scan_Handler,
			ServerStreams: true,
		},
	},
	Metadata: "pkg/rpc/service.proto",
}
