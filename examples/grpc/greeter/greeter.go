//go:build linux || darwin || windows

// Package greeter is a gRPC service written the way protoc-gen-go-grpc
// generates one, with JSON messages so that the example needs no protoc: a
// real service generates this file from its .proto.
package greeter

import (
	"context"

	"github.com/lesismal/fib/grpc"
)

type HelloRequest struct{ Name string }

type HelloReply struct{ Message string }

type GreeterServer interface {
	SayHello(context.Context, *HelloRequest) (*HelloReply, error)
	SayHellos(*HelloRequest, grpc.ServerStreamingServer[HelloReply]) error
}

func RegisterGreeterServer(s grpc.ServiceRegistrar, srv GreeterServer) {
	s.RegisterService(&Greeter_ServiceDesc, srv)
}

var Greeter_ServiceDesc = grpc.ServiceDesc{
	ServiceName: "greeter.Greeter",
	HandlerType: (*GreeterServer)(nil),
	Methods: []grpc.MethodDesc{{
		MethodName: "SayHello",
		Handler: func(srv any, ctx context.Context, dec func(any) error, interceptor grpc.UnaryServerInterceptor) (any, error) {
			in := new(HelloRequest)
			if err := dec(in); err != nil {
				return nil, err
			}
			if interceptor == nil {
				return srv.(GreeterServer).SayHello(ctx, in)
			}
			info := &grpc.UnaryServerInfo{Server: srv, FullMethod: "/greeter.Greeter/SayHello"}
			return interceptor(ctx, in, info, func(ctx context.Context, req any) (any, error) {
				return srv.(GreeterServer).SayHello(ctx, req.(*HelloRequest))
			})
		},
	}},
	Streams: []grpc.StreamDesc{{
		StreamName:    "SayHellos",
		ServerStreams: true,
		Handler: func(srv any, stream grpc.ServerStream) error {
			m := new(HelloRequest)
			if err := stream.RecvMsg(m); err != nil {
				return err
			}
			return srv.(GreeterServer).SayHellos(m, &grpc.GenericServerStream[HelloRequest, HelloReply]{ServerStream: stream})
		},
	}},
}

type GreeterClient struct{ cc grpc.ClientConnInterface }

func NewGreeterClient(cc grpc.ClientConnInterface) *GreeterClient { return &GreeterClient{cc} }

// json is the content subtype the example's messages travel as.
var json = grpc.CallContentSubtype("json")

func (c *GreeterClient) SayHello(ctx context.Context, in *HelloRequest, opts ...grpc.CallOption) (*HelloReply, error) {
	out := new(HelloReply)
	if err := c.cc.Invoke(ctx, "/greeter.Greeter/SayHello", in, out, append([]grpc.CallOption{json}, opts...)...); err != nil {
		return nil, err
	}
	return out, nil
}

func (c *GreeterClient) SayHellos(ctx context.Context, in *HelloRequest, opts ...grpc.CallOption) (grpc.ServerStreamingClient[HelloReply], error) {
	stream, err := c.cc.NewStream(ctx, &Greeter_ServiceDesc.Streams[0], "/greeter.Greeter/SayHellos", append([]grpc.CallOption{json}, opts...)...)
	if err != nil {
		return nil, err
	}
	x := &grpc.GenericClientStream[HelloRequest, HelloReply]{ClientStream: stream}
	if err := x.ClientStream.SendMsg(in); err != nil {
		return nil, err
	}
	if err := x.ClientStream.CloseSend(); err != nil {
		return nil, err
	}
	return x, nil
}
