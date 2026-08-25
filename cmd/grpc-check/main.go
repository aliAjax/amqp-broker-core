package main

import (
	"context"
	"fmt"
	"os"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/protobuf/types/known/structpb"
)

func main() {
	addr := "127.0.0.1:19096"
	if v := os.Getenv("BROKER_GRPC_ADDRESS"); v != "" {
		addr = v
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	conn, err := grpc.DialContext(ctx, addr, grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithBlock())
	if err != nil {
		panic(err)
	}
	defer conn.Close()
	call := func(method string, fields map[string]any) (*structpb.Struct, error) {
		in, err := structpb.NewStruct(fields)
		if err != nil {
			return nil, err
		}
		out := new(structpb.Struct)
		callCtx := metadata.NewOutgoingContext(ctx, metadata.Pairs("authorization", "Bearer change-me"))
		if err := conn.Invoke(callCtx, method, in, out); err != nil {
			return nil, err
		}
		return out, nil
	}
	health, err := call("/broker.v1.Management/Health", map[string]any{})
	if err != nil {
		panic(err)
	}
	depth, err := call("/broker.v1.Management/GetDepth", map[string]any{"name": "plain.q"})
	if err != nil {
		panic(err)
	}
	fmt.Printf("grpc health=%v depth=%v\n", health.AsMap(), depth.AsMap())
}
