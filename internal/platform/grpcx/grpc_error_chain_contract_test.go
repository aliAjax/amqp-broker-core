package grpcx

import (
	"context"
	"errors"
	"net"
	"syscall"
	"testing"
)

func TestStartPreservesAddressInUseError(t *testing.T) {
	occupied, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer occupied.Close()

	s := &Server{Address: occupied.Addr().String()}
	err = s.Start(context.Background())
	if err == nil {
		t.Fatal("Start returned nil on occupied address")
	}
	if !errors.Is(err, syscall.EADDRINUSE) {
		t.Fatalf("errors.Is(err, syscall.EADDRINUSE) = false: %T %v", err, err)
	}
	var opErr *net.OpError
	if !errors.As(err, &opErr) {
		t.Fatalf("errors.As(err, *net.OpError) = false: %T %v", err, err)
	}
}
