package broker

import (
	"context"
	"errors"
	"net"
	"os"
	"path/filepath"
	"syscall"
	"testing"
)

func TestBuildPreservesDataDirectoryErrorChain(t *testing.T) {
	root := t.TempDir()
	regular := filepath.Join(root, "not-a-directory")
	if err := os.WriteFile(regular, []byte("x"), 0600); err != nil {
		t.Fatal(err)
	}
	dataDir := filepath.Join(regular, "child")
	_, err := Build(Settings{NodeID: "node-contract", DataDir: dataDir, AMQPAddress: "127.0.0.1:0"})
	if err == nil {
		t.Fatal("Build returned nil error for child of regular file")
	}
	if !errors.Is(err, syscall.ENOTDIR) {
		t.Fatalf("errors.Is(err, syscall.ENOTDIR) = false: %T %v", err, err)
	}
	var pathErr *os.PathError
	if !errors.As(err, &pathErr) {
		t.Fatalf("errors.As(err, *os.PathError) = false: %T %v", err, err)
	}
}

func TestRuntimeStartPreservesListenErrorChain(t *testing.T) {
	occupied, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer occupied.Close()

	r, err := Build(Settings{
		NodeID:      "node-contract",
		DataDir:    t.TempDir(),
		AMQPAddress: occupied.Addr().String(),
	})
	if err != nil {
		t.Fatalf("Build: %v", err)
	}
	defer r.BodyLog.Close()

	err = r.Start(context.Background())
	if err == nil {
		t.Fatal("Runtime.Start returned nil on occupied address")
	}
	if !errors.Is(err, syscall.EADDRINUSE) {
		t.Fatalf("errors.Is(err, syscall.EADDRINUSE) = false: %T %v", err, err)
	}
	var opErr *net.OpError
	if !errors.As(err, &opErr) {
		t.Fatalf("errors.As(err, *net.OpError) = false: %T %v", err, err)
	}
}
