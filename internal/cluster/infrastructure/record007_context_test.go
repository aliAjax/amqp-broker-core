package infrastructure

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestAcquireCancellationDoesNotCreateLease(t *testing.T) {
	c := NewCoordinator()
	x, cancel := context.WithCancel(context.Background())
	cancel()
	if _, e := c.Acquire(x, 4, "n", time.Minute); !errors.Is(e, context.Canceled) {
		t.Fatalf("Acquire=%v", e)
	}
	ls, _ := c.List(context.Background())
	if len(ls) != 0 {
		t.Fatal("lease created")
	}
}
func TestReleaseCancellationPreservesLease(t *testing.T) {
	c := NewCoordinator()
	l, _ := c.Acquire(context.Background(), 5, "n", time.Minute)
	x, cancel := context.WithCancel(context.Background())
	cancel()
	if e := c.Release(x, l); !errors.Is(e, context.Canceled) {
		t.Fatalf("Release=%v", e)
	}
	ls, _ := c.List(context.Background())
	if len(ls) != 1 {
		t.Fatal("lease removed")
	}
}
func TestListReturnsCancellation(t *testing.T) {
	c := NewCoordinator()
	_, _ = c.Acquire(context.Background(), 6, "n", time.Minute)
	x, cancel := context.WithCancel(context.Background())
	cancel()
	ls, e := c.List(x)
	if !errors.Is(e, context.Canceled) || ls != nil {
		t.Fatalf("List=%v %v", ls, e)
	}
}
