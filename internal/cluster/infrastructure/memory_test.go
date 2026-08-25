package infrastructure

import (
	"context"
	"testing"
	"time"
)

func TestLeaseFencing(t *testing.T) {
	c := NewCoordinator()
	ctx := context.Background()
	first, err := c.Acquire(ctx, 7, "node-a", time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = c.Acquire(ctx, 7, "node-b", time.Minute); err == nil {
		t.Fatal("expected active lease conflict")
	}
	stale := first
	stale.Epoch--
	if _, err = c.Renew(ctx, stale, time.Minute); err == nil {
		t.Fatal("expected fencing rejection")
	}
	if err = c.Release(ctx, first); err != nil {
		t.Fatal(err)
	}
	second, err := c.Acquire(ctx, 7, "node-b", time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if second.Epoch <= first.Epoch {
		t.Fatal("epoch did not advance")
	}
}
