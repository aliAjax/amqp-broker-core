package worker

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestStopPreservesDeadlineError(t *testing.T) {
	ctx, cancel := context.WithDeadline(context.Background(), time.Unix(1, 0))
	defer cancel()
	s := New(nil, time.Second, nil)
	err := s.Stop(ctx)
	if err == nil {
		t.Fatal("Stop returned nil for an expired context while worker is not done")
	}
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("errors.Is(err, context.DeadlineExceeded) = false: %T %v", err, err)
	}
}
