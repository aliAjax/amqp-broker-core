package infrastructure

import (
	"context"
	"testing"
	"time"

	delivery "github.com/enterprise/amqp-broker-core/internal/delivery/domain"
)

func releasedRecoveryMessage() delivery.Message {
	return delivery.Message{
		ID:        "released-a",
		Address:   "telemetry.raw",
		Body:      []byte("payload"),
		State:     delivery.StateReleased,
		CreatedAt: time.Unix(10, 0),
	}
}

func TestReleasedMessageReturnsToReadySet(t *testing.T) {
	ctx := context.Background()
	store := NewMemory()
	if err := store.Put(ctx, releasedRecoveryMessage()); err != nil {
		t.Fatal(err)
	}
	items, err := store.ListReady(ctx, "telemetry.raw", 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(items) != 1 || items[0].ID != "released-a" {
		t.Fatalf("ready=%v, want released-a", items)
	}
}

func TestReadyDepthMatchesRecoveryState(t *testing.T) {
	ctx := context.Background()
	store := NewMemory()
	if err := store.Put(ctx, releasedRecoveryMessage()); err != nil {
		t.Fatal(err)
	}
	depth, err := store.Depth(ctx, "telemetry.raw")
	if err != nil {
		t.Fatal(err)
	}
	if depth != 1 {
		t.Fatalf("depth=%d, want 1", depth)
	}
}
