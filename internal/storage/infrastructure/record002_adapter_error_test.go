package infrastructure

import (
	"context"
	"errors"
	"testing"

	delivery "github.com/enterprise/amqp-broker-core/internal/delivery/domain"
)

func TestDeliveryAdapterGetPreservesNotFound(t *testing.T) {
	_, err := NewDeliveryMemory(NewMemory()).Get(context.Background(), "missing")
	if !errors.Is(err, delivery.ErrMessageNotFound) {
		t.Fatalf("lost: %v", err)
	}
}
func TestDeliveryAdapterUpdatePreservesNotFound(t *testing.T) {
	err := NewDeliveryMemory(NewMemory()).Update(context.Background(), delivery.Message{ID: "missing"})
	if !errors.Is(err, delivery.ErrMessageNotFound) {
		t.Fatalf("lost: %v", err)
	}
}
func TestDeliveryAdapterDeletePreservesNotFound(t *testing.T) {
	err := NewDeliveryMemory(NewMemory()).Delete(context.Background(), "missing")
	if !errors.Is(err, delivery.ErrMessageNotFound) {
		t.Fatalf("lost: %v", err)
	}
}
