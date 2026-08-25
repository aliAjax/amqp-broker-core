package application

import (
	"fmt"
	domain "github.com/enterprise/amqp-broker-core/internal/session/domain"
	"testing"
	"time"
)

func contractSession(id, connection string, channel uint16) *domain.Session {
	return domain.New(id, connection, channel, channel, time.Unix(1700000000, 0))
}

func TestRegistryRemovalReleasesChannel(t *testing.T) {
	r := NewRegistry()
	first := contractSession("session-old", "connection-a", 17)
	if err := r.Add(first); err != nil {
		t.Fatalf("Add first: %v", err)
	}
	r.Remove(first.ID)
	if _, ok := r.Get(first.ID); ok {
		t.Fatal("removed session remains in item index")
	}
	if got, ok := r.ByChannel("connection-a", 17); ok || got != nil {
		t.Fatalf("removed channel resolves to %#v, ok=%v", got, ok)
	}
	replacement := contractSession("session-new", "connection-a", 17)
	if err := r.Add(replacement); err != nil {
		t.Fatalf("channel was not released for replacement: %v", err)
	}
}

func TestRemoveConnectionClearsAllChannelIndexes(t *testing.T) {
	r := NewRegistry()
	for _, s := range []*domain.Session{
		contractSession("session-1", "connection-a", 1),
		contractSession("session-2", "connection-a", 2),
		contractSession("session-other", "connection-b", 1),
	} {
		if err := r.Add(s); err != nil {
			t.Fatalf("Add(%s): %v", s.ID, err)
		}
	}
	r.RemoveConnection("connection-a")
	for _, channel := range []uint16{1, 2} {
		if got, ok := r.ByChannel("connection-a", channel); ok || got != nil {
			t.Fatalf("removed connection channel %d resolves to %#v, ok=%v", channel, got, ok)
		}
		replacement := contractSession(fmt.Sprintf("replacement-%d", channel), "connection-a", channel)
		if err := r.Add(replacement); err != nil {
			t.Fatalf("channel %d was not released: %v", channel, err)
		}
	}
	if got, ok := r.ByChannel("connection-b", 1); !ok || got == nil || got.ID != "session-other" {
		t.Fatalf("unrelated connection was removed: %#v, ok=%v", got, ok)
	}
}
