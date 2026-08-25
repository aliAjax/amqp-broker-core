package application

import (
	"context"
	addressapp "github.com/enterprise/amqp-broker-core/internal/address/application"
	address "github.com/enterprise/amqp-broker-core/internal/address/domain"
	delivery "github.com/enterprise/amqp-broker-core/internal/delivery/domain"
	routingapp "github.com/enterprise/amqp-broker-core/internal/routing/application"
	routing "github.com/enterprise/amqp-broker-core/internal/routing/domain"
	storage "github.com/enterprise/amqp-broker-core/internal/storage/infrastructure"
	"testing"
	"time"
)

type fakeClock struct{ now time.Time }

func (f *fakeClock) Now() time.Time { return f.now }
func setup(t *testing.T) (context.Context, *Service, *addressapp.Service, *fakeClock) {
	t.Helper()
	ctx := context.Background()
	db := storage.NewMemory()
	clock := &fakeClock{now: time.Date(2026, 8, 21, 0, 0, 0, 0, time.UTC)}
	addresses := addressapp.NewService(db, db, clock)
	router := routingapp.NewRouter(db, db, routing.StandardMatcher{})
	service := NewService(storage.NewDeliveryMemory(db), db, db, router, storage.NewMemoryLog(), clock)
	return ctx, service, addresses, clock
}
func TestCreditReleaseAndRedelivery(t *testing.T) {
	ctx, s, a, _ := setup(t)
	if _, err := a.Create(ctx, address.Create{Name: "jobs", Kind: address.KindQueue}); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Publish(ctx, delivery.Publish{Address: "jobs", Body: []byte("one")}); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Register(ctx, "c1", "jobs", 1, delivery.AtLeastOnce); err != nil {
		t.Fatal(err)
	}
	msg, id, err := s.Acquire(ctx, "c1")
	if err != nil {
		t.Fatal(err)
	}
	if msg.Redelivered {
		t.Fatal("first delivery marked redelivered")
	}
	if _, _, err = s.Acquire(ctx, "c1"); err == nil {
		t.Fatal("expected credit exhaustion")
	}
	if err = s.Settle(ctx, "c1", id, delivery.StateReleased); err != nil {
		t.Fatal(err)
	}
	msg, _, err = s.Acquire(ctx, "c1")
	if err != nil {
		t.Fatal(err)
	}
	if !msg.Redelivered || msg.DeliveryCount != 2 {
		t.Fatalf("expected redelivery: %#v", msg)
	}
}
func TestTTLDeadLetter(t *testing.T) {
	ctx, s, a, clock := setup(t)
	if _, err := a.Create(ctx, address.Create{Name: "dead", Kind: address.KindQueue}); err != nil {
		t.Fatal(err)
	}
	if _, err := a.Create(ctx, address.Create{Name: "jobs", Kind: address.KindQueue, DeadLetter: "dead"}); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Publish(ctx, delivery.Publish{Address: "jobs", Body: []byte("expires"), TTL: time.Second}); err != nil {
		t.Fatal(err)
	}
	clock.now = clock.now.Add(2 * time.Second)
	n, err := s.SweepExpired(ctx)
	if err != nil || n != 1 {
		t.Fatalf("sweep=%d err=%v", n, err)
	}
	depth, err := s.Depth(ctx, "dead")
	if err != nil || depth != 1 {
		t.Fatalf("dead depth=%d err=%v", depth, err)
	}
	replayed, err := s.ReplayDead(ctx, 10)
	if err != nil || replayed != 1 {
		t.Fatalf("replayed=%d err=%v", replayed, err)
	}
	if depth, _ := s.Depth(ctx, "jobs"); depth != 1 {
		t.Fatalf("replayed jobs depth=%d", depth)
	}
}
func TestPauseRejectsIngress(t *testing.T) {
	ctx, s, a, _ := setup(t)
	if _, err := a.Create(ctx, address.Create{Name: "jobs", Kind: address.KindQueue}); err != nil {
		t.Fatal(err)
	}
	if _, err := a.Pause(ctx, "jobs", true); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Publish(ctx, delivery.Publish{Address: "jobs", Body: []byte("blocked")}); err == nil {
		t.Fatal("expected paused ingress rejection")
	}
}
