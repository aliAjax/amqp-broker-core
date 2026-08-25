package application

import (
	"context"
	addressapp "github.com/enterprise/amqp-broker-core/internal/address/application"
	address "github.com/enterprise/amqp-broker-core/internal/address/domain"
	deliveryapp "github.com/enterprise/amqp-broker-core/internal/delivery/application"
	delivery "github.com/enterprise/amqp-broker-core/internal/delivery/domain"
	routingapp "github.com/enterprise/amqp-broker-core/internal/routing/application"
	routing "github.com/enterprise/amqp-broker-core/internal/routing/domain"
	storage "github.com/enterprise/amqp-broker-core/internal/storage/infrastructure"
	"testing"
)

func fixture(t *testing.T) (context.Context, *Service, *deliveryapp.Service) {
	t.Helper()
	ctx := context.Background()
	db := storage.NewMemory()
	addresses := addressapp.NewService(db, db, nil)
	if _, err := addresses.Create(ctx, address.Create{Name: "jobs", Kind: address.KindQueue}); err != nil {
		t.Fatal(err)
	}
	router := routingapp.NewRouter(db, db, routing.StandardMatcher{})
	deliveries := deliveryapp.NewService(storage.NewDeliveryMemory(db), db, db, router, storage.NewMemoryLog(), nil)
	return ctx, NewService(storage.NewTransactionMemory(db), deliveries, nil, 0), deliveries
}
func TestCommitMakesStagedVisible(t *testing.T) {
	ctx, s, d := fixture(t)
	tx, err := s.Declare(ctx, "connection-1")
	if err != nil {
		t.Fatal(err)
	}
	if _, err = s.StagePublish(ctx, tx.ID, delivery.Publish{Address: "jobs", Body: []byte("commit")}); err != nil {
		t.Fatal(err)
	}
	if depth, _ := d.Depth(ctx, "jobs"); depth != 0 {
		t.Fatalf("staged depth=%d", depth)
	}
	if _, err = s.Commit(ctx, tx.ID); err != nil {
		t.Fatal(err)
	}
	if depth, _ := d.Depth(ctx, "jobs"); depth != 1 {
		t.Fatalf("committed depth=%d", depth)
	}
}
func TestRollbackRemovesStaged(t *testing.T) {
	ctx, s, d := fixture(t)
	tx, err := s.Declare(ctx, "connection-2")
	if err != nil {
		t.Fatal(err)
	}
	if _, err = s.StagePublish(ctx, tx.ID, delivery.Publish{Address: "jobs", Body: []byte("rollback")}); err != nil {
		t.Fatal(err)
	}
	if _, err = s.Rollback(ctx, tx.ID); err != nil {
		t.Fatal(err)
	}
	if depth, _ := d.Depth(ctx, "jobs"); depth != 0 {
		t.Fatalf("rollback depth=%d", depth)
	}
}
