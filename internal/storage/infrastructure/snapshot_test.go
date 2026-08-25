package infrastructure

import (
	"context"
	address "github.com/enterprise/amqp-broker-core/internal/address/domain"
	delivery "github.com/enterprise/amqp-broker-core/internal/delivery/domain"
	"path/filepath"
	"testing"
	"time"
)

func TestSnapshotRestoresAndReleasesUnsettled(t *testing.T) {
	ctx := context.Background()
	db := NewMemory()
	a, _ := address.New(address.Create{Name: "jobs", Kind: address.KindQueue}, time.Now())
	if err := db.Create(ctx, a); err != nil {
		t.Fatal(err)
	}
	repo := NewDeliveryMemory(db)
	msg, _ := delivery.NewMessage("m1", delivery.Publish{Address: "jobs", Body: []byte("data")}, time.Now())
	msg.State = delivery.StateAcquired
	if err := repo.Put(ctx, msg); err != nil {
		t.Fatal(err)
	}
	consumer, _ := delivery.NewConsumer("c1", "jobs", 1, delivery.AtLeastOnce, time.Now())
	consumer.Unsettled[1] = msg.ID
	if err := db.SaveConsumer(ctx, consumer); err != nil {
		t.Fatal(err)
	}
	path := t.TempDir()
	file := NewSnapshotFile(path)
	if err := file.Save(ctx, db); err != nil {
		t.Fatal(err)
	}
	restored := NewMemory()
	if err := file.Load(ctx, restored); err != nil {
		t.Fatal(err)
	}
	got, err := NewDeliveryMemory(restored).Get(ctx, msg.ID)
	if err != nil {
		t.Fatal(err)
	}
	if got.State != delivery.StateReleased || !got.Redelivered {
		t.Fatalf("message not released: %#v", got)
	}
	if filepath.Base(file.path) != "metadata.json" {
		t.Fatal("unexpected snapshot name")
	}
}
