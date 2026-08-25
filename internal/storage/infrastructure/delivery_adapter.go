package infrastructure

import (
	"context"
	"fmt"
	delivery "github.com/enterprise/amqp-broker-core/internal/delivery/domain"
)

type DeliveryMemory struct{ db *Memory }

func NewDeliveryMemory(db *Memory) *DeliveryMemory                        { return &DeliveryMemory{db: db} }
func (d *DeliveryMemory) Put(c context.Context, m delivery.Message) error { return d.db.Put(c, m) }
func (d *DeliveryMemory) Get(c context.Context, id string) (delivery.Message, error) {
	msg, err := d.db.GetDelivery(c, id)
	if err != nil {
		return msg, fmt.Errorf("get delivery: %w", err)
	}
	return msg, nil
}
func (d *DeliveryMemory) Update(c context.Context, m delivery.Message) error {
	if err := d.db.Update(c, m); err != nil {
		return fmt.Errorf("update delivery: %w", err)
	}
	return nil
}
func (d *DeliveryMemory) Delete(c context.Context, id string) error {
	if err := d.db.DeleteMessage(c, id); err != nil {
		return fmt.Errorf("delete delivery: %w", err)
	}
	return nil
}
func (d *DeliveryMemory) ListReady(c context.Context, a string, n int) ([]delivery.Message, error) {
	return d.db.ListReady(c, a, n)
}
func (d *DeliveryMemory) ListDead(c context.Context, n int) ([]delivery.Message, error) {
	return d.db.ListDead(c, n)
}
func (d *DeliveryMemory) Depth(c context.Context, a string) (int, error)    { return d.db.Depth(c, a) }
func (d *DeliveryMemory) All(c context.Context) ([]delivery.Message, error) { return d.db.All(c) }
