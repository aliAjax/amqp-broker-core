package infrastructure

import (
	"context"
	delivery "github.com/enterprise/amqp-broker-core/internal/delivery/domain"
)

type DeliveryMemory struct{ db *Memory }

func NewDeliveryMemory(db *Memory) *DeliveryMemory                        { return &DeliveryMemory{db: db} }
func (d *DeliveryMemory) Put(c context.Context, m delivery.Message) error { return d.db.Put(c, m) }
func (d *DeliveryMemory) Get(c context.Context, id string) (delivery.Message, error) {
	return d.db.GetDelivery(c, id)
}
func (d *DeliveryMemory) Update(c context.Context, m delivery.Message) error {
	return d.db.Update(c, m)
}
func (d *DeliveryMemory) Delete(c context.Context, id string) error { return d.db.DeleteMessage(c, id) }
func (d *DeliveryMemory) ListReady(c context.Context, a string, n int) ([]delivery.Message, error) {
	return d.db.ListReady(c, a, n)
}
func (d *DeliveryMemory) ListDead(c context.Context, n int) ([]delivery.Message, error) {
	return d.db.ListDead(c, n)
}
func (d *DeliveryMemory) Depth(c context.Context, a string) (int, error)    { return d.db.Depth(c, a) }
func (d *DeliveryMemory) All(c context.Context) ([]delivery.Message, error) { return d.db.All(c) }
