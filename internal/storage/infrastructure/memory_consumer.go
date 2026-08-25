package infrastructure

import (
	"context"
	"errors"
	delivery "github.com/enterprise/amqp-broker-core/internal/delivery/domain"
	"sort"
)

func (m *Memory) SaveConsumer(ctx context.Context, c delivery.Consumer) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	c.Unsettled = cloneUnsettled(c.Unsettled)
	m.consumers[c.ID] = c
	return nil
}
func (m *Memory) GetConsumer(ctx context.Context, id string) (delivery.Consumer, error) {
	if err := ctx.Err(); err != nil {
		return delivery.Consumer{}, err
	}
	m.mu.RLock()
	defer m.mu.RUnlock()
	c, ok := m.consumers[id]
	if !ok {
		return c, errors.New("consumer not found")
	}
	c.Unsettled = cloneUnsettled(c.Unsettled)
	return c, nil
}
func (m *Memory) ListConsumers(ctx context.Context, address string) ([]delivery.Consumer, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	m.mu.RLock()
	defer m.mu.RUnlock()
	out := []delivery.Consumer{}
	for _, c := range m.consumers {
		if address == "" || c.Address == address {
			c.Unsettled = cloneUnsettled(c.Unsettled)
			out = append(out, c)
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].ID < out[j].ID })
	return out, nil
}
func (m *Memory) DeleteConsumer(ctx context.Context, id string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if _, ok := m.consumers[id]; !ok {
		return errors.New("consumer not found")
	}
	delete(m.consumers, id)
	return nil
}
func cloneUnsettled(v map[uint32]string) map[uint32]string {
	out := make(map[uint32]string, len(v))
	for k, x := range v {
		out[k] = x
	}
	return out
}
