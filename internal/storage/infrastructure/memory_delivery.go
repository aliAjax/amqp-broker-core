package infrastructure

import (
	"context"
	delivery "github.com/enterprise/amqp-broker-core/internal/delivery/domain"
	"sort"
)

func (m *Memory) Put(ctx context.Context, msg delivery.Message) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if _, ok := m.messages[msg.ID]; ok {
		return errConflict("message already exists")
	}
	m.messages[msg.ID] = cloneMessage(msg)
	return nil
}
func (m *Memory) GetMessage(ctx context.Context, id string) (delivery.Message, error) {
	return m.message(ctx, id)
}
func (m *Memory) message(ctx context.Context, id string) (delivery.Message, error) {
	if err := ctx.Err(); err != nil {
		return delivery.Message{}, err
	}
	m.mu.RLock()
	defer m.mu.RUnlock()
	msg, ok := m.messages[id]
	if !ok {
		return msg, delivery.ErrMessageNotFound
	}
	return cloneMessage(msg), nil
}

// GetDelivery avoids Go's lack of overloaded Get methods when one store satisfies several domains.
func (m *Memory) GetDelivery(ctx context.Context, id string) (delivery.Message, error) {
	return m.message(ctx, id)
}
func (m *Memory) Update(ctx context.Context, msg delivery.Message) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if _, ok := m.messages[msg.ID]; !ok {
		return delivery.ErrMessageNotFound
	}
	m.messages[msg.ID] = cloneMessage(msg)
	return nil
}
func (m *Memory) DeleteMessage(ctx context.Context, id string) error { return m.deleteMessage(ctx, id) }
func (m *Memory) deleteMessage(ctx context.Context, id string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if _, ok := m.messages[id]; !ok {
		return delivery.ErrMessageNotFound
	}
	delete(m.messages, id)
	return nil
}
func (m *Memory) ListReady(ctx context.Context, address string, limit int) ([]delivery.Message, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	m.mu.RLock()
	defer m.mu.RUnlock()
	out := []delivery.Message{}
	for _, msg := range m.messages {
		if msg.Address == address && msg.State == delivery.StateAvailable {
			out = append(out, cloneMessage(msg))
		}
	}
	sort.SliceStable(out, func(i, j int) bool {
		if out[i].Priority != out[j].Priority {
			return out[i].Priority > out[j].Priority
		}
		return out[i].CreatedAt.Before(out[j].CreatedAt)
	})
	if limit > 0 && len(out) > limit {
		out = out[:limit]
	}
	return out, nil
}
func (m *Memory) ListDead(ctx context.Context, limit int) ([]delivery.Message, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	m.mu.RLock()
	defer m.mu.RUnlock()
	out := []delivery.Message{}
	for _, msg := range m.messages {
		if msg.State == delivery.StateDeadLettered || msg.State == delivery.StateExpired {
			out = append(out, cloneMessage(msg))
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].CreatedAt.Before(out[j].CreatedAt) })
	if limit > 0 && len(out) > limit {
		out = out[:limit]
	}
	return out, nil
}
func (m *Memory) Depth(ctx context.Context, address string) (int, error) {
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	m.mu.RLock()
	defer m.mu.RUnlock()
	n := 0
	for _, msg := range m.messages {
		if msg.Address == address && msg.State == delivery.StateAvailable {
			n++
		}
	}
	return n, nil
}
func (m *Memory) All(ctx context.Context) ([]delivery.Message, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	m.mu.RLock()
	defer m.mu.RUnlock()
	out := make([]delivery.Message, 0, len(m.messages))
	for _, msg := range m.messages {
		out = append(out, cloneMessage(msg))
	}
	return out, nil
}
func cloneMessage(v delivery.Message) delivery.Message {
	v.Body = append([]byte(nil), v.Body...)
	v.Headers = cloneMap(v.Headers)
	return v
}

type errConflict string

func (e errConflict) Error() string { return string(e) }
