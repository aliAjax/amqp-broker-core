package infrastructure

import (
	"context"
	"errors"
	"sort"
	"sync"

	address "github.com/enterprise/amqp-broker-core/internal/address/domain"
	delivery "github.com/enterprise/amqp-broker-core/internal/delivery/domain"
	transaction "github.com/enterprise/amqp-broker-core/internal/transaction/domain"
)

type Memory struct {
	mu           sync.RWMutex
	addresses    map[string]address.Address
	bindings     map[string]address.Binding
	messages     map[string]delivery.Message
	consumers    map[string]delivery.Consumer
	transactions map[string]transaction.Transaction
}

func NewMemory() *Memory {
	return &Memory{addresses: map[string]address.Address{}, bindings: map[string]address.Binding{}, messages: map[string]delivery.Message{}, consumers: map[string]delivery.Consumer{}, transactions: map[string]transaction.Transaction{}}
}
func (m *Memory) Create(ctx context.Context, a address.Address) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if _, ok := m.addresses[a.Name]; ok {
		return address.ErrConflict
	}
	m.addresses[a.Name] = a
	return nil
}
func (m *Memory) Get(ctx context.Context, name string) (address.Address, error) {
	if err := ctx.Err(); err != nil {
		return address.Address{}, err
	}
	m.mu.RLock()
	defer m.mu.RUnlock()
	a, ok := m.addresses[name]
	if !ok {
		return a, address.ErrNotFound
	}
	return a, nil
}
func (m *Memory) List(ctx context.Context) ([]address.Address, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	m.mu.RLock()
	defer m.mu.RUnlock()
	out := make([]address.Address, 0, len(m.addresses))
	for _, a := range m.addresses {
		out = append(out, a)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Name < out[j].Name })
	return out, nil
}
func (m *Memory) SetPaused(ctx context.Context, name string, paused bool) (address.Address, error) {
	if err := ctx.Err(); err != nil {
		return address.Address{}, err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	a, ok := m.addresses[name]
	if !ok {
		return a, address.ErrNotFound
	}
	a.Paused = paused
	a.Version++
	m.addresses[name] = a
	return a, nil
}
func (m *Memory) Delete(ctx context.Context, name string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if _, ok := m.addresses[name]; !ok {
		return address.ErrNotFound
	}
	delete(m.addresses, name)
	return nil
}
func (m *Memory) CreateBinding(ctx context.Context, b address.Binding) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if _, ok := m.bindings[b.ID]; ok {
		return errors.New("binding already exists")
	}
	m.bindings[b.ID] = cloneBinding(b)
	return nil
}
func (m *Memory) ListBindings(ctx context.Context, source string) ([]address.Binding, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	m.mu.RLock()
	defer m.mu.RUnlock()
	out := []address.Binding{}
	for _, b := range m.bindings {
		if source == "" || b.Source == source {
			out = append(out, cloneBinding(b))
		}
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].Priority == out[j].Priority {
			return out[i].ID < out[j].ID
		}
		return out[i].Priority > out[j].Priority
	})
	return out, nil
}
func (m *Memory) DeleteBinding(ctx context.Context, id string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if _, ok := m.bindings[id]; !ok {
		return errors.New("binding not found")
	}
	delete(m.bindings, id)
	return nil
}
func cloneBinding(b address.Binding) address.Binding { b.Filter = cloneMap(b.Filter); return b }
func cloneMap(v map[string]string) map[string]string {
	out := make(map[string]string, len(v))
	for k, x := range v {
		out[k] = x
	}
	return out
}
