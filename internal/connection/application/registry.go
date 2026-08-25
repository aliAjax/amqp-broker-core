package application

import (
	"errors"
	domain "github.com/enterprise/amqp-broker-core/internal/connection/domain"
	"sort"
	"sync"
	"time"
)

type Registry struct {
	mu    sync.RWMutex
	max   int
	items map[string]*domain.Connection
}

func NewRegistry(max int) *Registry {
	return &Registry{max: max, items: map[string]*domain.Connection{}}
}
func (r *Registry) Add(c *domain.Connection) error {
	if len(r.items) >= r.max {
		return errors.New("connection limit reached")
	}
	if _, ok := r.items[c.ID]; ok {
		return errors.New("connection already registered")
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	r.items[c.ID] = c
	return nil
}
func (r *Registry) Get(id string) (*domain.Connection, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	c, ok := r.items[id]
	return c, ok
}
func (r *Registry) Remove(id string) { r.mu.Lock(); delete(r.items, id); r.mu.Unlock() }
func (r *Registry) List() []domain.Snapshot {
	r.mu.RLock()
	defer r.mu.RUnlock()
	out := make([]domain.Snapshot, 0, len(r.items))
	for _, c := range r.items {
		out = append(out, c.Snapshot())
	}
	sort.Slice(out, func(i, j int) bool { return out[i].OpenedAt.Before(out[j].OpenedAt) })
	return out
}
func (r *Registry) Expired(now time.Time) []*domain.Connection {
	r.mu.RLock()
	defer r.mu.RUnlock()
	out := []*domain.Connection{}
	for _, c := range r.items {
		if c.IdleTimeout > 0 && now.Sub(c.LastActivity) > c.IdleTimeout {
			out = append(out, c)
		}
	}
	return out
}
func (r *Registry) Count() int { r.mu.RLock(); defer r.mu.RUnlock(); return len(r.items) }
