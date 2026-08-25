package application

import (
	"errors"
	domain "github.com/enterprise/amqp-broker-core/internal/session/domain"
	"sync"
)

type Registry struct {
	mu       sync.RWMutex
	items    map[string]*domain.Session
	channels map[string]string
}

func NewRegistry() *Registry {
	return &Registry{items: map[string]*domain.Session{}, channels: map[string]string{}}
}
func key(connection string, channel uint16) string { return connection + ":" + string(rune(channel)) }
func (r *Registry) Add(s *domain.Session) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	k := key(s.ConnectionID, s.LocalChannel)
	if _, ok := r.channels[k]; ok {
		return errors.New("session channel already exists")
	}
	r.items[s.ID] = s
	r.channels[k] = s.ID
	return nil
}
func (r *Registry) Get(id string) (*domain.Session, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	s, ok := r.items[id]
	return s, ok
}
func (r *Registry) ByChannel(connection string, channel uint16) (*domain.Session, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	id, ok := r.channels[key(connection, channel)]
	if !ok {
		return nil, false
	}
	s, ok := r.items[id]
	return s, ok
}
func (r *Registry) Remove(id string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	_, ok := r.items[id]
	if ok {
		delete(r.items, id)
	}
}
func (r *Registry) RemoveConnection(connection string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	for id, s := range r.items {
		if s.ConnectionID == connection {
			delete(r.items, id)
		}
	}
}
