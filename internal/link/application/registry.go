package application

import (
	"errors"
	domain "github.com/enterprise/amqp-broker-core/internal/link/domain"
	"sync"
)

type Registry struct {
	mu      sync.RWMutex
	items   map[string]*domain.Link
	handles map[string]string
}

func NewRegistry() *Registry {
	return &Registry{items: map[string]*domain.Link{}, handles: map[string]string{}}
}
func handleKey(session string, handle uint32) string { return session + ":" + string(rune(handle)) }
func (r *Registry) Add(l *domain.Link) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	k := handleKey(l.SessionID, l.Handle)
	if _, ok := r.handles[k]; ok {
		return errors.New("link handle already exists")
	}
	r.items[l.ID] = l
	r.handles[k] = l.ID
	return nil
}
func (r *Registry) Get(id string) (*domain.Link, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	l, ok := r.items[id]
	if !ok {
		return nil, false
	}
	return l, true
}
func (r *Registry) ByHandle(session string, handle uint32) (*domain.Link, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	id, ok := r.handles[handleKey(session, handle)]
	if !ok {
		return nil, false
	}
	l, ok := r.items[id]
	if !ok {
		return nil, false
	}
	return l, true
}
func (r *Registry) Remove(id string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if l, ok := r.items[id]; ok {
		delete(r.handles, handleKey(l.SessionID, l.Handle))
		delete(r.items, id)
	}
}
func (r *Registry) RemoveSession(session string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	for id, l := range r.items {
		if l.SessionID == session {
			delete(r.handles, handleKey(session, l.Handle))
			delete(r.items, id)
		}
	}
}
