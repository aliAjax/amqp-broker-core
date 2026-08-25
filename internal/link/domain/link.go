package domain

import (
	"errors"
	"sync"
	"time"
)

type Role bool

const (
	RoleSender   Role = false
	RoleReceiver Role = true
)

type State string

const (
	StateDetached   State = "detached"
	StateAttachSent State = "attach-sent"
	StateAttached   State = "attached"
	StateDetachSent State = "detach-sent"
)

type Link struct {
	mu             sync.RWMutex
	ID             string    `json:"id"`
	SessionID      string    `json:"session_id"`
	Name           string    `json:"name"`
	Handle         uint32    `json:"handle"`
	Role           Role      `json:"role"`
	Address        string    `json:"address"`
	State          State     `json:"state"`
	Credit         uint32    `json:"credit"`
	DeliveryCount  uint32    `json:"delivery_count"`
	MaxMessageSize uint64    `json:"max_message_size"`
	CreatedAt      time.Time `json:"created_at"`
}

func New(id, session, name, address string, handle uint32, role Role, now time.Time) (*Link, error) {
	if id == "" || session == "" || name == "" || address == "" {
		return nil, errors.New("link identity and address required")
	}
	return &Link{ID: id, SessionID: session, Name: name, Address: address, Handle: handle, Role: role, State: StateDetached, MaxMessageSize: 16 << 20, CreatedAt: now.UTC()}, nil
}
func (l *Link) Attach() error {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.State != StateDetached && l.State != StateAttachSent {
		return errors.New("link cannot attach")
	}
	l.State = StateAttached
	return nil
}
func (l *Link) Detach() {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.State = StateDetached
	l.Credit = 0
}
func (l *Link) Grant(credit uint32) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.Credit = credit
}
func (l *Link) Consume(size uint64) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.State != StateAttached {
		return errors.New("link is not attached")
	}
	if l.Role == RoleSender && l.Credit == 0 {
		return errors.New("link credit exhausted")
	}
	if size > l.MaxMessageSize {
		return errors.New("message exceeds link maximum")
	}
	if l.Role == RoleSender {
		l.Credit--
	}
	l.DeliveryCount++
	return nil
}
func (l *Link) Snapshot() (State, uint32) {
	l.mu.RLock()
	defer l.mu.RUnlock()
	return l.State, l.Credit
}
