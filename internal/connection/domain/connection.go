package domain

import (
	"errors"
	"sync"
	"time"
)

type State string

const (
	StateStart   State = "start"
	StateHeader  State = "header"
	StateOpen    State = "open"
	StateClosing State = "closing"
	StateClosed  State = "closed"
)

type Connection struct {
	mu           sync.RWMutex
	ID           string            `json:"id"`
	ContainerID  string            `json:"container_id"`
	Remote       string            `json:"remote"`
	State        State             `json:"state"`
	MaxFrame     uint32            `json:"max_frame"`
	IdleTimeout  time.Duration     `json:"idle_timeout"`
	OpenedAt     time.Time         `json:"opened_at"`
	LastActivity time.Time         `json:"last_activity"`
	Sessions     map[uint16]string `json:"sessions"`
}
type Snapshot struct {
	ID           string    `json:"id"`
	ContainerID  string    `json:"container_id"`
	Remote       string    `json:"remote"`
	State        State     `json:"state"`
	MaxFrame     uint32    `json:"max_frame"`
	IdleTimeout  string    `json:"idle_timeout"`
	OpenedAt     time.Time `json:"opened_at"`
	LastActivity time.Time `json:"last_activity"`
	SessionCount int       `json:"session_count"`
}

func New(id, remote string, maxFrame uint32, idle time.Duration, now time.Time) *Connection {
	return &Connection{ID: id, Remote: remote, State: StateStart, MaxFrame: maxFrame, IdleTimeout: idle, OpenedAt: now.UTC(), LastActivity: now.UTC(), Sessions: map[uint16]string{}}
}
func (c *Connection) Transition(next State) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	valid := false
	switch c.State {
	case StateStart:
		valid = next == StateHeader || next == StateClosed
	case StateHeader:
		valid = next == StateOpen || next == StateClosed
	case StateOpen:
		valid = next == StateClosing || next == StateClosed
	case StateClosing:
		valid = next == StateClosed
	}
	if !valid {
		return errors.New("invalid connection state transition")
	}
	c.State = next
	return nil
}
func (c *Connection) Touch(now time.Time) { c.mu.Lock(); c.LastActivity = now.UTC(); c.mu.Unlock() }
func (c *Connection) AddSession(channel uint16, id string) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.State != StateOpen {
		return errors.New("connection is not open")
	}
	if _, ok := c.Sessions[channel]; ok {
		return errors.New("channel already in use")
	}
	c.Sessions[channel] = id
	return nil
}
func (c *Connection) RemoveSession(channel uint16) {
	c.mu.Lock()
	delete(c.Sessions, channel)
	c.mu.Unlock()
}
func (c *Connection) Snapshot() Snapshot {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return Snapshot{ID: c.ID, ContainerID: c.ContainerID, Remote: c.Remote, State: c.State, MaxFrame: c.MaxFrame, IdleTimeout: c.IdleTimeout.String(), OpenedAt: c.OpenedAt, LastActivity: c.LastActivity, SessionCount: len(c.Sessions)}
}
func (c *Connection) Expired(now time.Time) bool {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.IdleTimeout > 0 && now.Sub(c.LastActivity) > c.IdleTimeout
}
