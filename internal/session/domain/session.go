package domain

import (
	"errors"
	"sync"
	"time"
)

type State string

const (
	StateUnmapped  State = "unmapped"
	StateBeginSent State = "begin-sent"
	StateMapped    State = "mapped"
	StateEndSent   State = "end-sent"
	StateEnded     State = "ended"
)

type Session struct {
	mu             sync.RWMutex
	ID             string            `json:"id"`
	ConnectionID   string            `json:"connection_id"`
	LocalChannel   uint16            `json:"local_channel"`
	RemoteChannel  uint16            `json:"remote_channel"`
	State          State             `json:"state"`
	NextOutgoingID uint32            `json:"next_outgoing_id"`
	IncomingWindow uint32            `json:"incoming_window"`
	OutgoingWindow uint32            `json:"outgoing_window"`
	Links          map[uint32]string `json:"links"`
	CreatedAt      time.Time         `json:"created_at"`
}

func New(id, connection string, local, remote uint16, now time.Time) *Session {
	return &Session{ID: id, ConnectionID: connection, LocalChannel: local, RemoteChannel: remote, State: StateUnmapped, IncomingWindow: 2048, OutgoingWindow: 2048, Links: map[uint32]string{}, CreatedAt: now.UTC()}
}
func (s *Session) Begin() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.State != StateUnmapped && s.State != StateBeginSent {
		return errors.New("session cannot begin")
	}
	s.State = StateMapped
	return nil
}
func (s *Session) End() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.State != StateMapped && s.State != StateEndSent {
		return errors.New("session cannot end")
	}
	s.State = StateEnded
	s.Links = map[uint32]string{}
	return nil
}
func (s *Session) Attach(handle uint32, linkID string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.State != StateMapped {
		return errors.New("session not mapped")
	}
	if _, ok := s.Links[handle]; ok {
		return errors.New("handle already attached")
	}
	if handle > 65535 {
		return errors.New("handle exceeds negotiated maximum")
	}
	s.Links[handle] = linkID
	return nil
}
func (s *Session) Detach(handle uint32) { s.mu.Lock(); delete(s.Links, handle); s.mu.Unlock() }
func (s *Session) ConsumeIncoming() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.IncomingWindow == 0 {
		return errors.New("incoming session window exhausted")
	}
	s.IncomingWindow--
	return nil
}
func (s *Session) AdvanceOutgoing() (uint32, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.OutgoingWindow == 0 {
		return 0, errors.New("outgoing session window exhausted")
	}
	id := s.NextOutgoingID
	s.NextOutgoingID++
	s.OutgoingWindow--
	return id, nil
}
func (s *Session) Flow(incoming, outgoing uint32) {
	s.mu.Lock()
	s.IncomingWindow = incoming
	s.OutgoingWindow = outgoing
	s.mu.Unlock()
}
