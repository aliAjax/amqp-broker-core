package domain

import (
	"testing"
	"time"
)

func newContractSession() *Session {
	return New("session-1", "connection-1", 7, 9, time.Unix(1700000000, 0))
}

func TestBeginMapsSessionForAttach(t *testing.T) {
	s := newContractSession()
	if err := s.Begin(); err != nil {
		t.Fatalf("Begin: %v", err)
	}
	if s.State != StateMapped {
		t.Fatalf("state after Begin = %q, want %q", s.State, StateMapped)
	}
	if err := s.Attach(11, "link-11"); err != nil {
		t.Fatalf("Attach after successful Begin: %v", err)
	}
}

func TestEndIsTerminal(t *testing.T) {
	s := newContractSession()
	s.State = StateMapped
	if err := s.End(); err != nil {
		t.Fatalf("End: %v", err)
	}
	if s.State != StateEnded {
		t.Fatalf("state after End = %q, want %q", s.State, StateEnded)
	}
}

func TestEndClearsEveryAttachedLink(t *testing.T) {
	s := newContractSession()
	s.State = StateMapped
	for handle, id := range map[uint32]string{3: "link-3", 8: "link-8", 13: "link-13"} {
		if err := s.Attach(handle, id); err != nil {
			t.Fatalf("Attach(%d): %v", handle, err)
		}
	}
	if err := s.End(); err != nil {
		t.Fatalf("End: %v", err)
	}
	if got := len(s.Links); got != 0 {
		t.Fatalf("links after End = %d, want 0: %#v", got, s.Links)
	}
}
