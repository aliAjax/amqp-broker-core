package domain

import (
	"errors"
	"time"
)

type State string

const (
	StateActive     State = "active"
	StateCommitted  State = "committed"
	StateRolledBack State = "rolled-back"
	StateTimedOut   State = "timed-out"
)

type OperationKind string

const (
	OpPublish OperationKind = "publish"
	OpAccept  OperationKind = "accept"
	OpRelease OperationKind = "release"
)

type Operation struct {
	Kind       OperationKind `json:"kind"`
	MessageID  string        `json:"message_id"`
	Address    string        `json:"address"`
	DeliveryID uint32        `json:"delivery_id"`
}
type Transaction struct {
	ID           string      `json:"id"`
	ConnectionID string      `json:"connection_id"`
	State        State       `json:"state"`
	CreatedAt    time.Time   `json:"created_at"`
	ExpiresAt    time.Time   `json:"expires_at"`
	CompletedAt  time.Time   `json:"completed_at,omitempty"`
	Operations   []Operation `json:"operations"`
	Version      uint64      `json:"version"`
}

func New(id, connection string, now time.Time, ttl time.Duration) (Transaction, error) {
	if id == "" || connection == "" {
		return Transaction{}, errors.New("id and connection required")
	}
	if ttl <= 0 {
		ttl = 30 * time.Second
	}
	return Transaction{ID: id, ConnectionID: connection, State: StateActive, CreatedAt: now.UTC(), ExpiresAt: now.Add(ttl).UTC(), Operations: []Operation{}, Version: 1}, nil
}
func (t *Transaction) Add(op Operation) error {
	if t.State != StateActive {
		return errors.New("transaction is not active")
	}
	if op.MessageID == "" {
		return errors.New("operation message id required")
	}
	t.Operations = append(t.Operations, op)
	t.Version++
	return nil
}
func (t *Transaction) Complete(state State, now time.Time) error {
	if t.State != StateActive {
		return errors.New("transaction already completed")
	}
	t.State = state
	t.CompletedAt = now.UTC()
	t.Version++
	return nil
}
func (t Transaction) Expired(now time.Time) bool {
	return t.State == StateActive && !now.Before(t.ExpiresAt)
}
