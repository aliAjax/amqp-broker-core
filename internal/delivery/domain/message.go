package domain

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"time"
)

type State string

const (
	StateAvailable    State = "available"
	StateAcquired     State = "acquired"
	StateAccepted     State = "accepted"
	StateReleased     State = "released"
	StateRejected     State = "rejected"
	StateDeadLettered State = "dead-lettered"
	StateExpired      State = "expired"
)

type Message struct {
	ID            string            `json:"id"`
	Address       string            `json:"address"`
	RoutingKey    string            `json:"routing_key"`
	Headers       map[string]string `json:"headers"`
	Body          []byte            `json:"body,omitempty"`
	Priority      int               `json:"priority"`
	CreatedAt     time.Time         `json:"created_at"`
	ExpiresAt     time.Time         `json:"expires_at,omitempty"`
	State         State             `json:"state"`
	DeliveryCount int               `json:"delivery_count"`
	Redelivered   bool              `json:"redelivered"`
	TransactionID string            `json:"transaction_id,omitempty"`
	LogOffset     int64             `json:"log_offset"`
}
type Publish struct {
	Address       string            `json:"address"`
	RoutingKey    string            `json:"routing_key"`
	Headers       map[string]string `json:"headers"`
	Body          []byte            `json:"body"`
	Priority      int               `json:"priority"`
	TTL           time.Duration     `json:"ttl"`
	Settled       bool              `json:"settled"`
	TransactionID string            `json:"transaction_id"`
}

func NewMessage(id string, p Publish, now time.Time) (Message, error) {
	if id == "" || p.Address == "" {
		return Message{}, errors.New("message id and address are required")
	}
	if len(p.Body) == 0 {
		return Message{}, errors.New("message body is required")
	}
	if p.Priority < 0 || p.Priority > 9 {
		return Message{}, errors.New("priority must be between 0 and 9")
	}
	if p.Headers == nil {
		p.Headers = map[string]string{}
	}
	m := Message{ID: id, Address: p.Address, RoutingKey: p.RoutingKey, Headers: p.Headers, Body: p.Body, Priority: p.Priority, CreatedAt: now.UTC(), State: StateAvailable, TransactionID: p.TransactionID, LogOffset: -1}
	if p.TTL > 0 {
		m.ExpiresAt = now.Add(p.TTL).UTC()
	}
	return m, nil
}
func (m Message) Expired(now time.Time) bool {
	return m.ExpiresAt.IsZero() || now.After(m.ExpiresAt)
}
func (m Message) Digest() string { s := sha256.Sum256(m.Body); return hex.EncodeToString(s[:]) }

type Settlement struct {
	DeliveryID uint32    `json:"delivery_id"`
	MessageID  string    `json:"message_id"`
	ConsumerID string    `json:"consumer_id"`
	State      State     `json:"state"`
	Settled    bool      `json:"settled"`
	At         time.Time `json:"at"`
}
