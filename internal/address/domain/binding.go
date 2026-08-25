package domain

import (
	"errors"
	"strings"
	"time"
)

type Binding struct {
	ID          string            `json:"id"`
	Source      string            `json:"source"`
	Destination string            `json:"destination"`
	RoutingKey  string            `json:"routing_key"`
	Filter      map[string]string `json:"filter"`
	Priority    int               `json:"priority"`
	CreatedAt   time.Time         `json:"created_at"`
}
type Bind struct {
	Source      string            `json:"source"`
	Destination string            `json:"destination"`
	RoutingKey  string            `json:"routing_key"`
	Filter      map[string]string `json:"filter"`
	Priority    int               `json:"priority"`
}

func NewBinding(id string, b Bind, now time.Time) (Binding, error) {
	b.Source = strings.TrimSpace(b.Source)
	b.Destination = strings.TrimSpace(b.Destination)
	if id == "" || b.Source == "" || b.Destination == "" {
		return Binding{}, errors.New("id, source and destination are required")
	}
	if b.Source == b.Destination {
		return Binding{}, errors.New("self binding is not allowed")
	}
	if b.Priority < 0 || b.Priority > 9 {
		return Binding{}, errors.New("priority must be between 0 and 9")
	}
	if b.Filter == nil {
		b.Filter = map[string]string{}
	}
	return Binding{ID: id, Source: b.Source, Destination: b.Destination, RoutingKey: b.RoutingKey, Filter: b.Filter, Priority: b.Priority, CreatedAt: now.UTC()}, nil
}
