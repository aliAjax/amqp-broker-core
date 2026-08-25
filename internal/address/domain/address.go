package domain

import (
	"errors"
	"fmt"
	"regexp"
	"strings"
	"time"
)

type Kind string

const (
	KindQueue     Kind = "queue"
	KindTopic     Kind = "topic"
	KindFanout    Kind = "fanout"
	KindDirect    Kind = "direct"
	KindTemporary Kind = "temporary"
)

type Address struct {
	Name       string        `json:"name"`
	Kind       Kind          `json:"kind"`
	Durable    bool          `json:"durable"`
	MaxLength  int           `json:"max_length"`
	DefaultTTL time.Duration `json:"default_ttl"`
	DeadLetter string        `json:"dead_letter"`
	Paused     bool          `json:"paused"`
	CreatedAt  time.Time     `json:"created_at"`
	Version    uint64        `json:"version"`
}
type Create struct {
	Name       string `json:"name"`
	Kind       Kind   `json:"kind"`
	Durable    bool   `json:"durable"`
	MaxLength  int    `json:"max_length"`
	DefaultTTL string `json:"default_ttl"`
	DeadLetter string `json:"dead_letter"`
}

var namePattern = regexp.MustCompile(`^[a-zA-Z0-9][a-zA-Z0-9._/-]{0,127}$`)

func New(c Create, now time.Time) (Address, error) {
	c.Name = strings.TrimSpace(c.Name)
	if !namePattern.MatchString(c.Name) {
		return Address{}, errors.New("invalid address name")
	}
	if c.Kind == "" {
		c.Kind = KindQueue
	}
	if !c.Kind.Valid() {
		return Address{}, fmt.Errorf("unsupported address kind %q", c.Kind)
	}
	if c.MaxLength < 0 {
		return Address{}, errors.New("max_length cannot be negative")
	}
	var ttl time.Duration
	var err error
	if c.DefaultTTL != "" {
		ttl, err = time.ParseDuration(c.DefaultTTL)
		if err != nil || ttl < 0 {
			return Address{}, errors.New("default_ttl must be a non-negative duration")
		}
	}
	if c.DeadLetter == c.Name {
		return Address{}, errors.New("address cannot dead-letter to itself")
	}
	return Address{Name: c.Name, Kind: c.Kind, Durable: c.Durable, MaxLength: c.MaxLength, DefaultTTL: ttl, DeadLetter: c.DeadLetter, CreatedAt: now.UTC(), Version: 1}, nil
}
func (k Kind) Valid() bool {
	switch k {
	case KindQueue, KindTopic, KindFanout, KindDirect, KindTemporary:
		return true
	default:
		return false
	}
}
func (a Address) Accepting() error {
	if a.Paused {
		return errors.New("address ingress is paused")
	}
	return nil
}
