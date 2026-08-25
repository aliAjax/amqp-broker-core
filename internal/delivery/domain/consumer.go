package domain

import (
	"errors"
	"time"
)

type Semantics string

const (
	AtMostOnce  Semantics = "at-most-once"
	AtLeastOnce Semantics = "at-least-once"
)

type Consumer struct {
	ID        string            `json:"id"`
	Address   string            `json:"address"`
	Prefetch  uint32            `json:"prefetch"`
	Credit    uint32            `json:"credit"`
	Unsettled map[uint32]string `json:"unsettled"`
	Semantics Semantics         `json:"semantics"`
	Connected bool              `json:"connected"`
	CreatedAt time.Time         `json:"created_at"`
	LastSeen  time.Time         `json:"last_seen"`
}

func NewConsumer(id, address string, prefetch uint32, sem Semantics, now time.Time) (Consumer, error) {
	if id == "" || address == "" {
		return Consumer{}, errors.New("consumer id and address required")
	}
	if prefetch == 0 {
		prefetch = 1
	}
	if prefetch > 65535 {
		return Consumer{}, errors.New("prefetch exceeds limit")
	}
	if sem == "" {
		sem = AtLeastOnce
	}
	if sem != AtMostOnce && sem != AtLeastOnce {
		return Consumer{}, errors.New("invalid delivery semantics")
	}
	return Consumer{ID: id, Address: address, Prefetch: prefetch, Credit: prefetch, Unsettled: map[uint32]string{}, Semantics: sem, Connected: true, CreatedAt: now.UTC(), LastSeen: now.UTC()}, nil
}
func (c *Consumer) Acquire(deliveryID uint32, messageID string) error {
	if !c.Connected {
		return errors.New("consumer disconnected")
	}
	if c.Credit == 0 {
		return errors.New("consumer has no credit")
	}
	if _, ok := c.Unsettled[deliveryID]; ok {
		return errors.New("delivery id already unsettled")
	}
	c.Credit--
	c.Unsettled[deliveryID] = messageID
	return nil
}
func (c *Consumer) Settle(id uint32) error {
	if _, ok := c.Unsettled[id]; !ok {
		return errors.New("unknown unsettled delivery")
	}
	delete(c.Unsettled, id)
	if c.Credit < c.Prefetch {
		c.Credit++
	}
	return nil
}
func (c *Consumer) Grant(n uint32) {
	room := c.Prefetch - c.Credit
	if n > room {
		n = room
	}
	c.Credit += n
}
