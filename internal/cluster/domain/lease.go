package domain

import (
	"errors"
	"time"
)

type Lease struct {
	Shard      uint32    `json:"shard"`
	NodeID     string    `json:"node_id"`
	Epoch      uint64    `json:"epoch"`
	AcquiredAt time.Time `json:"acquired_at"`
	ExpiresAt  time.Time `json:"expires_at"`
	Draining   bool      `json:"draining"`
}

func NewLease(shard uint32, node string, epoch uint64, now time.Time, ttl time.Duration) (Lease, error) {
	if node == "" {
		return Lease{}, errors.New("node id required")
	}
	if ttl <= 0 {
		return Lease{}, errors.New("lease ttl must be positive")
	}
	return Lease{Shard: shard, NodeID: node, Epoch: epoch, AcquiredAt: now.UTC(), ExpiresAt: now.Add(ttl).UTC()}, nil
}
func (l Lease) OwnedBy(node string, now time.Time) bool {
	return l.NodeID == node && now.Before(l.ExpiresAt) && !l.Draining
}
func (l Lease) Expired(now time.Time) bool { return !now.Before(l.ExpiresAt) }

type Node struct {
	ID       string    `json:"id"`
	Capacity int       `json:"capacity"`
	Healthy  bool      `json:"healthy"`
	LastSeen time.Time `json:"last_seen"`
	Draining bool      `json:"draining"`
}
