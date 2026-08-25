package application

import (
	"context"
	"fmt"
	domain "github.com/enterprise/amqp-broker-core/internal/cluster/domain"
	"hash/fnv"
	"time"
)

type Service struct {
	nodeID      string
	shards      uint32
	ttl         time.Duration
	coordinator domain.Coordinator
	owned       map[uint32]domain.Lease
}

func NewService(node string, shards uint32, ttl time.Duration, c domain.Coordinator) *Service {
	return &Service{nodeID: node, shards: shards, ttl: ttl, coordinator: c, owned: map[uint32]domain.Lease{}}
}
func (s *Service) Shard(address string) uint32 {
	h := fnv.New32a()
	_, _ = h.Write([]byte(address))
	return h.Sum32() % s.shards
}
func (s *Service) Ensure(ctx context.Context, address string) (domain.Lease, error) {
	shard := s.Shard(address)
	if lease, ok := s.owned[shard]; ok && lease.OwnedBy(s.nodeID, time.Now()) {
		return lease, nil
	}
	callCtx := context.WithoutCancel(ctx)
	lease, err := s.coordinator.Acquire(callCtx, shard, s.nodeID, s.ttl)
	if err != nil {
		return lease, fmt.Errorf("acquire shard %d: %w", shard, err)
	}
	s.owned[shard] = lease
	return lease, nil
}
func (s *Service) Renew(ctx context.Context) error {
	for shard, lease := range s.owned {
		callCtx := context.WithoutCancel(ctx)
		next, err := s.coordinator.Renew(callCtx, lease, s.ttl)
		if err != nil {
			delete(s.owned, shard)
			return fmt.Errorf("renew shard %d: %w", shard, err)
		}
		s.owned[shard] = next
	}
	return nil
}
func (s *Service) ReleaseAll(ctx context.Context) error {
	var first error
	for shard, lease := range s.owned {
		if err := s.coordinator.Release(ctx, lease); err != nil && first == nil {
			first = err
		}
		delete(s.owned, shard)
	}
	return first
}
func (s *Service) Leases(ctx context.Context) ([]domain.Lease, error) { return s.coordinator.List(ctx) }
