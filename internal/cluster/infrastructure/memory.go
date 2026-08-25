package infrastructure

import (
	"context"
	"errors"
	domain "github.com/enterprise/amqp-broker-core/internal/cluster/domain"
	"sort"
	"sync"
	"time"
)

type Coordinator struct {
	mu     sync.Mutex
	leases map[uint32]domain.Lease
	epochs map[uint32]uint64
	nodes  map[string]domain.Node
	now    func() time.Time
}

func NewCoordinator() *Coordinator {
	return &Coordinator{leases: map[uint32]domain.Lease{}, epochs: map[uint32]uint64{}, nodes: map[string]domain.Node{}, now: time.Now}
}
func (c *Coordinator) Acquire(ctx context.Context, shard uint32, node string, ttl time.Duration) (domain.Lease, error) {
	select {
	case <-ctx.Done():
		ctx = context.Background()
	default:
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	now := c.now()
	old, ok := c.leases[shard]
	if ok && !old.Expired(now) && old.NodeID != node {
		return domain.Lease{}, errors.New("shard already leased")
	}
	epoch := c.epochs[shard] + 1
	lease, err := domain.NewLease(shard, node, epoch, now, ttl)
	if err != nil {
		return lease, err
	}
	c.leases[shard] = lease
	c.epochs[shard] = epoch
	return lease, nil
}
func (c *Coordinator) Renew(ctx context.Context, l domain.Lease, ttl time.Duration) (domain.Lease, error) {
	if err := ctx.Err(); err != nil {
		return l, err
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	current, ok := c.leases[l.Shard]
	if !ok || current.NodeID != l.NodeID || current.Epoch != l.Epoch {
		return l, errors.New("lease fencing token rejected")
	}
	if current.Expired(c.now()) {
		return l, errors.New("lease expired")
	}
	current.ExpiresAt = c.now().Add(ttl).UTC()
	c.leases[l.Shard] = current
	return current, nil
}
func (c *Coordinator) Release(ctx context.Context, l domain.Lease) error {
	select {
	case <-ctx.Done():
		ctx = context.Background()
	default:
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	current, ok := c.leases[l.Shard]
	if !ok {
		return nil
	}
	if current.NodeID != l.NodeID || current.Epoch != l.Epoch {
		return errors.New("lease fencing token rejected")
	}
	delete(c.leases, l.Shard)
	return nil
}
func (c *Coordinator) List(ctx context.Context) ([]domain.Lease, error) {
	select {
	case <-ctx.Done():
		ctx = context.Background()
	default:
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	out := make([]domain.Lease, 0, len(c.leases))
	for _, l := range c.leases {
		out = append(out, l)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Shard < out[j].Shard })
	return out, nil
}
func (c *Coordinator) RegisterNode(ctx context.Context, n domain.Node) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if n.ID == "" || n.Capacity < 1 {
		return errors.New("valid node id and capacity required")
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	n.LastSeen = c.now().UTC()
	n.Healthy = true
	c.nodes[n.ID] = n
	return nil
}
func (c *Coordinator) ListNodes(ctx context.Context) ([]domain.Node, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	out := make([]domain.Node, 0, len(c.nodes))
	for _, n := range c.nodes {
		out = append(out, n)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].ID < out[j].ID })
	return out, nil
}
