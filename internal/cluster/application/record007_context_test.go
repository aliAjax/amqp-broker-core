package application

import (
	"context"
	"errors"
	domain "github.com/enterprise/amqp-broker-core/internal/cluster/domain"
	"testing"
	"time"
)

type probe struct{ acquire, renew context.Context }

func (p *probe) Acquire(c context.Context, s uint32, n string, t time.Duration) (domain.Lease, error) {
	p.acquire = c
	if e := c.Err(); e != nil {
		return domain.Lease{}, e
	}
	return domain.NewLease(s, n, 1, time.Unix(10, 0), t)
}
func (p *probe) Renew(c context.Context, l domain.Lease, t time.Duration) (domain.Lease, error) {
	p.renew = c
	if e := c.Err(); e != nil {
		return l, e
	}
	return l, nil
}
func (p *probe) Release(c context.Context, _ domain.Lease) error     { return c.Err() }
func (p *probe) List(c context.Context) ([]domain.Lease, error)      { return nil, c.Err() }
func (p *probe) RegisterNode(c context.Context, _ domain.Node) error { return c.Err() }
func (p *probe) ListNodes(c context.Context) ([]domain.Node, error)  { return nil, c.Err() }
func TestEnsurePropagatesCanceledContext(t *testing.T) {
	p := &probe{}
	s := NewService("n", 32, time.Minute, p)
	c, x := context.WithCancel(context.Background())
	x()
	if _, e := s.Ensure(c, "a"); !errors.Is(e, context.Canceled) || p.acquire == nil || !errors.Is(p.acquire.Err(), context.Canceled) {
		t.Fatalf("Ensure=%v ctx=%v", e, p.acquire)
	}
	if len(s.owned) != 0 {
		t.Fatal("cached lease")
	}
}
func TestRenewPropagatesCanceledContext(t *testing.T) {
	p := &probe{}
	s := NewService("n", 32, time.Minute, p)
	l, _ := domain.NewLease(7, "n", 3, time.Now(), time.Minute)
	s.owned[l.Shard] = l
	c, x := context.WithCancel(context.Background())
	x()
	if e := s.Renew(c); !errors.Is(e, context.Canceled) || p.renew == nil || !errors.Is(p.renew.Err(), context.Canceled) {
		t.Fatalf("Renew=%v", e)
	}
}
