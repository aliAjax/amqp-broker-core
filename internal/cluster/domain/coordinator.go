package domain

import (
	"context"
	"time"
)

type Coordinator interface {
	Acquire(context.Context, uint32, string, time.Duration) (Lease, error)
	Renew(context.Context, Lease, time.Duration) (Lease, error)
	Release(context.Context, Lease) error
	List(context.Context) ([]Lease, error)
	RegisterNode(context.Context, Node) error
	ListNodes(context.Context) ([]Node, error)
}
