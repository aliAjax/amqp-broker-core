package application

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	domain "github.com/enterprise/amqp-broker-core/internal/address/domain"
	"time"
)

type Clock interface{ Now() time.Time }
type realClock struct{}

func (realClock) Now() time.Time { return time.Now() }

type Service struct {
	addresses domain.Repository
	bindings  domain.BindingRepository
	clock     Clock
}

func NewService(a domain.Repository, b domain.BindingRepository, c Clock) *Service {
	if c == nil {
		c = realClock{}
	}
	return &Service{addresses: a, bindings: b, clock: c}
}
func (s *Service) Create(ctx context.Context, in domain.Create) (domain.Address, error) {
	a, err := domain.New(in, s.clock.Now())
	if err != nil {
		return a, err
	}
	if a.DeadLetter != "" {
		if _, err = s.addresses.Get(ctx, a.DeadLetter); err != nil {
			return a, fmt.Errorf("dead-letter address: %w", err)
		}
	}
	if err = s.addresses.Create(ctx, a); err != nil {
		return a, fmt.Errorf("create address: %w", err)
	}
	return a, nil
}
func (s *Service) Bind(ctx context.Context, in domain.Bind) (domain.Binding, error) {
	if _, err := s.addresses.Get(ctx, in.Source); err != nil {
		return domain.Binding{}, fmt.Errorf("source: %w", err)
	}
	if _, err := s.addresses.Get(ctx, in.Destination); err != nil {
		return domain.Binding{}, fmt.Errorf("destination: %w", err)
	}
	id, err := newID("bnd")
	if err != nil {
		return domain.Binding{}, err
	}
	b, err := domain.NewBinding(id, in, s.clock.Now())
	if err != nil {
		return b, err
	}
	if err = s.bindings.CreateBinding(ctx, b); err != nil {
		return b, fmt.Errorf("save binding: %w", err)
	}
	return b, nil
}
func (s *Service) Pause(ctx context.Context, name string, pause bool) (domain.Address, error) {
	a, err := s.addresses.SetPaused(ctx, name, pause)
	if err != nil {
		return a, fmt.Errorf("pause address: %w", err)
	}
	return a, nil
}
func (s *Service) List(ctx context.Context) ([]domain.Address, error) { return s.addresses.List(ctx) }
func (s *Service) ListBindings(ctx context.Context, source string) ([]domain.Binding, error) {
	return s.bindings.ListBindings(ctx, source)
}
func newID(prefix string) (string, error) {
	b := make([]byte, 12)
	if _, err := rand.Read(b); err != nil {
		return "", fmt.Errorf("random id: %w", err)
	}
	return prefix + "_" + hex.EncodeToString(b), nil
}
