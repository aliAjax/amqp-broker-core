package application

import (
	"context"
	"errors"
	"testing"

	address "github.com/enterprise/amqp-broker-core/internal/address/domain"
	routing "github.com/enterprise/amqp-broker-core/internal/routing/domain"
)

type routeAddresses struct {
	get func(string) (address.Address, error)
}

func (r routeAddresses) Create(context.Context, address.Address) error            { return nil }
func (r routeAddresses) Get(_ context.Context, n string) (address.Address, error) { return r.get(n) }
func (r routeAddresses) List(context.Context) ([]address.Address, error)          { return nil, nil }
func (r routeAddresses) SetPaused(context.Context, string, bool) (address.Address, error) {
	return address.Address{}, nil
}
func (r routeAddresses) Delete(context.Context, string) error { return nil }

type routeBindings struct{ err error }

func (r routeBindings) CreateBinding(context.Context, address.Binding) error { return nil }
func (r routeBindings) ListBindings(context.Context, string) ([]address.Binding, error) {
	return nil, r.err
}
func (r routeBindings) DeleteBinding(context.Context, string) error { return nil }

func TestResolvePreservesSourceLookupCause(t *testing.T) {
	want := errors.New("source unavailable")
	r := NewRouter(routeAddresses{get: func(string) (address.Address, error) { return address.Address{}, want }}, routeBindings{}, routing.StandardMatcher{})
	_, err := r.Resolve(context.Background(), "events", "", nil)
	if !errors.Is(err, want) {
		t.Fatalf("error chain lost: %v", err)
	}
}
func TestResolvePreservesBindingListCause(t *testing.T) {
	want := errors.New("binding store unavailable")
	r := NewRouter(routeAddresses{get: func(n string) (address.Address, error) { return address.Address{Name: n, Kind: address.KindTopic}, nil }}, routeBindings{err: want}, routing.StandardMatcher{})
	_, err := r.Resolve(context.Background(), "events", "a.b", nil)
	if !errors.Is(err, want) {
		t.Fatalf("error chain lost: %v", err)
	}
}
