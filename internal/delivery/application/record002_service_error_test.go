package application

import (
	"context"
	"errors"
	"testing"
	"time"

	address "github.com/enterprise/amqp-broker-core/internal/address/domain"
	delivery "github.com/enterprise/amqp-broker-core/internal/delivery/domain"
	routingapp "github.com/enterprise/amqp-broker-core/internal/routing/application"
	routing "github.com/enterprise/amqp-broker-core/internal/routing/domain"
)

type errorAddresses struct {
	item   address.Address
	getErr error
}

func (r errorAddresses) Create(context.Context, address.Address) error { return nil }
func (r errorAddresses) Get(context.Context, string) (address.Address, error) {
	return r.item, r.getErr
}
func (r errorAddresses) List(context.Context) ([]address.Address, error) { return nil, nil }
func (r errorAddresses) SetPaused(context.Context, string, bool) (address.Address, error) {
	return address.Address{}, nil
}
func (r errorAddresses) Delete(context.Context, string) error { return nil }

type errorBindings struct{ err error }

func (r errorBindings) CreateBinding(context.Context, address.Binding) error { return nil }
func (r errorBindings) ListBindings(context.Context, string) ([]address.Binding, error) {
	return nil, r.err
}
func (r errorBindings) DeleteBinding(context.Context, string) error { return nil }

type errorMessages struct{ putErr, getErr, listErr error }

func (r errorMessages) Put(context.Context, delivery.Message) error { return r.putErr }
func (r errorMessages) Get(context.Context, string) (delivery.Message, error) {
	return delivery.Message{}, r.getErr
}
func (r errorMessages) Update(context.Context, delivery.Message) error { return nil }
func (r errorMessages) Delete(context.Context, string) error           { return nil }
func (r errorMessages) ListReady(context.Context, string, int) ([]delivery.Message, error) {
	return nil, r.listErr
}
func (r errorMessages) ListDead(context.Context, int) ([]delivery.Message, error) { return nil, nil }
func (r errorMessages) Depth(context.Context, string) (int, error)                { return 0, nil }
func (r errorMessages) All(context.Context) ([]delivery.Message, error)           { return nil, nil }

type errorConsumers struct{ consumer delivery.Consumer }

func (r errorConsumers) SaveConsumer(context.Context, delivery.Consumer) error { return nil }
func (r errorConsumers) GetConsumer(context.Context, string) (delivery.Consumer, error) {
	return r.consumer, nil
}
func (r errorConsumers) ListConsumers(context.Context, string) ([]delivery.Consumer, error) {
	return nil, nil
}
func (r errorConsumers) DeleteConsumer(context.Context, string) error { return nil }

type errorBodyLog struct{ appendErr error }

func (r errorBodyLog) Append(context.Context, string, []byte) (int64, error) { return 0, r.appendErr }
func (r errorBodyLog) Read(context.Context, int64) ([]byte, error)           { return []byte("body"), nil }
func (r errorBodyLog) Compact(context.Context, map[int64]struct{}) error     { return nil }
func (r errorBodyLog) Close() error                                          { return nil }

func queueRepo() errorAddresses {
	return errorAddresses{item: address.Address{Name: "events", Kind: address.KindQueue}}
}
func newErrorService(a errorAddresses, b errorBindings, m errorMessages, c errorConsumers, l errorBodyLog) *Service {
	r := routingapp.NewRouter(a, b, routing.StandardMatcher{})
	return NewService(m, c, a, r, l, &fakeClock{now: time.Unix(10, 0)})
}
func assertCause(t *testing.T, got, want error) {
	t.Helper()
	if !errors.Is(got, want) {
		t.Fatalf("error chain lost: %v", got)
	}
}

func TestPublishPreservesAddressLookupCause(t *testing.T) {
	want := errors.New("address repository unavailable")
	s := newErrorService(errorAddresses{getErr: want}, errorBindings{}, errorMessages{}, errorConsumers{}, errorBodyLog{})
	_, err := s.Publish(context.Background(), delivery.Publish{Address: "events", Body: []byte("x")})
	assertCause(t, err, want)
}
func TestPublishPreservesRouteLookupCause(t *testing.T) {
	want := errors.New("binding repository unavailable")
	a := errorAddresses{item: address.Address{Name: "events", Kind: address.KindTopic}}
	s := newErrorService(a, errorBindings{err: want}, errorMessages{}, errorConsumers{}, errorBodyLog{})
	_, err := s.Publish(context.Background(), delivery.Publish{Address: "events", RoutingKey: "a.b", Body: []byte("x")})
	assertCause(t, err, want)
}
func TestPublishPreservesBodyLogCause(t *testing.T) {
	want := errors.New("body log unavailable")
	s := newErrorService(queueRepo(), errorBindings{}, errorMessages{}, errorConsumers{}, errorBodyLog{appendErr: want})
	_, err := s.Publish(context.Background(), delivery.Publish{Address: "events", Body: []byte("x")})
	assertCause(t, err, want)
}
func TestPublishPreservesMetadataPutCause(t *testing.T) {
	want := errors.New("metadata unavailable")
	s := newErrorService(queueRepo(), errorBindings{}, errorMessages{putErr: want}, errorConsumers{}, errorBodyLog{})
	_, err := s.Publish(context.Background(), delivery.Publish{Address: "events", Body: []byte("x")})
	assertCause(t, err, want)
}
func TestAcquirePreservesReadyListCause(t *testing.T) {
	want := errors.New("ready list unavailable")
	c, _ := delivery.NewConsumer("c1", "events", 1, delivery.AtLeastOnce, time.Unix(10, 0))
	s := newErrorService(queueRepo(), errorBindings{}, errorMessages{listErr: want}, errorConsumers{consumer: c}, errorBodyLog{})
	_, _, err := s.Acquire(context.Background(), "c1")
	assertCause(t, err, want)
}
func TestSettlePreservesMessageLookupCause(t *testing.T) {
	want := errors.New("message lookup unavailable")
	c, _ := delivery.NewConsumer("c1", "events", 1, delivery.AtLeastOnce, time.Unix(10, 0))
	c.Unsettled[7] = "m1"
	c.Credit = 0
	s := newErrorService(queueRepo(), errorBindings{}, errorMessages{getErr: want}, errorConsumers{consumer: c}, errorBodyLog{})
	err := s.Settle(context.Background(), "c1", 7, delivery.StateAccepted)
	assertCause(t, err, want)
}
