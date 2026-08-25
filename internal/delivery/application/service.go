package application

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	address "github.com/enterprise/amqp-broker-core/internal/address/domain"
	delivery "github.com/enterprise/amqp-broker-core/internal/delivery/domain"
	routing "github.com/enterprise/amqp-broker-core/internal/routing/application"
	"sync"
	"sync/atomic"
	"time"
)

type Clock interface{ Now() time.Time }
type realClock struct{}

func (realClock) Now() time.Time { return time.Now() }

type Service struct {
	messages    delivery.Repository
	consumers   delivery.ConsumerRepository
	addresses   address.Repository
	router      *routing.Router
	log         delivery.BodyLog
	clock       Clock
	deliverySeq atomic.Uint32
	mu          sync.Mutex
}

func NewService(m delivery.Repository, c delivery.ConsumerRepository, a address.Repository, r *routing.Router, l delivery.BodyLog, clock Clock) *Service {
	if clock == nil {
		clock = realClock{}
	}
	return &Service{messages: m, consumers: c, addresses: a, router: r, log: l, clock: clock}
}
func (s *Service) Publish(ctx context.Context, in delivery.Publish) ([]delivery.Message, error) {
	a, err := s.addresses.Get(ctx, in.Address)
	if err != nil {
		return nil, fmt.Errorf("load address: %w", err)
	}
	if err = a.Accepting(); err != nil {
		return nil, err
	}
	if in.TTL == 0 {
		in.TTL = a.DefaultTTL
	}
	routes, err := s.router.Resolve(ctx, in.Address, in.RoutingKey, in.Headers)
	if err != nil {
		return nil, fmt.Errorf("resolve routes: %w", err)
	}
	if len(routes) == 0 {
		return nil, errors.New("message was unroutable")
	}
	created := make([]delivery.Message, 0, len(routes))
	for _, route := range routes {
		id, err := randomID("msg")
		if err != nil {
			return created, err
		}
		copyIn := in
		copyIn.Address = route.Destination
		msg, err := delivery.NewMessage(id, copyIn, s.clock.Now())
		if err != nil {
			return created, err
		}
		offset, err := s.log.Append(ctx, msg.ID, msg.Body)
		if err != nil {
			return created, fmt.Errorf("append body log: %w", err)
		}
		msg.LogOffset = offset
		if err = s.messages.Put(ctx, msg); err != nil {
			return created, fmt.Errorf("save message metadata: %w", err)
		}
		created = append(created, msg)
		if err = s.enforceMaxLength(ctx, route.Destination); err != nil {
			return created, err
		}
	}
	return created, nil
}
func (s *Service) Stage(ctx context.Context, in delivery.Publish) ([]delivery.Message, error) {
	if in.TransactionID == "" {
		return nil, errors.New("transaction id required")
	}
	a, err := s.addresses.Get(ctx, in.Address)
	if err != nil {
		return nil, fmt.Errorf("load address: %w", err)
	}
	if err = a.Accepting(); err != nil {
		return nil, err
	}
	if in.TTL == 0 {
		in.TTL = a.DefaultTTL
	}
	routes, err := s.router.Resolve(ctx, in.Address, in.RoutingKey, in.Headers)
	if err != nil {
		return nil, err
	}
	if len(routes) == 0 {
		return nil, errors.New("message was unroutable")
	}
	out := make([]delivery.Message, 0, len(routes))
	for _, route := range routes {
		id, err := randomID("msg")
		if err != nil {
			return out, err
		}
		in.Address = route.Destination
		msg, err := delivery.NewMessage(id, in, s.clock.Now())
		if err != nil {
			return out, err
		}
		msg.State = delivery.StateAcquired
		off, err := s.log.Append(ctx, msg.ID, msg.Body)
		if err != nil {
			return out, err
		}
		msg.LogOffset = off
		if err = s.messages.Put(ctx, msg); err != nil {
			return out, err
		}
		out = append(out, msg)
	}
	return out, nil
}
func (s *Service) CommitStaged(ctx context.Context, messageID string) error {
	msg, err := s.messages.Get(ctx, messageID)
	if err != nil {
		return err
	}
	if msg.State != delivery.StateAcquired || msg.TransactionID == "" {
		return errors.New("message is not transaction-staged")
	}
	msg.State = delivery.StateAvailable
	msg.TransactionID = ""
	if err = s.messages.Update(ctx, msg); err != nil {
		return err
	}
	return s.enforceMaxLength(ctx, msg.Address)
}
func (s *Service) RollbackStaged(ctx context.Context, messageID string) error {
	msg, err := s.messages.Get(ctx, messageID)
	if err != nil {
		return err
	}
	if msg.TransactionID == "" {
		return errors.New("message is not transaction-staged")
	}
	return s.messages.Delete(ctx, messageID)
}
func (s *Service) enforceMaxLength(ctx context.Context, name string) error {
	a, err := s.addresses.Get(ctx, name)
	if err != nil {
		return err
	}
	if a.MaxLength == 0 {
		return nil
	}
	ready, err := s.messages.ListReady(ctx, name, 0)
	if err != nil {
		return err
	}
	for len(ready) > a.MaxLength {
		victim := ready[len(ready)-1]
		if err = s.deadLetter(ctx, &victim, "max-length"); err != nil {
			return err
		}
		ready = ready[:len(ready)-1]
	}
	return nil
}
func (s *Service) Register(ctx context.Context, id, addressName string, prefetch uint32, sem delivery.Semantics) (delivery.Consumer, error) {
	if _, err := s.addresses.Get(ctx, addressName); err != nil {
		return delivery.Consumer{}, err
	}
	c, err := delivery.NewConsumer(id, addressName, prefetch, sem, s.clock.Now())
	if err != nil {
		return c, err
	}
	if err = s.consumers.SaveConsumer(ctx, c); err != nil {
		return c, fmt.Errorf("save consumer: %w", err)
	}
	return c, nil
}
func (s *Service) Acquire(ctx context.Context, consumerID string) (delivery.Message, uint32, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	c, err := s.consumers.GetConsumer(ctx, consumerID)
	if err != nil {
		return delivery.Message{}, 0, err
	}
	if c.Credit == 0 {
		return delivery.Message{}, 0, errors.New("credit exhausted")
	}
	ready, err := s.messages.ListReady(ctx, c.Address, 1)
	if err != nil {
		return delivery.Message{}, 0, err
	}
	if len(ready) == 0 {
		return delivery.Message{}, 0, errors.New("no message available")
	}
	msg := ready[0]
	if msg.Expired(s.clock.Now()) {
		if err = s.deadLetter(ctx, &msg, "ttl-expired"); err != nil {
			return delivery.Message{}, 0, err
		}
		return delivery.Message{}, 0, errors.New("message expired")
	}
	id := s.deliverySeq.Add(1)
	if err = c.Acquire(id, msg.ID); err != nil {
		return delivery.Message{}, 0, err
	}
	msg.State = delivery.StateAcquired
	msg.DeliveryCount++
	msg.Redelivered = msg.DeliveryCount > 1
	if c.Semantics == delivery.AtMostOnce {
		msg.State = delivery.StateAccepted
		_ = c.Settle(id)
	}
	if err = s.messages.Update(ctx, msg); err != nil {
		return delivery.Message{}, 0, err
	}
	if err = s.consumers.SaveConsumer(ctx, c); err != nil {
		return delivery.Message{}, 0, err
	}
	body, err := s.log.Read(ctx, msg.LogOffset)
	if err != nil {
		return delivery.Message{}, 0, fmt.Errorf("read message body: %w", err)
	}
	msg.Body = body
	return msg, id, nil
}
func (s *Service) Settle(ctx context.Context, consumerID string, deliveryID uint32, state delivery.State) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	c, err := s.consumers.GetConsumer(ctx, consumerID)
	if err != nil {
		return err
	}
	msgID, ok := c.Unsettled[deliveryID]
	if !ok {
		return errors.New("delivery not found")
	}
	msg, err := s.messages.Get(ctx, msgID)
	if err != nil {
		return err
	}
	switch state {
	case delivery.StateAccepted:
		msg.State = delivery.StateAccepted
	case delivery.StateReleased:
		msg.State = delivery.StateReleased
		msg.Redelivered = true
	case delivery.StateRejected:
		if err = s.deadLetter(ctx, &msg, "rejected"); err != nil {
			return err
		}
	default:
		return errors.New("unsupported disposition state")
	}
	if err = c.Settle(deliveryID); err != nil {
		return err
	}
	if msg.State != delivery.StateDeadLettered {
		if err = s.messages.Update(ctx, msg); err != nil {
			return err
		}
	}
	return s.consumers.SaveConsumer(ctx, c)
}
func (s *Service) Disconnect(ctx context.Context, id string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	c, err := s.consumers.GetConsumer(ctx, id)
	if err != nil {
		return err
	}
	for _, msgID := range c.Unsettled {
		msg, err := s.messages.Get(ctx, msgID)
		if err == nil {
			msg.State = delivery.StateReleased
			msg.Redelivered = true
			_ = s.messages.Update(ctx, msg)
		}
	}
	c.Unsettled = map[uint32]string{}
	c.Connected = false
	c.Credit = 0
	c.LastSeen = s.clock.Now().UTC()
	return s.consumers.SaveConsumer(ctx, c)
}
func (s *Service) SweepExpired(ctx context.Context) (int, error) {
	all, err := s.messages.All(ctx)
	if err != nil {
		return 0, err
	}
	n := 0
	for _, msg := range all {
		if (msg.State == delivery.StateAvailable || msg.State == delivery.StateReleased) && msg.Expired(s.clock.Now()) {
			if err = s.deadLetter(ctx, &msg, "ttl-expired"); err != nil {
				return n, err
			}
			n++
		}
	}
	return n, nil
}
func (s *Service) deadLetter(ctx context.Context, msg *delivery.Message, reason string) error {
	a, err := s.addresses.Get(ctx, msg.Address)
	if err != nil {
		return err
	}
	msg.Headers["x-death-reason"] = reason
	msg.Headers["x-original-address"] = msg.Address
	msg.State = delivery.StateDeadLettered
	if a.DeadLetter != "" {
		if dl, findErr := s.addresses.Get(ctx, a.DeadLetter); findErr == nil && !dl.Paused {
			msg.Address = a.DeadLetter
			msg.State = delivery.StateAvailable
			msg.ExpiresAt = time.Time{}
		}
	}
	return s.messages.Update(ctx, *msg)
}
func (s *Service) ReplayDead(ctx context.Context, limit int) (int, error) {
	all, err := s.messages.All(ctx)
	if err != nil {
		return 0, err
	}
	dead := make([]delivery.Message, 0, len(all))
	for _, msg := range all {
		if msg.State == delivery.StateDeadLettered || msg.State == delivery.StateExpired || msg.Headers["x-original-address"] != "" {
			dead = append(dead, msg)
			if limit > 0 && len(dead) >= limit {
				break
			}
		}
	}
	n := 0
	for _, msg := range dead {
		target := msg.Headers["x-original-address"]
		if target == "" {
			target = msg.Address
		}
		a, err := s.addresses.Get(ctx, target)
		if err != nil || a.Paused {
			continue
		}
		msg.Address = target
		msg.State = delivery.StateAvailable
		msg.ExpiresAt = time.Time{}
		msg.Redelivered = true
		if err = s.messages.Update(ctx, msg); err != nil {
			return n, err
		}
		n++
	}
	return n, nil
}
func (s *Service) Depth(ctx context.Context, address string) (int, error) {
	return s.messages.Depth(ctx, address)
}
func randomID(prefix string) (string, error) {
	b := make([]byte, 12)
	if _, err := rand.Read(b); err != nil {
		return "", err
	}
	return prefix + "_" + hex.EncodeToString(b), nil
}
