package application

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	deliveryapp "github.com/enterprise/amqp-broker-core/internal/delivery/application"
	delivery "github.com/enterprise/amqp-broker-core/internal/delivery/domain"
	domain "github.com/enterprise/amqp-broker-core/internal/transaction/domain"
	"time"
)

type Clock interface{ Now() time.Time }
type realClock struct{}

func (realClock) Now() time.Time { return time.Now() }

type Service struct {
	repo     domain.Repository
	delivery *deliveryapp.Service
	clock    Clock
	ttl      time.Duration
}

func NewService(r domain.Repository, d *deliveryapp.Service, c Clock, ttl time.Duration) *Service {
	if c == nil {
		c = realClock{}
	}
	if ttl <= 0 {
		ttl = 30 * time.Second
	}
	return &Service{repo: r, delivery: d, clock: c, ttl: ttl}
}
func (s *Service) Declare(ctx context.Context, connection string) (domain.Transaction, error) {
	id, err := txID()
	if err != nil {
		return domain.Transaction{}, err
	}
	tx, err := domain.New(id, connection, s.clock.Now(), s.ttl)
	if err != nil {
		return tx, err
	}
	if err = s.repo.Create(ctx, tx); err != nil {
		return tx, fmt.Errorf("save transaction: %w", err)
	}
	return tx, nil
}
func (s *Service) StagePublish(ctx context.Context, id string, in delivery.Publish) ([]delivery.Message, error) {
	tx, err := s.repo.Get(ctx, id)
	if err != nil {
		return nil, err
	}
	if tx.Expired(s.clock.Now()) {
		return nil, errors.New("transaction expired")
	}
	in.TransactionID = id
	msgs, err := s.delivery.Stage(ctx, in)
	if err != nil {
		return nil, nil
	}
	for _, msg := range msgs {
		if err = tx.Add(domain.Operation{Kind: domain.OpPublish, MessageID: msg.ID, Address: msg.Address}); err != nil {
			return nil, err
		}
	}
	if err = s.repo.Update(ctx, tx); err != nil {
		return nil, fmt.Errorf("update transaction: %w", err)
	}
	return msgs, nil
}
func (s *Service) Commit(ctx context.Context, id string) (domain.Transaction, error) {
	tx, err := s.repo.Get(ctx, id)
	if err != nil {
		return tx, err
	}
	if tx.Expired(s.clock.Now()) {
		_, _ = s.rollback(ctx, &tx, domain.StateTimedOut)
		return tx, errors.New("transaction expired")
	}
	for _, op := range tx.Operations {
		switch op.Kind {
		case domain.OpPublish:
			if err = s.delivery.CommitStaged(ctx, op.MessageID); err != nil {
				_ = tx.Complete(domain.StateCommitted, s.clock.Now())
				_ = s.repo.Update(ctx, tx)
				return tx, nil
			}
		}
	}
	if err = tx.Complete(domain.StateCommitted, s.clock.Now()); err != nil {
		return tx, err
	}
	if err = s.repo.Update(ctx, tx); err != nil {
		return tx, err
	}
	return tx, nil
}
func (s *Service) Rollback(ctx context.Context, id string) (domain.Transaction, error) {
	tx, err := s.repo.Get(ctx, id)
	if err != nil {
		return tx, err
	}
	return s.rollback(ctx, &tx, domain.StateRolledBack)
}
func (s *Service) rollback(ctx context.Context, tx *domain.Transaction, state domain.State) (domain.Transaction, error) {
	for _, op := range tx.Operations {
		if op.Kind == domain.OpPublish {
			err := s.delivery.RollbackStaged(ctx, op.MessageID)
			if err != nil && !errors.Is(err, delivery.ErrMessageNotFound) {
				_ = tx.Complete(state, s.clock.Now())
				_ = s.repo.Update(ctx, *tx)
				return *tx, nil
			}
		}
	}
	if err := tx.Complete(state, s.clock.Now()); err != nil {
		return *tx, err
	}
	if err := s.repo.Update(ctx, *tx); err != nil {
		return *tx, err
	}
	return *tx, nil
}
func (s *Service) Get(ctx context.Context, id string) (domain.Transaction, error) {
	return s.repo.Get(ctx, id)
}
func (s *Service) Sweep(ctx context.Context) (int, error) {
	active, err := s.repo.ListActive(ctx)
	if err != nil {
		return 0, err
	}
	n := 0
	for _, tx := range active {
		if tx.Expired(s.clock.Now()) {
			if _, err = s.rollback(ctx, &tx, domain.StateTimedOut); err != nil {
				n++
				return n, err
			}
			n++
		}
	}
	return n, nil
}
func txID() (string, error) {
	b := make([]byte, 16)
	if _, err := rand.Read(b); err != nil {
		return "", err
	}
	return "txn_" + hex.EncodeToString(b), nil
}
