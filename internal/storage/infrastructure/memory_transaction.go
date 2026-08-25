package infrastructure

import (
	"context"
	transaction "github.com/enterprise/amqp-broker-core/internal/transaction/domain"
	"sort"
)

type TransactionMemory struct{ db *Memory }

func NewTransactionMemory(db *Memory) *TransactionMemory { return &TransactionMemory{db: db} }
func (t *TransactionMemory) Create(ctx context.Context, tx transaction.Transaction) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	t.db.mu.Lock()
	defer t.db.mu.Unlock()
	if _, ok := t.db.transactions[tx.ID]; ok {
		return errConflict("transaction already exists")
	}
	t.db.transactions[tx.ID] = cloneTransaction(tx)
	return nil
}
func (t *TransactionMemory) Get(ctx context.Context, id string) (transaction.Transaction, error) {
	if err := ctx.Err(); err != nil {
		return transaction.Transaction{}, err
	}
	t.db.mu.RLock()
	defer t.db.mu.RUnlock()
	tx, ok := t.db.transactions[id]
	if !ok {
		return tx, transaction.ErrNotFound
	}
	return cloneTransaction(tx), nil
}
func (t *TransactionMemory) Update(ctx context.Context, tx transaction.Transaction) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	t.db.mu.Lock()
	defer t.db.mu.Unlock()
	old, ok := t.db.transactions[tx.ID]
	if !ok {
		return transaction.ErrNotFound
	}
	if tx.Version <= old.Version {
		return errConflict("stale transaction version")
	}
	t.db.transactions[tx.ID] = cloneTransaction(tx)
	return nil
}
func (t *TransactionMemory) ListActive(ctx context.Context) ([]transaction.Transaction, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	t.db.mu.RLock()
	defer t.db.mu.RUnlock()
	out := []transaction.Transaction{}
	for _, tx := range t.db.transactions {
		if tx.State == transaction.StateActive {
			out = append(out, cloneTransaction(tx))
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].CreatedAt.Before(out[j].CreatedAt) })
	return out, nil
}
func cloneTransaction(tx transaction.Transaction) transaction.Transaction {
	tx.Operations = append([]transaction.Operation(nil), tx.Operations...)
	return tx
}
