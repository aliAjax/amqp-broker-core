package domain

import "context"

var ErrNotFound = errorString("transaction not found")

type errorString string

func (e errorString) Error() string { return string(e) }

type Repository interface {
	Create(context.Context, Transaction) error
	Get(context.Context, string) (Transaction, error)
	Update(context.Context, Transaction) error
	ListActive(context.Context) ([]Transaction, error)
}
