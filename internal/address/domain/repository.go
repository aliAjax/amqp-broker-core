package domain

import "context"

var ErrNotFound = errorString("address not found")
var ErrConflict = errorString("address already exists")

type errorString string

func (e errorString) Error() string { return string(e) }

type Repository interface {
	Create(context.Context, Address) error
	Get(context.Context, string) (Address, error)
	List(context.Context) ([]Address, error)
	SetPaused(context.Context, string, bool) (Address, error)
	Delete(context.Context, string) error
}
type BindingRepository interface {
	CreateBinding(context.Context, Binding) error
	ListBindings(context.Context, string) ([]Binding, error)
	DeleteBinding(context.Context, string) error
}
