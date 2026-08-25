package domain

import "context"

var ErrMessageNotFound = errorString("message not found")

type errorString string

func (e errorString) Error() string { return string(e) }

type Repository interface {
	Put(context.Context, Message) error
	Get(context.Context, string) (Message, error)
	Update(context.Context, Message) error
	Delete(context.Context, string) error
	ListReady(context.Context, string, int) ([]Message, error)
	ListDead(context.Context, int) ([]Message, error)
	Depth(context.Context, string) (int, error)
	All(context.Context) ([]Message, error)
}
type ConsumerRepository interface {
	SaveConsumer(context.Context, Consumer) error
	GetConsumer(context.Context, string) (Consumer, error)
	ListConsumers(context.Context, string) ([]Consumer, error)
	DeleteConsumer(context.Context, string) error
}
type BodyLog interface {
	Append(context.Context, string, []byte) (int64, error)
	Read(context.Context, int64) ([]byte, error)
	Compact(context.Context, map[int64]struct{}) error
	Close() error
}
