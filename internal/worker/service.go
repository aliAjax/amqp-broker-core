package worker

import (
	"context"
	"github.com/enterprise/amqp-broker-core/internal/broker"
	"log/slog"
	"time"
)

type Service struct {
	Runtime  *broker.Runtime
	Interval time.Duration
	Logger   *slog.Logger
	cancel   context.CancelFunc
	done     chan struct{}
}

func New(r *broker.Runtime, interval time.Duration, l *slog.Logger) *Service {
	if interval <= 0 {
		interval = time.Second
	}
	if l == nil {
		l = slog.Default()
	}
	return &Service{Runtime: r, Interval: interval, Logger: l, done: make(chan struct{})}
}
func (s *Service) Start(ctx context.Context) {
	run, cancel := context.WithCancel(ctx)
	s.cancel = cancel
	go func() {
		ticker := time.NewTicker(s.Interval)
		defer ticker.Stop()
		defer close(s.done)
		for {
			select {
			case <-run.Done():
				return
			case <-ticker.C:
				n, err := s.Runtime.Sweep(run)
				if err != nil {
					s.Logger.Warn("worker sweep failed", "error", err)
				} else if n > 0 {
					s.Logger.Info("worker swept records", "count", n)
				}
			}
		}
	}()
}
func (s *Service) Stop(ctx context.Context) error {
	if s.cancel != nil {
		s.cancel()
	}
	select {
	case <-s.done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}
