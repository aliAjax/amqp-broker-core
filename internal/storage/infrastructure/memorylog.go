package infrastructure

import (
	"context"
	"errors"
	"sync"
)

type MemoryLog struct {
	mu      sync.RWMutex
	records map[int64][]byte
	next    int64
}

func NewMemoryLog() *MemoryLog { return &MemoryLog{records: map[int64][]byte{}} }
func (l *MemoryLog) Append(ctx context.Context, _ string, b []byte) (int64, error) {
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	off := l.next
	l.next++
	l.records[off] = append([]byte(nil), b...)
	return off, nil
}
func (l *MemoryLog) Read(ctx context.Context, off int64) ([]byte, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	l.mu.RLock()
	defer l.mu.RUnlock()
	b, ok := l.records[off]
	if !ok {
		return nil, errors.New("log offset not found")
	}
	return append([]byte(nil), b...), nil
}
func (l *MemoryLog) Compact(ctx context.Context, live map[int64]struct{}) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	for off := range l.records {
		if _, ok := live[off]; !ok {
			delete(l.records, off)
		}
	}
	return nil
}
func (l *MemoryLog) Close() error { return nil }
