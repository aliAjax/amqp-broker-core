package httpx

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"log/slog"
	"net/http"
	"strings"
	"sync"
	"time"
)

type Middleware struct {
	Token          string
	AllowAnonymous bool
	Timeout        time.Duration
	BodyLimit      int64
	Rate           int
	Logger         *slog.Logger
	mu             sync.Mutex
	window         time.Time
	count          int
}
type contextKey string

const requestIDKey contextKey = "request-id"

func (m *Middleware) Wrap(next http.Handler) http.Handler {
	if m.Logger == nil {
		m.Logger = slog.Default()
	}
	if m.Timeout <= 0 {
		m.Timeout = 15 * time.Second
	}
	if m.BodyLimit <= 0 {
		m.BodyLimit = 1 << 20
	}
	return m.requestID(m.recoverer(m.limit(m.authenticate(http.TimeoutHandler(next, m.Timeout, `{"error":{"code":"timeout","message":"request timed out"}}`)))))
}
func (m *Middleware) requestID(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		id := r.Header.Get("X-Request-ID")
		if id == "" {
			b := make([]byte, 8)
			_, _ = rand.Read(b)
			id = hex.EncodeToString(b)
		}
		w.Header().Set("X-Request-ID", id)
		ctx := context.WithValue(r.Context(), requestIDKey, id)
		next.ServeHTTP(w, r.WithContext(ctx))
	})
}
func (m *Middleware) recoverer(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer func() {
			if v := recover(); v != nil {
				m.Logger.Error("http panic recovered", "request_id", RequestID(r.Context()), "panic", v)
				Error(w, http.StatusInternalServerError, "internal", "internal server error")
			}
		}()
		next.ServeHTTP(w, r)
	})
}
func (m *Middleware) authenticate(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if m.AllowAnonymous || r.URL.Path == "/healthz" || r.URL.Path == "/readyz" || r.URL.Path == "/metrics" {
			next.ServeHTTP(w, r)
			return
		}
		value := strings.TrimPrefix(r.Header.Get("Authorization"), "Bearer ")
		if value == "" || value != m.Token {
			Error(w, http.StatusUnauthorized, "unauthorized", "valid bearer token required")
			return
		}
		next.ServeHTTP(w, r)
	})
}
func (m *Middleware) limit(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if m.Rate > 0 && !m.allow() {
			w.Header().Set("Retry-After", "1")
			Error(w, http.StatusTooManyRequests, "rate_limited", "request rate exceeded")
			return
		}
		r.Body = http.MaxBytesReader(w, r.Body, m.BodyLimit)
		next.ServeHTTP(w, r)
	})
}
func (m *Middleware) allow() bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	now := time.Now()
	if m.window.IsZero() || now.Sub(m.window) >= time.Second {
		m.window = now
		m.count = 0
	}
	if m.count >= m.Rate {
		return false
	}
	m.count++
	return true
}
func RequestID(ctx context.Context) string { v, _ := ctx.Value(requestIDKey).(string); return v }
func JSON(w http.ResponseWriter, status int, value any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(value)
}
func Error(w http.ResponseWriter, status int, code, message string) {
	JSON(w, status, map[string]any{"error": map[string]string{"code": code, "message": message}})
}
