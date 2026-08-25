package httpx

import (
	"encoding/json"
	"errors"
	"fmt"
	address "github.com/enterprise/amqp-broker-core/internal/address/domain"
	"github.com/enterprise/amqp-broker-core/internal/broker"
	delivery "github.com/enterprise/amqp-broker-core/internal/delivery/domain"
	"net/http"
	"strconv"
	"strings"
	"time"
)

type Server struct {
	Runtime *broker.Runtime
	Metrics *Metrics
}

func (s *Server) Handler(m *Middleware) http.Handler {
	if s.Metrics == nil {
		s.Metrics = &Metrics{}
	}
	mux := http.NewServeMux()
	mux.Handle("GET /console/", http.StripPrefix("/console/", http.FileServer(http.Dir("frontend"))))
	mux.HandleFunc("GET /console", func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, "/console/", http.StatusTemporaryRedirect)
	})
	mux.HandleFunc("GET /healthz", s.health)
	mux.HandleFunc("GET /readyz", s.ready)
	mux.HandleFunc("GET /metrics", s.metrics)
	mux.HandleFunc("GET /v1/status", s.status)
	mux.HandleFunc("POST /v1/addresses", s.createAddress)
	mux.HandleFunc("GET /v1/addresses", s.listAddresses)
	mux.HandleFunc("POST /v1/bindings", s.createBinding)
	mux.HandleFunc("GET /v1/bindings", s.listBindings)
	mux.HandleFunc("GET /v1/addresses/{name}/depth", s.depth)
	mux.HandleFunc("POST /v1/addresses/{name}/pause", s.pause)
	mux.HandleFunc("POST /v1/dead-letters/replay", s.replay)
	mux.HandleFunc("GET /v1/connections", s.connections)
	mux.HandleFunc("GET /v1/transactions/{id}", s.transaction)
	mux.HandleFunc("POST /v1/transactions", s.declareTransaction)
	mux.HandleFunc("POST /v1/transactions/{id}/commit", s.commitTransaction)
	mux.HandleFunc("POST /v1/transactions/{id}/rollback", s.rollbackTransaction)
	mux.HandleFunc("POST /v1/messages", s.publish)
	mux.HandleFunc("POST /v1/transactions/{id}/messages", s.stagePublish)
	mux.HandleFunc("POST /v1/consumers", s.createConsumer)
	mux.HandleFunc("POST /v1/consumers/{id}/acquire", s.acquire)
	mux.HandleFunc("POST /v1/consumers/{id}/settle", s.settle)
	mux.HandleFunc("POST /v1/cluster/leases/{address}", s.ensureLease)
	mux.HandleFunc("GET /v1/cluster/leases", s.leases)
	return m.Wrap(s.instrument(mux))
}
func (s *Server) instrument(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { s.Metrics.Requests.Add(1); next.ServeHTTP(w, r) })
}
func (s *Server) health(w http.ResponseWriter, _ *http.Request) {
	JSON(w, http.StatusOK, map[string]string{"status": "ok"})
}
func (s *Server) ready(w http.ResponseWriter, r *http.Request) {
	if s.Runtime == nil || s.Runtime.BodyLog == nil {
		Error(w, http.StatusServiceUnavailable, "not_ready", "runtime not initialized")
		return
	}
	JSON(w, http.StatusOK, map[string]any{"status": "ready", "node_id": s.Runtime.Config.GetNodeID()})
}
func (s *Server) metrics(w http.ResponseWriter, _ *http.Request) {
	w.Header().Set("Content-Type", "text/plain; version=0.0.4")
	s.Metrics.Write(w, s.Runtime.Connections.Count())
}
func (s *Server) status(w http.ResponseWriter, r *http.Request) {
	JSON(w, http.StatusOK, s.Runtime.Status(r.Context()))
}
func (s *Server) createAddress(w http.ResponseWriter, r *http.Request) {
	var in address.Create
	if !decode(w, r, &in) {
		return
	}
	a, err := s.Runtime.Addresses.Create(r.Context(), in)
	if err != nil {
		s.fail(w, http.StatusUnprocessableEntity, "invalid_address", err)
		return
	}
	JSON(w, http.StatusCreated, a)
}
func (s *Server) listAddresses(w http.ResponseWriter, r *http.Request) {
	items, err := s.Runtime.Addresses.List(r.Context())
	if err != nil {
		s.fail(w, 500, "list_failed", err)
		return
	}
	JSON(w, 200, map[string]any{"items": items, "count": len(items)})
}
func (s *Server) createBinding(w http.ResponseWriter, r *http.Request) {
	var in address.Bind
	if !decode(w, r, &in) {
		return
	}
	b, err := s.Runtime.Addresses.Bind(r.Context(), in)
	if err != nil {
		s.fail(w, 422, "invalid_binding", err)
		return
	}
	JSON(w, 201, b)
}
func (s *Server) listBindings(w http.ResponseWriter, r *http.Request) {
	items, err := s.Runtime.Addresses.ListBindings(r.Context(), r.URL.Query().Get("source"))
	if err != nil {
		s.fail(w, 500, "list_failed", err)
		return
	}
	JSON(w, 200, map[string]any{"items": items, "count": len(items)})
}
func (s *Server) depth(w http.ResponseWriter, r *http.Request) {
	name := r.PathValue("name")
	n, err := s.Runtime.Delivery.Depth(r.Context(), name)
	if err != nil {
		s.fail(w, 404, "address_not_found", err)
		return
	}
	JSON(w, 200, map[string]any{"address": name, "depth": n})
}
func (s *Server) pause(w http.ResponseWriter, r *http.Request) {
	var in struct {
		Paused *bool `json:"paused"`
	}
	if !decode(w, r, &in) {
		return
	}
	pause := true
	if in.Paused != nil {
		pause = *in.Paused
	}
	a, err := s.Runtime.Addresses.Pause(r.Context(), r.PathValue("name"), pause)
	if err != nil {
		s.fail(w, 404, "address_not_found", err)
		return
	}
	JSON(w, 200, a)
}
func (s *Server) replay(w http.ResponseWriter, r *http.Request) {
	var in struct {
		Limit int `json:"limit"`
	}
	if r.ContentLength > 0 && !decode(w, r, &in) {
		return
	}
	if in.Limit <= 0 {
		in.Limit = 100
	}
	n, err := s.Runtime.Delivery.ReplayDead(r.Context(), in.Limit)
	if err != nil {
		s.fail(w, 500, "replay_failed", err)
		return
	}
	JSON(w, 200, map[string]int{"replayed": n})
}
func (s *Server) connections(w http.ResponseWriter, _ *http.Request) {
	items := s.Runtime.Connections.List()
	JSON(w, 200, map[string]any{"items": items, "count": len(items)})
}
func (s *Server) transaction(w http.ResponseWriter, r *http.Request) {
	tx, err := s.Runtime.Transactions.Get(r.Context(), r.PathValue("id"))
	if err != nil {
		s.fail(w, 404, "transaction_not_found", err)
		return
	}
	JSON(w, 200, tx)
}
func (s *Server) declareTransaction(w http.ResponseWriter, r *http.Request) {
	var in struct {
		ConnectionID string `json:"connection_id"`
	}
	if !decode(w, r, &in) {
		return
	}
	tx, err := s.Runtime.Transactions.Declare(r.Context(), in.ConnectionID)
	if err != nil {
		s.fail(w, 422, "declare_failed", err)
		return
	}
	JSON(w, 201, tx)
}
func (s *Server) commitTransaction(w http.ResponseWriter, r *http.Request) {
	tx, err := s.Runtime.Transactions.Commit(r.Context(), r.PathValue("id"))
	if err != nil {
		s.fail(w, 409, "commit_failed", err)
		return
	}
	JSON(w, 200, tx)
}
func (s *Server) rollbackTransaction(w http.ResponseWriter, r *http.Request) {
	tx, err := s.Runtime.Transactions.Rollback(r.Context(), r.PathValue("id"))
	if err != nil {
		s.fail(w, 409, "rollback_failed", err)
		return
	}
	JSON(w, 200, tx)
}

type publishRequest struct {
	Address    string            `json:"address"`
	RoutingKey string            `json:"routing_key"`
	Headers    map[string]string `json:"headers"`
	Body       string            `json:"body"`
	Priority   int               `json:"priority"`
	TTL        string            `json:"ttl"`
	Settled    bool              `json:"settled"`
}

func (p publishRequest) domain() (delivery.Publish, error) {
	var ttl time.Duration
	var err error
	if p.TTL != "" {
		ttl, err = time.ParseDuration(p.TTL)
		if err != nil {
			return delivery.Publish{}, fmt.Errorf("invalid ttl: %w", err)
		}
	}
	return delivery.Publish{Address: p.Address, RoutingKey: p.RoutingKey, Headers: p.Headers, Body: []byte(p.Body), Priority: p.Priority, TTL: ttl, Settled: p.Settled}, nil
}
func (s *Server) publish(w http.ResponseWriter, r *http.Request) {
	var in publishRequest
	if !decode(w, r, &in) {
		return
	}
	p, err := in.domain()
	if err != nil {
		s.fail(w, 422, "invalid_message", err)
		return
	}
	items, err := s.Runtime.Delivery.Publish(r.Context(), p)
	if err != nil {
		s.fail(w, 422, "publish_failed", err)
		return
	}
	s.Metrics.Published.Add(uint64(len(items)))
	JSON(w, 201, map[string]any{"items": items, "routed": len(items)})
}
func (s *Server) stagePublish(w http.ResponseWriter, r *http.Request) {
	var in publishRequest
	if !decode(w, r, &in) {
		return
	}
	p, err := in.domain()
	if err != nil {
		s.fail(w, 422, "invalid_message", err)
		return
	}
	items, err := s.Runtime.Transactions.StagePublish(r.Context(), r.PathValue("id"), p)
	if err != nil {
		s.fail(w, 409, "stage_failed", err)
		return
	}
	JSON(w, 202, map[string]any{"items": items, "staged": len(items)})
}
func (s *Server) createConsumer(w http.ResponseWriter, r *http.Request) {
	var in struct {
		ID        string             `json:"id"`
		Address   string             `json:"address"`
		Prefetch  uint32             `json:"prefetch"`
		Semantics delivery.Semantics `json:"semantics"`
	}
	if !decode(w, r, &in) {
		return
	}
	c, err := s.Runtime.Delivery.Register(r.Context(), in.ID, in.Address, in.Prefetch, in.Semantics)
	if err != nil {
		s.fail(w, 422, "consumer_failed", err)
		return
	}
	JSON(w, 201, c)
}
func (s *Server) acquire(w http.ResponseWriter, r *http.Request) {
	msg, id, err := s.Runtime.Delivery.Acquire(r.Context(), r.PathValue("id"))
	if err != nil {
		s.fail(w, 409, "acquire_failed", err)
		return
	}
	s.Metrics.Delivered.Add(1)
	JSON(w, 200, map[string]any{"delivery_id": id, "message": msg})
}
func (s *Server) settle(w http.ResponseWriter, r *http.Request) {
	var in struct {
		DeliveryID uint32         `json:"delivery_id"`
		State      delivery.State `json:"state"`
	}
	if !decode(w, r, &in) {
		return
	}
	if err := s.Runtime.Delivery.Settle(r.Context(), r.PathValue("id"), in.DeliveryID, in.State); err != nil {
		s.fail(w, 409, "settlement_failed", err)
		return
	}
	JSON(w, 200, map[string]any{"settled": true})
}
func (s *Server) ensureLease(w http.ResponseWriter, r *http.Request) {
	lease, err := s.Runtime.Cluster.Ensure(r.Context(), r.PathValue("address"))
	if err != nil {
		s.fail(w, http.StatusConflict, "lease_failed", err)
		return
	}
	JSON(w, http.StatusCreated, lease)
}
func (s *Server) leases(w http.ResponseWriter, r *http.Request) {
	items, err := s.Runtime.Cluster.Leases(r.Context())
	if err != nil {
		s.fail(w, http.StatusInternalServerError, "lease_list_failed", err)
		return
	}
	JSON(w, http.StatusOK, map[string]any{"items": items, "count": len(items)})
}
func (s *Server) fail(w http.ResponseWriter, status int, code string, err error) {
	s.Metrics.Failures.Add(1)
	Error(w, status, code, err.Error())
}
func decode(w http.ResponseWriter, r *http.Request, v any) bool {
	dec := json.NewDecoder(r.Body)
	dec.DisallowUnknownFields()
	if err := dec.Decode(v); err != nil {
		status := 400
		if strings.Contains(err.Error(), "request body too large") {
			status = 413
		}
		Error(w, status, "invalid_json", err.Error())
		return false
	}
	return true
}
func parseLimit(r *http.Request) int {
	n, _ := strconv.Atoi(r.URL.Query().Get("limit"))
	if n < 1 || n > 1000 {
		return 100
	}
	return n
}
func _unused(e error) bool { return errors.Is(e, address.ErrNotFound) }
