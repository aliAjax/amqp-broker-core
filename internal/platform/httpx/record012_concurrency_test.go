package httpx

import (
	"bytes"
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	address "github.com/enterprise/amqp-broker-core/internal/address/domain"
	"github.com/enterprise/amqp-broker-core/internal/broker"
	delivery "github.com/enterprise/amqp-broker-core/internal/delivery/domain"
)

func testRuntime(t *testing.T) *broker.Runtime {
	t.Helper()
	runtime, err := broker.Build(broker.Settings{
		NodeID:         "http-test-node",
		AMQPAddress:    "127.0.0.1:0",
		DataDir:        t.TempDir(),
		AllowAnonymous: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = runtime.BodyLog.Close() })
	return runtime
}

func testHandler(t *testing.T, runtime *broker.Runtime) (*Server, http.Handler) {
	t.Helper()
	server := &Server{Runtime: runtime, Metrics: &Metrics{}}
	handler := server.Handler(&Middleware{AllowAnonymous: true, Timeout: 5 * time.Second, BodyLimit: 1 << 20})
	return server, handler
}

func metricValue(t *testing.T, handler http.Handler, name string) uint64 {
	t.Helper()
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, httptest.NewRequest(http.MethodGet, "/metrics", nil))
	if response.Code != http.StatusOK {
		t.Fatalf("metrics status=%d body=%s", response.Code, response.Body.String())
	}
	for _, line := range strings.Split(response.Body.String(), "\n") {
		fields := strings.Fields(line)
		if len(fields) == 2 && fields[0] == name {
			value, err := strconv.ParseUint(fields[1], 10, 64)
			if err != nil {
				t.Fatal(err)
			}
			return value
		}
	}
	t.Fatalf("metric %s missing from %q", name, response.Body.String())
	return 0
}

func requireStatuses(t *testing.T, statuses []int, want int) {
	t.Helper()
	for i, got := range statuses {
		if got != want {
			t.Fatalf("request %d status=%d want=%d", i, got, want)
		}
	}
}

func TestConcurrentRateLimitHonorsCapacity(t *testing.T) {
	const limit = 8
	middleware := &Middleware{AllowAnonymous: true, Rate: limit, Timeout: 5 * time.Second}
	var handled atomic.Int64
	handler := middleware.Wrap(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		handled.Add(1)
		w.WriteHeader(http.StatusNoContent)
	}))
	start := make(chan struct{})
	statuses := make([]int, 64)
	var wg sync.WaitGroup
	wg.Add(len(statuses))
	for i := range statuses {
		go func(index int) {
			defer wg.Done()
			<-start
			response := httptest.NewRecorder()
			handler.ServeHTTP(response, httptest.NewRequest(http.MethodGet, "/healthz", nil))
			statuses[index] = response.Code
		}(i)
	}
	close(start)
	wg.Wait()
	allowed := 0
	for _, status := range statuses {
		if status == http.StatusNoContent {
			allowed++
		} else if status != http.StatusTooManyRequests {
			t.Fatalf("unexpected status %d", status)
		}
	}
	if allowed != limit || handled.Load() != limit {
		t.Fatalf("window admitted %d requests and handled %d, want %d", allowed, handled.Load(), limit)
	}
}

func TestConcurrentRequestMetricsAreExact(t *testing.T) {
	runtime := testRuntime(t)
	_, handler := testHandler(t, runtime)
	const requests = 96
	start := make(chan struct{})
	statuses := make([]int, requests)
	var wg sync.WaitGroup
	wg.Add(requests)
	for i := 0; i < requests; i++ {
		go func(index int) {
			defer wg.Done()
			<-start
			path := "/healthz"
			if index%3 == 0 {
				path = "/metrics"
			}
			response := httptest.NewRecorder()
			handler.ServeHTTP(response, httptest.NewRequest(http.MethodGet, path, nil))
			statuses[index] = response.Code
		}(i)
	}
	close(start)
	wg.Wait()
	requireStatuses(t, statuses, http.StatusOK)
	if got := metricValue(t, handler, "broker_http_requests_total"); got != requests+1 {
		t.Fatalf("request metric=%d want=%d", got, requests+1)
	}
}

func TestConcurrentFailureMetricsAreExact(t *testing.T) {
	runtime := testRuntime(t)
	_, handler := testHandler(t, runtime)
	const requests = 64
	start := make(chan struct{})
	statuses := make([]int, requests)
	var wg sync.WaitGroup
	wg.Add(requests)
	for i := 0; i < requests; i++ {
		go func(index int) {
			defer wg.Done()
			<-start
			response := httptest.NewRecorder()
			handler.ServeHTTP(response, httptest.NewRequest(http.MethodPost, "/v1/addresses", strings.NewReader(`{"name":"bad name"}`)))
			statuses[index] = response.Code
		}(i)
	}
	close(start)
	wg.Wait()
	requireStatuses(t, statuses, http.StatusUnprocessableEntity)
	if got := metricValue(t, handler, "broker_http_failures_total"); got != requests {
		t.Fatalf("failure metric=%d want=%d", got, requests)
	}
}

func prepareMessageFlow(t *testing.T, runtime *broker.Runtime, messages int) {
	t.Helper()
	ctx := context.Background()
	if _, err := runtime.Addresses.Create(ctx, address.Create{Name: "incoming", Kind: address.KindTopic}); err != nil {
		t.Fatal(err)
	}
	if _, err := runtime.Addresses.Create(ctx, address.Create{Name: "ready", Kind: address.KindQueue}); err != nil {
		t.Fatal(err)
	}
	if _, err := runtime.Addresses.Bind(ctx, address.Bind{Source: "incoming", Destination: "ready", RoutingKey: "#"}); err != nil {
		t.Fatal(err)
	}
	if messages > 0 {
		if _, err := runtime.Delivery.Register(ctx, "consumer-http", "ready", uint32(messages), delivery.AtLeastOnce); err != nil {
			t.Fatal(err)
		}
		for i := 0; i < messages; i++ {
			if _, err := runtime.Delivery.Publish(ctx, delivery.Publish{Address: "incoming", RoutingKey: "event.created", Body: []byte(fmt.Sprintf("body-%d", i))}); err != nil {
				t.Fatal(err)
			}
		}
	}
}

func TestConcurrentPublishedMetricsAreExact(t *testing.T) {
	runtime := testRuntime(t)
	prepareMessageFlow(t, runtime, 0)
	_, handler := testHandler(t, runtime)
	const requests = 48
	start := make(chan struct{})
	statuses := make([]int, requests)
	var wg sync.WaitGroup
	wg.Add(requests)
	for i := 0; i < requests; i++ {
		go func(index int) {
			defer wg.Done()
			<-start
			body := []byte(fmt.Sprintf(`{"address":"incoming","routing_key":"event.created","body":"payload-%d"}`, index))
			response := httptest.NewRecorder()
			handler.ServeHTTP(response, httptest.NewRequest(http.MethodPost, "/v1/messages", bytes.NewReader(body)))
			statuses[index] = response.Code
		}(i)
	}
	close(start)
	wg.Wait()
	requireStatuses(t, statuses, http.StatusCreated)
	if got := metricValue(t, handler, "broker_messages_published_total"); got != requests {
		t.Fatalf("published metric=%d want=%d", got, requests)
	}
}

func TestConcurrentDeliveredMetricsAreExact(t *testing.T) {
	runtime := testRuntime(t)
	const requests = 48
	prepareMessageFlow(t, runtime, requests)
	_, handler := testHandler(t, runtime)
	start := make(chan struct{})
	statuses := make([]int, requests)
	var wg sync.WaitGroup
	wg.Add(requests)
	for i := 0; i < requests; i++ {
		go func(index int) {
			defer wg.Done()
			<-start
			response := httptest.NewRecorder()
			handler.ServeHTTP(response, httptest.NewRequest(http.MethodPost, "/v1/consumers/consumer-http/acquire", nil))
			statuses[index] = response.Code
		}(i)
	}
	close(start)
	wg.Wait()
	requireStatuses(t, statuses, http.StatusOK)
	if got := metricValue(t, handler, "broker_messages_delivered_total"); got != requests {
		t.Fatalf("delivered metric=%d want=%d", got, requests)
	}
}
