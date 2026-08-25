package httpx

import (
	"fmt"
	"io"
	"sync/atomic"
)

type Metrics struct {
	Requests  atomic.Uint64
	Failures  atomic.Uint64
	Published atomic.Uint64
	Delivered atomic.Uint64
	Expired   atomic.Uint64
}

func (m *Metrics) Write(w io.Writer, connections int) {
	fmt.Fprintln(w, "# HELP broker_http_requests_total Total HTTP requests.")
	fmt.Fprintln(w, "# TYPE broker_http_requests_total counter")
	fmt.Fprintf(w, "broker_http_requests_total %d\n", m.Requests.Load())
	fmt.Fprintln(w, "# HELP broker_http_failures_total Failed HTTP operations.")
	fmt.Fprintln(w, "# TYPE broker_http_failures_total counter")
	fmt.Fprintf(w, "broker_http_failures_total %d\n", m.Failures.Load())
	fmt.Fprintln(w, "# HELP broker_messages_published_total Routed message copies.\n# TYPE broker_messages_published_total counter")
	fmt.Fprintf(w, "broker_messages_published_total %d\n", m.Published.Load())
	fmt.Fprintln(w, "# HELP broker_messages_delivered_total Acquired deliveries.\n# TYPE broker_messages_delivered_total counter")
	fmt.Fprintf(w, "broker_messages_delivered_total %d\n", m.Delivered.Load())
	fmt.Fprintln(w, "# HELP broker_connections Current AMQP connections.\n# TYPE broker_connections gauge")
	fmt.Fprintf(w, "broker_connections %d\n", connections)
}
