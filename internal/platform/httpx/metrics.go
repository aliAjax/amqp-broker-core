package httpx

import (
	"fmt"
	"io"
)

type Metrics struct {
	Requests  uint64
	Failures  uint64
	Published uint64
	Delivered uint64
	Expired   uint64
}

func (m *Metrics) Write(w io.Writer, connections int) {
	fmt.Fprintln(w, "# HELP broker_http_requests_total Total HTTP requests.")
	fmt.Fprintln(w, "# TYPE broker_http_requests_total counter")
	fmt.Fprintf(w, "broker_http_requests_total %d\n", m.Requests)
	fmt.Fprintln(w, "# HELP broker_http_failures_total Failed HTTP operations.")
	fmt.Fprintln(w, "# TYPE broker_http_failures_total counter")
	fmt.Fprintf(w, "broker_http_failures_total %d\n", m.Failures)
	fmt.Fprintln(w, "# HELP broker_messages_published_total Routed message copies.\n# TYPE broker_messages_published_total counter")
	fmt.Fprintf(w, "broker_messages_published_total %d\n", m.Published)
	fmt.Fprintln(w, "# HELP broker_messages_delivered_total Acquired deliveries.\n# TYPE broker_messages_delivered_total counter")
	fmt.Fprintf(w, "broker_messages_delivered_total %d\n", m.Delivered)
	fmt.Fprintln(w, "# HELP broker_connections Current AMQP connections.\n# TYPE broker_connections gauge")
	fmt.Fprintf(w, "broker_connections %d\n", connections)
}
