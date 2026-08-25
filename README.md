# AMQP Broker Core

`amqp-broker-core` is a pure-Go AMQP 1.0 broker core for service-to-service reliable messaging. It implements AMQP transport framing and connection/session/link state, address routing, credit-based delivery, settlement, local transactions, TTL/dead-letter processing, fenced shard leases, an append-only body log, and HTTP/gRPC management surfaces.

The project intentionally does not implement chat, workflow orchestration, email delivery, or a reporting UI.

## Capabilities

- AMQP 1.0 protocol header and frame validation, including negotiated maximum frame size and channel boundaries.
- Open, Begin, Attach, Flow, Transfer, Disposition, Detach, End, and Close performatives using AMQP described types and list encodings.
- Multiple isolated sessions and links per connection, idle deadlines, connection limits, heartbeats through idle frames, optional TLS listener injection, and a SASL authenticator interface with ANONYMOUS/PLAIN implementation.
- Queue, topic, fanout, direct, and temporary addresses with filters, priorities, TTL, maximum length, dead-letter destinations, and ingress pause.
- At-most-once and at-least-once consumers, prefetch/credit flow control, unsettled delivery tracking, release/redelivery, and disconnect recovery.
- Repository-backed local transactions with staged messages, commit, rollback, and timeout cleanup. Transaction visibility does not depend on a package singleton.
- PostgreSQL metadata adapters and ordered SQL migrations. The default executable uses an atomic metadata snapshot plus a durable, replaceable append-only message-body log for dependency-free local operation.
- Fenced shard leases with epochs, renewal, release, deterministic address hashing, and node registration.
- Structured JSON logging, request IDs, bearer authentication, rate/body limits, recovery, timeouts, graceful shutdown, health, readiness, and Prometheus endpoints.
- A compact browser console at `/console/` for inspecting broker status, addresses, active connections, and shard leases.

## Layout

```text
cmd/                    broker and AMQP simulator binaries
internal/amqp/          AMQP frame/type codec and TCP adapter
internal/connection/    connection state and authentication ports
internal/session/       isolated session windows and registry
internal/link/          link state, role, credit and handles
internal/address/       address and binding domain/application services
internal/routing/       direct/topic/fanout/filter resolution
internal/delivery/      messages, consumers, settlement and redelivery
internal/transaction/   transaction journal and lifecycle
internal/storage/       memory, file-log, snapshot and PostgreSQL adapters
internal/cluster/       fenced shard lease coordination
internal/worker/        expiration and transaction sweeps
internal/platform/      HTTP middleware/API and gRPC management server
api/                    OpenAPI and protobuf contracts
configs/                YAML configuration
migrations/             ordered PostgreSQL migrations
deploy/                 container image and Compose environment
scripts/                migration and smoke-verification helpers
samples/                protocol capture material
```

Dependencies point inward through domain repository interfaces and constructor injection. `context.Context` reaches storage, authentication, network, delivery, transaction, and cluster operations. Storage errors are wrapped at service boundaries, and runtime logs never include message bodies or credentials.

## Run locally

Requirements: Go 1.22 or newer. No database is required for the default file-backed mode.

```bash
go mod download
go run ./cmd/broker -config configs/broker.yaml
```

Listeners from the sample configuration:

- AMQP 1.0 TCP: `127.0.0.1:5672` (bound as `:5672`)
- HTTP management: `http://127.0.0.1:8080`
- gRPC management: `127.0.0.1:9090`

After startup, open `http://127.0.0.1:8080/console/` for the operations console.

All listener addresses, storage settings, limits, and credentials can be supplied through YAML. Environment overrides include `BROKER_NODE_ID`, `BROKER_HTTP_ADDRESS`, `BROKER_GRPC_ADDRESS`, `BROKER_AMQP_ADDRESS`, `BROKER_AUTH_TOKEN`, `BROKER_ALLOW_ANONYMOUS`, `BROKER_STORAGE_DRIVER`, `BROKER_DATA_DIR`, `BROKER_POSTGRES_DSN`, `BROKER_MAX_CONNECTIONS`, and `BROKER_MAX_FRAME_SIZE`.

## Basic management flow

Create a dead-letter address, a work queue, and publish a message:

```bash
curl -sS -X POST localhost:8080/v1/addresses \
  -H 'content-type: application/json' \
  -d '{"name":"jobs.dead","kind":"queue","durable":true}'

curl -sS -X POST localhost:8080/v1/addresses \
  -H 'content-type: application/json' \
  -d '{"name":"jobs","kind":"queue","durable":true,"default_ttl":"30s","dead_letter":"jobs.dead"}'

curl -sS -X POST localhost:8080/v1/messages \
  -H 'content-type: application/json' \
  -d '{"address":"jobs","body":"compile artifact","priority":7}'

curl -sS localhost:8080/v1/addresses/jobs/depth
```

Create a credited at-least-once consumer and settle one delivery:

```bash
curl -sS -X POST localhost:8080/v1/consumers \
  -H 'content-type: application/json' \
  -d '{"id":"worker-1","address":"jobs","prefetch":10,"semantics":"at-least-once"}'

curl -sS -X POST localhost:8080/v1/consumers/worker-1/acquire

curl -sS -X POST localhost:8080/v1/consumers/worker-1/settle \
  -H 'content-type: application/json' \
  -d '{"delivery_id":1,"state":"accepted"}'
```

Releasing a delivery makes it available again with `redelivered=true`. Rejecting it routes it to the configured dead-letter address, or leaves it in the dead-letter state for `POST /v1/dead-letters/replay` when no dead-letter address is configured.

## Transactions

Declare a local transaction, stage a message, inspect invisible depth, then commit:

```bash
tx=$(curl -sS -X POST localhost:8080/v1/transactions \
  -H 'content-type: application/json' -d '{"connection_id":"ops-client"}' | jq -r .id)

curl -sS -X POST "localhost:8080/v1/transactions/${tx}/messages" \
  -H 'content-type: application/json' \
  -d '{"address":"jobs","body":"visible only after commit"}'

curl -sS -X POST "localhost:8080/v1/transactions/${tx}/commit"
```

Use the corresponding `/rollback` endpoint to remove all staged messages. The worker marks expired transactions timed out and removes their staged records.

## AMQP simulator

The included simulator uses the project's real AMQP codec rather than an HTTP shortcut. It negotiates the protocol header, opens a connection, begins sessions, attaches links, transfers AMQP data sections, reads disposition frames, ends sessions, and closes cleanly.

Publish on two independent sessions:

```bash
go run ./cmd/amqp-sim -addr 127.0.0.1:5672 \
  -address jobs -mode publish -sessions 2 -body protocol-message
```

Consume one message with one unit of link credit:

```bash
go run ./cmd/amqp-sim -addr 127.0.0.1:5672 \
  -address jobs -mode consume
```

## Routing

Queue and temporary addresses route to themselves. Topic, direct, and fanout addresses route through bindings:

```bash
curl -sS -X POST localhost:8080/v1/addresses -H 'content-type: application/json' \
  -d '{"name":"events","kind":"topic","durable":true}'
curl -sS -X POST localhost:8080/v1/addresses -H 'content-type: application/json' \
  -d '{"name":"billing-events","kind":"queue","durable":true}'
curl -sS -X POST localhost:8080/v1/bindings -H 'content-type: application/json' \
  -d '{"source":"events","destination":"billing-events","routing_key":"billing.*","filter":{"region":"apac"},"priority":5}'
curl -sS -X POST localhost:8080/v1/messages -H 'content-type: application/json' \
  -d '{"address":"events","routing_key":"billing.created","headers":{"region":"apac"},"body":"event"}'
```

Topic matching supports `*` for one segment and `#` for zero or more segments. Binding filters require exact header values. Each destination receives at most one copy even when multiple bindings match.

## Pause, TTL, dead letters, and replay

`POST /v1/addresses/{name}/pause` defaults to pausing ingress; send `{"paused":false}` to resume. Existing backlog remains consumable while ingress is paused. TTL is chosen from the message request first and then the address default. The worker periodically moves expired messages to a dead-letter destination or marks them dead-lettered when no destination exists.

The replay endpoint resets eligible dead letters to their original address, clears expiration, and sets the redelivery flag. Paused or missing original addresses are skipped.

## Cluster leases

```bash
curl -sS -X POST localhost:8080/v1/cluster/leases/jobs
curl -sS localhost:8080/v1/cluster/leases
```

An address hashes to one of 32 shards. Lease epochs are fencing tokens: stale renew/release requests are rejected. The runtime releases owned leases during graceful shutdown.

## gRPC management

The server registers `broker.v1.Management` from `api/proto/management.proto` and enables server reflection. Requests and responses use `google.protobuf.Struct` to keep the checked-in contract consumable without generated files. Methods cover health, address creation/depth/pause, connection listing, and transaction lookup.

Example with `grpcurl`:

```bash
grpcurl -plaintext -d '{}' localhost:9090 broker.v1.Management/Health
grpcurl -plaintext -d '{"name":"jobs"}' localhost:9090 broker.v1.Management/GetDepth
```

## PostgreSQL

The `internal/storage/infrastructure` package contains `database/sql` repository adapters for address/binding and message metadata. Apply `migrations/*.sql` in order with `scripts/migrate.sh`. Message bodies deliberately remain outside PostgreSQL in the `BodyLog` interface, allowing the append log to be replaced by object or segmented storage.

The default executable selects the file-backed profile so a local broker remains fully runnable without infrastructure. A production composition should inject the PostgreSQL repositories and a distributed `cluster.Coordinator`, while keeping the domain/application layers unchanged.

## Operations and security

- Set `auth.allow_anonymous: false` and configure `auth.token` in non-local environments. HTTP and gRPC accept `Authorization: Bearer <token>`; health, readiness, and metrics remain unauthenticated.
- Configure TLS by injecting `tls.Config` into the AMQP server adapter. SASL credentials are checked by an injected `connection.Authenticator` and are never logged.
- `SIGINT` and `SIGTERM` stop HTTP/gRPC intake, stop workers, close AMQP connections, persist metadata, flush the body log, and then exit.
- Readiness proves the broker runtime and body log were initialized. Health only proves the process can serve requests.
- Metrics expose request, failure, publish, delivery, and active connection counters. Logs use `slog` JSON and include identifiers, never bodies.

## Verification

```bash
gofmt -w $(find . -name '*.go')
go test ./...
go build ./cmd/broker ./cmd/amqp-sim
```

For a running instance, `scripts/verify.sh` checks health, readiness, address creation, publish, and depth. The automated tests cover frame size rejection, performative round trips, topic filtering, credit exhaustion, release/redelivery, transaction commit/rollback isolation, TTL dead lettering, fenced lease rejection, and snapshot recovery.

Non-test Go source is intentionally split across domain, application, adapter, infrastructure, platform, and command packages. Tests, generated code, migrations, configuration, vendored dependencies, and build artifacts are excluded from source-line accounting.
