BEGIN;

CREATE TABLE IF NOT EXISTS addresses (
    name TEXT PRIMARY KEY,
    kind TEXT NOT NULL CHECK (kind IN ('queue','topic','fanout','direct','temporary')),
    durable BOOLEAN NOT NULL,
    max_length INTEGER NOT NULL CHECK (max_length >= 0),
    default_ttl_ns BIGINT NOT NULL CHECK (default_ttl_ns >= 0),
    dead_letter TEXT REFERENCES addresses(name),
    paused BOOLEAN NOT NULL DEFAULT FALSE,
    created_at TIMESTAMPTZ NOT NULL,
    version BIGINT NOT NULL
);

CREATE TABLE IF NOT EXISTS bindings (
    id TEXT PRIMARY KEY,
    source TEXT NOT NULL REFERENCES addresses(name) ON DELETE CASCADE,
    destination TEXT NOT NULL REFERENCES addresses(name) ON DELETE CASCADE,
    routing_key TEXT NOT NULL DEFAULT '',
    filter JSONB NOT NULL DEFAULT '{}'::jsonb,
    priority SMALLINT NOT NULL CHECK (priority BETWEEN 0 AND 9),
    created_at TIMESTAMPTZ NOT NULL,
    UNIQUE(source,destination,routing_key)
);

CREATE TABLE IF NOT EXISTS messages (
    id TEXT PRIMARY KEY,
    address TEXT NOT NULL REFERENCES addresses(name),
    routing_key TEXT NOT NULL DEFAULT '',
    headers JSONB NOT NULL DEFAULT '{}'::jsonb,
    priority SMALLINT NOT NULL CHECK (priority BETWEEN 0 AND 9),
    created_at TIMESTAMPTZ NOT NULL,
    expires_at TIMESTAMPTZ,
    state TEXT NOT NULL,
    delivery_count INTEGER NOT NULL DEFAULT 0,
    redelivered BOOLEAN NOT NULL DEFAULT FALSE,
    transaction_id TEXT,
    log_offset BIGINT NOT NULL,
    body_digest TEXT
);
CREATE INDEX IF NOT EXISTS messages_ready_idx ON messages(address,priority DESC,created_at) WHERE state IN ('available','released');
CREATE INDEX IF NOT EXISTS messages_expiry_idx ON messages(expires_at) WHERE expires_at IS NOT NULL;

CREATE TABLE IF NOT EXISTS consumers (
    id TEXT PRIMARY KEY,
    address TEXT NOT NULL REFERENCES addresses(name),
    prefetch INTEGER NOT NULL,
    semantics TEXT NOT NULL,
    connected BOOLEAN NOT NULL,
    last_seen TIMESTAMPTZ NOT NULL,
    unsettled JSONB NOT NULL DEFAULT '{}'::jsonb
);

CREATE TABLE IF NOT EXISTS transactions (
    id TEXT PRIMARY KEY,
    connection_id TEXT NOT NULL,
    state TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL,
    expires_at TIMESTAMPTZ NOT NULL,
    completed_at TIMESTAMPTZ,
    operations JSONB NOT NULL DEFAULT '[]'::jsonb,
    version BIGINT NOT NULL
);

CREATE TABLE IF NOT EXISTS cluster_leases (
    shard INTEGER PRIMARY KEY,
    node_id TEXT NOT NULL,
    epoch BIGINT NOT NULL,
    acquired_at TIMESTAMPTZ NOT NULL,
    expires_at TIMESTAMPTZ NOT NULL,
    draining BOOLEAN NOT NULL DEFAULT FALSE
);

CREATE TABLE IF NOT EXISTS broker_nodes (
    id TEXT PRIMARY KEY,
    capacity INTEGER NOT NULL,
    healthy BOOLEAN NOT NULL,
    last_seen TIMESTAMPTZ NOT NULL,
    draining BOOLEAN NOT NULL DEFAULT FALSE
);

COMMIT;
