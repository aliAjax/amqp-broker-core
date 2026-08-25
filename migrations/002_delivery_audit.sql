BEGIN;
CREATE TABLE IF NOT EXISTS delivery_settlements (
    message_id TEXT NOT NULL REFERENCES messages(id) ON DELETE CASCADE,
    delivery_id BIGINT NOT NULL,
    consumer_id TEXT NOT NULL,
    state TEXT NOT NULL,
    settled BOOLEAN NOT NULL,
    settled_at TIMESTAMPTZ NOT NULL,
    PRIMARY KEY(consumer_id,delivery_id)
);
CREATE TABLE IF NOT EXISTS storage_compactions (
    id BIGSERIAL PRIMARY KEY,
    node_id TEXT NOT NULL,
    started_at TIMESTAMPTZ NOT NULL,
    completed_at TIMESTAMPTZ,
    bytes_before BIGINT NOT NULL,
    bytes_after BIGINT,
    state TEXT NOT NULL
);
COMMIT;
