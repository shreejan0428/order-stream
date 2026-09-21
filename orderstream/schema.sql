CREATE TABLE IF NOT EXISTS orders (
    id UUID PRIMARY KEY,
    request_key TEXT UNIQUE NOT NULL,
    amount_cents BIGINT NOT NULL CHECK (amount_cents > 0),
    status TEXT NOT NULL CHECK (status IN ('created', 'cancelled')),
    version INTEGER NOT NULL CHECK (version > 0),
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE TABLE IF NOT EXISTS outbox (
    sequence BIGSERIAL PRIMARY KEY,
    event_id UUID UNIQUE NOT NULL,
    order_id UUID NOT NULL REFERENCES orders(id),
    payload JSONB NOT NULL,
    published_at TIMESTAMPTZ,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
ALTER TABLE outbox ADD COLUMN IF NOT EXISTS topic TEXT NOT NULL DEFAULT 'order-events';
CREATE INDEX IF NOT EXISTS outbox_pending ON outbox(sequence) WHERE published_at IS NULL;
CREATE TABLE IF NOT EXISTS processed_events (
    projection TEXT NOT NULL,
    event_id UUID NOT NULL,
    digest TEXT NOT NULL,
    outcome TEXT NOT NULL,
    processed_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (projection, event_id)
);
CREATE TABLE IF NOT EXISTS order_projection (
    projection TEXT NOT NULL,
    order_id UUID NOT NULL,
    amount_cents BIGINT NOT NULL CHECK (amount_cents > 0),
    status TEXT NOT NULL CHECK (status IN ('created', 'cancelled')),
    version INTEGER NOT NULL,
    PRIMARY KEY (projection, order_id)
);
CREATE TABLE IF NOT EXISTS failed_events (
    id BIGSERIAL PRIMARY KEY,
    projection TEXT NOT NULL,
    topic TEXT NOT NULL,
    partition_id INTEGER NOT NULL,
    offset_id BIGINT NOT NULL,
    raw_value TEXT NOT NULL,
    error TEXT NOT NULL,
    attempts INTEGER NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (projection, topic, partition_id, offset_id)
);
CREATE TABLE IF NOT EXISTS processing_metrics (
    projection TEXT PRIMARY KEY,
    duplicate_deliveries BIGINT NOT NULL DEFAULT 0
);
