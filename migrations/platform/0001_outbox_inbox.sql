CREATE TABLE IF NOT EXISTS event_outbox (
    id BIGSERIAL PRIMARY KEY,
    aggregate_type TEXT NOT NULL,
    aggregate_id TEXT NOT NULL,
    topic TEXT NOT NULL,
    message_key TEXT NOT NULL,
    event_type TEXT NOT NULL,
    payload JSONB NOT NULL,
    status TEXT NOT NULL DEFAULT 'pending',
    attempts INTEGER NOT NULL DEFAULT 0,
    next_attempt_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    last_error TEXT NOT NULL DEFAULT '',
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    published_at TIMESTAMPTZ NULL
);

CREATE INDEX IF NOT EXISTS idx_event_outbox_pending
ON event_outbox (status, next_attempt_at, id)
WHERE status IN ('pending','retry');

CREATE INDEX IF NOT EXISTS idx_event_outbox_failed
ON event_outbox (created_at DESC)
WHERE status = 'failed';

CREATE TABLE IF NOT EXISTS processed_messages (
    message_id TEXT PRIMARY KEY,
    source TEXT NOT NULL,
    message_type TEXT NOT NULL,
    processed_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_processed_messages_processed_at
ON processed_messages (processed_at DESC);

CREATE TABLE IF NOT EXISTS failed_messages (
    id BIGSERIAL PRIMARY KEY,
    source TEXT NOT NULL,
    topic TEXT NOT NULL DEFAULT '',
    message_key TEXT NOT NULL DEFAULT '',
    message_type TEXT NOT NULL DEFAULT '',
    payload JSONB NOT NULL DEFAULT '{}'::jsonb,
    error TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);