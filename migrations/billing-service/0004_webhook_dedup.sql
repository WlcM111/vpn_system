CREATE TABLE IF NOT EXISTS billing_webhook_events (
    id BIGSERIAL PRIMARY KEY,
    provider TEXT NOT NULL DEFAULT 'yookassa',
    event_type TEXT NOT NULL,
    payment_id TEXT NOT NULL,
    fingerprint TEXT NOT NULL,
    raw_payload JSONB NOT NULL DEFAULT '{}'::jsonb,
    received_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (provider, fingerprint)
);

CREATE INDEX IF NOT EXISTS idx_billing_webhook_events_payment_id
ON billing_webhook_events (payment_id, received_at DESC);