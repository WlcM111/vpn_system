-- Дедупликация входящих CryptoBot-вебхуков. CryptoBot ретраит при ошибках,
-- и один и тот же платёж может прийти много раз. UNIQUE на (provider, fingerprint),
-- где fingerprint = sha256(raw_body), гарантирует ровно одну обработку.
-- Это тот же принцип, что и в billing_webhook_events для YooKassa.
CREATE TABLE IF NOT EXISTS crypto_webhook_events (
    id BIGSERIAL PRIMARY KEY,
    provider TEXT NOT NULL DEFAULT 'cryptobot',
    update_type TEXT NOT NULL,
    invoice_id TEXT NOT NULL,
    fingerprint TEXT NOT NULL,
    raw_payload JSONB NOT NULL DEFAULT '{}'::jsonb,
    received_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (provider, fingerprint)
);

CREATE INDEX IF NOT EXISTS idx_crypto_webhook_events_invoice_id
    ON crypto_webhook_events (invoice_id, received_at DESC);