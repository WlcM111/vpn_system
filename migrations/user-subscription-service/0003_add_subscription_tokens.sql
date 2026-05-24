CREATE TABLE IF NOT EXISTS subscription_tokens (
    telegram_id BIGINT PRIMARY KEY REFERENCES telegram_users (telegram_id) ON DELETE CASCADE,
    token TEXT NOT NULL UNIQUE,
    expires_at TIMESTAMPTZ NULL,
    last_issued_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_subscription_tokens_token ON subscription_tokens (token);