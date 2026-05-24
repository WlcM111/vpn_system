ALTER TABLE subscription_tokens
    ADD COLUMN IF NOT EXISTS revoked_at TIMESTAMPTZ NULL,
    ADD COLUMN IF NOT EXISTS rotate_reason TEXT NOT NULL DEFAULT '';

CREATE INDEX IF NOT EXISTS idx_subscription_tokens_active_token
ON subscription_tokens (token)
WHERE revoked_at IS NULL;