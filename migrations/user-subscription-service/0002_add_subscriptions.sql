CREATE TABLE IF NOT EXISTS user_subscriptions (
    telegram_id BIGINT PRIMARY KEY REFERENCES telegram_users (telegram_id) ON DELETE CASCADE,
    status TEXT NOT NULL DEFAULT 'none',
    current_plan_code TEXT NULL REFERENCES subscription_plans (code),
    trial_used BOOLEAN NOT NULL DEFAULT FALSE,
    started_at TIMESTAMPTZ NULL,
    expires_at TIMESTAMPTZ NULL,
    canceled_at TIMESTAMPTZ NULL,
    last_payment_id TEXT NULL,
    country_code TEXT NOT NULL DEFAULT 'LT',
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_user_subscriptions_status ON user_subscriptions (status);
CREATE INDEX IF NOT EXISTS idx_user_subscriptions_expires_at ON user_subscriptions (expires_at);
CREATE INDEX IF NOT EXISTS idx_user_subscriptions_last_payment_id ON user_subscriptions (last_payment_id);