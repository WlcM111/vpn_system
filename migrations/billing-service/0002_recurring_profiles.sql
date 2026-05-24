CREATE TABLE IF NOT EXISTS billing_recurring_profiles (
    telegram_id BIGINT PRIMARY KEY,
    plan_code TEXT NOT NULL,
    duration_days INTEGER NOT NULL,
    amount_value NUMERIC(12,2) NOT NULL,
    currency TEXT NOT NULL DEFAULT 'RUB',
    payment_method_id TEXT NOT NULL DEFAULT '',
    auto_renew_enabled BOOLEAN NOT NULL DEFAULT FALSE,
    status TEXT NOT NULL DEFAULT 'active',
    next_charge_at TIMESTAMPTZ NULL,
    retry_count INTEGER NOT NULL DEFAULT 0,
    grace_until TIMESTAMPTZ NULL,
    last_payment_id TEXT NOT NULL DEFAULT '',
    last_failure_reason TEXT NOT NULL DEFAULT '',
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_billing_recurring_due
ON billing_recurring_profiles (auto_renew_enabled, status, next_charge_at);

CREATE INDEX IF NOT EXISTS idx_billing_recurring_grace_until
ON billing_recurring_profiles (status, grace_until);