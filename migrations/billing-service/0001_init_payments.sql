CREATE TABLE IF NOT EXISTS payments (
    id BIGSERIAL PRIMARY KEY,
    order_id TEXT NOT NULL UNIQUE,
    telegram_id BIGINT NOT NULL,
    checkout_type TEXT NOT NULL,
    plan_code TEXT NOT NULL DEFAULT '',
    duration_days INTEGER NOT NULL DEFAULT 0,

    payment_id TEXT NOT NULL DEFAULT '',
    status TEXT NOT NULL,
    amount_value NUMERIC(12,2) NOT NULL,
    currency TEXT NOT NULL,
    description TEXT NOT NULL DEFAULT '',
    confirmation_url TEXT NOT NULL DEFAULT '',
    idempotence_key TEXT NOT NULL,
    save_payment_method BOOLEAN NOT NULL DEFAULT FALSE,
    payment_method_id TEXT NULL,
    cancellation_reason TEXT NULL,

    metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
    raw_response JSONB NOT NULL DEFAULT '{}'::jsonb,

    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_payments_telegram_id ON payments (telegram_id);
CREATE INDEX IF NOT EXISTS idx_payments_payment_id ON payments (payment_id);
CREATE INDEX IF NOT EXISTS idx_payments_status ON payments (status);
CREATE INDEX IF NOT EXISTS idx_payments_checkout_type ON payments (checkout_type);

CREATE TABLE IF NOT EXISTS billing_customers (
    telegram_id BIGINT PRIMARY KEY,
    payment_method_id TEXT NOT NULL,
    method_type TEXT NOT NULL DEFAULT '',
    card_last4 TEXT NOT NULL DEFAULT '',
    card_expiry_month TEXT NOT NULL DEFAULT '',
    card_expiry_year TEXT NOT NULL DEFAULT '',
    bound_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);