CREATE TABLE IF NOT EXISTS billing_pending_refunds (
    payment_id TEXT PRIMARY KEY,
    amount_value TEXT NOT NULL,
    currency TEXT NOT NULL,
    attempts INTEGER NOT NULL DEFAULT 0,
    next_attempt_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    last_error TEXT NOT NULL DEFAULT '',
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_billing_pending_refunds_due
ON billing_pending_refunds (next_attempt_at)
WHERE attempts < 10;