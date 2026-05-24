INSERT INTO subscription_plans (code, title, duration_days, is_trial, is_active, sort_order)
VALUES
    ('monthly_30d', 'Подписка 30 дней', 30, FALSE, TRUE, 20),
    ('quarterly_90d', 'Подписка 90 дней', 90, FALSE, TRUE, 30)
ON CONFLICT (code) DO UPDATE SET
    title = EXCLUDED.title,
    duration_days = EXCLUDED.duration_days,
    is_trial = EXCLUDED.is_trial,
    is_active = EXCLUDED.is_active,
    sort_order = EXCLUDED.sort_order,
    updated_at = now();

ALTER TABLE user_subscriptions
    ADD COLUMN IF NOT EXISTS grace_until TIMESTAMPTZ NULL,
    ADD COLUMN IF NOT EXISTS auto_renew_enabled BOOLEAN NOT NULL DEFAULT FALSE,
    ADD COLUMN IF NOT EXISTS cancel_at_period_end BOOLEAN NOT NULL DEFAULT FALSE,
    ADD COLUMN IF NOT EXISTS access_rev BIGINT NOT NULL DEFAULT 0;

CREATE INDEX IF NOT EXISTS idx_user_subscriptions_grace_until ON user_subscriptions (grace_until);
CREATE INDEX IF NOT EXISTS idx_user_subscriptions_access_rev ON user_subscriptions (access_rev);