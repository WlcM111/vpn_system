CREATE TABLE IF NOT EXISTS telegram_users (
    telegram_id BIGINT PRIMARY KEY,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS subscription_plans (
    code TEXT PRIMARY KEY,
    title TEXT NOT NULL,
    duration_days INTEGER NOT NULL,
    is_trial BOOLEAN NOT NULL DEFAULT FALSE,
    is_active BOOLEAN NOT NULL DEFAULT TRUE,
    sort_order INTEGER NOT NULL DEFAULT 0,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

INSERT INTO subscription_plans (code, title, duration_days, is_trial, is_active, sort_order)
VALUES
    ('trial_3d', 'Пробный период 3 дня', 3, TRUE, TRUE, 10),
    ('monthly', 'Подписка 30 дней', 30, FALSE, TRUE, 20),
    ('quarterly', 'Подписка 90 дней', 90, FALSE, TRUE, 30)
ON CONFLICT (code) DO NOTHING;