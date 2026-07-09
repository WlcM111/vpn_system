-- Реферальная программа. Три таблицы:
--   referral_codes         — стабильный код (личная ссылка) на пользователя;
--   referrals              — факт «кого пригласил кто» + статус конверсии;
--   referral_reward_grants — журнал выданных бесплатных месяцев (идемпотентность).
--
-- Доступные месяцы НЕ хранятся числом, а вычисляются:
--   available = floor(converted_count / N) - sum(months_granted)
-- где N = REFERRAL_USERS_PER_MONTH. Начисление идемпотентно и аудируемо.
--
-- Идемпотентно (IF NOT EXISTS): повторный прогон безопасен.

CREATE TABLE IF NOT EXISTS referral_codes (
    telegram_id BIGINT PRIMARY KEY,
    code        TEXT NOT NULL UNIQUE,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_referral_codes_code ON referral_codes (code);

CREATE TABLE IF NOT EXISTS referrals (
    -- Приглашённый — PK: один приглашённый привязан РОВНО к одному рефереру навсегда.
    referee_telegram_id  BIGINT PRIMARY KEY,
    referrer_telegram_id BIGINT NOT NULL,
    status               TEXT NOT NULL DEFAULT 'pending', -- 'pending' | 'converted'
    created_at           TIMESTAMPTZ NOT NULL DEFAULT now(),
    converted_at         TIMESTAMPTZ NULL,
    CONSTRAINT chk_referral_not_self CHECK (referrer_telegram_id <> referee_telegram_id)
);

CREATE INDEX IF NOT EXISTS idx_referrals_referrer
    ON referrals (referrer_telegram_id)
    WHERE status = 'converted';

CREATE TABLE IF NOT EXISTS referral_reward_grants (
    id                   BIGSERIAL PRIMARY KEY,
    referrer_telegram_id BIGINT NOT NULL,
    months_granted       INTEGER NOT NULL CHECK (months_granted > 0),
    granted_at           TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_referral_grants_referrer
    ON referral_reward_grants (referrer_telegram_id);