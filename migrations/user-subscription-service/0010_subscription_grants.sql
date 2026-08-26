-- Журнал начислений доступа (grants ledger).
--
-- Зачем он нужен, если есть user_subscriptions.trial_used и last_payment_id.
--   trial_used — булев флаг в строке подписки. Он защищает от повторной выдачи,
--   пока строка цела, но не хранит НИ источника, НИ длительности, НИ момента
--   начисления. Из-за этого невозможно ни доказать инвариант «триал выдан ровно
--   один раз за всё время», ни разобрать инцидент «откуда взялись эти дни».
--   Журнал даёт и то, и другое: уникальный бизнес-ключ на уровне БД плюс аудит.
--
-- Бизнес-ключ по источникам:
--   trial    → 'trial'            (ровно один на пользователя навсегда);
--   paid     → payment_id         (один платёж = одно начисление);
--   referral → 'referral:<id>'    (одна выдача награды = одно начисление);
--   admin    → 'adm:<uuid>'       (ручное начисление, с указанием инициатора).
--
-- UNIQUE (telegram_id, source, business_key) — это и есть защита от двойного
-- начисления: она работает при гонке, повторе Kafka-сообщения, повторном
-- вебхуке и рестарте между commit и ack. Проверка в Go остаётся, но инвариант
-- держит БД.
--
-- Идемпотентно: повторный прогон безопасен.

CREATE TABLE IF NOT EXISTS subscription_grants (
    id              BIGSERIAL PRIMARY KEY,
    telegram_id     BIGINT      NOT NULL,

    source          TEXT        NOT NULL,
    business_key    TEXT        NOT NULL,

    duration_days   INTEGER     NOT NULL,

    -- От какого момента отсчитывался интервал и чем он закончился. Позволяет
    -- восстановить всю цепочку продлений без догадок.
    effective_from  TIMESTAMPTZ NOT NULL,
    effective_until TIMESTAMPTZ NOT NULL,

    granted_at      TIMESTAMPTZ NOT NULL DEFAULT now(),

    CONSTRAINT chk_subscription_grants_source
        CHECK (source IN ('trial', 'paid', 'referral', 'admin')),
    CONSTRAINT chk_subscription_grants_duration
        CHECK (duration_days > 0),
    CONSTRAINT chk_subscription_grants_interval
        CHECK (effective_until > effective_from),

    CONSTRAINT uq_subscription_grants_business_key
        UNIQUE (telegram_id, source, business_key)
);

CREATE INDEX IF NOT EXISTS idx_subscription_grants_user
    ON subscription_grants (telegram_id, granted_at DESC);

COMMENT ON TABLE subscription_grants IS
    'Append-only ledger of access grants; UNIQUE(telegram_id, source, business_key) enforces exactly-once accrual';

-- Backfill существующих триалов.
--
-- У пользователей с trial_used = true триал уже выдан, но записи в журнале нет.
-- Без backfill они смогли бы получить триал повторно после того, как код начнёт
-- опираться на журнал. Длительность и границы берём из фактических полей
-- подписки там, где они есть; иначе ставим минимально возможный корректный
-- интервал — запись нужна как ФАКТ выдачи, а не как точная реконструкция.
--
-- ON CONFLICT DO NOTHING делает шаг идемпотентным.
INSERT INTO subscription_grants (
    telegram_id, source, business_key, duration_days,
    effective_from, effective_until, granted_at
)
SELECT
    us.telegram_id,
    'trial',
    'trial',
    GREATEST(
        COALESCE(
            EXTRACT(DAY FROM (us.expires_at - us.started_at))::INTEGER,
            1
        ),
        1
    ),
    COALESCE(us.started_at, us.created_at),
    GREATEST(
        COALESCE(us.expires_at, us.created_at + interval '1 day'),
        COALESCE(us.started_at, us.created_at) + interval '1 second'
    ),
    COALESCE(us.started_at, us.created_at)
FROM user_subscriptions us
WHERE us.trial_used = true
ON CONFLICT (telegram_id, source, business_key) DO NOTHING;