-- Основная таблица инвойсов крипто-оплаты.
-- order_id — наш внутренний ID ("crypto-<uuid>"), уникален.
-- command_id — ID входящей Kafka-команды для дедупа (см. processed_messages для cross-service дедупа,
--   тут команда уникальна в пределах только этого сервиса).
-- invoice_id — ID, который вернул CryptoBot после createInvoice; используется для поиска при webhook.
-- amount_value — TEXT, потому что крипта может иметь больше 2 знаков после точки (BTC до 8),
--   а NUMERIC(20,8) под все валюты выглядит костыльно. TEXT с проверкой формата на уровне сервиса надёжнее.
CREATE TABLE IF NOT EXISTS crypto_invoices (
    id BIGSERIAL PRIMARY KEY,
    order_id TEXT NOT NULL UNIQUE,
    command_id TEXT NOT NULL DEFAULT '',
    telegram_id BIGINT NOT NULL,

    plan_code TEXT NOT NULL,
    duration_days INTEGER NOT NULL,

    asset TEXT NOT NULL,
    amount_value TEXT NOT NULL,
    description TEXT NOT NULL DEFAULT '',

    invoice_id TEXT NOT NULL DEFAULT '',
    pay_url TEXT NOT NULL DEFAULT '',
    status TEXT NOT NULL DEFAULT 'creating',
    -- допустимые значения: 'creating' | 'active' | 'paid' | 'expired' | 'failed'

    raw_create_response JSONB NOT NULL DEFAULT '{}'::jsonb,
    raw_last_webhook    JSONB NOT NULL DEFAULT '{}'::jsonb,

    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    paid_at    TIMESTAMPTZ NULL,
    expires_at TIMESTAMPTZ NULL
);

-- Уникальный command_id только для непустых значений (CryptoBot-вебхуки могут создавать
-- запись без command_id в будущих сценариях; пустые не должны блокироваться UNIQUE).
CREATE UNIQUE INDEX IF NOT EXISTS idx_crypto_invoices_command_id_non_empty
    ON crypto_invoices (command_id)
    WHERE command_id <> '';

-- Аналогично для invoice_id: пустые значения встречаются в статусе 'creating'.
CREATE UNIQUE INDEX IF NOT EXISTS idx_crypto_invoices_invoice_id_non_empty
    ON crypto_invoices (invoice_id)
    WHERE invoice_id <> '';

CREATE INDEX IF NOT EXISTS idx_crypto_invoices_telegram_id
    ON crypto_invoices (telegram_id, created_at DESC);

CREATE INDEX IF NOT EXISTS idx_crypto_invoices_status
    ON crypto_invoices (status, created_at DESC);