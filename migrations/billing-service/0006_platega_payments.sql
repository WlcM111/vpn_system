-- Platega — второй платёжный провайдер в той же таблице payments.
--
-- Миграция аддитивная: только новые столбцы со значениями по умолчанию,
-- ограничения и индексы. Старый код их не читает, поэтому откат кода не
-- требует отката схемы. Все существующие строки получают provider='yookassa'
-- (до этой миграции других провайдеров в payments не было: CryptoBot живёт в
-- crypto_invoices).
--
-- Повторный запуск безопасен: IF NOT EXISTS и проверки pg_constraint.

ALTER TABLE payments
    ADD COLUMN IF NOT EXISTS provider TEXT NOT NULL DEFAULT 'yookassa',
    -- Последний статус транзакции у провайдера (для Platega — из
    -- GET /transaction/{id}); пусто, пока статус не наблюдался.
    ADD COLUMN IF NOT EXISTS provider_status TEXT NOT NULL DEFAULT '',
    -- payload, с которым создавалась транзакция Platega (order_id исходного
    -- заказа). По нему колбэк находит заказ, если transactionId ещё не
    -- сохранён локально.
    ADD COLUMN IF NOT EXISTS provider_payload TEXT NOT NULL DEFAULT '',
    -- Срок действия ссылки на оплату (created + expiresIn из ответа Platega).
    ADD COLUMN IF NOT EXISTS expires_at TIMESTAMPTZ NULL,
    -- Момент записи события об оплате в outbox. Ровно один раз на заказ.
    ADD COLUMN IF NOT EXISTS paid_at TIMESTAMPTZ NULL,
    -- Когда сверить статус у провайдера. NULL — по расписанию не сверять.
    -- Значение в будущем после захвата строки работает как аренда (lease):
    -- упавший обработчик не держит заказ дольше аренды.
    ADD COLUMN IF NOT EXISTS next_check_at TIMESTAMPTZ NULL,
    ADD COLUMN IF NOT EXISTS check_attempts INTEGER NOT NULL DEFAULT 0,
    ADD COLUMN IF NOT EXISTS last_error TEXT NOT NULL DEFAULT '',
    -- Почему заказ ждёт ручной проверки (status='review_required').
    ADD COLUMN IF NOT EXISTS review_reason TEXT NOT NULL DEFAULT '';

DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'chk_payments_provider') THEN
        ALTER TABLE payments
            ADD CONSTRAINT chk_payments_provider
            CHECK (provider IN ('yookassa', 'platega')) NOT VALID;
    END IF;

    -- Полный перечень локальных статусов заказа Platega
    -- (internal/billing/platega/machine.go, AllLocalStatuses; сверяется тестом).
    IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'chk_payments_platega_status') THEN
        ALTER TABLE payments
            ADD CONSTRAINT chk_payments_platega_status
            CHECK (provider <> 'platega' OR status IN ('local_creating','provider_creating','create_failed','create_unknown','pending','succeeded','canceled','review_required','disputed','chargebacked','partially_refunded')) NOT VALID;
    END IF;

    -- Сумма заказа Platega — только положительная (в копейках не бывает 0 ₽).
    IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'chk_payments_platega_amount') THEN
        ALTER TABLE payments
            ADD CONSTRAINT chk_payments_platega_amount
            CHECK (provider <> 'platega' OR amount_value > 0) NOT VALID;
    END IF;

    -- Оплаченный заказ обязан иметь момент выдачи.
    IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'chk_payments_platega_paid_at') THEN
        ALTER TABLE payments
            ADD CONSTRAINT chk_payments_platega_paid_at
            CHECK (provider <> 'platega' OR status <> 'succeeded' OR paid_at IS NOT NULL) NOT VALID;
    END IF;

    -- После создания транзакции её идентификатор обязателен.
    IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'chk_payments_platega_payment_id') THEN
        ALTER TABLE payments
            ADD CONSTRAINT chk_payments_platega_payment_id
            CHECK (provider <> 'platega' OR payment_id <> '' OR status IN ('local_creating','provider_creating','create_failed','create_unknown','review_required')) NOT VALID;
    END IF;

    IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'chk_payments_check_attempts') THEN
        ALTER TABLE payments
            ADD CONSTRAINT chk_payments_check_attempts
            CHECK (check_attempts >= 0) NOT VALID;
    END IF;
END $$;

-- Проверка существующих строк. Новые столбцы заполнены значениями по
-- умолчанию, поэтому проверка проходит; повторная проверка — no-op.
ALTER TABLE payments VALIDATE CONSTRAINT chk_payments_provider;
ALTER TABLE payments VALIDATE CONSTRAINT chk_payments_platega_status;
ALTER TABLE payments VALIDATE CONSTRAINT chk_payments_platega_amount;
ALTER TABLE payments VALIDATE CONSTRAINT chk_payments_platega_paid_at;
ALTER TABLE payments VALIDATE CONSTRAINT chk_payments_platega_payment_id;
ALTER TABLE payments VALIDATE CONSTRAINT chk_payments_check_attempts;

-- Не больше одного создаваемого счёта Platega на пользователя: повторные
-- нажатия не порождают неконтролируемые новые счета даже при гонке.
CREATE UNIQUE INDEX IF NOT EXISTS uq_payments_platega_inflight
ON payments (telegram_id)
WHERE provider = 'platega' AND status IN ('local_creating', 'provider_creating');

-- Очередь сверки статусов (воркер выбирает next_check_at <= now()).
CREATE INDEX IF NOT EXISTS idx_payments_next_check_at
ON payments (next_check_at)
WHERE next_check_at IS NOT NULL;

-- Поиск заказа по payload из колбэка.
CREATE INDEX IF NOT EXISTS idx_payments_provider_payload
ON payments (provider_payload)
WHERE provider_payload <> '';
