-- ============================================================================
-- 0007: новые тарифы (полгода, год) + отслеживание напоминаний об истечении.
-- Идемпотентна: ON CONFLICT / IF NOT EXISTS — безопасно прогонять повторно.
-- ============================================================================

-- 1. Новые тарифные планы. FK user_subscriptions.current_plan_code требует,
--    чтобы план существовал в subscription_plans до активации подписки.
INSERT INTO subscription_plans (code, title, duration_days, is_trial, is_active, sort_order)
VALUES
    ('semiannual_180d', 'Подписка 180 дней', 180, FALSE, TRUE, 40),
    ('annual_360d', 'Подписка 360 дней', 360, FALSE, TRUE, 50)
ON CONFLICT (code) DO UPDATE SET
    title = EXCLUDED.title,
    duration_days = EXCLUDED.duration_days,
    is_trial = EXCLUDED.is_trial,
    is_active = EXCLUDED.is_active,
    sort_order = EXCLUDED.sort_order,
    updated_at = now();

-- 2. Колонка для воркера напоминаний об истечении подписки.
--    Хранит, какое именно напоминание уже отправлено для текущего срока,
--    чтобы не слать одно и то же повторно. Значения (строки-«вехи»):
--      'd7'      — отправлено напоминание за 7 дней
--      'd3'      — отправлено за 3 дня
--      'd1'      — отправлено за 1 день
--      'expired' — отправлено уведомление об окончании
--    Сбрасывается в NULL при продлении/новой подписке (новый expires_at).
ALTER TABLE user_subscriptions
    ADD COLUMN IF NOT EXISTS last_reminder_stage TEXT NULL,
    ADD COLUMN IF NOT EXISTS reminder_anchor_at TIMESTAMPTZ NULL;

-- reminder_anchor_at — на какой expires_at «навешены» отправленные напоминания.
-- Если expires_at изменился (продление), воркер видит расхождение и сбрасывает
-- last_reminder_stage, начиная цикл напоминаний заново для нового срока.

CREATE INDEX IF NOT EXISTS idx_user_subscriptions_expires_reminder
    ON user_subscriptions (expires_at)
    WHERE status IN ('trial', 'active');