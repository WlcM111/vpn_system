-- Балансировка нагрузки. Поля max_users, active_users, weight, last_heartbeat_at
-- уже существуют (0001). Constraint weight>0 уже добавлен (0004_hardening).
-- Здесь: индекс для быстрой выборки нод по стране + защита active_users от ухода в минус.

-- Быстрая выборка включённых нод по стране (для балансировщика).
CREATE INDEX IF NOT EXISTS idx_vpn_servers_country_enabled
ON vpn_servers (country_code, enabled);

-- active_users не должен уходить ниже нуля (страховка на уровне БД; приложение
-- тоже не даёт, через GREATEST(...-1, 0)).
DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint WHERE conname = 'chk_vpn_servers_active_users_nonneg'
    ) THEN
        ALTER TABLE vpn_servers
            ADD CONSTRAINT chk_vpn_servers_active_users_nonneg
            CHECK (active_users >= 0)
            NOT VALID;
    END IF;
END $$;

-- Если в проде active_users где-то разъехался в минус — выправляем в 0.
-- Это счётчик-подсказка для балансировщика, не источник истины о доступе,
-- поэтому правка безопасна (доступ пользователей не затрагивается).
UPDATE vpn_servers SET active_users = 0 WHERE active_users < 0;