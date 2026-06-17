-- vpn-orchestrator: серверы (ноды) и пул профилей (то, что видит пользователь).
--
-- ВАЖНО (идемпотентность): этот файл выполняется при каждом прогоне migrate.
-- Демонстрационные INSERT-данные УБРАНЫ намеренно:
--   1) колонка vless_url удаляется позже миграцией 0005_drop_dead_objects, поэтому
--      демо-INSERT с vless_url ломал повторный прогон (column "vless_url" does not exist);
--   2) реальные ноды и профили регистрируются через Admin API оркестратора
--      (POST /admin/nodes, POST /admin/pool-items), фейковые строки не нужны.
-- Таблицы создаются с vless_url для совместимости с историей миграций;
-- 0005 затем приводит схему к актуальному виду.

CREATE TABLE IF NOT EXISTS vpn_servers (
    id BIGSERIAL PRIMARY KEY,
    server_key TEXT NOT NULL UNIQUE,
    country_code TEXT NOT NULL,
    title TEXT NOT NULL,
    public_host TEXT NOT NULL DEFAULT '',
    node_id TEXT NOT NULL DEFAULT '',
    enabled BOOLEAN NOT NULL DEFAULT TRUE,
    max_users INTEGER NOT NULL DEFAULT 1000,
    active_users INTEGER NOT NULL DEFAULT 0,
    weight INTEGER NOT NULL DEFAULT 100,
    last_heartbeat_at TIMESTAMPTZ NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS vpn_pool_items (
    id BIGSERIAL PRIMARY KEY,
    item_key TEXT NOT NULL UNIQUE,
    server_key TEXT NULL REFERENCES vpn_servers (server_key) ON DELETE SET NULL,
    country_code TEXT NOT NULL,
    title TEXT NOT NULL,
    vless_url TEXT NOT NULL DEFAULT '',
    profile_type TEXT NOT NULL DEFAULT 'default',
    enabled BOOLEAN NOT NULL DEFAULT TRUE,
    sort_order INTEGER NOT NULL DEFAULT 100,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_vpn_pool_items_enabled_sort ON vpn_pool_items (enabled, sort_order, id);
CREATE INDEX IF NOT EXISTS idx_vpn_pool_items_country ON vpn_pool_items (country_code);
CREATE INDEX IF NOT EXISTS idx_vpn_pool_items_profile_type ON vpn_pool_items (profile_type);

-- Демо-данные намеренно не вставляются (см. комментарий вверху файла).