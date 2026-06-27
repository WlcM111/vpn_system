-- vpn-orchestrator: CDN-эндпоинты (VLESS-over-XHTTP фронты), привязанные к серверам.
--
-- CDN — не отдельная нода, а альтернативный способ подключения к exit-узлу через
-- CDN-фронт, скрывающий реальный IP. Каждый CDN может быть привязан к конкретному
-- серверу (server_key) — тогда он выдаётся пользователю, которому достался этот
-- сервер. CDN с server_key = NULL считается глобальным (fallback): выдаётся, если
-- для выбранного сервера нет персональной привязки.
--
-- Идемпотентно: CREATE TABLE IF NOT EXISTS. Реальные CDN добавляются через Admin
-- API (POST /admin/cdn-endpoints), демо-данные не вставляем.

CREATE TABLE IF NOT EXISTS vpn_cdn_endpoints (
    id BIGSERIAL PRIMARY KEY,
    cdn_key TEXT NOT NULL UNIQUE,
    -- Привязка к серверу. NULL = глобальный CDN (fallback для любого сервера).
    server_key TEXT NULL REFERENCES vpn_servers (server_key) ON DELETE SET NULL,
    enabled BOOLEAN NOT NULL DEFAULT TRUE,
    sort_order INTEGER NOT NULL DEFAULT 100,

    -- Параметры подключения (см. рабочий CDN-конфиг VLESS-over-XHTTP).
    address TEXT NOT NULL,
    server_name TEXT NOT NULL DEFAULT '',
    host TEXT NOT NULL DEFAULT '',
    port INTEGER NOT NULL DEFAULT 443,
    xhttp_path TEXT NOT NULL DEFAULT '/api/uploadFile/',
    mode TEXT NOT NULL DEFAULT 'packet-up',
    fingerprint TEXT NOT NULL DEFAULT 'chrome',
    alpn TEXT NOT NULL DEFAULT 'h2,http/1.1',
    remarks TEXT NOT NULL DEFAULT 'race-src-cdn',

    -- Параметры XHTTP-обфускации.
    padding_obfs_mode BOOLEAN NOT NULL DEFAULT TRUE,
    padding_placement TEXT NOT NULL DEFAULT 'cookie',
    padding_key TEXT NOT NULL DEFAULT 'ssid',
    padding_method TEXT NOT NULL DEFAULT 'tokenish',
    sc_max_buffered_posts INTEGER NOT NULL DEFAULT 256,
    sc_min_posts_interval_ms TEXT NOT NULL DEFAULT '5',

    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Быстрый подбор CDN по серверу (привязанные) и по глобальным (server_key IS NULL).
CREATE INDEX IF NOT EXISTS idx_vpn_cdn_endpoints_server
    ON vpn_cdn_endpoints (server_key)
    WHERE enabled = TRUE;

CREATE INDEX IF NOT EXISTS idx_vpn_cdn_endpoints_enabled_sort
    ON vpn_cdn_endpoints (enabled, sort_order, id);