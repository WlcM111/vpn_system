-- vpn-orchestrator: gRPC-эндпоинты (VLESS-over-gRPC), привязанные к серверам.
--
-- gRPC — не отдельная нода, а альтернативный транспорт к тому же exit-узлу через
-- nginx grpc_pass → xray vless-grpc-cdn-in (порт 10002). Каждый gRPC-эндпоинт может
-- быть привязан к серверу (server_key); эндпоинт с server_key = NULL считается
-- глобальным (fallback). Модель полностью зеркалит vpn_cdn_endpoints, но вынесена
-- в отдельную таблицу для чистоты семантики (gRPC ≠ CDN-фронт).
--
-- Идемпотентно: CREATE TABLE IF NOT EXISTS. Реальные эндпоинты добавляются через
-- Admin API (POST /admin/grpc-endpoints), демо-данные не вставляем.

CREATE TABLE IF NOT EXISTS vpn_grpc_endpoints (
    id BIGSERIAL PRIMARY KEY,
    grpc_key TEXT NOT NULL UNIQUE,
    -- Привязка к серверу. NULL = глобальный gRPC (fallback для любого сервера).
    server_key TEXT NULL REFERENCES vpn_servers (server_key) ON DELETE SET NULL,
    enabled BOOLEAN NOT NULL DEFAULT TRUE,
    sort_order INTEGER NOT NULL DEFAULT 100,

    -- inbound на узле, куда регистрируется пользователь для gRPC.
    inbound_tag TEXT NOT NULL DEFAULT 'vless-grpc-cdn-in',

    -- Параметры подключения (см. рабочую gRPC vless://-ссылку, подтверждённую в Happ).
    address TEXT NOT NULL,
    server_name TEXT NOT NULL DEFAULT '',
    host TEXT NOT NULL DEFAULT '',
    port INTEGER NOT NULL DEFAULT 443,
    service_name TEXT NOT NULL DEFAULT 'api.grpc',
    mode TEXT NOT NULL DEFAULT 'gun',
    fingerprint TEXT NOT NULL DEFAULT 'chrome',
    alpn TEXT NOT NULL DEFAULT 'h2',
    remarks TEXT NOT NULL DEFAULT 'race-src-grpc',

    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Быстрый подбор по серверу (привязанные) и по глобальным (server_key IS NULL).
CREATE INDEX IF NOT EXISTS idx_vpn_grpc_endpoints_server
    ON vpn_grpc_endpoints (server_key)
    WHERE enabled = TRUE;

CREATE INDEX IF NOT EXISTS idx_vpn_grpc_endpoints_enabled_sort
    ON vpn_grpc_endpoints (enabled, sort_order, id);