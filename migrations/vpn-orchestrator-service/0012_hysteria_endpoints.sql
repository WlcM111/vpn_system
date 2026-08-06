-- vpn-orchestrator: Hysteria2-эндпоинты, привязанные к серверам.
--
-- Hysteria2 — UDP/QUIC-транспорт. В отличие от VLESS у неё нет inbound'ов:
-- пользователь не регистрируется на узле заранее, вместо этого Hysteria при
-- каждом подключении спрашивает node-agent по HTTP «пускать этого?».
-- Паролем выступает VLESS-UUID пользователя — отдельных учёток не требуется.
--
-- Модель зеркалит vpn_grpc_endpoints. Привязка к серверу ОБЯЗАТЕЛЬНА по смыслу:
-- ссылка должна вести на ту ноду, где у пользователя есть доступ.
--
-- Идемпотентно: повторный прогон безопасен.

CREATE TABLE IF NOT EXISTS vpn_hysteria_endpoints (
    id BIGSERIAL PRIMARY KEY,
    hysteria_key TEXT NOT NULL UNIQUE,
    server_key TEXT NULL REFERENCES vpn_servers (server_key) ON DELETE SET NULL,
    enabled BOOLEAN NOT NULL DEFAULT TRUE,
    sort_order INTEGER NOT NULL DEFAULT 100,

    address TEXT NOT NULL,
    port INTEGER NOT NULL DEFAULT 443,
    sni TEXT NOT NULL DEFAULT '',
    -- insecure=1 нужен только для самоподписанных сертификатов.
    insecure BOOLEAN NOT NULL DEFAULT FALSE,
    -- obfs: пусто = выключена (текущая конфигурация ноды).
    obfs_type TEXT NOT NULL DEFAULT '',
    obfs_password TEXT NOT NULL DEFAULT '',
    remarks TEXT NOT NULL DEFAULT 'Hysteria',

    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_vpn_hysteria_endpoints_server
    ON vpn_hysteria_endpoints (server_key)
    WHERE enabled = TRUE;

CREATE INDEX IF NOT EXISTS idx_vpn_hysteria_endpoints_enabled_sort
    ON vpn_hysteria_endpoints (enabled, sort_order, id);