-- vpn-orchestrator: XHTTP-эндпоинты (VLESS + XHTTP + TLS), привязанные к серверам.
--
-- Прямой вход на ноду через её собственный домен: TLS на 443 терминирует nginx
-- ноды, путь эндпоинта проксируется в XHTTP-инбаунд Xray на 127.0.0.1. Вместе
-- с Reality и Hysteria2 это три профиля каждой ноды. CDN-эндпоинты
-- (vpn_cdn_endpoints) — отдельная сущность со своим email учётки и квотой;
-- эта таблица их не касается.
--
-- Привязка к server_key обязательна по смыслу (selectXHTTPForServer не имеет
-- фолбэка): UUID пользователя у каждого сервера свой.
--
-- Идемпотентно: CREATE ... IF NOT EXISTS. Строки добавляются через Admin API
-- (POST /admin/xhttp-endpoints) или напрямую SQL.

CREATE TABLE IF NOT EXISTS vpn_xhttp_endpoints (
    id BIGSERIAL PRIMARY KEY,
    xhttp_key TEXT NOT NULL UNIQUE,
    server_key TEXT NULL REFERENCES vpn_servers (server_key) ON DELETE SET NULL,
    enabled BOOLEAN NOT NULL DEFAULT TRUE,
    sort_order INTEGER NOT NULL DEFAULT 100,

    -- Инбаунд на узле, куда регистрируется пользователь. Значения по
    -- умолчанию нет: тег задаётся явно и совпадает с тегом в конфиге Xray ноды.
    inbound_tag TEXT NOT NULL,

    -- Адрес подключения и TLS: домен ноды, на который выпущен её сертификат.
    address TEXT NOT NULL,
    server_name TEXT NOT NULL DEFAULT '',
    host TEXT NOT NULL DEFAULT '',
    port INTEGER NOT NULL DEFAULT 443,

    -- Путь XHTTP: по нему nginx ноды отделяет туннель от обычного сайта.
    path TEXT NOT NULL,
    mode TEXT NOT NULL DEFAULT 'auto',
    fingerprint TEXT NOT NULL DEFAULT 'chrome',
    alpn TEXT NOT NULL DEFAULT 'h2,http/1.1',
    remarks TEXT NOT NULL DEFAULT '',

    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),

    CONSTRAINT chk_vpn_xhttp_endpoints_inbound_tag CHECK (btrim(inbound_tag) <> ''),
    CONSTRAINT chk_vpn_xhttp_endpoints_address CHECK (btrim(address) <> ''),
    CONSTRAINT chk_vpn_xhttp_endpoints_path CHECK (left(path, 1) = '/'),
    CONSTRAINT chk_vpn_xhttp_endpoints_port CHECK (port > 0 AND port <= 65535)
);

CREATE INDEX IF NOT EXISTS idx_vpn_xhttp_endpoints_enabled_sort
    ON vpn_xhttp_endpoints (enabled, sort_order, id);

CREATE INDEX IF NOT EXISTS idx_vpn_xhttp_endpoints_server_key
    ON vpn_xhttp_endpoints (server_key);
