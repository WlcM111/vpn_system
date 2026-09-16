-- vpn-orchestrator: Reality-эндпоинты (VLESS + TCP + Reality), привязанные к серверам.
--
-- Reality — четвёртый транспорт к тому же exit-узлу, принципиально отличающийся
-- от трёх существующих: он НЕ проходит через nginx. Reality сам терминирует TLS
-- и подделывает рукопожатие под чужой сайт, поэтому ему нужен собственный порт
-- (8443) и прямой доступ снаружи. Ни CDN, ни reverse-proxy перед ним поставить
-- нельзя — это ограничение протокола, а не конфигурации.
--
-- Зачем при трёх работающих транспортах. Все они завязаны на домен
-- race-src.com и его сертификат: при бане домена или проблемах с Let's Encrypt
-- перестают работать разом. Reality не использует ни домен, ни сертификат —
-- клиент подключается по IP и предъявляет SNI чужого сайта. Это независимый
-- запасной путь.
--
-- Модель зеркалит vpn_grpc_endpoints: привязка к server_key обязательна
-- (selectRealityForServer не имеет фолбэка на глобальный эндпоинт — адрес
-- узла у каждого свой, глобальный Reality не имеет смысла).
--
-- Идемпотентно: CREATE TABLE IF NOT EXISTS. Реальные эндпоинты добавляются
-- через Admin API (POST /admin/reality-endpoints) или напрямую SQL.

CREATE TABLE IF NOT EXISTS vpn_reality_endpoints (
    id BIGSERIAL PRIMARY KEY,
    reality_key TEXT NOT NULL UNIQUE,
    -- Привязка к серверу обязательна по смыслу: address — это IP конкретного
    -- узла. NULL допускается схемой ради симметрии с остальными таблицами,
    -- но selectRealityForServer такой эндпоинт не выберет.
    server_key TEXT NULL REFERENCES vpn_servers (server_key) ON DELETE SET NULL,
    enabled BOOLEAN NOT NULL DEFAULT TRUE,
    sort_order INTEGER NOT NULL DEFAULT 100,

    -- inbound на узле, куда регистрируется пользователь.
    inbound_tag TEXT NOT NULL DEFAULT 'vless-reality-in',

    -- Адрес подключения. Для Reality это IP узла, а не домен: домен здесь
    -- не нужен и только создаёт лишнюю зависимость.
    address TEXT NOT NULL,
    port INTEGER NOT NULL DEFAULT 8443,

    -- SNI, который клиент предъявляет в ClientHello. Должен совпадать с одним
    -- из serverNames в конфиге узла, иначе сервер не распознает клиента.
    -- Значение видит DPI, поэтому берём популярный российский домен.
    server_name TEXT NOT NULL,

    -- Публичный ключ X25519 из пары, сгенерированной командой `xray x25519`.
    -- Приватная половина остаётся на узле и в БД не хранится.
    public_key TEXT NOT NULL,

    -- shortId: hex-строка чётной длины, до 16 символов. Должна входить в
    -- список shortIds на узле. Пустая строка допустима, если узел разрешает
    -- пустой shortId.
    short_id TEXT NOT NULL DEFAULT '',

    -- spiderX: путь, по которому клиент «гуляет» по сайту-прикрытию перед
    -- установлением туннеля. Значение по умолчанию "/" достаточно.
    spider_x TEXT NOT NULL DEFAULT '/',

    -- flow: xtls-rprx-vision убирает двойное шифрование внутреннего TLS.
    -- Пустое значение означает обычный TLS-прокси без оптимизации.
    flow TEXT NOT NULL DEFAULT 'xtls-rprx-vision',

    -- Отпечаток TLS-библиотеки, под который маскируется ClientHello.
    fingerprint TEXT NOT NULL DEFAULT 'chrome',

    remarks TEXT NOT NULL DEFAULT 'reality',

    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Быстрый подбор по серверу.
CREATE INDEX IF NOT EXISTS idx_vpn_reality_endpoints_server
    ON vpn_reality_endpoints (server_key)
    WHERE enabled = TRUE;

CREATE INDEX IF NOT EXISTS idx_vpn_reality_endpoints_enabled_sort
    ON vpn_reality_endpoints (enabled, sort_order, id);

-- Ограничения на формат значений. NOT VALID: существующие строки (их нет)
-- не проверяются, блокировки на чтение не возникает.
DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint WHERE conname = 'chk_vpn_reality_port'
    ) THEN
        ALTER TABLE vpn_reality_endpoints
            ADD CONSTRAINT chk_vpn_reality_port
            CHECK (port > 0 AND port <= 65535)
            NOT VALID;
    END IF;

    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint WHERE conname = 'chk_vpn_reality_short_id'
    ) THEN
        -- shortId: hex чётной длины до 16 символов либо пусто.
        ALTER TABLE vpn_reality_endpoints
            ADD CONSTRAINT chk_vpn_reality_short_id
            CHECK (short_id = '' OR (short_id ~ '^[0-9a-f]+$' AND length(short_id) % 2 = 0 AND length(short_id) <= 16))
            NOT VALID;
    END IF;

    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint WHERE conname = 'chk_vpn_reality_flow'
    ) THEN
        ALTER TABLE vpn_reality_endpoints
            ADD CONSTRAINT chk_vpn_reality_flow
            CHECK (flow IN ('', 'xtls-rprx-vision'))
            NOT VALID;
    END IF;
END $$;

COMMENT ON TABLE vpn_reality_endpoints IS
    'VLESS + TCP + Reality: транспорт без домена и сертификата, отдельный порт мимо nginx.';
COMMENT ON COLUMN vpn_reality_endpoints.server_name IS
    'SNI сайта-прикрытия. Виден DPI, поэтому берётся популярный домен.';
COMMENT ON COLUMN vpn_reality_endpoints.public_key IS
    'Публичная половина пары X25519. Приватная хранится только на узле.';