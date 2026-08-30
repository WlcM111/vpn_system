-- Параметры эталонной конфигурации XHTTP из proxy-via-russian-cdn, которых
-- не хватало в схеме после миграции 0016.
--
-- Зачем. Эталон, проверенный на российских CDN, включает мультиплексирование
-- (enableXmux + блок xmux) и обфускацию padding через заголовок. Серверную
-- половину можно задать в config.json на узле, но клиент узнаёт параметры
-- только из ссылки: без этих колонок конфигурации разойдутся и рукопожатие
-- не сойдётся.
--
-- Совместимость. Дефолты пустые, поэтому сборщик ссылки их не добавляет и
-- существующие конфигурации не меняются. ADD COLUMN с константным DEFAULT
-- в PostgreSQL 11+ не переписывает таблицу.

ALTER TABLE vpn_cdn_endpoints
    -- Диапазон длины паддинга, например "100-1000". Пусто = дефолт ядра.
    ADD COLUMN IF NOT EXISTS x_padding_bytes TEXT NOT NULL DEFAULT '',
    -- Имя заголовка для паддинга при xPaddingPlacement=queryInHeader.
    ADD COLUMN IF NOT EXISTS x_padding_header TEXT NOT NULL DEFAULT '',
    -- Включение мультиплексирования сессий поверх ограниченного числа
    -- соединений. Через CDN с балансировкой по эджам это существенно.
    ADD COLUMN IF NOT EXISTS enable_xmux BOOLEAN NOT NULL DEFAULT FALSE,
    -- Параметры xmux одним JSON-объектом: набор полей задаётся ядром и
    -- меняется между версиями, поэтому колонка на каждое поле избыточна.
    ADD COLUMN IF NOT EXISTS xmux_json TEXT NOT NULL DEFAULT '';

DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint WHERE conname = 'chk_vpn_cdn_xmux_json'
    ) THEN
        ALTER TABLE vpn_cdn_endpoints
            ADD CONSTRAINT chk_vpn_cdn_xmux_json
            CHECK (xmux_json = '' OR xmux_json LIKE '{%}')
            NOT VALID;
    END IF;
END $$;

COMMENT ON COLUMN vpn_cdn_endpoints.enable_xmux IS
    'Мультиплексирование XHTTP. Требуется для CDN с балансировкой по эджам.';
COMMENT ON COLUMN vpn_cdn_endpoints.xmux_json IS
    'Параметры xmux одним JSON-объектом. Пусто = не передавать в extra.';
