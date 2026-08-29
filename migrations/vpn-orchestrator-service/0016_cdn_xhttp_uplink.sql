-- Параметры XHTTP-транспорта, добавленные в Xray-core PR #5414 (смержен 31.01.2026).
--
-- Зачем. Российские CDN по-разному ограничивают запросы к источнику:
--   * VK Cloud разрешает только GET и HEAD — POST отдаёт 405;
--   * Selectel и TimeWeb режут запросы к путям, не похожим на статический файл,
--     отдавая 403 с x-reason-code: 7 (проверено: /index.html → 200, / → 403).
-- Ядро Xray умеет подстраиваться под оба случая, но параметры надо донести
-- и до узла, и до клиентской ссылки. Узел настраивается вручную в config.json,
-- клиентская часть собирается здесь и уезжает в поле extra в vless://-ссылке.
--
-- Совместимость. Все колонки со значением по умолчанию '' или 0, что означает
-- «не задано». Сборщик ссылки такие поля в extra не добавляет, поэтому у
-- существующего эндпоинта ссылка не меняется ни на один байт. ADD COLUMN с
-- константным DEFAULT в PostgreSQL 11+ не переписывает таблицу и не берёт
-- долгую блокировку.

ALTER TABLE vpn_cdn_endpoints
    -- Метод восходящих запросов: POST (по умолчанию в ядре), GET, PUT, PATCH.
    -- Пусто = не передавать параметр, ядро использует POST.
    ADD COLUMN IF NOT EXISTS uplink_http_method TEXT NOT NULL DEFAULT '',
    -- Где передаются восходящие данные: body (по умолчанию), header, cookie.
    -- Для uplink_http_method = GET тело недоступно, нужен header или cookie.
    ADD COLUMN IF NOT EXISTS uplink_data_placement TEXT NOT NULL DEFAULT '',
    -- Имя заголовка или cookie для восходящих данных.
    ADD COLUMN IF NOT EXISTS uplink_data_key TEXT NOT NULL DEFAULT '',
    -- Размер одного куска base64 в заголовке или cookie, байты. 0 = дефолт ядра.
    ADD COLUMN IF NOT EXISTS uplink_chunk_size INTEGER NOT NULL DEFAULT 0,
    -- Суммарный объём данных одного восходящего запроса, байты. 0 = дефолт ядра.
    -- Должен быть согласован с large_client_header_buffers в nginx на узле,
    -- иначе nginx отвечает 431 и туннель рвётся.
    ADD COLUMN IF NOT EXISTS sc_max_each_post_bytes INTEGER NOT NULL DEFAULT 0,
    -- Где передаётся идентификатор сессии: path (по умолчанию), query, cookie, header.
    -- Вынос из path нужен, когда CDN фильтрует по виду пути.
    ADD COLUMN IF NOT EXISTS session_id_placement TEXT NOT NULL DEFAULT '',
    ADD COLUMN IF NOT EXISTS session_id_key TEXT NOT NULL DEFAULT '',
    -- Где передаётся порядковый номер. Если session_id_placement = path,
    -- ядро требует, чтобы seq_placement тоже был path.
    ADD COLUMN IF NOT EXISTS seq_placement TEXT NOT NULL DEFAULT '',
    ADD COLUMN IF NOT EXISTS seq_key TEXT NOT NULL DEFAULT '';

-- Ограничения на допустимые значения. NOT VALID: существующие строки не
-- проверяются при добавлении, блокировки на чтение нет. Пустая строка
-- разрешена везде и означает «параметр не передавать».
DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint WHERE conname = 'chk_vpn_cdn_uplink_method'
    ) THEN
        ALTER TABLE vpn_cdn_endpoints
            ADD CONSTRAINT chk_vpn_cdn_uplink_method
            CHECK (uplink_http_method IN ('', 'POST', 'GET', 'PUT', 'PATCH'))
            NOT VALID;
    END IF;

    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint WHERE conname = 'chk_vpn_cdn_uplink_placement'
    ) THEN
        ALTER TABLE vpn_cdn_endpoints
            ADD CONSTRAINT chk_vpn_cdn_uplink_placement
            CHECK (uplink_data_placement IN ('', 'body', 'header', 'cookie'))
            NOT VALID;
    END IF;

    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint WHERE conname = 'chk_vpn_cdn_session_placement'
    ) THEN
        ALTER TABLE vpn_cdn_endpoints
            ADD CONSTRAINT chk_vpn_cdn_session_placement
            CHECK (session_id_placement IN ('', 'path', 'query', 'cookie', 'header'))
            NOT VALID;
    END IF;

    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint WHERE conname = 'chk_vpn_cdn_seq_placement'
    ) THEN
        ALTER TABLE vpn_cdn_endpoints
            ADD CONSTRAINT chk_vpn_cdn_seq_placement
            CHECK (seq_placement IN ('', 'path', 'query', 'cookie', 'header'))
            NOT VALID;
    END IF;

    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint WHERE conname = 'chk_vpn_cdn_chunk_sizes_nonneg'
    ) THEN
        ALTER TABLE vpn_cdn_endpoints
            ADD CONSTRAINT chk_vpn_cdn_chunk_sizes_nonneg
            CHECK (uplink_chunk_size >= 0 AND sc_max_each_post_bytes >= 0)
            NOT VALID;
    END IF;
END $$;

COMMENT ON COLUMN vpn_cdn_endpoints.uplink_http_method IS
    'Метод восходящих запросов XHTTP. Пусто = POST (дефолт ядра). GET нужен для CDN, разрешающих только GET/HEAD.';
COMMENT ON COLUMN vpn_cdn_endpoints.uplink_data_placement IS
    'Где идут восходящие данные: body, header, cookie. Для GET тело недоступно.';
COMMENT ON COLUMN vpn_cdn_endpoints.sc_max_each_post_bytes IS
    'Объём данных одного восходящего запроса. Согласовать с large_client_header_buffers в nginx на узле.';
COMMENT ON COLUMN vpn_cdn_endpoints.session_id_placement IS
    'Где идёт идентификатор сессии. Вынос из path помогает против CDN, фильтрующих по виду пути.';