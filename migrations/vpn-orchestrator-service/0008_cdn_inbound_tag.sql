-- Добавляет inbound_tag в CDN-эндпоинты: в какой inbound на узле нужно
-- зарегистрировать пользователя, чтобы CDN-ссылка (XHTTP) заработала.
-- По умолчанию vless-xhttp-cdn-in (соответствует рабочему конфигу Xray на узле).

ALTER TABLE vpn_cdn_endpoints
    ADD COLUMN IF NOT EXISTS inbound_tag TEXT NOT NULL DEFAULT 'vless-xhttp-cdn-in';