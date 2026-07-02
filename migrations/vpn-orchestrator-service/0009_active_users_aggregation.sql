-- S2: переход с горячей строки vpn_servers.active_users на агрегацию из
-- vpn_user_node_credentials. Индекс ускоряет COUNT активных учёток по нодам.
-- Частичный индекс (enabled = true) — считаем только активные.
CREATE INDEX IF NOT EXISTS idx_vpn_user_node_credentials_server_enabled
ON vpn_user_node_credentials (server_key)
WHERE enabled = true;