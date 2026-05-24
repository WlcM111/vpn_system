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
    vless_url TEXT NOT NULL,
    profile_type TEXT NOT NULL DEFAULT 'default',
    enabled BOOLEAN NOT NULL DEFAULT TRUE,
    sort_order INTEGER NOT NULL DEFAULT 100,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_vpn_pool_items_enabled_sort ON vpn_pool_items (enabled, sort_order, id);
CREATE INDEX IF NOT EXISTS idx_vpn_pool_items_country ON vpn_pool_items (country_code);
CREATE INDEX IF NOT EXISTS idx_vpn_pool_items_profile_type ON vpn_pool_items (profile_type);

INSERT INTO vpn_servers (server_key, country_code, title, public_host, node_id, enabled, max_users, weight)
VALUES
    ('lt-main-1', 'LT', 'Lithuania Main 1', 'replace-me.example.com', '', TRUE, 1000, 100)
ON CONFLICT (server_key) DO NOTHING;

INSERT INTO vpn_pool_items (item_key, server_key, country_code, title, vless_url, profile_type, enabled, sort_order)
VALUES
    (
        'lt-main-1-default',
        'lt-main-1',
        'LT',
        'Lithuania Main',
        'vless://REPLACE_UUID@replace-me.example.com:443?encryption=none&security=tls&type=ws&host=replace-me.example.com&path=%2Fws&sni=replace-me.example.com#Lithuania-Main',
        'default',
        FALSE,
        10
    ),
    (
        'yt-filter-1',
        NULL,
        'NL',
        'YouTube Filtered',
        'vless://REPLACE_UUID@replace-me.example.com:443?encryption=none&security=tls&type=ws&host=replace-me.example.com&path=%2Fws&sni=replace-me.example.com#YouTube-Filtered',
        'ad_filter',
        FALSE,
        90
    )
ON CONFLICT (item_key) DO NOTHING;