ALTER TABLE vpn_servers
    ADD COLUMN IF NOT EXISTS port INTEGER NOT NULL DEFAULT 443,
    ADD COLUMN IF NOT EXISTS transport TEXT NOT NULL DEFAULT 'ws',
    ADD COLUMN IF NOT EXISTS security TEXT NOT NULL DEFAULT 'tls',
    ADD COLUMN IF NOT EXISTS default_inbound_tag TEXT NOT NULL DEFAULT 'vless-ws-tls',
    ADD COLUMN IF NOT EXISTS host_header TEXT NOT NULL DEFAULT '',
    ADD COLUMN IF NOT EXISTS sni TEXT NOT NULL DEFAULT '',
    ADD COLUMN IF NOT EXISTS ws_path TEXT NOT NULL DEFAULT '/ws',
    ADD COLUMN IF NOT EXISTS flow TEXT NOT NULL DEFAULT '',
    ADD COLUMN IF NOT EXISTS xray_api_addr TEXT NOT NULL DEFAULT '127.0.0.1:10085';

ALTER TABLE vpn_pool_items
    ADD COLUMN IF NOT EXISTS inbound_tag TEXT NOT NULL DEFAULT '',
    ADD COLUMN IF NOT EXISTS flow TEXT NOT NULL DEFAULT '',
    ADD COLUMN IF NOT EXISTS level INTEGER NOT NULL DEFAULT 0,
    ADD COLUMN IF NOT EXISTS host_header TEXT NOT NULL DEFAULT '',
    ADD COLUMN IF NOT EXISTS sni TEXT NOT NULL DEFAULT '',
    ADD COLUMN IF NOT EXISTS ws_path TEXT NOT NULL DEFAULT '';

CREATE TABLE IF NOT EXISTS vpn_user_node_credentials (
    telegram_id BIGINT NOT NULL,
    item_key TEXT NOT NULL,
    server_key TEXT NOT NULL REFERENCES vpn_servers (server_key) ON DELETE CASCADE,
    node_id TEXT NOT NULL,
    inbound_tag TEXT NOT NULL,
    email TEXT NOT NULL,
    vless_uuid TEXT NOT NULL,
    access_rev BIGINT NOT NULL DEFAULT 0,
    enabled BOOLEAN NOT NULL DEFAULT TRUE,
    last_synced_rev BIGINT NOT NULL DEFAULT 0,
    last_synced_at TIMESTAMPTZ NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (telegram_id, item_key)
);

CREATE UNIQUE INDEX IF NOT EXISTS idx_vpn_user_node_credentials_email
ON vpn_user_node_credentials (email);

CREATE INDEX IF NOT EXISTS idx_vpn_user_node_credentials_node
ON vpn_user_node_credentials (node_id, enabled);

CREATE INDEX IF NOT EXISTS idx_vpn_user_node_credentials_telegram
ON vpn_user_node_credentials (telegram_id, enabled);

CREATE INDEX IF NOT EXISTS idx_vpn_user_node_credentials_access_rev
ON vpn_user_node_credentials (access_rev);

CREATE TABLE IF NOT EXISTS vpn_node_sync_results (
    id BIGSERIAL PRIMARY KEY,
    node_id TEXT NOT NULL,
    server_key TEXT NOT NULL DEFAULT '',
    telegram_id BIGINT NOT NULL DEFAULT 0,
    access_rev BIGINT NOT NULL DEFAULT 0,
    command_id TEXT NOT NULL DEFAULT '',
    event_type TEXT NOT NULL,
    success BOOLEAN NOT NULL,
    error TEXT NOT NULL DEFAULT '',
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_vpn_node_sync_results_node_time
ON vpn_node_sync_results (node_id, created_at DESC);

CREATE INDEX IF NOT EXISTS idx_vpn_node_sync_results_user_rev
ON vpn_node_sync_results (telegram_id, access_rev);