CREATE TABLE IF NOT EXISTS vpn_user_access (
    telegram_id BIGINT PRIMARY KEY,
    status TEXT NOT NULL DEFAULT 'none',
    access_until TIMESTAMPTZ NULL,
    grace_until TIMESTAMPTZ NULL,
    access_rev BIGINT NOT NULL DEFAULT 0,
    country_code TEXT NOT NULL DEFAULT 'ALL',
    last_event_type TEXT NOT NULL DEFAULT '',
    last_event_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_vpn_user_access_status ON vpn_user_access (status);
CREATE INDEX IF NOT EXISTS idx_vpn_user_access_access_until ON vpn_user_access (access_until);
CREATE INDEX IF NOT EXISTS idx_vpn_user_access_access_rev ON vpn_user_access (access_rev);

CREATE TABLE IF NOT EXISTS vpn_user_server_assignments (
    telegram_id BIGINT NOT NULL,
    server_key TEXT NOT NULL REFERENCES vpn_servers (server_key) ON DELETE CASCADE,
    access_rev BIGINT NOT NULL DEFAULT 0,
    enabled BOOLEAN NOT NULL DEFAULT TRUE,
    assigned_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (telegram_id, server_key)
);

CREATE INDEX IF NOT EXISTS idx_vpn_user_server_assignments_server ON vpn_user_server_assignments (server_key);