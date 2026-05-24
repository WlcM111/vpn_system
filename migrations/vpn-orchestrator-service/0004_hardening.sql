DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint WHERE conname = 'chk_vpn_servers_port'
    ) THEN
        ALTER TABLE vpn_servers
            ADD CONSTRAINT chk_vpn_servers_port
            CHECK (port > 0 AND port <= 65535)
            NOT VALID;
    END IF;
END $$;

DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint WHERE conname = 'chk_vpn_servers_weight'
    ) THEN
        ALTER TABLE vpn_servers
            ADD CONSTRAINT chk_vpn_servers_weight
            CHECK (weight > 0)
            NOT VALID;
    END IF;
END $$;

DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint WHERE conname = 'chk_vpn_pool_items_level'
    ) THEN
        ALTER TABLE vpn_pool_items
            ADD CONSTRAINT chk_vpn_pool_items_level
            CHECK (level >= 0)
            NOT VALID;
    END IF;
END $$;

CREATE INDEX IF NOT EXISTS idx_vpn_user_node_credentials_unsynced
ON vpn_user_node_credentials (node_id, enabled, updated_at)
WHERE enabled = true AND last_synced_rev < access_rev;
