ALTER TABLE vpn_pool_items
    DROP COLUMN IF EXISTS vless_url;

DROP TABLE IF EXISTS vpn_user_server_assignments;