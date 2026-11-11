-- Optional XHTTP transport settings. Existing links keep their old defaults.
ALTER TABLE vpn_xhttp_endpoints
    ADD COLUMN IF NOT EXISTS x_padding_bytes TEXT NOT NULL DEFAULT '',
    ADD COLUMN IF NOT EXISTS sc_max_each_post_bytes INTEGER NOT NULL DEFAULT 0,
    ADD COLUMN IF NOT EXISTS sc_min_posts_interval_ms TEXT NOT NULL DEFAULT '',
    ADD COLUMN IF NOT EXISTS enable_xmux BOOLEAN NOT NULL DEFAULT FALSE,
    ADD COLUMN IF NOT EXISTS xmux_json TEXT NOT NULL DEFAULT '';

DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_constraint
                   WHERE conname = 'chk_vpn_xhttp_xmux_object'
                     AND conrelid = 'vpn_xhttp_endpoints'::regclass) THEN
        ALTER TABLE vpn_xhttp_endpoints ADD CONSTRAINT chk_vpn_xhttp_xmux_object
            CHECK (CASE WHEN btrim(xmux_json) = '' THEN NOT enable_xmux
                        ELSE jsonb_typeof(xmux_json::jsonb) = 'object'
                             AND (NOT enable_xmux OR xmux_json::jsonb <> '{}'::jsonb) END);
    END IF;
    IF NOT EXISTS (SELECT 1 FROM pg_constraint
                   WHERE conname = 'chk_vpn_xhttp_sc_max_post'
                     AND conrelid = 'vpn_xhttp_endpoints'::regclass) THEN
        ALTER TABLE vpn_xhttp_endpoints ADD CONSTRAINT chk_vpn_xhttp_sc_max_post
            CHECK (sc_max_each_post_bytes >= 0);
    END IF;
END $$;
