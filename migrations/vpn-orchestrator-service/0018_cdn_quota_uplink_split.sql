-- Раздельный учёт восходящего трафика в CDN-квоте.
--
-- ЗАЧЕМ. Happ показывает upload и download отдельными числами
-- (subscription-userinfo: upload=...; download=...). До этой миграции квота
-- хранила только СУММУ (observed_bytes = uplink + downlink), поэтому разделить
-- их на выдаче было нечем, и заголовок отдавал общий трафик пользователя по
-- всем транспортам — включая обычный VLESS и Hysteria.
--
-- ПОЧЕМУ ДВЕ КОЛОНКИ, А НЕ ЧЕТЫРЕ. Модель уже хранит пару
-- (baseline_bytes, observed_bytes) для суммы. Достаточно завести такую же пару
-- для uplink: downlink выводится вычитанием
--
--   used_down = (observed_bytes - baseline_bytes)
--             - (observed_uplink_bytes - baseline_uplink_bytes)
--
-- Обе величины — high-water mark одного и того же отчёта, перебазируются
-- одновременно, поэтому разность неотрицательна по построению. Хранить
-- четыре колонки значило бы дублировать инвариант, который и так соблюдается.
--
-- СОВМЕСТИМОСТЬ С УЖЕ НАКОПЛЕННЫМИ ДАННЫМИ. Существующие строки получают
-- нули: восходящая часть исторического трафика неизвестна и выдумывать её
-- нельзя. До первого отчёта после миграции у таких пользователей upload
-- покажется нулём, а весь накопленный объём — как download. Со следующего
-- отчёта агента расщепление становится точным. Суммарный расход при этом не
-- искажается ни на байт: observed_bytes не трогается.
--
-- Идемпотентно: повторный прогон безопасен.

ALTER TABLE vpn_user_cdn_quota
    ADD COLUMN IF NOT EXISTS baseline_uplink_bytes BIGINT NOT NULL DEFAULT 0,
    ADD COLUMN IF NOT EXISTS observed_uplink_bytes BIGINT NOT NULL DEFAULT 0;

-- Инвариант, зеркальный chk_vpn_cdn_quota_observed_ge_baseline: восходящий
-- high-water mark не может опуститься ниже своей базы. Сброс счётчика Xray
-- обрабатывается перебазированием в ApplyObservationTx, а не отрицательным
-- остатком.
--
-- NOT VALID — чтобы ALTER не брал долгую блокировку на существующих строках.
-- Новые и изменяемые строки проверяются полноценно.
DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint
        WHERE conname = 'chk_vpn_cdn_quota_uplink_ge_baseline'
    ) THEN
        ALTER TABLE vpn_user_cdn_quota
            ADD CONSTRAINT chk_vpn_cdn_quota_uplink_ge_baseline
            CHECK (observed_uplink_bytes >= baseline_uplink_bytes) NOT VALID;
    END IF;
END
$$;

-- Восходящая часть не может превышать общий кумулятив: иначе downlink,
-- вычисляемый вычитанием, ушёл бы в минус.
DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint
        WHERE conname = 'chk_vpn_cdn_quota_uplink_le_total'
    ) THEN
        ALTER TABLE vpn_user_cdn_quota
            ADD CONSTRAINT chk_vpn_cdn_quota_uplink_le_total
            CHECK (observed_uplink_bytes <= observed_bytes) NOT VALID;
    END IF;
END
$$;

COMMENT ON COLUMN vpn_user_cdn_quota.observed_uplink_bytes IS
    'High-water mark восходящей части CDN-кумулятива; downlink = (observed_bytes - baseline_bytes) - (observed_uplink_bytes - baseline_uplink_bytes)';
COMMENT ON COLUMN vpn_user_cdn_quota.baseline_uplink_bytes IS
    'Значение observed_uplink_bytes на начало периода; перебазируется вместе с baseline_bytes';