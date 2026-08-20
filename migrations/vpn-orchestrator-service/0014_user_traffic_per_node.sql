-- Трафик пользователей — хранение ПО УЗЛАМ.
--
-- Было: vpn_user_traffic (PK telegram_id) + GREATEST при апсерте. Две проблемы:
--   1) две ноды писали в одну строку и затирали друг друга — вместо суммы
--      показывался максимум одной ноды;
--   2) счётчики Xray обнуляются при его рестарте, а GREATEST оставлял старое
--      значение навсегда — цифра трафика у пользователя намертво замирала.
--
-- Стало: ключ (telegram_id, node_id), суммирование при чтении. Монотонность
-- счётчика теперь гарантирует node-agent (Agent.accumulateTraffic), поэтому
-- GREATEST здесь остаётся только как защита от повторов/переупорядочивания.
--
-- Идемпотентно: повторный прогон безопасен.

CREATE TABLE IF NOT EXISTS vpn_user_traffic_node (
    telegram_id BIGINT      NOT NULL,
    node_id     TEXT        NOT NULL,
    uplink      BIGINT      NOT NULL DEFAULT 0,
    downlink    BIGINT      NOT NULL DEFAULT 0,
    updated_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (telegram_id, node_id)
);

CREATE INDEX IF NOT EXISTS idx_vpn_user_traffic_node_tg
    ON vpn_user_traffic_node (telegram_id);

COMMENT ON TABLE vpn_user_traffic_node IS
    'Cumulative per-node user traffic; total is SUM over nodes';

-- Старая таблица vpn_user_traffic НЕ удаляется: в ней лежит исторический объём,
-- накопленный до перехода. Перенос истории делается отдельным шагом гайда,
-- осознанно (см. Часть 4), потому что его корректность зависит от того,
-- перезапускался ли Xray на узле.
