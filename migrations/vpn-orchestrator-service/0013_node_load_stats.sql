-- Показатели нагрузки ноды, приходящие в heartbeat.
--
-- До этого нагрузка оценивалась числом выданных учётных записей
-- (vpn_user_node_credentials), то есть ёмкостью, а не реальной загрузкой.
-- Теперь нода сообщает, сколько пользователей действительно передают данные
-- и с какой скоростью идёт трафик.
--
-- Идемпотентно: повторный прогон безопасен.

ALTER TABLE vpn_servers
    -- Пользователи, чьи счётчики трафика выросли за последний интервал.
    ADD COLUMN IF NOT EXISTS online_users INTEGER NOT NULL DEFAULT 0,
    -- Скорость трафика ноды на момент последнего heartbeat, байт/с.
    ADD COLUMN IF NOT EXISTS uplink_bps BIGINT NOT NULL DEFAULT 0,
    ADD COLUMN IF NOT EXISTS downlink_bps BIGINT NOT NULL DEFAULT 0,
    -- Полоса канала ноды, Мбит/с. 0 = не задана, тогда давление по каналу
    -- в расчёте нагрузки не учитывается.
    ADD COLUMN IF NOT EXISTS bandwidth_mbps INTEGER NOT NULL DEFAULT 0;

COMMENT ON COLUMN vpn_servers.online_users IS
    'Users that actually transferred data during the last collection interval';
COMMENT ON COLUMN vpn_servers.bandwidth_mbps IS
    'Uplink capacity in Mbps; 0 disables bandwidth pressure in load scoring';