-- P5: кумулятивный трафик пользователей (байты), агрегированный по всем узлам.
-- Обновляется событиями vpn.node_traffic от node-agent. Читается фидом подписки
-- для отдачи в заголовке Subscription-Userinfo (клиенты показывают объём).
CREATE TABLE IF NOT EXISTS vpn_user_traffic (
    telegram_id BIGINT PRIMARY KEY,
    uplink      BIGINT NOT NULL DEFAULT 0,
    downlink    BIGINT NOT NULL DEFAULT 0,
    updated_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);