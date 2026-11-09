-- vpn-orchestrator: Hysteria2 внутри Xray ноды (нода-вход каскада).
--
-- На обычной ноде Hysteria2 — отдельный сервер: при каждом подключении он
-- спрашивает агента по HTTP, есть ли такой UUID, и учёток не хранит. На ноде-
-- входе каскада Hysteria2 работает внутри Xray, чтобы весь трафик проходил
-- через одну маршрутизацию (RU-сервисы — напрямую, остальное — на выходную
-- ноду). Тогда пользователя нужно зарегистрировать в Hysteria2-инбаунде Xray,
-- как в Reality и XHTTP.
--
-- inbound_tag — тег этого инбаунда. Пустая строка (значение по умолчанию и
-- у всех существующих строк) — прежняя схема с отдельным сервером Hysteria2:
-- для таких нод ничего не меняется.
--
-- Идемпотентно: ADD COLUMN IF NOT EXISTS.

ALTER TABLE vpn_hysteria_endpoints
    ADD COLUMN IF NOT EXISTS inbound_tag TEXT NOT NULL DEFAULT '';

COMMENT ON COLUMN vpn_hysteria_endpoints.inbound_tag IS
    'Тег Hysteria2-инбаунда Xray (нода-вход каскада); пусто — отдельный сервер Hysteria2 с HTTP-аутентификацией через агента.';
