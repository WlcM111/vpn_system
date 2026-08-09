-- Профиль пользователя Telegram: ник и имя.
--
-- Раньше в telegram_users хранился только telegram_id, поэтому связаться с
-- человеком или построить отчёт по нику было невозможно без запросов к API
-- Telegram по каждому пользователю.
--
-- Нормализация: все добавляемые атрибуты функционально зависят напрямую от
-- первичного ключа telegram_id и не определяют друг друга, поэтому схема
-- остаётся в 3НФ. Храним текущее значение, а не историю: смена ника — редкое
-- событие, для бизнес-логики незначимое.
--
-- Идемпотентно: повторный прогон безопасен.

ALTER TABLE telegram_users
    ADD COLUMN IF NOT EXISTS username     TEXT NULL,
    ADD COLUMN IF NOT EXISTS first_name   TEXT NULL,
    ADD COLUMN IF NOT EXISTS last_name    TEXT NULL,
    ADD COLUMN IF NOT EXISTS last_seen_at TIMESTAMPTZ NULL;

-- Поиск по нику: частичный индекс, потому что у части пользователей ника нет.
CREATE INDEX IF NOT EXISTS idx_telegram_users_username
    ON telegram_users (lower(username))
    WHERE username IS NOT NULL;

-- Для отчётов «кто давно не заходил».
CREATE INDEX IF NOT EXISTS idx_telegram_users_last_seen
    ON telegram_users (last_seen_at DESC NULLS LAST);