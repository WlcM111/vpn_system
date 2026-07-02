-- P2 (CR-1): персистентный счётчик попыток обработки сообщений консьюмерами.
-- Без него счётчик жил в памяти и обнулялся при рестарте — отравленное сообщение
-- никогда не доходило до DLT. Теперь счётчик переживает рестарты.
CREATE TABLE IF NOT EXISTS consumer_attempts (
    attempt_key TEXT PRIMARY KEY,
    attempts    INT NOT NULL DEFAULT 0,
    updated_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- индекс для периодической чистки старых ключей (см. ниже)
CREATE INDEX IF NOT EXISTS idx_consumer_attempts_updated_at
ON consumer_attempts (updated_at);