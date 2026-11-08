-- Outbox: аренда события на время публикации (дефект D1).
--
-- Раньше LockPending переводил событие в 'processing', и если публикатор
-- останавливался (отмена контекста при рестарте сервиса, падение процесса,
-- сбой БД при отметке результата) до MarkPublished/MarkRetry, событие
-- оставалось в 'processing' навсегда: ни один проход его больше не выбирал.
-- Теперь захват ставит locked_at, и событие с истёкшей арендой снова берётся
-- в работу (повторная публикация безопасна — получатели идемпотентны).
--
-- Строки, зависшие в 'processing' ДО этой миграции (locked_at IS NULL),
-- автоматически не перезапускаются: среди них могут быть очень старые
-- уведомления. Их нужно просмотреть и при необходимости вернуть в очередь
-- вручную (см. docs/payments-platega.md, «События outbox, зависшие до миграции 0011»).

ALTER TABLE event_outbox
    ADD COLUMN IF NOT EXISTS locked_at TIMESTAMPTZ NULL;

CREATE INDEX IF NOT EXISTS idx_event_outbox_processing_locked
ON event_outbox (locked_at)
WHERE status = 'processing';
