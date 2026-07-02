-- S11: индексы для периодической чистки опубликованного outbox и старого inbox.
-- Ускоряют DELETE по времени, чтобы чистка не делала seq scan.
CREATE INDEX IF NOT EXISTS idx_event_outbox_published_at
ON event_outbox (published_at)
WHERE status = 'published';

CREATE INDEX IF NOT EXISTS idx_processed_messages_processed_at_cleanup
ON processed_messages (processed_at);