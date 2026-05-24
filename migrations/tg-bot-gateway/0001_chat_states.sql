CREATE TABLE IF NOT EXISTS tg_chat_states (
    telegram_id BIGINT PRIMARY KEY,
    state JSONB NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);