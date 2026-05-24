package tg_bot_gateway

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

type postgresStateStore struct{ pool *pgxpool.Pool }

func NewPostgresStateStore(pool *pgxpool.Pool) StateStore { return &postgresStateStore{pool: pool} }

func (s *postgresStateStore) Get(ctx context.Context, telegramID int64) (*ChatState, error) {
	var raw []byte
	err := s.pool.QueryRow(ctx, `SELECT state FROM tg_chat_states WHERE telegram_id=$1`, telegramID).Scan(&raw)
	if err != nil {
		if err == pgx.ErrNoRows {
			return &ChatState{Step: StepMainMenu}, nil
		}
		return nil, fmt.Errorf("get chat state: %w", err)
	}
	var st ChatState
	if err := json.Unmarshal(raw, &st); err != nil {
		return nil, err
	}
	if st.Step == "" {
		st.Step = StepMainMenu
	}
	return &st, nil
}

func (s *postgresStateStore) Set(ctx context.Context, telegramID int64, state *ChatState) error {
	if state == nil {
		state = &ChatState{Step: StepMainMenu}
	}
	raw, err := json.Marshal(state)
	if err != nil {
		return err
	}
	_, err = s.pool.Exec(ctx, `
		INSERT INTO tg_chat_states (telegram_id, state)
		VALUES ($1,$2::jsonb)
		ON CONFLICT (telegram_id) DO UPDATE SET state=EXCLUDED.state, updated_at=now()
	`, telegramID, string(raw))
	return err
}

func (s *postgresStateStore) Clear(ctx context.Context, telegramID int64) error {
	_, err := s.pool.Exec(ctx, `DELETE FROM tg_chat_states WHERE telegram_id=$1`, telegramID)
	return err
}
