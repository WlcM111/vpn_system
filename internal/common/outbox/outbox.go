package outbox

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

type Event struct {
	AggregateType string
	AggregateID   string
	Topic         string
	MessageKey    string
	EventType     string
	Payload       any
}

func AddTx(ctx context.Context, tx pgx.Tx, e Event) error {
	payload, err := json.Marshal(e.Payload)
	if err != nil {
		return fmt.Errorf("marshal outbox payload: %w", err)
	}
	_, err = tx.Exec(ctx, `
		INSERT INTO event_outbox (
			aggregate_type, aggregate_id, topic, message_key, event_type, payload
		) VALUES ($1,$2,$3,$4,$5,$6::jsonb)
	`, e.AggregateType, e.AggregateID, e.Topic, e.MessageKey, e.EventType, string(payload))
	if err != nil {
		return fmt.Errorf("insert outbox event: %w", err)
	}
	return nil
}

func MarkProcessed(ctx context.Context, tx pgx.Tx, source, messageID, messageType string) (bool, error) {
	if source == "" || messageID == "" {
		return false, fmt.Errorf("source and messageID are required")
	}
	ct, err := tx.Exec(ctx, `
		INSERT INTO processed_messages (message_id, source, message_type)
		VALUES ($1, $2, $3)
		ON CONFLICT (message_id) DO NOTHING
	`, source+":"+messageID, source, messageType)
	if err != nil {
		return false, fmt.Errorf("mark processed: %w", err)
	}
	return ct.RowsAffected() == 1, nil
}

type PendingEvent struct {
	ID         int64
	Topic      string
	MessageKey string
	Payload    []byte
	Attempts   int
}

func LockPending(ctx context.Context, pool *pgxpool.Pool, limit int) ([]PendingEvent, error) {
	rows, err := pool.Query(ctx, `
		WITH picked AS (
			SELECT id
			FROM event_outbox
			WHERE status IN ('pending','retry')
			  AND next_attempt_at <= now()
			ORDER BY id ASC
			LIMIT $1
			FOR UPDATE SKIP LOCKED
		)
		UPDATE event_outbox o
		SET status = 'processing', attempts = attempts + 1
		FROM picked
		WHERE o.id = picked.id
		RETURNING o.id, o.topic, o.message_key, o.payload, o.attempts
	`, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var out []PendingEvent
	for rows.Next() {
		var e PendingEvent
		if err := rows.Scan(&e.ID, &e.Topic, &e.MessageKey, &e.Payload, &e.Attempts); err != nil {
			return nil, err
		}
		out = append(out, e)
	}
	return out, rows.Err()
}

func MarkPublished(ctx context.Context, pool *pgxpool.Pool, id int64) error {
	_, err := pool.Exec(ctx, `
		UPDATE event_outbox
		SET status='published', published_at=now(), last_error=''
		WHERE id=$1
	`, id)
	return err
}

func MarkRetry(ctx context.Context, pool *pgxpool.Pool, id int64, errText string) error {
	_, err := pool.Exec(ctx, `
		UPDATE event_outbox
		SET status = CASE WHEN attempts >= 20 THEN 'failed' ELSE 'retry' END,
		    next_attempt_at = CASE
		        WHEN attempts >= 20 THEN next_attempt_at
		        ELSE now() + (LEAST(3600, POWER(2, LEAST(attempts, 10))::int) || ' seconds')::interval
		    END,
		    last_error = $2
		WHERE id = $1
	`, id, errText)
	return err
}

func SaveFailedMessage(ctx context.Context, pool *pgxpool.Pool, source, topic, key, messageType string, payload []byte, errText string) error {
	if len(payload) == 0 {
		payload = []byte(`{}`)
	}
	if !json.Valid(payload) {
		b, _ := json.Marshal(map[string]string{"raw": string(payload)})
		payload = b
	}
	_, err := pool.Exec(ctx, `
		INSERT INTO failed_messages (source, topic, message_key, message_type, payload, error)
		VALUES ($1,$2,$3,$4,$5::jsonb,$6)
	`, source, topic, key, messageType, string(payload), errText)
	return err
}
