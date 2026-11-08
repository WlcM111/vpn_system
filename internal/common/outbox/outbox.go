package outbox

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"
	"time"

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

// DefaultProcessingLease — через сколько событие, захваченное публикатором,
// но не отмеченное ни опубликованным, ни отложенным, снова берётся в работу.
// Публикация одного события занимает миллисекунды, а таймаут продюсера —
// секунды, поэтому двух минут достаточно с большим запасом.
const DefaultProcessingLease = 2 * time.Minute

// LockPending захватывает события к публикации: ожидающие (pending/retry,
// срок попытки наступил) и брошенные — 'processing' с истёкшей арендой
// (публикатор упал или был остановлен между захватом и отметкой результата).
func LockPending(ctx context.Context, pool *pgxpool.Pool, limit int) ([]PendingEvent, error) {
	return LockPendingWithLease(ctx, pool, limit, DefaultProcessingLease)
}

// LockPendingWithLease — LockPending с явной длительностью аренды.
func LockPendingWithLease(ctx context.Context, pool *pgxpool.Pool, limit int, lease time.Duration) ([]PendingEvent, error) {
	if lease <= 0 {
		lease = DefaultProcessingLease
	}
	rows, err := pool.Query(ctx, `
		WITH picked AS (
			SELECT id
			FROM event_outbox
			WHERE (status IN ('pending','retry') AND next_attempt_at <= now())
			   OR (status = 'processing' AND locked_at IS NOT NULL AND locked_at < now() - $2::interval)
			ORDER BY id ASC
			LIMIT $1
			FOR UPDATE SKIP LOCKED
		)
		UPDATE event_outbox o
		SET status = 'processing', attempts = attempts + 1, locked_at = now()
		FROM picked
		WHERE o.id = picked.id
		RETURNING o.id, o.topic, o.message_key, o.payload, o.attempts
	`, limit, strconv.FormatInt(lease.Milliseconds(), 10)+" milliseconds")
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
		SET status='published', published_at=now(), last_error='', locked_at=NULL
		WHERE id=$1
	`, id)
	return err
}

// maxPublishAttempts — после стольких неудачных публикаций событие
// переводится в 'failed' (и копируется в failed_messages).
const maxPublishAttempts = 20

func MarkRetry(ctx context.Context, pool *pgxpool.Pool, id int64, errText string) error {
	_, _, err := MarkRetryReturning(ctx, pool, id, errText)
	return err
}

// MarkRetryReturning откладывает событие после неудачной публикации и
// возвращает новый статус ('retry' или 'failed') и время следующей попытки.
func MarkRetryReturning(ctx context.Context, pool *pgxpool.Pool, id int64, errText string) (string, time.Time, error) {
	var status string
	var next time.Time
	err := pool.QueryRow(ctx, `
		UPDATE event_outbox
		SET status = CASE WHEN attempts >= $3 THEN 'failed' ELSE 'retry' END,
		    next_attempt_at = CASE
		        WHEN attempts >= $3 THEN next_attempt_at
		        ELSE now() + (LEAST(3600, POWER(2, LEAST(attempts, 10))::int) || ' seconds')::interval
		    END,
		    last_error = $2,
		    locked_at = NULL
		WHERE id = $1
		RETURNING status, next_attempt_at
	`, id, errText, maxPublishAttempts).Scan(&status, &next)
	return status, next, err
}

// ReleaseBlocked возвращает в очередь событие, которое в этом проходе не
// публиковалось, потому что раньше по тому же ключу упало другое событие
// (дефект D2). Попытка не засчитывается: раньше такие события расходовали
// счётчик попыток и могли уйти в 'failed', ни разу не попробовав
// опубликоваться и не попав в failed_messages. Следующая попытка — не
// раньше, чем у упавшего события того же ключа, чтобы сохранить порядок.
func ReleaseBlocked(ctx context.Context, pool *pgxpool.Pool, id int64, notBefore time.Time, reason string) error {
	_, err := pool.Exec(ctx, `
		UPDATE event_outbox
		SET status = CASE WHEN attempts > 1 THEN 'retry' ELSE 'pending' END,
		    attempts = GREATEST(attempts - 1, 0),
		    next_attempt_at = GREATEST($2, now()),
		    last_error = $3,
		    locked_at = NULL
		WHERE id = $1 AND status = 'processing'
	`, id, notBefore.UTC(), reason)
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
