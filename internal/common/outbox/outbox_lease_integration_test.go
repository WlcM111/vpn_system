//go:build integration

package outbox

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

// ============================================================================
// Дефекты D1/D2: аренда захваченных событий и возврат заблокированных
// событий без расхода попыток.
// ============================================================================

func addEvents(t *testing.T, pool *pgxpool.Pool, topic, key string, n int) {
	t.Helper()
	ctx := context.Background()
	tx, err := pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < n; i++ {
		if err := AddTx(ctx, tx, Event{
			AggregateType: "test", AggregateID: key, Topic: topic, MessageKey: key,
			EventType: "test.event", Payload: map[string]int{"n": i},
		}); err != nil {
			t.Fatal(err)
		}
	}
	if err := tx.Commit(ctx); err != nil {
		t.Fatal(err)
	}
}

func eventState(t *testing.T, pool *pgxpool.Pool, id int64) (string, int) {
	t.Helper()
	var status string
	var attempts int
	if err := pool.QueryRow(context.Background(), `SELECT status, attempts FROM event_outbox WHERE id=$1`, id).Scan(&status, &attempts); err != nil {
		t.Fatal(err)
	}
	return status, attempts
}

// D1: событие, захваченное упавшим публикатором, не остаётся в 'processing'
// навсегда — после аренды его забирает следующий проход.
func TestLeaseReclaimsAbandonedProcessing(t *testing.T) {
	pool := setupPostgres(t)
	ctx := context.Background()
	addEvents(t, pool, "t.topic", "k1", 1)

	first, err := LockPendingWithLease(ctx, pool, 10, time.Minute)
	if err != nil || len(first) != 1 {
		t.Fatalf("first claim: %v %v", first, err)
	}
	// Публикатор «упал»: ни MarkPublished, ни MarkRetry не вызваны.
	again, err := LockPendingWithLease(ctx, pool, 10, time.Minute)
	if err != nil || len(again) != 0 {
		t.Fatalf("fresh lease must not be reclaimed: %v %v", again, err)
	}
	if _, err := pool.Exec(ctx, `UPDATE event_outbox SET locked_at = now() - interval '2 minutes' WHERE id=$1`, first[0].ID); err != nil {
		t.Fatal(err)
	}
	reclaimed, err := LockPendingWithLease(ctx, pool, 10, time.Minute)
	if err != nil || len(reclaimed) != 1 || reclaimed[0].ID != first[0].ID || reclaimed[0].Attempts != 2 {
		t.Fatalf("abandoned event not reclaimed: %+v %v", reclaimed, err)
	}
	if err := MarkPublished(ctx, pool, reclaimed[0].ID); err != nil {
		t.Fatal(err)
	}
	var lockedAt *time.Time
	_ = pool.QueryRow(ctx, `SELECT locked_at FROM event_outbox WHERE id=$1`, first[0].ID).Scan(&lockedAt)
	if lockedAt != nil {
		t.Fatal("locked_at must be cleared after publish")
	}
}

// Строки, зависшие до миграции (locked_at IS NULL), сами не перезапускаются.
func TestLegacyProcessingRowsAreNotAutoRepublished(t *testing.T) {
	pool := setupPostgres(t)
	ctx := context.Background()
	addEvents(t, pool, "t.topic", "legacy", 1)
	if _, err := pool.Exec(ctx, `UPDATE event_outbox SET status='processing', locked_at=NULL, attempts=1`); err != nil {
		t.Fatal(err)
	}
	got, err := LockPendingWithLease(ctx, pool, 10, time.Millisecond)
	if err != nil || len(got) != 0 {
		t.Fatalf("legacy processing row republished: %v %v", got, err)
	}
}

// D2: заблокированные события не расходуют попытки и не уходят в 'failed',
// не попробовав опубликоваться.
func TestBlockedEventsKeepAttemptsAndOrder(t *testing.T) {
	pool := setupPostgres(t)
	ctx := context.Background()
	addEvents(t, pool, "t.topic", "user-1", 3)

	var published []int64
	failFirst := true
	for pass := 0; pass < 30; pass++ {
		if _, err := pool.Exec(ctx, `UPDATE event_outbox SET next_attempt_at = now() WHERE status IN ('pending','retry')`); err != nil {
			t.Fatal(err)
		}
		events, err := LockPendingWithLease(ctx, pool, 10, time.Minute)
		if err != nil {
			t.Fatal(err)
		}
		if len(events) == 0 {
			break
		}
		publishBatch(ctx, pool, events, "test", func(_ context.Context, e PendingEvent) error {
			// Голова цепочки падает 25 проходов подряд: больше лимита попыток.
			if failFirst && e.ID == events[0].ID && pass < 25 {
				return errors.New("kafka down")
			}
			published = append(published, e.ID)
			return nil
		})
	}
	rows, err := pool.Query(ctx, `SELECT id, status, attempts FROM event_outbox ORDER BY id`)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	var states []string
	for rows.Next() {
		var id int64
		var st string
		var at int
		_ = rows.Scan(&id, &st, &at)
		states = append(states, fmt.Sprintf("%d:%s:%d", id, st, at))
	}
	// Голова исчерпала попытки и ушла в failed (с копией в failed_messages),
	// остальные события той же цепочки остались ждать, не потратив попыток.
	head, headAttempts := eventState(t, pool, 1)
	if head != "failed" || headAttempts != maxPublishAttempts {
		t.Fatalf("head: %s/%d states=%v", head, headAttempts, states)
	}
	var failedCopies int
	_ = pool.QueryRow(ctx, `SELECT count(*) FROM failed_messages`).Scan(&failedCopies)
	if failedCopies != 1 {
		t.Fatalf("failed_messages = %d", failedCopies)
	}
	for _, id := range []int64{2, 3} {
		st, at := eventState(t, pool, id)
		if st == "failed" {
			t.Fatalf("blocked event %d failed without being attempted: %v", id, states)
		}
		_ = at
	}
	// После ухода головы в failed хвост публикуется по порядку.
	if len(published) != 2 || published[0] != 2 || published[1] != 3 {
		t.Fatalf("published order %v states=%v", published, states)
	}
}

// Отмена контекста посреди прохода возвращает события в очередь сразу.
func TestShutdownReleasesClaimedEvents(t *testing.T) {
	pool := setupPostgres(t)
	addEvents(t, pool, "t.topic", "k", 3)
	ctx, cancel := context.WithCancel(context.Background())
	events, err := LockPendingWithLease(ctx, pool, 10, time.Hour)
	if err != nil || len(events) != 3 {
		t.Fatal(events, err)
	}
	n := 0
	publishBatch(ctx, pool, events, "test", func(c context.Context, e PendingEvent) error {
		n++
		cancel()
		return c.Err()
	})
	for _, e := range events {
		st, at := eventState(t, pool, e.ID)
		if st == "processing" || at != 0 {
			t.Fatalf("event %d left %s/%d after shutdown", e.ID, st, at)
		}
	}
	again, err := LockPendingWithLease(context.Background(), pool, 10, time.Hour)
	if err != nil || len(again) != 3 {
		t.Fatalf("released events must be claimable immediately: %d %v", len(again), err)
	}
}
