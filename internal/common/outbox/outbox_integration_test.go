//go:build integration

package outbox

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/testcontainers/testcontainers-go"
	tcpostgres "github.com/testcontainers/testcontainers-go/modules/postgres"
	"github.com/testcontainers/testcontainers-go/wait"
)

// ============================================================================
// Интеграционные тесты transactional outbox / inbox на реальном PostgreSQL.
//
// Запуск:  go test -tags=integration ./internal/common/outbox/...
// Требует запущенного Docker.
//
// Это ядро гарантий доставки: outbox обеспечивает «не потеряем», inbox —
// «не применим дважды». Проверять их моками бессмысленно, потому что вся
// суть в поведении транзакций и блокировок самой БД.
// ============================================================================

// setupPostgres поднимает контейнер и накатывает платформенные миграции.
func setupPostgres(t *testing.T) *pgxpool.Pool {
	t.Helper()
	ctx := context.Background()

	container, err := tcpostgres.Run(ctx,
		"postgres:16-alpine",
		tcpostgres.WithDatabase("vpn_test"),
		tcpostgres.WithUsername("vpn"),
		tcpostgres.WithPassword("vpn"),
		testcontainers.WithWaitStrategy(
			wait.ForLog("database system is ready to accept connections").
				WithOccurrence(2).
				WithStartupTimeout(60*time.Second),
		),
	)
	if err != nil {
		t.Fatalf("не удалось поднять postgres: %v", err)
	}
	t.Cleanup(func() {
		if err := container.Terminate(context.Background()); err != nil {
			t.Logf("не удалось остановить контейнер: %v", err)
		}
	})

	dsn, err := container.ConnectionString(ctx, "sslmode=disable")
	if err != nil {
		t.Fatalf("не удалось получить DSN: %v", err)
	}

	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		t.Fatalf("не удалось подключиться: %v", err)
	}
	t.Cleanup(pool.Close)

	applyMigrations(t, pool, filepath.Join("..", "..", "..", "migrations", "platform"))
	return pool
}

// applyMigrations накатывает .sql-файлы каталога по возрастанию имени.
func applyMigrations(t *testing.T, pool *pgxpool.Pool, dir string) {
	t.Helper()
	ctx := context.Background()

	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatalf("не удалось прочитать каталог миграций %s: %v", dir, err)
	}

	names := make([]string, 0, len(entries))
	for _, e := range entries {
		if !e.IsDir() && filepath.Ext(e.Name()) == ".sql" {
			names = append(names, e.Name())
		}
	}
	// os.ReadDir уже отдаёт отсортированный список — порядок миграций сохранён.

	for _, name := range names {
		body, err := os.ReadFile(filepath.Join(dir, name))
		if err != nil {
			t.Fatalf("не удалось прочитать миграцию %s: %v", name, err)
		}
		if _, err := pool.Exec(ctx, string(body)); err != nil {
			t.Fatalf("миграция %s не применилась: %v", name, err)
		}
	}
}

// --------------------------------------------------------------------------

// Событие и бизнес-данные пишутся в ОДНОЙ транзакции: откат должен убирать оба.
func TestAddTxIsAtomicWithBusinessData(t *testing.T) {
	pool := setupPostgres(t)
	ctx := context.Background()

	if _, err := pool.Exec(ctx, `CREATE TABLE test_orders (id BIGSERIAL PRIMARY KEY, note TEXT)`); err != nil {
		t.Fatalf("не удалось создать тестовую таблицу: %v", err)
	}

	t.Run("коммит сохраняет и данные, и событие", func(t *testing.T) {
		tx, err := pool.Begin(ctx)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := tx.Exec(ctx, `INSERT INTO test_orders (note) VALUES ('ok')`); err != nil {
			t.Fatal(err)
		}
		if err := AddTx(ctx, tx, Event{
			AggregateType: "order", AggregateID: "1",
			Topic: "test.topic", MessageKey: "1",
			EventType: "order.created", Payload: map[string]string{"note": "ok"},
		}); err != nil {
			t.Fatal(err)
		}
		if err := tx.Commit(ctx); err != nil {
			t.Fatal(err)
		}

		var events int
		if err := pool.QueryRow(ctx, `SELECT count(*) FROM event_outbox WHERE aggregate_id='1'`).Scan(&events); err != nil {
			t.Fatal(err)
		}
		if events != 1 {
			t.Errorf("событий в outbox = %d, ожидалось 1", events)
		}
	})

	t.Run("откат не оставляет ни данных, ни события", func(t *testing.T) {
		tx, err := pool.Begin(ctx)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := tx.Exec(ctx, `INSERT INTO test_orders (note) VALUES ('rollback')`); err != nil {
			t.Fatal(err)
		}
		if err := AddTx(ctx, tx, Event{
			AggregateType: "order", AggregateID: "2",
			Topic: "test.topic", MessageKey: "2",
			EventType: "order.created", Payload: map[string]string{"note": "rollback"},
		}); err != nil {
			t.Fatal(err)
		}
		if err := tx.Rollback(ctx); err != nil {
			t.Fatal(err)
		}

		var events, orders int
		if err := pool.QueryRow(ctx, `SELECT count(*) FROM event_outbox WHERE aggregate_id='2'`).Scan(&events); err != nil {
			t.Fatal(err)
		}
		if err := pool.QueryRow(ctx, `SELECT count(*) FROM test_orders WHERE note='rollback'`).Scan(&orders); err != nil {
			t.Fatal(err)
		}
		if events != 0 || orders != 0 {
			t.Errorf("после отката: событий=%d, заказов=%d, ожидалось 0 и 0", events, orders)
		}
	})
}

// Инбокс: повторная доставка одного сообщения должна дать ровно один эффект.
func TestMarkProcessedIsIdempotent(t *testing.T) {
	pool := setupPostgres(t)
	ctx := context.Background()

	const (
		source    = "test-service"
		messageID = "msg-42"
		msgType   = "payment.succeeded"
	)

	first := markProcessedInTx(t, pool, source, messageID, msgType)
	if !first {
		t.Fatal("первая доставка должна вернуть inserted=true")
	}

	second := markProcessedInTx(t, pool, source, messageID, msgType)
	if second {
		t.Error("повторная доставка вернула inserted=true — побочный эффект применится дважды")
	}

	var rows int
	if err := pool.QueryRow(ctx,
		`SELECT count(*) FROM processed_messages WHERE message_id = $1`, source+":"+messageID).Scan(&rows); err != nil {
		t.Fatal(err)
	}
	if rows != 1 {
		t.Errorf("записей в processed_messages = %d, ожидалась 1", rows)
	}
}

// Один и тот же message_id от РАЗНЫХ сервисов — разные записи:
// каждый сервис обрабатывает сообщение независимо.
func TestMarkProcessedIsolatesSources(t *testing.T) {
	pool := setupPostgres(t)

	if !markProcessedInTx(t, pool, "service-a", "shared-id", "evt") {
		t.Fatal("service-a: первая доставка должна пройти")
	}
	if !markProcessedInTx(t, pool, "service-b", "shared-id", "evt") {
		t.Error("service-b: доставка должна пройти независимо от service-a")
	}
}

func markProcessedInTx(t *testing.T, pool *pgxpool.Pool, source, messageID, msgType string) bool {
	t.Helper()
	ctx := context.Background()

	tx, err := pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	inserted, err := MarkProcessed(ctx, tx, source, messageID, msgType)
	if err != nil {
		t.Fatal(err)
	}
	if err := tx.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	return inserted
}

func TestMarkProcessedValidatesInput(t *testing.T) {
	pool := setupPostgres(t)
	ctx := context.Background()

	tx, err := pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	if _, err := MarkProcessed(ctx, tx, "", "id", "type"); err == nil {
		t.Error("пустой source должен давать ошибку")
	}
	if _, err := MarkProcessed(ctx, tx, "src", "", "type"); err == nil {
		t.Error("пустой messageID должен давать ошибку")
	}
}

// Ключевая проверка FOR UPDATE SKIP LOCKED: несколько воркеров разбирают
// очередь параллельно и каждое событие достаётся ровно одному.
func TestLockPendingNoDoubleProcessing(t *testing.T) {
	pool := setupPostgres(t)
	ctx := context.Background()

	const total = 40
	tx, err := pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < total; i++ {
		if err := AddTx(ctx, tx, Event{
			AggregateType: "test", AggregateID: fmt.Sprintf("%d", i),
			Topic: "test.topic", MessageKey: fmt.Sprintf("key-%d", i),
			EventType: "test.event", Payload: map[string]int{"n": i},
		}); err != nil {
			t.Fatal(err)
		}
	}
	if err := tx.Commit(ctx); err != nil {
		t.Fatal(err)
	}

	const workers = 4
	var (
		mu      sync.Mutex
		claimed = make(map[int64]int)
		wg      sync.WaitGroup
	)

	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				events, err := LockPending(ctx, pool, 5)
				if err != nil {
					t.Errorf("LockPending: %v", err)
					return
				}
				if len(events) == 0 {
					return
				}
				for _, e := range events {
					mu.Lock()
					claimed[e.ID]++
					mu.Unlock()
					if err := MarkPublished(ctx, pool, e.ID); err != nil {
						t.Errorf("MarkPublished: %v", err)
					}
				}
			}
		}()
	}
	wg.Wait()

	if len(claimed) != total {
		t.Errorf("обработано уникальных событий %d, ожидалось %d", len(claimed), total)
	}
	for id, times := range claimed {
		if times != 1 {
			t.Errorf("событие %d захвачено %d раз — SKIP LOCKED не сработал", id, times)
		}
	}

	var pending int
	if err := pool.QueryRow(ctx,
		`SELECT count(*) FROM event_outbox WHERE published_at IS NULL`).Scan(&pending); err != nil {
		t.Fatal(err)
	}
	if pending != 0 {
		t.Errorf("осталось неопубликованных событий: %d", pending)
	}
}

// Неудачная публикация возвращает событие в очередь с ростом счётчика попыток.
func TestMarkRetryIncrementsAttempts(t *testing.T) {
	pool := setupPostgres(t)
	ctx := context.Background()

	tx, err := pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if err := AddTx(ctx, tx, Event{
		AggregateType: "test", AggregateID: "retry",
		Topic: "test.topic", MessageKey: "retry",
		EventType: "test.event", Payload: map[string]string{"a": "b"},
	}); err != nil {
		t.Fatal(err)
	}
	if err := tx.Commit(ctx); err != nil {
		t.Fatal(err)
	}

	events, err := LockPending(ctx, pool, 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(events) != 1 {
		t.Fatalf("получено событий %d, ожидалось 1", len(events))
	}

	if err := MarkRetry(ctx, pool, events[0].ID, "kafka недоступна"); err != nil {
		t.Fatal(err)
	}

	var attempts int
	var published *time.Time
	if err := pool.QueryRow(ctx,
		`SELECT attempts, published_at FROM event_outbox WHERE id = $1`, events[0].ID,
	).Scan(&attempts, &published); err != nil {
		t.Fatal(err)
	}
	if attempts < 1 {
		t.Errorf("attempts = %d, ожидалось не меньше 1", attempts)
	}
	if published != nil {
		t.Error("событие не должно быть помечено опубликованным после неудачи")
	}
}
