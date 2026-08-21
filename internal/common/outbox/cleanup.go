package outbox

import (
	"context"
	"log/slog"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

// RunCleanup периодически удаляет опубликованные события outbox и старые записи
// идемпотентности (processed_messages). Без этого таблицы растут бесконечно,
// раздувая индексы и замедляя vacuum (S9/S11).
//
// Безопасность: удаляем только status='published' старше outbox-TTL и
// processed_messages старше inbox-TTL. Inbox-TTL должен быть БОЛЬШЕ Kafka
// retention — иначе теоретически возможна повторная доставка сообщения, чья
// запись идемпотентности уже удалена. По умолчанию 30 дней (Kafka retention
// обычно 7 дней), что безопасно.
//
// Блокирующая: вызывать в горутине. Достаточно запускать в ОДНОМ сервисе
// (например vpn-orchestrator), т.к. таблицы общие. Если запустить в нескольких —
// тоже безопасно (DELETE идемпотентен), просто лишняя работа.
func RunCleanup(ctx context.Context, pool *pgxpool.Pool) {
	if pool == nil {
		return
	}

	interval := envDurationCleanup("OUTBOX_CLEANUP_INTERVAL", time.Hour)
	outboxTTL := envDurationCleanup("OUTBOX_PUBLISHED_TTL", 7*24*time.Hour)
	inboxTTL := envDurationCleanup("INBOX_PROCESSED_TTL", 30*24*time.Hour)
	syncTTL := envDurationCleanup("NODE_SYNC_RESULTS_TTL", 14*24*time.Hour)

	// первый прогон сразу
	cleanupOnce(ctx, pool, outboxTTL, inboxTTL, syncTTL)

	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			cleanupOnce(ctx, pool, outboxTTL, inboxTTL, syncTTL)
		}
	}
}

func cleanupOnce(ctx context.Context, pool *pgxpool.Pool, outboxTTL, inboxTTL, syncTTL time.Duration) {
	cctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()

	ct, err := pool.Exec(cctx, `
		DELETE FROM event_outbox
		WHERE status = 'published' AND published_at < now() - $1::interval
	`, intervalString(outboxTTL))
	if err != nil {
		slog.Error("outbox cleanup failed", "err", err)
	} else if n := ct.RowsAffected(); n > 0 {
		slog.Info("outbox cleanup done", "deleted_published", n)
	}

	ct, err = pool.Exec(cctx, `
		DELETE FROM processed_messages
		WHERE processed_at < now() - $1::interval
	`, intervalString(inboxTTL))
	if err != nil {
		slog.Error("inbox cleanup failed", "err", err)
	} else if n := ct.RowsAffected(); n > 0 {
		slog.Info("inbox cleanup done", "deleted_processed", n)
	}

	// P2: чистим залежавшиеся счётчики попыток (старше inbox-TTL).
	ct, err = pool.Exec(cctx, `
		DELETE FROM consumer_attempts
		WHERE updated_at < now() - $1::interval
	`, intervalString(inboxTTL))
	if err != nil {
		slog.Error("consumer_attempts cleanup failed", "err", err)
	} else if n := ct.RowsAffected(); n > 0 {
		slog.Info("consumer_attempts cleanup done", "deleted_attempts", n)
	}

	// Журнал результатов синхронизации узлов. Растёт на несколько тысяч строк
	// в сутки и до сих пор не чистился ничем: нужен только для диагностики
	// последних дней. Таблица может отсутствовать (общий пакет используют и
	// сервисы без неё) — тогда шаг молча пропускается.
	if syncTTL > 0 {
		ct, err = pool.Exec(cctx, `
			DELETE FROM vpn_node_sync_results
			WHERE created_at < now() - $1::interval
		`, intervalString(syncTTL))
		if err != nil {
			if !strings.Contains(err.Error(), "does not exist") {
				slog.Error("node sync results cleanup failed", "err", err)
			}
		} else if n := ct.RowsAffected(); n > 0 {
			slog.Info("node sync results cleanup done", "deleted_sync_results", n)
		}
	}
}

// intervalString переводит duration в строку для Postgres interval ('3600 seconds').
func intervalString(d time.Duration) string {
	return strconv.FormatInt(int64(d.Seconds()), 10) + " seconds"
}

func envDurationCleanup(key string, fallback time.Duration) time.Duration {
	v := strings.TrimSpace(os.Getenv(key))
	if v == "" {
		return fallback
	}
	d, err := time.ParseDuration(v)
	if err != nil || d <= 0 {
		return fallback
	}
	return d
}
