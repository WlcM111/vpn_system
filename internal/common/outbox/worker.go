package outbox

import (
	"context"
	"log/slog"
	"os"
	"strconv"
	"strings"
	"time"

	commonkafka "vpn-platform/internal/common/kafka"

	"github.com/jackc/pgx/v5/pgxpool"
)

// outboxBatchSize — размер батча публикации за один проход. Настраивается через
// env OUTBOX_BATCH_SIZE (S11). Больше батч — выше пропускная способность под
// нагрузкой, но дольше один проход.
func outboxBatchSize() int {
	v := strings.TrimSpace(os.Getenv("OUTBOX_BATCH_SIZE"))
	if v == "" {
		return 50
	}
	n, err := strconv.Atoi(v)
	if err != nil || n <= 0 {
		return 50
	}
	return n
}

func RunPublisher(ctx context.Context, pool *pgxpool.Pool, producer *commonkafka.Producer, serviceName string) {
	if pool == nil || producer == nil {
		return
	}

	batch := outboxBatchSize()
	idleSleep := 200 * time.Millisecond
	for {
		select {
		case <-ctx.Done():
			return
		case <-time.After(idleSleep):
		}

		events, err := LockPending(ctx, pool, batch)
		if err != nil {
			slog.Error("outbox lock pending failed", "service", serviceName, "err", err)
			idleSleep = 2 * time.Second
			continue
		}

		if len(events) == 0 {
			if idleSleep < 2*time.Second {
				idleSleep *= 2
			}
			continue
		}
		idleSleep = 200 * time.Millisecond

		publishBatch(ctx, pool, events, serviceName, func(ctx context.Context, e PendingEvent) error {
			return producer.PublishJSON(ctx, e.Topic, e.MessageKey, jsonRaw(e.Payload))
		})
	}
}

// publishBatch публикует захваченные события по порядку.
//
// Если событие упало, последующие события того же топика и ключа в этом
// проходе не публикуются (порядок событий одного ключа), а возвращаются в
// очередь без расхода попытки и не раньше упавшего (ReleaseBlocked).
//
// Если контекст отменён (остановка сервиса), неопубликованные события
// возвращаются в очередь сразу, а не ждут истечения аренды.
func publishBatch(ctx context.Context, pool *pgxpool.Pool, events []PendingEvent, serviceName string, publish func(context.Context, PendingEvent) error) {
	blocked := make(map[string]time.Time, 8)
	for i, e := range events {
		if ctx.Err() != nil {
			releaseOnShutdown(pool, events[i:], serviceName)
			return
		}
		chain := e.Topic + "\x00" + e.MessageKey
		if notBefore, ok := blocked[chain]; ok {
			if err := ReleaseBlocked(ctx, pool, e.ID, notBefore, "blocked by earlier failed event with same key"); err != nil {
				slog.Warn("outbox release blocked event failed", "service", serviceName, "id", e.ID, "err", err)
			}
			continue
		}
		if err := publish(ctx, e); err != nil {
			if ctx.Err() != nil {
				releaseOnShutdown(pool, events[i:], serviceName)
				return
			}
			status, next, mErr := MarkRetryReturning(ctx, pool, e.ID, err.Error())
			if mErr != nil {
				// Событие останется в 'processing' и будет перезахвачено после аренды.
				slog.Error("outbox mark retry failed", "service", serviceName, "id", e.ID, "err", mErr)
				next = time.Now().Add(DefaultProcessingLease)
			}
			if status == "failed" {
				if sErr := SaveFailedMessage(ctx, pool, serviceName, e.Topic, e.MessageKey, "outbox", e.Payload, err.Error()); sErr != nil {
					slog.Error("outbox save failed message failed", "service", serviceName, "id", e.ID, "err", sErr)
				}
			}
			blocked[chain] = next
			continue
		}
		if err := MarkPublished(ctx, pool, e.ID); err != nil {
			// Опубликовано, но не отмечено: после аренды событие уйдёт
			// повторно — получатели идемпотентны.
			slog.Error("outbox mark published failed", "service", serviceName, "id", e.ID, "err", err)
		}
	}
}

// releaseOnShutdown возвращает неопубликованные события в очередь при
// остановке сервиса (с собственным коротким таймаутом).
func releaseOnShutdown(pool *pgxpool.Pool, events []PendingEvent, serviceName string) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	for _, e := range events {
		if err := ReleaseBlocked(ctx, pool, e.ID, time.Now(), "publisher stopped"); err != nil {
			slog.Warn("outbox release on shutdown failed; lease will expire", "service", serviceName, "id", e.ID, "err", err)
			return
		}
	}
}

type jsonRaw []byte

func (j jsonRaw) MarshalJSON() ([]byte, error) { return j, nil }
