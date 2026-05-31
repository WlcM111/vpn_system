package outbox

import (
	"context"
	"log/slog"
	"time"

	commonkafka "vpn-platform/internal/common/kafka"

	"github.com/jackc/pgx/v5/pgxpool"
)

func RunPublisher(ctx context.Context, pool *pgxpool.Pool, producer *commonkafka.Producer, serviceName string) {
	if pool == nil || producer == nil {
		return
	}

	idleSleep := 200 * time.Millisecond
	for {
		select {
		case <-ctx.Done():
			return
		case <-time.After(idleSleep):
		}

		events, err := LockPending(ctx, pool, 50)
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

		// blockedKeys: если событие с данным message_key упало, мы НЕ публикуем
		// последующие события с тем же ключом в этом проходе — иначе нарушится
		// порядок событий одного пользователя (events keyed by telegram_id).
		// Они подхватятся в следующем цикле, после успешного ретрая упавшего.
		blockedKeys := make(map[string]struct{}, 8)
		for _, e := range events {
			if _, blocked := blockedKeys[e.MessageKey]; blocked {
				// откатываем статус с 'processing' обратно в 'pending', чтобы взять в следующий раз
				_ = MarkRetry(ctx, pool, e.ID, "blocked by earlier failed event with same key")
				continue
			}
			if err := producer.PublishJSON(ctx, e.Topic, e.MessageKey, jsonRaw(e.Payload)); err != nil {
				_ = MarkRetry(ctx, pool, e.ID, err.Error())
				if e.Attempts >= 20 {
					_ = SaveFailedMessage(ctx, pool, serviceName, e.Topic, e.MessageKey, "outbox", e.Payload, err.Error())
				}
				blockedKeys[e.MessageKey] = struct{}{}
				continue
			}
			_ = MarkPublished(ctx, pool, e.ID)
		}
	}
}

type jsonRaw []byte

func (j jsonRaw) MarshalJSON() ([]byte, error) { return j, nil }
