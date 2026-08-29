package kafka

import (
	"context"
	"errors"
	"log/slog"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	commonmetrics "vpn-platform/internal/common/metrics"

	kafkago "github.com/segmentio/kafka-go"
)

type Message = kafkago.Message

type Handler func(ctx context.Context, msg Message) error

var ErrSkip = errors.New("kafka: skip message")

// envInt читает целочисленную env-переменную с фолбэком.
func envInt(key string, fallback int) int {
	v := strings.TrimSpace(os.Getenv(key))
	if v == "" {
		return fallback
	}
	n, err := strconv.Atoi(v)
	if err != nil || n <= 0 {
		return fallback
	}
	return n
}

func NewReader(brokers []string, topic, groupID string) *kafkago.Reader {
	if len(brokers) == 0 {
		slog.Warn("kafka consumer disabled: no brokers provided", "topic", topic, "group_id", groupID)
		return nil
	}

	// S11: размеры фетча настраиваются через env. Под высокой нагрузкой
	// увеличение MinBytes делает чтение батчевее (меньше round-trip'ов к брокеру).
	minBytes := envInt("KAFKA_FETCH_MIN_BYTES", 1)
	maxBytes := envInt("KAFKA_FETCH_MAX_BYTES", 10e6)
	maxWaitMs := envInt("KAFKA_FETCH_MAX_WAIT_MS", 500)

	return kafkago.NewReader(kafkago.ReaderConfig{
		Brokers:        brokers,
		Topic:          topic,
		GroupID:        groupID,
		MinBytes:       minBytes,
		MaxBytes:       maxBytes,
		MaxWait:        time.Duration(maxWaitMs) * time.Millisecond,
		CommitInterval: 0,
		StartOffset:    kafkago.FirstOffset,
	})
}

// RunConsumerPool запускает N горутин-воркеров, каждая со своим Reader в одной
// consumer group. Kafka автоматически распределит партиции топика между
// воркерами (и между репликами сервиса). Это горизонтальный параллелизм
// обработки в пределах одного процесса (S1).
//
//	newReader — фабрика Reader'ов (каждый воркер получает свой, т.к. Reader
//	            не предназначен для конкурентного использования).
//	workers   — число воркеров. Имеет смысл не больше числа партиций топика
//	            (лишние будут простаивать). При workers<=1 запускается один воркер.
//
// Блокирующая: возвращает управление, когда все воркеры завершились (отмена ctx).
func RunConsumerPool(
	ctx context.Context,
	newReader func() *kafkago.Reader,
	workers int,
	serviceName string,
	handle Handler,
	dltProducer *Producer,
	dltTopic string,
) error {
	if workers <= 0 {
		workers = 1
	}
	var wg sync.WaitGroup
	for i := 0; i < workers; i++ {
		reader := newReader()
		if reader == nil {
			continue
		}
		wg.Add(1)
		go func(rd *kafkago.Reader) {
			defer wg.Done()
			defer func() { _ = rd.Close() }()
			if err := RunConsumerWithDLT(ctx, rd, serviceName, handle, dltProducer, dltTopic); err != nil {
				slog.Error("kafka consumer worker stopped", "service", serviceName, "err", err)
			}
		}(reader)
	}
	wg.Wait()
	return nil
}

func RunConsumerWithDLT(ctx context.Context, reader *kafkago.Reader, serviceName string, handle Handler, dltProducer *Producer, dltTopic string) error {
	if reader == nil {
		return nil
	}
	if handle == nil {
		return errors.New("nil kafka handler")
	}

	for {
		msg, err := reader.FetchMessage(ctx)
		if err != nil {
			if errors.Is(err, context.Canceled) || errors.Is(ctx.Err(), context.Canceled) {
				return nil
			}
			// Транзиентная ошибка fetch (брокер недоступен, потеря координатора
			// группы, rebalance после рестарта центра). НЕ убиваем цикл — kafka-go
			// сам переустановит соединение при следующем FetchMessage. Небольшая
			// пауза, чтобы не крутить busy-loop, и продолжаем.
			slog.Warn("kafka fetch failed, will retry",
				"service", serviceName, "err", err)
			select {
			case <-ctx.Done():
				return nil
			case <-time.After(2 * time.Second):
			}
			continue
		}

		attemptKey := msg.Topic + ":" + string(msg.Key) + ":" + time.Unix(0, msg.Time.UnixNano()).UTC().Format(time.RFC3339Nano)

		err = handle(ctx, msg)
		if err != nil {
			if errors.Is(err, ErrSkip) {
				commonmetrics.KafkaConsumedTotal.WithLabelValues(msg.Topic, "skip").Inc()
				_ = publishDLT(ctx, dltProducer, dltTopic, msg, serviceName, err)
				attemptStore.Reset(ctx, attemptKey)
				if commitErr := reader.CommitMessages(ctx, msg); commitErr != nil {
					return commitErr
				}
				continue
			}

			// P2: персистентный счётчик — переживает рестарты, гарантирует попадание
			// отравленного сообщения в DLT даже при регулярных перезапусках.
			attemptCount := attemptStore.Inc(ctx, attemptKey)
			slog.Error("kafka message handling failed",
				"service", serviceName,
				"topic", msg.Topic,
				"partition", msg.Partition,
				"offset", msg.Offset,
				"key", string(msg.Key),
				"attempt", attemptCount,
				"err", err,
			)

			if attemptCount >= 20 && dltProducer != nil && dltTopic != "" {
				if dltErr := publishDLT(ctx, dltProducer, dltTopic, msg, serviceName, err); dltErr != nil {
					slog.Error("failed to publish message to DLT, keeping offset uncommitted",
						"service", serviceName,
						"topic", msg.Topic,
						"partition", msg.Partition,
						"offset", msg.Offset,
						"key", string(msg.Key),
						"dlt_topic", dltTopic,
						"err", dltErr,
					)
					select {
					case <-ctx.Done():
						return nil
					case <-time.After(5 * time.Second):
					}
					continue
				}
				commonmetrics.KafkaConsumedTotal.WithLabelValues(msg.Topic, "dlt").Inc()
				attemptStore.Reset(ctx, attemptKey)
				if commitErr := reader.CommitMessages(ctx, msg); commitErr != nil {
					return commitErr
				}
				continue
			}

			// Повтор ТОГО ЖЕ сообщения на месте.
			//
			// Раньше здесь была пауза и continue. Это возвращало управление к
			// FetchMessage, который отдаёт СЛЕДУЮЩЕЕ сообщение, а не текущее:
			// упавшее сообщение молча пропускалось, offset не коммитился, и
			// повторная обработка наступала только после рестарта сервиса или
			// ребаланса группы. Для billing.events это означало потерю
			// подтверждённого платежа: деньги списаны, подписка не выдана.
			//
			// Теперь сообщение повторяется в цикле до успеха или до порога DLT.
			// Порядок в партиции сохраняется, потому что мы не идём дальше, пока
			// текущее сообщение не обработано.
			for err != nil && attemptCount < 20 {
				select {
				case <-ctx.Done():
					return nil
				case <-time.After(time.Duration(minInt(attemptCount, 10)) * 500 * time.Millisecond):
				}

				err = handle(ctx, msg)
				if err == nil {
					break
				}
				if errors.Is(err, ErrSkip) {
					break
				}
				attemptCount = attemptStore.Inc(ctx, attemptKey)
				slog.Error("kafka message retry failed",
					"service", serviceName,
					"topic", msg.Topic,
					"partition", msg.Partition,
					"offset", msg.Offset,
					"key", string(msg.Key),
					"attempt", attemptCount,
					"err", err,
				)
			}

			if err != nil {
				// Ретраи исчерпаны либо handler попросил пропустить сообщение.
				//
				// Коммитим ТОЛЬКО если сообщение реально легло в DLT. Раньше
				// ошибка публикации проглатывалась через `_ =`, а коммит шёл
				// в любом случае: сообщение исчезало бесследно, ни в топике,
				// ни в DLT. Для billing.events это означало потерю
				// подтверждённого платежа — деньги списаны, подписка не выдана,
				// и следа не осталось нигде.
				if dltProducer != nil && dltTopic != "" {
					if dltErr := publishDLT(ctx, dltProducer, dltTopic, msg, serviceName, err); dltErr != nil {
						slog.Error("failed to publish message to DLT, keeping offset uncommitted",
							"service", serviceName,
							"topic", msg.Topic,
							"partition", msg.Partition,
							"offset", msg.Offset,
							"key", string(msg.Key),
							"dlt_topic", dltTopic,
							"err", dltErr,
						)
						select {
						case <-ctx.Done():
							return nil
						case <-time.After(5 * time.Second):
						}
						continue
					}
					commonmetrics.KafkaConsumedTotal.WithLabelValues(msg.Topic, "dlt").Inc()
				}
				attemptStore.Reset(ctx, attemptKey)
				if commitErr := reader.CommitMessages(ctx, msg); commitErr != nil {
					return commitErr
				}
				continue
			}

			// Ретрай удался — фиксируем как обычную успешную обработку.
			attemptStore.Reset(ctx, attemptKey)
			commonmetrics.KafkaConsumedTotal.WithLabelValues(msg.Topic, "ok").Inc()
			if commitErr := reader.CommitMessages(ctx, msg); commitErr != nil {
				return commitErr
			}
			continue
		}

		attemptStore.Reset(ctx, attemptKey)
		commonmetrics.KafkaConsumedTotal.WithLabelValues(msg.Topic, "ok").Inc()
		if err := reader.CommitMessages(ctx, msg); err != nil {
			return err
		}
	}
}

func publishDLT(ctx context.Context, producer *Producer, topic string, msg Message, serviceName string, cause error) error {
	if producer == nil || topic == "" {
		return nil
	}
	payload := map[string]any{
		"service":    serviceName,
		"topic":      msg.Topic,
		"partition":  msg.Partition,
		"offset":     msg.Offset,
		"key":        string(msg.Key),
		"value":      string(msg.Value),
		"error":      cause.Error(),
		"created_at": time.Now().UTC(),
	}
	return producer.PublishJSON(ctx, topic, string(msg.Key), payload)
}

func minInt(a, b int) int {
	if a < b {
		return a
	}
	return b
}
