package crypto_billing

import (
	"context"
	"encoding/json"
	"log/slog"

	commonkafka "vpn-platform/internal/common/kafka"
	kafkacontracts "vpn-platform/internal/contracts/kafka"

	kafkago "github.com/segmentio/kafka-go"
)

// RunCommandConsumer запускает consumer для топика crypto.commands.
// Использует общий RunConsumerWithDLT, который сам делает manual commit, retry
// с экспоненциальным backoff и публикацию в DLT после 20 попыток.
func RunCommandConsumer(ctx context.Context, reader *kafkago.Reader, service *Service) error {
	if reader == nil {
		return nil
	}
	slog.Info("crypto-billing starting crypto.commands consumer")

	return commonkafka.RunConsumerWithDLT(
		ctx,
		reader,
		"crypto-billing-service",
		func(ctx context.Context, msg commonkafka.Message) error {
			// Сначала минимальный envelope — только тип, чтобы решить, какую структуру
			// парсить дальше. Это позволяет добавлять новые типы команд без изменения консьюмера.
			var envelope struct {
				Type      string `json:"type"`
				CommandID string `json:"command_id"`
			}
			if err := json.Unmarshal(msg.Value, &envelope); err != nil {
				slog.Warn("crypto-billing invalid envelope", "err", err)
				// ErrSkip отправит сообщение в DLT и закоммитит offset — это сигнал
				// "сообщение битое, не повторять", а не "временная ошибка".
				return commonkafka.ErrSkip
			}

			switch envelope.Type {
			case string(kafkacontracts.CryptoCommandCreateCheckout):
				var cmd kafkacontracts.CreateCryptoCheckoutCommand
				if err := json.Unmarshal(msg.Value, &cmd); err != nil {
					slog.Warn("crypto-billing invalid create_checkout", "err", err)
					return commonkafka.ErrSkip
				}
				return service.HandleCreateCheckout(ctx, &cmd)

			default:
				// Неизвестный тип — просто пропускаем, не флудим DLT.
				slog.Debug("crypto-billing ignored command type", "type", envelope.Type)
				return nil
			}
		},
		service.producer,
		commonkafka.TopicCryptoCommandsDLT,
	)
}
