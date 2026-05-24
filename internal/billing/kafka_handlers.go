package billing

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	commonkafka "vpn-platform/internal/common/kafka"
	"vpn-platform/internal/common/outbox"
	kafkacontracts "vpn-platform/internal/contracts/kafka"

	kafkago "github.com/segmentio/kafka-go"
)

func RunCommandConsumer(ctx context.Context, reader *kafkago.Reader, service *Service) error {
	return commonkafka.RunConsumerWithDLT(
		ctx,
		reader,
		"billing-service",
		func(ctx context.Context, msg commonkafka.Message) error {
			var envelope struct {
				Type      string `json:"type"`
				CommandID string `json:"command_id"`
				PaymentID string `json:"payment_id"`
				OrderID   string `json:"order_id"`
			}

			if err := json.Unmarshal(msg.Value, &envelope); err != nil {
				return commonkafka.ErrSkip
			}

			return processBillingMessageOnce(ctx, service, msg, envelope.Type, envelope.CommandID, envelope.PaymentID, envelope.OrderID, func(opCtx context.Context) error {
				switch envelope.Type {
				case string(kafkacontracts.BillingCommandCreateSubscriptionCheckout):
					var cmd kafkacontracts.CreateSubscriptionCheckoutCommand
					if err := json.Unmarshal(msg.Value, &cmd); err != nil {
						return commonkafka.ErrSkip
					}
					return service.HandleCreateSubscriptionCheckout(opCtx, &cmd)

				case string(kafkacontracts.BillingCommandBindCard):
					var cmd kafkacontracts.BindCardCommand
					if err := json.Unmarshal(msg.Value, &cmd); err != nil {
						return commonkafka.ErrSkip
					}
					return service.HandleBindCard(opCtx, &cmd)

				case string(kafkacontracts.BillingCommandUnbindCard):
					var cmd kafkacontracts.UnbindCardCommand
					if err := json.Unmarshal(msg.Value, &cmd); err != nil {
						return commonkafka.ErrSkip
					}
					return service.HandleUnbindCard(opCtx, &cmd)

				case string(kafkacontracts.BillingCommandDisableAutoRenew):
					var cmd kafkacontracts.DisableAutoRenewCommand
					if err := json.Unmarshal(msg.Value, &cmd); err != nil {
						return commonkafka.ErrSkip
					}
					return service.HandleDisableAutoRenew(opCtx, &cmd)

				default:
					return commonkafka.ErrSkip
				}
			})
		},
		service.producer,
		commonkafka.TopicBillingCommandsDLT,
	)
}

func RunSubscriptionEventsConsumer(ctx context.Context, reader *kafkago.Reader, service *Service) error {
	return commonkafka.RunConsumerWithDLT(
		ctx,
		reader,
		"billing-service",
		func(ctx context.Context, msg commonkafka.Message) error {
			var envelope struct {
				Type      string `json:"type"`
				CommandID string `json:"command_id"`
				PaymentID string `json:"payment_id"`
				OrderID   string `json:"order_id"`
			}

			if err := json.Unmarshal(msg.Value, &envelope); err != nil {
				return commonkafka.ErrSkip
			}

			if envelope.Type != string(kafkacontracts.SubscriptionEventActivated) {
				return nil
			}

			return processBillingMessageOnce(ctx, service, msg, envelope.Type, envelope.CommandID, envelope.PaymentID, envelope.OrderID, func(opCtx context.Context) error {
				var event kafkacontracts.SubscriptionActivatedEvent
				if err := json.Unmarshal(msg.Value, &event); err != nil {
					return commonkafka.ErrSkip
				}

				if event.TelegramID == 0 {
					return fmt.Errorf("subscription activated event has empty telegram_id")
				}

				return service.HandleSubscriptionActivated(opCtx, &event)
			})
		},
		service.producer,
		commonkafka.TopicSubscriptionEventsDLT,
	)
}

func processBillingMessageOnce(
	ctx context.Context,
	service *Service,
	msg commonkafka.Message,
	eventType string,
	commandID string,
	paymentID string,
	orderID string,
	handle func(opCtx context.Context) error,
) error {
	messageID := commandID
	if messageID == "" && paymentID != "" {
		messageID = paymentID + ":" + eventType
	}
	if messageID == "" && orderID != "" {
		messageID = orderID + ":" + eventType
	}
	if messageID == "" {
		messageID = msg.Topic + ":" + string(msg.Key) + ":" + fmt.Sprint(msg.Partition) + ":" + fmt.Sprint(msg.Offset)
	}

	tx, err := service.repo.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback(ctx) }()

	inserted, err := outbox.MarkProcessed(ctx, tx, "billing-service", messageID, eventType)
	if err != nil {
		return err
	}
	if !inserted {
		return tx.Commit(ctx)
	}

	opCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()

	if err := handle(opCtx); err != nil {
		return err
	}

	return tx.Commit(ctx)
}
