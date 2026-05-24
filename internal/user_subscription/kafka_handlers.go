package user_subscription

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

func RunSubscriptionCommandsConsumer(ctx context.Context, reader *kafkago.Reader, svc *Service) error {
	return commonkafka.RunConsumerWithDLT(
		ctx,
		reader,
		"user-subscription-service",
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

			return processSubscriptionMessageOnce(ctx, svc, msg, envelope.Type, envelope.CommandID, envelope.PaymentID, envelope.OrderID, func(opCtx context.Context) error {
				switch envelope.Type {
				case string(kafkacontracts.SubscriptionCommandStartTrial):
					var cmd kafkacontracts.StartTrialCommand
					if err := json.Unmarshal(msg.Value, &cmd); err != nil {
						return commonkafka.ErrSkip
					}
					return svc.HandleStartTrial(opCtx, &cmd)

				case string(kafkacontracts.SubscriptionCommandGetStatus):
					var cmd kafkacontracts.GetStatusCommand
					if err := json.Unmarshal(msg.Value, &cmd); err != nil {
						return commonkafka.ErrSkip
					}
					return svc.HandleGetStatus(opCtx, &cmd)

				case string(kafkacontracts.SubscriptionCommandGetLinks):
					var cmd kafkacontracts.GetLinksCommand
					if err := json.Unmarshal(msg.Value, &cmd); err != nil {
						return commonkafka.ErrSkip
					}
					return svc.HandleGetLinks(opCtx, &cmd)

				case string(kafkacontracts.SubscriptionCommandCancel):
					var cmd kafkacontracts.CancelSubscriptionCommand
					if err := json.Unmarshal(msg.Value, &cmd); err != nil {
						return commonkafka.ErrSkip
					}
					return svc.HandleCancel(opCtx, &cmd)

				default:
					return commonkafka.ErrSkip
				}
			})
		},
		svc.producer,
		commonkafka.TopicSubscriptionCommandsDLT,
	)
}

func RunBillingEventsConsumer(ctx context.Context, reader *kafkago.Reader, svc *Service) error {
	return commonkafka.RunConsumerWithDLT(
		ctx,
		reader,
		"user-subscription-service",
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

			return processSubscriptionMessageOnce(ctx, svc, msg, envelope.Type, envelope.CommandID, envelope.PaymentID, envelope.OrderID, func(opCtx context.Context) error {
				switch envelope.Type {
				case string(kafkacontracts.BillingEventPaymentSucceeded):
					var event kafkacontracts.BillingPaymentSucceededEvent
					if err := json.Unmarshal(msg.Value, &event); err != nil {
						return commonkafka.ErrSkip
					}
					return svc.HandleBillingPaymentSucceeded(opCtx, &event)

				case string(kafkacontracts.BillingEventPaymentMethodGone):
					var event kafkacontracts.BillingPaymentMethodUnboundEvent
					if err := json.Unmarshal(msg.Value, &event); err != nil {
						return commonkafka.ErrSkip
					}
					return svc.HandleBillingPaymentMethodUnbound(opCtx, &event)

				case string(kafkacontracts.BillingEventAutoRenewDisabled):
					var event kafkacontracts.BillingAutoRenewDisabledEvent
					if err := json.Unmarshal(msg.Value, &event); err != nil {
						return commonkafka.ErrSkip
					}
					return svc.HandleBillingAutoRenewDisabled(opCtx, &event)

				case string(kafkacontracts.BillingEventGraceStarted):
					var event kafkacontracts.BillingGraceStartedEvent
					if err := json.Unmarshal(msg.Value, &event); err != nil {
						return commonkafka.ErrSkip
					}
					return svc.HandleBillingGraceStarted(opCtx, &event)

				case string(kafkacontracts.BillingEventAccessExpired):
					var event kafkacontracts.BillingAccessExpiredEvent
					if err := json.Unmarshal(msg.Value, &event); err != nil {
						return commonkafka.ErrSkip
					}
					return svc.HandleBillingAccessExpired(opCtx, &event)

				default:
					return nil
				}
			})
		},
		svc.producer,
		commonkafka.TopicBillingEventsDLT,
	)
}

func processSubscriptionMessageOnce(
	ctx context.Context,
	svc *Service,
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

	tx, err := svc.repo.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback(ctx) }()

	inserted, err := outbox.MarkProcessed(ctx, tx, "user-subscription-service", messageID, eventType)
	if err != nil {
		return err
	}
	if !inserted {
		return tx.Commit(ctx)
	}

	opCtx, cancel := context.WithTimeout(ctx, 20*time.Second)
	defer cancel()

	if err := handle(opCtx); err != nil {
		return err
	}

	return tx.Commit(ctx)
}
