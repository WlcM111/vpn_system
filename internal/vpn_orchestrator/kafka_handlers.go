package vpn_orchestrator

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"time"

	commonkafka "vpn-platform/internal/common/kafka"
	"vpn-platform/internal/common/outbox"
	kafkacontracts "vpn-platform/internal/contracts/kafka"

	kafkago "github.com/segmentio/kafka-go"
)

func RunSubscriptionEventsConsumer(ctx context.Context, reader *kafkago.Reader, service *Service) error {
	if reader == nil {
		return nil
	}
	slog.Info("vpn-orchestrator starting subscription.events consumer")

	return commonkafka.RunConsumerWithDLT(
		ctx,
		reader,
		"vpn-orchestrator-service",
		func(ctx context.Context, msg commonkafka.Message) error {
			var envelope struct {
				Type      string `json:"type"`
				CommandID string `json:"command_id"`
				PaymentID string `json:"payment_id"`
				OrderID   string `json:"order_id"`
			}
			if err := json.Unmarshal(msg.Value, &envelope); err != nil {
				slog.Warn("vpn-orchestrator invalid event envelope", "err", err)
				return commonkafka.ErrSkip
			}

			return processOrchestratorMessageOnce(ctx, service, msg, envelope.Type, envelope.CommandID, envelope.PaymentID, envelope.OrderID, func(opCtx context.Context) error {
				switch envelope.Type {
				case string(kafkacontracts.SubscriptionEventTrialStarted):
					var event kafkacontracts.SubscriptionTrialStartedEvent
					if err := json.Unmarshal(msg.Value, &event); err != nil {
						slog.Warn("vpn-orchestrator invalid trial_started event", "err", err)
						return commonkafka.ErrSkip
					}
					return service.ApplyTrialStarted(opCtx, &event)

				case string(kafkacontracts.SubscriptionEventActivated):
					var event kafkacontracts.SubscriptionActivatedEvent
					if err := json.Unmarshal(msg.Value, &event); err != nil {
						slog.Warn("vpn-orchestrator invalid activated event", "err", err)
						return commonkafka.ErrSkip
					}
					return service.ApplyActivated(opCtx, &event)

				case string(kafkacontracts.SubscriptionEventCanceled):
					var event kafkacontracts.SubscriptionCanceledEvent
					if err := json.Unmarshal(msg.Value, &event); err != nil {
						slog.Warn("vpn-orchestrator invalid canceled event", "err", err)
						return commonkafka.ErrSkip
					}
					return service.ApplyCanceled(opCtx, &event)

				case string(kafkacontracts.SubscriptionEventGraceStarted):
					var event kafkacontracts.SubscriptionGraceStartedEvent
					if err := json.Unmarshal(msg.Value, &event); err != nil {
						slog.Warn("vpn-orchestrator invalid grace_started event", "err", err)
						return commonkafka.ErrSkip
					}
					return service.ApplyGraceStarted(opCtx, &event)

				case string(kafkacontracts.SubscriptionEventSuspended):
					var event kafkacontracts.SubscriptionSuspendedEvent
					if err := json.Unmarshal(msg.Value, &event); err != nil {
						slog.Warn("vpn-orchestrator invalid suspended event", "err", err)
						return commonkafka.ErrSkip
					}
					return service.ApplySuspended(opCtx, &event)

				default:
					slog.Debug("vpn-orchestrator ignored subscription event", "type", envelope.Type)
					return nil
				}
			})
		},
		service.producer,
		commonkafka.TopicSubscriptionEventsDLT,
	)
}

func RunVPNEventsConsumer(ctx context.Context, reader *kafkago.Reader, service *Service) error {
	if reader == nil {
		return nil
	}
	slog.Info("vpn-orchestrator starting vpn.events consumer")

	return commonkafka.RunConsumerWithDLT(
		ctx,
		reader,
		"vpn-orchestrator-service",
		func(ctx context.Context, msg commonkafka.Message) error {
			var envelope struct {
				Type      string `json:"type"`
				CommandID string `json:"command_id"`
				PaymentID string `json:"payment_id"`
				OrderID   string `json:"order_id"`
			}
			if err := json.Unmarshal(msg.Value, &envelope); err != nil {
				slog.Warn("vpn-orchestrator invalid vpn event envelope", "err", err)
				return commonkafka.ErrSkip
			}

			return processOrchestratorMessageOnce(ctx, service, msg, envelope.Type, envelope.CommandID, envelope.PaymentID, envelope.OrderID, func(opCtx context.Context) error {
				switch envelope.Type {
				case string(kafkacontracts.VPNEventNodeHeartbeat):
					var event kafkacontracts.VPNNodeHeartbeatEvent
					if err := json.Unmarshal(msg.Value, &event); err != nil {
						slog.Warn("vpn-orchestrator invalid heartbeat event", "err", err)
						return commonkafka.ErrSkip
					}
					return service.ApplyNodeHeartbeat(opCtx, &event)

				case string(kafkacontracts.VPNEventNodeUserSynced):
					var event kafkacontracts.VPNNodeUserSyncedEvent
					if err := json.Unmarshal(msg.Value, &event); err != nil {
						slog.Warn("vpn-orchestrator invalid user_synced event", "err", err)
						return commonkafka.ErrSkip
					}
					return service.ApplyNodeUserSynced(opCtx, &event)

				case string(kafkacontracts.VPNEventNodeUserRevoked):
					var event kafkacontracts.VPNNodeUserRevokedEvent
					if err := json.Unmarshal(msg.Value, &event); err != nil {
						slog.Warn("vpn-orchestrator invalid user_revoked event", "err", err)
						return commonkafka.ErrSkip
					}
					return service.ApplyNodeUserRevoked(opCtx, &event)

				case string(kafkacontracts.VPNEventNodeError):
					var event kafkacontracts.VPNNodeErrorEvent
					if err := json.Unmarshal(msg.Value, &event); err != nil {
						slog.Warn("vpn-orchestrator invalid node_error event", "err", err)
						return commonkafka.ErrSkip
					}
					return service.ApplyNodeError(opCtx, &event)

				default:
					slog.Debug("vpn-orchestrator ignored vpn event", "type", envelope.Type)
					return nil
				}
			})
		},
		service.producer,
		commonkafka.TopicVPNEventsDLT,
	)
}

func processOrchestratorMessageOnce(
	ctx context.Context,
	svc *Service,
	msg commonkafka.Message,
	eventType, commandID, paymentID, orderID string,
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

	inserted, err := outbox.MarkProcessed(ctx, tx, "vpn-orchestrator-service", messageID, eventType)
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
