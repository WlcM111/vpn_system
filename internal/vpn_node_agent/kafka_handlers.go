package vpn_node_agent

import (
	"context"
	"encoding/json"
	"log/slog"

	commonkafka "vpn-platform/internal/common/kafka"
	kafkacontracts "vpn-platform/internal/contracts/kafka"
)

func (a *Agent) consumeCommands(ctx context.Context) error {
	if a.reader == nil {
		return nil
	}
	slog.Info("vpn-node-agent starting vpn.commands consumer")

	return commonkafka.RunConsumerWithDLT(
		ctx,
		a.reader,
		"vpn-node-agent",
		func(ctx context.Context, msg commonkafka.Message) error {
			var envelope struct {
				Type      string `json:"type"`
				NodeID    string `json:"node_id"`
				CommandID string `json:"command_id"`
			}
			if err := json.Unmarshal(msg.Value, &envelope); err != nil {
				slog.Warn("vpn-node-agent invalid command envelope", "err", err)
				return commonkafka.ErrSkip
			}

			if envelope.NodeID != "" && envelope.NodeID != a.cfg.NodeID {
				return nil
			}

			if envelope.CommandID != "" {
				fresh, err := a.markCommandSeen(envelope.CommandID)
				if err != nil {
					return err
				}
				if !fresh {
					slog.Debug("vpn-node-agent duplicate command ignored", "command_id", envelope.CommandID, "type", envelope.Type)
					return nil
				}
			}

			opCtx, cancel := context.WithTimeout(ctx, a.cfg.ApplyTimeout)
			defer cancel()

			switch envelope.Type {
			case string(kafkacontracts.VPNCommandNodeSyncUser):
				var cmd kafkacontracts.NodeSyncUserCommand
				if err := json.Unmarshal(msg.Value, &cmd); err != nil {
					slog.Warn("vpn-node-agent invalid sync command", "err", err)
					return commonkafka.ErrSkip
				}
				if cmd.NodeID != a.cfg.NodeID {
					return nil
				}
				return a.handleSyncUser(opCtx, &cmd)

			case string(kafkacontracts.VPNCommandNodeRevokeUser):
				var cmd kafkacontracts.NodeRevokeUserCommand
				if err := json.Unmarshal(msg.Value, &cmd); err != nil {
					slog.Warn("vpn-node-agent invalid revoke command", "err", err)
					return commonkafka.ErrSkip
				}
				if cmd.NodeID != a.cfg.NodeID {
					return nil
				}
				return a.handleRevokeUser(opCtx, &cmd)

			case string(kafkacontracts.VPNCommandNodePing):
				return a.publishHeartbeat(opCtx)

			default:
				slog.Debug("vpn-node-agent ignored command type", "type", envelope.Type)
				return nil
			}
		},
		a.producer,
		commonkafka.TopicVPNCommandsDLT,
	)
}
