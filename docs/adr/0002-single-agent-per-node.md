# 2. One agent per VPN node, not one per protocol

**Status:** Accepted

## Context

Every VPN node runs Xray, which serves three transports (VLESS over WebSocket,
over XHTTP behind a CDN, and over gRPC), and Hysteria2, which serves a fourth
over QUIC. Both need the same things: users added and removed as subscriptions
change, traffic reported back, liveness signalled to the orchestrator.

Xray and Hysteria2 are unrelated processes with unrelated control interfaces —
Xray exposes a gRPC API, Hysteria2 an HTTP callback and a stats endpoint.

The question was whether one agent should drive both, or each protocol should
get its own.

## Decision

One agent per node drives every protocol on it.

The agent owns a single Kafka connection, consumes `vpn.node_sync_user`,
`vpn.node_revoke_user` and `vpn.node_ping`, and emits `vpn.node_heartbeat`,
`vpn.node_traffic`, `vpn.node_user_synced`, `vpn.node_user_revoked` and
`vpn.node_error`. It keeps one state file describing which users should exist
on the node.

Hysteria2 is integrated through its HTTP authentication callback: instead of
storing users in a config file, it asks the agent on every connection whether
to admit the client. The password presented is the user's VLESS UUID — the same
identifier already synchronised for Xray.

## Alternatives considered

**One agent per protocol.** Rejected. Two agents mean two Kafka consumers, two
state files and two sources of truth about the same user on the same machine.
Any divergence between them is a support ticket: the customer connects over one
transport and not another, for no reason visible from the outside.

**Configuration file for Hysteria2 users.** Rejected. It would require
rewriting the file and reloading the process on every subscription change, and
it creates a second user registry that can drift from the agent's state. The
HTTP callback removes the registry entirely: there is nothing to keep in sync
because the answer is computed from the state the agent already has.

**A separate password for Hysteria2.** Rejected. It would need its own
generation, storage, delivery and revocation path. Reusing the VLESS UUID means
revocation is automatic: the orchestrator disables the credential, the agent
stops admitting that UUID, and no new code is involved.

## Consequences

**Gained.** The node is one atomic unit: one process to deploy, one to monitor,
one to restart. Revocation propagates to all four transports through a single
code path, which is verified — a disabled UUID is refused by the Hysteria2
callback in the same way it disappears from Xray. Adding a fifth transport
means teaching the existing agent, not deploying another one.

**Paid.** The agent is a single point of failure for the node: if it stops,
users already connected keep working, but no new users are provisioned and no
traffic is reported. This is mitigated by a supervising restart loop and by the
orchestrator's reconcile pass, which re-applies the intended state after the
agent returns.

The agent also becomes the hot path for Hysteria2 connections: every handshake
waits on its HTTP response. The handler does a lock-protected lookup in an
in-memory map, so the cost is bounded, but the coupling is real and worth
remembering if the protocol list grows.