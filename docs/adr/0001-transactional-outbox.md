# 1. Transactional outbox instead of direct publishing

**Status:** Accepted

## Context

Five services write to PostgreSQL and publish events to Kafka: `billing`,
`crypto-billing`, `user-subscription`, `vpn-orchestrator` and the bot gateway.
Both writes are part of the same logical operation, and the two systems cannot
share a transaction.

That leaves two failure modes, and both are real:

**The event is lost.** The database commit succeeds, the broker is briefly
unreachable, publishing fails. A payment is recorded but the subscription is
never activated: the customer paid and got nothing.

**The event is a lie.** Publishing succeeds, then the transaction rolls back.
Consumers act on a state that was never persisted — access is granted for a
payment that does not exist.

Retrying in application code does not close this. A process can die between the
commit and the retry, and holding the transaction open across a network call to
the broker turns broker latency into database lock contention.

## Decision

Events are written to an `event_outbox` table **inside the same transaction as
the business data**. A background worker reads pending rows and publishes them
afterwards.

Three tables carry the pattern:

- `event_outbox` — outgoing events with status, attempt count and backoff
- `processed_messages` — consumer-side deduplication
- `failed_messages` — events that exhausted their retries

Delivery is at-least-once, so every consumer records the message id in
`processed_messages` inside the transaction that applies the side effect. A
redelivered message finds its id already there and does nothing.

Events sharing a message key are published in insertion order. When one fails,
later events with the same key are deferred rather than overtaking it — a
consumer never observes "subscription cancelled" before "subscription created".

After 20 attempts an event moves to `failed_messages` instead of blocking the
queue behind it.

## Alternatives considered

**Publish directly, retry on failure.** Simplest, and wrong: it is exactly the
dual-write problem above.

**Change data capture (Debezium on the WAL).** Correct and requires no
application changes, but adds Kafka Connect and its operational surface to a
system run by one person. The outbox gives the same guarantee with a table and
a goroutine.

**Two-phase commit.** Kafka has no XA support worth using, and 2PC introduces
blocking failure modes worse than the problem it solves.

## Consequences

**Gained.** No lost or phantom events. The broker can be down for minutes
without data loss — events accumulate and drain on recovery. Consumers are
idempotent by construction rather than by convention.

**Paid.** Publishing is asynchronous: an event appears at the consumer after
the polling interval rather than instantly. Every consumer must call
`MarkProcessed`, and forgetting it reintroduces double application — this is
the pattern's sharpest edge. Both tables need retention policies, otherwise
they grow without bound.

**Verification.** Integration tests run against a real PostgreSQL and assert
the properties that only a database can provide: rollback removes both the data
and its event, `FOR UPDATE SKIP LOCKED` hands each event to exactly one worker
under concurrency, and a redelivered message is applied once.

The implementation is published separately as
[pgoutbox](https://github.com/WlcM111/pgoutbox) under MIT.