# 5. Platega as a second payment provider inside billing

**Status:** Accepted

## Context

Card payments went through YooKassa only. A second provider, Platega, had to
become the recommended way to pay, while YooKassa stays available as the
second option and keeps handling payments that already exist (including late
webhooks for old payments).

Three properties of the Platega API shaped the design
(https://docs.platega.io, checked 2026-11-05):

- creating a transaction (`POST /transaction/process`) has **no documented
  idempotency key**: a timeout after the request was sent leaves the outcome
  unknown, and repeating the request may create a second invoice;
- callbacks are authenticated by the merchant id and API key in headers, are
  **retried only about three times** (5, 10 and 15 minutes), arrive in any
  order and may repeat; the documentation recommends confirming the status
  with `GET /transaction/{id}`;
- statuses are not monotonic: `CANCELED` may become `CONFIRMED`, and
  `CONFIRMED → DISPUTED → CONFIRMED` repeats the confirmation callback.

## Decision

Platega lives **inside `billing-service`**, in the same `payments` table
(`provider = 'platega'`), the same outbox and the same Kafka contracts. No new
service, database, broker or topic.

- The bot sends the user's explicit choice in
  `CreateSubscriptionCheckoutCommand.payment_provider`; an empty value (old
  bot) means YooKassa. Unknown values are rejected.
- Every status change of an order is decided by a pure state machine
  (`internal/billing/platega/machine.go`) from the authoritative `GET`
  response and the order row read under `FOR UPDATE`. Callbacks only schedule
  a check. The subscription is granted once: `paid_at` is set in the same
  transaction as the `BillingPaymentSucceededEvent` outbox record.
- A creation attempt is marked `provider_creating` **before** the HTTP call
  and committed. A redelivered command never repeats the call. A timeout or
  5xx yields `create_unknown`; the user never received a link, so the invoice
  cannot be paid, and the user is asked to choose again.
- A reconcile worker polls unpaid and cancelled orders with a decaying
  schedule, so a confirmed payment is found even if every callback was lost.
- Amounts are integer kopecks in Go and `NUMERIC(12,2)` in the database;
  amounts in JSON are parsed with exact rational arithmetic, never floats.

## Consequences

- Payment correctness does not depend on callback delivery, ordering or
  uniqueness, at the cost of a GET per callback and periodic polling (tens of
  requests per hour at the current volume).
- One order never produces two transactions through our code. If Platega
  reports a second paid transaction with the same payload, it is recorded as
  a separate purchase rather than silently dropped.
- Disputes, chargebacks and partial refunds are recorded and alerted, not
  acted upon automatically: revoking access after a chargeback is an operator
  decision.
- YooKassa code paths are unchanged except for repeated-tap reuse, a
  notification when creation fails (previously the user got nothing) and a
  provider filter that keeps YooKassa webhooks away from Platega rows.
