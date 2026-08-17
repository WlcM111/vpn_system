# 3. Access revocation independent of the billing provider

**Status:** Accepted

## Context

Access was originally revoked as a side effect of the billing lifecycle. The
billing service tracks recurring profiles, retries failed charges on a schedule
(15 minutes, 6 hours, 24 hours), holds a 72-hour grace period, and only then
suspends the subscription.

That chain works for one specific population: customers with a live recurring
profile. It silently skips everyone else.

The gap was found by querying production. Users whose subscription had expired
were still connected, because:

- a user who never enabled auto-renewal has no recurring profile, so the
  billing worker's query — which selects profiles in `active`, `retry` and
  `grace` — never sees them;
- the lazy expiry path that marks a subscription `expired` when its row is read
  changed the status but published no event, so the orchestrator was never told
  to remove credentials;
- the reconcile worker selected subscriptions by status without checking the
  expiry timestamp, and actively re-applied credentials for rows that had
  already lapsed.

The result was 45 accounts with working VPN access and no valid subscription.

## Decision

Revocation is driven by the subscription's own expiry, not by billing.

A sweep worker in `user-subscription` scans for lapsed subscriptions every 5
minutes and suspends them through the existing path: `MarkSuspendedTx` followed
by `SubscriptionSuspendedEvent`, which the orchestrator already handles by
publishing revoke commands to the node.

The margin before suspension depends on who owns the subscription:

| Subscription | Margin | Reason |
|---|---|---|
| Auto-renewal off | 15 minutes | billing will not renew it; the delay only absorbs a payment landing at the boundary |
| Auto-renewal on | 7 days | billing owns it through retries and grace; the sweep is a safety net, not a competitor |
| Grace period elapsed | 15 minutes | billing has already had its window |

Seven days is deliberately longer than billing's full cycle — roughly one day
of retries plus three days of grace. If billing is healthy it always suspends
first and the sweep never sees those rows. The sweep only fires when billing
itself has failed.

The reconcile worker's query now excludes rows whose expiry has passed, so it
can no longer resurrect access the sweep has revoked.

## Alternatives considered

**Fix the billing worker's query to cover users without a profile.** Rejected.
It keeps revocation coupled to the payment provider: a bug or outage in billing
still means customers keep access indefinitely. The failure mode is silent and
directly costs money.

**Publish an event from the lazy expiry path.** Rejected as insufficient. That
path only runs when something happens to read the row. A user who stops opening
the bot is never read and never expires.

**Revoke immediately at expiry, with no margin.** Rejected. A payment
confirmation arriving through a provider webhook seconds after expiry would
find the access already withdrawn, and the customer would see a broken service
immediately after paying.

## Consequences

**Gained.** Access ends when the subscription ends, regardless of payment
method, provider health or whether auto-renewal was ever enabled. The mechanism
is independent of billing, so a billing outage no longer means free service.

**Paid.** Two systems can now revoke the same subscription. Both paths converge
on the same idempotent operation and the sweep excludes rows already marked
expired, so a race produces one suspension rather than two — but the redundancy
is real and has to stay understood.

Revocation is not instantaneous: with a 5-minute scan and a 15-minute margin,
the worst case is about 20 minutes. Instant revocation is not achievable for
Xray transports anyway, because the UUID lives in the node's memory and only an
external trigger removes it.

**Verification.** The first production run suspended 45 subscriptions and the
node's active credential count fell to match the number of paying customers.
Reconcile passes dropped from 54 to 12 synchronised users, confirming it had
been re-provisioning expired accounts.