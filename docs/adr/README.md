# Architecture Decision Records

Each file records one decision: the situation that forced it, the options that
were on the table, what was chosen and what it costs.

Records are immutable. When a decision is revisited, a new record supersedes
the old one rather than editing it — the reasoning that led to the original
choice stays readable.

| # | Decision | Status |
|---|---|---|
| [0001](0001-transactional-outbox.md) | Transactional outbox instead of direct publishing | Accepted |
| [0002](0002-single-agent-per-node.md) | One agent per VPN node, not one per protocol | Accepted |
| [0003](0003-access-revocation-independent-of-billing.md) | Access revocation independent of the billing provider | Accepted |
| [0004](0004-aws-not-in-production.md) | AWS as infrastructure-as-code demonstration, not production hosting | Accepted |

Format: [Michael Nygard's template](https://cognitect.com/blog/2011/11/15/documenting-architecture-decisions).