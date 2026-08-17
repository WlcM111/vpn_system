# 4. AWS as infrastructure-as-code demonstration, not production hosting

**Status:** Accepted

## Context

VPN nodes are the part of the platform that scales horizontally: each new
country needs a machine running Xray and Hysteria2, registered with the
orchestrator. Provisioning them by hand does not scale past a handful, and
manual steps are where configuration drift starts.

Two questions were separate but easy to conflate: how nodes should be
provisioned, and where they should run.

## Decision

Nodes are described as code with Terraform, and production runs on a
traditional VPS provider rather than AWS.

The Terraform module in `deploy/terraform/node/` provisions an EC2 instance on
ARM, an Elastic IP, a security group opening only the ports the transports
need, and an IAM role scoped to that node's own parameters in SSM Parameter
Store. IMDSv2 is enforced. On first boot the instance installs Docker and Xray,
reads its configuration from SSM, and starts the agent, which registers itself
with the orchestrator through heartbeat. State lives in S3 with a DynamoDB
lock; a CloudWatch billing alarm guards against a forgotten instance.

Production nodes run elsewhere because of egress pricing. The analysis is in
[`docs/aws-cost.md`](../aws-cost.md); the short version:

| Traffic/month | AWS total | Comparable VPS |
|---|---|---|
| 100 GB | ~15 USD | ~5 USD |
| 1 TB | ~98 USD | ~5 USD |

The instance itself is competitive at roughly 14 USD per month. Egress is not:
AWS includes 100 GB and then charges 0.09 USD per gigabyte, which for a VPN —
where egress *is* the product — dominates everything else. At one terabyte the
bill is twenty times a comparable VPS with a far larger allowance.

## Alternatives considered

**Run production on AWS for consistency.** Rejected on cost. Twenty times the
price buys no property this workload needs: a VPN node is stateless, holds no
data worth replicating, and is rebuilt from Terraform in minutes if lost.

**Skip Terraform and keep provisioning by hand.** Rejected. Manual setup is
where drift begins, and it makes adding a country an afternoon of work instead
of one command. The module is useful independently of which provider runs it.

**Use AWS Spot instances to close the price gap.** Not adopted for steady-state
nodes: an interrupted node drops every user connected to it. Spot remains a
reasonable option for short-lived capacity, and the module works unchanged.

## Consequences

**Gained.** A node in a new region is one `terraform apply` and self-registers
without manual steps. Secrets never enter the repository — the instance reads
them from SSM under an IAM role limited to its own path. Cost is a documented
decision rather than a surprise on the invoice.

**Paid.** Provisioning is split across two providers: Terraform describes the
AWS path, while production nodes are still set up against a different provider.
That is a real inconsistency, and closing it means writing a second module for
the production provider — worth doing when the node count justifies it.

The module also stops short of a fully working VPN node: after `apply` the TLS
certificate has to be issued and the node registered with the orchestrator,
because both need DNS pointing at an address that only exists once the instance
does. Automating that would require Terraform to hold credentials for the
production database, which is a worse trade than two documented commands.