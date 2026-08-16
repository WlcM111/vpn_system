# AWS cost analysis

Region eu-north-1 (Stockholm), on-demand pricing as of August 2026. Figures are
indicative: AWS pricing changes, so check the calculator for current numbers.

## Monthly, excluding traffic

| Resource | Configuration | ~USD/month |
|---|---|---|
| EC2 t4g.small | 2 vCPU ARM, 2 GB, running 24/7 | 12–14 |
| EBS gp3 | 20 GB | 1.60 |
| Elastic IP | attached to a running instance | 0 |
| S3 (database dumps) | ~5 GB | 0.12 |
| DynamoDB (state lock) | pay-per-request, a handful of requests | ~0 |
| CloudWatch alarm | 1 alarm | 0.10 |
| **Subtotal** | | **~14–16** |

## Traffic is the deciding factor

For a VPN node, egress traffic dominates the bill. AWS includes 100 GB per
month, then charges 0.09 USD per GB.

| Traffic/month | Traffic cost | Total |
|---|---|---|
| 100 GB | 0 | ~15 |
| 500 GB | 36 | ~51 |
| 1 TB | 83 | ~98 |
| 2 TB | 173 | ~188 |

**Conclusion:** AWS is expensive for VPN egress. A comparable instance at a
traditional VPS provider costs around 5 USD with a far larger traffic
allowance, which is why production runs there.

## When AWS is the right choice

- Standing up a node in a region where the usual provider has no presence
- Short-lived capacity for a few hours (Spot instances cost a fraction of
  on-demand)
- Demonstrating infrastructure-as-code and cloud operations

## Avoiding surprise bills

1. A billing alarm is created by the Terraform module; the threshold is set
   through `billing_alarm_threshold_usd`.
2. Run `terraform destroy` after experiments.
3. Check separately that the Elastic IP was released — an unattached address
   is billed.
4. Review Cost Explorer monthly.