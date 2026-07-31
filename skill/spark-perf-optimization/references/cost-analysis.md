# Cost Analysis — Quantifying Spark Optimization Savings

Use measured resource time whenever the event log contains executor add/remove events. Do not estimate every executor as active for the full application wall time unless that is the only available evidence, and label that fallback clearly.

## Preferred calculation

For each executor $i$:

$$
\text{executor core-hours}_i = \text{cores}_i \times \frac{\text{active seconds}_i}{3600}
$$

Then:

$$
\text{total core-hours} = \sum_i \text{executor core-hours}_i + \text{driver core-hours}
$$

An executor's active interval starts at `SparkListenerExecutorAdded` and ends at `SparkListenerExecutorRemoved` or application end. State whether driver cores and active time came from configuration, platform billing, or an assumption.

## Pricing

$$
\text{cost per run} = \text{total core-hours} \times \text{currency per core-hour}
$$

$$
\text{annual savings} = (\text{baseline cost} - \text{candidate cost}) \times \text{annual successful runs}
$$

Use the team's actual billing export or chargeback rate when available. Public list prices change by provider, region, date, tier, and agreement; record the source URL, retrieval date, currency, and whether discounts are included.

## Retries and failures

Keep successful-run efficiency separate from failure waste:

```text
healthy cost per successful run = measured cost of a successful run
failure overhead = average failed-run cost × expected failed attempts per success
effective cost per success = healthy cost + failure overhead
```

Do not multiply a successful run by a failure-rate percentage unless the historical retry model supports that calculation.

## Example with synthetic values

Assume the analyzer reports:

| Measurement | Baseline | Candidate |
|---|---:|---:|
| Executor core-hours | 520 | 330 |
| Driver core-hours | 8 | 5 |
| Total core-hours | 528 | 335 |
| Runs per year | 365 | 365 |

At an illustrative rate of `$0.10/core-hour`:

```text
baseline cost/run = 528 × $0.10 = $52.80
candidate cost/run = 335 × $0.10 = $33.50
savings/run = $19.30
annual savings = $19.30 × 365 = $7,044.50
```

These values are synthetic. Replace every input with measured or approved values before publishing a claim.

## Sensitivity range

Provide at least three scenarios when rates or cadence are uncertain:

| Scenario | Rate | Runs/year | Annual savings |
|---|---:|---:|---:|
| Conservative | approved low rate | lower cadence | calculated value |
| Expected | expected rate | expected cadence | calculated value |
| Upper bound | approved high rate | upper cadence | calculated value |

## Claim checklist

Before including cost savings in a PR or presentation, verify:

1. Baseline and candidate process comparable data volumes and workload logic.
2. Executor core-hours use active lifetimes or are explicitly marked as estimates.
3. Driver cost is included or explicitly excluded.
4. Failure/retry cost is calculated separately.
5. Rate source, date, region, currency, and discounts are stated.
6. Run cadence is stated.
7. Hard compute savings are separated from SLA, engineering-time, and capacity benefits.
8. The result is presented as a range when important inputs vary.

## Soft benefits

Report these separately from compute savings:

- Earlier downstream SLA delivery
- Reduced failed-run and on-call burden
- Pool capacity returned sooner
- Headroom for future data growth
- Reduced operational variance