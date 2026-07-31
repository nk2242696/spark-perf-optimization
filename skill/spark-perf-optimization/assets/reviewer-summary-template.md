# Reviewer Summary Template

Keep the summary short and link to detailed evidence rather than embedding raw event-log content.

```text
<JobName> performance PR ready for review

Measured outcome
- Wall time: <BASELINE> -> <CANDIDATE> across <N> comparable runs
- Core-hours: <BASELINE> -> <CANDIDATE>, including <driver inclusion/exclusion>
- Reliability: <failure, OOM, or variance result>

Diagnosis
- <Event-log or external-diagnostic finding>
- <Dominant stage or plan finding>

Changes
- <File or setting>: <change and reason>
- <File or setting>: <change and reason>

Validation requested
- Output schema and row count against the baseline
- Per-key aggregates or hashes for business-critical columns
- Representative edge cases for changed logic

Risks
- <Correctness or cardinality risk and mitigation>
- <Capacity, cold-start, storage, or runtime-version risk>

Artifacts
- PR: <link>
- Sanitized comparison report: <link>
- Report deck: <link, if useful for the audience>
```

## Leadership variant

```text
<JobName> optimization measured across <N> comparable runs
- Wall time: <BASELINE> -> <CANDIDATE>
- Estimated compute cost: <BASELINE> -> <CANDIDATE> per run
- Annual estimate: <RANGE>, using <RATE SOURCE/DATE> and <CADENCE>
- Reliability/SLA effect: <MEASURED OUTCOME>
- Production rollout and monitoring: <STATUS>
```

## Technical variant

```text
<JobName> performance PR
- Dominant evidence: <stage/SQL/plan finding>
- Root cause: <compute, memory, shuffle, scheduler, or storage diagnosis>
- Change: <algorithm or configuration change>
- Correctness checks: <checks and result>
- Performance checks: <run count and environment controls>
- Residual risk: <risk>
- PR: <link>
```

Do not publish raw storage paths, user identifiers, SQL literals, customer data, fixed co-author identities, or unsupported savings claims.