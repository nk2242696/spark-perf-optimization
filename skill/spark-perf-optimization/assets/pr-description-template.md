# PR Description Template — Spark Perf Optimization

Use this as a starting point for the PR description. **Total must fit in 4000 characters** (Azure DevOps cap). Trim sections as needed.

---

```markdown
## Summary

`<TransformName>` performance optimization. Wall time **<BEFORE>m -> <AFTER>m (<XX>%)**, OOMs eliminated.

## Final state

| Metric | Before | After |
|---|---|---|
| Wall time (cold pool) | ~<XX>m | ~<XX>m |
| Wall time (warm pool) | ~<XX>m | ~<XX>m |
| OOM failure rate | ~<XX>% | 0% |
| Peak executors (delivered) | <XX> | <XX> |
| Per-run vCore-hours | ~<XXX> | ~<XXX> |
| Per-run cost at approved rate | ~<CURRENCY><XX> | ~<CURRENCY><XX> |

## Files changed

- `<path/to/Transform>.py` — `<one-line summary>`
- `<path/to/Dataset.sql>` — Spark conf tuning (`<TransformName>` row)
- (Other files if applicable)

## Round-by-round history

| R | Commit | Wall | Change |
|---|---|---|---|
| R1 | `<sha>` | XX.Xm | <baseline description> |
| R2 | `<sha>` | XX.Xm | <single change> |
| ... | ... | ... | ... |
| R<N> | `<sha>` | XX.Xm | <final change> |
| cleanup | `<sha>` | — | remove N redundant Spark confs matching defaults |

## Key Spark conf changes

```
<setting.one>     <BEFORE> -> <AFTER>  # evidence and reason
<setting.two>     <BEFORE> -> <AFTER>  # evidence and reason
```

## Code changes

1. **<Change>** — <why this addresses the measured bottleneck>
2. **<Change>** — <why this addresses the measured bottleneck>

## Validation checklist (for reviewers)

- [ ] Tier 1: count(*) of output vs prior production
- [ ] Tier 2: hash of output rows (excluding timestamp/run id columns) vs prior production
- [ ] Tier 3: per-segment row counts grouped by `<key>` vs prior production
- [ ] No new schema columns
- [ ] No change in downstream consumer behavior

## Risks

- **Environment variance** — <how cold/warm state, input, or pool delivery can affect the result>.
- **Capacity** — <resource requirement and effect on shared workloads>.
- **Correctness** — <logic, cardinality, ordering, or schema risk and mitigation>.

## Cost impact

- **Per-run savings:** ~<XXX> vCore-hours (~<XX>% reduction)
- **Monthly (~30 runs):** ~$<X,XXX> (range $<XXX> - $<X,XXX>)
- **Yearly (~365 runs):** ~$<XX,XXX> (range $<X,XXX> - $<XX,XXX>)

Rate source: <provider/billing source, region, currency, retrieval date, and discount assumptions>.
Cadence: <successful runs per month/year>.

Retry overhead is reported separately: <failed-run cost and expected attempts per success>.
```

---

## Tips for fitting under 4000 chars

If your description is too long:

1. **Drop the round-by-round table** — link to commit list instead. Or keep only the headline rounds (best/worst).
2. **Collapse the validation checklist** — move full checklist to a PR comment instead of the description.
3. **Trim the Spark conf section** — keep only the 3-5 most impactful changes; let the diff speak for the rest.
4. **Link detailed evidence** — keep the description focused on measured outcomes and validation.

## Tip for updating ADO PR descriptions

Do **not** use `az repos pr update --description "..."` on Windows PowerShell — it silently truncates multi-line strings to 10 chars. Use the REST API:

```powershell
$token = az account get-access-token --resource 499b84ac-1321-427f-aa17-267ca6975798 --query accessToken -o tsv
$body = @{
  title = "perf(<Transform>): <X>x faster, OOM-free (<XX>m -> <XX>m warm / <XX>m cold)"
  description = (Get-Content pr-description.md -Raw)
} | ConvertTo-Json -Depth 5

Invoke-RestMethod `
  -Uri "https://<org>.visualstudio.com/<project>/_apis/git/repositories/<repo>/pullRequests/<id>?api-version=7.1" `
  -Headers @{Authorization = "Bearer $token"; "Content-Type" = "application/json"} `
  -Method PATCH `
  -Body $body
```

The `499b84ac-1321-427f-aa17-267ca6975798` is the Azure DevOps resource ID; that's stable across orgs.
