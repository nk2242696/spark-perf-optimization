---
name: spark-perf-optimization
description: 'Iteratively optimize Apache Spark job performance on Synapse, OSS Spark, and Databricks by analyzing event logs and applying evidence-backed configuration or code fixes. Use for slow jobs, OOMs, executor under-delivery, AQE, speculation, scheduler delays, ADLS throttling, compute-cost reduction, round comparisons, reviewer summaries, PR descriptions, and performance reports.'
argument-hint: '<eventlog path or Spark performance goal>'
---

# Spark Performance Optimization with Eventlog Analysis

A local-first methodology and stdlib-only analyzer for iterative Spark optimization. It turns event-log measurements into a compute-versus-infrastructure diagnosis, directs the agent to the relevant supporting reference, and provides templates for comparisons, reviews, cost analysis, and stakeholder reports.

## When to Use This Skill

Use this skill any time you need to:

- **Investigate a slow or failing Spark job** (OOM, slow stages, idle executors, ADLS throttle)
- **Tune a daily/scheduled Spark transform** (especially on Azure Synapse)
- **Verify whether a perf change actually helped** (round-over-round eventlog comparison)
- **Diagnose Synapse pool delivery issues** ("I asked for 80 executors and only got 40")
- **Quantify cost savings** from a perf optimization PR
- **Write a reviewer-friendly PR description** for perf work
- **Generate a PowerPoint summary** for stakeholders / leadership

Do NOT use this skill for:
- Pure SQL query tuning with no Spark context (use a SQL-tuning skill)
- Application-logic refactoring unrelated to perf
- Initial Spark job authoring (use a Spark/PySpark scaffolding skill)

## Prerequisites

- Python 3.10+ on the analyzer machine (no pip install required — pure stdlib)
- Access to a Spark eventlog file, directory, Synapse/History Server ZIP, or gzip file
- Read access to the Spark job source (typically PySpark/Scala) and its Spark conf
- For PR/cost work: git access to the PR branch
- For PPTX generation: Node.js (v14+), optional LibreOffice for PDF conversion

## The 5-Step Optimization Loop

Use this repeatable loop:

```
┌─────────┐    ┌─────────┐    ┌──────────┐    ┌─────┐    ┌─────────┐
│ Capture │ -> │ Analyze │ -> │ Diagnose │ -> │ Fix │ -> │ Measure │ -> repeat
└─────────┘    └─────────┘    └──────────┘    └─────┘    └─────────┘
```

1. **Capture** — Run the job in a dev/test environment, save the Spark eventlog
2. **Analyze** — Use `scripts/spark_eventlog_analyze.py` to surface bottlenecks
3. **Diagnose** — **Classify the bottleneck as compute-bound or infra-bound** (see triage below). Compare to prior runs (`compare overview/executors/skew`); isolate the change responsible for any regression or win
4. **Fix** — Apply one targeted change at a time. **For compute-bound stages, audit the code first** (Workflow E); for infra-bound symptoms, tune conf (Workflow F)
5. **Measure** — Re-run; analyze the new eventlog; confirm or revert

**One change per round.** Bundling 3 changes into a single round means you cannot attribute a wall-time delta to any single cause.

## Code-First Triage (do this before reaching for spark.conf)

Configuration tuning can help infrastructure-bound symptoms, while algorithmic changes can remove entire shuffles, windows, or cardinality explosions. Reach for configuration first only when the evidence is unambiguously infrastructural; otherwise inspect the code driving the dominant stage.

| Symptom from analyzer | Bound | First lever |
|---|---|---|
| `ramp` shows 0 execs for 5+ minutes at start | Infra (cold pool) | **Conf** — Workflow F |
| `executors` peak much less than requested | Infra (pool delivery) | **Conf** — Workflow F |
| Executor JVM OOM in logs | Infra (memory) | **Conf** — Workflow F |
| ADLS HTTP 503 throttle on final write | Infra (write fan-out) | **Conf** — Workflow F |
| One host's tasks consistently slowest | Infra (noisy host) | **Conf** — Workflow F (speculation) |
| 1-2 stages = >60% wall, CPU >80% on those stages | **Compute** | **Code** — Workflow E |
| Wide-key Window dominates wall | **Compute** | **Code** — Workflow E (Pattern 5/6) |
| `explode(...)` → `leftouter` → filter pipeline | **Compute** | **Code** — Workflow E (Pattern 6) |
| Same df recomputed across 2+ downstream stages | **Compute** | **Code** — Workflow E (Pattern 4) |
| Shuffle bytes ≫ post-stage output bytes | **Compute** | **Code** — Workflow E (Pattern 3) |
| Join exists only to set a boolean / derived flag | **Compute** | **Code** — Workflow E (Pattern 1) |
| Stage spill > 10 GB with no skew | **Compute** | **Code** — Workflow E first, then conf |
| `skew` shows one partition with 50%+ of stage data, not a shuffle join | **Compute** | **Code** — Workflow E (Pattern 10/11) |
| All conf rounds plateaued, wall still dominated by 1-2 CPU-bound stages | **Compute** | **Code** — Workflow E |

If you can't classify it: prefer **Workflow E (code)** first. Reading the source of the dominant stage rarely takes more than 30 minutes and either surfaces an obvious algorithmic win or rules code out so you can confidently move to conf.

## Step-by-Step Workflows

### Workflow A: First look at a new eventlog

```bash
# Use the bundled analyzer. Pass either the eventlog file or a folder containing it.
python scripts/spark_eventlog_analyze.py overview <log>
python scripts/spark_eventlog_analyze.py executors <log>
python scripts/spark_eventlog_analyze.py ramp <log> --interval 60
python scripts/spark_eventlog_analyze.py stages <log> --top 10
```

Read [references/analyzer-usage.md](references/analyzer-usage.md) for the full subcommand reference.

After this initial pass, you should be able to answer:
- Total wall time, peak executor count, top SQL/stage durations
- Did the pool deliver the executors I asked for? How fast?
- Which 2-3 stages dominate the wall time?

### Workflow B: Round-over-round comparison

After each change + re-run, compare to the prior round:

```bash
python scripts/spark_eventlog_analyze.py compare overview  $PREV $CURR
python scripts/spark_eventlog_analyze.py compare executors $PREV $CURR
python scripts/spark_eventlog_analyze.py compare skew      $PREV $CURR
python scripts/spark_eventlog_analyze.py compare speculation $PREV $CURR
```

The summary table at the bottom of `compare` (wall_min, peak_exec, hosts, spec_tasks, failed) is the single most useful artifact for PR descriptions and chat updates.

### Workflow C: Drill into a slow stage

```bash
python scripts/spark_eventlog_analyze.py stages <log> --top 10     # find the worst stages
python scripts/spark_eventlog_analyze.py stage <log> <STAGE_ID>    # deep dive on one
python scripts/spark_eventlog_analyze.py skew <log>                # is it skew?
python scripts/spark_eventlog_analyze.py spill <log>               # is it memory pressure?
```

The `stage <id>` deep dive shows CPU%, GC%, fetch-wait%, memory/disk spill, and per-task duration distribution. Interpret with [references/analyzer-usage.md](references/analyzer-usage.md).

### Workflow D: Diagnose a bottleneck and pick a lever

Use the **Code-First Triage** table above to classify the symptom, then read the matching reference doc:

- **Compute-bound** → start at [references/code-rewrite-patterns.md](references/code-rewrite-patterns.md) (Workflow E)
- **Infra-bound** → start at [references/common-bottlenecks.md](references/common-bottlenecks.md) (Workflow F)

Both docs map observed symptoms to likely causes, candidate changes, and validation steps. Do not skip the code review for compute-bound symptoms simply because a configuration knob is easier to change.

### Workflow E: Rewrite the code (first lever for compute-bound stages)

When the diagnose step classifies the bottleneck as compute-bound (1-2 stages dominate wall, those stages have high CPU%, no infra signal like cold-pool or pool under-delivery), **start here**. Reference [references/code-rewrite-patterns.md](references/code-rewrite-patterns.md) for the catalogue of code-level rewrites with before/after examples and validation strategies. Patterns covered:

- Eliminate redundant joins by deriving from existing columns
- Catalyst CollapseWindow hint — share a base Window across multiple frames
- Narrow projection before a heavy shuffle
- Persist DataFrames consumed by multiple downstream stages
- Split a heavy window — extract per-key constants into a groupBy
- Replace explode-then-window-fill with a gap-targeted join (as-of pattern)
- Domain-aware bypass — skip rows the algorithm provably leaves unchanged
- Pre-filter window/aggregation input to "needs-work" rows
- Two-stage aggregation for skewed groupBy
- Salt the join key for hot-key skew
- Hoist filter predicates above joins

Also documents anti-patterns (xxhash64 surrogate keys, reflexive `cache()`, `coalesce(1)` before writes) and a validation checklist for any rewrite.

**Why this is the first lever, not the last:** configuration cannot eliminate work that the algorithm creates unnecessarily. If a stage is CPU-bound and dominates wall time, inspect whether a rewrite can reduce rows, columns, shuffles, or window state before adding resources.

### Workflow F: Tune Spark conf (first lever for infra symptoms, refinement for everything else)

Use this workflow when (a) the symptom is infra-bound per the triage table (cold pool, pool under-delivery, OOM, ADLS throttle, noisy host), OR (b) you've already done the code-first audit and need to dial in the last 10-30%.

For **infra symptoms**, read [references/common-bottlenecks.md](references/common-bottlenecks.md) — it maps each symptom to root cause and the conf change that fixed it. Covered:

- Cold-pool door-wait (job idle for minutes waiting for executors)
- Synapse pool under-delivery (asked 80, got 40)
- Executor JVM OOM
- Stage OOM (window/join-induced memory blowup)
- ADLS write throttling (HTTP 503 on final write)
- Host-level skew (one slow VM dragging the stage)
- Material memory or disk spill on a dominant stage
- Speculation not killing slow tasks

For **refinement after a code rewrite**, reference [references/synapse-spark-conf-reference.md](references/synapse-spark-conf-reference.md) for an experiment matrix and an explanation of each lever. Highlights:

- `NumExecutors` is the **actual Synapse pool reservation lever** — `dynamicAllocation.maxExecutors` is a Spark-side ceiling the pool ignores
- Speculation settings should be tuned only after task-duration evidence confirms stragglers
- Scheduler registration thresholds can reduce cold-pool waiting but must reflect expected pool delivery
- AQE advisory partition size should be derived from observed partition sizes, spill, and available parallelism

### Workflow G: Quantify cost savings

Once perf is acceptable, compute cost savings using the framework in [references/cost-analysis.md](references/cost-analysis.md):

```
executor core-hours = sum(executor cores × active executor lifetime)
total core-hours = executor core-hours + driver core-hours
cost per run = total core-hours × approved rate
annual estimate = (baseline cost - candidate cost) × successful runs/year
```

Present a sensitivity range (conservative / midpoint / optimistic) since pool delivery and retry rates vary.

### Workflow H: Write the PR description

Use [assets/pr-description-template.md](assets/pr-description-template.md) as a starting point. Note: Azure DevOps PR descriptions are capped at **4000 characters** — trim accordingly.

If `az repos pr update --description "..."` silently truncates to ~10 chars on Windows PowerShell, switch to the REST API:

```powershell
$token = az account get-access-token --resource 499b84ac-1321-427f-aa17-267ca6975798 --query accessToken -o tsv
Invoke-RestMethod -Uri "https://<org>.visualstudio.com/<project>/_apis/git/repositories/<repo>/pullRequests/<id>?api-version=7.1" `
  -Headers @{Authorization = "Bearer $token"; "Content-Type" = "application/json"} `
  -Method PATCH `
  -Body (@{ title = "..."; description = $body } | ConvertTo-Json -Depth 5)
```

### Workflow I: Reviewer bullet notes

Use [assets/reviewer-summary-template.md](assets/reviewer-summary-template.md) for a chat/email-friendly summary of the PR. Sections: impact, code changes, conf changes, validation to run, what NOT to change, risk areas.

### Workflow J: Generate stakeholder PowerPoint

Use [assets/build_perf_pptx.js](assets/build_perf_pptx.js) as a starting template. It produces a generic performance deck that must be populated only with measured or explicitly supplied values.

Read [assets/pptx-readme.md](assets/pptx-readme.md) for full customization guide. Quick version:

```bash
cd assets
npm install
node build_perf_pptx.js report.json output.pptx
```

To convert to PDF for sharing (requires LibreOffice):

```powershell
& "C:\Program Files\LibreOffice\program\soffice.exe" --headless --convert-to pdf output.pptx
```

For visual QA, render to images and inspect each slide — common issues are text overflow on the last card of a grid and footer text overlapping bullets on dark closing slides.

## Anti-Patterns (Things to Avoid)

Avoid these common mistakes:

1. **Don't reach for `spark.conf` tuning before auditing the code.** Configuration cannot remove unnecessary rows, joins, windows, or shuffles. It is the right first move when the evidence is unambiguously infrastructural; see the Code-First Triage table above.
2. **Don't bundle multiple changes per round.** You won't know which one moved the needle.
3. **Don't trust a single wall-time number.** Cold-pool variance is real (±30%). Confirm with 2-3 runs or use compare-mode on stable subset metrics (peak_exec, spec_tasks, skew ratios).
4. **Don't set `spark.sql.files.maxRecordsPerFile` globally in the Conf** — it adds ~2x CPU per task on every write, including intermediate ones. Scope it to the final write only via `setConf` + save/restore.
5. **Don't repartition to >1000 for ADLS writes without testing** — large partition counts re-trigger 503 throttling that you may have fixed earlier.
6. **Don't tune `dynamicAllocation.maxExecutors` thinking it changes pool delivery.** On Synapse, `NumExecutors` is the actual reservation request.
7. **Don't keep redundant Spark confs** that match defaults. Audit periodically and remove (e.g., `spark.sql.adaptive.coalescePartitions.enabled=true` is default-true when AQE is on).
8. **Don't hardcode `'0'` in a setConf restore.** Use `getConf(key, defaultValue)` to capture the previous value, then restore that. Hardcoding clobbers anyone else's setting.

## References (Bundled in this Skill)

| File | Purpose |
|------|---------|
| `scripts/spark_eventlog_analyze.py` | The analyzer (stdlib-only Python) |
| `assets/build_perf_pptx.js` | Parametrized PowerPoint generator template |
| `assets/pptx-readme.md` | PPTX customization guide |
| `references/analyzer-usage.md` | Full subcommand reference + interpretation guide |
| `references/methodology.md` | Detailed walkthrough of the 5-step loop |
| `references/code-rewrite-patterns.md` | **Start here for compute-bound bottlenecks** — algorithmic rewrites (asof joins, projection narrowing, persist, redundant-join elimination, etc.) |
| `references/common-bottlenecks.md` | **Start here for infra-bound symptoms** (cold pool, OOM, ADLS throttle, pool under-delivery, noisy host) — symptom → root cause → conf fix cookbook |
| `references/synapse-spark-conf-reference.md` | Proven Spark conf set + each lever explained — refinement after the code audit |
| `references/cost-analysis.md` | How to compute $/run, $/month, $/year savings |
| `assets/pr-description-template.md` | Markdown skeleton for ADO PR descriptions |
| `assets/reviewer-summary-template.md` | Chat/email-friendly bullet summary |

## Troubleshooting

| Issue | Solution |
|-------|----------|
| Analyzer cannot decode `.zst` or `.lz4` | Extract it first; ZIP and gzip are supported directly |
| Analyzer says "No application_* event log found" | Confirm the file or ZIP contains a Spark `application_*` event log |
| `pptxgenjs` not found | `npm install -g pptxgenjs` |
| LibreOffice PDF conversion fails | Run `soffice --headless` once interactively to accept any first-run dialogs |
| PR description silently truncated | Use the REST API; see Workflow G |
| PR description over 4000 chars | Trim or move details to a comment; the cap is enforced |
| Round wall time varies wildly run-to-run | Cold-pool variance; use `compare executors` to verify delivery, not wall time alone |
