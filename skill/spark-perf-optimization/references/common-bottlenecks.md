# Common Bottlenecks — Cookbook (conf-focused)

Symptom → root cause → fix. Patterns observed across multiple real Spark perf engagements (especially on Azure Synapse).

Each entry includes: how to detect via the bundled analyzer, what's actually happening, and the canonical fix.

> **Read this SECOND, not first.** For most compute-bound bottlenecks (1-2 stages dominate wall, those stages are CPU-bound with no infra signal), the bigger lever is the algorithm — start at [`code-rewrite-patterns.md`](code-rewrite-patterns.md).
>
> **Use this cookbook FIRST only when the symptom is unambiguously infra-bound:**
> - Cold-pool door-wait (job idle for minutes at start; see #1)
> - Synapse pool under-delivery — asked 80, got 40 (see #2)
> - Executor JVM OOM (conf-only fix)
> - Stage OOM caused by memory pressure, not cardinality (see #3)
> - ADLS HTTP 503 throttle on final write (see #5)
> - Host-level skew — one VM consistently slowest (see #8)
> - Speculation not killing slow tasks (see #6)
>
> **Use this cookbook SECOND to refine after a code rewrite,** to dial in pool size, speculation thresholds, AQE partition target, etc.

---

## 1. Cold-pool door-wait (job idle for minutes before any work)

**Symptom:** Job sits at "Submitting/Running" for 5-10 minutes with no stages making progress. `ramp` output shows 0 executors for the first 5+ minutes.

**Detect:**
```bash
python scripts/spark_eventlog_analyze.py ramp $LOG --interval 60
# Look for: T+0.0m to T+5.0m all show 0 executors, then a sudden jump
```

**Root cause:** Spark's scheduler waits for `spark.scheduler.minRegisteredResourcesRatio` (default 0.8 on YARN) fraction of requested executors to register before launching any work. On a cold Synapse pool, the last 10-20% of requested executors take a long time to come up, so the whole job stalls.

**Fix:**
```python
# In your job's Spark conf (or runtime defaults):
"spark.scheduler.minRegisteredResourcesRatio": "0.5",
"spark.scheduler.maxRegisteredResourcesWaitingTime": "300s"
```

Lower ratio = start work sooner, even if not all execs have registered. Cap the wait so we don't stall indefinitely.

Validate the threshold against several cold starts. Starting too early can underutilize a pool when the first stages need broad parallelism.

---

## 2. Synapse pool under-delivery (asked 80 execs, got 40)

**Symptom:** `executors` output shows peak executor count substantially less than what you set in `spark.dynamicAllocation.maxExecutors`.

**Detect:**
```bash
python scripts/spark_eventlog_analyze.py executors $LOG
# Compare "Peak concurrent execs" to your spark.dynamicAllocation.maxExecutors
```

**Root cause:** On Synapse, the actual pool reservation lever is the **`NumExecutors`** field in the Synapse session config (`%%configure` for notebooks, or via the Synapse API for spark batch jobs). The Spark-side `spark.dynamicAllocation.maxExecutors` is a *ceiling* — Spark will never request more than this — but the pool only allocates up to `NumExecutors`.

**Fix:** Increase `NumExecutors` on the Synapse session, then set `spark.dynamicAllocation.maxExecutors` to the same number. They must agree.

```python
# Synapse session config (notebook)
%%configure -f
{
  "driverMemory": "56g",
  "driverCores": 8,
  "executorMemory": "224g",
  "executorCores": 32,
  "numExecutors": 80
}

# Plus in spark conf
"spark.dynamicAllocation.initialExecutors": "50",
"spark.dynamicAllocation.minExecutors": "50",
"spark.dynamicAllocation.maxExecutors": "80"
```

Compare requested, registered, and peak-active executors across multiple runs; platform capacity can vary independently of Spark configuration.

---

## 3. Executor JVM OOM (`java.lang.OutOfMemoryError: Java heap space`)

**Symptom:** Executors die mid-job; stages show retry attempts; eventlog has `ExecutorLost` events with OOM reason.

**Detect:**
```bash
python scripts/spark_eventlog_analyze.py executors $LOG
# Look for non-zero "removed/lost" counts with OOM/exit reasons
python scripts/spark_eventlog_analyze.py stage $LOG <FAILING_STAGE_ID>
# Look for: peak_exec_mem distribution near the executor heap size
```

**Root cause:** Per-executor task memory + framework overhead exceeded the JVM heap. Often a window/sort that materializes a giant partition, or a wide join.

**Fix (in order of preference):**
1. **Reduce partition size** — increase `spark.sql.shuffle.partitions` (e.g., 800 → 2400) or repartition explicitly to break up the hot partitions.
2. **Increase executor memory** — change Synapse pool size (e.g., Large → XLarge) or per-executor memory if pool supports it.
3. **Increase memory overhead** — `spark.executor.memoryOverhead=12g` (default is 10% of heap, often too small for off-heap shuffle).
4. **Switch persist level** — `MEMORY_AND_DISK` instead of `MEMORY_ONLY` so spill is allowed instead of OOM.

Choose partition and memory values from observed partition sizes, spill, executor heap, and available task slots. The example values above are starting points, not universal defaults.

---

## 4. Stage-level memory spill (>1 TB to disk)

**Symptom:** Stage takes much longer than expected; `spill` subcommand shows multi-TB disk spill on one stage.

**Detect:**
```bash
python scripts/spark_eventlog_analyze.py spill $LOG
# Look for stages with disk_spill > 100 GB
python scripts/spark_eventlog_analyze.py stage $LOG <STAGE_ID>
# Look for: spill_disk distribution with non-zero p50
```

**Root cause:** The stage's working set (per-partition sort/hash buffer + intermediate state) exceeds available executor memory. Spark spills to local disk, which is much slower than RAM (~10-50x).

**Fix (in order of preference):**
1. **More partitions, smaller each** — `spark.sql.shuffle.partitions` up, OR explicit `df.repartition(N)` before the heavy stage.
2. **Lower `spark.memory.storageFraction`** to give more memory to execution (default 0.5, try 0.4).
3. **Bigger executors** if pool allows.
4. **Avoid wide aggregations** — rewrite to two-stage aggregation if a single `groupBy` is the spill source.

Validate the change by comparing per-task spill and task duration. Total spill can remain high even when smaller partitions improve wall time.

---

## 5. AQE coalesce making partitions too big

**Symptom:** AQE is enabled, but post-shuffle stages have very few partitions and each one is slow / spilling.

**Detect:**
```bash
python scripts/spark_eventlog_analyze.py stages $LOG --top 20
# Look for top stages with ntasks < 50 and high spill/duration
```

**Root cause:** Default `spark.sql.adaptive.advisoryPartitionSizeInBytes=64MB` tells AQE to coalesce small partitions up to ~64MB. If your data is dense or your `shuffle.partitions` is high, AQE can coalesce 2400 partitions down to 50, which kills parallelism for downstream work.

**Fix:**
```python
"spark.sql.adaptive.enabled": "true",
"spark.sql.adaptive.advisoryPartitionSizeInBytes": "32MB",
# This is the killer — disables AQE's "prefer fewer partitions" heuristic:
"spark.sql.adaptive.coalescePartitions.parallelismFirst": "false"
```

`parallelismFirst=false` tells AQE to prefer the advisory size over "use all available cores at min." Combined with a 32MB advisory size, you get more, smaller partitions = more parallelism + less spill.

Treat `32MB` as an experiment. Dense rows, compression, and operator working sets can make the best advisory size substantially different.

---

## 6. Speculation not firing on slow tasks

**Symptom:** A few outlier tasks in a stage take 10-100x the median task duration. Stage wall time = duration of slowest task.

**Detect:**
```bash
python scripts/spark_eventlog_analyze.py skew $LOG --top 10
# Look for stages with max/median > 10
python scripts/spark_eventlog_analyze.py stage $LOG <STAGE_ID>
# Look at "Per-task duration distribution": p90 << max means a few outliers
python scripts/spark_eventlog_analyze.py speculation $LOG
# Verify spec_tasks > 0
```

**Root cause:** Either speculation is disabled (default on Spark), or its trigger thresholds are too conservative for your workload.

**Fix:**
```python
"spark.speculation": "true",
"spark.speculation.multiplier": "2.0",       # task must be 2x median to be speculated
"spark.speculation.quantile": "0.85",         # only when 85% of tasks done
"spark.speculation.minTaskRuntime": "30s",    # don't speculate tasks under 30s
"spark.speculation.task.duration.threshold": "120s"  # absolute floor
```

Confirm that duplicate attempts reduce stage completion time rather than merely consuming additional executor capacity. Data skew often requires a data or join rewrite instead.

---

## 7. ADLS write throttling (HTTP 503 on final write)

**Symptom:** Final write stage takes 10-30+ minutes, much longer than expected for the data volume. Driver-side logs show 503 retries. Eventlog stage has high "unaccounted in run" time.

**Detect:**
```bash
python scripts/spark_eventlog_analyze.py stage $LOG <WRITE_STAGE_ID>
# Look for: very high "unaccounted in run" (>50% of run time), low CPU %
python scripts/spark_eventlog_analyze.py output $LOG
# The slow write is your top output stage
```

**Root cause:** Too many concurrent writers hammering ADLS Gen1/Gen2 from too many partitions. Each writer does `CreateFile` + `Write` + `Flush` + `Close`; throttle limits cap per-second operations.

**Fix:**
```python
# Coalesce or repartition down to a sensible writer count BEFORE the final write.
# Rule of thumb: aim for ~256MB-1GB per output file.
df.repartition(800).write...

# AND scope-limit records per file to prevent any single file getting too large
# (also helps if you want predictable file sizing):
prev = sqlc.getConf("spark.sql.files.maxRecordsPerFile", "0")
sqlc.setConf("spark.sql.files.maxRecordsPerFile", "5000000")
try:
    df.write...
finally:
    sqlc.setConf("spark.sql.files.maxRecordsPerFile", prev)
```

HTTP status codes are normally present in driver or storage diagnostics, not Spark event logs. Use event-log timing to identify a likely I/O-bound stage, then confirm throttling from those external logs before changing writer fan-out.

---

## 8. Host-level skew (one VM dragging the stage)

**Symptom:** A stage's tasks are evenly sized, but the slowest 5-10 tasks all ran on the same one or two executors.

**Detect:**
```bash
python scripts/spark_eventlog_analyze.py stage $LOG <STAGE_ID>
# Look at: "Top 10 slowest tasks" section — same host/exec_id repeated?
# Also: "Tasks per executor" — is one executor's total_dur dramatically higher?
```

**Root cause:** A specific VM in the pool is slow (noisy neighbor, hardware issue, throttled). Spark's locality preferences keep sending tasks there.

**Fix:**
1. **Enable speculation** (see #6) — speculation kills the slow tasks on duplicate executors.
2. **Disable strict locality wait** — `spark.locality.wait=0s` (default 3s) — Spark schedules tasks on any executor immediately instead of waiting for a node-local one.
3. **Long-term:** report to platform team if reproducible across runs.

Confirm host concentration over multiple stages or runs before attributing the issue to a VM. A single stage can reflect data locality or skew instead.

---

## Next: did conf get you to your goal?

If you've worked through the relevant entries above and the wall is still dominated by 1-2 stages doing genuine compute work (high CPU%, no infra signal), the next lever is the algorithm itself. Switch to [`code-rewrite-patterns.md`](code-rewrite-patterns.md).

If conf hit your goal: refine with [`synapse-spark-conf-reference.md`](synapse-spark-conf-reference.md), then move to cost analysis and PR writeup (Workflows G–J in the SKILL).

## 9. Driver OOM / driver-side hangs

**Symptom:** Job dies with `OutOfMemoryError` in driver logs; or last visible Spark event is from minutes ago with no stage progress.

**Detect:**
- Look at driver logs (separate from executor eventlog).
- In eventlog: `overview` shows runtime longer than the latest stage end-time + some buffer.

**Root cause:** Collecting too much data to driver, or running too many concurrent SQL operations from the driver.

**Fix:**
1. Find the `collect()`, `toPandas()`, or `count()` calls that pull large data to driver. Rewrite to write to storage and read elsewhere if possible.
2. Increase `spark.driver.memory` and `spark.driver.memoryOverhead`.
3. Reduce `spark.sql.broadcastTimeout` if drive is hanging on broadcast joins; or increase `spark.sql.autoBroadcastJoinThreshold` if joins are forcing shuffle when they could broadcast.

---

## 10. Inefficient broadcast joins

**Symptom:** Many tasks across many stages doing the same join, all reading the same small dimension table from disk.

**Detect:** Look at the SQL plan (`plan $LOG <SQL_ID>`) — `SortMergeJoin` showing where a `BroadcastHashJoin` would be appropriate (small table on one side).

**Fix:**
```python
"spark.sql.autoBroadcastJoinThreshold": "200MB"   # default is 10MB, often too small
# Or explicitly:
from pyspark.sql.functions import broadcast
df.join(broadcast(small_df), "key")
```

**Caution:** Broadcasting too-large tables OOMs the driver. 200MB is usually safe; above that, validate driver memory headroom.

---

## 11. Redundant Spark confs (cruft accumulation)

**Symptom:** Spark conf has 30+ entries; many match defaults; nobody knows what does what.

**Detect:** Manually audit each conf against the [Spark docs](https://spark.apache.org/docs/latest/configuration.html) for your version. Or check via `spark-submit --conf spark.sql.defaultSizeInBytes=8GB --verbose ...` and look for "overrode default."

**Fix:** Remove any conf where:
- The value matches the documented default.
- The conf controls a feature that's already implied by another conf (e.g., `spark.sql.adaptive.coalescePartitions.enabled=true` is implied by `spark.sql.adaptive.enabled=true`).
- The conf has been deprecated or no-ops in your Spark version.

Configuration cleanup improves maintainability but should not be reported as a performance gain unless a measured run shows one.

---

## 12. `maxRecordsPerFile` set globally (silently doubling write CPU)

**Symptom:** Hard to detect from eventlog alone — symptom is "writes are slower than they should be for the data volume."

**Detect:** Check your Spark conf string. If `spark.sql.files.maxRecordsPerFile` is set to a non-zero value globally, you have this problem.

**Root cause:** With `maxRecordsPerFile=N`, every write task computes record counts and creates new files when N is exceeded. This adds ~2x CPU overhead per task and can produce many tiny files for small partitions. Setting it globally affects every intermediate write too, not just the final one.

**Fix:** Scope the conf to the single write that needs it, save/restore the prior value:

```python
prev = sqlc.getConf("spark.sql.files.maxRecordsPerFile", "0")
sqlc.setConf("spark.sql.files.maxRecordsPerFile", "5000000")
try:
    df.write...   # The one big write that needs the cap
finally:
    sqlc.setConf("spark.sql.files.maxRecordsPerFile", prev)

# Subsequent writes (e.g., Helper.writeDfToGen1) now use prev, not '0'.
```

---

## Bottleneck triage decision tree

```
Is wall time dominated by 1-3 stages?
├── No → ramp problem. Check `executors` + `ramp`. Likely #1 (door-wait) or #2 (pool delivery).
└── Yes → drill into the top stage with `stage <id>`.
    │
    ├── CPU% > 90% and uniform task duration → genuinely compute-bound. Add executors (if pool delivers) or reduce work.
    │
    ├── GC% > 15% → memory pressure. Likely #3 (OOM imminent) or #4 (spill). Bigger heap or more partitions.
    │
    ├── fetch_wait% > 30% → shuffle bottleneck. Check #5 (AQE coalesce too aggressive) or #10 (broadcast missed).
    │
    ├── max/median > 10 (skew) → likely #6 (speculation off) or #8 (host issue). Turn on speculation first.
    │
    ├── unaccounted% > 50% on a write stage → I/O bound on sink. Likely #7 (ADLS throttle). Repartition down.
    │
    └── Else → run `all` and look for anomalies in `executors`, `skew`, `spill`, `speculation` outputs.
```

**Conf is tapped out?** If after applying the relevant fix above the dominant stage(s) still show CPU% > 80% on genuine compute work (not waiting/spilling/GC), switch to [`code-rewrite-patterns.md`](code-rewrite-patterns.md) for algorithmic rewrites.
