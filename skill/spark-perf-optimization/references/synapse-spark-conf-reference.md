# Synapse Spark Conf Reference

These values are illustrative experiment starting points, not a configuration bundle to copy wholesale. Confirm support and defaults for the Spark and Synapse runtime version, change one lever at a time, and retain a setting only when comparable runs support it.

## Example experiment set

```
spark.scheduler.minRegisteredResourcesRatio        0.5
spark.scheduler.maxRegisteredResourcesWaitingTime  300s
spark.speculation                                  true
spark.speculation.multiplier                       2.0
spark.speculation.quantile                         0.85
spark.speculation.minTaskRuntime                   30s
spark.speculation.task.duration.threshold          120s
spark.sql.shuffle.partitions                       2400
spark.sql.adaptive.enabled                         true
spark.sql.adaptive.advisoryPartitionSizeInBytes    33554432         # 32MB
spark.sql.adaptive.coalescePartitions.parallelismFirst false
spark.sql.adaptive.skewJoin.skewedPartitionThresholdInBytes 33554432
spark.sql.autoBroadcastJoinThreshold               209715200        # 200MB
spark.sql.files.maxPartitionBytes                  268435456        # 256MB
spark.executor.memoryOverhead                      12g
spark.driver.memoryOverhead                        4g
spark.memory.storageFraction                       0.4
spark.serializer                                   org.apache.spark.serializer.KryoSerializer
spark.executor.extraJavaOptions                    -XX:+UseG1GC -XX:InitiatingHeapOccupancyPercent=35
spark.driver.extraJavaOptions                      -XX:+UseG1GC
```

Plus Synapse session config:
```json
{
  "driverMemory": "56g",
  "driverCores": 8,
  "executorMemory": "224g",
  "executorCores": 32,
  "numExecutors": 80
}
```

## Per-conf rationale

### Scheduler / cold-start

**`spark.scheduler.minRegisteredResourcesRatio=0.5`** (default 0.8 on YARN, 0.0 on standalone)
Spark starts launching tasks when at least 50% of requested executors have registered. Lower = faster cold start. On Synapse, the last few executors are slow to come up; waiting for 80% can mean 5+ minutes of idle door-wait.

**`spark.scheduler.maxRegisteredResourcesWaitingTime=300s`** (default 30s)
Even if ratio isn't met, start after this timeout. We bumped this from default because cold Synapse pools can take 60-90s for the first executor, then 2-3s each for subsequent ones. 300s gives the ratio a chance to be met before falling through.

Measure application-start-to-first-task time and executor delivery before and after changing these settings.

### Speculation

**`spark.speculation=true`** (default false)
Spark launches a duplicate of any task running much longer than peers. The duplicate runs on a different executor; whichever finishes first wins. Massive win for stragglers (host-level skew, GC pauses, slow local disk).

**`spark.speculation.multiplier=2.0`** (default 1.5)
A task is "slow" if it takes >2x the median running task. We use 2.0 instead of 1.5 to avoid speculating tasks that are just slightly slow (which wastes resources).

**`spark.speculation.quantile=0.85`** (default 0.75)
Only consider speculation once 85% of a stage's tasks have completed. Higher = wait for more evidence the slow ones are outliers; less risk of speculating during a brief slowdown.

**`spark.speculation.minTaskRuntime=30s`** and **`spark.speculation.task.duration.threshold=120s`**
Don't bother speculating short tasks; the overhead isn't worth it.

Verify speculative attempts win often enough to offset the duplicate compute they consume.

### Shuffle partitions

**`spark.sql.shuffle.partitions=2400`** (default 200)
Higher = smaller per-partition working set = less spill, less OOM risk. With 80 executors × 32 cores = 2560 task slots, 2400 partitions gives ~3 tasks per slot for a single shuffle stage (with AQE coalescing as needed).

If you have fewer cores, scale down proportionally. Rule of thumb: 1-3x your total core count.

### AQE (Adaptive Query Execution)

**`spark.sql.adaptive.enabled=true`** (default true since Spark 3.2)
AQE rewrites query plans at runtime based on actual shuffle data sizes. It can coalesce small post-shuffle partitions, switch join strategies, and split skewed partitions.

**`spark.sql.adaptive.advisoryPartitionSizeInBytes=33554432` (32MB)** (default 64MB)
Target size for post-shuffle partitions after AQE coalesce. Smaller values can increase parallelism and reduce each task's working set, at the cost of more scheduling and file overhead.

**`spark.sql.adaptive.coalescePartitions.parallelismFirst=false`** (default true)
**This is the critical one.** With `true`, AQE will never coalesce below "use all cores" — it prefers parallelism over hitting the advisory size. With `false`, AQE actually hits the advisory size, producing more, smaller partitions. Counterintuitive but: more partitions = less per-partition spill = faster wall time when spill is your problem.

**`spark.sql.adaptive.skewJoin.skewedPartitionThresholdInBytes=33554432`** (default 256MB)
A partition is "skewed" if it's >32MB and >median × multiplier. Smaller threshold = more partitions detected as skewed and split. Aligned to our advisory size.

### Joins

**`spark.sql.autoBroadcastJoinThreshold=200MB`** (default 10MB)
Tables ≤ 200MB get broadcast to all executors instead of shuffled. 200MB chosen because our dimension tables are 50-150MB; default 10MB would have forced shuffles. **Watch driver memory** — broadcasts are gathered on the driver first.

### File I/O

**`spark.sql.files.maxPartitionBytes=256MB`** (default 128MB)
Max bytes per partition when reading files. Bigger = fewer, larger partitions on read = less scheduling overhead, but more memory per task. 256MB with 224g executors is comfortable.

### Memory tuning

**`spark.executor.memoryOverhead=12g`** (default = max(10% of heap, 384m))
Off-heap memory for shuffle, network buffers, and Python workers. Derive this from the container limit, heap, Python usage, and observed memory-limit failures rather than copying the example value.

**`spark.driver.memoryOverhead=4g`** (default = max(10% of heap, 384m))
Same idea for driver. 4g is generous; bump if you see driver-side off-heap pressure.

**`spark.memory.storageFraction=0.4`** (default 0.5)
Of the unified memory pool, this fraction is protected for storage. Lowering it can favor execution in a spill-heavy workload, but can also evict reusable cached data sooner.

### Serialization

**`spark.serializer=org.apache.spark.serializer.KryoSerializer`** (default Java)
Kryo can reduce serialization size and CPU for compatible object workloads. Benchmark it with the application's classes and register classes where appropriate; DataFrame execution often uses Spark SQL's internal binary format instead.

### Garbage collection

**`spark.executor.extraJavaOptions=-XX:+UseG1GC -XX:InitiatingHeapOccupancyPercent=35`**
G1GC is the recommended collector for executors >= 8GB. IHOP=35 tells G1 to start concurrent marking when 35% of heap is occupied (default 45%); earlier marking = lower pause times at the cost of more GC CPU overhead. For large heaps (200g+), earlier is better.

**`spark.driver.extraJavaOptions=-XX:+UseG1GC`**
Same collector on driver; no IHOP tweak needed since driver heap is smaller (56g).

---

## Confs we tried and REMOVED (cruft)

These match defaults or are implied by another conf — don't include them:

| Conf | Why removed |
|------|-------------|
| `spark.sql.adaptive.coalescePartitions.enabled=true` | Default true when AQE on |
| `spark.sql.adaptive.coalescePartitions.minPartitionNum` | Use `parallelismFirst=false` instead |
| `spark.sql.adaptive.coalescePartitions.minPartitionSize` | Use `advisoryPartitionSizeInBytes` |
| `spark.sql.adaptive.skewJoin.enabled=true` | Default true when AQE on |
| `spark.sql.adaptive.skewJoin.skewedPartitionFactor=5` | Default matches our need |

If you find yourself with 30+ confs, audit against Spark docs for your version and prune.

---

## Confs to AVOID setting globally

**`spark.sql.files.maxRecordsPerFile`** — Setting this in the conf adds ~2x CPU to every write task. Scope it to specific writes only:

```python
prev = sqlc.getConf("spark.sql.files.maxRecordsPerFile", "0")
sqlc.setConf("spark.sql.files.maxRecordsPerFile", "5000000")
try:
    big_df.write...
finally:
    sqlc.setConf("spark.sql.files.maxRecordsPerFile", prev)
```

**`spark.locality.wait`** — Default 3s is usually correct. Setting to `0s` disables Spark's preference for node-local data, which can hurt I/O performance. Only set to `0s` if you've confirmed locality wait is the bottleneck.

**`spark.dynamicAllocation.minExecutors=0`** — Setting min to 0 means Spark may scale all the way down to zero between SQL executions, then pay the cold-start cost again. Set min equal to a sensible baseline (e.g., 50) so Spark keeps a working set warm.

---

## Synapse-specific gotchas

### `NumExecutors` vs `spark.dynamicAllocation.maxExecutors`

These look like they do the same thing, they don't:
- **`NumExecutors`** (Synapse session config) — actual pool reservation. The Synapse platform requests this many VMs from Azure to host executors.
- **`spark.dynamicAllocation.maxExecutors`** (Spark conf) — Spark-side ceiling. Spark will never request more executors from the pool than this.

If `NumExecutors=50` and `maxExecutors=80`, you get 50. If `NumExecutors=80` and `maxExecutors=50`, you get 50. Set both to the same value.

### Pool delivery is best-effort

Even with `NumExecutors=80`, you often get only 60-80% delivered on a cold pool. The Synapse platform doesn't guarantee delivery if Azure is capacity-constrained in your region. Plan for 80% delivery as your effective max.

### Pool sizes

| Pool tier | Per-executor cores | Per-executor memory |
|-----------|-------------------|---------------------|
| Small (XSmall) | 4 | 28GB |
| Medium | 8 | 56GB |
| Large | 16 | 112GB |
| XLarge | 32 | 224GB |
| XXLarge | 64 | 432GB |

Pool tiers and limits can change. Verify the current values in the linked Microsoft documentation and in the target workspace before planning capacity. Bigger isn't always better: fewer larger executors can reduce network fan-out but increase GC concentration.

### Conf scope on Synapse

You can set Spark conf in three places, with precedence:
1. **Notebook `%%configure`** — overrides everything for that notebook session.
2. **Spark pool default config** — set in the pool definition; applied to every session that doesn't override.
3. **Code-level `sqlc.setConf(...)`** — runtime-only; doesn't affect session restarts.

For SQL-defined transforms (like `Dataset.sql` row-format), the conf is usually applied via a column on the table that the orchestrator reads.

---

## References

- [Spark configuration docs](https://spark.apache.org/docs/latest/configuration.html)
- [Spark SQL performance tuning](https://spark.apache.org/docs/latest/sql-performance-tuning.html)
- [Synapse Spark pool config](https://learn.microsoft.com/azure/synapse-analytics/spark/apache-spark-pool-configurations)
- [AQE deep dive](https://spark.apache.org/docs/latest/sql-performance-tuning.html#adaptive-query-execution)
