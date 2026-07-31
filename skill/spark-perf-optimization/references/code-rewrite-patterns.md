# Code Rewrite Patterns — Start Here for Compute-Bound Bottlenecks

A cookbook of **code-level** rewrites for Spark jobs. Read this **first** when event-log evidence shows that the bottleneck is compute-bound.

[`common-bottlenecks.md`](common-bottlenecks.md) covers the conf-focused playbook for infra-bound symptoms (cold pool, OOM, ADLS throttle, pool under-delivery, noisy host).

## When to read THIS doc first

- The analyzer shows 1-2 stages = >60% of wall, CPU% > 80% on those stages
- The dominant stage is a wide-key `Window`, a heavy join, or an `explode` followed by filtering
- A df is recomputed across 2+ downstream stages (missing `persist`)
- Shuffle bytes ≫ post-stage output bytes (carrying columns you'll throw away)
- A join exists purely to set a boolean / derive a small flag
- `skew` shows one partition with 50%+ of stage data and it's not a shuffle join AQE can fix
- You've plateaued on conf and the wall is still dominated by 1-2 CPU-bound stages

## When to read [`common-bottlenecks.md`](common-bottlenecks.md) FIRST instead

- `ramp` shows 0 executors for 5+ minutes at the start (cold pool — conf-only)
- `executors` peak is much less than what you requested (pool under-delivery — conf-only)
- Logs show executor JVM OOM (memory conf)
- ADLS HTTP 503 throttle on final write (write fan-out conf)
- One specific host's tasks are consistently slowest (noisy host — speculation conf)

If you're unsure: read this doc first. Reading the source of the dominant stage rarely takes more than 30 minutes and either surfaces an obvious algorithmic win or rules code out so you can confidently move to conf.

The patterns below are ordered by **risk × effort**, from drop-in surgical edits to algorithmic rewrites.

---

## Is a code rewrite worth pursuing? (triage)

Run this checklist against your current best round before touching code:

| Signal | What it means | Look at |
|---|---|---|
| 1-2 stages = >60% of wall, CPU% > 80% on those stages | You're compute-bound, not infra-bound. Conf can't help. | Patterns 1–11 |
| `compare overview` deltas flat across last 3 conf rounds | Conf lever is exhausted | Any pattern |
| Window stage's CPU% > 80% and partition count is already high | Window is doing the work it's supposed to do. Question whether the window itself is necessary. | Patterns 5, 6, 7 |
| One df is consumed by 2+ downstream stages and recomputed both times | Missing persist | Pattern 4 |
| Stage shuffle bytes ≫ post-stage output bytes | Carrying columns through shuffle you'll throw away | Pattern 3 |
| A join exists purely to set a boolean / derive a small flag | Join can be replaced with a column expression | Pattern 1 |
| `skew` shows one partition handles 50%+ of stage data | Skew that AQE can't fix because it's not a shuffle join | Patterns 10, 11 |
| You're doing `explode(...)` followed by `leftouter` then filter | You're generating rows you will throw away | Pattern 6 |

If none of these match AND the dominant stages are clearly infra-bound (long ramp, OOM, ADLS throttle), start at [`common-bottlenecks.md`](common-bottlenecks.md) instead. If you can't tell, read the source of the dominant stage anyway — the audit itself is the fastest classifier.

---

## Pattern 1: Eliminate redundant joins by deriving from existing columns

**When to use:** A join exists solely to produce a flag column or derived value that can be computed from columns already in the DataFrame.

**Symptom in eventlog:** Extra Exchange + SortMergeJoin/BroadcastHashJoin node in the physical plan whose output is then immediately consumed by a `withColumn` setting a boolean.

**Before:**
```python
# joins df to flagSource purely to set IsActive based on whether a match exists
result = df.join(flagSource, joinCols, "leftouter") \
           .withColumn("IsActive", F.col("flag_marker").isNotNull())
```

**After:** If `df` came from a prior leftouter join (line N) and the column you'd check on `flagSource` corresponds to a column already nullable from that prior join, just check that column directly:
```python
# Prior step: df = mainDf.join(sparseDf, joinCols, "leftouter")
# Any row that didn't match sparseDf already has sparseDf's cols as NULL.
result = df.withColumn("IsActive", F.col("a_col_from_sparse").isNotNull())
```

**Risk:** Low. Validate by computing the boolean both ways on a sample and asserting `F.sum(F.col("IsActive_new") != F.col("IsActive_old")) == 0`.

**Example:**
```python
# Before — line 172 (extra join + broadcast)
interpolationAllDatesDf = interpolationAllDatesDf \
    .join(businessDateDf, joinCols, "leftouter") \
    .withColumn("IsInterpolated", F.col("YearWeek").isNull())

# After (Date_Selected was already added by the leftouter at line 167;
# rows missing from interpolationDf have Date_Selected = NULL)
interpolationAllDatesDf = interpolationAllDatesDf \
    .withColumn("IsInterpolated", F.col("Date_Selected").isNull())
```
Removes a shuffle/broadcast that was running on every row of a ~10M-row dense grid.

---

## Pattern 2: Catalyst CollapseWindow hint — share a base Window across multiple frames

**When to use:** Multiple `Window.partitionBy(...).orderBy(...).rowsBetween(a, b)` definitions in the same query that differ only in frame bounds. Catalyst's `CollapseWindow` rule *should* merge them but often plans them as separate Window exec nodes (= separate shuffles) when the frames differ.

**Detect:** SQL plan has 2+ `Window` exec nodes back to back over the same partition + order spec.

**Before:**
```python
window_ff = Window.partitionBy(*dims).orderBy(date).rowsBetween(-sys.maxsize, 0)
window_bf = Window.partitionBy(*dims).orderBy(date).rowsBetween(0, sys.maxsize)

df = df.withColumn("ff", F.last(col, ignorenulls=True).over(window_ff)) \
       .withColumn("bf", F.first(col, ignorenulls=True).over(window_bf))
```

**After:** Define one base window, apply the frame on each `.over()` call. Stack all `withColumn` calls in one expression chain so Catalyst sees them in the same logical step:
```python
w_base = Window.partitionBy(*dims).orderBy(date)
df = df.select(
    "*",
    F.last(col, ignorenulls=True).over(w_base.rowsBetween(Window.unboundedPreceding, Window.currentRow)).alias("ff"),
    F.first(col, ignorenulls=True).over(w_base.rowsBetween(Window.currentRow, Window.unboundedFollowing)).alias("bf"),
)
```

**Risk:** Very low — pure refactor. Output identical; only the physical plan changes.

**Effect:** When CollapseWindow merges, one shuffle instead of two. On a heavy 21-col partition window over 10M+ rows this typically halves the most expensive stage.

---

## Pattern 3: Narrow projection before a heavy shuffle

**When to use:** A heavy shuffle stage (window, join, groupBy) carries columns that aren't needed downstream from that stage. The shuffle's network bytes are dominated by columns you'll drop later.

**Detect:** Compare stage `shuffle_write_bytes` vs the bytes of the columns the next stage actually consumes. If the ratio is >2×, you're carrying dead weight.

**Before:**
```python
# df has 30 columns, but the window only needs 5 of them, and the final select keeps 8.
df.withColumn("ff", F.last("value").over(w)).select(*finalCols)
```

**After:** Project to only what the heavy stage needs, plus the keys to rejoin afterward:
```python
# Hot path: only the columns needed by the window
hot = df.select(*windowKeys, "value")
hotResult = hot.withColumn("ff", F.last("value").over(w))

# Bring back the other columns via a co-partitioned join on the same keys
result = df.join(hotResult.select(*windowKeys, "ff"), windowKeys, "inner").select(*finalCols)
```

**Risk:** Low. The extra join is cheap because both sides are already partitioned on the window keys.

**When NOT to use:** If the columns you'd drop are tiny (a few short strings or ints), the savings won't justify the extra join.

---

## Pattern 4: Persist DataFrames consumed by multiple downstream stages

**When to use:** A DataFrame that takes meaningful work to materialize (heavy filter, aggregation, multi-join) is referenced as the input to two or more downstream operations. Without persist, Spark recomputes the upstream lineage for each downstream consumer.

**Detect:**
```bash
python scripts/spark_eventlog_analyze.py stages $LOG --top 20
# Look for: the same SQL exec ID appearing as a source for 2+ later stages,
# with the same shuffle hash signatures
```

You can also see the symptom in code: search for variables assigned once and then used in 2+ subsequent `.join(...)`, `.union(...)`, `.write(...)`, or `withColumn(... over windowOnThisDf)` calls.

**Before:**
```python
heavyDf = (rawDf
    .filter(...)
    .groupBy(*dims).agg(...)
    .join(otherDf, joinCols)
)
windowResult = heavyDf.withColumn("ff", F.last("value").over(w))
joinResult   = heavyDf.join(thirdDf, otherJoinCols)  # heavyDf recomputed here
```

**After:**
```python
from pyspark import StorageLevel

heavyDf = (rawDf
    .filter(...)
    .groupBy(*dims).agg(...)
    .join(otherDf, joinCols)
).persist(StorageLevel.MEMORY_AND_DISK)

windowResult = heavyDf.withColumn("ff", F.last("value").over(w))
joinResult   = heavyDf.join(thirdDf, otherJoinCols)

# (optional) unpersist after the last consumer materializes
windowResult.write...  # action triggers caching
heavyDf.unpersist()
```

**Risk:** Low. Use `MEMORY_AND_DISK` (not `MEMORY_ONLY`) so memory pressure spills instead of recomputing or OOM'ing. Verify peak storage memory in the executors tab fits within available cache memory.

Persist only when the lineage is reused and expensive enough to outweigh serialization, storage, and eviction costs. Confirm the effect with a plan and event-log comparison.

---

## Pattern 5: Split a heavy window — extract per-key constants into a groupBy

**When to use:** Inside one giant window operation, some of the window functions are equivalent to a per-key aggregate (e.g., forward-fill of a column whose value is **invariant within the partition key**). Those columns don't need to be in the window at all; they can be computed once per key via `groupBy` and joined back.

**Detect:** Read the source code of the window stage. For each `F.last(col, ignorenulls=True).over(w)`-style call, ask: *does `col`'s value vary across rows within the same partition?* If no, it's a constant.

**Before:**
```python
# constantCols are the same value for every row of a given dim — but they're included
# in a 21-column-partition window, weighing down the shuffle.
for c in constantCols:
    df = df.withColumn(c, F.last(c, ignorenulls=True).over(window_ff))
```

**After:**
```python
# Compute one value per dim, then broadcast-eligible (or SMJ if too big) join
constDf = df.groupBy(*dims).agg(
    *[F.first(c, ignorenulls=True).alias(c) for c in constantCols]
)
# Drop the original constant cols and bring them back via join
df = df.drop(*constantCols).join(F.broadcast(constDf), dims, "leftouter")
```

**Risk:** Low–medium. Verify the constants really are invariant per key:
```python
df.groupBy(*dims).agg(*[F.countDistinct(c).alias(f"{c}_dc") for c in constantCols]) \
  .filter(F.greatest(*[F.col(f"{c}_dc") for c in constantCols]) > 1) \
  .count()  # must be 0
```

**Effect:** Removes N window functions from the heavy stage (where N = number of true constants). On wide-key windows over big data, this can shave 15–25% off the dominant stage.

---

## Pattern 6: Replace explode-then-window-fill with a gap-targeted join (asof pattern)

**When to use:** The pipeline (1) explodes a key×date grid, (2) leftouter-joins sparse facts in, (3) runs a window to forward/back-fill the NULL gaps, (4) filters back to only the rows that were in actual gaps. The window step is the dominant cost.

This is the most common "expensive window" anti-pattern in time-series interpolation, slowly-changing-dimension fill, and event sessionization workloads.

**Detect:** Look for this code shape:
```python
grid = keys.withColumn("date", F.explode(F.expr("sequence(MinDate, MaxDate, interval 1 day)")))
densified = grid.join(sparseFacts, [...], "leftouter")
filled    = densified.withColumn("ff", F.last("value", ignorenulls=True).over(w)) \
                     .withColumn("bf", F.first("value", ignorenulls=True).over(w))
result    = filled.filter("...IsInGap == True...")
```

In the eventlog, this stage has:
- Very wide partition key (many cols)
- 4-digit task count
- CPU% > 80% on the slow tasks
- Stage duration dominated by skewed task durations (some partitions have huge date ranges)

**Before (sketch):**
```python
# 1. Dense grid: every (dim, calendar_date) pair
allDates = dimDf.withColumn(dateCol, F.explode(F.expr("sequence(MinDate, MaxDate, interval 1 day)")))

# 2. Leftouter to bring in sparse facts (most rows get NULL)
dense = allDates.join(sparseFacts, [*dims, dateCol], "leftouter")

# 3. The expensive window
w_ff = Window.partitionBy(*dims).orderBy(dateCol).rowsBetween(-sys.maxsize, 0)
w_bf = Window.partitionBy(*dims).orderBy(dateCol).rowsBetween(0, sys.maxsize)
dense = dense.withColumn("Date_ff", F.last("Date_Selected", True).over(w_ff)) \
             .withColumn("Date_bf", F.first("Date_Selected", True).over(w_bf)) \
             .withColumn("value_ff", F.last("value", True).over(w_ff)) \
             .withColumn("value_bf", F.first("value", True).over(w_bf))

# 4. Filter to only the gap rows (most rows discarded)
gapRows = dense.where("IsInterpolated == True AND Date_ff == ExpectedPrev AND Date_bf == ExpectedNext")
```

**After:** Generate ONLY the gap rows, then join twice to look up ff/bf values directly:
```python
# 1. Compute the gap skeleton ONCE: per partition, for each non-observed date,
#    the (prev_observed, next_observed) pair. This is usually a small dataframe
#    (number of gap dates is much smaller than the full calendar grid).
gapSkeleton = (
    calendarPerPartition
    .join(F.broadcast(observedDatesPerPartition.alias("o")), partKeys + [dateCol], "leftouter")
    .withColumn("IsObserved", F.col("o." + dateCol).isNotNull())
    .where("IsObserved == False")
    # via Window once: derive Prev/Next from neighboring observed dates
    .withColumn("Date_Prev", F.lag(...).over(...))
    .withColumn("Date_Next", F.lead(...).over(...))
    .select(*partKeys, dateCol, "Date_Prev", "Date_Next")
)

# 2. Expand by dim
gapRows = dimDf.join(F.broadcast(gapSkeleton), partKeys, "inner")
# columns: dims..., dateCol, Date_Prev, Date_Next

# 3. Look up ff and bf values via two joins on the SAME keys (Spark co-partitions)
ffLookup = sparseFacts.select(*dims, F.col(dateCol).alias("Date_Prev"),
                              *[F.col(c).alias(f"{c}_ff") for c in factCols])
bfLookup = sparseFacts.select(*dims, F.col(dateCol).alias("Date_Next"),
                              *[F.col(c).alias(f"{c}_bf") for c in factCols])
gapRows = gapRows.join(ffLookup, dims + ["Date_Prev"], "leftouter") \
                 .join(bfLookup, dims + ["Date_Next"], "leftouter")

# 4. Interpolate (same math) — no window needed
result = self.interpolate(gapRows, dateCol, "Date_Prev", "Date_Next", ...)

# 5. Union with the observed rows
final = result.union(sparseFacts.withColumn("IsInterpolated", F.lit(False)))
```

**Why this is dramatically faster:**
- The dominant cost in the "before" version is the wide-key window over the dense grid. The "after" version replaces that window with two equi-joins on `(dim, date)`. Both joins shuffle on the SAME key, so the second is essentially free (already co-partitioned).
- Row count flowing through the heavy stage drops from `dim_count × days_in_range` to `dim_count × gap_count`. For most real workloads `gap_count / days_in_range` is 0.1–0.3.
- No more wide-partition window = no more skewed-data outliers blocking the wall.

**Risk:** Medium. Math is unchanged (same interpolate function), but you're restructuring the pipeline. Validation strategy:
- Schema diff: `set(before.columns) == set(after.columns)`
- COUNT match per `(partKey, date)` group
- `sum(value)` match per `(partKey, date)` group
- Distinct dim-key count match
- Spot-check 50 random gap rows for identical `value_ff`/`value_bf`/`interpolated_value`

**Expected effect:** the rewrite removes the wide-key fill window and reduces the rows entering the expensive path. The actual improvement depends on gap density, join cardinality, key width, partitioning, and storage behavior; validate correctness and compare event logs before making a performance claim.

**Lesson:** when both code and event-log evidence show this pattern, test the algorithmic rewrite before continuing broad configuration tuning.

---

## Pattern 7: Domain-aware bypass — skip rows that the algorithm provably leaves unchanged

**When to use:** Some subset of input rows passes through an expensive operation unchanged. You can detect this subset cheaply with metadata, route them around the heavy operation, and union them back at the end.

**Common cases:**
- Interpolation: dim combinations with **no gaps** in their date range — the window produces the same value that was already there.
- Deduplication: keys that already appear exactly once — no dedupe needed.
- Slowly-changing-dimension merge: source rows whose business key + hash matches the target — already up to date.
- Any `coalesce(a, b)` where `a is NEVER NULL` for a known partition — `b` lookup is unnecessary.

**Detect:** Read the heavy operation's logic and identify the early-exit predicate. For interpolation: trace `interpolate()` — if `is_interpolated == False`, it returns `value_col` unchanged.

**Pattern:**
```python
# 1. Cheap eligibility check (metadata aggregation, no heavy compute)
eligibleKeys = bypassEligibilityCheck(df)   # e.g., dims with zero gaps

# 2. Split
bypass  = df.join(F.broadcast(eligibleKeys), keyCols, "leftsemi")
needsWork = df.join(F.broadcast(eligibleKeys), keyCols, "leftanti")

# 3. Heavy operation runs ONLY on rows that need it
processed = heavyOperation(needsWork)

# 4. Adapt bypass schema to match processed schema (add the missing cols as identity / NULL)
bypass = bypass.withColumn("IsInterpolated", F.lit(False)) \
               .withColumn("value_orig", F.col("value")) \
               .select(*processed.columns)

# 5. Union — no shuffle needed because both sides are already partitioned by keyCols
result = processed.unionByName(bypass)
```

**Risk:** Medium. The bypass branch must produce output schema- and value-identical to what the heavy operation would have produced for those rows. Always validate:
- Run the heavy operation on a sample of bypass-eligible rows
- Assert byte-for-byte equality with the bypass branch output

**Effect:** Wall time scales with `1 - bypass_ratio`. If 60% of rows are eligible, wall drops ~50% (heavy stage row count cut 60%, but some overhead remains).

**Caveat:** Only worth it if `bypass_ratio` is materially > 0. Measure with a one-off eligibility analyzer notebook before committing to the refactor.

---

## Pattern 8: Pre-filter window/aggregation input to "needs-work" rows

**When to use:** A window or aggregation operates on rows that include many "trivial" rows that don't affect the result. Filter those out, do the heavy work on a smaller set, and rejoin or union.

**Difference from Pattern 7:** Pattern 7 is about whole partitions you can skip end-to-end. Pattern 8 is about removing irrelevant rows WITHIN partitions you still need to process.

**Example:**
```python
# Before: window runs over all 10M rows, but only rows with value IS NOT NULL contribute
df = df.withColumn("ff", F.last("value", ignorenulls=True).over(w))

# After: do the window on only the contributing rows, then re-densify
sparse = df.where("value IS NOT NULL").select(*partKeys, dateCol, "value")
filled = sparse.withColumn("ff", F.last("value").over(w))  # smaller df, faster window
df = df.join(filled.select(*partKeys, dateCol, "ff"), partKeys + [dateCol], "leftouter")
```

**Risk:** Medium. Verify the trivial rows really don't affect the window output (for `ignorenulls=True` aggregates, NULL rows are indeed ignored, so the optimization is safe). Be careful with ordering-sensitive functions.

---

## Pattern 9: Replace cross-product + filter with a direct join on the constraining table

**When to use:** Code constructs a cross product (or near-cross product) and then filters down using a predicate that could have been expressed as a join key.

**Before:**
```python
result = a.crossJoin(b).where("a.region == b.region AND a.date >= b.startDate AND a.date <= b.endDate")
```

**After:** If the predicate is an equi-condition, use a join:
```python
result = a.join(b, "region").where("a.date >= b.startDate AND a.date <= b.endDate")
```

For range-only predicates with no equi-key, use a range-bucketed join (compute a bucket col on both sides, equi-join on bucket, then filter the residual range predicate).

**Risk:** Low when an equi-key exists; medium for range-bucketed joins.

---

## Pattern 10: Two-stage aggregation for skewed groupBy

**When to use:** A `groupBy(*keys).agg(...)` is skewed: one or two keys have orders of magnitude more rows than the rest. AQE skewJoin doesn't help because this is an aggregate, not a join.

**Detect:**
```bash
python scripts/spark_eventlog_analyze.py skew $LOG --top 5
# Aggregation stages with max/median > 10 and a few partitions doing most of the work
```

**Before:**
```python
agg = df.groupBy(*keys).agg(F.sum("value").alias("total"))
```

**After:** Add a random salt to break up the hot keys, aggregate twice:
```python
N = 32  # tune: 16–64 typical
salted = df.withColumn("_salt", (F.rand() * N).cast("int"))
preAgg = salted.groupBy(*keys, "_salt").agg(F.sum("value").alias("_partial"))
agg = preAgg.groupBy(*keys).agg(F.sum("_partial").alias("total"))
```

**Risk:** Low for sum/count/min/max (associative + commutative). Do NOT use for `collect_list`/`collect_set` if order matters, or for percentile aggregates (re-aggregation is not exact).

---

## Pattern 11: Salt the join key for hot-key skew

**When to use:** A SortMergeJoin is skewed on a single hot key (one value of the join key dominates). AQE skewJoin handles many cases, but for extreme skew (>50% of rows on one key) or when AQE's threshold is mistuned, salting is the explicit fix.

**Before:**
```python
result = big.join(small, "key")   # one key value has 50%+ of rows
```

**After:**
```python
N = 8  # tune by hot-key cardinality
# Salt the big side with a random suffix
big_s = big.withColumn("key_salted", F.concat(F.col("key"), F.lit("_"), (F.rand() * N).cast("int").cast("string")))
# Replicate the small side N times, one per salt value
small_s = small.withColumn("_salt", F.explode(F.array(*[F.lit(i) for i in range(N)]))) \
               .withColumn("key_salted", F.concat(F.col("key"), F.lit("_"), F.col("_salt").cast("string"))) \
               .drop("_salt")
result = big_s.join(small_s, "key_salted").drop("key_salted")
```

**Risk:** Medium. The small side row count multiplies by N — make sure it still fits broadcast threshold if you want a BHJ, or that the resulting shuffle is acceptable. Validate join cardinality matches the unsalted version.

---

## Pattern 12: Hoist filter predicates above joins (and reorder joins)

**When to use:** Filters are applied AFTER joins when they could be applied before, reducing the input to the join.

**Detect:** Read the SQL plan — `Filter` nodes appearing above join nodes whose predicate references only one side's columns.

**Before:**
```python
result = a.join(b, "key").where("a.dt > '2024-01-01' AND a.region == 'US'")
```

**After:**
```python
aFiltered = a.where("dt > '2024-01-01' AND region == 'US'")
result = aFiltered.join(b, "key")
```

**Note:** Catalyst usually does this for you (`PushDownPredicate`), but the rule can fail when the filter predicate references a column produced by a `withColumn` between the source and the join, or when the predicate uses a UDF.

**Risk:** Very low — same result, just earlier.

---

## Anti-patterns: code rewrites that LOOK helpful but usually aren't

These commonly fail or introduce correctness risk:

### A1. Surrogate hash key for a wide window partition (e.g., `xxhash64` over 20 cols)
**Why it fails:** xxhash64 (and similar non-cryptographic 64-bit hashes) have structural collision risks with nullable columns of the same type. Two different dim tuples like `(x, NULL, ..., NULL)` and `(NULL, ..., NULL, x)` can map to the same hash. Even a single collision corrupts the window result. Random birthday math at 10M keys gives ~5e-6 collision probability, but structural null-handling collisions can be much higher.

If you must reduce wide-key shuffle CPU, use:
```python
F.sha2(F.to_json(F.struct(*cols), {"ignoreNullFields": "false"}), 256)
```
which is collision-safe but adds ~10–20% CPU for the hash itself. Often not worth it; prefer Patterns 5 or 6 instead.

### A2. `repartition(N, *dims)` immediately before a window over the same dims
**Why it fails:** The window operation already inserts an Exchange on those dims. Your explicit repartition becomes a second, redundant shuffle. Spark sometimes optimizes the duplicate away, but if N differs from `spark.sql.shuffle.partitions`, you've forced two shuffles.

Use this only when you need a SPECIFIC partition count that differs from the default (e.g., before a write with bounded file count).

### A3. `df.cache()` everywhere
**Why it fails:** Cache only helps when a DataFrame is consumed multiple times AND the upstream cost is high AND the cached size fits. Cache on a one-shot pipeline adds serialization overhead with no benefit and can push other data out of memory. Use Pattern 4 deliberately, not reflexively.

### A4. `coalesce(1)` before a write
**Why it fails:** Single-task writes serialize the entire dataset through one executor. For anything over a few hundred MB this is dramatically slower than the parallel write. Use `repartition(N)` with N tuned for 256 MB–1 GB per output file.

### A5. Rewriting a window as a self-join
**Why it fails:** `df.join(df, ...)` against the same DataFrame typically materializes both sides through the same lineage and can quadratically blow up shuffle bytes. Spark's window operator is purpose-built and almost always wins. Only consider this if the window has 5+ functions with different partition specs that can't be collapsed.

---

## Validation checklist for any code rewrite

Before merging any rewrite from this doc, run these checks against the original:

1. **Row count match:** `before.count() == after.count()`
2. **Schema match:** `set(before.dtypes) == set(after.dtypes)`
3. **Per-key sum match:** for numeric output cols, sums per business key match within float tolerance
4. **Per-key count match:** `before.groupBy(*keys).count()` == `after.groupBy(*keys).count()` for all keys
5. **Spot check:** sample 50–100 random keys; full row equality

For the **gap-targeted join rewrite (Pattern 6)** specifically, also:
- Distinct (dim × date) tuple count match (catches accidental row duplication from misaligned joins)
- For each interpolated row: `value_ff`/`value_bf`/`Date_ff`/`Date_bf` identical
- Edge cases: dims with exactly 1 observed date; dims with no gaps; dims with gaps at series boundaries

Run validation on a representative cycle's data before committing, then re-run on production data after deploying behind a feature flag if available.

---

## When to stop and ship

Code rewrites have a steeper risk curve than conf changes. After each round:

| Wall reduction this round | Action |
|---|---|
| > 15% | Big win, ship and measure 2–3 more runs to confirm stability |
| 5–15% | Worthwhile, ship |
| 1–5% | Marginal — run 2–3 times to rule out cold-pool variance before shipping |
| < 1% or regression | Revert; the rewrite didn't help OR validation gap is hiding a regression |

A rewrite that yields a 3% wall improvement but introduces correctness risk is **not** a win. The conf-tuning round-over-round attribution rule still applies: one rewrite per round, measure independently.
