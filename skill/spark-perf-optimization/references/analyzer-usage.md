# Spark Eventlog Analyzer — Usage Reference

`spark_eventlog_analyze.py` is a single, reusable Python CLI for analyzing Spark eventlogs (Synapse, OSS Spark, Databricks — any application that writes JSON-line eventlogs). Pure stdlib, no pip install required.

## Input format

Pass either:
- A Spark eventlog file directly, e.g. `application_example_0001`
- A directory containing one (the script picks the first `application_*` file inside). Useful when you unzip a Synapse log bundle into a folder and pass the folder.

Gzip files and ZIP archives are streamed directly. Extract `.zst` and `.lz4` inputs first.

## Command shape

```
python spark_eventlog_analyze.py <subcommand> <log> [options]
```

Subcommand always comes first. Single-log subcommands take one positional log; `compare` takes one subcommand name followed by 2+ logs.

Get the full subcommand list any time with `python spark_eventlog_analyze.py --help`.

## Subcommand reference

### `overview <log>`
High-level application summary: app id, total runtime, executor count and peak, top SQL executions by duration, top stages by wall time, and a naive efficiency calculation (useful core-min / capacity). Use this first on every new eventlog.

### `executors <log>`
Executor topology: total added, distinct hosts, cores per executor, packing density (execs per host), peak concurrent, removal reasons, Spark task slots at peak. Answers *"did the pool actually deliver the executors I asked for?"*.

### `ramp <log> [--interval SEC]`
Text chart of executor count over time. Default sample interval is 120s; use `--interval 60` for finer detail. Each row prints `T+MM.Xm: N execs ###...` so you can eyeball how long it took to ramp.

### `stages <log> [--top N] [--min-min M]`
All stages sorted by wall time descending. Shows tasks, shuffle read/write, output records, sum of executor run time, failure/speculation counts. Use `--min-min 0.5` to hide stages shorter than 30s.

### `stage <log> STAGE_ID`
Per-task deep dive for one stage. Shows:
- Time metrics (duration, run, CPU, GC, fetch wait, scheduler delay) with min/p50/p90/p99/max distributions
- Memory metrics (peak exec memory, memory spill, disk spill)
- Shuffle metrics (local/remote bytes read, shuffle write)
- I/O metrics (input/output bytes)
- CPU/GC/fetch utilization percentages of run time
- Time breakdown across all tasks (where the hours go)
- Tasks per executor (catches uneven distribution)
- Top 10 slowest tasks with executor and host

This is the single most useful diagnostic for any bottleneck stage.

### `skew <log> [--top N]`
Per-stage skew ratio (max task duration / median). High ratios indicate one or two tasks are 100x+ slower than the rest, typically due to partition skew. Combine with `stage` deep dive on the worst offender.

### `shuffle <log> [--top N]`
Stages by total shuffle bytes (read + write). Useful for identifying which stages drive network and disk pressure.

### `spill <log> [--top N]`
Stages with memory and/or disk spill. If you see spill, memory pressure is real — either too few executors, too small per-executor heap, or bad partition sizing.

### `speculation <log>`
Total speculative tasks launched, kills (any reason vs speculation reason), and per-stage breakdown of speculation activity. Use this to verify speculation is firing where you expect it to.

### `output <log> [--top N]`
Stages by output records and bytes. Quick way to find your final write stages.

### `sql <log>`
All Spark SQL executions with start/end times relative to app start and duration. Sorted by duration descending. Use the SQL id with `plan`.

### `plan <log> SQL_ID`
Print the captured physical plan (first 4KB) for one SQL execution. Pair with `sql` to find the SQL id you want.

### `all <log>`
Convenience: runs overview + executors + ramp + stages + skew + shuffle + spill + speculation in one shot. Pipe to a file for a complete dossier.

### `compare <subcommand> <log1> <log2> [<log3> ...] [--top N]`
Runs the named subcommand on each log, then prints a side-by-side summary table at the end (wall_min, peak_exec, hosts, spec_tasks, failed). Currently supports: overview, executors, stages, skew, shuffle, spill, speculation, sql. **The most useful subcommand for round-over-round perf comparisons.**

## Quick examples

```powershell
$PY  = "scripts\spark_eventlog_analyze.py"
$R1  = "C:\path\to\round1\application_example_0001"
$R2  = "C:\path\to\round2\application_example_0002"

# First look at a new run
python $PY overview $R1

# Did the pool actually deliver the executors?
python $PY executors $R1
python $PY ramp $R1 --interval 60

# Slowest stages, then drill into the slowest one
python $PY stages $R1 --top 10
python $PY stage  $R1 610

# Is skew a problem?
python $PY skew $R1 --top 10

# Is speculation firing?
python $PY speculation $R1

# Full dossier to a file
python $PY all $R1 > round1_dossier.txt

# Compare two rounds head-to-head
python $PY compare overview  $R1 $R2
python $PY compare executors $R1 $R2
python $PY compare skew      $R1 $R2

# Pull a physical plan for a SQL execution identified above
python $PY sql  $R1
python $PY plan $R1 <SQL_ID>
```

## Interpreting `stage` deep-dive output

The deep dive prints distributions for ~25 task-level metrics. What to look for:

**CPU / run percentage**
- `> 90%`: tasks are CPU-bound. Adding more executors won't help individual task speed; only more parallelism reduces total time.
- `< 30%`: tasks are I/O-bound (shuffle fetch, disk, network). More executors help by spreading the I/O load.

**GC / run percentage**
- `< 5%`: healthy.
- `> 15%`: tasks are GC-thrashing. Bump executor memory, or shrink partition size by increasing `shuffle.partitions` / repartition count.

**fetch_wait / run percentage**
- `< 5%`: shuffle is healthy.
- `> 30%`: tasks are blocking on shuffle reads. Could indicate insufficient `shuffle.partitions`, or skew (a few partitions hog the network).

**unaccounted in run**
- Time inside Executor Run Time not explained by CPU + GC + fetch. Usually this is I/O write wait (especially in final parquet write stages going to ADLS). If high (>50% of run time), the bottleneck is the sink, not Spark.

**Tasks per executor**
- Healthy: each executor handles roughly the same task count and total duration.
- Unhealthy: one or two executors have 5-10x the work. Investigate locality preferences and partition keys.

**Top 10 slowest tasks**
- Look for clustering on one host or executor (suggests a slow node, rare these days but possible on shared infra). Look for retries (`att=1`+) which indicate task failure under load.

## Tips

- Big eventlogs (1-5 GB) take 30-90s to parse. The script is single-pass per invocation. If you'll run many subcommands, just use `all` once and search the output.
- Capture an entire round's analysis: `python scripts/spark_eventlog_analyze.py all $LOG > round_NN.txt`, then diff two rounds with your favorite diff tool.
- The `compare` summary table at the end is the most condensed apples-to-apples view across rounds. Drop it into PR descriptions and chat updates:
  ```
  log                       wall_min  peak_exec  hosts  spec_tasks  failed
  baseline                       60.0        24     24           0       0
  candidate                      42.0        24     24          18       0
  ```
- For Synapse pool capacity issues specifically: `executors` and `ramp` are your two best friends. They tell you exactly how many executors the pool agreed to give Spark, and how quickly. Anything below the requested `NumExecutors` is a platform-side issue, not a Spark config issue.
- The script captures up to 50,000 task durations per stage to compute skew. For stages with more tasks than that, skew is approximate but still trustworthy.

## Extending the analyzer

The script is structured as:
1. `ParsedLog` class — single-pass eventlog parser; accumulates aggregates per stage and (optionally) raw tasks for one filtered stage.
2. `cmd_*` functions — each subcommand is a separate handler that pulls what it needs from a `ParsedLog`.
3. `build_parser()` + `main()` — argparse wiring.

To add a new analysis, add a `cmd_yourname(p: ParsedLog, ...)` function and register it in `build_parser()` and `main()`. The parser already collects nearly everything useful from each `SparkListenerTaskEnd` event, so most new analyses don't require parser changes.

Useful raw data already available on `ParsedLog`:
- `.app_start`, `.app_end`, `.app_id`, `.app_name`
- `.execs` — dict: exec_id -> {host, cores, added_ts, removed_ts, removed_reason}
- `.stages` — dict: stage_id -> {sub, end, ntasks, name, details, parents, failure}
- `.task_per_stage` — dict: stage_id -> per-stage aggregates
- `.sql_starts`, `.sql_ends`, `.sql_desc`, `.sql_plans` — dicts keyed by sql_id
- `.killed_reasons` — Counter
- `.filtered_tasks` — list of full task records, populated only if `stage_filter` passed
