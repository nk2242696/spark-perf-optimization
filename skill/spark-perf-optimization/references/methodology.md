# Methodology — Iterative Spark Performance Optimization

This five-step loop is designed for production Spark workloads where each recommendation must be traceable to event-log evidence. Keep raw logs and identifiable workload details private, and publish only sanitized measurements that have an explicit source.

The 5-step loop, in detail. Each step has concrete actions and exit criteria.

## Step 1: Capture

**Goal:** Get a clean eventlog from a representative run.

**Actions:**
1. Trigger a job run in a dev/test environment (the same Spark pool as production if possible).
2. On Synapse, wait for the job to complete, then download the Spark eventlog from the Synapse Studio job history page. It comes as a zip.
3. Unzip into a known folder. The zip contains an `application_*` file (and sometimes additional files like `eventLog.json`).
4. Name the folder after the round number: `eventlog-round-01`, `eventlog-round-02`, etc.

**Exit criteria:** You have an extracted `application_*` file on disk and know its absolute path.

**Pitfalls:**
- A failed/incomplete run produces an `application_*.inprogress` file that the analyzer will skip. If you need to analyze a failed run, rename it (drop `.inprogress`).
- Synapse pool state (warm vs cold) heavily affects wall time. Compare runs on the same pool state when possible, or use cold-pool baselines.

## Step 2: Analyze

**Goal:** Surface bottlenecks with concrete metrics.

**Actions:** Run the analyzer in this order. Each subsequent command narrows the search.

```bash
PY=scripts/spark_eventlog_analyze.py
LOG=eventlog-round-XX/application_xxxxx_0001_1

python $PY overview  $LOG    # wall time, peak execs, top SQLs, top stages
python $PY executors $LOG    # what did the pool deliver?
python $PY ramp      $LOG --interval 60   # how fast did execs arrive?
python $PY stages    $LOG --top 10        # what dominates wall time?
```

For any stage in the top 10, do a deep dive:

```bash
python $PY stage $LOG <STAGE_ID>          # per-task metrics
python $PY skew  $LOG --top 10            # is it a skew problem?
python $PY spill $LOG                     # is it memory pressure?
python $PY speculation $LOG               # is speculation firing?
```

**Exit criteria:** You can name the top 2-3 contributors to wall time and the root cause (CPU-bound? I/O? Skew? Idle waiting? Memory pressure?).

## Step 3: Diagnose

**Goal:** Compare to the prior round to validate progress (or identify regression).

**Actions:**
```bash
python $PY compare overview    $PREV $CURR
python $PY compare executors   $PREV $CURR
python $PY compare skew        $PREV $CURR
python $PY compare speculation $PREV $CURR
```

The summary table at the bottom of `compare` is the single most important artifact for the round. It looks like:

```
log                       wall_min  peak_exec  hosts  spec_tasks  failed
baseline                       60.0        24     24           0       0
candidate                      42.0        24     24          18       0
```

**Interpretation:**
- Wall time delta is the headline number, but cold-pool variance is real (±30%). Don't over-interpret a single ~10% swing.
- `peak_exec` tells you whether your last conf change affected pool delivery.
- `spec_tasks` going up while `failed` stays at 0 = speculation is firing, which is good if those stages are slow.
- `failed` going up = regression — investigate immediately.

**Exit criteria:** You have evidence-based attribution of any wall-time delta to a specific cause (or you have ruled out causes and declared the delta noise).

## Step 4: Fix

**Goal:** Apply ONE targeted change. Commit and push.

**Actions:**
1. Pick the highest-leverage bottleneck from Step 2.
2. Look it up in [common-bottlenecks.md](common-bottlenecks.md) for the canonical fix.
3. Make the smallest possible change — usually one Spark conf line OR one code line.
4. Commit with a descriptive message that names the round number and the lever:
   ```
  git commit -m "Round NN: <lever changed>" -m "<reasoning>"
   git push origin <branch>
   ```

**Anti-pattern:** Bundling 2+ changes into a single round. If wall time changes, you cannot tell which change caused it. Subsequent rounds must then disambiguate, which wastes runs.

**Exit criteria:** PR branch is updated with exactly one commit attributable to this round's change.

## Step 5: Measure

**Goal:** Re-run with the new change and capture the next eventlog.

**Actions:**
1. Trigger a new dev-env job run on the updated branch.
2. Download and extract the eventlog.
3. Go back to Step 2.

**When to stop iterating:**
- You hit a clear diminishing-returns ceiling (3+ rounds with <5% wall-time delta).
- The bottleneck is no longer in Spark (it's downstream sink throughput, upstream data growth, or pool delivery limits the platform team owns).
- You've achieved your business goal (SLA, cost target).

After stopping:
1. Run [cost-analysis.md](cost-analysis.md) to quantify savings.
2. Use the templates to write the PR description and reviewer summary.
3. Optionally generate a PPTX for stakeholder communication.

## Per-round artifacts

For every round, save (at minimum):
- The full `all` dossier: `python $PY all $LOG > round-NN-dossier.txt`
- The commit SHA on the PR branch
- The wall time and peak_exec from the `overview`

This makes the round-by-round table in the PR description trivial to populate.

## Long-running engagement structure

A multi-round engagement commonly progresses through these stages:

- **Rounds 1-4:** Stabilize — kill OOMs, fix obvious correctness issues. Wall time may go *up* as retries are eliminated; that's fine.
- **Rounds 5-10:** Right-size — pool, executor memory/cores, executor count. Wall time drops sharply.
- **Rounds 11-15:** Tune — AQE, speculation, scheduler. Diminishing single-lever wins.
- **Rounds 16-20:** Polish — repartition counts, file output sizing, conf cleanup.
- **Final:** Validation, PR description, stakeholder summary, cost analysis.

Expect non-monotonic progress. A larger repartition count can, for example, reintroduce storage throttling even when it improves compute parallelism. Revert and try again: a controlled regression is useful evidence.
