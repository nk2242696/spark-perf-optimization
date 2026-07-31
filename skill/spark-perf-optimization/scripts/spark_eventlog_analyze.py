"""Generic Spark eventlog analyzer.

Replaces the per-round/per-stage one-off scripts (analyze_round*.py,
r*_analyze.py, executor_topology.py, stage*_deep.py, r*_stage*.py,
sql_plans*.py, find_top_shuffle.py, round*_spill_check.py, etc.) with a
single CLI tool driven by subcommands.

Usage:
    python spark_eventlog_analyze.py overview LOG
    python spark_eventlog_analyze.py executors LOG
    python spark_eventlog_analyze.py ramp LOG [--interval SEC]
    python spark_eventlog_analyze.py stages LOG [--top N] [--min-min M]
    python spark_eventlog_analyze.py stage LOG STAGE_ID
    python spark_eventlog_analyze.py skew LOG [--top N]
    python spark_eventlog_analyze.py shuffle LOG [--top N]
    python spark_eventlog_analyze.py speculation LOG
    python spark_eventlog_analyze.py spill LOG [--top N]
    python spark_eventlog_analyze.py output LOG
    python spark_eventlog_analyze.py sql LOG
    python spark_eventlog_analyze.py plan LOG SQL_ID
    python spark_eventlog_analyze.py all LOG       # overview + execs + stages + skew + spec + spill
    python spark_eventlog_analyze.py compare <subcmd> LOG1 LOG2 ... [opts]

LOG may be either:
    - a Spark eventlog file (e.g. application_example_0001)
    - a directory containing an `application_*` eventlog file
    - a gzip file or ZIP archive containing an `application_*` event log
    - .zst / .lz4 files must be extracted first

Notes:
    - All numbers from one pass over the log per invocation.
    - Per-task internals (`stage`) only buffer tasks for the requested stage.
"""
from __future__ import annotations

import argparse
import gzip
import io
import json
import os
import sys
import zipfile
from collections import defaultdict
from contextlib import contextmanager
from statistics import median
from typing import Any, Iterator, TextIO


# ---------------------------------------------------------------------------
# Eventlog discovery
# ---------------------------------------------------------------------------
def resolve_log_path(path: str) -> str:
    """Accept a file or a directory; if a directory, look for application_* inside."""
    if os.path.isfile(path):
        return path
    if os.path.isdir(path):
        candidates = sorted(
            f for f in os.listdir(path)
            if f.startswith("application_") and not f.endswith(".inprogress")
        )
        if not candidates:
            candidates = sorted(
                f for f in os.listdir(path) if f.startswith("application_")
            )
        if not candidates:
            raise FileNotFoundError(
                f"No application_* eventlog file found in directory: {path}"
            )
        return os.path.join(path, candidates[0])
    raise FileNotFoundError(f"Path not found: {path}")


@contextmanager
def open_log_text(path: str) -> Iterator[TextIO]:
    """Open a raw, gzip, or ZIP-contained event log as a text stream."""
    lower_path = path.lower()
    if lower_path.endswith(".gz"):
        with gzip.open(path, "rt", encoding="utf-8", errors="ignore") as stream:
            yield stream
        return

    if lower_path.endswith(".zip"):
        with zipfile.ZipFile(path) as archive:
            files = [entry for entry in archive.infolist() if not entry.is_dir()]
            if len(files) > 10_000:
                raise ValueError("ZIP contains too many files (limit: 10,000)")

            expanded_size = sum(entry.file_size for entry in files)
            if expanded_size > 20 * 1024**3:
                raise ValueError("ZIP expanded size exceeds the 20 GB safety limit")

            candidates = [
                entry
                for entry in files
                if os.path.basename(entry.filename).startswith("application_")
                and not entry.filename.endswith(".inprogress")
            ]
            if not candidates:
                candidates = [
                    entry
                    for entry in files
                    if os.path.basename(entry.filename).startswith("application_")
                ]
            if not candidates:
                raise FileNotFoundError("No application_* event log found in ZIP archive")

            selected = max(candidates, key=lambda entry: entry.file_size)
            if selected.flag_bits & 0x1:
                raise ValueError("Encrypted ZIP event logs are not supported")
            if selected.compress_size and selected.file_size / selected.compress_size > 1_000:
                raise ValueError("ZIP entry exceeds the allowed compression ratio")

            with archive.open(selected) as raw_stream:
                with io.TextIOWrapper(raw_stream, encoding="utf-8", errors="ignore") as stream:
                    yield stream
        return

    with open(path, "r", encoding="utf-8", errors="ignore") as stream:
        yield stream


# ---------------------------------------------------------------------------
# Parser - single pass, accumulating everything we might need.
# Pass `stage_filter` to also retain raw per-task records for that stage only.
# ---------------------------------------------------------------------------
class ParsedLog:
    def __init__(self, path: str, stage_filter: int | None = None) -> None:
        self.path = path
        self.stage_filter = stage_filter

        self.app_start: int | None = None
        self.app_end: int | None = None
        self.app_id: str | None = None
        self.app_name: str | None = None

        # executors: id -> {host, cores, added_ts, removed_ts, removed_reason}
        self.execs: dict[str, dict[str, Any]] = {}

        # stages: id -> aggregates
        self.stages: dict[int, dict[str, Any]] = {}

        # per-stage task counters
        self.task_per_stage: dict[int, dict[str, int]] = defaultdict(
            lambda: {
                "total": 0,
                "failed": 0,
                "killed": 0,
                "killed_spec": 0,
                "speculative": 0,
                "retried": 0,
                "sumdur_ms": 0,
                "max_dur_ms": 0,
                "shuffle_read": 0,
                "shuffle_write": 0,
                "input_bytes": 0,
                "output_bytes": 0,
                "records_out": 0,
                "spill_mem": 0,
                "spill_disk": 0,
                "task_durs_ms": [],  # for skew calc; capped below
                "fetch_wait_ms": 0,
                "gc_ms": 0,
                "cpu_ns": 0,
                "run_ms": 0,
            }
        )
        # cap how many durations we retain per stage to keep memory bounded
        self._dur_cap = 50_000

        # SQL executions
        self.sql_starts: dict[int, int] = {}
        self.sql_ends: dict[int, int] = {}
        self.sql_desc: dict[int, str] = {}
        # SQL physical plans: sql_id -> simple plan text (first 2KB)
        self.sql_plans: dict[int, str] = {}

        self.killed_reasons: dict[str, int] = defaultdict(int)

        # When stage_filter is set, accumulate full task records for that stage
        self.filtered_tasks: list[dict[str, Any]] = []

        self._parse()

    def _parse(self) -> None:
        with open_log_text(self.path) as f:
            for line in f:
                try:
                    e = json.loads(line)
                except Exception:
                    continue
                ev = e.get("Event")
                if ev == "SparkListenerApplicationStart":
                    self.app_start = e.get("Timestamp")
                    self.app_id = e.get("App ID")
                    self.app_name = e.get("App Name")
                elif ev == "SparkListenerApplicationEnd":
                    self.app_end = e.get("Timestamp")
                elif ev == "SparkListenerExecutorAdded":
                    eid = e.get("Executor ID")
                    info = e.get("Executor Info") or {}
                    self.execs[eid] = {
                        "host": info.get("Host"),
                        "cores": info.get("Total Cores"),
                        "added_ts": e.get("Timestamp"),
                        "removed_ts": None,
                        "removed_reason": None,
                    }
                elif ev == "SparkListenerExecutorRemoved":
                    eid = e.get("Executor ID")
                    if eid in self.execs:
                        self.execs[eid]["removed_ts"] = e.get("Timestamp")
                        self.execs[eid]["removed_reason"] = e.get("Removed Reason")
                elif ev == "org.apache.spark.sql.execution.ui.SparkListenerSQLExecutionStart":
                    sid = e.get("executionId")
                    self.sql_starts[sid] = e.get("time")
                    self.sql_desc[sid] = (e.get("description") or "")[:120]
                    plan = e.get("physicalPlanDescription") or ""
                    if plan:
                        self.sql_plans[sid] = plan[:4000]
                elif ev == "org.apache.spark.sql.execution.ui.SparkListenerSQLExecutionEnd":
                    self.sql_ends[e.get("executionId")] = e.get("time")
                elif ev == "SparkListenerStageSubmitted":
                    si = e.get("Stage Info") or {}
                    sid = si.get("Stage ID")
                    s = self.stages.setdefault(sid, {})
                    s["sub"] = si.get("Submission Time")
                    s["ntasks"] = si.get("Number of Tasks")
                    s["name"] = (si.get("Stage Name") or "")[:100]
                    s["details"] = (si.get("Details") or "")[:200]
                    s["parents"] = si.get("Parent IDs") or []
                elif ev == "SparkListenerStageCompleted":
                    si = e.get("Stage Info") or {}
                    sid = si.get("Stage ID")
                    s = self.stages.setdefault(sid, {})
                    s["end"] = si.get("Completion Time")
                    s["failure"] = si.get("Failure Reason")
                elif ev == "SparkListenerTaskStart":
                    sid = e.get("Stage ID")
                    ti = e.get("Task Info") or {}
                    if ti.get("Speculative"):
                        self.task_per_stage[sid]["speculative"] += 1
                elif ev == "SparkListenerTaskEnd":
                    self._handle_task_end(e)

    def _handle_task_end(self, e: dict[str, Any]) -> None:
        sid = e.get("Stage ID")
        ti = e.get("Task Info") or {}
        tm = e.get("Task Metrics") or {}
        sr = tm.get("Shuffle Read Metrics") or {}
        sw = tm.get("Shuffle Write Metrics") or {}
        im = tm.get("Input Metrics") or {}
        om = tm.get("Output Metrics") or {}
        reason = e.get("Reason")

        dur = (ti.get("Finish Time") or 0) - (ti.get("Launch Time") or 0)
        agg = self.task_per_stage[sid]
        agg["total"] += 1
        agg["sumdur_ms"] += dur
        if dur > agg["max_dur_ms"]:
            agg["max_dur_ms"] = dur
        agg["run_ms"] += tm.get("Executor Run Time") or 0
        agg["cpu_ns"] += tm.get("Executor CPU Time") or 0
        agg["gc_ms"] += tm.get("JVM GC Time") or 0
        agg["fetch_wait_ms"] += sr.get("Fetch Wait Time") or 0
        agg["shuffle_read"] += (sr.get("Local Bytes Read") or 0) + (
            sr.get("Remote Bytes Read") or 0
        )
        agg["shuffle_write"] += sw.get("Shuffle Bytes Written") or 0
        agg["input_bytes"] += im.get("Bytes Read") or 0
        agg["output_bytes"] += om.get("Bytes Written") or 0
        agg["records_out"] += om.get("Records Written") or 0
        agg["spill_mem"] += tm.get("Memory Bytes Spilled") or 0
        agg["spill_disk"] += tm.get("Disk Bytes Spilled") or 0
        if len(agg["task_durs_ms"]) < self._dur_cap:
            agg["task_durs_ms"].append(dur)

        if isinstance(reason, dict):
            rkind = reason.get("Reason") or ""
            if rkind == "TaskKilled":
                agg["killed"] += 1
                kreason = reason.get("killReason") or "unknown"
                self.killed_reasons[kreason] += 1
                if "speculat" in kreason.lower() or "another attempt" in kreason.lower():
                    agg["killed_spec"] += 1
            if "Fail" in rkind:
                agg["failed"] += 1
        if ti.get("Attempt", 0) > 0 and not ti.get("Speculative"):
            agg["retried"] += 1

        if self.stage_filter is not None and sid == self.stage_filter:
            self.filtered_tasks.append(
                {
                    "stage": sid,
                    "task": ti.get("Task ID"),
                    "exec": ti.get("Executor ID"),
                    "host": ti.get("Host"),
                    "speculative": ti.get("Speculative"),
                    "attempt": ti.get("Attempt"),
                    "launch": ti.get("Launch Time"),
                    "finish": ti.get("Finish Time"),
                    "dur_ms": dur,
                    "run_ms": tm.get("Executor Run Time") or 0,
                    "cpu_ns": tm.get("Executor CPU Time") or 0,
                    "deser_ms": tm.get("Executor Deserialize Time") or 0,
                    "result_ser_ms": tm.get("Result Serialization Time") or 0,
                    "gc_ms": tm.get("JVM GC Time") or 0,
                    "peak_mem": tm.get("Peak Execution Memory") or 0,
                    "spill_mem": tm.get("Memory Bytes Spilled") or 0,
                    "spill_disk": tm.get("Disk Bytes Spilled") or 0,
                    "fetch_wait_ms": sr.get("Fetch Wait Time") or 0,
                    "local_bytes": sr.get("Local Bytes Read") or 0,
                    "remote_bytes": sr.get("Remote Bytes Read") or 0,
                    "shuffle_write": sw.get("Shuffle Bytes Written") or 0,
                    "input_bytes": im.get("Bytes Read") or 0,
                    "output_bytes": om.get("Bytes Written") or 0,
                    "records_out": om.get("Records Written") or 0,
                    "records_in": im.get("Records Read") or 0,
                    "sched_delay_ms": ti.get("Scheduler Delay") or 0,
                    "getting_result_ms": ti.get("Getting Result Time") or 0,
                }
            )

    # ---------- helpers ----------
    def total_min(self) -> float | None:
        if self.app_start and self.app_end:
            return (self.app_end - self.app_start) / 60000
        return None

    def stage_wall_min(self, sid: int) -> float:
        s = self.stages.get(sid, {})
        if s.get("sub") and s.get("end"):
            return (s["end"] - s["sub"]) / 60000
        return 0.0


# ---------------------------------------------------------------------------
# Formatting helpers
# ---------------------------------------------------------------------------
def fmt_bytes(n: int) -> str:
    for unit in ("B", "KB", "MB", "GB", "TB", "PB"):
        if abs(n) < 1024:
            return f"{n:.1f}{unit}"
        n /= 1024
    return f"{n:.1f}EB"


def hr(title: str = "", width: int = 100, ch: str = "=") -> str:
    if not title:
        return ch * width
    pad = width - len(title) - 2
    left = pad // 2
    right = pad - left
    return f"{ch * left} {title} {ch * right}"


# ---------------------------------------------------------------------------
# Subcommand handlers
# ---------------------------------------------------------------------------
def cmd_overview(p: ParsedLog) -> None:
    print(hr("OVERVIEW"))
    print(f"App:           {p.app_id}  ({p.app_name})")
    print(f"Log:           {p.path}")
    total = p.total_min()
    if total is None:
        print("Total runtime: (incomplete — no ApplicationEnd)")
    else:
        print(f"Total runtime: {total:.2f} min")

    n_added = len(p.execs)
    peak, peak_t = _executor_peak(p)
    print(
        f"Executors:     {n_added} added, peak {peak} concurrent"
        + (f" @ T+{(peak_t - p.app_start) / 60000:.1f}m" if peak_t and p.app_start else "")
    )

    # SQL durations
    sql = []
    for sid in sorted(p.sql_starts):
        s = p.sql_starts[sid]
        e_ = p.sql_ends.get(sid)
        if e_:
            sql.append((sid, (e_ - s) / 60000, s, e_))
    print()
    print(f"SQL executions (>0.5 min, top 10):")
    for sid, dur, s, e_ in sorted(sql, key=lambda x: -x[1])[:10]:
        if dur < 0.5:
            continue
        t0 = (s - p.app_start) / 60000 if p.app_start else 0
        t1 = (e_ - p.app_start) / 60000 if p.app_start else 0
        print(
            f"  SQL {sid:>4}: {dur:>6.2f}m  T+{t0:>5.2f}m -> T+{t1:>5.2f}m   "
            f"{p.sql_desc.get(sid, '')}"
        )

    # top stages
    print()
    print(f"Top 12 stages by wall time:")
    stages = []
    for sid, info in p.stages.items():
        wall = p.stage_wall_min(sid)
        if wall > 0:
            stages.append((sid, wall, info))
    stages.sort(key=lambda x: -x[1])
    for sid, wall, info in stages[:12]:
        agg = p.task_per_stage[sid]
        n = info.get("ntasks", 0)
        shf = agg["shuffle_read"] / (1024**3)
        sw = agg["shuffle_write"] / (1024**3)
        run = agg["run_ms"] / 60000
        spec = f" spec={agg['speculative']}" if agg["speculative"] else ""
        kspec = f" killSpec={agg['killed_spec']}" if agg["killed_spec"] else ""
        fail = f" fail={agg['failed']}" if agg["failed"] else ""
        print(
            f"  s{sid:>4} wall={wall:>6.2f}m tasks={n:>5} shfR={shf:>6.1f}GB "
            f"shfW={sw:>6.1f}GB sumRun={run:>6.0f}m{spec}{kspec}{fail}"
        )

    # efficiency
    if total and peak:
        total_run_min = sum(a["run_ms"] / 60000 for a in p.task_per_stage.values())
        capacity = peak * _typical_cores(p) * total
        print()
        print(
            f"Efficiency:    useful core-min={total_run_min:>7.0f}   "
            f"capacity (peak*{_typical_cores(p)}c*wall)={capacity:>7.0f}   "
            f"naive={total_run_min / capacity * 100:>5.1f}%"
        )


def cmd_executors(p: ParsedLog) -> None:
    print(hr("EXECUTOR TOPOLOGY"))
    if not p.execs:
        print("No executors found.")
        return
    hosts = [e["host"] for e in p.execs.values() if e["host"]]
    cores_dist: dict[int, int] = defaultdict(int)
    for e in p.execs.values():
        cores_dist[e["cores"]] += 1
    host_counts: dict[str, int] = defaultdict(int)
    for h in hosts:
        host_counts[h] += 1
    per_host_dist: dict[int, int] = defaultdict(int)
    for c in host_counts.values():
        per_host_dist[c] += 1

    print(f"Total executors added:    {len(p.execs)}")
    print(f"Distinct hosts:           {len(set(hosts))}")
    print(f"Cores per executor:       " + ", ".join(
        f"{k}c x {v}" for k, v in sorted(cores_dist.items(), key=lambda x: -x[1])
    ))
    print(f"Packing (execs per host):")
    for execs_per_host, n_hosts in sorted(per_host_dist.items()):
        print(
            f"  {n_hosts:>4} hosts x {execs_per_host} exec(s) "
            f"= {n_hosts * execs_per_host} executors"
        )

    # Peak concurrent
    peak, peak_t = _executor_peak(p)
    print(f"Peak concurrent:          {peak}")

    # Removed during run
    removed_during = [
        e for e in p.execs.values()
        if e["removed_ts"] and p.app_end and e["removed_ts"] < p.app_end - 1000
    ]
    if removed_during:
        print()
        print(f"Executors removed during run: {len(removed_during)}")
        reason_counts: dict[str, int] = defaultdict(int)
        for e in removed_during:
            r = (e["removed_reason"] or "")[:80]
            reason_counts[r] += 1
        for r, c in sorted(reason_counts.items(), key=lambda x: -x[1])[:10]:
            print(f"  [{c:>4}]  {r}")

    # Total Spark task slots at peak (NOT Synapse pool vcore allocation,
    # which is platform-side and not visible in the eventlog).
    typical = _typical_cores(p)
    print()
    print(f"Spark task slots at peak: {peak * typical}  ({peak} execs x {typical}c)")


def cmd_ramp(p: ParsedLog, interval_sec: int = 120) -> None:
    print(hr(f"EXECUTOR RAMP (every {interval_sec}s)"))
    if not (p.app_start and p.app_end):
        print("Missing application start/end timestamps.")
        return
    events = []
    for eid, info in p.execs.items():
        if info["added_ts"]:
            events.append((info["added_ts"], +1))
        if info["removed_ts"]:
            events.append((info["removed_ts"], -1))
    events.sort()
    sample = p.app_start
    interval_ms = interval_sec * 1000
    cur = 0
    idx = 0
    while sample <= p.app_end:
        while idx < len(events) and events[idx][0] <= sample:
            cur += events[idx][1]
            idx += 1
        delta = (sample - p.app_start) / 60000
        bar = "#" * min(cur, 100)
        print(f"  T+{delta:>5.1f}m: {cur:>3} execs {bar}")
        sample += interval_ms


def cmd_stages(p: ParsedLog, top: int = 25, min_min: float = 0.0) -> None:
    print(hr("STAGES BY WALL TIME"))
    rows = []
    for sid, info in p.stages.items():
        wall = p.stage_wall_min(sid)
        if wall < min_min:
            continue
        rows.append((sid, wall, info))
    rows.sort(key=lambda x: -x[1])
    print(
        f"  {'stage':>6} {'wall_min':>9} {'tasks':>6} {'shfR_GB':>8} {'shfW_GB':>8} "
        f"{'out_rec':>13} {'sumRun_min':>10} {'failed':>6} {'spec':>5}  name"
    )
    for sid, wall, info in rows[:top]:
        agg = p.task_per_stage[sid]
        print(
            f"  s{sid:>5} {wall:>9.2f} {info.get('ntasks', 0):>6} "
            f"{agg['shuffle_read'] / (1024**3):>8.1f} "
            f"{agg['shuffle_write'] / (1024**3):>8.1f} "
            f"{agg['records_out']:>13,} "
            f"{agg['run_ms'] / 60000:>10.0f} "
            f"{agg['failed']:>6} {agg['speculative']:>5}  {info.get('name', '')[:60]}"
        )


def cmd_stage(p: ParsedLog, stage_id: int) -> None:
    if not p.filtered_tasks:
        print(f"No tasks found for stage {stage_id}.")
        return
    tasks = p.filtered_tasks
    info = p.stages.get(stage_id, {})
    wall = p.stage_wall_min(stage_id)
    print(hr(f"STAGE {stage_id} DEEP DIVE"))
    print(f"Name:       {info.get('name', '?')}")
    print(f"Wall time:  {wall:.2f} min   tasks collected: {len(tasks)}")
    print()

    def stats(field: str, divisor: float = 1.0, label: str | None = None) -> None:
        vals = sorted(t[field] / divisor for t in tasks)
        n = len(vals)
        if n == 0:
            return
        label = label or field
        print(
            f"  {label:<28} min={vals[0]:>10.1f} p50={vals[n // 2]:>10.1f} "
            f"p90={vals[n * 9 // 10]:>10.1f} p99={vals[min(n - 1, n * 99 // 100)]:>10.1f} "
            f"max={vals[-1]:>10.1f} sum={sum(vals):>12.1f}"
        )

    print("Time metrics (seconds):")
    stats("dur_ms", 1000, "total task duration")
    stats("run_ms", 1000, "executor run time")
    stats("cpu_ns", 1e9, "executor CPU time")
    stats("gc_ms", 1000, "JVM GC time")
    stats("fetch_wait_ms", 1000, "shuffle fetch wait")
    stats("deser_ms", 1000, "executor deserialize")
    stats("sched_delay_ms", 1000, "scheduler delay")
    stats("getting_result_ms", 1000, "getting result")
    print()
    print("Memory metrics (MB):")
    stats("peak_mem", 1024**2, "peak exec memory")
    stats("spill_mem", 1024**2, "memory spill")
    stats("spill_disk", 1024**2, "disk spill")
    print()
    print("Shuffle metrics (MB):")
    stats("local_bytes", 1024**2, "local bytes read")
    stats("remote_bytes", 1024**2, "remote bytes read")
    stats("shuffle_write", 1024**2, "shuffle write")
    print()
    print("I/O metrics (MB):")
    stats("input_bytes", 1024**2, "input bytes")
    stats("output_bytes", 1024**2, "output bytes")

    # CPU utilization
    print()
    print(hr("CPU UTILIZATION (per task)"))
    cpu_pct, gc_pct, fetch_pct = [], [], []
    for t in tasks:
        run = t["run_ms"]
        if run <= 0:
            continue
        cpu_pct.append((t["cpu_ns"] / 1e6) / run * 100)
        gc_pct.append(t["gc_ms"] / run * 100)
        fetch_pct.append(t["fetch_wait_ms"] / run * 100)

    def pstat(vals: list[float], label: str) -> None:
        if not vals:
            return
        vals = sorted(vals)
        n = len(vals)
        print(
            f"  {label:<25} p10={vals[n // 10]:>6.1f}% p50={vals[n // 2]:>6.1f}% "
            f"p90={vals[n * 9 // 10]:>6.1f}% max={vals[-1]:>6.1f}% "
            f"avg={sum(vals) / n:>6.1f}%"
        )

    pstat(cpu_pct, "CPU / run")
    pstat(gc_pct, "GC / run")
    pstat(fetch_pct, "fetch_wait / run")

    # Time breakdown
    total_dur = sum(t["dur_ms"] for t in tasks) / 1000 / 3600
    if total_dur > 0:
        total_run = sum(t["run_ms"] for t in tasks) / 1000 / 3600
        total_cpu = sum(t["cpu_ns"] for t in tasks) / 1e9 / 3600
        total_gc = sum(t["gc_ms"] for t in tasks) / 1000 / 3600
        total_fetch = sum(t["fetch_wait_ms"] for t in tasks) / 1000 / 3600
        total_deser = sum(t["deser_ms"] for t in tasks) / 1000 / 3600
        total_sched = sum(t["sched_delay_ms"] for t in tasks) / 1000 / 3600
        unaccounted = total_run - total_cpu - total_gc - total_fetch
        print()
        print(hr(f"TIME BREAKDOWN (sum across {len(tasks)} tasks, hours)"))
        print(f"  task duration:      {total_dur:>8.1f}h (100.0%)")
        print(f"  executor run:       {total_run:>8.1f}h ({total_run / total_dur * 100:>5.1f}%)")
        print(f"  executor CPU:       {total_cpu:>8.1f}h ({total_cpu / total_dur * 100:>5.1f}%)")
        print(f"  JVM GC:             {total_gc:>8.1f}h ({total_gc / total_dur * 100:>5.1f}%)")
        print(f"  shuffle fetch wait: {total_fetch:>8.1f}h ({total_fetch / total_dur * 100:>5.1f}%)")
        print(f"  deserialize:        {total_deser:>8.1f}h ({total_deser / total_dur * 100:>5.1f}%)")
        print(f"  scheduler delay:    {total_sched:>8.1f}h ({total_sched / total_dur * 100:>5.1f}%)")
        print(
            f"  unaccounted in run: {unaccounted:>8.1f}h "
            f"({unaccounted / total_dur * 100:>5.1f}%) <- typically I/O write wait"
        )

    # per-executor distribution
    print()
    print(hr("TASKS PER EXECUTOR"))
    per_ex: dict[str, list[int]] = defaultdict(list)
    for t in tasks:
        per_ex[t["exec"]].append(t["dur_ms"])
    print(f"  {'exec':>5} {'count':>5} {'avg_s':>8} {'max_s':>8} {'sum_min':>8}")
    for ex in sorted(per_ex, key=lambda x: int(x) if x and str(x).isdigit() else 0):
        d = per_ex[ex]
        print(
            f"  {str(ex):>5} {len(d):>5} {sum(d) / len(d) / 1000:>8.1f} "
            f"{max(d) / 1000:>8.1f} {sum(d) / 60000:>8.1f}"
        )

    # Top slow tasks
    print()
    print(hr("TOP 10 SLOWEST TASKS"))
    slow = sorted(tasks, key=lambda x: -x["dur_ms"])[:10]
    for t in slow:
        attempt = t.get("attempt") or 0
        flags = []
        if t.get("speculative"):
            flags.append("spec")
        if attempt > 0:
            flags.append(f"att={attempt}")
        flag_str = " " + ",".join(flags) if flags else ""
        print(
            f"  task={t['task']:>7} exec={t['exec']:>4} host={t['host']:<40} "
            f"dur={t['dur_ms'] / 1000:>6.1f}s run={t['run_ms'] / 1000:>6.1f}s "
            f"gc={t['gc_ms'] / 1000:>5.1f}s fetch={t['fetch_wait_ms'] / 1000:>5.1f}s"
            f"{flag_str}"
        )


def cmd_skew(p: ParsedLog, top: int = 15) -> None:
    print(hr("STAGE SKEW (max / median task duration)"))
    print(f"  {'stage':>6} {'tasks':>6} {'median_s':>9} {'max_s':>9} {'ratio':>7} {'wall_min':>9}  name")
    rows = []
    for sid, agg in p.task_per_stage.items():
        durs = agg["task_durs_ms"]
        if len(durs) < 4:
            continue
        med = median(durs)
        if med <= 0:
            continue
        mx = max(durs)
        rows.append((sid, mx / med, med, mx, len(durs)))
    rows.sort(key=lambda x: -x[1])
    for sid, ratio, med, mx, n in rows[:top]:
        info = p.stages.get(sid, {})
        wall = p.stage_wall_min(sid)
        print(
            f"  s{sid:>5} {n:>6} {med / 1000:>9.1f} {mx / 1000:>9.1f} "
            f"{ratio:>7.1f} {wall:>9.2f}  {info.get('name', '')[:60]}"
        )


def cmd_shuffle(p: ParsedLog, top: int = 15) -> None:
    print(hr("TOP STAGES BY SHUFFLE BYTES"))
    rows = []
    for sid, agg in p.task_per_stage.items():
        total = agg["shuffle_read"] + agg["shuffle_write"]
        if total > 0:
            rows.append((sid, agg["shuffle_read"], agg["shuffle_write"], total))
    rows.sort(key=lambda x: -x[3])
    print(f"  {'stage':>6} {'shfRead':>11} {'shfWrite':>11} {'total':>11} {'wall_min':>9}  name")
    for sid, rd, wr, tot in rows[:top]:
        info = p.stages.get(sid, {})
        print(
            f"  s{sid:>5} {fmt_bytes(rd):>11} {fmt_bytes(wr):>11} "
            f"{fmt_bytes(tot):>11} {p.stage_wall_min(sid):>9.2f}  "
            f"{info.get('name', '')[:60]}"
        )


def cmd_speculation(p: ParsedLog) -> None:
    print(hr("SPECULATION"))
    total_spec = sum(a["speculative"] for a in p.task_per_stage.values())
    total_killed = sum(a["killed"] for a in p.task_per_stage.values())
    total_killed_spec = sum(a["killed_spec"] for a in p.task_per_stage.values())
    print(f"Total speculative tasks launched:        {total_spec}")
    print(f"Total tasks killed (any reason):         {total_killed}")
    print(f"Total tasks killed (speculation reason): {total_killed_spec}")
    if p.killed_reasons:
        print()
        print("Top kill reasons:")
        for r, c in sorted(p.killed_reasons.items(), key=lambda x: -x[1])[:10]:
            print(f"  [{c:>4}]  {r}")
    print()
    print("Stages with speculation activity:")
    rows = [
        (sid, a)
        for sid, a in p.task_per_stage.items()
        if a["speculative"] or a["killed_spec"]
    ]
    rows.sort(key=lambda x: -x[1]["speculative"])
    for sid, a in rows[:20]:
        wall = p.stage_wall_min(sid)
        print(
            f"  s{sid:>4} wall={wall:>6.2f}m tasks={a['total']:>5} "
            f"spec={a['speculative']:>4} killed_spec={a['killed_spec']:>4} "
            f"killed_total={a['killed']:>4}"
        )


def cmd_spill(p: ParsedLog, top: int = 15) -> None:
    print(hr("TOP STAGES BY SPILL"))
    rows = []
    for sid, agg in p.task_per_stage.items():
        total = agg["spill_mem"] + agg["spill_disk"]
        if total > 0:
            rows.append((sid, agg["spill_mem"], agg["spill_disk"], total))
    if not rows:
        print("No spill observed.")
        return
    rows.sort(key=lambda x: -x[3])
    print(f"  {'stage':>6} {'spill_mem':>11} {'spill_disk':>11} {'total':>11} {'wall_min':>9}  name")
    for sid, sm, sd, tot in rows[:top]:
        info = p.stages.get(sid, {})
        print(
            f"  s{sid:>5} {fmt_bytes(sm):>11} {fmt_bytes(sd):>11} "
            f"{fmt_bytes(tot):>11} {p.stage_wall_min(sid):>9.2f}  "
            f"{info.get('name', '')[:60]}"
        )


def cmd_output(p: ParsedLog, top: int = 10) -> None:
    print(hr("TOP STAGES BY OUTPUT RECORDS"))
    rows = []
    for sid, agg in p.task_per_stage.items():
        if agg["records_out"] > 0:
            rows.append((sid, agg["records_out"], agg["output_bytes"]))
    rows.sort(key=lambda x: -x[1])
    print(f"  {'stage':>6} {'records':>15} {'bytes':>11} {'wall_min':>9}  name")
    for sid, rec, by in rows[:top]:
        info = p.stages.get(sid, {})
        print(
            f"  s{sid:>5} {rec:>15,} {fmt_bytes(by):>11} "
            f"{p.stage_wall_min(sid):>9.2f}  {info.get('name', '')[:60]}"
        )


def cmd_sql(p: ParsedLog) -> None:
    print(hr("SQL EXECUTIONS"))
    print(f"  {'id':>4} {'dur_min':>8} {'start':>9} {'end':>9}  description")
    sql = []
    for sid in sorted(p.sql_starts):
        s = p.sql_starts[sid]
        e_ = p.sql_ends.get(sid)
        dur = (e_ - s) / 60000 if e_ else None
        sql.append((sid, dur, s, e_))
    for sid, dur, s, e_ in sorted(sql, key=lambda x: -(x[1] or -1)):
        dur_s = f"{dur:>8.2f}" if dur is not None else " (open)"
        t0 = f"T+{(s - p.app_start) / 60000:>6.2f}m" if p.app_start else "?"
        t1 = f"T+{(e_ - p.app_start) / 60000:>6.2f}m" if e_ and p.app_start else "?"
        print(f"  {sid:>4} {dur_s} {t0:>9} {t1:>9}  {p.sql_desc.get(sid, '')}")


def cmd_plan(p: ParsedLog, sql_id: int) -> None:
    print(hr(f"SQL PLAN {sql_id}"))
    plan = p.sql_plans.get(sql_id)
    if not plan:
        print(f"No physical plan captured for SQL execution {sql_id}.")
        return
    print(f"Description: {p.sql_desc.get(sql_id, '')}")
    print()
    print(plan)


def cmd_all(p: ParsedLog) -> None:
    cmd_overview(p)
    print()
    cmd_executors(p)
    print()
    cmd_ramp(p)
    print()
    cmd_stages(p)
    print()
    cmd_skew(p)
    print()
    cmd_shuffle(p)
    print()
    cmd_spill(p)
    print()
    cmd_speculation(p)


# ---------------------------------------------------------------------------
# Comparison across multiple logs
# ---------------------------------------------------------------------------
def cmd_compare(logs: list[ParsedLog], subcmd: str, **kw: Any) -> None:
    for i, p in enumerate(logs):
        name = os.path.basename(p.path)[:25]
        print(hr(f"[{name}]  {p.path}"))
        if subcmd == "overview":
            cmd_overview(p)
        elif subcmd == "executors":
            cmd_executors(p)
        elif subcmd == "stages":
            cmd_stages(p, top=kw.get("top", 15))
        elif subcmd == "skew":
            cmd_skew(p, top=kw.get("top", 10))
        elif subcmd == "shuffle":
            cmd_shuffle(p, top=kw.get("top", 10))
        elif subcmd == "spill":
            cmd_spill(p, top=kw.get("top", 10))
        elif subcmd == "speculation":
            cmd_speculation(p)
        elif subcmd == "sql":
            cmd_sql(p)
        else:
            print(f"  (compare: unknown subcommand '{subcmd}')")
        if i < len(logs) - 1:
            print()

    # short one-line summary across all rounds
    print()
    print(hr("COMPARISON SUMMARY"))
    print(f"  {'log':<30} {'wall_min':>9} {'peak_exec':>9} {'hosts':>6} {'spec_tasks':>11} {'failed':>7}")
    for p in logs:
        peak, _ = _executor_peak(p)
        hosts = len({e["host"] for e in p.execs.values() if e["host"]})
        spec = sum(a["speculative"] for a in p.task_per_stage.values())
        fail = sum(a["failed"] for a in p.task_per_stage.values())
        wall = p.total_min() or 0
        name = os.path.basename(p.path)[:30]
        print(
            f"  {name:<30} {wall:>9.2f} {peak:>9} {hosts:>6} {spec:>11} {fail:>7}"
        )


# ---------------------------------------------------------------------------
# Helpers used across handlers
# ---------------------------------------------------------------------------
def _executor_peak(p: ParsedLog) -> tuple[int, int]:
    events = []
    for info in p.execs.values():
        if info["added_ts"]:
            events.append((info["added_ts"], +1))
        if info["removed_ts"]:
            events.append((info["removed_ts"], -1))
    events.sort()
    cur = peak = 0
    peak_t = 0
    for ts, d in events:
        cur += d
        if cur > peak:
            peak = cur
            peak_t = ts
    return peak, peak_t


def _typical_cores(p: ParsedLog) -> int:
    cores = [e["cores"] for e in p.execs.values() if e["cores"]]
    if not cores:
        return 1
    counts: dict[int, int] = defaultdict(int)
    for c in cores:
        counts[c] += 1
    return max(counts, key=counts.get)


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------
def build_parser() -> argparse.ArgumentParser:
    ap = argparse.ArgumentParser(
        prog="spark_eventlog_analyze",
        description="Generic Spark eventlog analyzer (overview/executors/ramp/"
                    "stages/stage/skew/shuffle/spill/speculation/sql/plan/all/compare).",
    )
    sub = ap.add_subparsers(dest="cmd", required=True)

    def with_log(parser: argparse.ArgumentParser) -> argparse.ArgumentParser:
        parser.add_argument("log", help="Spark eventlog file or directory containing application_*")
        return parser

    with_log(sub.add_parser("overview", help="Application summary"))
    with_log(sub.add_parser("executors", help="Executor topology and packing"))
    r = with_log(sub.add_parser("ramp", help="Executor count over time"))
    r.add_argument("--interval", type=int, default=120, help="Seconds between samples (default 120)")
    s = with_log(sub.add_parser("stages", help="Stages sorted by wall time"))
    s.add_argument("--top", type=int, default=25)
    s.add_argument("--min-min", type=float, default=0.0, help="Filter out stages shorter than N min")
    sd = with_log(sub.add_parser("stage", help="Per-task internals for one stage"))
    sd.add_argument("stage_id", type=int)
    sk = with_log(sub.add_parser("skew", help="Per-stage skew (max/median task duration)"))
    sk.add_argument("--top", type=int, default=15)
    sh = with_log(sub.add_parser("shuffle", help="Stages by shuffle bytes"))
    sh.add_argument("--top", type=int, default=15)
    sp = with_log(sub.add_parser("spill", help="Stages with spill"))
    sp.add_argument("--top", type=int, default=15)
    o = with_log(sub.add_parser("output", help="Stages by records written"))
    o.add_argument("--top", type=int, default=10)
    with_log(sub.add_parser("speculation", help="Speculation activity"))
    with_log(sub.add_parser("sql", help="SQL executions sorted by duration"))
    pl = with_log(sub.add_parser("plan", help="Physical plan for one SQL execution"))
    pl.add_argument("sql_id", type=int)
    with_log(sub.add_parser("all", help="Overview + executors + ramp + stages + skew + shuffle + spill + speculation"))

    cmp_ = sub.add_parser("compare", help="Run a subcommand across multiple logs")
    cmp_.add_argument(
        "what",
        choices=["overview", "executors", "stages", "skew", "shuffle", "spill", "speculation", "sql"],
    )
    cmp_.add_argument("logs", nargs="+", help="Two or more eventlog files or directories")
    cmp_.add_argument("--top", type=int, default=10)
    return ap


def main(argv: list[str] | None = None) -> int:
    ap = build_parser()
    args = ap.parse_args(argv)

    if args.cmd == "compare":
        if len(args.logs) < 2:
            ap.error("compare requires at least 2 logs")
        parsed = [ParsedLog(resolve_log_path(p)) for p in args.logs]
        cmd_compare(parsed, args.what, top=args.top)
        return 0

    path = resolve_log_path(args.log)
    stage_filter = args.stage_id if args.cmd == "stage" else None
    p = ParsedLog(path, stage_filter=stage_filter)

    if args.cmd == "overview":
        cmd_overview(p)
    elif args.cmd == "executors":
        cmd_executors(p)
    elif args.cmd == "ramp":
        cmd_ramp(p, interval_sec=args.interval)
    elif args.cmd == "stages":
        cmd_stages(p, top=args.top, min_min=args.min_min)
    elif args.cmd == "stage":
        cmd_stage(p, args.stage_id)
    elif args.cmd == "skew":
        cmd_skew(p, top=args.top)
    elif args.cmd == "shuffle":
        cmd_shuffle(p, top=args.top)
    elif args.cmd == "spill":
        cmd_spill(p, top=args.top)
    elif args.cmd == "output":
        cmd_output(p, top=args.top)
    elif args.cmd == "speculation":
        cmd_speculation(p)
    elif args.cmd == "sql":
        cmd_sql(p)
    elif args.cmd == "plan":
        cmd_plan(p, args.sql_id)
    elif args.cmd == "all":
        cmd_all(p)
    else:
        ap.error(f"Unknown subcommand: {args.cmd}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
