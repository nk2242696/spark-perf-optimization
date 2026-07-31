# Spark Performance Optimization

Analyze Apache Spark event logs locally and install a guided GitHub Copilot skill for iterative performance work in OSS Spark, Azure Synapse, and Databricks.

## Features

- Analyze raw Spark event logs, event-log directories, gzip files, and ZIP archives.
- Inspect application overview, executors, ramp-up, stages, skew, shuffle, spill, output, speculation, SQL executions, and physical plans.
- Compare baseline and candidate event logs with the bundled command-line analyzer.
- Install or update the bundled `spark-perf-optimization` personal Copilot skill atomically.
- Keep analysis local. The extension does not upload event logs or call an independent model service.

## Requirements

- VS Code 1.96 or newer.
- Python 3.10 or newer available as `python`, `python3`, or through the `sparkPerf.pythonPath` setting.
- GitHub Copilot Chat for the guided skill workflow. The event-log analyzer works without Copilot.

The analyzer uses only the Python standard library.

## Getting Started

1. Open the Command Palette.
2. Run **Spark Perf: Install/Update Spark Performance Skill**.
3. Reload VS Code when prompted.
4. Run **Spark Perf: Analyze Spark Event Log** and select a raw log, gzip file, or ZIP archive.
5. Review the **Spark Performance Optimization** output channel.

Use **Spark Perf: Open CLI Terminal** for the complete analyzer command surface, including baseline-versus-candidate comparisons.

## Commands

| Command | Purpose |
|---|---|
| `Spark Perf: Analyze Spark Event Log` | Select a log and run the complete local analysis. |
| `Spark Perf: Open CLI Terminal` | Open a terminal at the installed analyzer. |
| `Spark Perf: Install/Update Spark Performance Skill` | Install the bundled personal Copilot skill. |
| `Spark Perf: Uninstall Spark Performance Skill` | Remove the skill installed by this extension. |

## Configuration

`Spark Performance Optimization: Python Path` (`sparkPerf.pythonPath`) sets an explicit Python 3.10+ executable. Leave it empty for automatic discovery.

## Privacy and Security

Spark event logs can contain application names, storage paths, SQL plans, schemas, user identifiers, and literals. The extension processes selected files locally and does not add them to source control. Treat analyzer output and generated reports as sensitive until reviewed and sanitized.

ZIP input is streamed without extraction. The analyzer rejects encrypted entries, suspicious compression ratios, archives with excessive entries, and archives whose declared expanded size exceeds its safety limit.

## Limitations

- HTTP storage errors and driver-only failures may require driver or platform logs in addition to the Spark event log.
- Recommendations are evidence-driven starting points, not universal Spark configuration defaults.
- Cost estimates require an approved rate, active executor lifetimes, run cadence, and clearly stated assumptions.

## Support

See [SUPPORT.md](SUPPORT.md) for issue-reporting guidance. Source and issue tracking are hosted at https://github.com/nk2242696/spark-perf-optimization.

## License

MIT
