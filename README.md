# Spark Performance Optimization

Local-first Apache Spark performance analysis for OSS Spark, Azure Synapse, and Databricks.

This repository packages three pieces as one distributable tool:

- A Copilot skill containing the iterative optimization methodology.
- A stdlib-only event-log analyzer with a stable command-line interface.
- A VS Code extension for installing the personal skill and launching analysis.

## Current CLI

```powershell
python skill/spark-perf-optimization/scripts/spark_eventlog_analyze.py overview <eventlog-or-zip>
python skill/spark-perf-optimization/scripts/spark_eventlog_analyze.py all <eventlog-or-zip>
python skill/spark-perf-optimization/scripts/spark_eventlog_analyze.py compare overview <baseline> <candidate>
```

Raw event logs, directories, ZIP archives, and gzip files are accepted. The default output and existing subcommands remain compatible with the original analyzer.

## Skill Layout

The complete distributable skill is under `skill/spark-perf-optimization/`. To install it manually, copy that directory to `~/.copilot/skills/spark-perf-optimization` and reload VS Code.

## Development

```powershell
python -m unittest discover -s tests -v
cd extension
npm install
npm run compile
npm run package
```

The analyzer core has no third-party Python dependencies. Node.js is required only to build the VS Code extension and PowerPoint asset.

## Privacy

Spark event logs may contain application names, paths, SQL plans, schema names, and user identifiers. Keep raw logs outside source control and sanitize generated artifacts before sharing them.

## Status

The local analyzer, archive support, skill installer, VS Code commands, tests, and VSIX packaging are implemented. Public skill content uses generic or synthetic examples; identifiable case studies belong in the ignored private overlay described in `PRIVATE_CASE_STUDIES.md`.

The extension manifest targets the intended public repository at `https://github.com/nk2242696/spark-perf-optimization`. Before Marketplace publication, create that repository, confirm that the `nikhil-kumar` Marketplace publisher exists and is accessible, enable private security reporting, and optionally add an owned 128×128 or larger PNG icon.