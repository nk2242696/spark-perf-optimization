# PowerPoint Generator

`build_perf_pptx.js` creates a seven-slide Spark performance report from JSON. It contains no customer, workload, repository, pricing, or performance defaults.

## Generate a deck

```bash
npm install
node build_perf_pptx.js report.json output.pptx
```

The output argument is optional and defaults to `spark-performance-report.pptx` unless `outputFile` is present in the JSON.

## Input example

All values below are synthetic placeholders.

```json
{
  "jobName": "Example Spark Job",
  "author": "Report Author",
  "organization": "Example Organization",
  "date": "YYYY-MM-DD",
  "baseline": {
    "wallTime": "60 min",
    "coreHours": "500",
    "diskSpill": "120 GB",
    "costPerRun": "$50"
  },
  "candidate": {
    "wallTime": "40 min",
    "coreHours": "340",
    "diskSpill": "100 GB",
    "costPerRun": "$34"
  },
  "findings": [
    "One stage accounted for most application wall time.",
    "The physical plan showed an avoidable single-partition exchange."
  ],
  "changes": [
    "Replaced the single-partition write with a measured partition count."
  ],
  "validation": [
    "Output schema, row count, and per-key aggregates matched the baseline.",
    "Three comparable candidate runs were measured."
  ],
  "cost": {
    "rateSource": "Approved internal rate, retrieved YYYY-MM-DD",
    "cadence": "365 successful runs per year",
    "baselineAnnual": "$18,250",
    "candidateAnnual": "$12,410"
  },
  "nextSteps": [
    "Deploy with a rollback path.",
    "Monitor wall time, executor delivery, spill, failures, and output volume."
  ]
}
```

Required fields are `jobName`, `author`, `date`, `baseline`, and `candidate`. Optional list fields fall back to `No data supplied` rather than inventing content.

## Claim hygiene

- Populate the deck only with values traceable to event logs, platform diagnostics, billing data, or an explicitly stated assumption.
- Do not include raw storage paths, user identifiers, SQL literals, access tokens, or customer data.
- Keep successful-run compute, retry waste, and soft operational benefits separate.
- State whether runs used comparable input, runtime, pool, and cache conditions.
- Describe annual cost results as estimates and preserve rate and cadence assumptions.

## Visual QA

Open the generated deck and inspect every slide for clipped text and overflow. Long bullets should be shortened or moved to an appendix. For automated workflows, convert to PDF, render pages to images, and inspect each image before distribution.