const fs = require("fs");
const pptxgen = require("pptxgenjs");

const inputPath = process.argv[2];
if (!inputPath) {
  console.error("Usage: node build_perf_pptx.js <report.json> [output.pptx]");
  process.exit(1);
}

const data = JSON.parse(fs.readFileSync(inputPath, "utf8"));
const required = ["jobName", "author", "date", "baseline", "candidate"];
const missing = required.filter((field) => !data[field]);
if (missing.length) {
  throw new Error(`Missing required fields: ${missing.join(", ")}`);
}

const outputPath = process.argv[3] || data.outputFile || "spark-performance-report.pptx";
const pptx = new pptxgen();
pptx.layout = "LAYOUT_WIDE";
pptx.author = data.author;
pptx.title = `${data.jobName} performance optimization`;
pptx.subject = "Spark event-log performance analysis";
pptx.company = data.organization || "";
pptx.lang = "en-US";

const C = {
  ink: "172033",
  blue: "146C94",
  teal: "19A7A0",
  pale: "EAF6F6",
  paper: "F8FAFC",
  white: "FFFFFF",
  muted: "526071",
  green: "16825D",
  red: "C2413B",
};
const W = 13.333;
const H = 7.5;

function addHeader(slide, title, subtitle) {
  slide.background = { color: C.paper };
  slide.addText(title, {
    x: 0.65, y: 0.45, w: 11.9, h: 0.55,
    fontFace: "Aptos Display", fontSize: 28, bold: true, color: C.ink, margin: 0,
  });
  if (subtitle) {
    slide.addText(subtitle, {
      x: 0.65, y: 1.0, w: 11.9, h: 0.35,
      fontFace: "Aptos", fontSize: 11, color: C.muted, margin: 0,
    });
  }
  slide.addShape(pptx.ShapeType.line, {
    x: 0.65, y: 1.38, w: 12.0, h: 0,
    line: { color: C.teal, width: 2 },
  });
}

function addFooter(slide, page) {
  slide.addText(`${data.jobName} | ${data.date}`, {
    x: 0.65, y: H - 0.35, w: 10, h: 0.2,
    fontFace: "Aptos", fontSize: 8, color: C.muted, margin: 0,
  });
  slide.addText(String(page), {
    x: 12.0, y: H - 0.35, w: 0.65, h: 0.2,
    fontFace: "Aptos", fontSize: 8, color: C.muted, align: "right", margin: 0,
  });
}

function addMetric(slide, x, title, before, after, note) {
  slide.addShape(pptx.ShapeType.rect, {
    x, y: 1.8, w: 3.75, h: 2.2,
    rectRadius: 0.04, fill: { color: C.white }, line: { color: "D8E0E8", width: 1 },
  });
  slide.addText(title, {
    x: x + 0.25, y: 2.05, w: 3.25, h: 0.3,
    fontFace: "Aptos", fontSize: 12, bold: true, color: C.muted, margin: 0,
  });
  slide.addText(`${before}  →  ${after}`, {
    x: x + 0.25, y: 2.55, w: 3.25, h: 0.55,
    fontFace: "Aptos Display", fontSize: 24, bold: true, color: C.blue, margin: 0,
  });
  slide.addText(note || "Measured from comparable runs", {
    x: x + 0.25, y: 3.35, w: 3.25, h: 0.35,
    fontFace: "Aptos", fontSize: 10, color: C.muted, margin: 0,
  });
}

function bulletText(items) {
  return (items || ["No data supplied"]).map((item) => ({
    text: String(item),
    options: { bullet: { indent: 14 }, hanging: 3, breakLine: true },
  }));
}

// Title
{
  const slide = pptx.addSlide();
  slide.background = { color: C.ink };
  slide.addShape(pptx.ShapeType.rect, {
    x: 0, y: 0, w: 0.28, h: H,
    fill: { color: C.teal }, line: { color: C.teal },
  });
  slide.addText(data.jobName, {
    x: 0.85, y: 1.7, w: 11.5, h: 1.0,
    fontFace: "Aptos Display", fontSize: 44, bold: true, color: C.white, margin: 0,
  });
  slide.addText("Spark performance optimization", {
    x: 0.85, y: 2.85, w: 11.5, h: 0.55,
    fontFace: "Aptos", fontSize: 23, color: "9DE5DF", margin: 0,
  });
  slide.addText(`${data.author} | ${data.date}`, {
    x: 0.85, y: 5.85, w: 11.5, h: 0.35,
    fontFace: "Aptos", fontSize: 13, color: "C7D2DE", margin: 0,
  });
}

// Measured outcome
{
  const slide = pptx.addSlide();
  addHeader(slide, "Measured outcome", "Use comparable input, runtime, and pool conditions");
  const metrics = data.metrics || [
    { name: "Wall time", baseline: data.baseline.wallTime, candidate: data.candidate.wallTime },
    { name: "Core-hours", baseline: data.baseline.coreHours, candidate: data.candidate.coreHours },
    { name: "Disk spill", baseline: data.baseline.diskSpill, candidate: data.candidate.diskSpill },
  ];
  metrics.slice(0, 3).forEach((metric, index) => {
    addMetric(slide, 0.65 + index * 4.1, metric.name, metric.baseline ?? "n/a", metric.candidate ?? "n/a", metric.note);
  });
  slide.addText(data.measurementNote || "Values are supplied by the report author; the generator does not infer or verify claims.", {
    x: 0.8, y: 4.7, w: 11.7, h: 0.65,
    fontFace: "Aptos", fontSize: 15, color: C.ink, align: "center", margin: 0,
  });
  addFooter(slide, 2);
}

// Diagnosis
{
  const slide = pptx.addSlide();
  addHeader(slide, "Evidence and diagnosis", "Findings should be traceable to event-log commands or external diagnostics");
  slide.addText(bulletText(data.findings), {
    x: 0.9, y: 1.75, w: 11.5, h: 4.8,
    fontFace: "Aptos", fontSize: 18, color: C.ink, breakLine: false,
    margin: 0.05, paraSpaceAfterPt: 14,
  });
  addFooter(slide, 3);
}

// Changes
{
  const slide = pptx.addSlide();
  addHeader(slide, "Changes tested", "Keep one independently measurable change per round");
  slide.addText(bulletText(data.changes), {
    x: 0.9, y: 1.75, w: 11.5, h: 4.8,
    fontFace: "Aptos", fontSize: 18, color: C.ink,
    margin: 0.05, paraSpaceAfterPt: 14,
  });
  addFooter(slide, 4);
}

// Validation
{
  const slide = pptx.addSlide();
  addHeader(slide, "Correctness and performance validation", "Separate output equivalence from speed and cost measurements");
  slide.addText(bulletText(data.validation), {
    x: 0.9, y: 1.75, w: 11.5, h: 4.8,
    fontFace: "Aptos", fontSize: 18, color: C.ink,
    margin: 0.05, paraSpaceAfterPt: 14,
  });
  addFooter(slide, 5);
}

// Cost
{
  const slide = pptx.addSlide();
  addHeader(slide, "Cost model", "Use active executor lifetimes and disclose rate, region, date, currency, and cadence");
  addMetric(slide, 1.0, "Cost per run", data.baseline.costPerRun ?? "n/a", data.candidate.costPerRun ?? "n/a", data.cost?.rateSource);
  addMetric(slide, 5.0, "Core-hours", data.baseline.coreHours ?? "n/a", data.candidate.coreHours ?? "n/a", "Executors plus driver");
  addMetric(slide, 9.0, "Annual estimate", data.cost?.baselineAnnual ?? "n/a", data.cost?.candidateAnnual ?? "n/a", data.cost?.cadence);
  slide.addText("Estimated savings are not measured billing savings. Preserve the assumptions with the published result.", {
    x: 0.9, y: 4.7, w: 11.5, h: 0.55,
    fontFace: "Aptos", fontSize: 14, color: C.red, bold: true, align: "center", margin: 0,
  });
  addFooter(slide, 6);
}

// Next steps
{
  const slide = pptx.addSlide();
  addHeader(slide, "Rollout and next steps", "Monitor production behavior and retain a rollback path");
  slide.addText(bulletText(data.nextSteps), {
    x: 0.9, y: 1.75, w: 11.5, h: 4.8,
    fontFace: "Aptos", fontSize: 18, color: C.ink,
    margin: 0.05, paraSpaceAfterPt: 14,
  });
  addFooter(slide, 7);
}

pptx.writeFile({ fileName: outputPath });