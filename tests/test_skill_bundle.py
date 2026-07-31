from __future__ import annotations

import re
import unittest
from pathlib import Path


ROOT = Path(__file__).parents[1]
SKILL_ROOT = ROOT / "skill" / "spark-perf-optimization"
FORBIDDEN_PUBLIC_PATTERNS = {
    "internal workload name": r"BacklogUsageByCustomer|USTF_|OfferRestrictions|UsageBySubscription",
    "internal organization name": r"\bBBUC\b|\bCSCP\b",
    "private savings claim": r"\$1\.6M|\$1\.5M|\$91K|\$19\.5K",
    "private performance claim": r"86m\s*(?:->|→)\s*27m|27m warm|34m cold|21 rounds",
    "fixed co-author identity": r"223556219\+Copilot|Co-authored-by:\s*Copilot",
    "private round metric": r"56\.64|27\.31|4594",
    "internal-looking application id": r"application_17\d{6,}",
    "engagement attribution": r"source engagement|historical win ratio|from real engagements",
}


class SkillBundleTests(unittest.TestCase):
    def test_frontmatter_name_matches_directory(self) -> None:
        text = (SKILL_ROOT / "SKILL.md").read_text(encoding="utf-8")
        self.assertTrue(text.startswith("---\n"))
        self.assertIn("name: spark-perf-optimization", text.split("---", 2)[1])
        self.assertEqual(SKILL_ROOT.name, "spark-perf-optimization")

    def test_relative_markdown_links_resolve(self) -> None:
        text = (SKILL_ROOT / "SKILL.md").read_text(encoding="utf-8")
        links = re.findall(r"\[[^]]+\]\(([^)]+)\)", text)
        local_links = [link.split("#", 1)[0] for link in links if "://" not in link]
        missing = [link for link in local_links if not (SKILL_ROOT / link).exists()]
        self.assertEqual(missing, [])

    def test_required_attached_resources_exist(self) -> None:
        required = [
            "scripts/spark_eventlog_analyze.py",
            "references/analyzer-usage.md",
            "references/code-rewrite-patterns.md",
            "references/common-bottlenecks.md",
            "references/cost-analysis.md",
            "assets/build_perf_pptx.js",
            "assets/pr-description-template.md",
            "assets/reviewer-summary-template.md",
        ]
        missing = [path for path in required if not (SKILL_ROOT / path).is_file()]
        self.assertEqual(missing, [])

    def test_public_bundle_has_no_private_case_study_markers(self) -> None:
        public_files = [
            path
            for path in SKILL_ROOT.rglob("*")
            if path.is_file() and path.suffix.lower() in {".js", ".json", ".md", ".py"}
        ]
        findings: list[str] = []
        for path in public_files:
            text = path.read_text(encoding="utf-8")
            for label, pattern in FORBIDDEN_PUBLIC_PATTERNS.items():
                if re.search(pattern, text, flags=re.IGNORECASE):
                    findings.append(f"{path.relative_to(SKILL_ROOT)}: {label}")

        self.assertEqual(findings, [])


if __name__ == "__main__":
    unittest.main()