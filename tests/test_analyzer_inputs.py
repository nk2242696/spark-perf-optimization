from __future__ import annotations

import gzip
import importlib.util
import io
import json
import tempfile
import unittest
import zipfile
from contextlib import redirect_stdout
from pathlib import Path


ROOT = Path(__file__).parents[1]
ANALYZER_PATH = (
    ROOT
    / "skill"
    / "spark-perf-optimization"
    / "scripts"
    / "spark_eventlog_analyze.py"
)
SPEC = importlib.util.spec_from_file_location("spark_eventlog_analyze", ANALYZER_PATH)
assert SPEC and SPEC.loader
ANALYZER = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(ANALYZER)


def event_log_text() -> str:
    events = [
        {
            "Event": "SparkListenerApplicationStart",
            "Timestamp": 1_000,
            "App ID": "app-test",
            "App Name": "Synthetic test",
        },
        {"Event": "SparkListenerApplicationEnd", "Timestamp": 61_000},
    ]
    return "\n".join(json.dumps(event) for event in events) + "\n"


class AnalyzerInputTests(unittest.TestCase):
    def test_reads_zip_without_extraction(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            archive_path = Path(directory) / "eventlog.zip"
            with zipfile.ZipFile(archive_path, "w", zipfile.ZIP_DEFLATED) as archive:
                archive.writestr("nested/application_test_1", event_log_text())

            parsed = ANALYZER.ParsedLog(str(archive_path))

            self.assertEqual(parsed.app_id, "app-test")
            self.assertEqual(parsed.total_min(), 1.0)

    def test_reads_gzip(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            gzip_path = Path(directory) / "application_test_1.gz"
            with gzip.open(gzip_path, "wt", encoding="utf-8") as stream:
                stream.write(event_log_text())

            parsed = ANALYZER.ParsedLog(str(gzip_path))

            self.assertEqual(parsed.app_name, "Synthetic test")

    def test_preserves_overview_command(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            log_path = Path(directory) / "application_test_1"
            log_path.write_text(event_log_text(), encoding="utf-8")
            output = io.StringIO()

            with redirect_stdout(output):
                exit_code = ANALYZER.main(["overview", str(log_path)])

            self.assertEqual(exit_code, 0)
            self.assertIn("Total runtime: 1.00 min", output.getvalue())


if __name__ == "__main__":
    unittest.main()